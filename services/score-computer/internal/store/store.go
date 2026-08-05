// Package store implements store behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iicpc/schemas/topics"
	"github.com/iicpc/score-computer/internal/score"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS score_progress (
	run_group_id      TEXT NOT NULL,
	session_id        TEXT PRIMARY KEY,
	submission_id     TEXT NOT NULL DEFAULT '',
	contestant_id     TEXT NOT NULL DEFAULT '',
	terminal_status   TEXT,
	terminal_at       TIMESTAMPTZ,
	valid_fills       BIGINT,
	total_fills       BIGINT,
	correctness_score DOUBLE PRECISION,
	violation_count   BIGINT,
	correctness_at_ns BIGINT,
	sent_count        BIGINT,
	acked_count       BIGINT,
	matched_count     BIGINT,
	jitter_p50_us         DOUBLE PRECISION,
	jitter_p99_us         DOUBLE PRECISION,
	jitter_p999_us        DOUBLE PRECISION,
	jitter_max_us         DOUBLE PRECISION,
	jitter_inversion_rate DOUBLE PRECISION,
	updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_score_progress_group ON score_progress(run_group_id);
-- Telemetry-completeness counters mirrored from CorrectnessScoreEvent
-- (sent/acked events the validator drained, orders matched across both
-- streams). Idempotent migration for tables created before the counters
-- existed; NULL/0 means UNKNOWN, which the coverage gate treats as ungateable.
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS sent_count BIGINT;
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS acked_count BIGINT;
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS matched_count BIGINT;
-- P-G jitter (docs/multi-contestant-audit.md §5): cross-flow processing-order
-- inversion magnitude, mirrored from CorrectnessScoreEvent. NULL/0 means no
-- recorded inversions (or full-replay mode), same convention as the schema field.
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS jitter_p50_us DOUBLE PRECISION;
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS jitter_p99_us DOUBLE PRECISION;
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS jitter_p999_us DOUBLE PRECISION;
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS jitter_max_us DOUBLE PRECISION;
ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS jitter_inversion_rate DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS scoring_config (
	config_id                    TEXT PRIMARY KEY,
	correctness_dq_threshold     DOUBLE PRECISION NOT NULL,
	max_error_rate               DOUBLE PRECISION NOT NULL,
	max_p99_ns                   BIGINT NOT NULL,
	wave_duration_ns             BIGINT NOT NULL,
	min_coverage                 DOUBLE PRECISION NOT NULL DEFAULT 0.90,
	updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- min_coverage: the telemetry-completeness threshold — minimum per-session
-- matched/sent coverage below which violation-based DQ is suppressed and the
-- run is flagged incomplete_telemetry. Must run before the seed INSERT below
-- references the column on pre-existing tables; the DEFAULT seeds existing
-- config rows at 0.90, judge-tunable like the other knobs.
ALTER TABLE scoring_config ADD COLUMN IF NOT EXISTS min_coverage DOUBLE PRECISION NOT NULL DEFAULT 0.90;

INSERT INTO scoring_config(config_id, correctness_dq_threshold, max_error_rate, max_p99_ns, wave_duration_ns, min_coverage)
VALUES ('v1', 0.95, 0.01, 1000000, 20000000000, 0.90)
ON CONFLICT (config_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS scores (
	run_group_id              TEXT PRIMARY KEY,
	submission_id             TEXT NOT NULL,
	contestant_id             TEXT NOT NULL DEFAULT '',
	team_name                 TEXT NOT NULL DEFAULT '',
	peak_sustained_tps        BIGINT NOT NULL,
	p99_at_peak_ns            BIGINT NOT NULL,
	spike_recovery_ns         BIGINT NOT NULL DEFAULT 0,
	total_correctness         DOUBLE PRECISION NOT NULL,
	disqualified              BOOLEAN NOT NULL,
	disqualification_code     TEXT NOT NULL DEFAULT '',
	rank                      BIGINT,
	rank_delta                BIGINT,
	incomplete_telemetry      BOOLEAN NOT NULL DEFAULT false,
	jitter_p50_us             DOUBLE PRECISION NOT NULL DEFAULT 0,
	jitter_p99_us             DOUBLE PRECISION NOT NULL DEFAULT 0,
	jitter_p999_us            DOUBLE PRECISION NOT NULL DEFAULT 0,
	jitter_max_us             DOUBLE PRECISION NOT NULL DEFAULT 0,
	jitter_inversion_rate     DOUBLE PRECISION NOT NULL DEFAULT 0,
	score_detail              JSONB NOT NULL DEFAULT '{}'::jsonb,
	computed_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
	published_at              TIMESTAMPTZ
);
-- incomplete_telemetry: the run's correctness inputs failed the coverage gate
-- (see scoring_config.min_coverage); persisted on the row so the leaderboard
-- can surface it without parsing score_detail. Idempotent migration; the
-- false default is correct for historical rows (the gate never fired on them).
ALTER TABLE scores ADD COLUMN IF NOT EXISTS incomplete_telemetry BOOLEAN NOT NULL DEFAULT false;
-- P-G jitter, worst-session value across the run-group (see score.Compute).
-- DEFAULT 0 keeps historical rows valid; 0 means no recorded inversions.
ALTER TABLE scores ADD COLUMN IF NOT EXISTS jitter_p50_us DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS jitter_p99_us DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS jitter_p999_us DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS jitter_max_us DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS jitter_inversion_rate DOUBLE PRECISION NOT NULL DEFAULT 0;
DROP INDEX IF EXISTS idx_scores_sort_v2;
CREATE INDEX IF NOT EXISTS idx_scores_sort_v3 ON scores
	(disqualified ASC, peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC);
CREATE INDEX IF NOT EXISTS idx_scores_contestant ON scores(contestant_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_scores_submission ON scores(submission_id, computed_at DESC);
`

const rankOrderBy = `disqualified ASC, peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC`

// Store groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Store struct {
	meta      *pgxpool.Pool
	timescale *pgxpool.Pool
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(ctx context.Context, metadataURL, timescaleURL string) (*Store, error) {
	meta, err := pgxpool.New(ctx, metadataURL)
	if err != nil {
		return nil, fmt.Errorf("metadata pgx pool: %w", err)
	}
	if _, err := meta.Exec(ctx, createTableSQL); err != nil {
		meta.Close()
		return nil, fmt.Errorf("metadata schema: %w", err)
	}
	ts, err := pgxpool.New(ctx, timescaleURL)
	if err != nil {
		meta.Close()
		return nil, fmt.Errorf("timescale pgx pool: %w", err)
	}
	return &Store{meta: meta, timescale: ts}, nil
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Close() {
	s.meta.Close()
	s.timescale.Close()
}

// Healthcheck applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Healthcheck(ctx context.Context) error {
	if err := s.meta.Ping(ctx); err != nil {
		return err
	}
	return s.timescale.Ping(ctx)
}

// Config applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Config(ctx context.Context) (score.Config, error) {
	var cfg score.Config
	var maxP99, waveDuration int64
	err := s.meta.QueryRow(ctx, `
SELECT correctness_dq_threshold, max_error_rate, max_p99_ns, wave_duration_ns, min_coverage
  FROM scoring_config WHERE config_id='v1'`).
		Scan(&cfg.CorrectnessDQThreshold, &cfg.MaxErrorRate, &maxP99, &waveDuration, &cfg.MinCoverage)
	if err != nil {
		return cfg, err
	}
	if maxP99 > 0 {
		cfg.MaxP99NS = uint64(maxP99)
	}
	if waveDuration > 0 {
		cfg.WaveDurationNS = uint64(waveDuration)
	}
	return cfg.WithDefaults(), nil
}

// RecordStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) RecordStatus(ctx context.Context, ev topics.BenchmarkStatusUpdated) ([]string, error) {
	if ev.Status != topics.RunStatusCompleted && ev.Status != topics.RunStatusFailed {
		return nil, nil
	}
	contestantID := ""
	if runGroupID, submissionID, runContestantID, err := s.lookupRun(ctx, ev.SessionID); err == nil {
		if ev.RunGroupID == "" {
			ev.RunGroupID = runGroupID
		}
		if ev.SubmissionID == "" {
			ev.SubmissionID = submissionID
		}
		contestantID = runContestantID
	} else if ev.RunGroupID == "" || ev.SubmissionID == "" {
		return nil, err
	}
	_, err := s.meta.Exec(ctx, `
INSERT INTO score_progress
	(run_group_id, session_id, submission_id, contestant_id, terminal_status, terminal_at)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (session_id) DO UPDATE SET
	run_group_id=EXCLUDED.run_group_id,
	submission_id=EXCLUDED.submission_id,
	contestant_id=EXCLUDED.contestant_id,
	terminal_status=EXCLUDED.terminal_status,
	terminal_at=EXCLUDED.terminal_at,
	updated_at=now()`,
		ev.RunGroupID, ev.SessionID, ev.SubmissionID, contestantID, ev.Status, ev.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return s.ReadyRunGroups(ctx, ev.RunGroupID)
}

// RecordCorrectness applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) RecordCorrectness(ctx context.Context, ev topics.CorrectnessScoreEvent) ([]string, error) {
	runGroupID, submissionID, contestantID, err := s.lookupRun(ctx, ev.SessionID)
	if err != nil {
		return nil, err
	}
	if ev.ContestantID != "" {
		contestantID = ev.ContestantID
	}
	_, err = s.meta.Exec(ctx, `
INSERT INTO score_progress
	(run_group_id, session_id, submission_id, contestant_id, valid_fills, total_fills,
	 correctness_score, violation_count, correctness_at_ns, sent_count, acked_count, matched_count,
	 jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (session_id) DO UPDATE SET
	run_group_id=EXCLUDED.run_group_id,
	submission_id=EXCLUDED.submission_id,
	contestant_id=EXCLUDED.contestant_id,
	valid_fills=EXCLUDED.valid_fills,
	total_fills=EXCLUDED.total_fills,
	correctness_score=EXCLUDED.correctness_score,
	violation_count=EXCLUDED.violation_count,
	correctness_at_ns=EXCLUDED.correctness_at_ns,
	sent_count=EXCLUDED.sent_count,
	acked_count=EXCLUDED.acked_count,
	matched_count=EXCLUDED.matched_count,
	jitter_p50_us=EXCLUDED.jitter_p50_us,
	jitter_p99_us=EXCLUDED.jitter_p99_us,
	jitter_p999_us=EXCLUDED.jitter_p999_us,
	jitter_max_us=EXCLUDED.jitter_max_us,
	jitter_inversion_rate=EXCLUDED.jitter_inversion_rate,
	updated_at=now()`,
		runGroupID, ev.SessionID, submissionID, contestantID, int64(ev.ValidFills), int64(ev.TotalFills),
		ev.CorrectnessScore, int64(ev.ViolationCount), int64(ev.ComputedAtNS),
		int64(ev.SentCount), int64(ev.AckedCount), int64(ev.MatchedCount),
		ev.JitterP50US, ev.JitterP99US, ev.JitterP999US, ev.JitterMaxUS, ev.JitterInvRate)
	if err != nil {
		return nil, err
	}
	return s.ReadyRunGroups(ctx, runGroupID)
}

// lookupRun applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) lookupRun(ctx context.Context, sessionID string) (string, string, string, error) {
	var runGroupID, submissionID, contestantID string
	err := s.meta.QueryRow(ctx, `
SELECT COALESCE(run_group_id,''), submission_id, contestant_id
  FROM runs WHERE session_id=$1`, sessionID).
		Scan(&runGroupID, &submissionID, &contestantID)
	if err != nil {
		return "", "", "", fmt.Errorf("lookup run %s: %w", sessionID, err)
	}
	if runGroupID == "" {
		return "", "", "", fmt.Errorf("run %s has no run_group_id", sessionID)
	}
	return runGroupID, submissionID, contestantID, nil
}

// ReadyRunGroups applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) ReadyRunGroups(ctx context.Context, runGroupID string) ([]string, error) {
	rows, err := s.meta.Query(ctx, `
SELECT p.run_group_id
  FROM score_progress p
  JOIN runs r ON r.session_id=p.session_id
  LEFT JOIN scores sc ON sc.run_group_id=p.run_group_id
 WHERE p.run_group_id=$1 AND sc.run_group_id IS NULL
 GROUP BY p.run_group_id
HAVING COUNT(DISTINCT p.session_id) = (SELECT COUNT(DISTINCT session_id) FROM runs WHERE run_group_id=$1)
   AND COUNT(DISTINCT p.session_id) >= 1
   AND BOOL_AND(p.terminal_status IN ('completed','failed'))
   AND BOOL_AND(p.total_fills IS NOT NULL)`, runGroupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PendingRunGroups applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) PendingRunGroups(ctx context.Context) ([]string, error) {
	rows, err := s.meta.Query(ctx, `
SELECT p.run_group_id
  FROM score_progress p
  JOIN runs r ON r.session_id=p.session_id
  LEFT JOIN scores sc ON sc.run_group_id=p.run_group_id
 WHERE sc.run_group_id IS NULL
 GROUP BY p.run_group_id
HAVING COUNT(DISTINCT p.session_id) = (SELECT COUNT(DISTINCT session_id) FROM runs WHERE run_group_id=p.run_group_id)
   AND COUNT(DISTINCT p.session_id) >= 1
   AND BOOL_AND(p.terminal_status IN ('completed','failed'))
   AND BOOL_AND(p.total_fills IS NOT NULL)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// LoadInput applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) LoadInput(ctx context.Context, runGroupID string) (score.Input, error) {
	cfg, err := s.Config(ctx)
	if err != nil {
		return score.Input{}, err
	}
	var in score.Input
	in.RunGroupID = runGroupID
	in.Config = cfg
	err = s.meta.QueryRow(ctx, `
SELECT g.submission_id, g.contestant_id, COALESCE(sub.team_name, '')
  FROM run_groups g
  LEFT JOIN submissions sub ON sub.submission_id=g.submission_id
 WHERE g.run_group_id=$1`, runGroupID).
		Scan(&in.SubmissionID, &in.ContestantID, &in.TeamName)
	if err != nil {
		return in, err
	}
	rows, err := s.meta.Query(ctx, `
SELECT r.session_id, r.contestant_id, sc.name, sc.duration_ns, sc.task_specs,
       p.valid_fills, p.total_fills, p.violation_count,
       COALESCE(p.sent_count, 0), COALESCE(p.acked_count, 0), COALESCE(p.matched_count, 0),
       COALESCE(p.jitter_p50_us, 0), COALESCE(p.jitter_p99_us, 0), COALESCE(p.jitter_p999_us, 0),
       COALESCE(p.jitter_max_us, 0), COALESCE(p.jitter_inversion_rate, 0)
  FROM runs r
  JOIN scenarios sc ON sc.scenario_id=r.scenario_id
  JOIN score_progress p ON p.session_id=r.session_id
 WHERE r.run_group_id=$1
 ORDER BY sc.sort_order, sc.name`, runGroupID)
	if err != nil {
		return in, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			sess                                                       score.Session
			taskJSON                                                   []byte
			valid, total, violations, durNS                            int64
			sent, acked, matched                                       int64
			jitterP50, jitterP99, jitterP999, jitterMax, jitterInvRate float64
			contestantID                                               string
		)
		if err := rows.Scan(&sess.SessionID, &contestantID, &sess.Scenario, &durNS, &taskJSON, &valid, &total, &violations,
			&sent, &acked, &matched, &jitterP50, &jitterP99, &jitterP999, &jitterMax, &jitterInvRate); err != nil {
			return in, err
		}
		if in.ContestantID == "" {
			in.ContestantID = contestantID
		}
		if err := json.Unmarshal(taskJSON, &sess.TaskSpecs); err != nil {
			return in, fmt.Errorf("unmarshal task_specs: %w", err)
		}
		sess.DurationNS = nonNegativeUint64(durNS)
		sess.Correct = score.Correctness{
			SessionID:      sess.SessionID,
			ValidFills:     nonNegativeUint64(valid),
			TotalFills:     nonNegativeUint64(total),
			ViolationCount: nonNegativeUint64(violations),
			SentCount:      nonNegativeUint64(sent),
			AckedCount:     nonNegativeUint64(acked),
			MatchedCount:   nonNegativeUint64(matched),
			JitterP50US:    jitterP50,
			JitterP99US:    jitterP99,
			JitterP999US:   jitterP999,
			JitterMaxUS:    jitterMax,
			JitterInvRate:  jitterInvRate,
		}
		sess.Metrics, err = s.loadMetrics(ctx, sess.SessionID, in.ContestantID)
		if err != nil {
			return in, err
		}
		in.Sessions = append(in.Sessions, sess)
	}
	return in, rows.Err()
}

// loadMetrics applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) loadMetrics(ctx context.Context, sessionID, contestantID string) ([]score.MetricRow, error) {
	rows, err := s.timescale.Query(ctx, `
SELECT wave_index,
       COALESCE(MAX(p99_ns), 0),
       COALESCE(AVG(tps_1s), 0),
       COALESCE(MAX(error_rate), 0)
  FROM metrics
 WHERE session_id=$1 AND ($2='' OR contestant_id=$2)
 GROUP BY wave_index
 ORDER BY wave_index`, sessionID, contestantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []score.MetricRow
	for rows.Next() {
		var r score.MetricRow
		var p99 int64
		if err := rows.Scan(&r.WaveIndex, &p99, &r.TPS1S, &r.ErrorRate); err != nil {
			return nil, err
		}
		r.P99NS = nonNegativeUint64(p99)
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveScore applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) SaveScore(ctx context.Context, res score.Result) (bool, error) {
	detail, err := json.Marshal(res)
	if err != nil {
		return false, err
	}
	peak, err := uint64ToInt64(res.PeakSustainedTPS)
	if err != nil {
		return false, err
	}
	p99, err := uint64ToInt64(res.P99AtPeakNS)
	if err != nil {
		return false, err
	}
	recovery, err := uint64ToInt64(res.SpikeRecoveryNS)
	if err != nil {
		return false, err
	}
	tag, err := s.meta.Exec(ctx, `
INSERT INTO scores
	(run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps, p99_at_peak_ns,
	 spike_recovery_ns, total_correctness, disqualified, disqualification_code, incomplete_telemetry,
	 jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate,
	 score_detail, computed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,now())
ON CONFLICT (run_group_id) DO NOTHING`,
		res.RunGroupID, res.SubmissionID, res.ContestantID, res.TeamName, peak,
		p99, recovery, res.TotalCorrectness, res.Disqualified,
		res.DisqualificationCode, res.IncompleteTelemetry,
		res.JitterP50US, res.JitterP99US, res.JitterP999US, res.JitterMaxUS, res.JitterInvRate, detail)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// MarkPublished applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) MarkPublished(ctx context.Context, runGroupID string, rank, rankDelta int64) error {
	_, err := s.meta.Exec(ctx, `
UPDATE scores SET rank=$2, rank_delta=$3, published_at=now()
 WHERE run_group_id=$1`, runGroupID, rank, rankDelta)
	return err
}

// RankForRunGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) RankForRunGroup(ctx context.Context, runGroupID string) (int64, error) {
	var rank int64
	err := s.meta.QueryRow(ctx, `
SELECT rank FROM (
	SELECT run_group_id,
	       ROW_NUMBER() OVER (ORDER BY `+rankOrderBy+`) AS rank
	  FROM scores
) ranked
 WHERE run_group_id=$1`, runGroupID).Scan(&rank)
	if err != nil {
		return 0, err
	}
	return rank, nil
}

// ExistingScore applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) ExistingScore(ctx context.Context, runGroupID string) (score.Result, bool, error) {
	var detail []byte
	err := s.meta.QueryRow(ctx, `SELECT score_detail FROM scores WHERE run_group_id=$1`, runGroupID).Scan(&detail)
	if errors.Is(err, pgx.ErrNoRows) {
		return score.Result{}, false, nil
	}
	if err != nil {
		return score.Result{}, false, err
	}
	var res score.Result
	if err := json.Unmarshal(detail, &res); err != nil {
		return score.Result{}, false, err
	}
	return res, true, nil
}

// RecordPoolStats applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) RecordPoolStats(service string) {
	record := func(prefix string, pool *pgxpool.Pool) {
		_ = prefix
		stats := pool.Stat()
		_ = stats
	}
	_ = service
	record("", s.meta)
}

// NowNS performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NowNS() uint64 { return uint64(time.Now().UnixNano()) }

// nonNegativeUint64 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func nonNegativeUint64(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

// uint64ToInt64 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func uint64ToInt64(v uint64) (int64, error) {
	const maxInt64 = uint64(^uint64(0) >> 1)
	if v > maxInt64 {
		return 0, fmt.Errorf("score value %d exceeds PostgreSQL BIGINT", v)
	}
	return int64(v), nil
}

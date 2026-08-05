// Package read implements store behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package read

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxChartPoints         = 20000
	maxViolationRows       = 1000
	maxLeaderboardRows     = 500
	defaultLeaderboardRows = 100
	maxLeaderboardOffset   = 100000
)

const rankedOrder = `disqualified ASC, peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC`

// leaderboardQuerySQL is the static (order-by-independent) body of the paged
// Leaderboard() query. Kept as a named const so tests can pin the selected
// column list without a live Postgres connection.
const leaderboardQuerySQL = `
SELECT rank, run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
       disqualification_code, COALESCE(rank_delta,0), computed_at,
       jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate
  FROM (
	SELECT ROW_NUMBER() OVER (ORDER BY ` + rankedOrder + `) AS rank,
	       run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
	       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
	       disqualification_code, rank_delta, computed_at,
	       jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate
	  FROM scores
  ) ranked
 WHERE ($1='' OR run_group_id=$1)
   AND ($2='' OR submission_id=$2)
   AND ($3='' OR contestant_id=$3)
   AND ($4='' OR team_name ILIKE '%' || $4 || '%')
   AND ($7='' OR EXISTS (
         SELECT 1 FROM runs r2
           JOIN scenarios sc2 ON sc2.scenario_id = r2.scenario_id
          WHERE r2.run_group_id = ranked.run_group_id AND sc2.name = $7))
`

// scoreForRunGroupQuerySQL is the static body of the single-row
// scoreForRunGroup() query, named for the same reason as leaderboardQuerySQL.
const scoreForRunGroupQuerySQL = `
SELECT rank, run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
       disqualification_code, COALESCE(rank_delta,0), computed_at,
       jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate
  FROM (
	SELECT ROW_NUMBER() OVER (ORDER BY ` + rankedOrder + `) AS rank,
	       run_group_id, submission_id, contestant_id, team_name, peak_sustained_tps,
	       p99_at_peak_ns, spike_recovery_ns, total_correctness, disqualified,
	       disqualification_code, rank_delta, computed_at,
	       jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate
	  FROM scores
  ) ranked
 WHERE run_group_id=$1`

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
		return nil, err
	}
	ts, err := pgxpool.New(ctx, timescaleURL)
	if err != nil {
		meta.Close()
		return nil, err
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

// LeaderboardResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardResponse struct {
	Source     string           `json:"source"`
	Rows       []LeaderboardRow `json:"rows"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// LeaderboardQuery groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardQuery struct {
	Limit        int
	Sort         string
	Order        string
	Cursor       string
	RunGroupID   string
	SubmissionID string
	ContestantID string
	TeamID       string
	TeamName     string
	// Scenario filters to run-groups that ran a session with this scenario name
	// (constant / ramp / stress). Empty = every scenario. The scores row is
	// per run-group, so this is an EXISTS join to runs+scenarios, not a column.
	Scenario string
}

// LeaderboardRow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardRow struct {
	Rank                 int64   `json:"rank"`
	RunGroupID           string  `json:"run_group_id"`
	SubmissionID         string  `json:"submission_id"`
	ContestantID         string  `json:"contestant_id"`
	TeamName             string  `json:"team_name"`
	PeakSustainedTPS     uint64  `json:"peak_sustained_tps"`
	P99NSAtPeakTPS       uint64  `json:"p99_ns_at_peak_tps"`
	SpikeRecoveryNS      uint64  `json:"spike_recovery_ns"`
	TotalCorrectness     float64 `json:"total_correctness"`
	Disqualified         bool    `json:"disqualified"`
	DisqualificationCode string  `json:"disqualification_code,omitempty"`
	RankDelta            int64   `json:"rank_delta"`
	ComputedAtUnixNS     int64   `json:"computed_at_ns"`
	// Jitter* is the P-G jitter (cross-flow processing-order inversion
	// magnitude, microseconds) worst-case across the run-group's sessions,
	// mirrored from CorrectnessScoreEvent. Zero means no recorded inversions.
	JitterP50US   float64 `json:"jitter_p50_us"`
	JitterP99US   float64 `json:"jitter_p99_us"`
	JitterP999US  float64 `json:"jitter_p999_us"`
	JitterMaxUS   float64 `json:"jitter_max_us"`
	JitterInvRate float64 `json:"jitter_inversion_rate"`
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Leaderboard(ctx context.Context, q LeaderboardQuery) (LeaderboardResponse, error) {
	limit := q.Limit
	if limit <= 0 || limit > maxLeaderboardRows {
		limit = defaultLeaderboardRows
	}
	offset, err := decodeLeaderboardCursor(q.Cursor)
	if err != nil {
		return LeaderboardResponse{}, err
	}
	contestantFilter := q.ContestantID
	if contestantFilter == "" {
		contestantFilter = q.TeamID
	}
	orderBy := leaderboardOrderBy(q.Sort, q.Order)
	rows, err := s.meta.Query(ctx, leaderboardQuerySQL+`
 ORDER BY `+orderBy+`
 LIMIT $5 OFFSET $6`, q.RunGroupID, q.SubmissionID, contestantFilter, q.TeamName, limit+1, offset, q.Scenario)
	if err != nil {
		if isUndefinedTable(err) {
			return LeaderboardResponse{Source: "frozen", Rows: []LeaderboardRow{}}, nil
		}
		return LeaderboardResponse{}, err
	}
	defer rows.Close()
	resp := LeaderboardResponse{Source: "frozen", Rows: []LeaderboardRow{}}
	hasMore := false
	for rows.Next() {
		var r LeaderboardRow
		var peak, p99, recovery int64
		var computed time.Time
		if err := rows.Scan(&r.Rank, &r.RunGroupID, &r.SubmissionID, &r.ContestantID, &r.TeamName, &peak, &p99,
			&recovery, &r.TotalCorrectness, &r.Disqualified, &r.DisqualificationCode, &r.RankDelta, &computed,
			&r.JitterP50US, &r.JitterP99US, &r.JitterP999US, &r.JitterMaxUS, &r.JitterInvRate); err != nil {
			return resp, err
		}
		r.PeakSustainedTPS = nonNegativeUint64(peak)
		r.P99NSAtPeakTPS = nonNegativeUint64(p99)
		r.SpikeRecoveryNS = nonNegativeUint64(recovery)
		r.ComputedAtUnixNS = computed.UnixNano()
		if len(resp.Rows) < limit {
			resp.Rows = append(resp.Rows, r)
		} else {
			hasMore = true
		}
	}
	if err := rows.Err(); err != nil {
		return resp, err
	}
	if hasMore {
		resp.NextCursor = encodeLeaderboardCursor(offset + limit)
	}
	return resp, nil
}

// isUndefinedTable performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// leaderboardCursor groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type leaderboardCursor struct {
	Offset int `json:"offset"`
}

// RunDetail groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunDetail struct {
	RunGroupID string          `json:"run_group_id"`
	Score      *LeaderboardRow `json:"score,omitempty"`
	Sessions   []SessionDetail `json:"sessions"`
	// ViolationCounts is the aggregate count per (session, violation_type) for
	// this run-group — computed with GROUP BY, not a truncated row sample. The
	// orderbook can emit tens of millions of violation rows, so returning them
	// raw is neither useful nor affordable.
	ViolationCounts []ViolationCount `json:"violation_counts"`
}

// SessionDetail groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SessionDetail struct {
	SessionID string `json:"session_id"`
	Scenario  string `json:"scenario"`
	Status    string `json:"status"`
	// Per-scenario correctness from score_progress (nil until the validator has
	// scored this session). CorrectnessScore = ValidFills/TotalFills.
	CorrectnessScore *float64      `json:"correctness_score,omitempty"`
	ValidFills       *int64        `json:"valid_fills,omitempty"`
	TotalFills       *int64        `json:"total_fills,omitempty"`
	Timeline         []MetricPoint `json:"timeline"`
}

// MetricPoint groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type MetricPoint struct {
	TimeUnixNS     int64   `json:"time_unix_ns"`
	WaveIndex      int     `json:"wave_index"`
	P50NS          uint64  `json:"p50_ns"`
	P90NS          uint64  `json:"p90_ns"`
	P99NS          uint64  `json:"p99_ns"`
	RTP50NS        uint64  `json:"rt_p50_ns"`
	RTP90NS        uint64  `json:"rt_p90_ns"`
	RTP99NS        uint64  `json:"rt_p99_ns"`
	TPS1S          float64 `json:"tps_1s"`
	ErrorRate      float64 `json:"error_rate"`
	HDREncoded     string  `json:"hdr_encoded,omitempty"`
	RTHDREncoded   string  `json:"rt_hdr_encoded,omitempty"`
	SlipHDREncoded string  `json:"slip_hdr_encoded,omitempty"`
	// MatchHDREncoded: taker-fill matching-latency HDR (FIX 851=2). Own chart.
	MatchHDREncoded string `json:"match_hdr_encoded,omitempty"`
}

// ViolationCount is the aggregate number of correctness violations of one type
// within one session. Keep this type aligned with the runtime contract around it.
type ViolationCount struct {
	SessionID     string `json:"session_id"`
	ViolationType string `json:"violation_type"`
	Count         int64  `json:"count"`
}

// RunDetail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) RunDetail(ctx context.Context, runGroupID string) (RunDetail, error) {
	d := RunDetail{RunGroupID: runGroupID}
	scoreRow, ok, err := s.scoreForRunGroup(ctx, runGroupID)
	if err != nil {
		return d, err
	}
	if ok {
		d.Score = &scoreRow
	}
	rows, err := s.meta.Query(ctx, `
SELECT r.session_id, sc.name, r.status, p.correctness_score, p.valid_fills, p.total_fills
  FROM runs r JOIN scenarios sc ON sc.scenario_id=r.scenario_id
  LEFT JOIN score_progress p ON p.session_id=r.session_id
 WHERE r.run_group_id=$1
 ORDER BY sc.sort_order, sc.name`, runGroupID)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var sd SessionDetail
		if err := rows.Scan(&sd.SessionID, &sd.Scenario, &sd.Status, &sd.CorrectnessScore, &sd.ValidFills, &sd.TotalFills); err != nil {
			return d, err
		}
		sd.Timeline, err = s.Chart(ctx, sd.SessionID)
		if err != nil {
			return d, err
		}
		if sd.Timeline == nil {
			sd.Timeline = []MetricPoint{}
		}
		// The run-detail view fetches all sessions and (when live) polls. The HDR percentile
		// chart only needs the latest HDR snapshot per wave, so drop the (large, base64) blobs
		// from every other point — keeping the numeric timeline intact. This shrinks the
		// payload by ~100x (one HDR blob per wave instead of one per metric point).
		keepLastPerWaveHDR(sd.Timeline)
		d.Sessions = append(d.Sessions, sd)
	}
	if err := rows.Err(); err != nil {
		return d, err
	}
	if d.Sessions == nil {
		d.Sessions = []SessionDetail{}
	}
	d.ViolationCounts, err = s.ViolationCounts(ctx, runGroupID)
	if d.ViolationCounts == nil {
		d.ViolationCounts = []ViolationCount{}
	}
	return d, err
}

// keepLastPerWaveHDR clears the HDR base64 blobs (hdr/rt/slip/match) on every metric point except
// the latest one per wave_index. The timeline is ordered by time, so the last occurrence of a
// wave is its newest. Numeric fields are left untouched. Used to slim the run-detail payload;
// the per-session /api/charts endpoint keeps full HDR.
func keepLastPerWaveHDR(tl []MetricPoint) {
	lastIdx := make(map[int]int, len(tl))
	for i := range tl {
		lastIdx[tl[i].WaveIndex] = i
	}
	for i := range tl {
		if lastIdx[tl[i].WaveIndex] != i {
			tl[i].HDREncoded = ""
			tl[i].RTHDREncoded = ""
			tl[i].SlipHDREncoded = ""
			tl[i].MatchHDREncoded = ""
		}
	}
}

// Chart applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Chart(ctx context.Context, sessionID string) ([]MetricPoint, error) {
	rows, err := s.timescale.Query(ctx, `
SELECT EXTRACT(EPOCH FROM time) * 1000000000, wave_index,
       COALESCE(p50_ns,0), COALESCE(p90_ns,0), COALESCE(p99_ns,0),
       COALESCE(rt_p50_ns,0), COALESCE(rt_p90_ns,0), COALESCE(rt_p99_ns,0),
       COALESCE(tps_1s,0), COALESCE(error_rate,0), hdr_encoded, rt_hdr_encoded, slip_hdr_encoded, match_hdr_encoded
  FROM metrics
 WHERE session_id=$1
 ORDER BY time
 LIMIT $2`, sessionID, maxChartPoints)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetricPoint
	for rows.Next() {
		var p MetricPoint
		var p50, p90, p99, rt50, rt90, rt99 int64
		var nsFloat float64
		var hdr, rtHdr, slipHdr, matchHdr []byte
		if err := rows.Scan(&nsFloat, &p.WaveIndex, &p50, &p90, &p99, &rt50, &rt90, &rt99, &p.TPS1S, &p.ErrorRate, &hdr, &rtHdr, &slipHdr, &matchHdr); err != nil {
			return nil, err
		}
		p.TimeUnixNS = int64(nsFloat)
		p.P50NS, p.P90NS, p.P99NS = nonNegativeUint64(p50), nonNegativeUint64(p90), nonNegativeUint64(p99)
		p.RTP50NS, p.RTP90NS, p.RTP99NS = nonNegativeUint64(rt50), nonNegativeUint64(rt90), nonNegativeUint64(rt99)
		if len(hdr) > 0 {
			p.HDREncoded = base64.StdEncoding.EncodeToString(hdr)
		}
		if len(rtHdr) > 0 {
			p.RTHDREncoded = base64.StdEncoding.EncodeToString(rtHdr)
		}
		if len(slipHdr) > 0 {
			p.SlipHDREncoded = base64.StdEncoding.EncodeToString(slipHdr)
		}
		if len(matchHdr) > 0 {
			p.MatchHDREncoded = base64.StdEncoding.EncodeToString(matchHdr)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ViolationCounts returns the per-category violation totals for a run-group,
// read straight from the pre-aggregated correctness_summary row (one row per
// session). There is no per-violation table: storing one row per violation
// reached tens of millions of rows at high TPS for data nothing rendered, so the
// validator writes these category counters at score time instead.
func (s *Store) ViolationCounts(ctx context.Context, runGroupID string) ([]ViolationCount, error) {
	rows, err := s.meta.Query(ctx, `
SELECT r.session_id, cs.price_violations, cs.self_trades, cs.phantom_fills,
       cs.time_violations, cs.cancel_replace_loss, cs.overfills,
       cs.missed_fills, cs.lost_orders, cs.lost_cancels
  FROM runs r
  JOIN correctness_summary cs ON cs.session_id = r.session_id
 WHERE r.run_group_id = $1`, runGroupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ViolationCount
	for rows.Next() {
		var sessionID string
		var price, selfTrade, phantom, timeV, crl, overfill int64
		var missed, lostOrders, lostCancels int64
		if err := rows.Scan(&sessionID, &price, &selfTrade, &phantom, &timeV, &crl, &overfill,
			&missed, &lostOrders, &lostCancels); err != nil {
			return nil, err
		}
		// These must cover EVERY class counted into violation_count, or the
		// breakdown silently fails to add up to the headline the UI shows beside
		// it. missed_fills was the omission that made this obvious: one session
		// reported 7,860 violations of which 7,663 were missed fills, so the
		// categories shown totalled 196.
		for _, c := range []ViolationCount{
			{SessionID: sessionID, ViolationType: "price", Count: price},
			{SessionID: sessionID, ViolationType: "self_trade", Count: selfTrade},
			{SessionID: sessionID, ViolationType: "phantom", Count: phantom},
			{SessionID: sessionID, ViolationType: "time", Count: timeV},
			{SessionID: sessionID, ViolationType: "cancel_replace_loss", Count: crl},
			{SessionID: sessionID, ViolationType: "overfill", Count: overfill},
			{SessionID: sessionID, ViolationType: "missed_fill", Count: missed},
			{SessionID: sessionID, ViolationType: "lost_order", Count: lostOrders},
			{SessionID: sessionID, ViolationType: "lost_cancel", Count: lostCancels},
		} {
			if c.Count > 0 {
				out = append(out, c)
			}
		}
	}
	return out, rows.Err()
}

// ActiveSession groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ActiveSession struct {
	SessionID string `json:"session_id"`
	Scenario  string `json:"scenario"`
	Status    string `json:"status"`
}

// ActiveRun groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ActiveRun struct {
	RunGroupID string          `json:"run_group_id"`
	TeamName   string          `json:"team_name"`
	Sessions   []ActiveSession `json:"sessions"`
}

// ActiveRuns applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) ActiveRuns(ctx context.Context) ([]ActiveRun, error) {
	rows, err := s.meta.Query(ctx, `
SELECT rg.run_group_id, COALESCE(sub.team_name,''), r.session_id, sc.name, r.status
  FROM run_groups rg
  JOIN runs r ON r.run_group_id = rg.run_group_id
  JOIN scenarios sc ON sc.scenario_id = r.scenario_id
  LEFT JOIN submissions sub ON sub.submission_id = rg.submission_id
 WHERE rg.run_group_id IN (
   SELECT run_group_id FROM runs WHERE status NOT IN ('completed','failed')
 )
 ORDER BY rg.created_at DESC, sc.sort_order, sc.name
 LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	idx := map[string]int{}
	var out []ActiveRun
	for rows.Next() {
		var rgID, team string
		var sess ActiveSession
		if err := rows.Scan(&rgID, &team, &sess.SessionID, &sess.Scenario, &sess.Status); err != nil {
			return nil, err
		}
		i, ok := idx[rgID]
		if !ok {
			i = len(out)
			idx[rgID] = i
			out = append(out, ActiveRun{RunGroupID: rgID, TeamName: team})
		}
		out[i].Sessions = append(out[i].Sessions, sess)
	}
	return out, rows.Err()
}

// ActiveSessionContestant groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ActiveSessionContestant struct {
	SessionID    string
	ContestantID string
}

// ActiveSessionContestants returns the contestant_id for each session belonging to a run
// that is not yet completed or failed.
func (s *Store) ActiveSessionContestants(ctx context.Context) ([]ActiveSessionContestant, error) {
	rows, err := s.meta.Query(ctx, `
SELECT r.session_id, COALESCE(sub.contestant_id,'')
  FROM run_groups rg
  JOIN runs r ON r.run_group_id = rg.run_group_id
  LEFT JOIN submissions sub ON sub.submission_id = rg.submission_id
 WHERE r.status NOT IN ('completed','failed')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActiveSessionContestant
	for rows.Next() {
		var sc ActiveSessionContestant
		if err := rows.Scan(&sc.SessionID, &sc.ContestantID); err != nil {
			return nil, err
		}
		if sc.ContestantID != "" {
			out = append(out, sc)
		}
	}
	return out, rows.Err()
}

// String applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) String() string { return fmt.Sprintf("read.Store(%p)", s) }

// scoreForRunGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) scoreForRunGroup(ctx context.Context, runGroupID string) (LeaderboardRow, bool, error) {
	rows, err := s.meta.Query(ctx, scoreForRunGroupQuerySQL, runGroupID)
	if err != nil {
		return LeaderboardRow{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return LeaderboardRow{}, false, rows.Err()
	}
	var r LeaderboardRow
	var peak, p99, recovery int64
	var computed time.Time
	if err := rows.Scan(&r.Rank, &r.RunGroupID, &r.SubmissionID, &r.ContestantID, &r.TeamName, &peak, &p99,
		&recovery, &r.TotalCorrectness, &r.Disqualified, &r.DisqualificationCode, &r.RankDelta, &computed,
		&r.JitterP50US, &r.JitterP99US, &r.JitterP999US, &r.JitterMaxUS, &r.JitterInvRate); err != nil {
		return LeaderboardRow{}, false, err
	}
	r.PeakSustainedTPS = nonNegativeUint64(peak)
	r.P99NSAtPeakTPS = nonNegativeUint64(p99)
	r.SpikeRecoveryNS = nonNegativeUint64(recovery)
	r.ComputedAtUnixNS = computed.UnixNano()
	return r, true, rows.Err()
}

// nonNegativeUint64 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func nonNegativeUint64(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

// leaderboardOrderBy performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func leaderboardOrderBy(sortField, order string) string {
	sortField = strings.ToLower(strings.TrimSpace(sortField))
	sortField = strings.ReplaceAll(sortField, "-", "_")
	order = strings.ToLower(strings.TrimSpace(order))
	type sortSpec struct {
		column      string
		defaultDesc bool
		tiebreak    string
	}
	specs := map[string]sortSpec{
		"":                   {column: "rank", defaultDesc: false, tiebreak: "run_group_id ASC"},
		"rank":               {column: "rank", defaultDesc: false, tiebreak: "run_group_id ASC"},
		"peak_tps":           {column: "peak_sustained_tps", defaultDesc: true, tiebreak: "p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"peak_sustained_tps": {column: "peak_sustained_tps", defaultDesc: true, tiebreak: "p99_at_peak_ns ASC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"p99":                {column: "p99_at_peak_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"p99_at_peak":        {column: "p99_at_peak_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"p99_ns_at_peak_tps": {column: "p99_at_peak_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, spike_recovery_ns ASC, total_correctness DESC, run_group_id ASC"},
		"spike_recovery":     {column: "spike_recovery_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, total_correctness DESC, run_group_id ASC"},
		"spike_recovery_ns":  {column: "spike_recovery_ns", defaultDesc: false, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, total_correctness DESC, run_group_id ASC"},
		"total_score":        {column: "total_correctness", defaultDesc: true, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, run_group_id ASC"},
		"total_correctness":  {column: "total_correctness", defaultDesc: true, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, run_group_id ASC"},
		"correctness":        {column: "total_correctness", defaultDesc: true, tiebreak: "peak_sustained_tps DESC, p99_at_peak_ns ASC, spike_recovery_ns ASC, run_group_id ASC"},
		"team_name":          {column: "team_name", defaultDesc: false, tiebreak: "rank ASC, run_group_id ASC"},
		"computed_at":        {column: "computed_at", defaultDesc: true, tiebreak: "rank ASC, run_group_id ASC"},
	}
	spec, ok := specs[sortField]
	if !ok {
		spec = specs[""]
	}
	dir := "ASC"
	if spec.defaultDesc {
		dir = "DESC"
	}
	switch order {
	case "asc":
		dir = "ASC"
	case "desc":
		dir = "DESC"
	}
	return "disqualified ASC, " + spec.column + " " + dir + ", " + spec.tiebreak
}

// encodeLeaderboardCursor performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func encodeLeaderboardCursor(offset int) string {
	payload, _ := json.Marshal(leaderboardCursor{Offset: offset})
	return base64.RawURLEncoding.EncodeToString(payload)
}

// decodeLeaderboardCursor performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func decodeLeaderboardCursor(cursor string) (int, error) {
	if strings.TrimSpace(cursor) == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("invalid cursor")
	}
	var decoded leaderboardCursor
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return 0, fmt.Errorf("invalid cursor")
	}
	if decoded.Offset < 0 || decoded.Offset > maxLeaderboardOffset {
		return 0, fmt.Errorf("invalid cursor")
	}
	return decoded.Offset, nil
}

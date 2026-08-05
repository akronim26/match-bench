// Package store implements postgres behavior.
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

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS submissions (
	submission_id  TEXT PRIMARY KEY,
	contestant_id  TEXT NOT NULL DEFAULT '',
	sha256         TEXT NOT NULL UNIQUE,
	language       TEXT NOT NULL,
	protocol       TEXT NOT NULL,
	port           INT  NOT NULL,
	team_name      TEXT NOT NULL DEFAULT '',
	artifact_path  TEXT NOT NULL,
	image_ref      TEXT NOT NULL DEFAULT '',
	status         TEXT NOT NULL DEFAULT 'uploaded',
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE submissions ADD COLUMN IF NOT EXISTS image_ref TEXT NOT NULL DEFAULT '';

-- A "scenario" is one named load pattern (constant / spike / ramp). Its
-- task_specs JSONB is the full task list — every TaskSpec is one tokio task
-- in a bot-worker = one TCP connection = one constant-rate sender. The
-- controller shards this list across worker pods at session start. Values
-- are seeded by submission-api on startup (see SeedScenarios) and remain
-- editable by judges via direct SQL.
CREATE TABLE IF NOT EXISTS scenarios (
	scenario_id    TEXT PRIMARY KEY,
	name           TEXT NOT NULL UNIQUE,                -- constant | spike | ramp
	duration_ns    BIGINT NOT NULL,
	task_specs     JSONB NOT NULL,
	-- sort_order is the position in the run-group's execution sequence.
	-- 1=constant, 2=spike, 3=ramp by default. Used by ListRunsByGroup so the
	-- frontend renders the three sessions in execution order rather than
	-- alphabetical (which would put 'ramp' before 'spike').
	sort_order     INT NOT NULL DEFAULT 0,
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent migration: add sort_order to pre-existing scenarios tables.
ALTER TABLE scenarios ADD COLUMN IF NOT EXISTS sort_order INT NOT NULL DEFAULT 0;

-- A run_group is one "click benchmark" by a contestant. It expands into N
-- child runs rows, one per scenario in the scenarios table. The partial
-- unique index on (submission_id) WHERE NOT terminal enforces "at most one
-- active benchmark per submission" — the same idempotency invariant the
-- old runs(submission_id) partial index used to enforce, now lifted to the
-- parent because a group has multiple concurrent child runs sharing
-- submission_id.
CREATE TABLE IF NOT EXISTS run_groups (
	run_group_id   TEXT PRIMARY KEY,
	submission_id  TEXT NOT NULL,
	contestant_id  TEXT NOT NULL DEFAULT '',
	status         TEXT NOT NULL DEFAULT 'requested',   -- requested | running | completed | failed
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
	session_id     TEXT PRIMARY KEY,
	submission_id  TEXT NOT NULL,
	contestant_id  TEXT NOT NULL DEFAULT '',
	run_group_id   TEXT,                                -- NULL only for legacy single-session runs
	scenario_id    TEXT,                                -- which scenario this session ran
	status         TEXT NOT NULL DEFAULT 'requested',
	message        TEXT NOT NULL DEFAULT '',
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Schema migration: add run_group_id / scenario_id columns to existing runs
-- tables. ADD COLUMN IF NOT EXISTS is idempotent and safe to run on every
-- startup. Existing rows get NULL, which is the documented "legacy" state.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS run_group_id TEXT;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS scenario_id  TEXT;

-- Drop the old per-submission partial unique index on runs. Replaced by the
-- run_groups equivalent below — a group has N concurrent child runs that
-- legitimately share submission_id, so the constraint cannot live on runs
-- anymore.
DROP INDEX IF EXISTS idx_runs_one_active_per_submission;

-- Partial unique index — at most one active run_group per submission.
-- Idempotency on POST /submissions/{id}/benchmark relies on this: the INSERT
-- path either succeeds (no active group existed) or fails with 23505
-- unique_violation, at which point the handler re-queries and returns the
-- existing run_group_id with HTTP 200.
--
-- HARD INVARIANT: terminal run_group statuses ('completed', 'failed') are
-- derived from the child runs' terminal statuses and written only by the
-- bot-fleet-controller. The exception is the controller's startup recovery
-- sweep, which writes 'failed' directly to release this index before its
-- consumers start consuming benchmark.requested.
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_groups_one_active_per_submission
	ON run_groups (submission_id)
	WHERE status NOT IN ('completed', 'failed');

CREATE INDEX IF NOT EXISTS idx_runs_submission_id ON runs (submission_id);
CREATE INDEX IF NOT EXISTS idx_runs_run_group_id ON runs (run_group_id);
CREATE INDEX IF NOT EXISTS idx_run_groups_submission_id ON run_groups (submission_id);

-- Defensive: within one run-group, there is at most one row per scenario.
-- A re-publish of benchmark.requested for the same (group, scenario) must
-- not produce a duplicate runs row. NULLs in run_group_id are common (legacy)
-- so the partial WHERE clause is required for the unique constraint to be
-- meaningful.
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_unique_scenario_per_group
	ON runs (run_group_id, scenario_id)
	WHERE run_group_id IS NOT NULL AND scenario_id IS NOT NULL;
`

// SubmissionMeta groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SubmissionMeta struct {
	SubmissionID string
	ContestantID string
	SHA256       string
	Language     string
	Protocol     string
	Port         int
	TeamName     string
	ArtifactPath string
	ImageRef     string
	Status       string
	CreatedAt    time.Time
}

// PostgresStore groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	start := time.Now()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		recordDB("submission-api", "connect", start, err)
		return nil, fmt.Errorf("pgx pool: %w", err)
	}

	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		recordDB("submission-api", "startup_schema_migration", start, err)
		return nil, fmt.Errorf("create tables: %w", err)
	}
	recordDB("submission-api", "startup_schema_migration", start, nil)

	return &PostgresStore{pool: pool}, nil
}

// RunMeta groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunMeta struct {
	SessionID    string
	SubmissionID string
	ContestantID string
	RunGroupID   string // empty for legacy single-session runs
	ScenarioID   string // empty for legacy single-session runs
	Status       string
	Message      string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// RunGroupMeta groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunGroupMeta struct {
	RunGroupID   string
	SubmissionID string
	ContestantID string
	TeamName     string
	Status       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// RunGroupListFilter groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunGroupListFilter struct {
	// ContestantID, when non-empty, restricts to one contestant. Empty lists
	// every contestant's run-groups (auth is off — the runs page is public).
	ContestantID string
	// Search is a case-insensitive substring matched against contestant_id and
	// the submission's team_name. Empty = no search filter.
	Search        string
	SubmissionIDs []string
	Limit         int
}

// ScenarioRow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ScenarioRow struct {
	ScenarioID string
	Name       string // constant | spike | ramp
	SortOrder  int    // execution order in the run-group (1=constant, 2=spike, 3=ramp)
	DurationNs uint64
	TaskSpecs  []topics.TaskSpec
	CreatedAt  time.Time
}

// FindActiveRunGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) FindActiveRunGroup(ctx context.Context, submissionID string) (*RunGroupMeta, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT run_group_id, submission_id, contestant_id, status, created_at, updated_at
		   FROM run_groups
		  WHERE submission_id = $1
		    AND status NOT IN ('completed', 'failed')
		  LIMIT 1`,
		submissionID,
	)
	var g RunGroupMeta
	err := row.Scan(&g.RunGroupID, &g.SubmissionID, &g.ContestantID, &g.Status, &g.CreatedAt, &g.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("submission-api", "find_active_run_group", start, nil)
			return nil, nil
		}
		recordDB("submission-api", "find_active_run_group", start, err)
		return nil, fmt.Errorf("%w: find active run-group: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "find_active_run_group", start, nil)
	return &g, nil
}

// GetRunGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) GetRunGroup(ctx context.Context, runGroupID string) (*RunGroupMeta, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT run_group_id, submission_id, contestant_id, status, created_at, updated_at
		   FROM run_groups WHERE run_group_id = $1`,
		runGroupID,
	)
	var g RunGroupMeta
	err := row.Scan(&g.RunGroupID, &g.SubmissionID, &g.ContestantID, &g.Status, &g.CreatedAt, &g.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("submission-api", "get_run_group", start, nil)
			return nil, nil
		}
		recordDB("submission-api", "get_run_group", start, err)
		return nil, fmt.Errorf("%w: get run-group: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "get_run_group", start, nil)
	return &g, nil
}

// ListRunGroups applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) ListRunGroups(ctx context.Context, filter RunGroupListFilter) ([]RunGroupMeta, error) {
	start := time.Now()
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	submissionIDs := filter.SubmissionIDs
	if submissionIDs == nil {
		submissionIDs = []string{}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT rg.run_group_id, rg.submission_id, rg.contestant_id,
		       COALESCE(s.team_name, ''), rg.status, rg.created_at, rg.updated_at
		  FROM run_groups rg
		  LEFT JOIN submissions s ON s.submission_id = rg.submission_id
		 WHERE ($1 = '' OR rg.contestant_id = $1)
		   AND (cardinality(COALESCE($2::text[], ARRAY[]::text[])) = 0
		        OR rg.submission_id = ANY($2::text[]))
		   AND ($4 = '' OR rg.contestant_id ILIKE '%' || $4 || '%'
		        OR COALESCE(s.team_name, '') ILIKE '%' || $4 || '%')
		 ORDER BY rg.created_at DESC
		 LIMIT $3`,
		filter.ContestantID,
		submissionIDs,
		limit,
		filter.Search,
	)
	if err != nil {
		recordDB("submission-api", "list_run_groups", start, err)
		return nil, fmt.Errorf("%w: list run-groups: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	defer rows.Close()
	var out []RunGroupMeta
	for rows.Next() {
		var g RunGroupMeta
		if err := rows.Scan(&g.RunGroupID, &g.SubmissionID, &g.ContestantID, &g.TeamName, &g.Status, &g.CreatedAt, &g.UpdatedAt); err != nil {
			recordDB("submission-api", "list_run_groups", start, err)
			return nil, fmt.Errorf("%w: scan run-group: %v", cerrs.ErrStoreDatabaseFailed, err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		recordDB("submission-api", "list_run_groups", start, err)
		return nil, fmt.Errorf("%w: list run-groups rows: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "list_run_groups", start, nil)
	return out, nil
}

// GetRun applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) GetRun(ctx context.Context, sessionID string) (*RunMeta, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT session_id, submission_id, contestant_id,
		        COALESCE(run_group_id, ''), COALESCE(scenario_id, ''),
		        status, message, created_at, updated_at
		   FROM runs WHERE session_id = $1`,
		sessionID,
	)
	var r RunMeta
	err := row.Scan(&r.SessionID, &r.SubmissionID, &r.ContestantID,
		&r.RunGroupID, &r.ScenarioID, &r.Status, &r.Message, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("submission-api", "get_run", start, nil)
			return nil, nil
		}
		recordDB("submission-api", "get_run", start, err)
		return nil, fmt.Errorf("%w: get run: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "get_run", start, nil)
	return &r, nil
}

// ListRunsByGroup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) ListRunsByGroup(ctx context.Context, runGroupID string) ([]RunMeta, error) {
	start := time.Now()
	rows, err := s.pool.Query(ctx,
		`SELECT r.session_id, r.submission_id, r.contestant_id,
		        COALESCE(r.run_group_id, ''), COALESCE(r.scenario_id, ''),
		        r.status, r.message, r.created_at, r.updated_at
		   FROM runs r
		   LEFT JOIN scenarios s ON s.scenario_id = r.scenario_id
		  WHERE r.run_group_id = $1
		  ORDER BY s.sort_order, s.name`,
		runGroupID,
	)
	if err != nil {
		recordDB("submission-api", "list_runs_by_group", start, err)
		return nil, fmt.Errorf("%w: list runs by group: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	defer rows.Close()

	out := make([]RunMeta, 0, 3)
	for rows.Next() {
		var r RunMeta
		if err := rows.Scan(&r.SessionID, &r.SubmissionID, &r.ContestantID,
			&r.RunGroupID, &r.ScenarioID, &r.Status, &r.Message, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("%w: scan run: %v", cerrs.ErrStoreDatabaseFailed, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		recordDB("submission-api", "list_runs_by_group", start, err)
		return out, err
	}
	recordDB("submission-api", "list_runs_by_group", start, nil)
	return out, nil
}

// InsertRunGroupWithChildren applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) InsertRunGroupWithChildren(ctx context.Context, g RunGroupMeta, children []RunMeta) error {
	start := time.Now()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		recordDB("submission-api", "insert_run_group_with_children", start, err)
		return fmt.Errorf("%w: begin tx: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO run_groups
			(run_group_id, submission_id, contestant_id, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		g.RunGroupID, g.SubmissionID, g.ContestantID, g.Status, g.CreatedAt, g.UpdatedAt,
	); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			recordDB("submission-api", "insert_run_group_with_children", start, err)
			return cerrs.ErrActiveRunGroupExists
		}
		recordDB("submission-api", "insert_run_group_with_children", start, err)
		return fmt.Errorf("%w: insert run-group: %v", cerrs.ErrStoreDatabaseFailed, err)
	}

	for _, r := range children {
		if _, err := tx.Exec(ctx,
			`INSERT INTO runs
				(session_id, submission_id, contestant_id, run_group_id, scenario_id,
				 status, message, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			r.SessionID, r.SubmissionID, r.ContestantID, r.RunGroupID, r.ScenarioID,
			r.Status, r.Message, r.CreatedAt, r.UpdatedAt,
		); err != nil {
			recordDB("submission-api", "insert_run_group_with_children", start, err)
			return fmt.Errorf("%w: insert child run: %v", cerrs.ErrStoreDatabaseFailed, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		recordDB("submission-api", "insert_run_group_with_children", start, err)
		return fmt.Errorf("%w: commit run-group tx: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "insert_run_group_with_children", start, nil)
	return nil
}

// RecomputeRunGroupStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) RecomputeRunGroupStatus(ctx context.Context, runGroupID string) error {
	start := time.Now()
	row := s.pool.QueryRow(ctx, `
		SELECT
			SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END) AS failed_count,
			SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
			SUM(CASE WHEN status = 'requested' THEN 1 ELSE 0 END) AS requested_count,
			COUNT(*)                                              AS total_count
		FROM runs
		WHERE run_group_id = $1
	`, runGroupID)
	var failed, completed, requested, total int
	if err := row.Scan(&failed, &completed, &requested, &total); err != nil {
		recordDB("submission-api", "recompute_run_group_status", start, err)
		return fmt.Errorf("%w: recompute run-group status: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	if total == 0 {
		recordDB("submission-api", "recompute_run_group_status", start, nil)
		return nil
	}
	var newStatus string
	switch {
	case completed == total:
		newStatus = "completed"
	case failed > 0 && failed+completed == total:
		newStatus = "failed"
	case requested == total:
		newStatus = "requested"
	default:
		newStatus = "running"
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE run_groups SET status = $2, updated_at = now() WHERE run_group_id = $1`,
		runGroupID, newStatus,
	); err != nil {
		recordDB("submission-api", "recompute_run_group_status", start, err)
		return fmt.Errorf("%w: update run-group status: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "recompute_run_group_status", start, nil)
	return nil
}

// UpdateRunStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
// ClaimNextRunInGroup atomically claims the next undispatched run of a group
// for sequential-within-group execution (decision 2026-08-02: a 4-scenario
// run group must hold at most ONE order band at a time, so scenarios dispatch
// one after another). The earliest `requested` run flips to `queued` and is
// returned; nil means the group has nothing left to dispatch. Ordering is by
// session_id: children are UUIDv7s minted in ListScenarios order (sort_order),
// so id order IS scenario order. The `status='requested'` guard plus
// FOR UPDATE SKIP LOCKED make concurrent claims (status-topic redelivery,
// future consumer replicas) safe: each run is dispatched at most once.
func (s *PostgresStore) ClaimNextRunInGroup(ctx context.Context, runGroupID string) (*RunMeta, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`UPDATE runs SET status = 'queued', updated_at = now()
		  WHERE session_id = (
		        SELECT session_id FROM runs
		         WHERE run_group_id = $1 AND status = 'requested'
		         ORDER BY session_id
		         LIMIT 1
		         FOR UPDATE SKIP LOCKED)
		    AND status = 'requested'
		 RETURNING session_id, submission_id, contestant_id, run_group_id, scenario_id`,
		runGroupID,
	)
	var m RunMeta
	err := row.Scan(&m.SessionID, &m.SubmissionID, &m.ContestantID, &m.RunGroupID, &m.ScenarioID)
	if errors.Is(err, pgx.ErrNoRows) {
		recordDB("submission-api", "claim_next_run", start, nil)
		return nil, nil
	}
	if err != nil {
		recordDB("submission-api", "claim_next_run", start, err)
		return nil, fmt.Errorf("%w: claim next run: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "claim_next_run", start, nil)
	return &m, nil
}

func (s *PostgresStore) UpdateRunStatus(ctx context.Context, sessionID, status, message string) error {
	start := time.Now()
	tag, err := s.pool.Exec(ctx,
		`UPDATE runs
		    SET status = $2, message = $3, updated_at = now()
		  WHERE session_id = $1
		    AND status NOT IN ('completed', 'failed')`,
		sessionID, status, message,
	)
	if err != nil {
		recordDB("submission-api", "update_run_status", start, err)
		return fmt.Errorf("%w: update run status: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	if tag.RowsAffected() == 0 {
		var currentStatus string
		err := s.pool.QueryRow(ctx,
			`SELECT status FROM runs WHERE session_id = $1`, sessionID,
		).Scan(&currentStatus)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				recordDB("submission-api", "update_run_status", start, err)
				return cerrs.ErrRunNotFound
			}
			recordDB("submission-api", "update_run_status", start, err)
			return fmt.Errorf("%w: check run status: %v", cerrs.ErrStoreDatabaseFailed, err)
		}
		recordDB("submission-api", "update_run_status", start, nil)
		return nil
	}
	recordDB("submission-api", "update_run_status", start, nil)
	return nil
}

// ListScenarios applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) ListScenarios(ctx context.Context) ([]ScenarioRow, error) {
	start := time.Now()
	rows, err := s.pool.Query(ctx,
		`SELECT scenario_id, name, sort_order, duration_ns, task_specs, created_at
		   FROM scenarios
		  ORDER BY sort_order, name`,
	)
	if err != nil {
		recordDB("submission-api", "list_scenarios", start, err)
		return nil, fmt.Errorf("%w: list scenarios: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	defer rows.Close()

	out := make([]ScenarioRow, 0, 3)
	for rows.Next() {
		var sc ScenarioRow
		var taskSpecsJSON []byte
		if err := rows.Scan(&sc.ScenarioID, &sc.Name, &sc.SortOrder, &sc.DurationNs, &taskSpecsJSON, &sc.CreatedAt); err != nil {
			return nil, fmt.Errorf("%w: scan scenario: %v", cerrs.ErrStoreDatabaseFailed, err)
		}
		if err := json.Unmarshal(taskSpecsJSON, &sc.TaskSpecs); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_specs for %q: %v", cerrs.ErrStoreDatabaseFailed, sc.Name, err)
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		recordDB("submission-api", "list_scenarios", start, err)
		return out, err
	}
	recordDB("submission-api", "list_scenarios", start, nil)
	return out, nil
}

// SeedScenarios applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) SeedScenarios(ctx context.Context, scenarios []ScenarioRow, reseed bool) error {
	start := time.Now()
	for _, sc := range scenarios {
		taskSpecsJSON, err := json.Marshal(sc.TaskSpecs)
		if err != nil {
			recordDB("submission-api", "seed_scenarios", start, err)
			return fmt.Errorf("%w: marshal task_specs for %q: %v", cerrs.ErrStoreDatabaseFailed, sc.Name, err)
		}
		conflict := `ON CONFLICT (name) DO NOTHING`
		if reseed {
			conflict = `ON CONFLICT (name) DO UPDATE SET
			               duration_ns = EXCLUDED.duration_ns,
			               task_specs  = EXCLUDED.task_specs,
			               sort_order  = EXCLUDED.sort_order`
		}
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO scenarios (scenario_id, name, sort_order, duration_ns, task_specs)
			 VALUES ($1, $2, $3, $4, $5) `+conflict,
			sc.ScenarioID, sc.Name, sc.SortOrder, sc.DurationNs, taskSpecsJSON,
		); err != nil {
			recordDB("submission-api", "seed_scenarios", start, err)
			return fmt.Errorf("%w: seed scenario %q: %v", cerrs.ErrStoreDatabaseFailed, sc.Name, err)
		}
		if _, err := s.pool.Exec(ctx,
			`UPDATE scenarios SET sort_order = $2
			  WHERE name = $1 AND sort_order = 0`,
			sc.Name, sc.SortOrder,
		); err != nil {
			recordDB("submission-api", "seed_scenarios", start, err)
			return fmt.Errorf("%w: backfill sort_order for %q: %v", cerrs.ErrStoreDatabaseFailed, sc.Name, err)
		}
	}
	recordDB("submission-api", "seed_scenarios", start, nil)
	return nil
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) Close() {
	s.pool.Close()
}

// Ping applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// RecordPoolStats applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) RecordPoolStats() {
	stats := s.pool.Stat()
	labels := metrics.Labels("service", "submission-api")
	metrics.Gauge("pgxpool_acquired_conns", "Acquired pgxpool connections.", labels, float64(stats.AcquiredConns()))
	metrics.Gauge("pgxpool_idle_conns", "Idle pgxpool connections.", labels, float64(stats.IdleConns()))
	metrics.Gauge("pgxpool_total_conns", "Total pgxpool connections.", labels, float64(stats.TotalConns()))
	metrics.Gauge("pgxpool_max_conns", "Maximum pgxpool connections.", labels, float64(stats.MaxConns()))
	metrics.Gauge("pgxpool_empty_acquire_total", "pgxpool empty acquire count.", labels, float64(stats.EmptyAcquireCount()))
	metrics.Gauge("pgxpool_canceled_acquire_total", "pgxpool canceled acquire count.", labels, float64(stats.CanceledAcquireCount()))
}

// FindBySHA256 applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) FindBySHA256(ctx context.Context, sha256hex string) (submissionID string, found bool, err error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id FROM submissions WHERE sha256 = $1 LIMIT 1`,
		sha256hex,
	)
	err = row.Scan(&submissionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("submission-api", "find_submission_by_sha256", start, nil)
			return "", false, nil
		}
		recordDB("submission-api", "find_submission_by_sha256", start, err)
		return "", false, fmt.Errorf("sha256 lookup: %w", err)
	}
	recordDB("submission-api", "find_submission_by_sha256", start, nil)
	return submissionID, true, nil
}

// GetByID applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) GetByID(ctx context.Context, submissionID string) (*SubmissionMeta, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id, contestant_id, sha256, language, protocol, port,
		        team_name, artifact_path, image_ref, status, created_at
		 FROM submissions WHERE submission_id = $1`,
		submissionID,
	)

	var m SubmissionMeta
	err := row.Scan(
		&m.SubmissionID, &m.ContestantID, &m.SHA256, &m.Language,
		&m.Protocol, &m.Port, &m.TeamName, &m.ArtifactPath,
		&m.ImageRef, &m.Status, &m.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("submission-api", "get_submission_by_id", start, nil)
			return nil, nil
		}
		recordDB("submission-api", "get_submission_by_id", start, err)
		return nil, fmt.Errorf("%w: get submission: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "get_submission_by_id", start, nil)
	return &m, nil
}

// Insert applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) Insert(ctx context.Context, m SubmissionMeta) error {
	start := time.Now()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO submissions
			(submission_id, contestant_id, sha256, language, protocol, port,
			 team_name, artifact_path, image_ref, status, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		m.SubmissionID, m.ContestantID, m.SHA256, m.Language,
		m.Protocol, m.Port, m.TeamName, m.ArtifactPath,
		m.ImageRef, m.Status, m.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			recordDB("submission-api", "insert_submission", start, err)
			return cerrs.ErrDuplicateSubmission
		}
		recordDB("submission-api", "insert_submission", start, err)
		return fmt.Errorf("%w: insert submission: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "insert_submission", start, nil)
	return nil
}

// ClaimSubmissionContestantIfEmpty applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *PostgresStore) ClaimSubmissionContestantIfEmpty(ctx context.Context, submissionID, contestantID string) (claimed bool, err error) {
	if contestantID == "" {
		return false, nil
	}
	start := time.Now()
	tag, err := s.pool.Exec(ctx,
		`UPDATE submissions
		    SET contestant_id = $2
		  WHERE submission_id = $1
		    AND contestant_id = ''`,
		submissionID,
		contestantID,
	)
	if err != nil {
		recordDB("submission-api", "claim_submission_contestant", start, err)
		return false, fmt.Errorf("%w: claim submission contestant: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	recordDB("submission-api", "claim_submission_contestant", start, nil)
	return tag.RowsAffected() == 1, nil
}

// recordDB performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordDB(service, operation string, start time.Time, err error) {
	labels := metrics.Labels("service", service, "operation", operation)
	metrics.Histogram("db_query_duration_seconds", "PostgreSQL query duration in seconds.", labels, metrics.SinceSeconds(start))
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("db_query_total", "PostgreSQL queries by operation and result.", metrics.Labels("service", service, "operation", operation, "result", result), 1)
}

// Package handler implements benchmark behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
)

// runGroupChild groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type runGroupChild struct {
	SessionID    string    `json:"session_id"`
	ScenarioID   string    `json:"scenario_id"`
	ScenarioName string    `json:"scenario_name"`
	Status       string    `json:"status"`
	Message      string    `json:"message,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// benchmarkResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type benchmarkResponse struct {
	RunGroupID   string          `json:"run_group_id"`
	SubmissionID string          `json:"submission_id"`
	ContestantID string          `json:"contestant_id,omitempty"`
	TeamName     string          `json:"team_name,omitempty"`
	Status       string          `json:"status"` // run-group's status (requested | running | completed | failed)
	CreatedAt    time.Time       `json:"created_at"`
	Runs         []runGroupChild `json:"runs"`
}

// runResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type runResponse struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"`
	RunGroupID   string    `json:"run_group_id,omitempty"`
	ScenarioID   string    `json:"scenario_id,omitempty"`
	Status       string    `json:"status"`
	Message      string    `json:"message"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// StartBenchmark performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func StartBenchmark(pg *store.PostgresStore, pub publisher.Publisher, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		submissionID := chi.URLParam(r, "submission_id")
		if submissionID == "" {
			writeError(w, http.StatusBadRequest, "missing submission_id")
			return
		}

		ctx := r.Context()

		sub, err := pg.GetByID(ctx, submissionID)
		if err != nil {
			log.ErrorContext(ctx, "get submission", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if sub == nil {
			writeError(w, http.StatusNotFound, "submission not found")
			return
		}
		contestantID := contestantIDFromContext(r.Context())
		if contestantID == "" {
			writeError(w, http.StatusUnauthorized, "benchmark requires an authenticated contestant")
			return
		}
		sub, err = claimOrResolveOwner(ctx, pg, sub, contestantID)
		if err != nil {
			if errors.Is(err, cerrs.ErrSubmissionNotFound) {
				writeError(w, http.StatusNotFound, "submission not found")
				return
			}
			log.ErrorContext(ctx, "claim submission contestant", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to bind submission")
			return
		}
		if sub.ContestantID != contestantID {
			writeError(w, http.StatusNotFound, "submission not found")
			return
		}
		if sub.Status != topics.StatusReady {
			writeError(w, http.StatusBadRequest, "submission is not in 'ready' status (current: "+sub.Status+")")
			return
		}
		if sub.ImageRef == "" {
			writeError(w, http.StatusBadRequest, "submission is ready but has no built image ref")
			return
		}

		scenarios, err := pg.ListScenarios(ctx)
		if err != nil {
			log.ErrorContext(ctx, "list scenarios", "error", err)
			writeError(w, http.StatusInternalServerError, "scenario lookup failed")
			return
		}
		if len(scenarios) == 0 {
			log.ErrorContext(ctx, "scenarios table is empty — seed must run before benchmarks can be triggered")
			writeError(w, http.StatusInternalServerError, "no scenarios configured")
			return
		}

		if existing, err := pg.FindActiveRunGroup(ctx, submissionID); err != nil {
			log.ErrorContext(ctx, "find active run-group", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		} else if existing != nil {
			metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "joined_existing"), 1)
			metrics.Counter("active_run_group_conflicts_total", "Benchmark requests that joined an existing active run-group.", nil, 1)
			respondWithGroup(ctx, w, pg, log, existing, scenarios, http.StatusOK)
			return
		}

		runGroupID, err := newUUIDv7()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate run_group_id")
			return
		}
		now := time.Now().UTC()
		group := store.RunGroupMeta{
			RunGroupID:   runGroupID,
			SubmissionID: submissionID,
			ContestantID: sub.ContestantID,
			Status:       topics.RunStatusRequested,
			CreatedAt:    now,
			UpdatedAt:    now,
		}

		children := make([]store.RunMeta, 0, len(scenarios))
		for _, sc := range scenarios {
			sessionID, err := newUUIDv7()
			if err != nil {
				writeError(w, http.StatusInternalServerError, "failed to generate session_id")
				return
			}
			children = append(children, store.RunMeta{
				SessionID:    sessionID,
				SubmissionID: submissionID,
				ContestantID: sub.ContestantID,
				RunGroupID:   runGroupID,
				ScenarioID:   sc.ScenarioID,
				Status:       topics.RunStatusRequested,
				Message:      "",
				CreatedAt:    now,
				UpdatedAt:    now,
			})
		}

		// The first child is dispatched immediately below, so it enters the DB
		// as `queued` — the sequential-dispatch claim query only ever touches
		// `requested` rows and must never re-dispatch it.
		children[0].Status = topics.RunStatusQueued

		if err := pg.InsertRunGroupWithChildren(ctx, group, children); err != nil {
			if errors.Is(err, cerrs.ErrActiveRunGroupExists) {
				existing, ferr := pg.FindActiveRunGroup(ctx, submissionID)
				if ferr != nil || existing == nil {
					log.ErrorContext(ctx, "active run-group conflict but no row found", "submission_id", submissionID, "error", ferr)
					writeError(w, http.StatusInternalServerError, "concurrent benchmark request conflict")
					return
				}
				metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "race_joined_existing"), 1)
				metrics.Counter("active_run_group_conflicts_total", "Benchmark requests that joined an existing active run-group.", nil, 1)
				respondWithGroup(ctx, w, pg, log, existing, scenarios, http.StatusOK)
				return
			}
			log.ErrorContext(ctx, "insert run-group", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to create run-group")
			return
		}

		// SEQUENTIAL-WITHIN-GROUP (decision 2026-08-02): publish ONLY the first
		// scenario. Every child run exists in the DB as `requested`; the
		// benchmark-status consumer dispatches the next one when the current
		// session reaches a terminal state (continue-on-fail). A group
		// therefore holds at most one order band at a time — 4 contestants'
		// groups run concurrently instead of one group monopolizing all 4
		// bands. Scenario order = ListScenarios sort_order (correctness gate
		// first).
		first := children[0]
		if err := pub.PublishBenchmarkRequested(ctx, publisher.BenchmarkMeta{
			SessionID:    first.SessionID,
			SubmissionID: first.SubmissionID,
			ContestantID: first.ContestantID,
			RunGroupID:   first.RunGroupID,
			ScenarioID:   first.ScenarioID,
			RequestedAt:  now,
		}); err != nil {
			log.ErrorContext(ctx, "publish benchmark.requested",
				"session_id", first.SessionID, "scenario_id", first.ScenarioID, "error", err)
			metrics.Counter("benchmark_publish_failures_total", "Benchmark publish failures by topic.", metrics.Labels("topic", topics.TopicBenchmarkRequested), 1)
			metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "publish_failed"), 1)
			writeError(w, http.StatusInternalServerError, "failed to publish benchmark request")
			return
		}

		log.InfoContext(ctx, "benchmark requested",
			"submission_id", submissionID,
			"run_group_id", runGroupID,
			"scenarios", len(children))
		metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "started"), 1)
		metrics.Counter("run_groups_created_total", "Run-groups created by submission-api.", nil, 1)
		for _, sc := range scenarios {
			metrics.Counter("run_group_children_created_total", "Child runs created by scenario.", metrics.Labels("scenario_name", sc.Name), 1)
		}
		respondWithGroup(ctx, w, pg, log, &group, scenarios, http.StatusAccepted)
	}
}

// ListRunGroups performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ListRunGroups(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		// Auth is off: the runs page is public and lists every contestant's
		// run-groups. `?contestant=` is a free-text search over contestant_id /
		// team_name, not an identity gate.
		filter := store.RunGroupListFilter{
			Search:        strings.TrimSpace(q.Get("contestant")),
			SubmissionIDs: q["submission_id"],
			Limit:         limit,
		}
		groups, err := pg.ListRunGroups(r.Context(), filter)
		if err != nil {
			log.ErrorContext(r.Context(), "list run-groups", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to list run-groups")
			return
		}
		scenarios, err := pg.ListScenarios(r.Context())
		if err != nil {
			log.ErrorContext(r.Context(), "list scenarios", "error", err)
			writeError(w, http.StatusInternalServerError, "scenario lookup failed")
			return
		}
		out := make([]benchmarkResponse, 0, len(groups))
		for _, group := range groups {
			resp, err := groupResponse(r.Context(), pg, &group, scenarios)
			if err != nil {
				log.ErrorContext(r.Context(), "list runs by group", "run_group_id", group.RunGroupID, "error", err)
				writeError(w, http.StatusInternalServerError, "failed to load child runs")
				return
			}
			out = append(out, resp)
		}
		writeJSON(w, http.StatusOK, struct {
			RunGroups []benchmarkResponse `json:"run_groups"`
		}{RunGroups: out})
	}
}

// GetRunGroup performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func GetRunGroup(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runGroupID := chi.URLParam(r, "run_group_id")
		if runGroupID == "" {
			writeError(w, http.StatusBadRequest, "missing run_group_id")
			return
		}
		ctx := r.Context()
		group, err := pg.GetRunGroup(ctx, runGroupID)
		if err != nil {
			log.ErrorContext(ctx, "get run-group", "run_group_id", runGroupID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if group == nil {
			writeError(w, http.StatusNotFound, "run-group not found")
			return
		}
		// Auth is off: run-group detail is public (any run is viewable by anyone),
		// so no per-contestant ownership gate here.
		scenarios, err := pg.ListScenarios(ctx)
		if err != nil {
			log.ErrorContext(ctx, "list scenarios", "error", err)
			writeError(w, http.StatusInternalServerError, "scenario lookup failed")
			return
		}
		respondWithGroup(ctx, w, pg, log, group, scenarios, http.StatusOK)
	}
}

// GetRun performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func GetRun(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := chi.URLParam(r, "session_id")
		if sessionID == "" {
			writeError(w, http.StatusBadRequest, "missing session_id")
			return
		}
		run, err := pg.GetRun(r.Context(), sessionID)
		if err != nil {
			log.ErrorContext(r.Context(), "get run", "session_id", sessionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if run == nil {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		if contestantID := contestantIDFromContext(r.Context()); contestantID == "" {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		} else if run.ContestantID != contestantID {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeJSON(w, http.StatusOK, runResponse{
			SessionID:    run.SessionID,
			SubmissionID: run.SubmissionID,
			RunGroupID:   run.RunGroupID,
			ScenarioID:   run.ScenarioID,
			Status:       run.Status,
			Message:      run.Message,
			CreatedAt:    run.CreatedAt,
			UpdatedAt:    run.UpdatedAt,
		})
	}
}

// respondWithGroup performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func respondWithGroup(
	ctx context.Context,
	w http.ResponseWriter,
	pg *store.PostgresStore,
	log *slog.Logger,
	group *store.RunGroupMeta,
	scenarios []store.ScenarioRow,
	httpStatus int,
) {
	resp, err := groupResponse(ctx, pg, group, scenarios)
	if err != nil {
		log.ErrorContext(ctx, "list runs by group", "run_group_id", group.RunGroupID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load child runs")
		return
	}
	writeJSON(w, httpStatus, resp)
}

// groupResponse performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func groupResponse(
	ctx context.Context,
	pg *store.PostgresStore,
	group *store.RunGroupMeta,
	scenarios []store.ScenarioRow,
) (benchmarkResponse, error) {
	scenarioName := make(map[string]string, len(scenarios))
	for _, sc := range scenarios {
		scenarioName[sc.ScenarioID] = sc.Name
	}

	runs, err := pg.ListRunsByGroup(ctx, group.RunGroupID)
	if err != nil {
		return benchmarkResponse{}, err
	}
	children := make([]runGroupChild, 0, len(runs))
	for _, r := range runs {
		children = append(children, runGroupChild{
			SessionID:    r.SessionID,
			ScenarioID:   r.ScenarioID,
			ScenarioName: scenarioName[r.ScenarioID],
			Status:       r.Status,
			Message:      r.Message,
			CreatedAt:    r.CreatedAt,
			UpdatedAt:    r.UpdatedAt,
		})
	}
	return benchmarkResponse{
		RunGroupID:   group.RunGroupID,
		SubmissionID: group.SubmissionID,
		ContestantID: group.ContestantID,
		TeamName:     group.TeamName,
		Status:       group.Status,
		CreatedAt:    group.CreatedAt,
		Runs:         children,
	}, nil
}

// newUUIDv7 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newUUIDv7() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

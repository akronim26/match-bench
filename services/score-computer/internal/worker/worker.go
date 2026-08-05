// Package worker implements worker behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/iicpc/score-computer/internal/publisher"
	"github.com/iicpc/score-computer/internal/redis"
	"github.com/iicpc/score-computer/internal/score"
	"github.com/iicpc/score-computer/internal/store"
)

// Worker groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Worker struct {
	store     *store.Store
	redis     *redis.Client
	publisher *publisher.Publisher
	log       *slog.Logger
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(st *store.Store, redisClient *redis.Client, pub *publisher.Publisher, log *slog.Logger) *Worker {
	return &Worker{store: st, redis: redisClient, publisher: pub, log: log}
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (w *Worker) Run(ctx context.Context, ready <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case runGroupID, ok := <-ready:
			if !ok {
				return
			}
			w.Score(ctx, runGroupID)
		}
	}
}

// Score applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (w *Worker) Score(ctx context.Context, runGroupID string) {
	start := time.Now()
	result := "ok"
	if err := w.score(ctx, runGroupID); err != nil {
		result = "error"
		w.log.Error("score run-group failed", "run_group_id", runGroupID, "error", err)
	}
	metrics.Counter("scorer_run_groups_scored_total", "Run-groups scored by result.", metrics.Labels("result", result), 1)
	metrics.Histogram("scorer_run_group_score_duration_seconds", "Score-computer run-group scoring duration in seconds.", metrics.Labels("result", result), metrics.SinceSeconds(start))
}

// score applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (w *Worker) score(ctx context.Context, runGroupID string) error {
	in, err := w.store.LoadInput(ctx, runGroupID)
	if err != nil {
		return err
	}
	res, err := score.Compute(in)
	if err != nil {
		return err
	}
	inserted, err := w.store.SaveScore(ctx, res)
	if err != nil || !inserted {
		return err
	}

	rank, err := w.store.RankForRunGroup(ctx, runGroupID)
	if err != nil {
		return err
	}
	rankDelta := int64(0)
	if err := w.store.MarkPublished(ctx, runGroupID, rank, rankDelta); err != nil {
		return err
	}
	ev := topics.LeaderboardUpdateEvent{
		RunGroupID:           res.RunGroupID,
		SubmissionID:         res.SubmissionID,
		ContestantID:         res.ContestantID,
		TeamName:             res.TeamName,
		Rank:                 rank,
		RankDelta:            rankDelta,
		PeakSustainedTPS:     res.PeakSustainedTPS,
		P99NSAtPeakTPS:       res.P99AtPeakNS,
		SpikeRecoveryNS:      res.SpikeRecoveryNS,
		TotalCorrectness:     res.TotalCorrectness,
		Disqualified:         res.Disqualified,
		DisqualificationCode: res.DisqualificationCode,
		UpdatedAtNS:          store.NowNS(),
	}
	return w.publisher.Publish(ctx, ev)
}

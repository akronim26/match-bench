// Package consumer implements kafka behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
	"github.com/iicpc/submission-api/internal/utils"
	kafka "github.com/segmentio/kafka-go"
)

// RunStatusStore defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type RunStatusStore interface {
	UpdateRunStatus(ctx context.Context, sessionID, status, message string) error
	RecomputeRunGroupStatus(ctx context.Context, runGroupID string) error
	// ClaimNextRunInGroup atomically flips the group's earliest `requested`
	// run to `queued` and returns it; nil when nothing is left to dispatch.
	ClaimNextRunInGroup(ctx context.Context, runGroupID string) (*store.RunMeta, error)
}

// BenchmarkPublisher is the slice of publisher.Publisher this consumer needs
// to dispatch a group's next scenario.
type BenchmarkPublisher interface {
	PublishBenchmarkRequested(ctx context.Context, meta publisher.BenchmarkMeta) error
}

// BenchmarkStatusConsumer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkStatusConsumer struct {
	reader *kafka.Reader
	pub    BenchmarkPublisher
	pg     RunStatusStore
	log    *slog.Logger
}

// NewBenchmarkStatusConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewBenchmarkStatusConsumer(brokers, groupID string, pg RunStatusStore, pub BenchmarkPublisher, log *slog.Logger) *BenchmarkStatusConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        utils.ParseBrokers(brokers),
		GroupID:        groupID,
		Topic:          topics.TopicBenchmarkStatusUpdated,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
	})
	return &BenchmarkStatusConsumer{reader: r, pg: pg, pub: pub, log: log}
}

// terminalRunStatus reports whether a session status ends the run. Both
// outcomes advance the group's sequential dispatch: continue-on-fail
// (decision 2026-08-02) — a failed correctness gate still lets the scale
// scenarios produce their metrics, and correctness scores 0 via
// pass-1-only aggregation.
func terminalRunStatus(status string) bool {
	return status == topics.RunStatusCompleted || status == topics.RunStatusFailed
}

// advanceRunGroup dispatches the group's next scenario after a session ends.
// Returns false when the status message must NOT be committed (claim or
// publish failed) so Kafka redelivery retries the whole claim+publish —
// redelivery-safe because the claim only touches `requested` rows and a
// failed publish resets its row back to `requested`.
func (c *BenchmarkStatusConsumer) advanceRunGroup(ctx context.Context, msg topics.BenchmarkStatusUpdated) bool {
	next, err := c.pg.ClaimNextRunInGroup(ctx, msg.RunGroupID)
	if err != nil {
		c.log.Error("claim next run in group failed", "run_group_id", msg.RunGroupID, "error", err)
		return false
	}
	if next == nil {
		return true // group fully dispatched; RecomputeRunGroupStatus owns the terminal state
	}
	if err := c.pub.PublishBenchmarkRequested(ctx, publisher.BenchmarkMeta{
		SessionID:    next.SessionID,
		SubmissionID: next.SubmissionID,
		ContestantID: next.ContestantID,
		RunGroupID:   next.RunGroupID,
		ScenarioID:   next.ScenarioID,
		RequestedAt:  time.Now().UTC(),
	}); err != nil {
		c.log.Error("publish next scenario failed; resetting for redelivery",
			"session_id", next.SessionID, "run_group_id", msg.RunGroupID, "error", err)
		if rerr := c.pg.UpdateRunStatus(ctx, next.SessionID, topics.RunStatusRequested, ""); rerr != nil {
			c.log.Error("reset claimed run failed — run may be stuck in queued",
				"session_id", next.SessionID, "error", rerr)
		}
		return false
	}
	metrics.Counter("run_group_sequential_dispatches_total", "Next-scenario dispatches by the status consumer.", nil, 1)
	c.log.Info("dispatched next scenario in group",
		"run_group_id", msg.RunGroupID, "session_id", next.SessionID, "scenario_id", next.ScenarioID,
		"after_session", msg.SessionID, "after_status", msg.Status)
	return true
}

// Start applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *BenchmarkStatusConsumer) Start(ctx context.Context) {
	c.log.Info("benchmark status consumer started", "topic", topics.TopicBenchmarkStatusUpdated)
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			recordConsume(topics.TopicBenchmarkStatusUpdated, "fetch_error", 0)
			c.log.Error("fetch message failed", "error", err)
			continue
		}
		start := time.Now()

		var msg topics.BenchmarkStatusUpdated
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			recordConsume(topics.TopicBenchmarkStatusUpdated, "decode_error", metrics.SinceSeconds(start))
			c.log.Error("unmarshal benchmark status update", "error", err, "key", string(m.Key))
			recordCommit(topics.TopicBenchmarkStatusUpdated, c.reader.CommitMessages(ctx, m))
			continue
		}
		if !validRunStatus(msg.Status) {
			recordConsume(topics.TopicBenchmarkStatusUpdated, "invalid_status", metrics.SinceSeconds(start))
			c.log.Error("invalid benchmark run status", "session_id", msg.SessionID, "status", msg.Status)
			recordCommit(topics.TopicBenchmarkStatusUpdated, c.reader.CommitMessages(ctx, m))
			continue
		}

		if err := c.pg.UpdateRunStatus(ctx, msg.SessionID, msg.Status, msg.Message); err != nil {
			if errors.Is(err, cerrs.ErrRunNotFound) {
				recordConsume(topics.TopicBenchmarkStatusUpdated, "unknown_run", metrics.SinceSeconds(start))
				c.log.Warn("status update for unknown run", "session_id", msg.SessionID, "status", msg.Status)
				recordCommit(topics.TopicBenchmarkStatusUpdated, c.reader.CommitMessages(ctx, m))
				continue
			}
			recordConsume(topics.TopicBenchmarkStatusUpdated, "store_error", metrics.SinceSeconds(start))
			c.log.Error("update run status failed", "session_id", msg.SessionID, "error", err)
			continue
		}
		metrics.Counter("run_status_updates_total", "Run status updates applied by submission-api.", metrics.Labels("status", msg.Status), 1)

		if msg.RunGroupID != "" {
			// Sequential-within-group dispatch BEFORE recompute: a terminal
			// session hands its order band back, and the group's next
			// scenario goes out on the same event. Not-committed on failure
			// so redelivery retries.
			if terminalRunStatus(msg.Status) {
				if !c.advanceRunGroup(ctx, msg) {
					recordConsume(topics.TopicBenchmarkStatusUpdated, "advance_error", metrics.SinceSeconds(start))
					continue
				}
			}
			if err := c.pg.RecomputeRunGroupStatus(ctx, msg.RunGroupID); err != nil {
				c.log.Warn("recompute run-group status failed",
					"run_group_id", msg.RunGroupID,
					"session_id", msg.SessionID,
					"error", err)
			}
		}

		recordConsume(topics.TopicBenchmarkStatusUpdated, "ok", metrics.SinceSeconds(start))
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			recordCommit(topics.TopicBenchmarkStatusUpdated, err)
			c.log.Warn("commit failed", "session_id", msg.SessionID, "error", err)
		} else {
			recordCommit(topics.TopicBenchmarkStatusUpdated, nil)
		}
	}
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *BenchmarkStatusConsumer) Close() error {
	return c.reader.Close()
}

// recordConsume performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordConsume(topic, result string, durationSeconds float64) {
	labels := metrics.Labels("service", "submission-api", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	if durationSeconds > 0 {
		metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, durationSeconds)
	}
}

// recordCommit performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordCommit(topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "submission-api", "topic", topic, "result", result), 1)
}

// validRunStatus performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func validRunStatus(status string) bool {
	switch status {
	case topics.RunStatusRequested,
		topics.RunStatusDeploying,
		topics.RunStatusWaitingReady,
		topics.RunStatusBarrierFired,
		topics.RunStatusRunning,
		topics.RunStatusCompleted,
		topics.RunStatusFailed:
		return true
	default:
		return false
	}
}

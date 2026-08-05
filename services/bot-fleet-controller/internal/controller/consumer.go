// Package controller implements consumer behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

const defaultMaxConcurrentSessions = 4

// sessionRunner is the subset of *Runner the consumer needs to dispatch a
// session; narrowed to an interface so tests can inject a fake and assert on
// concurrent dispatch without standing up Postgres/orchestrator dependencies.
type sessionRunner interface {
	Run(ctx context.Context, req topics.BenchmarkRequested)
}

// statusPublisher is the subset of *Producer the consumer needs to announce
// a session as queued; narrowed to an interface so tests can inject a fake.
type statusPublisher interface {
	PublishStatus(ctx context.Context, evt topics.BenchmarkStatusUpdated) error
}

// Consumer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Consumer struct {
	benchmarkReader *kafka.Reader
	botReadyReader  *kafka.Reader
	runner          sessionRunner
	publisher       statusPublisher
	sessions        *SessionManager
	log             *slog.Logger
	sem             chan struct{}
	wg              sync.WaitGroup
}

// NewConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewConsumer(brokers, benchmarkGroup, botReadyGroup string, runner sessionRunner, publisher statusPublisher, sessions *SessionManager, log *slog.Logger) *Consumer {
	return NewConsumerWithConcurrency(brokers, benchmarkGroup, botReadyGroup, runner, publisher, sessions, log, defaultMaxConcurrentSessions)
}

// NewConsumerWithConcurrency is NewConsumer with an explicit bound on
// simultaneously-dispatched sessions (MAX_CONCURRENT_SESSIONS).
func NewConsumerWithConcurrency(brokers, benchmarkGroup, botReadyGroup string, runner sessionRunner, publisher statusPublisher, sessions *SessionManager, log *slog.Logger, maxConcurrentSessions int) *Consumer {
	if maxConcurrentSessions <= 0 {
		maxConcurrentSessions = defaultMaxConcurrentSessions
	}
	brokerList := parseBrokers(brokers)
	return &Consumer{
		benchmarkReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokerList,
			GroupID:        benchmarkGroup,
			Topic:          topics.TopicBenchmarkRequested,
			MinBytes:       1,
			MaxBytes:       1 << 20,
			MaxWait:        100 * time.Millisecond,
			CommitInterval: 0,
		}),
		botReadyReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokerList,
			GroupID:        botReadyGroup,
			Topic:          topics.TopicBotReady,
			MinBytes:       1,
			MaxBytes:       1 << 20,
			MaxWait:        100 * time.Millisecond,
			CommitInterval: 0,
		}),
		runner:    runner,
		publisher: publisher,
		sessions:  sessions,
		log:       log,
		sem:       make(chan struct{}, maxConcurrentSessions),
	}
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) Close() {
	_ = c.benchmarkReader.Close()
	_ = c.botReadyReader.Close()
}

// StartBenchmarkRequested applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) StartBenchmarkRequested(ctx context.Context) {
	c.log.Info("benchmark.requested consumer started")
	for {
		m, err := c.benchmarkReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			recordConsumer(topics.TopicBenchmarkRequested, "fetch_error", 0)
			c.log.Error("fetch benchmark.requested", "error", err)
			continue
		}
		var req topics.BenchmarkRequested
		if err := json.Unmarshal(m.Value, &req); err != nil {
			recordConsumer(topics.TopicBenchmarkRequested, "decode_error", 0)
			c.log.Error("unmarshal benchmark.requested", "error", err, "key", string(m.Key))
			recordControllerCommit(topics.TopicBenchmarkRequested, c.benchmarkReader.CommitMessages(ctx, m))
			continue
		}

		c.publishQueued(ctx, req)

		if !c.acquireDispatchSlot(ctx) {
			return
		}

		// Commit on dispatch acceptance, not on session completion: a
		// session now runs for its full duration in a background
		// goroutine, and holding the fetch/commit loop open until it
		// finishes would collapse concurrency back to one session at a
		// time. This makes redelivery at-most-once for a session that was
		// accepted but crashes mid-dispatch (e.g. controller restart) —
		// acceptable because a failed run is user-retryable, and strictly
		// better than the at-least-once alternative, which would leak
		// duplicate goroutines racing the same session ID.
		if err := c.benchmarkReader.CommitMessages(ctx, m); err != nil {
			recordControllerCommit(topics.TopicBenchmarkRequested, err)
			c.log.Warn("commit benchmark.requested", "session_id", req.SessionID, "error", err)
		} else {
			recordControllerCommit(topics.TopicBenchmarkRequested, nil)
		}

		c.startSession(ctx, req)
	}
}

// WaitSessions blocks until every dispatched session goroutine has returned
// or timeout elapses, and reports whether the join completed. Called during
// shutdown between stopping the fetch loop and closing the Kafka producer, so
// in-flight sessions get a real window to publish their failure status and
// delete their sandbox slots instead of racing process exit.
func (c *Consumer) WaitSessions(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// publishQueued announces a decoded benchmark.requested as queued, before
// dispatch may block on acquireDispatchSlot's semaphore, so a session sitting
// behind a full MAX_CONCURRENT_SESSIONS gate is visible instead of silently
// absent between "requested" and whatever status the run eventually reaches.
func (c *Consumer) publishQueued(ctx context.Context, req topics.BenchmarkRequested) {
	if c.publisher == nil {
		return
	}
	evt := topics.BenchmarkStatusUpdated{
		SessionID:    req.SessionID,
		SubmissionID: req.SubmissionID,
		RunGroupID:   req.RunGroupID,
		Status:       topics.RunStatusQueued,
		Message:      "awaiting dispatch slot",
		UpdatedAt:    time.Now().UTC(),
	}
	if err := c.publisher.PublishStatus(ctx, evt); err != nil {
		c.log.Error("publish queued status failed", "session_id", req.SessionID, "error", err)
	}
}

// acquireDispatchSlot blocks until a concurrency slot is free or ctx is
// done, returning false in the latter case.
func (c *Consumer) acquireDispatchSlot(ctx context.Context) bool {
	select {
	case c.sem <- struct{}{}:
		return true
	default:
	}
	metrics.Counter("controller_admission_blocked_total", "Session admissions blocked by scarce capacity.", metrics.Labels("reason", "concurrency_limit"), 1)
	select {
	case c.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// startSession launches dispatch in its own tracked goroutine. The wg.Add
// happens on the caller's side of the go statement so WaitSessions can never
// observe a zero counter while a launched session hasn't started yet.
func (c *Consumer) startSession(ctx context.Context, req topics.BenchmarkRequested) {
	c.wg.Add(1)
	go c.dispatch(ctx, req)
}

// dispatch runs one session's full lifecycle in its own goroutine, isolated
// from every other in-flight session: a panic or slow run here neither
// blocks the fetch/commit loop nor any other session's dispatch goroutine.
func (c *Consumer) dispatch(ctx context.Context, req topics.BenchmarkRequested) {
	start := time.Now()
	log := c.log.With("session_id", req.SessionID, "submission_id", req.SubmissionID)
	defer func() {
		c.wg.Done()
		<-c.sem
		if p := recover(); p != nil {
			recordConsumer(topics.TopicBenchmarkRequested, "panic", metrics.SinceSeconds(start))
			log.Error("session dispatch panicked", "panic", p)
		}
	}()
	c.runner.Run(ctx, req)
	recordConsumer(topics.TopicBenchmarkRequested, "ok", metrics.SinceSeconds(start))
}

// StartBotReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) StartBotReady(ctx context.Context) {
	c.log.Info("bot.ready consumer started")
	for {
		m, err := c.botReadyReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			recordConsumer(topics.TopicBotReady, "fetch_error", 0)
			c.log.Error("fetch bot.ready", "error", err)
			continue
		}
		start := time.Now()

		var sig topics.ReadySignal
		if err := json.Unmarshal(m.Value, &sig); err != nil {
			recordConsumer(topics.TopicBotReady, "decode_error", metrics.SinceSeconds(start))
			c.log.Error("unmarshal bot.ready", "error", err, "key", string(m.Key))
			recordControllerCommit(topics.TopicBotReady, c.botReadyReader.CommitMessages(ctx, m))
			continue
		}

		if ok := c.sessions.DispatchReady(sig); !ok {
			metrics.Counter("controller_unknown_ready_signal_total", "Ready signals for unknown sessions.", nil, 1)
			c.log.Warn("ready signal for unknown session",
				"session_id", sig.SessionID,
				"worker_id", sig.WorkerID,
				"worker_index", sig.WorkerIndex,
			)
		}
		recordConsumer(topics.TopicBotReady, "ok", metrics.SinceSeconds(start))

		if err := c.botReadyReader.CommitMessages(ctx, m); err != nil {
			recordControllerCommit(topics.TopicBotReady, err)
			c.log.Warn("commit bot.ready", "session_id", sig.SessionID, "error", err)
		} else {
			recordControllerCommit(topics.TopicBotReady, nil)
		}
	}
}

// recordConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordConsumer(topic, result string, durationSeconds float64) {
	labels := metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	if durationSeconds > 0 {
		metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, durationSeconds)
	}
}

// recordControllerCommit performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordControllerCommit(topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result), 1)
}

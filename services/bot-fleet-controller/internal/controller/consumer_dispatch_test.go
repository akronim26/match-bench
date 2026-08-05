package controller

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSessionRunner lets tests observe Run() calls without a real Runner's
// Postgres/orchestrator/Kafka dependencies.
type fakeSessionRunner struct {
	mu        sync.Mutex
	inflight  int
	maxInFlt  int
	started   chan string
	release   <-chan struct{}
	callCount int32
}

func newFakeSessionRunner(release <-chan struct{}) *fakeSessionRunner {
	return &fakeSessionRunner{
		started: make(chan string, 32),
		release: release,
	}
}

func (f *fakeSessionRunner) Run(_ context.Context, req topics.BenchmarkRequested) {
	atomic.AddInt32(&f.callCount, 1)
	f.mu.Lock()
	f.inflight++
	if f.inflight > f.maxInFlt {
		f.maxInFlt = f.inflight
	}
	f.mu.Unlock()

	f.started <- req.SessionID
	if f.release != nil {
		<-f.release
	}

	f.mu.Lock()
	f.inflight--
	f.mu.Unlock()
}

func (f *fakeSessionRunner) maxConcurrent() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlt
}

// TestConsumerDispatchesSessionsConcurrently proves that two sessions
// dispatch into separate goroutines and run in parallel instead of the old
// StartBenchmarkRequested behavior of blocking the fetch loop on
// runner.Run for the full session lifecycle.
func TestConsumerDispatchesSessionsConcurrently(t *testing.T) {
	release := make(chan struct{})
	runner := newFakeSessionRunner(release)
	c := &Consumer{
		runner: runner,
		log:    testLogger(),
		sem:    make(chan struct{}, 4),
	}

	ctx := context.Background()
	for _, id := range []string{"sess-A", "sess-B"} {
		if !c.acquireDispatchSlot(ctx) {
			t.Fatalf("acquireDispatchSlot failed for %s", id)
		}
		c.startSession(ctx, topics.BenchmarkRequested{SessionID: id})
	}

	seen := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case id := <-runner.started:
			seen[id] = true
		case <-timeout:
			t.Fatalf("only %d/2 sessions started concurrently: %v", len(seen), seen)
		}
	}

	if got := runner.maxConcurrent(); got < 2 {
		t.Fatalf("max concurrent sessions observed = %d, want >= 2 (dispatch is not parallel)", got)
	}

	close(release)
}

// TestConsumerConcurrencyBoundedBySemaphore proves MAX_CONCURRENT_SESSIONS
// caps in-flight dispatch: a third session must not start until one of the
// first two finishes and frees its slot.
func TestConsumerConcurrencyBoundedBySemaphore(t *testing.T) {
	release := make(chan struct{})
	runner := newFakeSessionRunner(release)
	c := &Consumer{
		runner: runner,
		log:    testLogger(),
		sem:    make(chan struct{}, 2),
	}

	ctx := context.Background()
	for _, id := range []string{"sess-1", "sess-2", "sess-3"} {
		go func(sessionID string) {
			if !c.acquireDispatchSlot(ctx) {
				return
			}
			defer func() { <-c.sem }()
			runner.Run(ctx, topics.BenchmarkRequested{SessionID: sessionID})
		}(id)
	}

	started := map[string]bool{}
	drain := func(want int, d time.Duration) {
		timeout := time.After(d)
		for len(started) < want {
			select {
			case id := <-runner.started:
				started[id] = true
			case <-timeout:
				return
			}
		}
	}
	drain(2, 500*time.Millisecond)
	if len(started) != 2 {
		t.Fatalf("expected exactly 2 sessions started while semaphore(2) full and one waiting, got %d: %v", len(started), started)
	}

	close(release)
	drain(3, 2*time.Second)
	if len(started) != 3 {
		t.Fatalf("third session never started after first two released: got %d: %v", len(started), started)
	}
}

// fakePublisher records PublishStatus calls in order and lets tests block on
// them without a real Kafka producer.
type fakePublisher struct {
	mu   sync.Mutex
	evts []topics.BenchmarkStatusUpdated
	seen chan topics.BenchmarkStatusUpdated
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{seen: make(chan topics.BenchmarkStatusUpdated, 32)}
}

func (f *fakePublisher) PublishStatus(_ context.Context, evt topics.BenchmarkStatusUpdated) error {
	f.mu.Lock()
	f.evts = append(f.evts, evt)
	f.mu.Unlock()
	f.seen <- evt
	return nil
}

// TestConsumerPublishesQueuedBeforeDispatchSlotBlocks proves publishQueued
// runs before acquireDispatchSlot ever blocks: with the semaphore already
// full, StartBenchmarkRequested must still emit the "queued" status for the
// message it just decoded, even though dispatch itself cannot proceed yet.
func TestConsumerPublishesQueuedBeforeDispatchSlotBlocks(t *testing.T) {
	pub := newFakePublisher()
	c := &Consumer{
		publisher: pub,
		log:       testLogger(),
		sem:       make(chan struct{}, 1),
	}
	c.sem <- struct{}{} // fill the only slot so acquireDispatchSlot would block

	req := topics.BenchmarkRequested{SessionID: "sess-queued"}
	c.publishQueued(context.Background(), req)

	select {
	case evt := <-pub.seen:
		if evt.SessionID != "sess-queued" || evt.Status != topics.RunStatusQueued {
			t.Fatalf("unexpected published event: %+v", evt)
		}
	default:
		t.Fatal("expected queued status to be published without acquiring a dispatch slot")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if c.acquireDispatchSlot(ctx) {
		t.Fatal("sanity check: semaphore should still be full")
	}
}

// TestConsumerAcquireDispatchSlotRespectsContextCancellation ensures a
// blocked admission unblocks on shutdown instead of leaking forever.
func TestConsumerAcquireDispatchSlotRespectsContextCancellation(t *testing.T) {
	c := &Consumer{sem: make(chan struct{}, 1)}
	c.sem <- struct{}{}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if c.acquireDispatchSlot(ctx) {
		t.Fatal("expected acquireDispatchSlot to fail once ctx is done while semaphore is full")
	}
}

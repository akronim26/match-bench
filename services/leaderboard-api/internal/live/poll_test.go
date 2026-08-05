package live

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	sessions []ActiveSessionContestant
	err      error
}

func (f *fakeStore) ActiveSessionContestants(ctx context.Context) ([]ActiveSessionContestant, error) {
	return f.sessions, f.err
}

type fakeRedis struct {
	mu     sync.Mutex
	values map[string][]byte
	hashes map[string]map[string]string
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{values: map[string][]byte{}, hashes: map[string]map[string]string{}}
}

func (f *fakeRedis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.values[key]
	return v, ok, nil
}

func (f *fakeRedis) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hashes[key], nil
}

type fakeBroker struct {
	mu       sync.Mutex
	payloads []any
}

func (f *fakeBroker) BroadcastLive(payload any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.payloads = append(f.payloads, payload)
}

func (f *fakeBroker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.payloads)
}

func (f *fakeBroker) last() any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.payloads) == 0 {
		return nil
	}
	return f.payloads[len(f.payloads)-1]
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestPoller_BroadcastsMatchingLiveMetrics(t *testing.T) {
	store := &fakeStore{sessions: []ActiveSessionContestant{{SessionID: "s1", ContestantID: "c1"}}}
	redis := newFakeRedis()
	redis.values["live:s1:latest"] = []byte("3")
	redis.hashes["contestant:c1:s1:3"] = map[string]string{
		"p50_ns": "100", "p99_ns": "200", "p999_ns": "300",
		"tps_1s": "1234.5", "error_rate": "0.01", "updated_at_ns": "999",
		"wave_index": "3", "session_id": "s1",
	}
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	p.tick(context.Background())

	if broker.count() != 1 {
		t.Fatalf("expected 1 broadcast, got %d", broker.count())
	}
	got, ok := broker.last().(LiveMetrics)
	if !ok {
		t.Fatalf("expected LiveMetrics payload, got %T", broker.last())
	}
	want := LiveMetrics{
		ContestantID: "c1", SessionID: "s1", WaveIndex: 3,
		P50NS: 100, P99NS: 200, P999NS: 300,
		TPS1s: 1234.5, ErrorRate: 0.01, UpdatedAtNS: 999,
	}
	if got != want {
		t.Fatalf("payload mismatch: got %+v want %+v", got, want)
	}
}

func TestPoller_NoLivePointer_SkipsSession(t *testing.T) {
	store := &fakeStore{sessions: []ActiveSessionContestant{{SessionID: "s1", ContestantID: "c1"}}}
	redis := newFakeRedis()
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	p.tick(context.Background())

	if broker.count() != 0 {
		t.Fatalf("expected 0 broadcasts, got %d", broker.count())
	}
}

func TestPoller_EmptyHash_SkippedWithoutPanic(t *testing.T) {
	store := &fakeStore{sessions: []ActiveSessionContestant{{SessionID: "s1", ContestantID: "c1"}}}
	redis := newFakeRedis()
	redis.values["live:s1:latest"] = []byte("5")
	// no hash set for contestant:c1:s1:5
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	p.tick(context.Background())

	if broker.count() != 0 {
		t.Fatalf("expected 0 broadcasts, got %d", broker.count())
	}
}

func TestPoller_StoreError_SkipsTickWithoutPanic(t *testing.T) {
	store := &fakeStore{err: errors.New("boom")}
	redis := newFakeRedis()
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	p.tick(context.Background())

	if broker.count() != 0 {
		t.Fatalf("expected 0 broadcasts, got %d", broker.count())
	}
}

func TestPoller_UnchangedUpdatedAt_SkipsSecondBroadcast(t *testing.T) {
	store := &fakeStore{sessions: []ActiveSessionContestant{{SessionID: "s1", ContestantID: "c1"}}}
	redis := newFakeRedis()
	redis.values["live:s1:latest"] = []byte("3")
	redis.hashes["contestant:c1:s1:3"] = map[string]string{
		"p50_ns": "100", "p99_ns": "200", "p999_ns": "300",
		"tps_1s": "1234.5", "error_rate": "0.01", "updated_at_ns": "999",
		"wave_index": "3", "session_id": "s1",
	}
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	p.tick(context.Background())
	p.tick(context.Background())

	if broker.count() != 1 {
		t.Fatalf("expected 1 broadcast after two ticks with unchanged updated_at_ns, got %d", broker.count())
	}
}

func TestPoller_ChangedUpdatedAt_BroadcastsAgain(t *testing.T) {
	store := &fakeStore{sessions: []ActiveSessionContestant{{SessionID: "s1", ContestantID: "c1"}}}
	redis := newFakeRedis()
	redis.values["live:s1:latest"] = []byte("3")
	redis.hashes["contestant:c1:s1:3"] = map[string]string{
		"p50_ns": "100", "p99_ns": "200", "p999_ns": "300",
		"tps_1s": "1234.5", "error_rate": "0.01", "updated_at_ns": "999",
		"wave_index": "3", "session_id": "s1",
	}
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	p.tick(context.Background())

	redis.hashes["contestant:c1:s1:3"]["updated_at_ns"] = "1000"
	p.tick(context.Background())

	if broker.count() != 2 {
		t.Fatalf("expected 2 broadcasts after updated_at_ns changed, got %d", broker.count())
	}
}

func TestPoller_PersistentParseFailure_RateLimitsWarnings(t *testing.T) {
	store := &fakeStore{sessions: []ActiveSessionContestant{{SessionID: "s1", ContestantID: "c1"}}}
	redis := newFakeRedis()
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	// live pointer present but not parseable as an int -> parse-warn path,
	// on every tick, for a persistently-failing session.
	redis.values["live:s1:latest"] = []byte("not-a-number")

	for i := 0; i < parseWarnEveryNTicks+5; i++ {
		p.tick(context.Background())
	}

	entry := p.parseWarnState["s1"]
	if entry == nil {
		t.Fatal("expected parse-warn state to be tracked for session s1")
	}
	// After parseWarnEveryNTicks+5 ticks with an unchanged failure, exactly
	// two warnings should have been logged (tick 1, and tick parseWarnEveryNTicks+1),
	// so ticksSinceLog should be 4 (5 ticks past the second log).
	if entry.ticksSinceLog != 4 {
		t.Fatalf("expected ticksSinceLog to be 4 after rate-limited relogging, got %d", entry.ticksSinceLog)
	}
}

func TestPoller_Run_StopsPromptlyOnCancel(t *testing.T) {
	store := &fakeStore{}
	redis := newFakeRedis()
	broker := &fakeBroker{}
	p := New(store, redis, broker, 10*time.Millisecond, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return within 200ms of ctx cancellation")
	}
}

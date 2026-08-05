package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPartitionLeaseAllocatorAcquireRelease(t *testing.T) {
	a := NewPartitionLeaseAllocator(4)

	parts, err := a.Acquire(context.Background(), "sess-a", 3)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("got %d partitions, want 3", len(parts))
	}
	if got := a.LeasedCount(); got != 3 {
		t.Fatalf("LeasedCount = %d, want 3", got)
	}

	a.Release("sess-a")
	if got := a.LeasedCount(); got != 0 {
		t.Fatalf("LeasedCount after release = %d, want 0", got)
	}
}

func TestPartitionLeaseAllocatorNoOverlap(t *testing.T) {
	a := NewPartitionLeaseAllocator(6)

	p1, err := a.Acquire(context.Background(), "sess-1", 3)
	if err != nil {
		t.Fatalf("Acquire sess-1: %v", err)
	}
	p2, err := a.Acquire(context.Background(), "sess-2", 3)
	if err != nil {
		t.Fatalf("Acquire sess-2: %v", err)
	}

	seen := map[int]string{}
	for _, p := range p1 {
		seen[p] = "sess-1"
	}
	for _, p := range p2 {
		if owner, ok := seen[p]; ok {
			t.Fatalf("partition %d leased to both %s and sess-2", p, owner)
		}
	}
}

func TestPartitionLeaseAllocatorBlocksUntilCapacityFrees(t *testing.T) {
	a := NewPartitionLeaseAllocator(2)

	if _, err := a.Acquire(context.Background(), "sess-1", 2); err != nil {
		t.Fatalf("Acquire sess-1: %v", err)
	}

	done := make(chan []int, 1)
	errCh := make(chan error, 1)
	go func() {
		parts, err := a.Acquire(context.Background(), "sess-2", 1)
		if err != nil {
			errCh <- err
			return
		}
		done <- parts
	}()

	select {
	case <-done:
		t.Fatal("sess-2 acquired before sess-1 released — no admission blocking")
	case <-time.After(100 * time.Millisecond):
	}

	a.Release("sess-1")

	select {
	case parts := <-done:
		if len(parts) != 1 {
			t.Fatalf("got %d partitions, want 1", len(parts))
		}
	case err := <-errCh:
		t.Fatalf("Acquire sess-2: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("sess-2 never acquired after sess-1 released")
	}
}

func TestPartitionLeaseAllocatorAcquireRespectsContextTimeout(t *testing.T) {
	a := NewPartitionLeaseAllocator(1)
	if _, err := a.Acquire(context.Background(), "sess-1", 1); err != nil {
		t.Fatalf("Acquire sess-1: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := a.Acquire(ctx, "sess-2", 1); err == nil {
		t.Fatal("expected Acquire to time out while capacity is exhausted")
	}
}

func TestBandLeaseAllocatorAcquireReleaseFourBands(t *testing.T) {
	a := NewBandLeaseAllocator(4)

	bands, err := a.Acquire(context.Background(), "sess-a", 1)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if len(bands) != 1 || bands[0] < 0 || bands[0] > 3 {
		t.Fatalf("unexpected band lease: %v", bands)
	}
	if got := a.LeasedCount(); got != 1 {
		t.Fatalf("LeasedCount = %d, want 1", got)
	}
	a.Release("sess-a")
	if got := a.LeasedCount(); got != 0 {
		t.Fatalf("LeasedCount after release = %d, want 0", got)
	}
}

func TestBandLeaseAllocatorExclusiveNoTwoSessionsShareABand(t *testing.T) {
	a := NewBandLeaseAllocator(4)
	seen := map[int]string{}
	for i := 0; i < 4; i++ {
		sessionID := fmt.Sprintf("sess-%d", i)
		bands, err := a.Acquire(context.Background(), sessionID, 1)
		if err != nil {
			t.Fatalf("Acquire %s: %v", sessionID, err)
		}
		for _, b := range bands {
			if owner, ok := seen[b]; ok {
				t.Fatalf("band %d leased to both %s and %s", b, owner, sessionID)
			}
			seen[b] = sessionID
		}
	}
	// A 5th session must block: all 4 bands are exhausted.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := a.Acquire(ctx, "sess-overflow", 1); err == nil {
		t.Fatal("expected 5th concurrent session to block/time out — only 4 exclusive bands exist")
	}
}

// TestBothOrBlockAtomicity pins the admission contract in Runner.Run: a
// session must never end up holding a partition lease without also holding
// its order band lease, or vice versa. This test drives the two allocators
// directly the way Run does — acquire partitions, then bands, releasing
// partitions if the band acquire fails — and asserts the session holds
// nothing in either allocator afterward.
func TestBothOrBlockAtomicity(t *testing.T) {
	partitions := NewPartitionLeaseAllocator(4)
	bands := NewBandLeaseAllocator(1) // only 1 band total, forces the band acquire to fail for a 2nd session

	// First session takes the only band, plus some partitions.
	if _, err := partitions.Acquire(context.Background(), "sess-1", 2); err != nil {
		t.Fatalf("sess-1 partitions: %v", err)
	}
	if _, err := bands.Acquire(context.Background(), "sess-1", 1); err != nil {
		t.Fatalf("sess-1 band: %v", err)
	}

	// Second session: partitions succeed (plenty free), but the band
	// allocator is exhausted — Run must release the partitions it just took.
	leased, err := partitions.Acquire(context.Background(), "sess-2", 2)
	if err != nil {
		t.Fatalf("sess-2 partitions: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := bands.Acquire(ctx, "sess-2", 1); err == nil {
		t.Fatal("expected sess-2 band acquire to fail — only 1 band, held by sess-1")
	}
	partitions.Release("sess-2")

	if got := partitions.LeasedCount(); got != 2 {
		t.Fatalf("expected sess-2's partitions released, LeasedCount = %d, want 2 (only sess-1's)", got)
	}
	if got := bands.LeasedCount(); got != 1 {
		t.Fatalf("expected only sess-1's band leased, LeasedCount = %d, want 1", got)
	}
	_ = leased
}

func TestPartitionLeaseAllocatorRejectsOverTotal(t *testing.T) {
	a := NewPartitionLeaseAllocator(4)
	if _, err := a.Acquire(context.Background(), "sess-1", 5); err == nil {
		t.Fatal("expected error requesting more partitions than exist")
	}
}

func TestPartitionLeaseAllocatorDuplicateSessionRejected(t *testing.T) {
	a := NewPartitionLeaseAllocator(4)
	if _, err := a.Acquire(context.Background(), "sess-1", 2); err != nil {
		t.Fatalf("Acquire sess-1: %v", err)
	}
	if _, err := a.Acquire(context.Background(), "sess-1", 1); err == nil {
		t.Fatal("expected error re-acquiring for a session that already holds a lease")
	}
}

// TestPartitionLeaseAllocatorConcurrentAdmissionRace hammers Acquire/Release
// from many goroutines and asserts the invariant that never breaks: total
// leased partitions across concurrently-held sessions never exceeds the
// allocator's total, and no two live sessions ever share a partition.
func TestPartitionLeaseAllocatorConcurrentAdmissionRace(t *testing.T) {
	const total = 8
	const sessions = 40
	a := NewPartitionLeaseAllocator(total)

	var wg sync.WaitGroup
	var mu sync.Mutex
	held := map[int]string{}
	violations := 0

	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessionID := fmt.Sprintf("race-sess-%d", i)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			want := 1 + i%total
			if want > total {
				want = total
			}
			parts, err := a.Acquire(ctx, sessionID, want)
			if err != nil {
				return
			}
			mu.Lock()
			for _, p := range parts {
				if owner, ok := held[p]; ok {
					violations++
					t.Errorf("partition %d double-leased: %s and %s", p, owner, sessionID)
				}
				held[p] = sessionID
			}
			mu.Unlock()

			time.Sleep(time.Millisecond)

			mu.Lock()
			for _, p := range parts {
				delete(held, p)
			}
			mu.Unlock()
			a.Release(sessionID)
		}(i)
	}
	wg.Wait()

	if violations > 0 {
		t.Fatalf("%d double-lease violations under concurrent admission", violations)
	}
	if got := a.LeasedCount(); got != 0 {
		t.Fatalf("LeasedCount after all releases = %d, want 0", got)
	}
}

// TestPartitionLeaseAllocatorFIFOFairness pins the head-of-line guarantee: a
// large request queued FIRST is served before smaller later arrivals, even
// when interim releases would satisfy the small one. Unwritable under the old
// broadcast-wakeup design (scheduler-order lottery); deterministic with
// in-lock grant hand-off.
func TestPartitionLeaseAllocatorFIFOFairness(t *testing.T) {
	a := NewPartitionLeaseAllocator(24)
	ctx := context.Background()

	// Occupy 20 of 24: five 4-partition sessions.
	for i := 0; i < 5; i++ {
		if _, err := a.Acquire(ctx, fmt.Sprintf("small-%d", i), 4); err != nil {
			t.Fatalf("setup acquire: %v", err)
		}
	}

	bigDone := make(chan []int, 1)
	go func() {
		parts, err := a.Acquire(ctx, "big", 16)
		if err != nil {
			t.Errorf("big acquire: %v", err)
		}
		bigDone <- parts
	}()
	waitForQueueDepth(t, a, 1)

	// A small request arriving AFTER big must queue behind it even though 4
	// partitions are free right now.
	lateDone := make(chan struct{})
	go func() {
		if _, err := a.Acquire(ctx, "late-small", 4); err != nil {
			t.Errorf("late-small acquire: %v", err)
		}
		close(lateDone)
	}()
	waitForQueueDepth(t, a, 2)

	// Free 12 more (16 total free): enough for big, and along the way enough
	// for late-small several times over — big must win every intermediate
	// release.
	a.Release("small-0")
	a.Release("small-1")
	select {
	case <-bigDone:
		t.Fatal("big served before enough capacity freed")
	case <-lateDone:
		t.Fatal("late-small overtook the queued big request")
	case <-time.After(50 * time.Millisecond):
	}
	a.Release("small-2")

	parts := <-bigDone
	if len(parts) != 16 {
		t.Fatalf("big got %d partitions, want 16", len(parts))
	}
	// late-small still waits (free = 0 after big took 16 of the 16 free).
	select {
	case <-lateDone:
		t.Fatal("late-small served with zero free partitions")
	case <-time.After(50 * time.Millisecond):
	}
	a.Release("small-3")
	<-lateDone
}

// TestPartitionLeaseAllocatorCancelledHeadUnblocksNext proves a head waiter
// that gives up (ctx cancel) does not wedge the queue behind it.
func TestPartitionLeaseAllocatorCancelledHeadUnblocksNext(t *testing.T) {
	a := NewPartitionLeaseAllocator(8)
	ctx := context.Background()
	if _, err := a.Acquire(ctx, "holder", 6); err != nil {
		t.Fatalf("setup: %v", err)
	}

	bigCtx, cancelBig := context.WithCancel(ctx)
	bigErr := make(chan error, 1)
	go func() {
		_, err := a.Acquire(bigCtx, "doomed-big", 8)
		bigErr <- err
	}()
	waitForQueueDepth(t, a, 1)

	smallDone := make(chan struct{})
	go func() {
		if _, err := a.Acquire(ctx, "small", 2); err != nil {
			t.Errorf("small acquire: %v", err)
		}
		close(smallDone)
	}()
	waitForQueueDepth(t, a, 2)

	// 2 partitions are free the whole time, but small sits behind doomed-big.
	select {
	case <-smallDone:
		t.Fatal("small overtook queued head")
	case <-time.After(50 * time.Millisecond):
	}

	cancelBig()
	if err := <-bigErr; err == nil {
		t.Fatal("cancelled head returned nil error")
	}
	// Head removal must re-serve the queue: small now fits from existing free.
	<-smallDone
}

func waitForQueueDepth(t *testing.T, a *PartitionLeaseAllocator, depth int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		n := len(a.queue)
		a.mu.Unlock()
		if n >= depth {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue never reached depth %d", depth)
}

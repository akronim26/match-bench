package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/iicpc/libs/metrics"
)

// PartitionLeaseAllocator hands out exclusive workload.assignments partitions
// to sessions so two sessions' worker specs never collide on the same
// partition (see docs/multi-contestant-audit.md §1/§2: worker_index %
// partitions let worker 0 of every session land on partition 0). It is an
// in-memory bitmap — sufficient while the controller runs at replicas=1;
// promote to a Postgres-backed table before scaling replicas.
type PartitionLeaseAllocator struct {
	mu         sync.Mutex
	total      int
	free       map[int]struct{}
	leasedBy   map[string][]int
	queue      []*leaseWaiter
	metricName string
	reason     string
}

// leaseWaiter is one blocked Acquire in FIFO order. Release serves the HEAD
// only — head-of-line blocking is deliberate: freed partitions accumulate for
// the oldest waiter instead of being skimmed by smaller later arrivals, which
// under the previous broadcast-wakeup scheme let a large request starve
// indefinitely behind a stream of small ones. The grant is delivered (leasedBy
// updated, partitions moved) inside the allocator lock; the waiter never
// re-races.
type leaseWaiter struct {
	sessionID string
	count     int
	grant     chan []int // buffered(1); receiving means the lease is already recorded
}

// NewPartitionLeaseAllocator builds an allocator over workload.assignments
// partitions [0, total).
func NewPartitionLeaseAllocator(total int) *PartitionLeaseAllocator {
	return newLeaseAllocator(total, "partitions", "partition_leases")
}

// NewBandLeaseAllocator builds a second, independent allocator over EXCLUSIVE
// orders.sent/orders.acked partition bands [0, total) — same generic
// bitmap-lease mechanics as PartitionLeaseAllocator (Acquire blocks until
// count free bands are available, Release returns them), just over band
// indices instead of workload.assignments partition indices. Bands are
// acquired alongside partition leases at session admission (both-or-block,
// same LEASE_ACQUIRE_TIMEOUT) so a session never runs with only one of the
// two leases held.
func NewBandLeaseAllocator(total int) *PartitionLeaseAllocator {
	return newLeaseAllocator(total, "order_bands", "order_band_leases")
}

func newLeaseAllocator(total int, metricName, reason string) *PartitionLeaseAllocator {
	free := make(map[int]struct{}, total)
	for i := 0; i < total; i++ {
		free[i] = struct{}{}
	}
	return &PartitionLeaseAllocator{
		total:      total,
		free:       free,
		leasedBy:   make(map[string][]int),
		metricName: metricName,
		reason:     reason,
	}
}

// Acquire blocks until count free partitions are available for sessionID, or
// ctx is done. Free partitions below count is the cross-session capacity
// check validateWorkerCapacity never was: it now blocks admission instead of
// silently colliding two sessions' worker specs on the same partition.
func (a *PartitionLeaseAllocator) Acquire(ctx context.Context, sessionID string, count int) ([]int, error) {
	if count <= 0 {
		return nil, fmt.Errorf("lease count must be positive, got %d", count)
	}
	a.mu.Lock()
	if count > a.total {
		a.mu.Unlock()
		return nil, fmt.Errorf("requested %d partitions exceeds workload.assignments partition count %d", count, a.total)
	}
	if _, already := a.leasedBy[sessionID]; already {
		a.mu.Unlock()
		return nil, fmt.Errorf("session %s already holds a partition lease", sessionID)
	}
	// Fast path only when nobody is queued: an empty-queue check keeps FIFO
	// order — a new arrival must not overtake an already-waiting session even
	// if the free pool happens to cover it.
	if len(a.queue) == 0 && len(a.free) >= count {
		parts := a.takeLocked(sessionID, count)
		a.mu.Unlock()
		return parts, nil
	}
	w := &leaseWaiter{sessionID: sessionID, count: count, grant: make(chan []int, 1)}
	a.queue = append(a.queue, w)
	a.mu.Unlock()
	metrics.Counter("controller_admission_blocked_total", "Session admissions blocked by scarce capacity.", metrics.Labels("reason", a.reason), 1)

	select {
	case parts := <-w.grant:
		return parts, nil
	case <-ctx.Done():
		a.mu.Lock()
		for i, q := range a.queue {
			if q == w {
				a.queue = append(a.queue[:i], a.queue[i+1:]...)
				// Removing a waiter can unblock the one behind it.
				a.serveQueueLocked()
				a.mu.Unlock()
				return nil, ctx.Err()
			}
		}
		// Not in the queue: Release granted us concurrently with cancellation.
		// The lease is already recorded — undo it so a failed admission never
		// leaks partitions.
		a.mu.Unlock()
		parts := <-w.grant
		a.mu.Lock()
		for _, p := range parts {
			a.free[p] = struct{}{}
		}
		delete(a.leasedBy, sessionID)
		a.reportLocked()
		a.serveQueueLocked()
		a.mu.Unlock()
		return nil, ctx.Err()
	}
}

// takeLocked moves count partitions from free to sessionID's lease and returns
// them sorted. Callers must hold mu and have checked len(free) >= count.
func (a *PartitionLeaseAllocator) takeLocked(sessionID string, count int) []int {
	parts := make([]int, 0, count)
	for p := range a.free {
		parts = append(parts, p)
		if len(parts) == count {
			break
		}
	}
	sort.Ints(parts)
	for _, p := range parts {
		delete(a.free, p)
	}
	a.leasedBy[sessionID] = parts
	a.reportLocked()
	return parts
}

// serveQueueLocked grants leases to queued waiters strictly from the head:
// if the head fits, grant and continue with the next head; if it does not,
// stop — freed partitions accumulate for it (head-of-line blocking is the
// fairness guarantee). Callers must hold mu.
func (a *PartitionLeaseAllocator) serveQueueLocked() {
	for len(a.queue) > 0 {
		head := a.queue[0]
		if len(a.free) < head.count {
			return
		}
		parts := a.takeLocked(head.sessionID, head.count)
		a.queue = a.queue[1:]
		head.grant <- parts
	}
}

// Release returns sessionID's leased partitions to the free pool. Safe to
// call on a session that holds no lease (no-op).
func (a *PartitionLeaseAllocator) Release(sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	parts, ok := a.leasedBy[sessionID]
	if !ok {
		return
	}
	for _, p := range parts {
		a.free[p] = struct{}{}
	}
	delete(a.leasedBy, sessionID)
	a.reportLocked()
	a.serveQueueLocked()
}

// LeasedCount returns the total number of partitions currently leased across
// all sessions.
func (a *PartitionLeaseAllocator) LeasedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total - len(a.free)
}

// reportLocked publishes the leased-partitions gauge. Callers must hold mu.
func (a *PartitionLeaseAllocator) reportLocked() {
	metrics.Gauge(
		"controller_leased_"+a.metricName,
		"Resources ("+a.metricName+") currently leased by in-flight sessions.",
		nil,
		float64(a.total-len(a.free)),
	)
}

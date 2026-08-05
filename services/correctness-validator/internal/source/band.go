package source

import (
	"sync"

	"github.com/iicpc/schemas/topics"
)

// BandCache learns each session's exclusively-leased order_band from
// workload.assignments (topics.WorkloadSpec.OrderBand, already stamped by
// bot-fleet-controller's band-lease allocator) so StreamSession can restrict
// its Kafka readers to that band instead of scanning every partition.
//
// Entries are removed on read (GetAndDelete): a session's band is only ever
// needed once, at validation time, so deleting then bounds memory without a
// TTL sweep. Sessions in flight at process startup (band learned before this
// consumer catches up) or produced by band-unaware workers fall back to
// topics.OrderBandUnset, StreamSession's existing full-scan back-compat path.
// SessionMeta is what the band consumer learns per session from its
// WorkloadSpec: the exclusively-leased order band, and whether every task is
// max-rate (target_rps == 0) — the signature of the pass-1 `correctness`
// scenario, which validateSession uses to pick full-replay vs invariants mode
// without any new schema field.
type SessionMeta struct {
	Band    uint32
	MaxRate bool
}

type BandCache struct {
	mu sync.Mutex
	m  map[string]SessionMeta
}

// NewBandCache builds an empty cache.
func NewBandCache() *BandCache {
	return &BandCache{m: make(map[string]SessionMeta)}
}

// Set records sessionID's order_band, overwriting any prior value (workers
// within a session all carry the same band, so last-write is idempotent).
func (c *BandCache) Set(sessionID string, meta SessionMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[sessionID] = meta
}

// GetAndDelete returns sessionID's cached order_band and removes it, or
// OrderBandUnset if the session was never observed.
func (c *BandCache) GetAndDelete(sessionID string) SessionMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	meta, ok := c.m[sessionID]
	if !ok {
		return SessionMeta{Band: topics.OrderBandUnset}
	}
	delete(c.m, sessionID)
	return meta
}

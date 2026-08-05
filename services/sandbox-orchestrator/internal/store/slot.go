// Package store implements slot behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package store

import (
	"sync"
	"time"
)

type SlotState string

const (
	StateCreating    SlotState = "creating"    // Pod exists, not yet Ready
	StateReady       SlotState = "ready"       // Pod Ready condition is True
	StateFailed      SlotState = "failed"      // ImagePullBackOff, CrashLoopBackOff, or Failed phase
	StateTerminating SlotState = "terminating" // DELETE called, cleanup in progress
)

// Slot groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Slot struct {
	SlotID    string
	Image     string
	Port      int   // primary (first declared) port; kept for back-compat callers
	Ports     []int // every declared port; len 1 for single-port slots
	State     SlotState
	Message   string // human-readable reason for the current state
	Endpoint  Endpoint
	CreatedAt time.Time
}

// Endpoint groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Endpoint struct {
	Host string // e.g. algo-{slot_id}.sandbox.svc.cluster.local
	Port int
}

// SlotStore groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SlotStore struct {
	mu    sync.RWMutex
	slots map[string]*Slot
}

// NewSlotStore performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewSlotStore() *SlotStore {
	return &SlotStore{slots: make(map[string]*Slot)}
}

// Get applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *SlotStore) Get(slotID string) (*Slot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slot, ok := s.slots[slotID]
	if !ok {
		return nil, false
	}
	cp := *slot
	return &cp, true
}

// Put applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *SlotStore) Put(slot *Slot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *slot
	s.slots[slot.SlotID] = &cp
}

// Delete applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *SlotStore) Delete(slotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.slots, slotID)
}

// Len applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *SlotStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.slots)
}

// List applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *SlotStore) List() []Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Slot, 0, len(s.slots))
	for _, slot := range s.slots {
		out = append(out, *slot)
	}
	return out
}

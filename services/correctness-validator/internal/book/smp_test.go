// Package book tests self-match prevention (skip-and-continue) in the reference book.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package book

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/schemas/topics"
)

// smpLimit is `limit` with a self-match-prevention id attached.
func smpLimit(id string, side model.Side, price int64, qty uint64, smp uint32) *model.Order {
	o := limit(id, side, price, qty)
	// HasSMPID is what distinguishes "no id" from "id 0" — see model.Order. A caller
	// that set only SMPID would get id 0 treated as absent.
	o.SMPID, o.HasSMPID = smp, smp != topics.SMPIDNone
	return o
}

// TestSMPSkipsSameID is the core rule: an aggressor never matches a resting order
// carrying the same SMP id. It SKIPS that order and continues to the next one at the
// price level; nothing is cancelled.
//
// Skip-and-continue was chosen over the cancel-based policies real venues also offer
// (cancel-resting, cancel-aggressor, cancel-both) because those emit an
// engine-initiated cancel for an order the client never asked to cancel — a new
// message class through the capture, the ack stream, and the validator's lost-order
// accounting, which currently treats an unacted-on order as a violation.
func TestSMPSkipsSameID(t *testing.T) {
	e := NewEngine()
	// Two resting sells at the same price: the first shares the taker's id.
	e.Process(smpLimit("mine", model.Sell, 100, 10, 3))
	e.Process(smpLimit("other", model.Sell, 100, 10, 5))

	e.DrainFills()
	e.Process(smpLimit("taker", model.Buy, 100, 10, 3))
	fills := e.DrainFills()

	// The taker must have matched "other", not "mine".
	var matchedIDs []string
	for _, f := range fills {
		matchedIDs = append(matchedIDs, f.OrderID)
	}
	for _, id := range matchedIDs {
		if id == "mine" {
			t.Fatalf("aggressor matched a resting order sharing its SMP id; fills: %v", matchedIDs)
		}
	}
	if len(fills) == 0 {
		t.Fatalf("aggressor should have skipped to 'other' and filled, got no fills")
	}

	// And the skipped order must still be RESTING — skip-and-continue cancels nothing.
	if !e.IsResting("mine") {
		t.Error("skipped order was removed from the book; skip-and-continue must not cancel it")
	}
}

// TestSMPNoneIsUnconstrained: an order carrying no SMP id matches anything, including
// another order with no id. This is what keeps pass-2 (scale) runs behaving exactly as
// before — SMP is not graded there and the field is omitted from the wire entirely.
func TestSMPNoneIsUnconstrained(t *testing.T) {
	e := NewEngine()
	e.Process(smpLimit("rest", model.Sell, 100, 10, topics.SMPIDNone))
	e.DrainFills()
	e.Process(smpLimit("take", model.Buy, 100, 10, topics.SMPIDNone))
	if fills := e.DrainFills(); len(fills) == 0 {
		t.Error("two id-less orders must match; SMP applies only to orders that carry an id")
	}
}

// TestSMPZeroIsNotNone guards the sentinel choice. SMPIDNone is u32::MAX, NOT 0,
// because 0 is a valid id. If they were conflated, every id-less order would look like
// participant 0 and — under skip-and-continue — an engine would refuse to match
// anything at all.
func TestSMPZeroIsNotNone(t *testing.T) {
	e := NewEngine()
	e.Process(smpLimit("rest0", model.Sell, 100, 10, 0))
	e.DrainFills()
	// A DIFFERENT id must still match id 0.
	e.Process(smpLimit("take1", model.Buy, 100, 10, 1))
	if fills := e.DrainFills(); len(fills) == 0 {
		t.Error("id 1 must match resting id 0")
	}

	e2 := NewEngine()
	e2.Process(smpLimit("rest0b", model.Sell, 100, 10, 0))
	e2.DrainFills()
	// The SAME id (0) must NOT match.
	e2.Process(smpLimit("take0", model.Buy, 100, 10, 0))
	if fills := e2.DrainFills(); len(fills) != 0 {
		t.Errorf("id 0 must not match resting id 0; got fills %v", fills)
	}
	if !e2.IsResting("rest0b") {
		t.Error("skipped id-0 order should still be resting")
	}
}

// TestSMPFullySelfCrossingBookProducesNoFills: when every order shares one id — which
// is exactly what pass 1 looked like BEFORE this work, since its single task gave every
// order one identity — a correct engine produces no fills at all. That is the state
// that made pass 1 unscoreable: the reference book matched anyway and flagged every
// fill as a self-trade, so a correct engine scored 0 and an engine that refused to
// trade scored 1.0.
func TestSMPFullySelfCrossingBookProducesNoFills(t *testing.T) {
	e := NewEngine()
	for i, id := range []string{"a", "b", "c"} {
		e.Process(smpLimit(id, model.Sell, int64(100+i), 10, 7))
	}
	e.DrainFills()
	e.Process(smpLimit("agg", model.Buy, 200, 30, 7))
	if fills := e.DrainFills(); len(fills) != 0 {
		t.Errorf("all orders share an SMP id, so nothing may match; got %d fills", len(fills))
	}
	for _, id := range []string{"a", "b", "c"} {
		if !e.IsResting(id) {
			t.Errorf("order %q must remain resting", id)
		}
	}
}

// TestSMPAggressorContinuesPastSkippedToDeeperLevel: skipping must not stop the
// aggressor. After passing over its own order it continues through the level and on to
// worse prices, exactly as it would if the skipped order were not there.
func TestSMPAggressorContinuesPastSkippedToDeeperLevel(t *testing.T) {
	e := NewEngine()
	e.Process(smpLimit("mine100", model.Sell, 100, 10, 2)) // same id as taker
	e.Process(smpLimit("other101", model.Sell, 101, 10, 6))
	e.DrainFills()

	e.Process(smpLimit("taker", model.Buy, 101, 10, 2))
	fills := e.DrainFills()
	if len(fills) == 0 {
		t.Fatal("aggressor must continue to the next price level after skipping its own order")
	}
	for _, f := range fills {
		if f.OrderID == "mine100" {
			t.Errorf("matched own order at the better price: %+v", f)
		}
	}
	if !e.IsResting("mine100") {
		t.Error("skipped order at the better price must remain resting")
	}
}

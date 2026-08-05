// Package validate defines tests for v2 test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validate

import (
	"reflect"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

// ordWithFlow performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ordWithFlow(id string, kind model.Kind, side model.Side, price int64, qty uint64, flow model.Flow, seq uint32, t3 uint64, resp ...model.Response) *model.Order {
	return &model.Order{
		OrderID: id, Kind: kind, Side: side, Price: price, Qty: qty,
		Flow: flow, TCPSeq: seq, T3Ns: t3, Responses: resp,
	}
}

// replaceOrd performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func replaceOrd(id, orig string, side model.Side, price int64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, OrigOrderID: orig, Kind: model.Replace, Side: side, Price: price, Qty: qty, Responses: resp}
}

// TestTimePriorityViolationFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestTimePriorityViolationFlagged(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 10),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 20, fill(5, 100)),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 3, 30, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("expected 1 time-priority violation, got %+v", r)
	}
	// The report also carries S1's missed_fill — it is the victim of this very jump, and
	// under-reporting is itself a violation now — so assert on the CLASS being present
	// rather than on a position in the examples slice.
	if violationsOfType(r, Time) != 1 {
		t.Fatalf("expected a Time violation entry, got %+v", r.Violations)
	}
}

// TestTimePriorityCleanNotFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestTimePriorityCleanNotFlagged(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 10, fill(5, 100)),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 20),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 3, 30, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations != 0 {
		t.Fatalf("clean FIFO must not flag a time violation, got %+v", r)
	}
}

// TestSelfTradeViolationFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSelfTradeViolationFlagged(t *testing.T) {
	// Both sides carry the SAME SMP id, which is what makes this a self-trade. The
	// shared task id in the order ids is irrelevant now: keying on that was the bug
	// that made every pass-1 fill look like a self-trade, since pass 1 has one task.
	ordered := []*model.Order{
		orderSMP("sess_7_1_O", model.NewLimit, model.Sell, 100, 10, 4, fill(10, 100)),
		orderSMP("sess_7_2_O", model.NewLimit, model.Buy, 100, 10, 4, fill(10, 100)),
	}
	r := runStream(ordered, nil)
	if r.SelfTrades == 0 {
		t.Fatalf("expected a self-trade violation, got %+v", r)
	}
	foundSelf := false
	for _, v := range r.Violations {
		if v.Type == SelfTrade {
			foundSelf = true
		}
	}
	if !foundSelf {
		t.Fatalf("expected a SelfTrade violation entry, got %+v", r.Violations)
	}
}

// TestSelfTradeCleanNotFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSelfTradeCleanNotFlagged(t *testing.T) {
	ordered := []*model.Order{
		order("sess_7_1_O", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("sess_9_2_O", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}
	r := runStream(ordered, nil)
	if r.SelfTrades != 0 {
		t.Fatalf("distinct participants must not be a self-trade, got %+v", r)
	}
}

// TestCancelReplacePriorityLossFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCancelReplacePriorityLossFlagged(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("B_old", model.NewLimit, model.Buy, 100, 10, fa, 1, 10),
		ordWithFlow("B_other", model.NewLimit, model.Buy, 99, 10, fa, 2, 20),
		{OrderID: "B_old_R", OrigOrderID: "B_old", Kind: model.Replace, Side: model.Buy, Price: 99, Qty: 10,
			Flow: fa, TCPSeq: 3, T3Ns: 30, Responses: []model.Response{fill(10, 99)}},
		ordWithFlow("S1", model.NewLimit, model.Sell, 99, 10, fa, 4, 40, fill(10, 99)),
	}
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations == 0 {
		t.Fatalf("expected a cancel-replace priority-loss violation, got %+v", r)
	}
	found := false
	for _, v := range r.Violations {
		if v.Type == CancelReplaceLoss {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a CancelReplaceLoss violation entry, got %+v", r.Violations)
	}
}

// TestCancelReplaceQtyDecreaseKeepsPriority performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCancelReplaceQtyDecreaseKeepsPriority(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 10),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 20),
		replaceOrd("S1_R", "S1", model.Sell, 100, 5, fill(5, 100)),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 4, 40, fill(5, 100)),
	}
	ordered[2].Flow, ordered[2].TCPSeq, ordered[2].T3Ns = fa, 3, 30
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations != 0 || violationsOfType(r, CancelReplaceLoss) != 0 {
		t.Fatalf("qty-only decrease keeps priority; must not flag, got %+v", r)
	}
}

// TestCrossFlowTieSuppressesTimeViolation performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCrossFlowTieSuppressesTimeViolation(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 2, SrcPort: 2}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 1000),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fb, 1, 1050, fill(5, 100)),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 2, 2000, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations != 0 {
		t.Fatalf("Δ50ns cross-flow tie must suppress the time violation, got %+v", r)
	}
}

// TestCrossFlowBeyondToleranceFlagsTimeViolation performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCrossFlowBeyondToleranceFlagsTimeViolation(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 2, SrcPort: 2}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 1000),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fb, 1, 1150, fill(5, 100)),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 2, 2000, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("Δ150ns is beyond tolerance; the time violation must be flagged, got %+v", r)
	}
}

// TestSameFlowStrictNoTolerance performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSameFlowStrictNoTolerance(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 1000),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 1001, fill(5, 100)),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 3, 2000, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := runStream(ordered, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("same-flow ordering is strict; a 1ns gap must still flag, got %+v", r)
	}
}

// TestReportDeterministicFullSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestReportDeterministicFullSession(t *testing.T) {
	build := func() []*model.Order {
		fa := model.Flow{SrcIP: 1, SrcPort: 1}
		fb := model.Flow{SrcIP: 2, SrcPort: 2}
		o := []*model.Order{
			ordWithFlow("sess_1_1_O", model.NewLimit, model.Sell, 100, 10, fa, 1, 10, fill(10, 100)),
			ordWithFlow("sess_2_2_O", model.NewLimit, model.Buy, 100, 10, fb, 1, 20, fill(10, 100)),
			ordWithFlow("sess_1_3_O", model.NewLimit, model.Sell, 101, 5, fa, 2, 30),
			ordWithFlow("sess_3_4_O", model.NewLimit, model.Buy, 100, 5, fa, 3, 40, fill(5, 100)),
			ordWithFlow("sess_3_5_O", model.NewLimit, model.Buy, 200, 100, fb, 2, 50, fill(100, 200)),
		}
		return replay.Order(o)
	}
	r1 := runStream(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
	r2 := runStream(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("CorrectnessReport not deterministic:\n r1=%+v\n r2=%+v", r1, r2)
	}
}

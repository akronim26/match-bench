// Exact per-class counters vs the retained violation examples.
//
// Report.Violations keeps at most maxViolationExamplesPerType (100) examples per class
// so a long adversarial run cannot grow it without bound. Anything that DERIVES a count
// by iterating that slice therefore saturates at 100 — which is what the store did for
// time_violations, self_trades and cancel_replace_loss. A real pass-1 run persisted
// time=100 and self=100: numbers that look like measurements and are actually the cap,
// hiding both the true magnitude and whether an SMP-correct engine was self-matching.
package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestExactCountersSurviveTheExampleCap drives one class well past the cap and asserts
// the counters keep counting while the examples stop being retained.
func TestExactCountersSurviveTheExampleCap(t *testing.T) {
	const n = maxViolationExamplesPerType + 250

	v := NewStreamValidator()
	// Each pair is a fill at a price the reference never produced for that order.
	for i := 0; i < n; i++ {
		id := "B" + itoaLocal(i)
		v.Apply(order("S"+itoaLocal(i), model.NewLimit, model.Sell, 100, 10))
		v.Apply(order(id, model.NewLimit, model.Buy, 100, 10, fill(10, 101)))
	}
	r := v.Finish()

	if r.PriceViolations != uint64(n) {
		t.Fatalf("PriceViolations = %d, want %d — the exact counter must not saturate", r.PriceViolations, n)
	}
	if got := violationsOfType(r, Price); got != maxViolationExamplesPerType {
		t.Fatalf("retained Price examples = %d, want the cap %d", got, maxViolationExamplesPerType)
	}
	if r.ViolationCount() < uint64(n) {
		t.Fatalf("ViolationCount() = %d, want >= %d — it must count every add(), not the examples", r.ViolationCount(), n)
	}
}

// TestCancelReplaceLossHasItsOwnExactCounter: TimeViolations covers both ordering
// classes, so the split used to be recoverable only by counting capped examples. Past
// 100 of either class that split silently became a lie.
func TestCancelReplaceLossHasItsOwnExactCounter(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	// A repriced order filled ahead of an order already resting at the new level.
	ordered := []*model.Order{
		ordWithFlow("B_old", model.NewLimit, model.Buy, 100, 10, fa, 1, 10),
		ordWithFlow("B_other", model.NewLimit, model.Buy, 99, 10, fa, 2, 20),
		{OrderID: "B_old_R", OrigOrderID: "B_old", Kind: model.Replace, Side: model.Buy, Price: 99, Qty: 10,
			Flow: fa, TCPSeq: 3, T3Ns: 30, Responses: []model.Response{fill(10, 99)}},
		ordWithFlow("S1", model.NewLimit, model.Sell, 99, 10, fa, 4, 40, fill(10, 99)),
	}
	r := runStream(ordered, nil)

	if r.CancelReplaceLosses != 1 {
		t.Fatalf("CancelReplaceLosses = %d, want 1: %+v", r.CancelReplaceLosses, r)
	}
	if r.TimeViolations != 1 {
		t.Fatalf("TimeViolations = %d, want 1 (it covers both ordering classes)", r.TimeViolations)
	}
	// This is the subtraction the store persists as time_violations.
	if timeOnly := r.TimeViolations - r.CancelReplaceLosses; timeOnly != 0 {
		t.Fatalf("Time-only = %d, want 0 — this jump was a cancel-replace loss", timeOnly)
	}
}

// TestPlainQueueJumpIsNotCountedAsCancelReplaceLoss guards the other direction.
func TestPlainQueueJumpIsNotCountedAsCancelReplaceLoss(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("S2", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10),
	}, nil)

	if r.CancelReplaceLosses != 0 {
		t.Fatalf("CancelReplaceLosses = %d, want 0 — nothing was repriced", r.CancelReplaceLosses)
	}
	if timeOnly := r.TimeViolations - r.CancelReplaceLosses; timeOnly != 1 {
		t.Fatalf("Time-only = %d, want 1", timeOnly)
	}
}

// Scenario coverage for the streaming validator. These were the stream/batch
// equivalence tests: each drove the same fixture through validate.Run and
// StreamValidator and asserted the two scored identically. With the batch path deleted
// there is nothing to compare against, so each now asserts the outcome DIRECTLY — which
// is what the equivalence assertion was standing in for anyway.
package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// runStream drives the StreamValidator with already-ordered orders (as the streaming
// source's reorderer delivers them) plus phantoms, and returns the report.
func runStream(ordered []*model.Order, phantoms []ReportedFill) Report {
	v := NewStreamValidator()
	for _, o := range ordered {
		v.Apply(o)
	}
	for _, pf := range phantoms {
		v.AddPhantom(pf)
	}
	return v.Finish()
}

func TestStreamCleanCross(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}, nil)
	if r.TotalFills != 2 || r.ValidFills != 2 {
		t.Fatalf("expected 2/2 valid, got %+v", r)
	}
	if r.CorrectnessScore() != 1.0 || len(r.Violations) != 0 {
		t.Fatalf("expected score 1.0 no violations, got %+v", r)
	}
	if r.ScoredFills != r.TotalFills {
		t.Fatalf("ScoredFills = %d, want TotalFills = %d", r.ScoredFills, r.TotalFills)
	}
}

func TestStreamOverfill(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 20),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(6, 100), fill(6, 100)),
	}, nil)
	if r.Overfills != 1 {
		t.Fatalf("expected 1 overfill, got %+v", r)
	}
	if r.TotalFills != 2 || r.ValidFills != 1 {
		t.Fatalf("expected 2 total / 1 valid, got %+v", r)
	}
	if violationsOfType(r, Overfill) != 1 {
		t.Fatalf("expected an Overfill violation entry, got %+v", r.Violations)
	}
}

func TestStreamWrongPrice(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
	}, nil)
	if r.PriceViolations != 1 || r.ValidFills != 0 {
		t.Fatalf("expected 1 price violation / 0 valid, got %+v", r)
	}
}

func TestStreamFillBeyondReferenceQty(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 20, fill(15, 100)),
	}, nil)
	if r.PriceViolations != 1 || r.ValidFills != 0 {
		t.Fatalf("expected reported-beyond-reference flagged, got %+v", r)
	}
}

// TestStreamPhantomIsCountedButNotAViolation: a fill reported for an order_id that was
// never sent is NOT a violation class. It fails the orders.sent join upstream and never
// reaches the scored order set, so flagging it was double-counting the join. It stays
// visible only as the unmatched_responses counter.
func TestStreamPhantomIsCountedButNotAViolation(t *testing.T) {
	r := runStream(nil, []ReportedFill{{OrderID: "ghost", Qty: 5, Price: 100}})
	if r.PhantomFills != 1 {
		t.Fatalf("phantom must still be COUNTED, got %+v", r)
	}
	if r.TotalFills != 0 {
		t.Fatalf("a phantom must not enter the real fill set, got TotalFills=%d", r.TotalFills)
	}
	if len(r.Violations) != 0 {
		t.Fatalf("phantom must raise no violation, got %v", r.Violations)
	}
	if r.DirtyOrders != 0 {
		t.Fatalf("phantom must not dirty any order, got %d", r.DirtyOrders)
	}
	// The old stream code set ScoredFills = TotalFills - PhantomFills on the false
	// premise that phantoms were counted into TotalFills. On this fixture that
	// underflowed uint64 to 2^64-1.
	if r.ScoredFills != 0 {
		t.Fatalf("ScoredFills = %d, want 0 — phantoms are never netted out of a fill count", r.ScoredFills)
	}
}

// Time-priority: S1 and S2 both rest at 100; B1 crosses and the reference fills S1
// (FIFO). The contestant instead reports S2 filled — a queue jump.
func TestStreamTimePriority(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("S2", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10),
	}, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("expected the queue jump flagged as a time violation, got %+v", r)
	}
	if violationsOfType(r, Time) != 1 {
		t.Fatalf("expected a Time violation entry, got %+v", r.Violations)
	}
}

// Cancel + replace churn: orders leave the book mid-session (exercises eviction +
// finalize-on-exit + Forget), with a phantom for good measure. C1 cancels S1.
func TestStreamChurnMixed(t *testing.T) {
	c1 := order("C1", model.Cancel, model.Sell, 0, 0)
	c1.OrigOrderID = "S1"
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 101, 10),
		order("S2", model.NewLimit, model.Sell, 100, 20),
		c1,
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
		order("B2", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
		order("S3", model.NewLimit, model.Sell, 100, 5),
	}, []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})

	// B1 and B2 each took 10 from S2's 20 at 100 — both legitimate.
	if r.ValidFills != 2 || r.PriceViolations != 0 || r.Overfills != 0 {
		t.Fatalf("both buys are legitimate fills against S2, got %+v", r)
	}
	// S2 was filled for 20 by the reference and reported nothing: a missed fill.
	if r.MissedFills != 1 {
		t.Fatalf("MissedFills = %d, want 1 (S2 reported none of its 20): %+v", r.MissedFills, r)
	}
	if r.PhantomFills != 1 {
		t.Fatalf("PhantomFills = %d, want 1", r.PhantomFills)
	}
}

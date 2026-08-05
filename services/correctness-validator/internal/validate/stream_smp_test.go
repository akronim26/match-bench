// Tests for the checks that only the streaming validator implements, now that the
// batch path is gone: self-match detection against the reference book's RESTING state,
// under-reporting as a violation, and a deterministic report.
package validate

import (
	"reflect"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestStreamSelfMatchAgainstRestingBookFlagged is the only way a self-match can be
// detected at all. The reference book applies skip-and-continue, so for a genuine
// self-match it produces NO trade — there is nothing in DrainTrades to compare against,
// and the trade-derived check can never fire. The resting state is the only evidence.
func TestStreamSelfMatchAgainstRestingBookFlagged(t *testing.T) {
	// Both sides carry SMP id 4. The reference skips the match and leaves S1 resting;
	// the contestant reports a fill against it anyway.
	r := runStream([]*model.Order{
		orderSMP("S1", model.NewLimit, model.Sell, 100, 10, 4),
		orderSMP("B1", model.NewLimit, model.Buy, 100, 10, 4, fill(10, 100)),
	}, nil)

	if r.SelfTrades != 1 {
		t.Fatalf("SelfTrades = %d, want 1 — a fill against a same-SMP resting order is a self-trade, got %+v", r.SelfTrades, r)
	}
	if violationsOfType(r, SelfTrade) != 1 {
		t.Fatalf("want exactly one SelfTrade violation entry, got %+v", r.Violations)
	}
	// It must NOT be reported as a generic price violation: that says nothing about the
	// rule the contestant actually broke.
	if r.PriceViolations != 0 {
		t.Fatalf("PriceViolations = %d, want 0 (a self-match must not surface as a price violation): %+v", r.PriceViolations, r)
	}
}

// TestStreamDistinctSMPIDsAreNotSelfTrades: the whole point of rotating SMP ids across
// one connection is that different ids match normally.
func TestStreamDistinctSMPIDsAreNotSelfTrades(t *testing.T) {
	r := runStream([]*model.Order{
		orderSMP("S1", model.NewLimit, model.Sell, 100, 10, 4, fill(10, 100)),
		orderSMP("B1", model.NewLimit, model.Buy, 100, 10, 5, fill(10, 100)),
	}, nil)

	if r.SelfTrades != 0 {
		t.Fatalf("distinct SMP ids must match normally, got %+v", r)
	}
	if r.ValidFills != 2 || r.CorrectnessScore() != 1.0 {
		t.Fatalf("want 2 valid fills and score 1.0, got %+v score=%v", r, r.CorrectnessScore())
	}
}

// TestStreamIDLessOrdersMatchNormally: absent (SMPIDNone) means unconstrained, so
// pass-2 traffic — which carries no id at all — must be unaffected by the SMP check.
func TestStreamIDLessOrdersMatchNormally(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}, nil)

	if r.SelfTrades != 0 || r.ValidFills != 2 {
		t.Fatalf("id-less orders must match normally: %+v", r)
	}
}

// TestStreamUnderReportIsAMissedFill reverses TestUnderReportIsValid, which asserted the
// opposite and was deleted with the batch path. If the reference matched an order, the
// contestant owes an execution report for it; silence is a dropped fill. Both S1 (which
// reported nothing at all) and B1 (which reported 5 of 10) are under-reporters here.
func TestStreamUnderReportIsAMissedFill(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(5, 100)),
	}, nil)

	if r.MissedFills != 2 {
		t.Fatalf("MissedFills = %d, want 2 (S1 reported nothing, B1 reported 5 of 10): %+v", r.MissedFills, r)
	}
	if r.DirtyOrders != 2 || r.CorrectnessScore() != 0.0 {
		t.Fatalf("both orders must be dirty (score 0), got dirty=%d score=%v: %+v", r.DirtyOrders, r.CorrectnessScore(), r)
	}
}

// TestStreamAckerScoresZero is the case the MissedFill check exists for: an engine that
// ACKs every order and matches nothing. Every order the reference filled is a MissedFill,
// so every one of them is dirty. Without this check such an engine scored a clean 1.0.
func TestStreamAckerScoresZero(t *testing.T) {
	// Ten crossing pairs, all acked (ExecType "0"), none filled.
	var ordered []*model.Order
	for i := 0; i < 10; i++ {
		ordered = append(ordered,
			order(seqID("S", i), model.NewLimit, model.Sell, 100, 10, ackOnly()),
			order(seqID("B", i), model.NewLimit, model.Buy, 100, 10, ackOnly()),
		)
	}
	r := runStream(ordered, nil)

	if r.TotalFills != 0 {
		t.Fatalf("an acker reports no fills, got TotalFills=%d", r.TotalFills)
	}
	if r.MissedFills != 20 {
		t.Fatalf("MissedFills = %d, want 20 — every order the reference matched: %+v", r.MissedFills, r)
	}
	if got := r.CorrectnessScore(); got != 0.0 {
		t.Fatalf("an acker must score 0, got %v (%+v)", got, r)
	}
}

// TestStreamCleanEngineIsUnaffectedByTheMissedFillCheck guards the other direction: the
// check must not fire on an engine that reports every fill the reference produced.
func TestStreamCleanEngineIsUnaffectedByTheMissedFillCheck(t *testing.T) {
	r := runStream([]*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}, nil)

	if r.MissedFills != 0 || r.DirtyOrders != 0 {
		t.Fatalf("a complete report must raise no MissedFill: %+v", r)
	}
	if r.CorrectnessScore() != 1.0 {
		t.Fatalf("score = %v, want 1.0: %+v", r.CorrectnessScore(), r)
	}
}

// TestStreamReportIsDeterministic pins report stability. Finish() finalizes whatever is
// still resting at session end, and the pending set is a map — iterating it directly
// makes the Violations example slice come out in a different order run to run, on
// identical input. The stored violation examples must not depend on map iteration order.
func TestStreamReportIsDeterministic(t *testing.T) {
	build := func() []*model.Order {
		return []*model.Order{
			order("S1", model.NewLimit, model.Sell, 100, 20),
			order("S2", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
			order("S3", model.NewLimit, model.Sell, 101, 10),
			order("B1", model.NewLimit, model.Buy, 100, 10, fill(6, 100), fill(6, 100)),
			order("B2", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
		}
	}
	for i := 0; i < 8; i++ {
		r1 := runStream(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
		r2 := runStream(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
		if !reflect.DeepEqual(r1, r2) {
			t.Fatalf("streaming report not deterministic on run %d:\n r1=%+v\n r2=%+v", i, r1, r2)
		}
	}
}

// ackOnly is an execution report that acknowledges the order without filling it.
func ackOnly() model.Response {
	return model.Response{ExecType: "0"}
}

// seqID builds a distinct order id per index without pulling in strconv.
func seqID(prefix string, i int) string {
	return prefix + string(rune('a'+i%26))
}

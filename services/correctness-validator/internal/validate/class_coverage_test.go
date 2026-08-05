// Violation-class reachability. Two classes (Time, CancelReplaceLoss) were detectable
// only by the deleted batch validator and could NEVER fire on the live streaming path,
// and SelfTrade's streaming check was dead code the moment the reference book started
// applying skip-and-continue. None of that was caught, because the tests compared the
// two implementations to each other rather than asserting any class actually fires.
//
// This file closes that hole: every violation class full mode is responsible for gets a
// fixture that provokes it, and the set of classes those fixtures produce is compared
// against the declared expectation. A class that becomes unreachable fails here, and so
// does a class added without being wired up.
package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

// fullModeClasses is every ViolationType StreamValidator (VALIDATOR_MODE=full) is
// responsible for emitting — which is all of them. LostOrder/LostCancel arrive by a
// different route than the rest (StreamSession reports them through AddLost, because an
// unanswered order has no ingress timestamp and cannot be replayed), so they are asserted
// separately below rather than through classFixtures.
var fullModeClasses = []ViolationType{
	Overfill,
	MissedFill,
	Price,
	Time,
	SelfTrade,
	CancelReplaceLoss,
}

// fullModeLostClasses is the AddLost half of full mode's responsibility.
var fullModeLostClasses = map[ViolationType]model.Kind{
	LostOrder:  model.NewLimit,
	LostCancel: model.Cancel,
}

// classFixtures provokes each class. Every fixture is ordered as the streaming source
// would deliver it.
var classFixtures = map[ViolationType]func() []*model.Order{
	// Reported cumulative fill exceeds the order's own quantity.
	Overfill: func() []*model.Order {
		return []*model.Order{
			order("S1", model.NewLimit, model.Sell, 100, 20),
			order("B1", model.NewLimit, model.Buy, 100, 10, fill(6, 100), fill(6, 100)),
		}
	},
	// The reference filled this order; the contestant reported less than all of it.
	MissedFill: func() []*model.Order {
		return []*model.Order{
			order("S1", model.NewLimit, model.Sell, 100, 10),
			order("B1", model.NewLimit, model.Buy, 100, 10, fill(5, 100)),
		}
	},
	// A fill at a price the reference never produced for this order.
	Price: func() []*model.Order {
		return []*model.Order{
			order("S1", model.NewLimit, model.Sell, 100, 10),
			order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
		}
	},
	// S2 filled while S1, resting ahead of it at the same price, went unreported.
	Time: func() []*model.Order {
		return []*model.Order{
			order("S1", model.NewLimit, model.Sell, 100, 10),
			order("S2", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
			order("B1", model.NewLimit, model.Buy, 100, 10),
		}
	},
	// A repriced order filled ahead of an order already resting at the new level.
	CancelReplaceLoss: func() []*model.Order {
		fa := model.Flow{SrcIP: 1, SrcPort: 1}
		o := []*model.Order{
			ordWithFlow("B_old", model.NewLimit, model.Buy, 100, 10, fa, 1, 10),
			ordWithFlow("B_other", model.NewLimit, model.Buy, 99, 10, fa, 2, 20),
			{OrderID: "B_old_R", OrigOrderID: "B_old", Kind: model.Replace, Side: model.Buy, Price: 99, Qty: 10,
				Flow: fa, TCPSeq: 3, T3Ns: 30, Responses: []model.Response{fill(10, 99)}},
			ordWithFlow("S1", model.NewLimit, model.Sell, 99, 10, fa, 4, 40, fill(10, 99)),
		}
		return replay.Order(o)
	},
	// A fill against a resting order carrying the same self-match-prevention id.
	SelfTrade: func() []*model.Order {
		return []*model.Order{
			orderSMP("S1", model.NewLimit, model.Sell, 100, 10, 4),
			orderSMP("B1", model.NewLimit, model.Buy, 100, 10, 4, fill(10, 100)),
		}
	},
}

// TestEveryFullModeClassIsReachable asserts each class fires on its own fixture.
func TestEveryFullModeClassIsReachable(t *testing.T) {
	for _, class := range fullModeClasses {
		build, ok := classFixtures[class]
		if !ok {
			t.Errorf("%s has no fixture — every class full mode emits must be provoked here", class)
			continue
		}
		r := runStream(build(), nil)
		if violationsOfType(r, class) == 0 {
			t.Errorf("%s is UNREACHABLE in the streaming validator: fixture produced %+v", class, r.Violations)
		}
	}
}

// TestEveryLostClassIsReachableInFullMode covers the AddLost route, and asserts the
// three properties the other classes are held to: it fires, its counter moves, and it
// drops the score.
func TestEveryLostClassIsReachableInFullMode(t *testing.T) {
	for class, kind := range fullModeLostClasses {
		v := NewStreamValidator()
		v.Apply(order("CLEAN", model.NewLimit, model.Sell, 100, 10))
		v.AddLost("DROPPED", kind)
		r := v.Finish()

		if violationsOfType(r, class) == 0 {
			t.Errorf("%s is UNREACHABLE in full mode: %+v", class, r.Violations)
		}
		if r.LostOrders+r.LostCancels == 0 {
			t.Errorf("%s fired but neither lost counter moved: %+v", class, r)
		}
		if r.DirtyOrders == 0 || r.CorrectnessScore() >= 1.0 {
			t.Errorf("%s fired but did not affect the score (dirty=%d score=%v)", class, r.DirtyOrders, r.CorrectnessScore())
		}
	}
}

// TestEveryFullModeClassIsCounted: the retained Violation example and the exact counter
// are separate mechanisms (examples are capped at 100 per type, counters never are), so a
// class can be exampled without being counted. Both must move together, or the stored
// violation_count and the leaderboard disagree with the examples shown to a contestant.
func TestEveryFullModeClassIsCounted(t *testing.T) {
	counterOf := map[ViolationType]func(Report) uint64{
		Overfill:          func(r Report) uint64 { return r.Overfills },
		MissedFill:        func(r Report) uint64 { return r.MissedFills },
		Price:             func(r Report) uint64 { return r.PriceViolations },
		Time:              func(r Report) uint64 { return r.TimeViolations },
		CancelReplaceLoss: func(r Report) uint64 { return r.TimeViolations }, // shares the ordering counter
		SelfTrade:         func(r Report) uint64 { return r.SelfTrades },
	}
	for _, class := range fullModeClasses {
		r := runStream(classFixtures[class](), nil)
		if got := counterOf[class](r); got == 0 {
			t.Errorf("%s fired but its exact counter stayed 0: %+v", class, r)
		}
		if r.ViolationCount() == 0 {
			t.Errorf("%s fired but ViolationCount() is 0: %+v", class, r)
		}
	}
}

// TestEveryFullModeClassDirtiesItsOrder is what actually makes a class matter. The score
// is order-level: a class that is detected, counted, and stored but never marks its order
// dirty changes nothing about the contestant's score. That was the exact shape of the
// original defect — lost orders, lost cancels and both Time classes were all detected and
// invisible to the score at once.
func TestEveryFullModeClassDirtiesItsOrder(t *testing.T) {
	for _, class := range fullModeClasses {
		r := runStream(classFixtures[class](), nil)
		if r.DirtyOrders == 0 {
			t.Errorf("%s fired but dirtied no order, so it cannot affect the score: %+v", class, r)
		}
		if r.CorrectnessScore() >= 1.0 {
			t.Errorf("%s fired but the session still scored %v", class, r.CorrectnessScore())
		}
	}
}

// TestPriceBranchesAreAllReachable: Price has three distinct causes and they are not
// interchangeable — the first two are ordinary fabrications, the third is a
// quantity overrun that the queue-jump check gets first refusal on. A refactor that
// collapses or shadows one would silently stop reporting a real failure mode.
func TestPriceBranchesAreAllReachable(t *testing.T) {
	cases := []struct {
		name   string
		detail string
		orders []*model.Order
	}{
		{
			name:   "reference produced no fill for this order",
			detail: "reference engine produced no fill for this order",
			// B1 crosses nothing: the only ask is at 101, and B1 bids 100.
			orders: []*model.Order{
				order("S1", model.NewLimit, model.Sell, 101, 10),
				order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
			},
		},
		{
			name:   "price not produced for this order",
			detail: "reported fill price not produced by the reference engine for this order",
			orders: []*model.Order{
				order("S1", model.NewLimit, model.Sell, 100, 10),
				order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
			},
		},
		{
			name:   "cumulative exceeds reference qty",
			detail: "cumulative reported 15 exceeds reference fill qty 10",
			orders: []*model.Order{
				order("S1", model.NewLimit, model.Sell, 100, 10),
				order("B1", model.NewLimit, model.Buy, 100, 20, fill(15, 100)),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := runStream(c.orders, nil)
			found := false
			for _, v := range r.Violations {
				if v.Type == Price && v.Detail == c.detail {
					found = true
				}
			}
			if !found {
				t.Fatalf("no Price violation with detail %q; got %+v", c.detail, r.Violations)
			}
		})
	}
}

// TestInvariantsModeClassesAreReachable does the same for pass 2. Its class set is
// deliberately different: it has no reference book, so it cannot speak to Price,
// SelfTrade or MissedFill, and it owns the two classes full mode cannot see — an order or
// a cancel that got no response at all.
func TestInvariantsModeClassesAreReachable(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}

	lost := NewInvariantsValidator(500)
	lost.Apply(ordWithFlow("O1", model.NewLimit, model.Buy, 100, 10, fa, 1, 10))
	lostRep := lost.Finish()
	if violationsOfType(lostRep, LostOrder) == 0 || lostRep.LostOrders == 0 {
		t.Errorf("LostOrder unreachable in invariants mode: %+v", lostRep)
	}

	cancel := NewInvariantsValidator(500)
	c := ordWithFlow("C1", model.Cancel, model.Buy, 0, 0, fa, 1, 10)
	c.OrigOrderID = "O1"
	cancel.Apply(c)
	cancelRep := cancel.Finish()
	if violationsOfType(cancelRep, LostCancel) == 0 || cancelRep.LostCancels == 0 {
		t.Errorf("LostCancel unreachable in invariants mode: %+v", cancelRep)
	}

	over := NewInvariantsValidator(500)
	over.Apply(ordWithFlow("O2", model.NewLimit, model.Buy, 100, 10, fa, 1, 10,
		model.Response{ExecType: "2", FillQty: 11, FillPrice: 100, T7Ns: 20}))
	overRep := over.Finish()
	if violationsOfType(overRep, Overfill) == 0 || overRep.Overfills == 0 {
		t.Errorf("Overfill unreachable in invariants mode: %+v", overRep)
	}

	// Per-flow FIFO breach: O4 (later TCPSeq) is answered before O3.
	fifo := NewInvariantsValidator(500)
	fifo.Apply(ordWithFlow("O3", model.NewLimit, model.Buy, 100, 10, fa, 1, 10,
		model.Response{ExecType: "0", T7Ns: 200}))
	fifo.Apply(ordWithFlow("O4", model.NewLimit, model.Buy, 100, 10, fa, 2, 20,
		model.Response{ExecType: "0", T7Ns: 100}))
	fifoRep := fifo.Finish()
	if violationsOfType(fifoRep, Time) == 0 || fifoRep.TimeViolations == 0 {
		t.Errorf("Time (per-flow FIFO) unreachable in invariants mode: %+v", fifoRep)
	}
}

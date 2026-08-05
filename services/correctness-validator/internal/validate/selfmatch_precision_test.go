// Precision of the self-match check.
//
// Pass 1 rotates 8 SMP ids across one connection, so a busy price level routinely holds
// orders from several ids at once — including the aggressor's own. "Some order sharing
// my id rests at this price" is therefore the NORMAL state of the book, not evidence of
// anything. The check has to distinguish that from a fill that could only have come from
// a same-id maker.
package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestFillAgainstAnotherParticipantIsValidEvenWhenOwnOrderRestsAtThatPrice is the shape
// pass-1 traffic produces constantly: the aggressor's own id is resting at the level, the
// reference skips it (skip-and-continue) and matches the NEXT maker, and the contestant
// reports exactly that fill. It is a correct fill and must score as one.
func TestFillAgainstAnotherParticipantIsValidEvenWhenOwnOrderRestsAtThatPrice(t *testing.T) {
	r := runStream([]*model.Order{
		// Same id as the aggressor: the reference skips it and it stays resting.
		orderSMP("S_same", model.NewLimit, model.Sell, 100, 10, 4),
		// Different id: this is what the reference actually matches.
		orderSMP("S_other", model.NewLimit, model.Sell, 100, 10, 5, fill(10, 100)),
		orderSMP("B1", model.NewLimit, model.Buy, 100, 10, 4, fill(10, 100)),
	}, nil)

	if r.SelfTrades != 0 {
		t.Fatalf("SelfTrades = %d, want 0 — the fill came from id 5, id 4 merely also rests at 100: %+v",
			r.SelfTrades, r.Violations)
	}
	if r.ValidFills != 2 {
		t.Fatalf("ValidFills = %d, want 2 (both sides of a legitimate cross): %+v", r.ValidFills, r)
	}
	if r.CorrectnessScore() != 1.0 {
		t.Fatalf("score = %v, want 1.0: %+v", r.CorrectnessScore(), r)
	}
}

// TestSelfMatchStillFlaggedWhenTheReferenceProducedNoFill keeps the other direction: a
// fill the reference never produced, against a level where only the aggressor's own id
// rests, is a genuine self-match and must still be reported as one rather than as a
// generic price violation.
func TestSelfMatchStillFlaggedWhenTheReferenceProducedNoFill(t *testing.T) {
	r := runStream([]*model.Order{
		orderSMP("S1", model.NewLimit, model.Sell, 100, 10, 4),
		orderSMP("B1", model.NewLimit, model.Buy, 100, 10, 4, fill(10, 100)),
	}, nil)

	if r.SelfTrades != 1 {
		t.Fatalf("SelfTrades = %d, want 1: %+v", r.SelfTrades, r.Violations)
	}
	if r.PriceViolations != 0 {
		t.Fatalf("PriceViolations = %d, want 0 — a self-match must be reported as itself", r.PriceViolations)
	}
}

// TestOverfillBeatsSelfMatch: reporting more than the order's own quantity is a hard
// arithmetic violation and must not be reclassified by anything.
func TestOverfillBeatsSelfMatch(t *testing.T) {
	r := runStream([]*model.Order{
		orderSMP("S1", model.NewLimit, model.Sell, 100, 20, 4),
		orderSMP("B1", model.NewLimit, model.Buy, 100, 10, 4, fill(6, 100), fill(6, 100)),
	}, nil)

	if r.Overfills != 1 {
		t.Fatalf("Overfills = %d, want 1: %+v", r.Overfills, r)
	}
}

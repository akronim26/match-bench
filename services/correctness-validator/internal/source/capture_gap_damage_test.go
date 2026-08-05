// What a capture gap costs beyond the order it loses.
//
// An order whose responses the capture never published is excluded from scoring — the
// contestant is not billed for the platform's loss. But the order itself is also never
// fed to the reference book, while the CONTESTANT did process it (it answered; the bot
// recorded the response). From that point the reference book is missing liquidity the
// contestant has, and every later fill against that liquidity looks to the validator like
// a fill the reference never produced.
//
// So one lost order does not cost one order's grading. It costs that order plus every
// subsequent fill that touches the resting liquidity it should have contributed.
package source

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/pipeline"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
)

func sentEvt(id, side string, price, qty uint64) topics.OrderSentEvent {
	return topics.OrderSentEvent{
		SessionID: "S", OrderID: id, Side: side, PayloadType: "NEW", OrdType: "LIMIT",
		Price: price, Qty: qty, SMPID: topics.SMPIDNone,
	}
}

func ackEvtFor(id string, port uint16, seq uint32, t3 uint64, exec string, fq, fp uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: "S", ContestantID: "c", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: port, TCPSeq: seq,
		T3XDPIngressNS: t3, T7XDPEgressNS: t3 + 500,
		ExecType: exec, FillQty: fq, FillPrice: fp,
	}
}

// TestCaptureGapPoisonsLaterOrders: three sell orders rest at 100 and three buyers cross
// them. The capture loses every response for the FIRST maker, so the reference book never
// sees it — yet the contestant filled against it and reports that fill honestly.
//
// The maker itself is correctly excluded (capture gap, not the contestant's fault). The
// BUYER, whose responses were captured perfectly and whose behaviour is correct, is
// scored against a book that is missing the liquidity it traded with.
func TestCaptureGapPoisonsLaterOrders(t *testing.T) {
	fp := uint64(100 * topics.TelemetryPriceScale)

	build := func(loseMakerAcks bool) validate.Report {
		v := validate.NewStreamValidator()

		// Maker S1: the contestant answered it, but the capture published nothing.
		s1 := pipeline.AssembleOrder(sentEvt("S1", "SELL", 100, 10),
			[]topics.OrderAckedEvent{ackEvtFor("S1", 5, 1, 10, "2", 10, fp)})
		if loseMakerAcks {
			// The join's outcomeCaptureGap path: excluded from scoring, and — the point
			// of this test — never applied to the reference book either.
			v.AddCaptureGap()
		} else {
			v.Apply(s1)
		}

		// Buyer B1 crosses S1 and honestly reports the fill it received.
		v.Apply(pipeline.AssembleOrder(sentEvt("B1", "BUY", 100, 10),
			[]topics.OrderAckedEvent{ackEvtFor("B1", 5, 2, 20, "2", 10, fp)}))
		return v.Finish()
	}

	healthy := build(false)
	if healthy.PriceViolations != 0 || healthy.CorrectnessScore() != 1.0 {
		t.Fatalf("baseline must be clean: %+v score=%v", healthy, healthy.CorrectnessScore())
	}

	// CHARACTERIZATION: this asserts what the validator does TODAY, which is wrong, so
	// that the cost is measured rather than assumed and the day it is fixed this test
	// fails and gets updated deliberately.
	//
	// The buyer did nothing wrong and its own responses were captured perfectly, yet it
	// is scored against a book missing the maker it traded with. Measured on a real
	// pass-1 run: 3,718 capture gaps alongside 20,249 price violations, ~5.4 collateral
	// violations per lost order — a correct engine cannot score 1.0 while any capture
	// loss remains, because the loss is charged to whoever traded with the lost order.
	//
	// The fix is to apply a capture-gap order to the reference book for its STATE while
	// still excluding it from grading: the sent event carries side, price, qty, kind and
	// SMP id, which is everything the book needs. What it lacks is T3 (captured from the
	// response), so it would have to be sequenced by the bot's SendTSNS instead — exact
	// for pass 1, which is one connection, and a few ms of NTP skew away from the T3
	// domain for multi-flow pass 2. That trade-off is why this is recorded rather than
	// silently patched.
	damaged := build(true)
	if damaged.PriceViolations != 1 {
		t.Fatalf("PriceViolations = %d, want 1 — the collateral damage of one capture gap: %+v",
			damaged.PriceViolations, damaged.Violations)
	}
	if damaged.CaptureGaps != 1 {
		t.Fatalf("CaptureGaps = %d, want 1", damaged.CaptureGaps)
	}
	// The gap order is excluded from scoring, so the only order graded is the innocent
	// buyer — and it scores 0.
	if damaged.ScoredOrders != 1 || damaged.DirtyOrders != 1 {
		t.Fatalf("scored=%d dirty=%d, want 1/1 (only the buyer is graded, and it is blamed)",
			damaged.ScoredOrders, damaged.DirtyOrders)
	}
	if !damaged.Tainted {
		t.Error("a 50% capture-gap rate must taint the result")
	}
}

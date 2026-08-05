// Package pipeline defines tests for the sent+acked join.
//
// These tests used to drive pipeline.Run — the batch entry point (Assemble → replay.Order
// → validate.Run), deleted along with the rest of the batch path. What remains of this
// package on the live path is AssembleOrder, which the streaming source calls per order,
// so the join is exercised directly and anything scoring-related runs through the
// StreamValidator that production actually uses.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package pipeline

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
)

// sent performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sent(id, side, payload, ordType string, price, qty uint64) topics.OrderSentEvent {
	return topics.OrderSentEvent{
		SessionID: "S", OrderID: id, Side: side, PayloadType: payload, OrdType: ordType,
		Price: price, Qty: qty,
		// Explicitly id-less. The Go zero value of SMPID is 0, which is a VALID
		// self-match-prevention id, so a synthetic event that omits this reads as
		// "participant 0" and self-matches against every other synthetic order.
		SMPID: topics.SMPIDNone,
	}
}

// acked performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func acked(id string, srcPort uint16, tcpSeq uint32, t3 uint64, exec string, fillQty, fillPrice uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: "S", ContestantID: "c1", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: srcPort, TCPSeq: tcpSeq,
		T3XDPIngressNS: t3, T7XDPEgressNS: t3 + 1000, PodServiceTimeNS: 1000,
		ExecType: exec, FillQty: fillQty, FillPrice: fillPrice,
	}
}

// TestAssembleOrderRequiresAnAck: an order that was sent but never answered is not a
// scoreable order here — it has no flow, no TCP seq and no responses to compare. The
// streaming source drops it the same way (it only assembles when acks exist), and
// invariants mode is what accounts for it, as a lost order.
func TestAssembleOrderRequiresAnAck(t *testing.T) {
	if o := AssembleOrder(sent("NOACK", "BUY", "NEW", "LIMIT", 100, 5), nil); o != nil {
		t.Fatalf("AssembleOrder with no acks = %+v, want nil", o)
	}
}

// TestAssembledCleanCrossScoresPerfect drives the assembled orders through the
// StreamValidator, so the join and the scoring agree on the same fixture end to end.
func TestAssembledCleanCrossScoresPerfect(t *testing.T) {
	fp := 100 * topics.TelemetryPriceScale
	s1 := AssembleOrder(sent("S1", "SELL", "NEW", "LIMIT", 100, 10),
		[]topics.OrderAckedEvent{acked("S1", 5, 1, 10, "2", 10, fp)})
	b1 := AssembleOrder(sent("B1", "BUY", "NEW", "LIMIT", 100, 10),
		[]topics.OrderAckedEvent{acked("B1", 5, 2, 20, "2", 10, fp)})
	if s1 == nil || b1 == nil {
		t.Fatalf("assembly returned nil: s1=%v b1=%v", s1, b1)
	}

	v := validate.NewStreamValidator()
	v.Apply(s1)
	v.Apply(b1)
	// A fill reported for an order_id that was never sent is a phantom: the
	// unmatched_responses metric, not a violation class.
	v.AddPhantom(validate.ReportedFill{OrderID: "ghost", Qty: 5, Price: int64(fp)})
	r := v.Finish()

	if r.TotalFills != 2 || r.ValidFills != 2 || r.PhantomFills != 1 {
		t.Fatalf("expected 2 real fills / 2 valid / 1 phantom counted separately, got %+v", r)
	}
	if got := r.CorrectnessScore(); got != 1.0 {
		t.Fatalf("expected score 1.0 (phantom excluded from scoring), got %v", got)
	}
}

// TestAssembledScaledPriceDomainIsValid pins C1: the sent event carries an unscaled
// price and AssembleOrder multiplies it by TelemetryPriceScale, while the ack's
// fill_price is already in the scaled domain. If those two disagree, every legitimate
// fill reads as a price violation.
func TestAssembledScaledPriceDomainIsValid(t *testing.T) {
	const tick = uint64(10_000)
	scaled := tick * topics.TelemetryPriceScale
	mk := AssembleOrder(sent("MK", "SELL", "NEW", "LIMIT", tick, 10),
		[]topics.OrderAckedEvent{acked("MK", 5, 1, 10, "2", 10, scaled)})
	tk := AssembleOrder(sent("TK", "BUY", "NEW", "LIMIT", tick, 10),
		[]topics.OrderAckedEvent{acked("TK", 5, 2, 20, "2", 10, scaled)})

	v := validate.NewStreamValidator()
	v.Apply(mk)
	v.Apply(tk)
	r := v.Finish()

	if r.TotalFills != 2 {
		t.Fatalf("expected 2 reported fills, got %d (%+v)", r.TotalFills, r)
	}
	if r.ValidFills != 2 || r.PriceViolations != 0 {
		t.Fatalf("C1: legitimate fills at the scaled fill_price domain must be VALID; got %d valid, %d price-violations (%+v)",
			r.ValidFills, r.PriceViolations, r)
	}
}

// TestAssemblePrefersSentOrigOrderID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAssemblePrefersSentOrigOrderID(t *testing.T) {
	s := topics.OrderSentEvent{
		SessionID: "S", OrderID: "C1", Side: "BUY", PayloadType: "CANCEL", OrdType: "LIMIT",
		OrigOrderID: "REAL-TARGET", SMPID: topics.SMPIDNone,
	}
	a := acked("C1", 7, 1, 10, "0", 0, 0)
	a.OrigOrderID = "CONTESTANT-LIES"
	o := AssembleOrder(s, []topics.OrderAckedEvent{a})
	if o == nil {
		t.Fatal("expected an assembled order")
	}
	if o.OrigOrderID != "REAL-TARGET" {
		t.Errorf("H13: OrigOrderID = %q, want bot-authoritative REAL-TARGET", o.OrigOrderID)
	}
}

// TestAssembleMapsKindAndFlow performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAssembleMapsKindAndFlow(t *testing.T) {
	m1 := AssembleOrder(sent("m1", "SELL", "NEW", "MARKET", 0, 7),
		[]topics.OrderAckedEvent{acked("m1", 9, 1, 100, "2", 7, 50)})
	if m1.Kind.String() != "NewMarket" {
		t.Errorf("m1 kind = %v, want NewMarket", m1.Kind)
	}
	if m1.Flow.SrcPort != 9 || m1.TCPSeq != 1 {
		t.Errorf("m1 flow/seq wrong: %+v", m1.Flow)
	}

	ca := acked("c1", 9, 2, 100, "0", 0, 0)
	ca.OrigOrderID = "orig-1"
	c1 := AssembleOrder(sent("c1", "BUY", "CANCEL", "LIMIT", 0, 0), []topics.OrderAckedEvent{ca})
	if c1.Kind.String() != "Cancel" || c1.OrigOrderID != "orig-1" {
		t.Errorf("c1 cancel/orig wrong: kind=%v orig=%q", c1.Kind, c1.OrigOrderID)
	}
}

// TestAssembleSMPIDPresence: only SMPIDNone means absent. Id 0 is a VALID id — treating
// it as absent would silently exempt 1 in every smp_id_count orders from self-match
// prevention.
func TestAssembleSMPIDPresence(t *testing.T) {
	withID := sent("A", "BUY", "NEW", "LIMIT", 100, 1)
	withID.SMPID = 0
	o := AssembleOrder(withID, []topics.OrderAckedEvent{acked("A", 5, 1, 10, "0", 0, 0)})
	if !o.HasSMPID || o.SMPID != 0 {
		t.Errorf("SMP id 0 must be honoured as a real id, got HasSMPID=%v SMPID=%d", o.HasSMPID, o.SMPID)
	}

	none := AssembleOrder(sent("B", "BUY", "NEW", "LIMIT", 100, 1),
		[]topics.OrderAckedEvent{acked("B", 5, 2, 10, "0", 0, 0)})
	if none.HasSMPID {
		t.Errorf("SMPIDNone must read as absent, got HasSMPID=true (SMPID=%d)", none.SMPID)
	}
	var _ *model.Order = none
}

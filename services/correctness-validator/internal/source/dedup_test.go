// Package source tests the join's duplicate-ack suppression.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package source

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/pipeline"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
)

func ackEvt(id, exec string, t7, fillQty, fillPrice uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: "S", ContestantID: "team-dedup", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: 5, TCPSeq: 1,
		T3XDPIngressNS: 1000, T7XDPEgressNS: t7,
		ExecType: exec, FillQty: fillQty, FillPrice: fillPrice,
	}
}

// TestPendingOrderDropsRedeliveredAcks: a producer retry republishes the whole batch, so
// the same execution report arrives twice. Near-duplicates (a different T7, exec type or
// fill) are distinct reports and must be kept.
func TestPendingOrderDropsRedeliveredAcks(t *testing.T) {
	fillAck := ackEvt("A", "2", 1000, 5, 100)
	otherT7 := ackEvt("A", "2", 2000, 5, 100)
	otherExec := ackEvt("A", "1", 1000, 5, 100)
	otherQty := ackEvt("A", "2", 1000, 6, 100)

	var po pendingOrder
	for _, c := range []struct {
		name string
		a    topics.OrderAckedEvent
		want bool
	}{
		{"first", fillAck, true},
		{"exact redelivery", fillAck, false},
		{"redelivery again", fillAck, false},
		{"different t7", otherT7, true},
		{"different exec type", otherExec, true},
		{"different fill qty", otherQty, true},
		{"redelivery of a near-duplicate", otherT7, false},
	} {
		if got := po.addAck(c.a); got != c.want {
			t.Errorf("%s: addAck = %v, want %v", c.name, got, c.want)
		}
	}
	if len(po.acks) != 4 {
		t.Fatalf("held %d acks, want 4 (dupes dropped, near-dupes kept): %+v", len(po.acks), po.acks)
	}
	// Arrival order must survive: the responses drive cumulative-fill comparison in
	// the order the contestant reported them.
	wantOrder := []topics.OrderAckedEvent{fillAck, otherT7, otherExec, otherQty}
	for i, a := range po.acks {
		if a != wantOrder[i] {
			t.Errorf("acks[%d] = %+v, want %+v", i, a, wantOrder[i])
		}
	}
}

// TestRedeliveredFillIsNotScoredTwice is why the dedup exists: counted twice, a single
// 10-lot fill on a 10-lot order reads as 20 reported against qty 10 — an overfill
// violation manufactured out of a broker retry.
func TestRedeliveredFillIsNotScoredTwice(t *testing.T) {
	sentEvt := func(id, side string, price, qty uint64) topics.OrderSentEvent {
		return topics.OrderSentEvent{
			SessionID: "S", OrderID: id, Side: side, PayloadType: "NEW", OrdType: "LIMIT",
			Price: price, Qty: qty, SMPID: topics.SMPIDNone,
		}
	}
	fp := uint64(100 * topics.TelemetryPriceScale)

	build := func(dupe bool) validate.Report {
		sPending := &pendingOrder{}
		bPending := &pendingOrder{}
		sPending.addAck(ackEvt("S1", "2", 1000, 10, fp))
		bAck := ackEvt("B1", "2", 1100, 10, fp)
		bPending.addAck(bAck)
		if dupe {
			bPending.addAck(bAck) // producer retry
		}
		v := validate.NewStreamValidator()
		v.Apply(pipeline.AssembleOrder(sentEvt("S1", "SELL", 100, 10), sPending.acks))
		v.Apply(pipeline.AssembleOrder(sentEvt("B1", "BUY", 100, 10), bPending.acks))
		return v.Finish()
	}

	clean := build(false)
	if clean.Overfills != 0 || clean.CorrectnessScore() != 1.0 {
		t.Fatalf("baseline must be clean, got %+v", clean)
	}
	withDupe := build(true)
	if withDupe.Overfills != 0 {
		t.Fatalf("a redelivered ack must not manufacture an overfill, got %+v", withDupe)
	}
	if withDupe.TotalFills != clean.TotalFills || withDupe.CorrectnessScore() != clean.CorrectnessScore() {
		t.Fatalf("redelivery changed the score: clean=%+v withDupe=%+v", clean, withDupe)
	}
}

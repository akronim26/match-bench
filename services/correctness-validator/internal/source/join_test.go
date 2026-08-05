// Package source tests how the sent+acked join classifies a pending order.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package source

import (
	"testing"

	"github.com/iicpc/schemas/topics"
)

// TestPendingOutcome pins the distinction that makes the lost-order check safe to score.
//
// "No orders.acked record" does NOT mean the contestant failed to answer. Measured on a
// real 1.84M-order pass-1 run, the reference engine answered 100% of orders (the bot
// recorded a response for every one, zero timeouts) while the eBPF capture published
// responses for only 64% of them — so scoring every unanswered-in-capture order as a
// dropped order billed the contestant for 668k of the platform's own losses.
//
// The bot is an independent witness and it already reports per order on the wire:
// TimedOut means the bot itself never got a response, RecvDoneTSNS != 0 means it did.
// Only the former is the contestant's fault.
func TestPendingOutcome(t *testing.T) {
	sentWith := func(timedOut bool, recvDone uint64) topics.OrderSentEvent {
		return topics.OrderSentEvent{OrderID: "A", TimedOut: timedOut, RecvDoneTSNS: recvDone}
	}
	cases := []struct {
		name string
		po   pendingOrder
		want pendingOutcome
	}{
		{
			name: "sent and answered",
			po: pendingOrder{
				hasSent: true, sent: sentWith(false, 500),
				acks: []topics.OrderAckedEvent{{OrderID: "A", ExecType: "0"}},
			},
			want: outcomeMatched,
		},
		{
			name: "no ack captured, and the bot timed out waiting — the contestant dropped it",
			po:   pendingOrder{hasSent: true, sent: sentWith(true, 0)},
			want: outcomeLost,
		},
		{
			name: "no ack captured, but the bot DID get a response — capture loss, not the contestant",
			po:   pendingOrder{hasSent: true, sent: sentWith(false, 500)},
			want: outcomeCaptureGap,
		},
		{
			name: "no ack captured, bot neither timed out nor recorded a response — unprovable, do not score",
			po:   pendingOrder{hasSent: true, sent: sentWith(false, 0)},
			want: outcomeCaptureGap,
		},
		{
			name: "answered but never sent",
			po:   pendingOrder{acks: []topics.OrderAckedEvent{{OrderID: "ghost", ExecType: "2", FillQty: 5}}},
			want: outcomeUnsent,
		},
		{
			name: "neither (cannot happen; must not be reported as a drop)",
			po:   pendingOrder{},
			want: outcomeUnsent,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.po.outcome(); got != c.want {
				t.Errorf("outcome = %v, want %v", got, c.want)
			}
		})
	}
}

// TestTimedOutBeatsRecvDone: if the bot marked the order timed out, that is the
// authoritative verdict even if a partial receive timestamp is also present.
func TestTimedOutBeatsRecvDone(t *testing.T) {
	po := pendingOrder{hasSent: true, sent: topics.OrderSentEvent{OrderID: "A", TimedOut: true, RecvDoneTSNS: 500}}
	if got := po.outcome(); got != outcomeLost {
		t.Errorf("outcome = %v, want outcomeLost", got)
	}
}

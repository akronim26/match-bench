// Package validate tests the order-level correctness score.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validate

import (
	"math"
	"testing"
)

func closeTo(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("score = %v, want %v", got, want)
	}
}

// TestScoreIsOrderLevel pins the denominator change. The score was
// ValidFills/ScoredFills — a PER-FILL ratio — which structurally could not express
// three of the four violation classes the audit requires pass 2 to score "exact":
// lost orders, lost cancels, per-flow FIFO breaches and cross-flow queue-jumps are all
// PER-ORDER, and a lost order produces zero fills, so a fill-denominated ratio can
// never see it. That is why an engine that acked ~216k orders and filled none scored
// 1.0 while a correct engine logging 35,431 violations also scored 1.0.
func TestScoreIsOrderLevel(t *testing.T) {
	var r Report
	r.ScoredOrders = 100
	r.DirtyOrders = 0
	closeTo(t, r.CorrectnessScore(), 1.0)

	r.DirtyOrders = 25
	closeTo(t, r.CorrectnessScore(), 0.75)

	r.DirtyOrders = 100
	closeTo(t, r.CorrectnessScore(), 0.0)
}

// TestScoreCountsAnOrderOnceHoweverManyViolations: an order with three violations is
// one dirty order, not three. Otherwise a single pathological order could drive the
// score negative, and severity weighting is a separate policy question from "did this
// order behave correctly at all".
func TestScoreCountsAnOrderOnceHoweverManyViolations(t *testing.T) {
	var r Report
	r.ScoredOrders = 10
	// Same order id flagged by three different classes.
	r.MarkDirty("o1")
	r.MarkDirty("o1")
	r.MarkDirty("o1")
	if r.DirtyOrders != 1 {
		t.Fatalf("DirtyOrders = %d, want 1 — an order is dirty or clean, not dirty N times", r.DirtyOrders)
	}
	closeTo(t, r.CorrectnessScore(), 0.9)
}

// TestScoreNoOrdersIsPerfect: a session that applied no orders at all cannot be judged,
// so it scores 1.0. This is NOT the old ScoredFills==0 hole — that returned 1.0 for an
// engine which received hundreds of thousands of orders and simply never filled any.
// Under order-level scoring those orders are counted and lost, so they score 0.
func TestScoreNoOrdersIsPerfect(t *testing.T) {
	var r Report
	closeTo(t, r.CorrectnessScore(), 1.0)
}

// TestAckerScoresZero is the case the whole change exists for: an engine that ACKs
// every order and never fills anything. Every order is a lost order, so every order is
// dirty.
func TestAckerScoresZero(t *testing.T) {
	var r Report
	const n = 1000
	r.ScoredOrders = n
	for i := 0; i < n; i++ {
		r.MarkDirty(orderID(i))
	}
	if r.DirtyOrders != n {
		t.Fatalf("DirtyOrders = %d, want %d", r.DirtyOrders, n)
	}
	closeTo(t, r.CorrectnessScore(), 0.0)
}

// TestOverfillStillCountsAgainstTheScore: overfill was the ONLY class that affected the
// old score, and it must keep doing so under the new denominator — the change adds
// classes, it does not swap one for others.
func TestOverfillStillCountsAgainstTheScore(t *testing.T) {
	var r Report
	r.ScoredOrders = 4
	r.MarkDirty("overfilled")
	closeTo(t, r.CorrectnessScore(), 0.75)
}

func orderID(i int) string {
	return "order-" + string(rune('a'+i%26)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

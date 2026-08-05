// Dropped orders — an order sent on the wire that the contestant never answered.
//
// Neither mode could see these in production. The streaming source emits an order only
// when it has at least one ack (`emitReady`), so a dropped order never reached Apply at
// all: full mode never counted it in ScoredOrders, and invariants mode's lost-order
// branch fired only for orders whose responses failed the T7 sanity gate. An engine that
// answered the orders it could handle and silently dropped the rest was scored purely on
// what it chose to answer — the same hole as the acker, one level up.
package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestFullModeScoresLostOrders: a dropped order must be graded, not skipped.
func TestFullModeScoresLostOrders(t *testing.T) {
	v := NewStreamValidator()
	v.Apply(order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)))
	v.Apply(order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)))
	v.AddLost("DROPPED", model.NewLimit)
	r := v.Finish()

	if r.LostOrders != 1 {
		t.Fatalf("LostOrders = %d, want 1: %+v", r.LostOrders, r)
	}
	if violationsOfType(r, LostOrder) != 1 {
		t.Fatalf("want a LostOrder violation entry, got %+v", r.Violations)
	}
	if r.ScoredOrders != 3 {
		t.Fatalf("ScoredOrders = %d, want 3 — a dropped order is still an order the engine was sent", r.ScoredOrders)
	}
	if r.DirtyOrders != 1 {
		t.Fatalf("DirtyOrders = %d, want 1 (the dropped one): %+v", r.DirtyOrders, r)
	}
	if got := r.CorrectnessScore(); got != 2.0/3.0 {
		t.Fatalf("score = %v, want 2/3", got)
	}
}

// TestFullModeScoresLostCancels: a dropped CANCEL is its own class. Silently ignoring
// cancels is a distinct (and lucrative) failure mode from ignoring new orders.
func TestFullModeScoresLostCancels(t *testing.T) {
	v := NewStreamValidator()
	v.AddLost("C1", model.Cancel)
	r := v.Finish()

	if r.LostCancels != 1 || r.LostOrders != 0 {
		t.Fatalf("want LostCancels=1 LostOrders=0, got %+v", r)
	}
	if violationsOfType(r, LostCancel) != 1 {
		t.Fatalf("want a LostCancel violation entry, got %+v", r.Violations)
	}
}

// TestFullModeDropEverythingScoresZero is the gaming case this closes: an engine that
// answers nothing at all used to be invisible to full mode (no acks → no assembled
// orders → ScoredOrders 0 → the empty-session guard returns 1.0).
func TestFullModeDropEverythingScoresZero(t *testing.T) {
	v := NewStreamValidator()
	for i := 0; i < 50; i++ {
		v.AddLost(seqID("O", i)+itoaLocal(i), model.NewLimit)
	}
	r := v.Finish()

	if r.ScoredOrders != 50 || r.DirtyOrders != 50 {
		t.Fatalf("want 50 scored / 50 dirty, got %+v", r)
	}
	if got := r.CorrectnessScore(); got != 0.0 {
		t.Fatalf("an engine that answers nothing must score 0, got %v", got)
	}
}

// TestFullModeSelectiveDropIsScored: answer half correctly, drop the other half. The
// score must reflect the drops rather than being computed over the answered subset.
func TestFullModeSelectiveDropIsScored(t *testing.T) {
	v := NewStreamValidator()
	v.Apply(order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)))
	v.Apply(order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)))
	v.AddLost("D1", model.NewLimit)
	v.AddLost("D2", model.NewLimit)
	r := v.Finish()

	if got := r.CorrectnessScore(); got != 0.5 {
		t.Fatalf("2 clean of 4 sent must score 0.5, got %v (%+v)", got, r)
	}
}

// TestInvariantsScoresLostViaAddLost: pass 2 gets the same signal from the source, and
// must not double-count it against its own Apply-time lost branch.
func TestInvariantsScoresLostViaAddLost(t *testing.T) {
	v := NewInvariantsValidator(500)
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	v.Apply(ordWithFlow("O1", model.NewLimit, model.Buy, 100, 10, fa, 1, 10,
		model.Response{ExecType: "0", T7Ns: 20}))
	v.AddLost("DROPPED", model.NewLimit)
	r := v.Finish()

	if r.LostOrders != 1 {
		t.Fatalf("LostOrders = %d, want 1: %+v", r.LostOrders, r)
	}
	if r.ScoredOrders != 2 {
		t.Fatalf("ScoredOrders = %d, want 2 (one answered + one dropped)", r.ScoredOrders)
	}
	if got := r.CorrectnessScore(); got != 0.5 {
		t.Fatalf("score = %v, want 0.5", got)
	}
}

// TestInvariantsLostEverythingScoresZero pins the ScoredOrders denominator. `applied` is
// incremented before the lost-order branch returns, while Finish computes
// `applied + LostOrders + LostCancels` on the premise that lost orders returned BEFORE
// being counted — so each lost order landed in the denominator twice and an engine that
// answered nothing scored 0.5 instead of 0.
func TestInvariantsLostEverythingScoresZero(t *testing.T) {
	v := NewInvariantsValidator(500)
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	const n = 20
	for i := 0; i < n; i++ {
		// No responses at all: the Apply-time lost branch.
		v.Apply(ordWithFlow(seqID("O", i)+itoaLocal(i), model.NewLimit, model.Buy, 100, 10, fa, uint32(i), uint64(10+i)))
	}
	r := v.Finish()

	if r.LostOrders != n {
		t.Fatalf("LostOrders = %d, want %d", r.LostOrders, n)
	}
	if r.ScoredOrders != n {
		t.Fatalf("ScoredOrders = %d, want %d — a lost order must be counted ONCE", r.ScoredOrders, n)
	}
	if got := r.CorrectnessScore(); got != 0.0 {
		t.Fatalf("an engine that answered nothing must score 0, got %v (%+v)", got, r)
	}
}

func itoaLocal(i int) string {
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

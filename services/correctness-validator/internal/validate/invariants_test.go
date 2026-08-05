package validate

import (
	"fmt"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestCrossFlowPredicate_WorkedExamples pins down the W-window predicate with the
// exact numbers from the task spec.
func TestCrossFlowPredicate_WorkedExamples(t *testing.T) {
	// A arrives at 1000us, B arrives at 1300us, W=500us. B processed first (caller
	// already knows this). gap=300<=500 -> inversion, NOT a violation, jitter=300us.
	isInv, violation, jitterNs := CrossFlowPredicate(1000*1000, 1300*1000, 500)
	if !isInv || violation || jitterNs != 300*1000 {
		t.Fatalf("example 1: got isInv=%v violation=%v jitterNs=%d, want isInv=true violation=false jitterNs=300000",
			isInv, violation, jitterNs)
	}

	// A arrives at 1000us, B at 1800us, W=500us. gap=800>500 -> violation, jitter=800us.
	isInv, violation, jitterNs = CrossFlowPredicate(1000*1000, 1800*1000, 500)
	if !isInv || !violation || jitterNs != 800*1000 {
		t.Fatalf("example 2: got isInv=%v violation=%v jitterNs=%d, want isInv=true violation=true jitterNs=800000",
			isInv, violation, jitterNs)
	}

	// A arrives at 1000us, B arrives at 900us (B arrived FIRST). Not an inversion —
	// arrival order and processing order agree.
	isInv, violation, jitterNs = CrossFlowPredicate(1000*1000, 900*1000, 500)
	if isInv || violation || jitterNs != 0 {
		t.Fatalf("example 3: got isInv=%v violation=%v jitterNs=%d, want all false/zero", isInv, violation, jitterNs)
	}
}

// TestCrossFlowPredicate_ExactlyAtWindow: gap == W is NOT a violation ("<=" tolerance).
func TestCrossFlowPredicate_ExactlyAtWindow(t *testing.T) {
	isInv, violation, jitterNs := CrossFlowPredicate(1000, 1500, 500) // ns scale, gap=500ns... use us properly below
	_ = isInv
	_ = violation
	_ = jitterNs
	isInv, violation, jitterNs = CrossFlowPredicate(1_000_000, 1_500_000, 500) // gap exactly 500us
	if !isInv || violation {
		t.Fatalf("gap==W must not violate: isInv=%v violation=%v jitterNs=%d", isInv, violation, jitterNs)
	}
}

func flowA() model.Flow { return model.Flow{SrcIP: 1, SrcPort: 1} }
func flowB() model.Flow { return model.Flow{SrcIP: 2, SrcPort: 2} }

func invOrd(id string, kind model.Kind, flow model.Flow, tcpSeq uint32, t3 uint64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, Kind: kind, Flow: flow, TCPSeq: tcpSeq, T3Ns: t3, Qty: qty, Responses: resp}
}

func respAt(qty, price, t7 uint64) model.Response {
	return model.Response{ExecType: "2", FillQty: qty, FillPrice: price, T7Ns: t7}
}

func TestInvariants_OverfillDetected(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(6, 100, 2000), respAt(6, 100, 3000)))
	r := v.Finish()
	if r.Overfills != 1 {
		t.Fatalf("expected 1 overfill, got %+v", r)
	}
}

func TestInvariants_LostOrderCounted(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10)) // no responses
	r := v.Finish()
	if r.LostOrders != 1 {
		t.Fatalf("expected 1 lost order, got %+v", r)
	}
}

func TestInvariants_LostCancelCounted(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("C1", model.Cancel, flowA(), 1, 1000, 0)) // no responses
	r := v.Finish()
	if r.LostCancels != 1 {
		t.Fatalf("expected 1 lost cancel, got %+v", r)
	}
}

func TestInvariants_PerFlowFIFOBreachFlagged(t *testing.T) {
	v := NewInvariantsValidator(500)
	// Same flow, TCPSeq 1 then 2, but seq-2's order is processed (min T7) before seq-1's.
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 5000)))
	v.Apply(invOrd("O2", model.NewLimit, flowA(), 2, 1500, 10, respAt(10, 100, 2000)))
	r := v.Finish()
	if r.TimeViolations == 0 {
		t.Fatalf("expected a per-flow FIFO breach, got %+v", r)
	}
}

func TestInvariants_PerFlowFIFOCleanNotFlagged(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 2000)))
	v.Apply(invOrd("O2", model.NewLimit, flowA(), 2, 1500, 10, respAt(10, 100, 5000)))
	r := v.Finish()
	if r.TimeViolations != 0 {
		t.Fatalf("clean FIFO must not flag, got %+v", r)
	}
}

func TestInvariants_CrossFlowWithinWindowNotViolation(t *testing.T) {
	v := NewInvariantsValidator(500) // W=500us
	// A arrives at t3=1000us, processed (T7) at 10000ns.
	v.Apply(invOrd("A", model.NewLimit, flowA(), 1, 1000*1000, 10, respAt(10, 100, 10_000_000)))
	// B arrives at t3=1300us (gap 300us <= W), processed BEFORE A (T7 smaller).
	v.Apply(invOrd("B", model.NewLimit, flowB(), 1, 1300*1000, 10, respAt(10, 100, 5_000_000)))
	r := v.Finish()
	if r.TimeViolations != 0 {
		t.Fatalf("gap within W must not be a scored violation, got %+v", r)
	}
	if r.Jitter.Count != 1 {
		t.Fatalf("expected 1 recorded inversion in the jitter histogram, got %+v", r.Jitter)
	}
}

func TestInvariants_CrossFlowBeyondWindowViolatesAndRecordsJitter(t *testing.T) {
	v := NewInvariantsValidator(500) // W=500us
	v.Apply(invOrd("A", model.NewLimit, flowA(), 1, 1000*1000, 10, respAt(10, 100, 10_000_000)))
	// B arrives at t3=1800us (gap 800us > W), processed before A.
	v.Apply(invOrd("B", model.NewLimit, flowB(), 1, 1800*1000, 10, respAt(10, 100, 5_000_000)))
	r := v.Finish()
	if r.TimeViolations != 1 {
		t.Fatalf("gap beyond W must be a scored violation, got %+v", r)
	}
	if r.Jitter.Count != 1 || r.Jitter.MaxUs != 800 {
		t.Fatalf("expected jitter recorded at 800us, got %+v", r.Jitter)
	}
}

func TestInvariants_UnmatchedResponseIsMetricOnlyNotScored(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 2000)))
	v.AddUnmatched("ghost", 5, 100)
	r := v.Finish()
	if r.PhantomFills != 1 {
		t.Fatalf("expected unmatched response counted as metric, got %+v", r)
	}
	if r.CorrectnessScore() != 1.0 {
		t.Fatalf("unmatched response must not affect score, got %v", r.CorrectnessScore())
	}
}

// TestInvariants_EndToEndSyntheticStream feeds a small synthetic sent/acked-derived
// order stream through invariants mode end-to-end and asserts on the resulting
// violations + jitter stats together.
func TestInvariants_EndToEndSyntheticStream(t *testing.T) {
	v := NewInvariantsValidator(500)

	// Clean same-flow pair: no violations.
	v.Apply(invOrd("clean1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 1_000_000)))
	v.Apply(invOrd("clean2", model.NewLimit, flowA(), 2, 2000, 10, respAt(10, 100, 2_000_000)))

	// Cross-flow, small skew (within W): inversion but not a violation.
	v.Apply(invOrd("tie1", model.NewLimit, flowA(), 3, 10_000*1000, 10, respAt(10, 100, 20_000_000)))
	v.Apply(invOrd("tie2", model.NewLimit, flowB(), 1, 10_200*1000, 10, respAt(10, 100, 15_000_000)))

	// Cross-flow, large skew (beyond W): a violation.
	v.Apply(invOrd("far1", model.NewLimit, flowA(), 4, 50_000*1000, 10, respAt(10, 100, 60_000_000)))
	v.Apply(invOrd("far2", model.NewLimit, flowB(), 2, 52_000*1000, 10, respAt(10, 100, 55_000_000)))

	// A lost order.
	v.Apply(invOrd("lost1", model.NewLimit, flowA(), 5, 99_000*1000, 10))

	// An overfill.
	v.Apply(invOrd("over1", model.NewLimit, flowB(), 3, 60_000*1000, 5, respAt(3, 100, 61_000_000), respAt(3, 100, 62_000_000)))

	r := v.Finish()
	if r.LostOrders != 1 {
		t.Fatalf("expected 1 lost order, got %+v", r)
	}
	if r.Overfills != 1 {
		t.Fatalf("expected 1 overfill, got %+v", r)
	}
	if r.TimeViolations != 1 {
		t.Fatalf("expected exactly 1 scored cross-flow time violation (far1/far2), got %+v", r)
	}
	if r.Jitter.Count != 2 {
		t.Fatalf("expected 2 recorded inversions (tie + far), got %+v", r.Jitter)
	}
	if r.Jitter.MaxUs != 2000 {
		t.Fatalf("expected max jitter 2000us (far pair), got %+v", r.Jitter)
	}
}

// TestInvariants_ChecksFireOnEmergenceNotAtFinish proves violations are visible as
// soon as the offending order emerges from the (small) T7 reorder window, well before
// Finish is called on the rest of the stream.
func TestInvariants_ChecksFireOnEmergenceNotAtFinish(t *testing.T) {
	v := NewInvariantsValidatorWithWindow(500, 2) // tiny window forces frequent release

	// Same-flow FIFO breach: seq 2 processed (T7) before seq 1.
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 5000)))
	v.Apply(invOrd("O2", model.NewLimit, flowA(), 2, 1500, 10, respAt(10, 100, 2000)))
	// More pushes to cross the 2*window=4 release threshold twice, so O1 (seq=1,
	// t7=5000 — the highest T7 among the first four, so it's held back by the first
	// release) itself emerges and its FIFO breach against the meanwhile-advanced
	// lastSeq is actually checked.
	v.Apply(invOrd("O3", model.NewLimit, flowA(), 3, 2000, 10, respAt(10, 100, 3000)))
	v.Apply(invOrd("O4", model.NewLimit, flowA(), 4, 2500, 10, respAt(10, 100, 4000)))
	v.Apply(invOrd("O5", model.NewLimit, flowA(), 5, 3000, 10, respAt(10, 100, 6000)))
	v.Apply(invOrd("O6", model.NewLimit, flowA(), 6, 3500, 10, respAt(10, 100, 7000)))

	// At this point the window has released at least twice; the FIFO breach between
	// O1/O2 must already be counted, without ever calling Finish.
	if v.rep.TimeViolations == 0 {
		t.Fatalf("expected FIFO violation to be visible before Finish, got %+v", v.rep)
	}

	// Feed a long tail well past the window and confirm the buffered window state
	// (not the report) stays bounded — O(window), not O(total orders fed).
	for i := 0; i < 5000; i++ {
		v.Apply(invOrd("bulk", model.NewLimit, flowB(), uint32(i+1), uint64(i+1)*1000,
			10, respAt(10, 100, uint64(i+1)*1000)))
	}
	if len(v.window.buf) > 2*v.window.window {
		t.Fatalf("T7 reorder window buffer grew unbounded: len=%d, window=%d", len(v.window.buf), v.window.window)
	}

	v.Finish()
}

// TestInvariants_LateArrivalCountedNotCrash: an order whose minT7Ns falls behind the
// window's watermark (its correct slot in processing order was already released and
// checked) must be counted as T7ReorderLate, not crash and not corrupt the FIFO/
// cross-flow result for already-emitted orders.
func TestInvariants_LateArrivalCountedNotCrash(t *testing.T) {
	v := NewInvariantsValidatorWithWindow(500, 1) // window=1 -> release triggers at buf len 2

	v.Apply(invOrd("A", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 100_000)))
	v.Apply(invOrd("B", model.NewLimit, flowA(), 2, 2000, 10, respAt(10, 100, 200_000)))
	// The push above should have released A (watermark=100_000) once the buffer hit
	// 2*window=2.
	if v.window.watermark == 0 {
		t.Fatalf("expected window to have released at least once, watermark=%d", v.window.watermark)
	}

	// A "late" order with minT7Ns behind the watermark.
	v.Apply(invOrd("late1", model.NewLimit, flowA(), 3, 3000, 10, respAt(10, 100, 50_000)))

	r := v.Finish()
	if r.T7ReorderLate != 1 {
		t.Fatalf("expected 1 late arrival counted, got %+v", r)
	}
}

// TestInvariants_T7AnomalyGate pins the sanity gate: a response with t7 < t3
// (impossible on one clock — stream corruption) or t7-t3 beyond the cap must
// not position the order's processing time, and each rejection is counted.
func TestInvariants_T7AnomalyGate(t *testing.T) {
	v := NewInvariantsValidatorWithWindow(500, 4)
	// Order whose ONLY response is corrupt (t7 < t3): rejected -> order has no
	// usable response -> counted lost, one anomaly.
	v.Apply(&model.Order{
		OrderID: "bad", Flow: model.Flow{SrcPort: 1}, TCPSeq: 1, T3Ns: 1_000_000, Qty: 1,
		Responses: []model.Response{{T7Ns: 999_999, ExecType: "0"}},
	})
	// Order with one corrupt and one sane response: sane one positions it.
	v.Apply(&model.Order{
		OrderID: "mixed", Flow: model.Flow{SrcPort: 1}, TCPSeq: 2, T3Ns: 2_000_000, Qty: 1,
		Responses: []model.Response{
			{T7Ns: 1_000_000, ExecType: "0"},           // t7 < t3: anomaly
			{T7Ns: 2_500_000, ExecType: "0"},           // sane
		},
	})
	// Straggler beyond the cap.
	v.SetT7AnomalyCapNs(1_000_000) // 1ms cap for the test
	v.Apply(&model.Order{
		OrderID: "late", Flow: model.Flow{SrcPort: 1}, TCPSeq: 3, T3Ns: 3_000_000, Qty: 1,
		Responses: []model.Response{{T7Ns: 3_000_000 + 2_000_000, ExecType: "0"}},
	})
	rep := v.Finish()
	if rep.T7Anomalies != 3 {
		t.Fatalf("T7Anomalies = %d, want 3", rep.T7Anomalies)
	}
	if rep.LostOrders != 2 {
		t.Fatalf("LostOrders = %d, want 2 (bad + late have no usable response)", rep.LostOrders)
	}
}

// TestInvariants_TaintWhenLateRateExceeded pins the taint flag: too many
// orders escaping T7-ordered grading marks the result unreliable instead of
// silently partial.
func TestInvariants_TaintWhenLateRateExceeded(t *testing.T) {
	v := NewInvariantsValidatorWithWindow(500, 4)
	v.SetLateTaintRate(0.20)
	// 2 anomalous of 4 applied = 50% > 20% -> tainted.
	for i, t3 := range []uint64{1_000_000, 2_000_000} {
		v.Apply(&model.Order{
			OrderID: fmt.Sprintf("ok-%d", i), Flow: model.Flow{SrcPort: 1}, TCPSeq: uint32(i + 1),
			T3Ns: t3, Qty: 1,
			Responses: []model.Response{{T7Ns: t3 + 1000, ExecType: "0"}},
		})
	}
	for i, t3 := range []uint64{3_000_000, 4_000_000} {
		v.Apply(&model.Order{
			OrderID: fmt.Sprintf("bad-%d", i), Flow: model.Flow{SrcPort: 1}, TCPSeq: uint32(i + 3),
			T3Ns: t3, Qty: 1,
			Responses: []model.Response{{T7Ns: t3 - 1, ExecType: "0"}},
		})
	}
	rep := v.Finish()
	if !rep.Tainted {
		t.Fatal("expected Tainted at 50% anomaly rate with 20% threshold")
	}
	if rep.TaintReason == "" {
		t.Fatal("TaintReason empty")
	}
	clean := NewInvariantsValidatorWithWindow(500, 4)
	clean.Apply(&model.Order{
		OrderID: "ok", Flow: model.Flow{SrcPort: 1}, TCPSeq: 1, T3Ns: 1_000_000, Qty: 1,
		Responses: []model.Response{{T7Ns: 1_001_000, ExecType: "0"}},
	})
	if rep2 := clean.Finish(); rep2.Tainted {
		t.Fatal("clean stream must not taint")
	}
}

// TestInvariants_SmallWindowEquivalentOnCleanStream pins the window-shrink
// safety argument: on a stream whose T7 disorder fits the window, a tiny
// window produces the identical report to a huge one (zero late drops).
func TestInvariants_SmallWindowEquivalentOnCleanStream(t *testing.T) {
	build := func(window int) Report {
		v := NewInvariantsValidatorWithWindow(500, window)
		for i := 0; i < 100; i++ {
			t3 := uint64(1_000_000 + i*10_000)
			v.Apply(&model.Order{
				OrderID: fmt.Sprintf("o-%d", i), Flow: model.Flow{SrcPort: uint16(1 + i%3)},
				TCPSeq: uint32(i + 1), T3Ns: t3, Qty: 1,
				Responses: []model.Response{{T7Ns: t3 + 5_000, ExecType: "0"}},
			})
		}
		return v.Finish()
	}
	small, big := build(4), build(1<<16)
	if small.T7ReorderLate != 0 {
		t.Fatalf("clean stream lated %d records in small window", small.T7ReorderLate)
	}
	if small.TimeViolations != big.TimeViolations || small.Jitter.Count != big.Jitter.Count ||
		small.LostOrders != big.LostOrders || small.Tainted != big.Tainted {
		t.Fatalf("small/big window reports diverge: %+v vs %+v", small, big)
	}
}

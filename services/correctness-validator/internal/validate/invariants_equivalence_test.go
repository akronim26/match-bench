package validate

import (
	"sort"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// --- test-only port of the OLD Finish()-time batch algorithm (pre-streaming) ---
// This intentionally duplicates the pre-redesign logic (map of all orders, single
// sort + sweep at Finish) as an equivalence oracle. It must never be used outside
// tests / shipped in production code.

type oldInvOrder struct {
	orderID string
	kind    model.Kind
	flow    model.Flow
	tcpSeq  uint32
	t3Ns    uint64
	qty     uint64
	minT7Ns uint64
	hasResp bool
}

type oldBatchValidator struct {
	windowUs uint64
	orders   map[string]*oldInvOrder
	rep      Report
}

func newOldBatchValidator(windowUs uint64) *oldBatchValidator {
	return &oldBatchValidator{windowUs: windowUs, orders: make(map[string]*oldInvOrder)}
}

func (v *oldBatchValidator) Apply(o *model.Order) {
	rec := &oldInvOrder{orderID: o.OrderID, kind: o.Kind, flow: o.Flow, tcpSeq: o.TCPSeq, t3Ns: o.T3Ns, qty: o.Qty}
	var cumFilled uint64
	for _, resp := range o.Responses {
		if !isFill(resp.ExecType, resp.FillQty) {
			continue
		}
		v.rep.TotalFills++
		cumFilled += resp.FillQty
		if cumFilled > o.Qty {
			v.rep.Overfills++
			v.rep.add(Overfill, o.OrderID, resp.FillQty, int64(resp.FillPrice), "overfill")
		} else {
			v.rep.ValidFills++
		}
	}
	// minT7Ns is the first response (min T7) across ALL responses — fills AND acks —
	// matching the corrected production spec (invariants.go's Apply): an order that
	// acks early but fills late is positioned by its earliest response, not its
	// earliest fill.
	for _, resp := range o.Responses {
		if resp.T7Ns == 0 {
			continue
		}
		if !rec.hasResp || resp.T7Ns < rec.minT7Ns {
			rec.minT7Ns = resp.T7Ns
			rec.hasResp = true
		}
	}
	v.orders[o.OrderID] = rec
}

func (v *oldBatchValidator) AddUnmatched(_ string, _ uint64, _ int64) {
	v.rep.PhantomFills++
}

func (v *oldBatchValidator) Finish() Report {
	responded := make([]*oldInvOrder, 0, len(v.orders))
	for _, rec := range v.orders {
		if rec.hasResp {
			responded = append(responded, rec)
		} else {
			v.rep.add(lostViolationType(rec.kind), rec.orderID, 0, 0, "lost")
			if rec.kind == model.Cancel {
				v.rep.LostCancels++
			} else {
				v.rep.LostOrders++
			}
		}
	}

	sort.Slice(responded, func(i, j int) bool { return responded[i].minT7Ns < responded[j].minT7Ns })
	lastSeq := make(map[model.Flow]uint32)
	lastSeqSet := make(map[model.Flow]bool)
	for _, rec := range responded {
		if lastSeqSet[rec.flow] && seqLE(rec.tcpSeq, lastSeq[rec.flow]) {
			v.rep.TimeViolations++
			v.rep.add(Time, rec.orderID, 0, 0, "fifo")
		}
		lastSeq[rec.flow] = rec.tcpSeq
		lastSeqSet[rec.flow] = true
	}

	var samplesNs []uint64
	var max1T3, max2T3 uint64
	var max1Flow model.Flow
	var max1Set, max2Set bool
	for _, rec := range responded {
		candT3, candSet := max1T3, max1Set
		if max1Set && rec.flow == max1Flow {
			candT3, candSet = max2T3, max2Set
		}
		if candSet {
			isInv, violation, jitterNs := CrossFlowPredicate(rec.t3Ns, candT3, v.windowUs)
			if isInv {
				samplesNs = append(samplesNs, jitterNs)
				if violation {
					v.rep.TimeViolations++
					v.rep.add(Time, rec.orderID, 0, 0, "cross-flow")
				}
			}
		}
		if !max1Set || rec.t3Ns > max1T3 {
			if max1Set && max1Flow != rec.flow && (!max2Set || max1T3 > max2T3) {
				max2T3, max2Set = max1T3, true
			}
			max1T3, max1Flow, max1Set = rec.t3Ns, rec.flow, true
		} else if rec.flow != max1Flow && (!max2Set || rec.t3Ns > max2T3) {
			max2T3, max2Set = rec.t3Ns, true
		}
	}

	n := len(samplesNs)
	if n > 0 {
		sorted := make([]uint64, n)
		copy(sorted, samplesNs)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		v.rep.Jitter = JitterStats{
			Count:         uint64(n),
			InversionRate: float64(n) / float64(len(responded)),
			MaxUs:         float64(sorted[n-1]) / 1000.0,
		}
	}
	return v.rep
}

// buildSyntheticSession returns a fixed, deterministic order set exercising: clean
// same-flow FIFO, a FIFO breach, within-window and beyond-window cross-flow
// inversions, lost orders/cancels, and an overfill — fed in shuffled (non-T7-sorted)
// order, as a real Kafka-drained stream would arrive.
func buildSyntheticSession() []*model.Order {
	return []*model.Order{
		invOrd("clean1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 1_000_000)),
		invOrd("clean2", model.NewLimit, flowA(), 2, 2000, 10, respAt(10, 100, 2_000_000)),
		invOrd("fifo_breach_1", model.NewLimit, flowA(), 10, 3000, 10, respAt(10, 100, 9_000_000)),
		invOrd("fifo_breach_2", model.NewLimit, flowA(), 11, 3500, 10, respAt(10, 100, 4_000_000)),
		invOrd("tie1", model.NewLimit, flowA(), 12, 10_000*1000, 10, respAt(10, 100, 20_000_000)),
		invOrd("tie2", model.NewLimit, flowB(), 1, 10_200*1000, 10, respAt(10, 100, 15_000_000)),
		invOrd("far1", model.NewLimit, flowA(), 13, 50_000*1000, 10, respAt(10, 100, 60_000_000)),
		invOrd("far2", model.NewLimit, flowB(), 2, 52_000*1000, 10, respAt(10, 100, 55_000_000)),
		invOrd("lost1", model.NewLimit, flowA(), 14, 99_000*1000, 10),
		invOrd("lostcancel1", model.Cancel, flowB(), 3, 99_500*1000, 0),
		invOrd("over1", model.NewLimit, flowB(), 4, 60_000*1000, 5, respAt(3, 100, 61_000_000), respAt(3, 100, 62_000_000)),
		invOrd("clean3", model.NewLimit, flowB(), 5, 70_000*1000, 10, respAt(10, 100, 70_000_000)),
	}
}

// TestInvariants_StreamingEquivalentToOldBatch feeds the same synthetic session
// through the new streaming InvariantsValidator and the old Finish()-time batch
// oracle and asserts the accounting fields agree exactly. Jitter percentiles are
// intentionally excluded from the exact comparison: the new histogram is a bounded
// fixed-bucket approximation by design (see jitterHistogram's doc comment) instead of
// the old unbounded sorted-slice exact calculator, so P50/P99/P999 are not expected
// to match bit-for-bit. Count and MaxUs, which the new implementation still tracks
// exactly, ARE compared exactly.
func TestInvariants_StreamingEquivalentToOldBatch(t *testing.T) {
	session := buildSyntheticSession()

	newV := NewInvariantsValidator(500)
	for _, o := range session {
		newV.Apply(o)
	}
	newV.AddUnmatched("ghost", 1, 100)
	got := newV.Finish()

	oldV := newOldBatchValidator(500)
	for _, o := range session {
		oldV.Apply(o)
	}
	oldV.AddUnmatched("ghost", 1, 100)
	want := oldV.Finish()

	if got.TotalFills != want.TotalFills ||
		got.ValidFills != want.ValidFills ||
		got.PhantomFills != want.PhantomFills ||
		got.Overfills != want.Overfills ||
		got.TimeViolations != want.TimeViolations ||
		got.LostOrders != want.LostOrders ||
		got.LostCancels != want.LostCancels {
		t.Fatalf("streaming result diverges from old batch oracle:\n got=%+v\nwant=%+v", got, want)
	}
	if got.Jitter.Count != want.Jitter.Count {
		t.Fatalf("jitter count diverges: got=%d want=%d", got.Jitter.Count, want.Jitter.Count)
	}
	if got.Jitter.MaxUs != want.Jitter.MaxUs {
		t.Fatalf("jitter max diverges: got=%v want=%v", got.Jitter.MaxUs, want.Jitter.MaxUs)
	}
}

// Pass-2 book-free invariants mode (docs/multi-contestant-audit.md §5, P-F pass 2 /
// P-G). No reference matching engine: checks are per-order/per-flow accounting plus a
// cross-flow processing-order comparison against arrival time (t3), replacing the
// full reference-book replay that pass 1 (single-connection, invariants.go's sibling
// batch/stream validators) already owns.
//
// Memory during ingestion is O(reorder_window + flows), independent of session
// length: orders are fed through a bounded T7 reorder window (t7Reorderer, mirroring
// source/reorder.go's T3 window but keyed by minT7Ns) and emerge in processing order.
// Every check — per-flow FIFO, cross-flow priority vs t3, jitter — runs INCREMENTALLY
// as each order emerges from the window, then the record is dropped immediately. There
// is no per-session map and no Finish()-time batch pass: Finish only flushes whatever
// is still buffered in the window and returns the report accumulated so far.
package validate

import (
	"fmt"
	"math/bits"
	"sort"

	"github.com/iicpc/correctness-validator/internal/model"
)

// DefaultCrossFlowWindowUs is the default W for the cross-flow priority-vs-t3 check
// (env CROSS_FLOW_WINDOW_US).
const DefaultCrossFlowWindowUs = 500

// DefaultT7ReorderWindow caps the in-flight T7 reorder buffer (orders). Same
// magnitude as source.DefaultReorderWindow (env VALIDATOR_T7_REORDER_WINDOW). Shrunk from 1<<20 once T7-stream anomalies
// became observable (T7ReorderLate + T7Anomalies on the score event): real T7
// disorder is join/partition skew — ms-scale, thousands of records — and any
// window miss now taints the result instead of silently dropping orders.
const DefaultT7ReorderWindow = 1 << 16

// CrossFlowPredicate is the P-F/P-G W-window predicate, pinned down precisely:
//
// Let A and B be orders on different flows. t3(X) is X's kernel-capture arrival
// time. "B processed before A" means B's first response (min T7) occurred before
// A's. An INVERSION is: A arrived first (t3(A) < t3(B)) but B was processed first.
//
// For an inversion, jitter is ALWAYS recorded (for the P-G histogram) as the
// magnitude t3(B) - t3(A), regardless of whether it trips the window. It is a
// SCORED violation only when that magnitude exceeds the window W: the reordering is
// then too large to be explained by ordinary cross-flow skew (per-flow FIFO already
// covers same-flow ordering; W is the forgiveness for real skew across independent
// TCP flows).
//
// Worked examples (µs), pinned by TestCrossFlowPredicate_WorkedExamples:
//   - t3(A)=1000, t3(B)=1300, W=500: inversion, gap=300<=500 -> NOT a violation,
//     jitter=300 IS recorded.
//   - t3(A)=1000, t3(B)=1800, W=500: inversion, gap=800>500 -> violation AND
//     jitter=800 recorded.
//   - t3(A)=1000, t3(B)=900 (B arrived first): NOT an inversion (arrival order and
//     processing order agree) -> no violation, no jitter.
//
// isInversion tells the caller whether A arrived before B (the precondition for
// this predicate to apply at all — the caller must independently know B was
// processed before A for this pair to be considered).
func CrossFlowPredicate(t3ANs, t3BNs uint64, windowUs uint64) (isInversion bool, violation bool, jitterNs uint64) {
	if t3ANs >= t3BNs {
		return false, false, 0
	}
	jitterNs = t3BNs - t3ANs
	violation = jitterNs > windowUs*1000
	return true, violation, jitterNs
}

// JitterStats summarizes the P-G inversion-magnitude distribution for one session.
type JitterStats struct {
	Count         uint64
	InversionRate float64 // inversions / total processed orders
	P50Us         float64
	P99Us         float64
	P999Us        float64
	MaxUs         float64
}

// jitterHistogram accumulates inversion magnitudes (ns) for one session in a
// fixed-bucket log2 histogram: one uint64 counter per power-of-two bucket (ns range
// covered by uint64 needs at most 65 buckets), plus a running max and count kept
// exactly. Size is O(1) — independent of the number of samples — which is what makes
// jitter tracking safe for an unbounded-length session; a sorted-slice percentile
// calculator (the old approach) grows without bound and reintroduces the same OOM
// failure class this redesign removes. Percentiles are therefore bucket-resolution
// estimates (the lower bound of the bucket containing the percentile rank); Count and
// MaxUs remain exact since those don't require bucketing.
type jitterHistogram struct {
	buckets [65]uint64 // buckets[0] = ns==0, buckets[i] = ns in [2^(i-1), 2^i) for i>=1
	count   uint64
	maxNs   uint64
}

func (h *jitterHistogram) record(ns uint64) {
	idx := bits.Len64(ns)
	h.buckets[idx]++
	h.count++
	if ns > h.maxNs {
		h.maxNs = ns
	}
}

func (h *jitterHistogram) percentileNs(p float64) uint64 {
	if h.count == 0 {
		return 0
	}
	rank := uint64(p * float64(h.count-1))
	var cum uint64
	for i, c := range h.buckets {
		cum += c
		if cum > rank {
			if i == 0 {
				return 0
			}
			return uint64(1) << uint(i-1)
		}
	}
	return h.maxNs
}

func (h *jitterHistogram) stats(totalProcessed uint64) JitterStats {
	if h.count == 0 {
		return JitterStats{}
	}
	rate := 0.0
	if totalProcessed > 0 {
		rate = float64(h.count) / float64(totalProcessed)
	}
	return JitterStats{
		Count:         h.count,
		InversionRate: rate,
		P50Us:         float64(h.percentileNs(0.50)) / 1000.0,
		P99Us:         float64(h.percentileNs(0.99)) / 1000.0,
		P999Us:        float64(h.percentileNs(0.999)) / 1000.0,
		MaxUs:         float64(h.maxNs) / 1000.0,
	}
}

// DefaultT7AnomalyCapNs bounds t7-t3 for a response to participate in
// processing-order grading (env VALIDATOR_T7_ANOMALY_CAP_MS): a pathologically
// late ack (stale socket flushing minutes later) would otherwise drag the T7
// watermark forward and mass-late everything behind it. Matches the ingester's
// 15s timed-out convention.
const DefaultT7AnomalyCapNs = 15_000_000_000

// DefaultLateTaintRate is the (T7ReorderLate+T7Anomalies)/orders ratio above
// which the session result is marked Tainted (env VALIDATOR_LATE_TAINT_RATE).
const DefaultLateTaintRate = 0.001

// invRec is the lightweight record carried through the T7 reorder window — just what
// the incremental checks need. It is dropped as soon as the incremental checks for it
// have run; nothing keyed by order ID is retained across orders.
type invRec struct {
	orderID string
	flow    model.Flow
	tcpSeq  uint32
	t3Ns    uint64
	minT7Ns uint64
}

// t7Reorderer emits invRecs in ascending minT7Ns order using bounded memory, mirroring
// source/reorder.go's Reorderer pattern (accumulate to 2*window, sort, release the
// safe first half) but keyed directly by minT7Ns — no per-flow promotion is needed
// here since minT7Ns is already known in full at Push time (unlike T3, which needs
// head-of-line promotion from TCP retransmission ordering).
type t7Reorderer struct {
	window    int
	buf       []invRec
	watermark uint64
	started   bool
	emit      func(invRec)
	late      func()
}

func newT7Reorderer(window int, emit func(invRec), late func()) *t7Reorderer {
	if window < 1 {
		window = 1
	}
	return &t7Reorderer{window: window, emit: emit, late: late}
}

// push adds one record. If it arrives after the watermark already advanced past its
// minT7Ns (i.e. its correct slot in the emission order was already released), it
// cannot be inserted into an already-sorted-and-released prefix — count it as a late
// arrival instead of silently corrupting the FIFO/cross-flow checks with an
// out-of-order record.
func (r *t7Reorderer) push(rec invRec) {
	if r.started && rec.minT7Ns < r.watermark {
		r.late()
		return
	}
	r.buf = append(r.buf, rec)
	if len(r.buf) >= 2*r.window {
		r.release(r.window)
	}
}

func (r *t7Reorderer) flush() {
	r.release(len(r.buf))
}

func (r *t7Reorderer) release(n int) {
	if n <= 0 || len(r.buf) == 0 {
		return
	}
	if n > len(r.buf) {
		n = len(r.buf)
	}
	sortInvRecs(r.buf)
	for i := 0; i < n; i++ {
		rec := r.buf[i]
		if rec.minT7Ns > r.watermark || !r.started {
			r.watermark = rec.minT7Ns
			r.started = true
		}
		r.emit(rec)
	}
	rest := make([]invRec, len(r.buf)-n)
	copy(rest, r.buf[n:])
	r.buf = rest
}

func sortInvRecs(buf []invRec) {
	sort.SliceStable(buf, func(i, j int) bool { return buf[i].minT7Ns < buf[j].minT7Ns })
}

// InvariantsValidator implements the pass-2 book-free checks. Feed it every order (in
// any order — arrival/EffectiveT3 order from the existing source Reorderer is fine)
// via Apply, then call Finish for the report. Internally, orders with a response are
// pushed through a bounded T7 reorder window and checked incrementally as they emerge
// in processing (min-T7) order; orders with zero responses are counted as lost
// immediately in Apply and never enter the window (see Apply's doc comment for why).
type InvariantsValidator struct {
	windowUs uint64
	rep      Report
	window   *t7Reorderer

	// Incremental per-flow FIFO state: O(flows).
	lastSeq    map[model.Flow]uint32
	lastSeqSet map[model.Flow]bool

	// Incremental cross-flow tracker: O(1). Running max t3 seen so far in
	// processing order, plus a second-best from a different flow so a same-flow
	// max never masks a cross-flow jump (see the comment on the original
	// Finish()-time version below, now inlined into onEmit).
	max1T3, max2T3   uint64
	max1Flow         model.Flow
	max1Set, max2Set bool

	jitter        *jitterHistogram
	processed     uint64
	anomalyCapNs  uint64
	lateTaintRate float64
	applied       uint64
	// captureGaps counts orders excluded from grading because the platform lost their
	// response records (see AddCaptureGap).
	captureGaps uint64
}

// NewInvariantsValidator constructs an invariants-mode validator with cross-flow
// window W (env CROSS_FLOW_WINDOW_US, default DefaultCrossFlowWindowUs) and the
// default T7 reorder window (env VALIDATOR_T7_REORDER_WINDOW via
// NewInvariantsValidatorWithWindow).
func NewInvariantsValidator(windowUs uint64) *InvariantsValidator {
	return NewInvariantsValidatorWithWindow(windowUs, DefaultT7ReorderWindow)
}

// NewInvariantsValidatorWithWindow is NewInvariantsValidator with an explicit T7
// reorder window size (orders), for tests and for main.go's VALIDATOR_T7_REORDER_WINDOW
// env wiring.
func NewInvariantsValidatorWithWindow(windowUs uint64, t7Window int) *InvariantsValidator {
	if windowUs == 0 {
		windowUs = DefaultCrossFlowWindowUs
	}
	if t7Window <= 0 {
		t7Window = DefaultT7ReorderWindow
	}
	v := &InvariantsValidator{
		windowUs:   windowUs,
		lastSeq:    make(map[model.Flow]uint32),
		lastSeqSet: make(map[model.Flow]bool),
		jitter:     &jitterHistogram{},
	}
	v.window = newT7Reorderer(t7Window, v.onEmit, v.onLate)
	v.anomalyCapNs = DefaultT7AnomalyCapNs
	v.lateTaintRate = DefaultLateTaintRate
	return v
}

// SetT7AnomalyCapNs overrides the t7-t3 participation cap (0 keeps the default).
func (v *InvariantsValidator) SetT7AnomalyCapNs(capNs uint64) {
	if capNs > 0 {
		v.anomalyCapNs = capNs
	}
}

// SetLateTaintRate overrides the taint threshold (0 keeps the default).
func (v *InvariantsValidator) SetLateTaintRate(rate float64) {
	if rate > 0 {
		v.lateTaintRate = rate
	}
}

// Apply registers one order's accounting: overfill (own qty only — book-free) is
// checked immediately in Apply (as before — already streaming). Lost order/cancel
// accounting is ALSO decided immediately here rather than deferred: an order with
// zero responses never gets a minT7Ns, so it can never take part in the T7-ordered
// FIFO/cross-flow checks anyway — there is nothing cheaper available from the
// assembly/source layer (model.Order carries only Responses; there's no separate
// "timed out with zero acks" signal from upstream), so detecting hasResp==false here,
// at Apply time, is the cheapest correct place to count it and is O(1), no retention.
// Orders WITH at least one response are pushed into the bounded T7 reorder window;
// FIFO/cross-flow/jitter checks run on each as it emerges (onEmit), and the record is
// dropped immediately after.
func (v *InvariantsValidator) Apply(o *model.Order) {
	var cumFilled uint64
	for _, resp := range o.Responses {
		if !isFill(resp.ExecType, resp.FillQty) {
			continue
		}
		v.rep.TotalFills++
		cumFilled += resp.FillQty
		if cumFilled > o.Qty {
			v.rep.Overfills++
			v.rep.add(Overfill, o.OrderID, resp.FillQty, int64(resp.FillPrice),
				"cumulative reported fill qty exceeds order qty (book-free own-qty check)")
		} else {
			v.rep.ValidFills++
		}
	}
	// minT7Ns is the first response (min T7) across ALL responses — fills AND acks —
	// per the documented spec, not fills-first-then-fallback: an order that acks
	// early but fills late must still be positioned by its earliest response, or the
	// FIFO/cross-flow checks below see a spuriously late processing time for it and
	// flag violations that aren't real.
	var hasResp bool
	var minT7Ns uint64
	for _, resp := range o.Responses {
		if resp.T7Ns == 0 {
			continue
		}
		// Sanity gate: t7 < t3 is impossible on the capture pod's single
		// monotonic clock — its presence means the t7 stream is corrupt
		// (matcher pairing bug, capture restart), and t7-t3 beyond the cap is
		// a pathological straggler that would drag the reorder watermark.
		// Either way the response must not position this order's processing
		// time; count it so corruption taints instead of silently poisoning.
		if resp.T7Ns < o.T3Ns || resp.T7Ns-o.T3Ns > v.anomalyCapNs {
			v.rep.T7Anomalies++
			continue
		}
		if !hasResp || resp.T7Ns < minT7Ns {
			minT7Ns = resp.T7Ns
			hasResp = true
		}
	}
	if !hasResp {
		v.rep.add(lostViolationType(o.Kind), o.OrderID, 0, 0,
			"order/cancel sent but never received any response")
		if o.Kind == model.Cancel {
			v.rep.LostCancels++
		} else {
			v.rep.LostOrders++
		}
		return
	}
	// Counted only once the order is actually graded on processing order. It used to be
	// incremented above the lost branch, so a lost order landed in BOTH `applied` and
	// LostOrders — and Finish sums the two, so every lost order sat in the score
	// denominator twice. An engine that answered nothing scored 0.5 instead of 0.
	// `applied` is also the taint-rate denominator, where lost orders never belonged
	// either: taint measures whether the t7 stream is trustworthy, and an order with no
	// t7 says nothing about that.
	v.applied++
	v.window.push(invRec{
		orderID: o.OrderID,
		flow:    o.Flow,
		tcpSeq:  o.TCPSeq,
		t3Ns:    o.T3Ns,
		minT7Ns: minT7Ns,
	})
}

// onEmit runs the FIFO + cross-flow + jitter checks for one order the instant it
// emerges from the T7 reorder window in processing order, then discards it — this is
// the incremental replacement for the old Finish()-time batch sweep.
func (v *InvariantsValidator) onEmit(rec invRec) {
	v.processed++

	// Per-flow FIFO: within a flow, TCPSeq must be non-decreasing in processing
	// (min-T7) order. A "Time violation" is out-of-order TCPSeq within one flow.
	if v.lastSeqSet[rec.flow] && seqLE(rec.tcpSeq, v.lastSeq[rec.flow]) {
		v.rep.TimeViolations++
		v.rep.add(Time, rec.orderID, 0, 0, "out-of-order TCPSeq within one flow")
	}
	v.lastSeq[rec.flow] = rec.tcpSeq
	v.lastSeqSet[rec.flow] = true

	// Cross-flow priority vs t3, within window W. Each order contributes at most
	// ONE jitter sample — the gap to the latest-arriving cross-flow order
	// processed before it (a running max of t3, with a second-best from a
	// different flow so a same-flow max never masks a cross-flow jump).
	candT3, candSet := v.max1T3, v.max1Set
	if v.max1Set && rec.flow == v.max1Flow {
		candT3, candSet = v.max2T3, v.max2Set
	}
	if candSet {
		isInv, violation, jitterNs := CrossFlowPredicate(rec.t3Ns, candT3, v.windowUs)
		if isInv {
			v.jitter.record(jitterNs)
			if violation {
				v.rep.TimeViolations++
				v.rep.add(Time, rec.orderID, 0, 0, "cross-flow processed after a later-arriving order beyond window W")
			}
		}
	}
	if !v.max1Set || rec.t3Ns > v.max1T3 {
		if v.max1Set && v.max1Flow != rec.flow && (!v.max2Set || v.max1T3 > v.max2T3) {
			v.max2T3, v.max2Set = v.max1T3, true
		}
		v.max1T3, v.max1Flow, v.max1Set = rec.t3Ns, rec.flow, true
	} else if rec.flow != v.max1Flow && (!v.max2Set || rec.t3Ns > v.max2T3) {
		v.max2T3, v.max2Set = rec.t3Ns, true
	}
}

// onLate counts a record that arrived after the T7 reorder window's watermark had
// already advanced past its minT7Ns — its correct slot in processing order was
// already released and checked, so re-inserting it now would produce an incorrect
// (out-of-order) FIFO/cross-flow result rather than a merely-missing one. Counted and
// dropped instead of crashing or silently corrupting state.
func (v *InvariantsValidator) onLate() {
	v.rep.T7ReorderLate++
}

// AddUnmatched records a response reported for an order_id never sent. Kept as the
// unmatched_responses metric only (docs/multi-contestant-audit.md §5: phantom-fill
// is removed from scoring in both modes).
func (v *InvariantsValidator) AddUnmatched(_ string, _ uint64, _ int64) {
	v.rep.PhantomFills++
}

// AddCaptureGap records an order whose response the bot received but whose orders.acked
// record never reached the validator. Pass 2 grades processing ORDER, which is derived
// entirely from captured T7 timestamps, so an order with no capture record contributes
// nothing and must not be counted against the contestant either — see the full-mode
// AddCaptureGap for the measurement that motivated this.
func (v *InvariantsValidator) AddCaptureGap() {
	v.captureGaps++
}

// AddLost records an order the bot sent that the contestant never answered, reported by
// the source rather than discovered in Apply.
//
// Apply's own lost branch can only fire for an order that REACHED it, and the join emits
// an order only once it has at least one ack — so before this, Apply's branch caught
// only orders whose responses failed the T7 sanity gate, never a genuinely dropped one.
// It deliberately does not touch `applied`: that counts orders entering the T7 reorder
// window, and Finish adds the lost counters to it separately.
func (v *InvariantsValidator) AddLost(orderID string, kind model.Kind) {
	if kind == model.Cancel {
		v.rep.LostCancels++
	} else {
		v.rep.LostOrders++
	}
	v.rep.add(lostViolationType(kind), orderID, 0, 0,
		"order/cancel sent but never received any response")
}

// Finish flushes whatever remains buffered in the T7 reorder window (running the same
// incremental onEmit checks on it) and returns the accumulated report. There is no
// separate batch pass here — Finish's only job left is draining the tail of the
// window.
func (v *InvariantsValidator) Finish() Report {
	v.window.flush()
	v.rep.Jitter = v.jitter.stats(v.processed)
	// See the ScoredFills doc comment on Report: invariants mode's TotalFills is
	// real fills only (phantoms never touch it), so it is the denominator directly.
	v.rep.ScoredFills = v.rep.TotalFills
	// Order-level denominator: every order this validator graded. v.applied counts
	// orders pushed into the T7 reorder window; lost orders return before that, so
	// they are added explicitly or an engine that loses everything would divide by
	// zero and score a perfect 1.0.
	v.rep.ScoredOrders = v.applied + v.rep.LostOrders + v.rep.LostCancels
	suspect := v.rep.T7ReorderLate + v.rep.T7Anomalies
	if v.applied > 0 && float64(suspect)/float64(v.applied) > v.lateTaintRate {
		v.rep.Tainted = true
		v.rep.TaintReason = fmt.Sprintf(
			"t7 stream unreliable: %d late + %d anomalous of %d orders exceeds rate %.4f",
			v.rep.T7ReorderLate, v.rep.T7Anomalies, v.applied, v.lateTaintRate)
	}
	v.rep.CaptureGaps = v.captureGaps
	// Capture loss taints for the same reason the T7 anomalies above do: the grading is
	// only as trustworthy as the sample it ran on.
	if total := v.rep.ScoredOrders + v.captureGaps; total > 0 &&
		float64(v.captureGaps)/float64(total) > DefaultCaptureGapTaintRate {
		v.rep.Tainted = true
		v.rep.TaintReason = fmt.Sprintf(
			"capture lost the responses for %d of %d orders (%.1f%%): the contestant answered them but no orders.acked record arrived, so they are excluded from grading",
			v.captureGaps, total, 100*float64(v.captureGaps)/float64(total))
	}
	return v.rep
}

func lostViolationType(k model.Kind) ViolationType {
	if k == model.Cancel {
		return LostCancel
	}
	return LostOrder
}

// seqLE reports whether a <= b using wraparound-safe TCP sequence comparison
// (mirrors source.seqLessLocal / replay's TCPSeq handling).
func seqLE(a, b uint32) bool {
	return a == b || int32(a-b) < 0
}

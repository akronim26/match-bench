// Package validate implements validate behavior.
//
// This file holds the shared report vocabulary — violation classes, the Report and its
// order-level score — used by both validation modes: StreamValidator (full mode,
// reference-book replay, stream.go) and InvariantsValidator (pass 2, book-free,
// invariants.go). There is no batch mode; see stream.go's package comment.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validate

import (
	"fmt"

	"github.com/iicpc/correctness-validator/internal/book"
	"github.com/iicpc/correctness-validator/internal/model"
)

type ViolationType string

const (
	Overfill ViolationType = "overfill"
	// MissedFill: the reference book matched this order, but the contestant reported
	// no fill for it. Without this an engine that ACKs everything and matches nothing
	// is invisible to every other check — it has no fills, so no fill-driven class can
	// fire, and lost_order means "no response at all", which an ack satisfies. That is
	// how an acker scored a clean 1.0.
	MissedFill        ViolationType = "missed_fill"
	Price             ViolationType = "price"
	Time              ViolationType = "time"
	SelfTrade         ViolationType = "self_trade"
	CancelReplaceLoss ViolationType = "cancel_replace_loss"
	// LostOrder/LostCancel: an order/cancel sent on the wire that never got any
	// response. Emitted by BOTH modes via AddLost — the streaming source reports these
	// separately from replayed orders because an unanswered order has no ingress
	// timestamp (T3 is captured from the response), so it has no place in the replay
	// timeline. Full mode used to be blind to them entirely, which made dropping an
	// order free: the join emits only orders that have at least one ack.
	LostOrder  ViolationType = "lost_order"
	LostCancel ViolationType = "lost_cancel"
)

// Violation groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Violation struct {
	Type          ViolationType
	OrderID       string
	ReportedQty   uint64
	ReportedPrice int64
	Detail        string
}

// ReportedFill groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ReportedFill struct {
	OrderID string
	Qty     uint64
	Price   int64
}

// Report groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Report struct {
	TotalFills      uint64
	ValidFills      uint64
	PhantomFills    uint64
	Overfills       uint64
	PriceViolations uint64
	// TimeViolations counts BOTH ordering classes: plain time-priority breaches and
	// cancel-replace priority losses. CancelReplaceLosses is the exact size of the
	// second, so Time-only = TimeViolations - CancelReplaceLosses.
	TimeViolations      uint64
	CancelReplaceLosses uint64
	SelfTrades          uint64
	// MissedFills counts orders the reference book matched but the contestant never
	// filled. Per ORDER, not per fill — the point is the absence of fills.
	MissedFills uint64
	Violations  []Violation

	// totalViolations is the exact count of every add() call, independent of how
	// many example Violation structs got retained in Violations (see add,
	// maxViolationExamplesPerType). ViolationCount() reports this, not
	// len(Violations), so capping the example slice for memory never makes the
	// reported/stored violation_count metric lie.
	totalViolations uint64
	// violationExamplesByType caps how many example Violation structs are retained
	// per ViolationType in Violations (N=100 each, maxViolationExamplesPerType). The
	// exact counters (Overfills, PriceViolations, TimeViolations, SelfTrades,
	// PhantomFills, LostOrders, LostCancels, and totalViolations above) are never
	// capped and stay authoritative; this map only bounds the O(violations) memory
	// the example slice would otherwise grow to on a long adversarial run.
	violationExamplesByType map[ViolationType]int

	LostOrders  uint64
	LostCancels uint64
	// CaptureGaps counts orders excluded from grading because the platform lost their
	// response records: the bot recorded a response but no orders.acked record arrived.
	// Reported so an operator can tell a contestant that dropped orders apart from a
	// capture that lost the evidence — the two look identical downstream, and
	// conflating them once cost a correct engine a third of its score.
	CaptureGaps uint64
	// Invariants-mode-only fields (zero in full-replay mode).
	Jitter JitterStats
	// T7ReorderLate counts orders whose minT7Ns arrived after the T7 reorder
	// window had already evicted (and released) that point in the timeline. These
	// are excluded from the incremental FIFO/cross-flow checks (their ordering
	// slot is gone) but must not crash or silently vanish.
	T7ReorderLate uint64
	// T7Anomalies counts responses rejected from processing-order grading by
	// the sanity gate (t7 < t3, or t7-t3 beyond the anomaly cap).
	T7Anomalies uint64
	// Tainted marks the invariants result unreliable: too many orders escaped
	// T7-ordered grading (late + anomalous above the taint rate). The score is
	// still published, flagged.
	Tainted     bool
	TaintReason string

	// ScoredFills is the fill-level reported metric (the score itself is order-level;
	// see ScoredOrders). In BOTH modes it equals TotalFills: a phantom fill never
	// enters TotalFills — AddPhantom/AddUnmatched increment PhantomFills alone, as the
	// unmatched_responses metric — so there is nothing to net out. Stream mode used to
	// set TotalFills-PhantomFills here on the false premise that phantoms were included,
	// which underflowed this uint64 on any session with more phantoms than real fills.
	ScoredFills uint64

	// ScoredOrders is the denominator of CorrectnessScore: every order the validator
	// actually graded. DirtyOrders is how many of those had at least one violation of
	// any class.
	//
	// The score is ORDER-level, not fill-level. It used to be ValidFills/ScoredFills,
	// which structurally could not express three of the four classes the audit
	// requires pass 2 to score "exact" (docs/multi-contestant-audit.md §5): lost
	// orders, lost cancels, per-flow FIFO breaches and cross-flow queue-jumps are all
	// per-ORDER, and a lost order produces zero fills — so a fill-denominated ratio
	// can never see it. The observable consequence was that an engine which acked
	// ~216k orders and filled none scored 1.0, identically to a correct engine that
	// logged 35,431 violations.
	ScoredOrders uint64
	DirtyOrders  uint64
	// dirtyByOrder dedupes: an order with three violations is ONE dirty order, not
	// three. Severity weighting is a separate policy question from "did this order
	// behave correctly at all", and counting per violation would let a single
	// pathological order drive the ratio past zero.
	dirtyByOrder map[string]struct{}
}

// MarkDirty records that `orderID` violated some invariant. Idempotent per order.
func (r *Report) MarkDirty(orderID string) {
	if r.dirtyByOrder == nil {
		r.dirtyByOrder = make(map[string]struct{})
	}
	if _, seen := r.dirtyByOrder[orderID]; seen {
		return
	}
	r.dirtyByOrder[orderID] = struct{}{}
	r.DirtyOrders++
}

// CorrectnessScore applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r Report) CorrectnessScore() float64 {
	// Phantom-fill is removed as a scored violation class (docs/multi-contestant-audit.md
	// §5, decision 2026-07-16): a fabricated fill is already excluded from the assembled
	// order set upstream (it fails the orders.sent join), so PhantomFills is kept as the
	// unmatched_responses metric only and is not counted against the score. ScoredFills
	// is set by whichever mode ran (StreamValidator.Finish or InvariantsValidator.Finish)
	// and is a reported metric, not this denominator.
	// A session that graded no orders cannot be judged, so it scores 1.0. This is NOT
	// the old ScoredFills==0 hole: that returned 1.0 for an engine which received
	// hundreds of thousands of orders and simply never filled any. Those orders are
	// now counted and marked lost, so such an engine scores 0.
	if r.ScoredOrders == 0 {
		return 1.0
	}
	clean := r.ScoredOrders - r.DirtyOrders
	if r.DirtyOrders > r.ScoredOrders {
		clean = 0 // defensive: never let the ratio go negative
	}
	return float64(clean) / float64(r.ScoredOrders)
}

// ViolationCount applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r Report) ViolationCount() uint64 {
	return r.totalViolations
}

// maxViolationExamplesPerType caps how many Violation example structs are retained
// per ViolationType in Report.Violations, so the example slice stays O(1) per type
// (O(#types) overall) instead of growing unbounded with session length. The exact
// per-type counters (and totalViolations/ViolationCount) are unaffected by this cap.
const maxViolationExamplesPerType = 100

// isFill performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

// add applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Report) add(t ViolationType, id string, qty uint64, price int64, detail string) {
	r.totalViolations++
	// Every violation class flows through here, so this is where an order becomes
	// dirty. Hooking the single choke point rather than each call site means a class
	// added later scores automatically instead of being silently counted-but-ignored —
	// which is exactly how lost orders, lost cancels and both Time classes ended up
	// detected but absent from the score.
	//
	r.MarkDirty(id)
	if r.violationExamplesByType == nil {
		r.violationExamplesByType = make(map[ViolationType]int)
	}
	if r.violationExamplesByType[t] >= maxViolationExamplesPerType {
		return
	}
	r.violationExamplesByType[t]++
	r.Violations = append(r.Violations, Violation{
		Type: t, OrderID: id, ReportedQty: qty, ReportedPrice: price, Detail: detail,
	})
}

// flagJump applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Report) flagJump(e *book.Engine, o *model.Order, jumper string, qty uint64, price int64) {
	r.TimeViolations++
	if e.Repriced(o.OrderID) {
		// Counted exactly, not derived from the retained examples. TimeViolations covers
		// both ordering classes, so the split used to be recovered by counting entries in
		// Violations — which is capped at maxViolationExamplesPerType, so any run with
		// more than 100 of either class reported exactly 100 and looked plausible.
		r.CancelReplaceLosses++
		r.add(CancelReplaceLoss, o.OrderID, qty, price,
			fmt.Sprintf("repriced order filled ahead of %s, which was already resting at the new level (lost time priority on REPLACE)", jumper))
		return
	}
	r.add(Time, o.OrderID, qty, price,
		fmt.Sprintf("filled ahead of earlier same-price order %s (time priority)", jumper))
}

// hasPrice reports whether the reference produced a fill for this order at `price`.
func hasPrice(m map[int64]struct{}, p int64) bool {
	_, ok := m[p]
	return ok
}

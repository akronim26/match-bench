// Streaming, bounded-memory validation, and the only validation path there is: it feeds
// orders into the reference engine in EffectiveT3 order and finalizes each order's
// correctness check the moment it leaves the reference book (fully filled, cancelled,
// replaced) — or at session end for orders that rest forever. Per-order reference fills
// are accumulated from the engine's drained fills and discarded on finalize, so memory
// is O(live book), not O(session).
//
// The batch validator this replaced (validate.Run + source.DrainSession) is deleted. It
// buffered the whole session, was reachable only from its own tests, and its differences
// from this path — a whole-session queue-jump scan, an end-of-session view of the book,
// and an aggressive-fill tolerance knob that production never read — were liabilities to
// keep in lockstep rather than behavior anyone relied on.
package validate

import (
	"fmt"
	"sort"

	"github.com/iicpc/correctness-validator/internal/book"
	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

// orderRef is one order's accumulated reference fills, built incrementally from the
// engine's drained fills while the order is live, then consumed on finalize.
type orderRef struct {
	qty    uint64
	prices map[int64]struct{}
}

// StreamValidator validates a session incrementally with bounded memory.
type StreamValidator struct {
	engine  *book.Engine
	pending map[string]*model.Order // orders in the book (or being processed), keyed by id
	ref     map[string]*orderRef    // per-live-order accumulated reference fills
	shorted *shortedIndex           // finalized orders the contestant under-reported
	// captureGaps counts orders excluded from scoring because the platform lost their
	// response records (see AddCaptureGap).
	captureGaps uint64
	rep         Report
}

// DefaultCaptureGapTaintRate is the share of a session's orders that may be lost by the
// capture before the resulting score is flagged unreliable. Set at 1%: below that the
// sample is still representative; above it the score is being computed on whatever
// fraction of the session survived, and a leaderboard should say so rather than present
// it as authoritative.
const DefaultCaptureGapTaintRate = 0.01

// NewStreamValidator constructs an empty streaming validator.
func NewStreamValidator() *StreamValidator {
	return &StreamValidator{
		engine:  book.NewEngine(),
		pending: make(map[string]*model.Order),
		ref:     make(map[string]*orderRef),
		shorted: newShortedIndex(),
	}
}

// levelKey identifies one side of one price level.
type levelKey struct {
	side  model.Side
	price int64
}

// shortedCapacity bounds how many under-reported orders stay queue-jump evidence. An
// engine that fills nothing shorts every order it is sent, so this set must not be
// allowed to grow with the session. 64Ki entries is far beyond any plausible live book
// while costing ~1.5MB at worst; older entries fall out oldest-first.
const shortedCapacity = 1 << 16

// shortedOrder is one finalized order the reference filled and the contestant did not
// fully report, kept with the book sequence it held (the engine forgets that on
// finalize).
type shortedOrder struct {
	o   *model.Order
	seq uint64
}

// shortedIndex is the bounded, level-indexed record of under-reported orders that have
// already LEFT the reference book.
//
// It exists because a queue jump can only be recognised from the victim's side. When
// the contestant reports a fill for an order the reference never filled, the question
// is whether an EARLIER order at that level should have received it — but by then that
// earlier order has usually been finalized and dropped from `pending` (it was consumed
// by the very match under dispute, or it simply sorts first at Finish). Scanning only
// the live book, the streaming path could never emit Time or CancelReplaceLoss and
// downgraded every queue jump to a generic price violation.
//
// Membership is deliberately narrow — only orders the reference filled and the
// contestant left unreported. All three conditions together (earlier in the queue, the
// reference filled it, the contestant did not report it) are what distinguishes a
// priority breach from a merely fabricated fill, and they make this tighter than the
// deleted batch scan, which considered every order that ever existed at the price.
type shortedIndex struct {
	byLevel map[levelKey][]shortedOrder
	ring    []shortedOrder
	pos     int
}

func newShortedIndex() *shortedIndex {
	return &shortedIndex{byLevel: make(map[levelKey][]shortedOrder)}
}

func (s *shortedIndex) push(o *model.Order, seq uint64) {
	e := shortedOrder{o: o, seq: seq}
	if len(s.ring) < shortedCapacity {
		s.ring = append(s.ring, e)
	} else {
		s.evict(s.ring[s.pos])
		s.ring[s.pos] = e
		s.pos = (s.pos + 1) % shortedCapacity
	}
	k := levelKey{o.Side, o.Price}
	s.byLevel[k] = append(s.byLevel[k], e)
}

func (s *shortedIndex) evict(e shortedOrder) {
	k := levelKey{e.o.Side, e.o.Price}
	lvl := s.byLevel[k]
	for i := range lvl {
		if lvl[i].o == e.o {
			s.byLevel[k] = append(lvl[:i], lvl[i+1:]...)
			break
		}
	}
	if len(s.byLevel[k]) == 0 {
		delete(s.byLevel, k)
	}
}

func (s *shortedIndex) at(side model.Side, price int64) []shortedOrder {
	return s.byLevel[levelKey{side, price}]
}

// Apply feeds the next order (in EffectiveT3 order) into the reference engine and
// finalizes any orders that left the book as a result.
func (v *StreamValidator) Apply(o *model.Order) {
	// Order-level score denominator, counted as orders arrive. A fill-denominated score
	// structurally cannot express the per-ORDER violation classes (lost orders, lost
	// cancels, FIFO breaches, missed fills), which is how an engine that acked ~216k
	// orders and filled none used to score 1.0.
	v.rep.ScoredOrders++
	v.pending[o.OrderID] = o
	v.engine.Process(o)

	for _, f := range v.engine.DrainFills() {
		r := v.ref[f.OrderID]
		if r == nil {
			r = &orderRef{prices: make(map[int64]struct{})}
			v.ref[f.OrderID] = r
		}
		r.qty += f.Qty
		r.prices[f.Price] = struct{}{}
	}
	// Trades are drained but not inspected for self-matching: the engine applies
	// skip-and-continue, so a same-SMP pair never produces a trade in the first place.
	// A trade-derived self-trade check can therefore never fire — the resting-state
	// check in scoreOrder is the one that can. Draining keeps the engine's buffer from
	// growing O(session).
	v.engine.DrainTrades()

	for _, id := range v.engine.DrainEvicted() {
		v.finalize(id)
	}
	// An order that neither rested nor was evicted (a taker that fully filled, a
	// market order, a cancel/replace action whose own id never rests) is finalized now.
	if _, stillPending := v.pending[o.OrderID]; stillPending && !v.engine.IsResting(o.OrderID) {
		v.finalize(o.OrderID)
	}
}

// Finish finalizes every order still resting (it never left the book), folds in the
// phantom fills, and returns the report.
func (v *StreamValidator) Finish() Report {
	// Finalize in the SAME total order the session was replayed in, not in map
	// iteration order. The per-type counters are order-independent, but the retained
	// Violations examples are not: iterating v.pending directly makes the stored
	// examples differ run to run on byte-identical input, so a re-validated session
	// would produce a different violations payload each time.
	rest := make([]*model.Order, 0, len(v.pending))
	for _, o := range v.pending {
		rest = append(rest, o)
	}
	sort.Slice(rest, func(i, j int) bool { return replay.Less(rest[i], rest[j]) })
	for _, o := range rest {
		v.finalize(o.OrderID)
	}
	// AddPhantom increments PhantomFills only — a phantom never enters TotalFills — so
	// the scored fill denominator is TotalFills as-is. ScoredFills is a reported metric;
	// CorrectnessScore itself is order-level (ScoredOrders/DirtyOrders).
	v.rep.ScoredFills = v.rep.TotalFills
	// A score computed while the platform lost a material share of the responses is a
	// score computed on a sample. Publish it, flagged, rather than presenting it as
	// authoritative — the same contract invariants mode uses for an unreliable T7 stream.
	v.rep.CaptureGaps = v.captureGaps
	total := v.rep.ScoredOrders + v.captureGaps
	if total > 0 && float64(v.captureGaps)/float64(total) > DefaultCaptureGapTaintRate {
		v.rep.Tainted = true
		v.rep.TaintReason = fmt.Sprintf(
			"capture lost the responses for %d of %d orders (%.1f%%): the contestant answered them but no orders.acked record arrived, so they are excluded from the score",
			v.captureGaps, total, 100*float64(v.captureGaps)/float64(total))
	}
	return v.rep
}

// AddPhantom records a fill reported for an order_id that was never sent. Counted as
// the unmatched_responses metric only — NOT a violation class: such a fill fails the
// orders.sent join upstream and never reaches the scored order set, so flagging it was
// double-counting the join.
func (v *StreamValidator) AddPhantom(_ ReportedFill) {
	v.rep.PhantomFills++
}

// AddLost records an order the bot sent that the contestant never answered.
//
// It cannot go through Apply: the eBPF capture derives T3 from the response, so an
// unanswered order has no ingress timestamp and cannot be placed in the replay timeline
// or fed to the reference book. It is graded standalone — counted, flagged, and dirty —
// which is enough, because the failure is unconditional: no response is wrong whatever
// the book would have done.
//
// Without this, dropping an order was free. The join emits an order only once it has an
// ack, so a dropped order never reached this validator at all: it was absent from
// ScoredOrders, and an engine that answered nothing scored 1.0 through the
// empty-session guard, exactly like the acker did before MissedFill.
// AddCaptureGap records an order the bot got a response for whose orders.acked record
// never reached the validator — the platform lost the evidence.
//
// It is deliberately NOT scored in either direction: there is nothing to compare, and
// billing the contestant for it is exactly the mistake that dragged a reference engine
// which answered 100% of 1.84M orders down to a 0.51 score. It is not free either — a
// score computed while a third of the responses are missing is a score computed on a
// sample, so a material rate taints the session (see Finish).
func (v *StreamValidator) AddCaptureGap() {
	v.captureGaps++
}

func (v *StreamValidator) AddLost(orderID string, kind model.Kind) {
	v.rep.ScoredOrders++
	if kind == model.Cancel {
		v.rep.LostCancels++
	} else {
		v.rep.LostOrders++
	}
	v.rep.add(lostViolationType(kind), orderID, 0, 0,
		"order/cancel sent but never received any response")
}

func (v *StreamValidator) finalize(id string) {
	o := v.pending[id]
	if o == nil {
		return // already finalized (or never tracked)
	}
	delete(v.pending, id)
	r := v.ref[id]
	delete(v.ref, id)
	if shorted := v.scoreOrder(o, r); shorted {
		// Read the sequence before Forget drops it: this order stays queue-jump evidence
		// after it has left the book (see shortedIndex).
		if seq, ok := v.engine.SeqOf(id); ok {
			v.shorted.push(o, seq)
		}
	}
	v.engine.Forget(id)
}

// scoreOrder is the per-order comparison, driven by this order's accumulated reference
// fills (r) and the live book. It reports whether the order was UNDER-REPORTED (the
// reference filled it for more than the contestant claimed), which makes it evidence
// for a later queue-jump check — see shortedIndex.
func (v *StreamValidator) scoreOrder(o *model.Order, r *orderRef) bool {
	var cumReported uint64
	for _, resp := range o.Responses {
		if !isFill(resp.ExecType, resp.FillQty) {
			continue
		}
		v.rep.TotalFills++
		cumReported += resp.FillQty
		price := int64(resp.FillPrice)

		switch {
		case cumReported > o.Qty:
			v.rep.Overfills++
			v.rep.add(Overfill, o.OrderID, resp.FillQty, price,
				fmt.Sprintf("cumulative reported %d exceeds order qty %d", cumReported, o.Qty))

		case r == nil:
			v.classifyUnexplained(o, price, resp.FillQty,
				"reference engine produced no fill for this order")

		case !hasPrice(r.prices, price):
			v.classifyUnexplained(o, price, resp.FillQty,
				"reported fill price not produced by the reference engine for this order")

		case cumReported > r.qty:
			v.classifyUnexplained(o, price, resp.FillQty,
				fmt.Sprintf("cumulative reported %d exceeds reference fill qty %d", cumReported, r.qty))

		default:
			v.rep.ValidFills++
		}
	}

	// Under-reporting IS a violation. If the reference matched this order, the
	// contestant owes an execution report for it and silence is a dropped fill. The
	// deleted TestUnderReportIsValid asserted the opposite, but it encoded an
	// undocumented assumption rather than a spec requirement — and it left the platform
	// unable to tell a correct engine from one that ACKs everything and matches nothing,
	// since every other check is driven by a fill the contestant reported. An engine
	// reporting none was untouched by all of them and scored a clean 1.0.
	if r != nil && cumReported < r.qty {
		v.rep.MissedFills++
		v.rep.add(MissedFill, o.OrderID, cumReported, 0,
			fmt.Sprintf("reference engine filled %d for this order; contestant reported %d",
				r.qty, cumReported))
		return true
	}
	return false
}

// classifyUnexplained names the rule broken by a fill the reference did not produce.
//
// It is only ever reached for a fill that is ALREADY wrong — the reference produced no
// fill for this order, none at this price, or less than the contestant claims. Its job is
// to say WHY, in decreasing order of specificity: a self-match, then a queue jump, then
// the generic price violation.
//
// The self-match test lives here, and not ahead of the reference comparison, because "an
// order sharing my SMP id rests at this price" is the ordinary state of a pass-1 book:
// eight ids rotate across one connection, so a busy level routinely holds the aggressor's
// own id alongside everyone else's. Testing it first therefore reclassified perfectly
// legitimate fills — the reference skips the same-id maker under skip-and-continue and
// matches the NEXT one, the contestant reports exactly that fill, and it was scored a
// self-trade because the skipped order was still sitting there. Against a real pass-1 run
// that turned a correct engine's clean crosses into violations.
func (v *StreamValidator) classifyUnexplained(o *model.Order, price int64, qty uint64, detail string) {
	if v.selfMatchedAgainstBook(o, price) {
		v.rep.SelfTrades++
		v.rep.add(SelfTrade, o.OrderID, qty, price,
			"filled against a resting order carrying the same self-match-prevention id")
		return
	}
	if jumper, ok := v.queueJump(o, price); ok {
		v.rep.flagJump(v.engine, o, jumper, qty, price)
		return
	}
	v.rep.PriceViolations++
	v.rep.add(Price, o.OrderID, qty, price, detail)
}

// selfMatchedAgainstBook reports whether the contestant filled `o` at `price` against a
// resting order carrying the SAME self-match-prevention id.
//
// An order with no SMP id cannot self-match — absent means unconstrained, which is what
// keeps pass-2 traffic (which carries no id at all) unaffected by this check.
func (v *StreamValidator) selfMatchedAgainstBook(o *model.Order, price int64) bool {
	if !o.HasSMPID {
		return false
	}
	return v.engine.RestingSMPMatch(oppositeOf(o.Side), price, o.SMPID)
}

// oppositeOf performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func oppositeOf(s model.Side) model.Side {
	if s == model.Buy {
		return model.Sell
	}
	return model.Buy
}

// queueJump finds the earliest order that should have had time priority over o at the
// same price+side (lower book sequence, not a cross-flow tie).
//
// Two populations are searched: orders still RESTING (the live queue), and orders that
// have left the book but were UNDER-REPORTED by the contestant (shortedIndex) — the
// victims of a jump usually depart before the jumper is scored, which is why searching
// the live book alone left this check unable to fire.
func (v *StreamValidator) queueJump(o *model.Order, price int64) (string, bool) {
	mySeq, ok := v.engine.SeqOf(o.OrderID)
	if !ok {
		return "", false // o never rested, so it never queued
	}
	var (
		best    string
		bestSeq uint64
		found   bool
	)
	consider := func(id string, other *model.Order, otherSeq uint64) {
		if id == o.OrderID || other.Side != o.Side || other.Price != price {
			return
		}
		if otherSeq >= mySeq {
			return
		}
		if replay.CrossFlowTie(o, other) {
			return
		}
		if !found || otherSeq < bestSeq || (otherSeq == bestSeq && id < best) {
			best, bestSeq, found = id, otherSeq, true
		}
	}
	for id, other := range v.pending {
		otherSeq, ok := v.engine.SeqOf(id)
		if !ok {
			continue
		}
		consider(id, other, otherSeq)
	}
	for _, e := range v.shorted.at(o.Side, price) {
		consider(e.o.OrderID, e.o, e.seq)
	}
	return best, found
}

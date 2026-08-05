// Package book implements book behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package book

import (
	"github.com/google/btree"
	"github.com/iicpc/correctness-validator/internal/model"
)

// Fill groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Fill struct {
	OrderID string
	Price   int64
	Qty     uint64
}

// Trade groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Trade struct {
	MakerOrderID string
	TakerOrderID string
	Price        int64
	Qty          uint64
	MakerSeq     uint64
	// Self-match-prevention identity of both sides, carried here so scoring keys on
	// what was actually on the wire. Identity used to be re-derived by parsing the TASK
	// id out of the order id — and pass 1 runs exactly one task, so every order looked
	// like the same participant, every trade looked like a self-trade, and not one of
	// 1.35M fills was ever accepted. That parser is deleted.
	MakerSMPID    uint32
	MakerHasSMPID bool
	TakerSMPID    uint32
	TakerHasSMPID bool
}

// RestingState groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RestingState struct {
	OrderID   string
	Side      model.Side
	Price     int64
	Seq       uint64
	Remaining uint64
	// SMP identity, so scoring can tell whether a contestant's reported fill matched
	// an order sharing its self-match-prevention id.
	SMPID    uint32
	HasSMPID bool
}

// restingOrder groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type restingOrder struct {
	orderID   string
	side      model.Side
	price     int64
	remaining uint64
	seq       uint64 // arrival rank (FIFO/time priority within a level)
	// smpID is the resting order's self-match-prevention id, valid only when hasSMP.
	// An aggressor carrying the same id skips this order (see matchAndRest).
	smpID  uint32
	hasSMP bool
}

// priceLevel groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type priceLevel struct {
	price  int64
	orders []*restingOrder // FIFO: front = oldest = time priority
}

// Engine groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Engine struct {
	asks       *btree.BTreeG[*priceLevel] // ascending: best ask = Min
	bids       *btree.BTreeG[*priceLevel] // descending: best bid = Min (highest price)
	index      map[string]*restingOrder
	seq        uint64
	fills      []Fill
	trades     []Trade
	repriced   map[string]bool
	seqByOrder map[string]uint64
	// evicted accumulates the order IDs removed from the book during the current
	// Process call (maker fully consumed, cancel, replace-remove). The streaming
	// validator drains it after each Process to finalize departed orders.
	evicted []string
}

// NewEngine performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewEngine() *Engine {
	return &Engine{
		asks:       btree.NewG[*priceLevel](32, func(a, b *priceLevel) bool { return a.price < b.price }),
		bids:       btree.NewG[*priceLevel](32, func(a, b *priceLevel) bool { return a.price > b.price }),
		index:      make(map[string]*restingOrder),
		repriced:   make(map[string]bool),
		seqByOrder: make(map[string]uint64),
	}
}

// Repriced applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Repriced(orderID string) bool { return e.repriced[orderID] }

// SeqOf applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) SeqOf(orderID string) (uint64, bool) {
	s, ok := e.seqByOrder[orderID]
	return s, ok
}

// Fills applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Fills() []Fill { return e.fills }

// Trades applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Trades() []Trade { return e.trades }

// DrainFills returns the fills produced since the last drain and clears the buffer,
// so a streaming consumer attributes them per-order without the engine retaining
// O(session) fills. (Batch callers use Fills() and never drain.)
func (e *Engine) DrainFills() []Fill {
	f := e.fills
	e.fills = nil
	return f
}

// DrainTrades returns + clears trades produced since the last drain (see DrainFills).
func (e *Engine) DrainTrades() []Trade {
	t := e.trades
	e.trades = nil
	return t
}

// DrainEvicted returns + clears the order IDs removed from the book during the
// Process call(s) since the last drain, so the streaming validator can finalize them.
func (e *Engine) DrainEvicted() []string {
	ev := e.evicted
	e.evicted = nil
	return ev
}

// Forget drops the per-order metadata (seq, repriced) for an order the streaming
// validator has finalized, bounding those maps to live orders. Batch never calls it.
func (e *Engine) Forget(orderID string) {
	delete(e.seqByOrder, orderID)
	delete(e.repriced, orderID)
}

// IsResting reports whether the order is currently in the book.
func (e *Engine) IsResting(orderID string) bool {
	_, ok := e.index[orderID]
	return ok
}

// RestingSMPMatch reports whether an order carrying `smpID` is currently resting on
// `side` at `price`.
//
// This is how a contestant self-match is detected, and the resting state is the ONLY
// evidence available: the engine applies skip-and-continue, so for a genuine self-match
// it produces no trade at all and there is nothing in Trades()/DrainTrades() to compare
// against. Scanning one price level keeps this O(level) — Resting() would copy the whole
// book on every fill.
func (e *Engine) RestingSMPMatch(side model.Side, price int64, smpID uint32) bool {
	level, ok := e.tree(side).Get(&priceLevel{price: price})
	if !ok {
		return false
	}
	for _, ro := range level.orders {
		if ro.hasSMP && ro.smpID == smpID {
			return true
		}
	}
	return false
}

// Resting applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Resting() map[string]RestingState {
	out := make(map[string]RestingState, len(e.index))
	for id, ro := range e.index {
		out[id] = RestingState{
			OrderID: id, Side: ro.side, Price: ro.price, Seq: ro.seq, Remaining: ro.remaining,
			SMPID: ro.smpID, HasSMPID: ro.hasSMP,
		}
	}
	return out
}

// tree applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) tree(side model.Side) *btree.BTreeG[*priceLevel] {
	if side == model.Buy {
		return e.bids
	}
	return e.asks
}

// Process applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Process(o *model.Order) {
	switch o.Kind {
	case model.NewLimit:
		e.matchAndRest(o, true)
	case model.NewMarket:
		e.matchAndRest(o, false)
	case model.Cancel:
		e.remove(o.OrigOrderID)
	case model.Replace:
		e.replace(o)
	}
}

// matchAndRest applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) matchAndRest(o *model.Order, rest bool) {
	remaining := o.Qty
	opp := oppositeSide(o.Side)
	oppTree := e.tree(opp)
	// Levels temporarily removed because every maker in them shares the aggressor's
	// SMP id; restored before this call returns (see the loop's tail).
	var skippedLevels []*priceLevel

	for remaining > 0 {
		level, ok := oppTree.Min()
		if !ok {
			break
		}
		if o.Kind == model.NewLimit && !crosses(o.Side, o.Price, level.price) {
			break
		}
		// skipped holds makers passed over by self-match prevention. They are NOT
		// cancelled — skip-and-continue leaves them resting — but they must come out
		// of the front of the FIFO so the loop can reach the next maker, then go back
		// in their original order once this aggressor is done.
		var skipped []*restingOrder
		for len(level.orders) > 0 && remaining > 0 {
			maker := level.orders[0]
			// Self-match prevention: an aggressor never matches a resting order
			// carrying the same SMP id. SMPIDNone is unconstrained on either side, so
			// two id-less orders (all of pass-2's traffic) match exactly as before.
			if o.HasSMPID && maker.hasSMP && maker.smpID == o.SMPID {
				skipped = append(skipped, maker)
				level.orders = level.orders[1:]
				continue
			}
			traded := min(remaining, maker.remaining)
			e.fills = append(e.fills,
				Fill{OrderID: o.OrderID, Price: level.price, Qty: traded},
				Fill{OrderID: maker.orderID, Price: level.price, Qty: traded},
			)
			e.trades = append(e.trades, Trade{
				MakerOrderID:  maker.orderID,
				TakerOrderID:  o.OrderID,
				Price:         level.price,
				Qty:           traded,
				MakerSeq:      maker.seq,
				MakerSMPID:    maker.smpID,
				MakerHasSMPID: maker.hasSMP,
				TakerSMPID:    o.SMPID,
				TakerHasSMPID: o.HasSMPID,
			})
			maker.remaining -= traded
			remaining -= traded
			if maker.remaining == 0 {
				level.orders = level.orders[1:]
				delete(e.index, maker.orderID)
				e.evicted = append(e.evicted, maker.orderID)
			}
		}
		// Put skipped makers back at the FRONT: they arrived before anything still in
		// the level, so restoring them elsewhere would silently reorder time priority
		// and show up as a FIFO violation against a correct engine.
		if len(skipped) > 0 {
			level.orders = append(skipped, level.orders...)
		}
		if len(level.orders) == 0 {
			oppTree.Delete(level)
			continue
		}
		if o.HasSMPID && allSameSMP(level.orders, o.SMPID) {
			// Every remaining maker at this level shares the aggressor's id, so this
			// level can yield nothing more. The aggressor must still CONTINUE to the
			// next price level — that is the "continue" in skip-and-continue — so the
			// level is parked out of the tree for this call and restored afterwards
			// rather than breaking out of the match loop entirely.
			skippedLevels = append(skippedLevels, level)
			oppTree.Delete(level)
			continue
		}
		// Level still has matchable makers but the aggressor stopped (filled, or a
		// limit price that no longer crosses): nothing further to do here.
		break
	}
	// Restore levels parked by self-match prevention. They were never cancelled — the
	// orders in them are still live and must be visible to the NEXT aggressor.
	for _, lv := range skippedLevels {
		oppTree.ReplaceOrInsert(lv)
	}

	if rest && remaining > 0 && o.Kind == model.NewLimit {
		e.insert(o, remaining)
	}
}

// allSameSMP reports whether every order in the level shares `smp`. Used to break out
// of a price level the aggressor can make no further progress against, so a fully
// self-crossing book terminates instead of spinning.
func allSameSMP(orders []*restingOrder, smp uint32) bool {
	for _, o := range orders {
		if !o.hasSMP || o.smpID != smp {
			return false
		}
	}
	return len(orders) > 0
}

// insert applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) insert(o *model.Order, remaining uint64) {
	e.seq++
	ro := &restingOrder{orderID: o.OrderID, side: o.Side, price: o.Price, remaining: remaining, seq: e.seq, smpID: o.SMPID, hasSMP: o.HasSMPID}
	e.index[o.OrderID] = ro
	e.seqByOrder[o.OrderID] = e.seq
	tree := e.tree(o.Side)
	key := &priceLevel{price: o.Price}
	if level, ok := tree.Get(key); ok {
		level.orders = append(level.orders, ro)
	} else {
		key.orders = []*restingOrder{ro}
		tree.ReplaceOrInsert(key)
	}
}

// remove applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) remove(orderID string) {
	ro, ok := e.index[orderID]
	if !ok {
		return
	}
	tree := e.tree(ro.side)
	if level, ok := tree.Get(&priceLevel{price: ro.price}); ok {
		for i, x := range level.orders {
			if x.orderID == orderID {
				level.orders = append(level.orders[:i], level.orders[i+1:]...)
				break
			}
		}
		if len(level.orders) == 0 {
			tree.Delete(level)
		}
	}
	delete(e.index, orderID)
	e.evicted = append(e.evicted, orderID)
}

// replace applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) replace(o *model.Order) {
	ro, ok := e.index[o.OrigOrderID]
	if !ok {
		return
	}
	if o.Price == ro.price && o.Qty <= ro.remaining {
		ro.remaining = o.Qty
		if o.OrderID != "" && o.OrderID != ro.orderID {
			oldID := ro.orderID
			delete(e.index, oldID)
			ro.orderID = o.OrderID
			e.index[o.OrderID] = ro
			if s, ok := e.seqByOrder[oldID]; ok {
				e.seqByOrder[o.OrderID] = s
				delete(e.seqByOrder, oldID)
			}
		}
		return
	}
	if o.Price != ro.price {
		e.repriced[orReplaceID(o)] = true
	}
	// The re-inserted order inherits the ORIGINAL resting order's SMP identity. A
	// replace does not change who owns the order, and rebuilding it without the id
	// would silently exempt every repriced order from self-match prevention: the
	// reference would then match it against a same-id aggressor that a compliant
	// contestant correctly skipped, and score the contestant for a missed fill.
	// Taking it from `ro` rather than from `o` also means this holds whether or not
	// the wire carries the id on the replace message itself.
	smpID, hasSMP := ro.smpID, ro.hasSMP
	e.remove(o.OrigOrderID)
	repl := &model.Order{OrderID: orReplaceID(o), Side: o.Side, Price: o.Price}
	repl.SMPID, repl.HasSMPID = smpID, hasSMP
	e.insert(repl, o.Qty)
}

// orReplaceID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func orReplaceID(o *model.Order) string {
	if o.OrderID != "" {
		return o.OrderID
	}
	return o.OrigOrderID
}

// crosses performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func crosses(aggressorSide model.Side, aggressorPrice, restingPrice int64) bool {
	if aggressorSide == model.Buy {
		return restingPrice <= aggressorPrice
	}
	return restingPrice >= aggressorPrice
}

// oppositeSide performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func oppositeSide(s model.Side) model.Side {
	if s == model.Buy {
		return model.Sell
	}
	return model.Buy
}

// min performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

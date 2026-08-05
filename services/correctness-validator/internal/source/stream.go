// Streaming session source: the only path that reads a session off Kafka. (It replaced
// a whole-session drain-then-validate pass, since deleted.)
//
// It k-way merges every partition of orders.sent AND orders.acked by event time
// (sent → SendTSNS, ack → T3 ingress; the bot and eBPF nodes are NTP-synced within
// a few ms, so these are comparable within a small skew). A single consumer walks
// that merged stream, joins each order to its acks within a bounded join window, and
// feeds joined orders into the windowed Reorderer, which emits them in EXACTLY
// replay.Order's order (proven in reorder_test.go) to the validator.
//
// Memory is bounded everywhere: one batch buffered per partition reader, a join
// buffer holding ~JoinWindow worth of orders, the Reorderer's window, and the
// engine's live book. Nothing is O(session). Per-order build + EffectiveT3 promotion
// match the batch path exactly, so scores are identical.
package source

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/pipeline"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// DefaultReorderWindow caps the in-flight reorder buffer (orders).
	DefaultReorderWindow = 1 << 20
	// DefaultJoinWindowNS: an order's ack arrives within this of its send (NTP skew +
	// network RTT + jitter). 500ms is generous; orders are emitted once the merge's
	// event-time watermark passes their send time + this window.
	DefaultJoinWindowNS = uint64(500 * 1000 * 1000)
	partitionChanDepth  = 4
	// maxGapSamples bounds the capture-gap send-time sample used for diagnosis.
	maxGapSamples = 200_000
)

// StreamCounts is the per-session event accounting for the store record.
type StreamCounts struct {
	SentEvents    uint64
	AckedEvents   uint64
	MatchedOrders uint64
	// LostOrders counts orders sent on the wire that the BOT never got a response for.
	// Separate from MatchedOrders because these never become model.Orders — they have
	// no ingress timestamp to place them in the replay.
	LostOrders uint64
	// CaptureGapLastSecond counts capture gaps whose order was SENT within the final
	// second of the session, and CaptureGapSpreadNs is the send-time span between the
	// earliest and latest gap. Together they separate the two candidate causes: a
	// teardown race (the capture stops while the last responses are still arriving)
	// concentrates gaps at the very end, while a steady-state loss spreads them across
	// the whole run.
	CaptureGapLastSecond uint64
	CaptureGapSpreadNs   uint64
	// CaptureGaps counts orders the bot got a response for whose orders.acked record
	// never reached this validator. These are the PLATFORM's losses, not the
	// contestant's, and are excluded from scoring in both directions. A non-trivial
	// rate here means the score was computed on a sample, which is why it taints the
	// result rather than being logged and forgotten.
	CaptureGaps uint64
}

// tsBatch is one decoded batch tagged with its lead event time, for the merge.
type tsBatch struct {
	ts    uint64 // sent: first event SendTSNS; ack: first event T3
	isAck bool
	sent  []topics.OrderSentEvent
	acks  []topics.OrderAckedEvent
}

// pendingOrder holds an order's sent + accumulated acks until it is emitted.
type pendingOrder struct {
	sent    topics.OrderSentEvent
	hasSent bool
	acks    []topics.OrderAckedEvent
	t3      uint64 // ack ingress time (shared across an order's acks); 0 until first ack
	sendTS  uint64 // sent send time; 0 until sent seen
}

// pendingOutcome is what the join decided a pending order is.
type pendingOutcome int

const (
	// outcomeMatched: sent and answered — a scoreable order.
	outcomeMatched pendingOutcome = iota
	// outcomeLost: sent on the wire and the BOT never got a response. The contestant
	// dropped it, and it is scored against them.
	outcomeLost
	// outcomeCaptureGap: sent, and the bot got a response, but no orders.acked record
	// for it reached this validator. The platform lost the evidence, so the order is
	// not scoreable either way — excluded from the denominator, counted separately.
	outcomeCaptureGap
	// outcomeUnsent: responses for an order_id with no sent event (phantom fills), or
	// nothing at all.
	outcomeUnsent
)

// outcome classifies this pending order for emitReady.
//
// The distinction between outcomeLost and outcomeCaptureGap is the whole point. An
// absent orders.acked record does NOT prove the contestant failed to answer: on a
// measured 1.84M-order pass-1 run the reference engine answered 100% of orders (the bot
// recorded a response for every one, zero timeouts) while the capture published only
// 64% of them. Treating "no ack captured" as a dropped order billed that engine for
// 668k of the platform's own losses and dragged a correct engine to a 0.51 score.
//
// The bot is an independent witness and reports its verdict per order on the wire:
// TimedOut means the bot itself waited and got nothing. That — not the absence of a
// capture record — is what the contestant is accountable for. RecvDoneTSNS != 0 is
// positive proof the response existed and the capture is what lost it. When the bot
// asserts neither, nothing is provable, so the order is not scored.
func (p *pendingOrder) outcome() pendingOutcome {
	if !p.hasSent {
		return outcomeUnsent
	}
	if len(p.acks) > 0 {
		return outcomeMatched
	}
	if p.sent.TimedOut {
		return outcomeLost
	}
	return outcomeCaptureGap
}

// addAck appends an ack unless it is a redelivery of one already held for this order,
// reporting whether it was new.
//
// Production is at-least-once: a producer retry republishes a whole batch, and without
// this the same execution report is counted twice — inflating cumulative reported qty
// and manufacturing overfill violations against an engine that did nothing wrong. The
// deleted batch collector deduped on exactly this key (order id, exec type, T7); the
// streaming join never did, so the dedup would have been lost with the batch path.
//
// The scan is linear over the acks already held for THIS order (a handful: an ack plus
// its fills), which is why there is no per-order map to allocate 445k times. Scope is
// the join window rather than the whole session, so a redelivery arriving after the
// order was emitted is not caught — that is a bounded-memory trade, and such a straggler
// surfaces as a phantom fill rather than being silently double-counted.
func (p *pendingOrder) addAck(a topics.OrderAckedEvent) bool {
	for _, held := range p.acks {
		if held.ExecType == a.ExecType && held.T7XDPEgressNS == a.T7XDPEgressNS &&
			held.FillQty == a.FillQty && held.FillPrice == a.FillPrice {
			return false
		}
	}
	p.acks = append(p.acks, a)
	return true
}

// bandPartitionSet returns the set of partition IDs covered by band (mirroring
// topics.BandPartition's own base/wrap arithmetic), for numPartitions total
// partitions and bandWidth partitions per band. Used to restrict readers to
// only the session's exclusively-leased band instead of scanning every
// partition (see docs/multi-contestant-audit.md order-band section).
func bandPartitionSet(band uint32, numPartitions, bandWidth int32) map[int]struct{} {
	if numPartitions <= 1 {
		return map[int]struct{}{0: {}}
	}
	if bandWidth < 1 {
		bandWidth = 1
	}
	if bandWidth > numPartitions {
		bandWidth = numPartitions
	}
	base := int32(int64(band) * int64(bandWidth))
	set := make(map[int]struct{}, bandWidth)
	for i := int32(0); i < bandWidth; i++ {
		set[int((base+i)%numPartitions)] = struct{}{}
	}
	return set
}

// filterPartitions keeps only the partitions in allowed, preserving order. A
// nil allowed means "no restriction" (back-compat: OrderBandUnset).
func filterPartitions(parts []kafka.Partition, allowed map[int]struct{}) []kafka.Partition {
	if allowed == nil {
		return parts
	}
	out := make([]kafka.Partition, 0, len(allowed))
	for _, p := range parts {
		if _, ok := allowed[p.ID]; ok {
			out = append(out, p)
		}
	}
	return out
}

// StreamSession validates a session with bounded memory. apply is called with each
// order in EffectiveT3 order; addPhantom with each fill reported for an order_id that
// was never sent; addLost with each order sent on the wire that got no response at all
// (it has no ingress timestamp, so it cannot be replayed — see emitReady).
// orderBand/bandWidth restrict the partitions read to that
// session's exclusively-leased band (4x less broker read amplification than a
// full-topic scan); pass topics.OrderBandUnset for orderBand to fall back to
// reading every partition (back-compat with band-unaware sessions). The
// session-id filter in decodeBatch stays in effect regardless, as a guard
// against stale-tail cross-band leakage. Returns event counts and the
// contestant id.
func StreamSession(
	ctx context.Context,
	brokers []string,
	sessionID string,
	window int,
	orderBand uint32,
	bandWidth int32,
	apply func(*model.Order),
	addPhantom func(orderID string, qty uint64, price int64),
	addLost func(orderID string, kind model.Kind),
	addCaptureGap func(),
) (StreamCounts, string, error) {
	if window <= 0 {
		window = DefaultReorderWindow
	}
	var counts StreamCounts
	if len(brokers) == 0 {
		return counts, "", fmt.Errorf("no kafka brokers configured")
	}

	// Discover partitions of both topics.
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return counts, "", fmt.Errorf("dial broker: %w", err)
	}
	sentParts, errS := conn.ReadPartitions(topics.TopicOrdersSent)
	ackParts, errA := conn.ReadPartitions(topics.TopicOrdersAcked)
	conn.Close()
	if errS != nil {
		return counts, "", fmt.Errorf("read partitions %s: %w", topics.TopicOrdersSent, errS)
	}
	if errA != nil {
		return counts, "", fmt.Errorf("read partitions %s: %w", topics.TopicOrdersAcked, errA)
	}

	if orderBand != topics.OrderBandUnset {
		sentParts = filterPartitions(sentParts, bandPartitionSet(orderBand, int32(len(sentParts)), bandWidth))
		ackParts = filterPartitions(ackParts, bandPartitionSet(orderBand, int32(len(ackParts)), bandWidth))
	}

	mctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Launch one reader per (topic, partition); each owns a bounded channel.
	type stream struct {
		ch   chan tsBatch
		head tsBatch
		live bool
		errp *error
	}
	var streams []*stream
	launch := func(topic string, partition int, isAck bool) {
		ch := make(chan tsBatch, partitionChanDepth)
		var rerr error
		s := &stream{ch: ch, errp: &rerr}
		streams = append(streams, s)
		go func() {
			defer close(ch)
			rerr = streamPartition(mctx, brokers, topic, partition, sessionID, isAck, ch)
		}()
	}
	for _, p := range sentParts {
		launch(topics.TopicOrdersSent, p.ID, false)
	}
	for _, p := range ackParts {
		launch(topics.TopicOrdersAcked, p.ID, true)
	}

	// Prime each stream's head.
	for _, s := range streams {
		if b, ok := <-s.ch; ok {
			s.head, s.live = b, true
		}
	}

	// Send times of capture-gap orders, for the teardown-vs-steady-state diagnosis
	// below. Bounded: a session that loses more than this has a problem the exact
	// distribution will not change the diagnosis of.
	var gapSendTS []uint64
	var maxSendTS uint64
	pending := make(map[string]*pendingOrder)
	order := make([]string, 0, 1024) // insertion order of pending ids, for FIFO emit
	contestant := ""
	duplicateAcks := 0
	r := NewReorderer(window, apply)
	joinWindow := DefaultJoinWindowNS

	// emitReady flushes orders whose send time is older than watermark-joinWindow
	// (all their acks have arrived) into the reorderer; if watermark==0 it flushes all.
	emitReady := func(watermark uint64, flushAll bool) {
		cut := 0
		for _, id := range order {
			po := pending[id]
			if po == nil {
				cut++
				continue // already emitted
			}
			ref := po.sendTS
			if ref == 0 {
				ref = po.t3
			}
			if !flushAll && ref+joinWindow > watermark {
				break // this and everything after it (FIFO by send time) is too recent
			}
			switch po.outcome() {
			case outcomeMatched:
				o := pipeline.AssembleOrder(po.sent, po.acks)
				if o != nil {
					counts.MatchedOrders++
					r.Push(o)
				}
			case outcomeLost:
				// Sent, and the bot waited and got nothing. This does NOT go through
				// the reorderer or the reference book: T3 comes from the response
				// capture, so an unanswered order has no ingress timestamp and no place
				// in the replay timeline. It is reported straight to the validator,
				// which grades it standalone. Discarding it here — the previous
				// behavior — meant dropping an order cost the contestant nothing.
				counts.LostOrders++
				addLost(id, model.KindFrom(po.sent.PayloadType, po.sent.OrdType))
			case outcomeCaptureGap:
				// The response existed (the bot saw it) but no capture record reached
				// us. Not scoreable in either direction — see pendingOrder.outcome.
				counts.CaptureGaps++
				if po.sent.SendTSNS != 0 && len(gapSendTS) < maxGapSamples {
					gapSendTS = append(gapSendTS, po.sent.SendTSNS)
				}
				addCaptureGap()
			case outcomeUnsent:
				for _, a := range po.acks { // ack with no sent → phantom fills
					if streamIsFill(a.ExecType, a.FillQty) {
						addPhantom(id, a.FillQty, int64(a.FillPrice))
					}
				}
			}
			delete(pending, id)
			cut++
		}
		if cut > 0 {
			order = order[cut:]
		}
	}

	// k-way merge by event time.
	for {
		// pick the live stream with the smallest head.ts
		min := -1
		for i, s := range streams {
			if s.live && (min < 0 || s.head.ts < streams[min].head.ts) {
				min = i
			}
		}
		if min < 0 {
			break // all streams drained
		}
		b := streams[min].head
		// process this batch
		if b.isAck {
			for _, a := range b.acks {
				counts.AckedEvents++
				if contestant == "" {
					contestant = a.ContestantID
				}
				po := pending[a.OrderID]
				if po == nil {
					po = &pendingOrder{}
					pending[a.OrderID] = po
					order = append(order, a.OrderID)
				}
				if !po.addAck(a) {
					duplicateAcks++
					continue
				}
				if po.t3 == 0 {
					po.t3 = a.T3XDPIngressNS
				}
			}
		} else {
			for _, s := range b.sent {
				counts.SentEvents++
				// Session end, for locating capture gaps in the timeline.
				if s.SendTSNS > maxSendTS {
					maxSendTS = s.SendTSNS
				}
				po := pending[s.OrderID]
				if po == nil {
					po = &pendingOrder{}
					pending[s.OrderID] = po
					order = append(order, s.OrderID)
				}
				po.sent, po.hasSent, po.sendTS = s, true, s.SendTSNS
			}
		}
		emitReady(b.ts, false)
		// refill head
		if nb, ok := <-streams[min].ch; ok {
			streams[min].head = nb
		} else {
			streams[min].live = false
		}
	}

	// drain: emit everything left, then flush the reorderer.
	emitReady(0, true)
	r.Flush()

	for _, s := range streams {
		if s.errp != nil && *s.errp != nil {
			return counts, contestant, fmt.Errorf("partition reader: %w", *s.errp)
		}
	}
	if len(gapSendTS) > 0 && maxSendTS > 0 {
		lo, hi := gapSendTS[0], gapSendTS[0]
		for _, ts := range gapSendTS {
			if ts < lo {
				lo = ts
			}
			if ts > hi {
				hi = ts
			}
			if maxSendTS-ts <= uint64(time.Second) {
				counts.CaptureGapLastSecond++
			}
		}
		counts.CaptureGapSpreadNs = hi - lo
	}
	if duplicateAcks > 0 {
		metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.",
			metrics.Labels("topic", "orders_acked_duplicates"), float64(duplicateAcks))
		slog.Warn("stream: dropped duplicate orders.acked events", "session_id", sessionID, "duplicates", duplicateAcks)
	}
	metrics.Histogram("validator_session_events_buffered",
		"Bounded in-flight orders per validated session (streaming).", nil, float64(window))
	return counts, contestant, nil
}

func streamIsFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

// streamPartition reads [start,last) of one (topic,partition) for the session and
// sends each decoded batch (tagged with its lead event time) to ch.
func streamPartition(ctx context.Context, brokers []string, topic string, partition int, sessionID string, isAck bool, ch chan<- tsBatch) error {
	start, last, err := partitionOffsets(ctx, brokers[0], topic, partition, sessionID)
	if err != nil {
		return err
	}
	if start >= last {
		return nil
	}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: partition, MinBytes: 1, MaxBytes: 10 << 20})
	defer r.Close()
	if err := r.SetOffset(start); err != nil {
		return fmt.Errorf("set offset %s/%d: %w", topic, partition, err)
	}
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read %s/%d: %w", topic, partition, err)
		}
		if b, ok := decodeBatch(m, sessionID, isAck); ok {
			select {
			case ch <- b:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if m.Offset >= last-1 {
			break
		}
	}
	return nil
}

// decodeBatch unmarshals a Kafka message into a tsBatch for the session, or ok=false
// to skip (decode error, other session, or empty).
func decodeBatch(m kafka.Message, sessionID string, isAck bool) (tsBatch, bool) {
	if isAck {
		var b topics.OrderAckedBatch
		if err := msgpack.Unmarshal(m.Value, &b); err != nil {
			recordDecodeError(topics.TopicOrdersAcked, m, err)
			return tsBatch{}, false
		}
		if b.SessionID != sessionID || len(b.Events) == 0 {
			return tsBatch{}, false
		}
		return tsBatch{ts: b.Events[0].T3XDPIngressNS, isAck: true, acks: b.Events}, true
	}
	// orders.sent uses the POSITIONAL V2 envelope (Rust OrderSentBatchV2Ref) with
	// session_id/submission_id/worker_id hoisted out of the per-event payload. Decoding
	// it as the older named-map OrderSentBatch does not error — it silently yields a
	// garbage SessionID, so the filter below dropped every batch and this validator
	// reported sent=0 against a topic holding ~600k records. See
	// schemas/go/topics/wire_contract_test.go, which pins the layout against real
	// producer bytes.
	var b topics.OrderSentBatchV2
	if err := msgpack.Unmarshal(m.Value, &b); err != nil {
		recordDecodeError(topics.TopicOrdersSent, m, err)
		return tsBatch{}, false
	}
	if b.SessionID != sessionID || len(b.Events) == 0 {
		return tsBatch{}, false
	}
	events := b.IntoEvents()
	return tsBatch{ts: events[0].SendTSNS, isAck: false, sent: events}, true
}

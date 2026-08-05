package source

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

// TestBandPartitionSet checks the pure partition-selection helper against
// topics.BandPartition's own base/wrap arithmetic: every orderID's placement
// (topics.BandPartition) must land inside its band's set, and a different
// band's set must never contain it.
func TestBandPartitionSet(t *testing.T) {
	const numPartitions, bandWidth = int32(24), int32(6)
	for band := uint32(0); band < 4; band++ {
		set := bandPartitionSet(band, numPartitions, bandWidth)
		if len(set) != int(bandWidth) {
			t.Fatalf("band %d: got %d partitions, want %d", band, len(set), bandWidth)
		}
		for i := 0; i < 50; i++ {
			orderID := fmt.Sprintf("order-%d-%d", band, i)
			p := topics.BandPartition(band, orderID, numPartitions, bandWidth)
			if _, ok := set[int(p)]; !ok {
				t.Fatalf("band %d: BandPartition placed %q on partition %d, outside bandPartitionSet %v", band, orderID, p, set)
			}
		}
	}
	// Bands must be disjoint (exclusive lease invariant).
	s0 := bandPartitionSet(0, numPartitions, bandWidth)
	s2 := bandPartitionSet(2, numPartitions, bandWidth)
	for p := range s0 {
		if _, ok := s2[p]; ok {
			t.Fatalf("band 0 and band 2 partition sets overlap at partition %d: %v vs %v", p, s0, s2)
		}
	}
}

// TestFilterPartitions_NilAllowedIsNoRestriction covers the OrderBandUnset
// back-compat path: no allowed set means every discovered partition is kept.
func TestFilterPartitions_NilAllowedIsNoRestriction(t *testing.T) {
	parts := []kafka.Partition{{ID: 0}, {ID: 1}, {ID: 2}}
	got := filterPartitions(parts, nil)
	if len(got) != len(parts) {
		t.Fatalf("filterPartitions(nil) = %d partitions, want %d (unrestricted)", len(got), len(parts))
	}
}

// TestFilterPartitions_RestrictsToAllowed covers the band-restricted path.
func TestFilterPartitions_RestrictsToAllowed(t *testing.T) {
	parts := []kafka.Partition{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}
	got := filterPartitions(parts, map[int]struct{}{1: {}, 3: {}})
	if len(got) != 2 || got[0].ID != 1 || got[1].ID != 3 {
		t.Fatalf("filterPartitions restricted = %+v, want partitions [1,3]", got)
	}
}

// writeEventsToPartition msgpack-encodes a single-topic batch and writes it
// directly to an explicit partition (bypassing the balancer), so the test can
// place events in a specific band regardless of the live topic's key hashing.
func writeEventsToPartition(ctx context.Context, t *testing.T, brokers []string, topic string, partition int, payload []byte) {
	t.Helper()
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, partition)
	if err != nil {
		t.Fatalf("dial leader %s/%d: %v", topic, partition, err)
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.WriteMessages(kafka.Message{Value: payload}); err != nil {
		t.Fatalf("write %s/%d: %v", topic, partition, err)
	}
}

// sentBatchPayload encodes the POSITIONAL V2 envelope the real bot-fleet producer
// writes. It must not use the older named-map OrderSentBatch: that encoding is not what
// StreamSession decodes, so a V1 fixture would make this test pass while the production
// path stayed broken — which is precisely how sent=0 went unnoticed.
func sentBatchPayload(t *testing.T, sid string, events []topics.OrderSentEvent) []byte {
	t.Helper()
	fields := make([]topics.OrderSentEventFields, 0, len(events))
	var submissionID, workerID string
	if len(events) > 0 {
		submissionID, workerID = events[0].SubmissionID, events[0].WorkerID
	}
	if workerID == "" {
		workerID = "w0"
	}
	for _, e := range events {
		fields = append(fields, topics.OrderSentEventFields{
			TaskID:         e.TaskID,
			OrderID:        e.OrderID,
			TargetSendTSNS: e.TargetSendTSNS,
			SendTSNS:       e.SendTSNS,
			RecvDoneTSNS:   e.RecvDoneTSNS,
			TimedOut:       e.TimedOut,
			Price:          e.Price,
			Qty:            e.Qty,
			Side:           e.Side,
			PayloadType:    e.PayloadType,
			OrdType:        e.OrdType,
			OrigOrderID:    e.OrigOrderID,
			BarrierEpochNs: e.BarrierEpochNs,
		})
	}
	payload, err := msgpack.Marshal(topics.OrderSentBatchV2{
		SessionID:    sid,
		SubmissionID: submissionID,
		WorkerID:     workerID,
		Events:       fields,
	})
	if err != nil {
		t.Fatalf("msgpack marshal sent: %v", err)
	}
	return payload
}

func ackedBatchPayload(t *testing.T, sid string, events []topics.OrderAckedEvent) []byte {
	t.Helper()
	payload, err := msgpack.Marshal(topics.OrderAckedBatch{SessionID: sid, ContestantID: "team-x", Events: events})
	if err != nil {
		t.Fatalf("msgpack marshal acked: %v", err)
	}
	return payload
}

// TestIntegration_BandIsolation is the two-session isolation test: session A
// (band 0) and session B (band 2) each get a legitimate order placed in their
// own band's partitions, plus a "leaked" event carrying the SAME session id
// written directly into the OTHER band's partitions (simulating a stale-tail
// cross-band leftover). StreamSession, called with each session's real
// orderBand, must count only its own band's events — proving the reader
// never opened the other band's partitions at all (the session-id filter
// alone would NOT catch this, since the leaked event carries the correct
// session id; exclusion here can only come from partition restriction).
// Needs a live broker with >= 4x bandWidth partitions on orders.sent/acked:
// set KAFKA_BROKERS.
func TestIntegration_BandIsolation(t *testing.T) {
	brokersCSV := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersCSV == "" {
		t.Skip("set KAFKA_BROKERS to run the band isolation integration test")
	}
	brokers := strings.Split(brokersCSV, ",")
	ctx := context.Background()

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	sentParts, err := conn.ReadPartitions(topics.TopicOrdersSent)
	if err != nil {
		conn.Close()
		t.Fatalf("read partitions: %v", err)
	}
	ackParts, err := conn.ReadPartitions(topics.TopicOrdersAcked)
	conn.Close()
	if err != nil {
		t.Fatalf("read partitions: %v", err)
	}
	numPartitions := int32(len(sentParts))
	if int32(len(ackParts)) < numPartitions {
		numPartitions = int32(len(ackParts))
	}
	const bandWidth = int32(2)
	const bandA, bandB = uint32(0), uint32(2)
	if numPartitions < int32(bandB+1)*bandWidth {
		t.Skipf("need >= %d partitions on orders.sent/orders.acked for band isolation (band %d), have %d", (bandB+1)*uint32(bandWidth), bandB, numPartitions)
	}

	setA := bandPartitionSet(bandA, numPartitions, bandWidth)
	setB := bandPartitionSet(bandB, numPartitions, bandWidth)
	var partA, partB int
	for p := range setA {
		partA = p
		break
	}
	for p := range setB {
		partB = p
		break
	}

	now := time.Now()
	sidA := newUUIDv7(now)
	sidB := newUUIDv7(now.Add(time.Millisecond))

	// Legitimate events, correctly placed in each session's own band.
	writeEventsToPartition(ctx, t, brokers, topics.TopicOrdersSent, partA,
		sentBatchPayload(t, sidA, []topics.OrderSentEvent{{SessionID: sidA, OrderID: mkID(sidA, 1, 0), SendTSNS: uint64(now.UnixNano()), Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"}}))
	writeEventsToPartition(ctx, t, brokers, topics.TopicOrdersAcked, partA,
		ackedBatchPayload(t, sidA, []topics.OrderAckedEvent{ack(sidA, mkID(sidA, 1, 0), 1, 1, uint64(now.UnixNano()), "0", 0, 0)}))

	writeEventsToPartition(ctx, t, brokers, topics.TopicOrdersSent, partB,
		sentBatchPayload(t, sidB, []topics.OrderSentEvent{{SessionID: sidB, OrderID: mkID(sidB, 2, 0), SendTSNS: uint64(now.UnixNano()), Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"}}))
	writeEventsToPartition(ctx, t, brokers, topics.TopicOrdersAcked, partB,
		ackedBatchPayload(t, sidB, []topics.OrderAckedEvent{ack(sidB, mkID(sidB, 2, 0), 2, 1, uint64(now.UnixNano()), "0", 0, 0)}))

	// Leaked events: session A's id written into band B's partitions, and
	// vice versa. A correct band-restricted reader for sidA must never see
	// these, even though the session id matches.
	writeEventsToPartition(ctx, t, brokers, topics.TopicOrdersSent, partB,
		sentBatchPayload(t, sidA, []topics.OrderSentEvent{{SessionID: sidA, OrderID: mkID(sidA, 99, 0), SendTSNS: uint64(now.UnixNano()), Price: 999, Qty: 1, Side: "SELL", PayloadType: "NEW", OrdType: "LIMIT"}}))
	writeEventsToPartition(ctx, t, brokers, topics.TopicOrdersSent, partA,
		sentBatchPayload(t, sidB, []topics.OrderSentEvent{{SessionID: sidB, OrderID: mkID(sidB, 99, 0), SendTSNS: uint64(now.UnixNano()), Price: 999, Qty: 1, Side: "SELL", PayloadType: "NEW", OrdType: "LIMIT"}}))

	drain := func(sessionID string, band uint32) StreamCounts {
		counts, _, err := StreamSession(ctx, brokers, sessionID, 0, band, bandWidth,
			func(*model.Order) {},
			func(string, uint64, int64) {},
			func(string, model.Kind) {},
			func() {},
		)
		if err != nil {
			t.Fatalf("StreamSession(%s): %v", sessionID, err)
		}
		return counts
	}

	countsA := drain(sidA, bandA)
	if countsA.SentEvents != 1 {
		t.Errorf("session A (band %d): SentEvents = %d, want 1 (leaked band-%d event must be excluded)", bandA, countsA.SentEvents, bandB)
	}

	countsB := drain(sidB, bandB)
	if countsB.SentEvents != 1 {
		t.Errorf("session B (band %d): SentEvents = %d, want 1 (leaked band-%d event must be excluded)", bandB, countsB.SentEvents, bandA)
	}
}

// Package source defines tests for session offset resolution, plus the Kafka fixture
// helpers shared by this package's integration tests.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package source

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

// TestSessionStartFromID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSessionStartFromID(t *testing.T) {
	const id = "019e8a61-64f4-7383-b63d-728af69ca072"
	const wantMS = int64(0x019e8a6164f4)

	got, ok := sessionStartFromID(id)
	if !ok {
		t.Fatalf("sessionStartFromID(%q) ok=false, want a valid UUIDv7 timestamp", id)
	}
	if got.UnixMilli() != wantMS {
		t.Fatalf("sessionStartFromID(%q) = %d ms, want %d ms", id, got.UnixMilli(), wantMS)
	}
}

// TestSessionStartFromIDFallback performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSessionStartFromIDFallback(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"synthetic test id", "itest-1780438099188"},
		{"empty", ""},
		{"too short", "019e8a61"},
		{"not hex", "zzzzzzzz-64f4-7383-b63d-728af69ca072"},
		{"v4 (version nibble != 7)", "019e8a61-64f4-4383-b63d-728af69ca072"},
		{"zero timestamp", "00000000-0000-7000-8000-000000000000"},
		{"no dashes, wrong length", "019e8a6164f47383b63d728af69ca0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok := sessionStartFromID(c.id); ok {
				t.Errorf("sessionStartFromID(%q) ok=true, want false (must fall back to earliest)", c.id)
			}
		})
	}
}

// TestStartOffsetForSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestStartOffsetForSession(t *testing.T) {
	const validID = "019e8a61-64f4-7383-b63d-728af69ca072"
	startMS := int64(0x019e8a6164f4)

	var sawRequest time.Time
	lookup := func(at time.Time) (int64, error) {
		sawRequest = at
		return 4242, nil
	}

	got, err := startOffsetForSession(validID, lookup)
	if err != nil {
		t.Fatalf("startOffsetForSession: %v", err)
	}
	if got != 4242 {
		t.Fatalf("start offset = %d, want 4242 (the time-lookup result)", got)
	}
	wantReq := time.UnixMilli(startMS).Add(-startMargin)
	if !sawRequest.Equal(wantReq) {
		t.Fatalf("time lookup requested %v, want %v (session-start − %v margin)", sawRequest, wantReq, startMargin)
	}

	called := false
	off, err := startOffsetForSession("itest-123", func(time.Time) (int64, error) {
		called = true
		return 0, nil
	})
	if err != nil {
		t.Fatalf("startOffsetForSession fallback: %v", err)
	}
	if called {
		t.Errorf("non-UUIDv7 id triggered a time lookup; want fallback without a broker round-trip")
	}
	if off != kafka.FirstOffset {
		t.Errorf("fallback start offset = %d, want kafka.FirstOffset (%d)", off, kafka.FirstOffset)
	}
}

// TestResolveStart performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestResolveStart(t *testing.T) {
	if got := resolveStart(-1, 591); got != 591 {
		t.Fatalf("seek=-1 (no session events) must map to last=591, got %d", got)
	}
	if got := resolveStart(0, 297); got != 0 {
		t.Fatalf("seek=0 must be honored, got %d", got)
	}
	if got := resolveStart(42, 1197); got != 42 {
		t.Fatalf("seek=42 must be honored, got %d", got)
	}
}

// TestDecodeBatchSurvivesGarbage: one unparseable message must not sink the session
// read. This was the batch collectors' decode-error test; decodeBatch is the streaming
// path's equivalent choke point.
func TestDecodeBatchSurvivesGarbage(t *testing.T) {
	const sid = "sess-decode"
	garbage := kafka.Message{Partition: 3, Offset: 42, Value: []byte{0xc1}}

	if _, ok := decodeBatch(garbage, sid, false); ok {
		t.Error("garbage orders.sent message decoded ok=true, want false")
	}
	if _, ok := decodeBatch(garbage, sid, true); ok {
		t.Error("garbage orders.acked message decoded ok=true, want false")
	}

	// V2 positional envelope — the format the producer actually writes.
	sentPayload, err := msgpack.Marshal(topics.OrderSentBatchV2{
		SessionID: sid,
		Events: []topics.OrderSentEventFields{
			{OrderID: "A", Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT", SendTSNS: 7},
		},
	})
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	b, ok := decodeBatch(kafka.Message{Value: sentPayload}, sid, false)
	if !ok || len(b.sent) != 1 {
		t.Fatalf("valid sent batch after a decode failure: ok=%v events=%d", ok, len(b.sent))
	}

	ackPayload, err := msgpack.Marshal(topics.OrderAckedBatch{SessionID: sid, Events: []topics.OrderAckedEvent{
		{SessionID: sid, OrderID: "A", ExecType: "0", T7XDPEgressNS: 1500},
	}})
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	if b, ok := decodeBatch(kafka.Message{Value: ackPayload}, sid, true); !ok || len(b.acks) != 1 {
		t.Fatalf("valid acked batch after a decode failure: ok=%v events=%d", ok, len(b.acks))
	}
}

// newUUIDv7 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newUUIDv7(at time.Time) string {
	ms := uint64(at.UnixMilli())
	return fmt.Sprintf("%012x-%04x-7%03x-8%03x-%012x",
		ms&0xffffffffffff, 0x64f4, 0x383, 0x63d, uint64(0x728af69ca072))
}

// writeSent performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeSent(ctx context.Context, t *testing.T, brokers []string, sid string, events []topics.OrderSentEvent) {
	t.Helper()
	batch := sentBatchV2(sid, events)
	payload, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topics.TopicOrdersSent,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(sid), Value: payload}); err != nil {
		t.Fatalf("write orders.sent: %v", err)
	}
}

// sentBatchV2 builds the positional V2 envelope from full events, hoisting the identity
// fields the way the Rust producer does, so no test encodes the superseded named-map
// format (a Go-to-Go round trip of the wrong envelope passes while Go and Rust disagree,
// which is how the sent=0 bug survived four tests).
func sentBatchV2(sid string, events []topics.OrderSentEvent) topics.OrderSentBatchV2 {
	fields := make([]topics.OrderSentEventFields, 0, len(events))
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
	return topics.OrderSentBatchV2{SessionID: sid, WorkerID: "w0", Events: fields}
}

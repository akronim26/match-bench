package source

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

func mkID(sid string, bot, seq int) string { return fmt.Sprintf("%s_%d_%d_O", sid, bot, seq) }

func ack(sid, id string, port uint16, seq uint32, t3 uint64, exec string, fq, fp uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: sid, ContestantID: "team-x", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: port, TCPSeq: seq,
		T3XDPIngressNS: t3, T7XDPEgressNS: t3 + 500, PodServiceTimeNS: 500,
		ExecType: exec, FillQty: fq, FillPrice: fp,
	}
}

func writeAcked(ctx context.Context, t *testing.T, brokers []string, sid string, events []topics.OrderAckedEvent) {
	t.Helper()
	payload, err := msgpack.Marshal(topics.OrderAckedBatch{SessionID: sid, ContestantID: "team-x", Events: events})
	if err != nil {
		t.Fatalf("msgpack marshal acked: %v", err)
	}
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topics.TopicOrdersAcked, Balancer: &kafka.LeastBytes{}, RequiredAcks: kafka.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(sid), Value: payload}); err != nil {
		t.Fatalf("write orders.acked: %v", err)
	}
}

// TestIntegration_StreamSessionScoresAKnownSession produces a real session to Kafka and
// asserts the streaming source + StreamValidator score it as expected.
//
// This was the stream-vs-batch equivalence test. With the batch path deleted there is
// nothing to compare against, so the fixture's expected verdict is now spelled out
// directly — which is stronger: the old assertion would have passed had BOTH paths been
// wrong in the same way. Needs a live broker: set KAFKA_BROKERS.
func TestIntegration_StreamSessionScoresAKnownSession(t *testing.T) {
	brokersCSV := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersCSV == "" {
		t.Skip("set KAFKA_BROKERS to run the streaming-source integration test")
	}
	brokers := strings.Split(brokersCSV, ",")
	ctx := context.Background()
	sc := uint64(topics.TelemetryPriceScale)
	sid := newUUIDv7(time.Now())

	// realistic ns timestamps spread ~1s apart so the mid-stream windowed emit path
	// (not just the final flush) is exercised; sent precedes its ack by ~5ms.
	base := uint64(1_700_000_000_000_000_000)
	t3 := func(i int) uint64 { return base + uint64(i)*1_000_000_000 }
	st := func(i int) uint64 { return t3(i) - 5_000_000 }
	sent := func(bot, seq, i int, price, qty uint64, side string) topics.OrderSentEvent {
		return topics.OrderSentEvent{SessionID: sid, OrderID: mkID(sid, bot, seq), SendTSNS: st(i),
			Price: price, Qty: qty, Side: side, PayloadType: "NEW", OrdType: "LIMIT"}
	}
	sents := []topics.OrderSentEvent{
		sent(7, 1, 0, 100, 10, "SELL"),
		sent(7, 2, 1, 100, 10, "SELL"),
		sent(8, 3, 2, 100, 10, "BUY"),
		sent(8, 4, 3, 101, 5, "SELL"),
		sent(9, 5, 4, 100, 10, "BUY"),
		sent(9, 6, 5, 100, 10, "BUY"), // never answered: the contestant dropped it
	}
	writeSent(ctx, t, brokers, sid, sents)

	acks := []topics.OrderAckedEvent{
		ack(sid, mkID(sid, 7, 1), 7, 1, t3(0), "2", 10, 100*sc), // S1 filled by the reference, reported
		ack(sid, mkID(sid, 7, 2), 7, 2, t3(1), "0", 0, 0),       // S2 rests, never filled
		ack(sid, mkID(sid, 8, 3), 8, 3, t3(2), "2", 10, 100*sc), // B1 valid fill 10@100 against S1
		ack(sid, mkID(sid, 8, 4), 8, 4, t3(3), "0", 0, 0),       // S4 rests @101, never filled
		ack(sid, mkID(sid, 9, 5), 9, 5, t3(4), "2", 10, 100*sc), // B2 fill, but the book is empty at 100
		ack(sid, "ghost", 9, 6, t3(5), "2", 5, 50*sc),           // phantom (never sent)
	}
	writeAcked(ctx, t, brokers, sid, acks)

	sv := validate.NewStreamValidator()
	counts, contestant, err := StreamSession(ctx, brokers, sid, 0, topics.OrderBandUnset, 0,
		sv.Apply,
		func(id string, qty uint64, price int64) {
			sv.AddPhantom(validate.ReportedFill{OrderID: id, Qty: qty, Price: price})
		},
		sv.AddLost,
		sv.AddCaptureGap,
	)
	if err != nil {
		t.Fatalf("StreamSession: %v", err)
	}
	r := sv.Finish()

	if contestant != "team-x" {
		t.Errorf("contestant = %q, want team-x", contestant)
	}
	// Six sent orders, five answered; the ghost has no sent event so it is not an order.
	if counts.SentEvents != 6 || counts.AckedEvents != 6 || counts.MatchedOrders != 5 {
		t.Fatalf("counts = %+v, want sent=6 acked=6 matched=5 — join or produce is broken", counts)
	}
	// The unanswered order must reach the validator rather than being discarded by the
	// join, or dropping an order costs the contestant nothing.
	if counts.LostOrders != 1 || r.LostOrders != 1 {
		t.Fatalf("counts.LostOrders = %d, report.LostOrders = %d, want 1 each: %+v", counts.LostOrders, r.LostOrders, r)
	}
	if r.ScoredOrders != 6 {
		t.Fatalf("ScoredOrders = %d, want 6 (5 answered + 1 dropped)", r.ScoredOrders)
	}
	// S1+B1 cross legitimately (2 valid fills). B2's fill has no reference counterpart:
	// S2 rests at 100 but B2 is a BUY, and the only ask left is S4 at 101.
	if r.ValidFills != 2 {
		t.Fatalf("ValidFills = %d, want 2 (S1 and B1): %+v", r.ValidFills, r)
	}
	if r.TotalFills != 3 {
		t.Fatalf("TotalFills = %d, want 3 real reported fills: %+v", r.TotalFills, r)
	}
	if r.PhantomFills != 1 || r.ScoredFills != r.TotalFills {
		t.Fatalf("PhantomFills = %d (want 1), ScoredFills = %d (want %d): %+v",
			r.PhantomFills, r.ScoredFills, r.TotalFills, r)
	}
	// Two orders misbehaved — B2's unsupported fill and the dropped one — so 4 of 6 are
	// clean.
	if r.DirtyOrders != 2 {
		t.Fatalf("DirtyOrders = %d, want 2 (B2 and the dropped order): %+v", r.DirtyOrders, r)
	}
	if got, want := r.CorrectnessScore(), 4.0/6.0; got != want {
		t.Fatalf("score = %.6f, want %.6f: %+v", got, want, r)
	}
	t.Logf("OK: matched=%d score=%.4f total=%d valid=%d violations=%d",
		counts.MatchedOrders, r.CorrectnessScore(), r.TotalFills, r.ValidFills, r.ViolationCount())
}

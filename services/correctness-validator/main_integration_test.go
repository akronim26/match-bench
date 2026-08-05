// Package main defines tests for main integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/publisher"
	"github.com/iicpc/correctness-validator/internal/store"
	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

// itEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func itEnv(key string) string { return strings.TrimSpace(os.Getenv(key)) }

// discardLogger performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestIntegration_ValidateSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_ValidateSession(t *testing.T) {
	brokersCSV := itEnv("KAFKA_BROKERS")
	dbURL := itEnv("DATABASE_URL")
	if brokersCSV == "" || dbURL == "" {
		t.Skip("set KAFKA_BROKERS and DATABASE_URL to run the validator integration test")
	}
	brokers := parseBrokers(brokersCSV)
	ctx := context.Background()
	sessionID := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	const contestant = "team-itest"

	produceSession(ctx, t, brokers, sessionID, contestant)

	since := snapshotOffsets(ctx, t, brokers, topics.TopicScoresCorrectness)

	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer st.Close()
	pub := publisher.New(brokersCSV)
	defer pub.Close()

	v := &validator{
		log:         discardLogger(),
		brokers:     brokers,
		store:       st,
		pub:         pub,
		settleDelay: 0,
	}
	if err := v.validateSession(ctx, sessionID); err != nil {
		t.Fatalf("validateSession: %v", err)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	var (
		gotContestant                                      string
		total, valid, vcount, phantom, overfill, priceViol int64
		sentCount, ackedCount, matchedCount                int64
		score                                              float64
	)
	err = pool.QueryRow(ctx, `
SELECT contestant_id, total_fills, valid_fills, violation_count,
       phantom_fills, overfills, price_violations, correctness_score,
       sent_count, acked_count, matched_count
FROM correctness_summary WHERE session_id=$1`, sessionID).
		Scan(&gotContestant, &total, &valid, &vcount, &phantom, &overfill, &priceViol, &score,
			&sentCount, &ackedCount, &matchedCount)
	if err != nil {
		t.Fatalf("query summary: %v", err)
	}
	if gotContestant != contestant {
		t.Errorf("contestant_id = %q, want %q", gotContestant, contestant)
	}
	if total != 4 || valid != 2 || vcount != 2 || phantom != 1 || overfill != 1 || priceViol != 0 {
		t.Errorf("summary counts = total %d valid %d violations %d phantom %d overfill %d price %d; want 4/2/2/1/1/0",
			total, valid, vcount, phantom, overfill, priceViol)
	}
	if score < 0.49 || score > 0.51 {
		t.Errorf("correctness_score = %v, want ~0.5", score)
	}
	if sentCount != 2 || ackedCount != 6 || matchedCount != 2 {
		t.Errorf("summary completeness = sent %d acked %d matched %d; want 2/6/2",
			sentCount, ackedCount, matchedCount)
	}

	// The per-violation table is gone (it grew to millions of rows and nothing read
	// it), so the summary row is the ONLY source for the breakdown the UI renders.
	// The category counts asserted above cover phantom/overfill/price; these three
	// complete the set. Their absence is what let a session report 7,860 violations
	// while the rendered categories totalled 196 — the missing class was missed_fills.
	var missedFills, lostOrders, lostCancels int64
	if err := pool.QueryRow(ctx, `
SELECT missed_fills, lost_orders, lost_cancels
FROM correctness_summary WHERE session_id=$1`, sessionID).
		Scan(&missedFills, &lostOrders, &lostCancels); err != nil {
		t.Fatalf("query violation classes: %v", err)
	}
	if missedFills != 0 || lostOrders != 0 || lostCancels != 0 {
		t.Errorf("missed_fills/lost_orders/lost_cancels = %d/%d/%d, want 0/0/0 for this fixture",
			missedFills, lostOrders, lostCancels)
	}

	ev, ok := readNewScore(ctx, t, brokers, since, sessionID)
	if !ok {
		t.Fatal("CorrectnessScoreEvent not found on scores.correctness")
	}
	if ev.ContestantID != contestant || ev.TotalFills != 4 || ev.ValidFills != 2 || ev.ViolationCount != 2 {
		t.Errorf("published event = %+v; want contestant %q total 4 valid 2 violations 2", ev, contestant)
	}
	if ev.CorrectnessScore < 0.49 || ev.CorrectnessScore > 0.51 {
		t.Errorf("published score = %v, want ~0.5", ev.CorrectnessScore)
	}
	if ev.SentCount != 2 || ev.AckedCount != 6 || ev.MatchedCount != 2 {
		t.Errorf("published completeness = sent %d acked %d matched %d; want 2/6/2",
			ev.SentCount, ev.AckedCount, ev.MatchedCount)
	}

	status, done, err := st.SummaryStatus(ctx, sessionID)
	if err != nil || !done || status != store.StatusScored {
		t.Errorf("SummaryStatus after save = (%q,%v,%v), want (%q,true,nil)", status, done, err, store.StatusScored)
	}
}

// TestIntegration_TriggerConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_TriggerConsumer(t *testing.T) {
	brokersCSV := itEnv("KAFKA_BROKERS")
	dbURL := itEnv("DATABASE_URL")
	if brokersCSV == "" || dbURL == "" {
		t.Skip("set KAFKA_BROKERS + DATABASE_URL to run the trigger-consumer integration test")
	}
	brokers := parseBrokers(brokersCSV)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessionID := fmt.Sprintf("itest-trigger-%d", time.Now().UnixNano())
	const contestant = "team-trigger"

	produceSession(ctx, t, brokers, sessionID, contestant)
	produceStatus(ctx, t, brokers, sessionID, topics.RunStatusRunning)
	produceStatus(ctx, t, brokers, sessionID, topics.RunStatusCompleted)

	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()
	pub := publisher.New(brokersCSV)
	defer pub.Close()
	v := &validator{
		log:               discardLogger(),
		brokers:           brokers,
		store:             st,
		pub:               pub,
		settleDelay:       0,
		validationTimeout: 30 * time.Second,
	}

	group := fmt.Sprintf("itest-validator-%d", time.Now().UnixNano())
	go v.runStatusConsumer(ctx, brokers, group)

	deadline := time.After(90 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if status, done, _ := st.SummaryStatus(ctx, sessionID); done && status == store.StatusScored {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the completed session to be validated")
		}
	}
}

// produceSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func produceSession(ctx context.Context, t *testing.T, brokers []string, sessionID, contestant string) {
	t.Helper()
	// POSITIONAL V2 envelope (identity fields hoisted) — what the Rust bot-fleet
	// producer writes and what StreamSession decodes. Encoding the superseded named-map
	// OrderSentBatch here would make this integration test pass against a format
	// nothing produces.
	sent := topics.OrderSentBatchV2{
		SessionID: sessionID,
		WorkerID:  "w0",
		Events: []topics.OrderSentEventFields{
			{OrderID: "M1", Price: 100, Qty: 10, Side: "SELL", PayloadType: "NEW", OrdType: "LIMIT"},
			{OrderID: "T1", Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"},
		},
	}
	acked := topics.OrderAckedBatch{
		SessionID:    sessionID,
		ContestantID: contestant,
		Events: []topics.OrderAckedEvent{
			{SessionID: sessionID, ContestantID: contestant, OrderID: "M1", SrcIP: 1, SrcPort: 1000, TCPSeq: 1, T3XDPIngressNS: 1000, T7XDPEgressNS: 1500, ExecType: "0"},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "M1", SrcIP: 1, SrcPort: 1000, TCPSeq: 1, T3XDPIngressNS: 1000, T7XDPEgressNS: 1600, ExecType: "2", FillQty: 10, FillPrice: 100 * topics.TelemetryPriceScale},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "T1", SrcIP: 2, SrcPort: 2000, TCPSeq: 1, T3XDPIngressNS: 2000, T7XDPEgressNS: 2500, ExecType: "0"},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "T1", SrcIP: 2, SrcPort: 2000, TCPSeq: 1, T3XDPIngressNS: 2000, T7XDPEgressNS: 2600, ExecType: "2", FillQty: 10, FillPrice: 100 * topics.TelemetryPriceScale},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "T1", SrcIP: 2, SrcPort: 2000, TCPSeq: 1, T3XDPIngressNS: 2000, T7XDPEgressNS: 2700, ExecType: "2", FillQty: 5, FillPrice: 100 * topics.TelemetryPriceScale},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "PH", SrcIP: 3, SrcPort: 3000, TCPSeq: 1, T3XDPIngressNS: 3000, T7XDPEgressNS: 3100, ExecType: "2", FillQty: 3, FillPrice: 100 * topics.TelemetryPriceScale},
		},
	}
	writeMsgpack(ctx, t, brokers, topics.TopicOrdersSent, sessionID, sent)
	writeMsgpack(ctx, t, brokers, topics.TopicOrdersAcked, sessionID, acked)
}

// writeMsgpack performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeMsgpack(ctx context.Context, t *testing.T, brokers []string, topic, key string, v any) {
	t.Helper()
	payload, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatalf("msgpack marshal %s: %v", topic, err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		t.Fatalf("write %s: %v", topic, err)
	}
}

// produceStatus performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func produceStatus(ctx context.Context, t *testing.T, brokers []string, sessionID, status string) {
	t.Helper()
	payload, err := json.Marshal(topics.BenchmarkStatusUpdated{SessionID: sessionID, Status: status})
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topics.TopicBenchmarkStatusUpdated,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(sessionID), Value: payload}); err != nil {
		t.Fatalf("write status: %v", err)
	}
}

// snapshotOffsets performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func snapshotOffsets(ctx context.Context, t *testing.T, brokers []string, topic string) map[int]int64 {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		t.Fatalf("read partitions %s: %v", topic, err)
	}
	out := make(map[int]int64, len(parts))
	for _, p := range parts {
		c, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			t.Fatalf("dial leader %s/%d: %v", topic, p.ID, err)
		}
		last, err := c.ReadLastOffset()
		c.Close()
		if err != nil {
			t.Fatalf("read last offset %s/%d: %v", topic, p.ID, err)
		}
		out[p.ID] = last
	}
	return out
}

// readNewScore performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func readNewScore(ctx context.Context, t *testing.T, brokers []string, since map[int]int64, sessionID string) (*topics.CorrectnessScoreEvent, bool) {
	t.Helper()
	for pid, start := range since {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topics.TopicScoresCorrectness,
			Partition: pid,
			MinBytes:  1,
			MaxBytes:  1 << 20,
		})
		if err := r.SetOffset(start); err != nil {
			r.Close()
			t.Fatalf("set offset %d: %v", pid, err)
		}
		for {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			m, err := r.ReadMessage(rctx)
			cancel()
			if err != nil {
				break
			}
			var ev topics.CorrectnessScoreEvent
			if json.Unmarshal(m.Value, &ev) == nil && ev.SessionID == sessionID {
				r.Close()
				return &ev, true
			}
		}
		r.Close()
	}
	return nil, false
}

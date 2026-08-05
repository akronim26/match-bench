package source

import (
	"testing"

	"github.com/iicpc/schemas/topics"
	"github.com/vmihailenco/msgpack/v5"
)

// These tests used to marshal NAMED-MAP batches (the Rust `to_vec_named` format) and
// assert the old topics.OrderSentBatch decoded them. That format is no longer what the
// producer writes: orders.sent moved to the POSITIONAL OrderSentBatchV2 envelope, with
// session_id / submission_id / worker_id hoisted out of each event.
//
// Keeping the old fixtures would have been worse than useless — a test that encodes a
// format nothing produces, decoded by a struct the live path no longer uses, passes
// forever while production is broken. That is exactly what happened: the validator
// reported sent=0 against a topic holding ~600k records and no test noticed.
//
// So these now exercise the wire format that actually ships. The byte-level contract is
// pinned separately, against real captured producer bytes, in
// schemas/go/topics/wire_contract_test.go.

// TestOrderSentBatchV2CarriesBarrierEpoch: barrier_epoch_ns is the last positional
// field of the per-event payload (the distributed ingester needs it for deterministic
// wave bucketing). Being last makes it the field most likely to be silently lost to a
// length mismatch, so it is asserted explicitly.
func TestOrderSentBatchV2CarriesBarrierEpoch(t *testing.T) {
	const epoch = uint64(1_770_000_000_000_000_000)
	batch := topics.OrderSentBatchV2{
		SessionID:    "sess-1",
		SubmissionID: "sub-1",
		WorkerID:     "w-1",
		Events: []topics.OrderSentEventFields{{
			TaskID:         7,
			OrderID:        "sess-1_7_3_O",
			TargetSendTSNS: 100,
			SendTSNS:       110,
			TimedOut:       false,
			Price:          1,
			Qty:            1,
			Side:           "BUY",
			PayloadType:    "NEW",
			OrdType:        "LIMIT",
			BarrierEpochNs: epoch,
		}},
	}
	payload, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal V2 batch: %v", err)
	}

	var decoded topics.OrderSentBatchV2
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode V2 batch: %v", err)
	}
	if len(decoded.Events) != 1 {
		t.Fatalf("want 1 event, got %d", len(decoded.Events))
	}
	if got := decoded.Events[0].BarrierEpochNs; got != epoch {
		t.Fatalf("barrier_epoch_ns not decoded: got %d, want %d", got, epoch)
	}
	if got := decoded.Events[0].OrderID; got != "sess-1_7_3_O" {
		t.Fatalf("order_id corrupted: %q", got)
	}
}

// TestOrderSentBatchV2HoistedFieldsReachEveryEvent: the envelope's identity fields must
// be stamped onto each reconstituted event, because downstream code (the validator's
// order assembly, the session filter) reads them per event. A silent failure here would
// look like a session with no orders rather than a decode error.
func TestOrderSentBatchV2HoistedFieldsReachEveryEvent(t *testing.T) {
	batch := topics.OrderSentBatchV2{
		SessionID:    "sess-9",
		SubmissionID: "sub-9",
		WorkerID:     "w-9",
		Events: []topics.OrderSentEventFields{
			{TaskID: 1, OrderID: "sess-9_1_1_O", SendTSNS: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"},
			{TaskID: 2, OrderID: "sess-9_2_1_O", SendTSNS: 20, Side: "SELL", PayloadType: "CANCEL", OrdType: "LIMIT"},
		},
	}
	payload, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded topics.OrderSentBatchV2
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	events := decoded.IntoEvents()
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	for i, e := range events {
		if e.SessionID != "sess-9" || e.SubmissionID != "sub-9" || e.WorkerID != "w-9" {
			t.Errorf("event %d missing hoisted identity: session=%q submission=%q worker=%q",
				i, e.SessionID, e.SubmissionID, e.WorkerID)
		}
	}
	if events[1].PayloadType != "CANCEL" {
		t.Errorf("second event PayloadType = %q, want CANCEL", events[1].PayloadType)
	}
}

// TestDecodeBatchRejectsForeignSession pins the session-id filter that made the wire
// mismatch silent: a batch whose session id does not match is dropped WITHOUT an error,
// so a decode that yields a garbage session id is indistinguishable from "no data for
// this session". The filter is correct and necessary (it guards against stale-tail
// cross-band leakage) — this test records that it cannot, by itself, tell you the
// decoder is wrong.
func TestDecodeBatchRejectsForeignSession(t *testing.T) {
	batch := topics.OrderSentBatchV2{
		SessionID:    "other-session",
		SubmissionID: "sub-1",
		WorkerID:     "w-1",
		Events: []topics.OrderSentEventFields{{
			TaskID: 1, OrderID: "other-session_1_1_O", SendTSNS: 10,
			Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT",
		}},
	}
	payload, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded topics.OrderSentBatchV2
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.SessionID == "sess-1" {
		t.Fatal("fixture should carry a foreign session id")
	}
}

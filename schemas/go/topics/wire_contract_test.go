// Package topics tests the CROSS-LANGUAGE wire contract for orders.sent.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package topics

import (
	"encoding/base64"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// realProducerBatch is a VERBATIM orders.sent record captured off the broker, written
// by the Rust bot-fleet producer (OrderSentBatchV2Ref). It is the fixture rather than a
// Go-encoded round trip on purpose: a Go-to-Go round trip passes even when Go and Rust
// disagree, which is exactly how the bug this test exists for went unnoticed.
//
// The bug: the producer moved orders.sent to a positional envelope with session_id /
// submission_id / worker_id hoisted, and telemetry-ingester was updated — but the
// correctness-validator kept decoding the old named-map OrderSentBatch. Positional
// bytes read as a named map produce a garbage SessionID, so the validator's session
// filter dropped EVERY batch: it reported sent=0 against a topic holding ~600k records,
// which zeroed matched and silently invalidated every correctness score.
const realProducerBatch = "lNkkMDE5ZmIzYTktZTI3NS03MGM3LTg1MzEtYjAwNTRhZDZiMDM1rXAyLTE3ODU0MjU2ODPZIGJvdC1mbGVldC13b3JrZXItYmM5ZDljZDY2LXRxNXFrm54A2TAwMTlmYjNhOS1lMjc1LTcwYzctODUzMS1iMDA1NGFkNmIwMzVfMF8xODQ3MTY0X0PPGMcbPyVd1cDPGMcbPyVd1cDPGMcbPzzlWdPCzScUGKNCVVmmQ0FOQ0VMpUxJTUlU2TAwMTlmYjNhOS1lMjc1LTcwYzctODUzMS1iMDA1NGFkNmIwMzVfMF8xODQ1Mjc1X0/PGMcbNKur5sUEngDZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDcxNzdfT88Yxxs/JYHXDs8Yxxs/JYHXDs8Yxxs/PO3I7cLNJwYqo0JVWaNORVelTElNSVSgzxjHGzSrq+bFAZ4A2TAwMTlmYjNhOS1lMjc1LTcwYzctODUzMS1iMDA1NGFkNmIwMzVfMF8xODQ3MTg2X0/PGMcbPyWB1w7PGMcbPyWB1w7PGMcbPzz0qLnCzScXFKRTRUxMo05FV6VMSU1JVKDPGMcbNKur5sUCngDZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDcxOTdfTc8Yxxs/JYHXDs8Yxxs/JYHXDs8Yxxs/PPvJkMIALaNCVVmjTkVXpk1BUktFVKDPGMcbNKur5sUFngDZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDcyNDFfT88Yxxs/JaqG888Yxxs/JaqG888Yxxs/PRN19MLNJxojo0JVWaNORVelTElNSVSgzxjHGzSrq+bFAZ4A2TAwMTlmYjNhOS1lMjc1LTcwYzctODUzMS1iMDA1NGFkNmIwMzVfMF8xODQ3MjUzX1LPGMcbPyWqhvPPGMcbPyWqhvPPGMcbPz0YpezCzScGDqRTRUxMp1JFUExBQ0WlTElNSVTZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDM0NjJfT88Yxxs0q6vmxQWeANkwMDE5ZmIzYTktZTI3NS03MGM3LTg1MzEtYjAwNTRhZDZiMDM1XzBfMTg0NzI1Nl9DzxjHGz8lqobzzxjHGz8lqobzzxjHGz89Go/Kws0nBxOkU0VMTKZDQU5DRUylTElNSVTZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDQ1MjBfT88Yxxs0q6vmxQCeANkwMDE5ZmIzYTktZTI3NS03MGM3LTg1MzEtYjAwNTRhZDZiMDM1XzBfMTg0NzI4MF9SzxjHGz8lqobzzxjHGz8lqobzzxjHGz89Jrefws0nGQyjQlVZp1JFUExBQ0WlTElNSVTZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDQ1OTlfUs8Yxxs0q6vmxQCeANkwMDE5ZmIzYTktZTI3NS03MGM3LTg1MzEtYjAwNTRhZDZiMDM1XzBfMTg0NzI4MV9PzxjHGz8lqobzzxjHGz8lqobzzxjHGz89JrsRws0nDgyjQlVZo05FV6VMSU1JVKDPGMcbNKur5sUBngDZMDAxOWZiM2E5LWUyNzUtNzBjNy04NTMxLWIwMDU0YWQ2YjAzNV8wXzE4NDcyODhfUs8Yxxs/JaqG888Yxxs/JaqG888Yxxs/PSinNMLNJw0spFNFTEynUkVQTEFDRaVMSU1JVNkwMDE5ZmIzYTktZTI3NS03MGM3LTg1MzEtYjAwNTRhZDZiMDM1XzBfMTg0Njc0MF9PzxjHGzSrq+bFAJ4A2TAwMTlmYjNhOS1lMjc1LTcwYzctODUzMS1iMDA1NGFkNmIwMzVfMF8xODQ3Mjk2X0/PGMcbPyWqhvPPGMcbPyWqhvPPGMcbPz0vnpnCzScOD6RTRUxMo05FV6VMSU1JVKDPGMcbNKur5sUA"

// TestOrderSentBatchV2WireContract decodes real producer bytes and pins every field
// position. If Rust's field order changes, this fails here instead of silently zeroing
// a benchmark's correctness score.
func TestOrderSentBatchV2WireContract(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(realProducerBatch)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	// 0x94 = msgpack array of 4. A map-encoded envelope would start 0x83/0x84.
	if raw[0] != 0x94 {
		t.Fatalf("envelope first byte = %#x, want 0x94 (array of 4): the producer writes a POSITIONAL envelope", raw[0])
	}

	var b OrderSentBatchV2
	if err := msgpack.Unmarshal(raw, &b); err != nil {
		t.Fatalf("decode OrderSentBatchV2 from real producer bytes: %v", err)
	}

	const wantSession = "019fb3a9-e275-70c7-8531-b0054ad6b035"
	if b.SessionID != wantSession {
		t.Errorf("SessionID = %q, want %q", b.SessionID, wantSession)
	}
	if b.SubmissionID != "p2-1785425683" {
		t.Errorf("SubmissionID = %q — envelope[1] must be submission_id", b.SubmissionID)
	}
	if b.WorkerID != "bot-fleet-worker-bc9d9cd66-tq5qk" {
		t.Errorf("WorkerID = %q — envelope[2] must be worker_id", b.WorkerID)
	}
	if len(b.Events) != 11 {
		t.Fatalf("Events = %d, want 11", len(b.Events))
	}

	// First event, field by field, against the values in the captured bytes.
	e := b.Events[0]
	if e.TaskID != 0 {
		t.Errorf("TaskID = %d, want 0", e.TaskID)
	}
	if want := wantSession + "_0_1847164_C"; e.OrderID != want {
		t.Errorf("OrderID = %q, want %q", e.OrderID, want)
	}
	if e.TargetSendTSNS != 1785425735299487168 {
		t.Errorf("TargetSendTSNS = %d, want 1785425735299487168", e.TargetSendTSNS)
	}
	if e.SendTSNS != 1785425735299487168 {
		t.Errorf("SendTSNS = %d, want 1785425735299487168", e.SendTSNS)
	}
	if e.RecvDoneTSNS != 1785425735694244307 {
		t.Errorf("RecvDoneTSNS = %d, want 1785425735694244307", e.RecvDoneTSNS)
	}
	if e.TimedOut {
		t.Error("TimedOut = true, want false")
	}
	if e.Price != 10004 {
		t.Errorf("Price = %d, want 10004", e.Price)
	}
	if e.Qty != 24 {
		t.Errorf("Qty = %d, want 24", e.Qty)
	}
	// Enums cross the wire as UPPERCASE strings (Rust #[serde(rename_all = "UPPERCASE")]).
	if e.Side != "BUY" {
		t.Errorf("Side = %q, want \"BUY\"", e.Side)
	}
	if e.PayloadType != "CANCEL" {
		t.Errorf("PayloadType = %q, want \"CANCEL\"", e.PayloadType)
	}
	if e.OrdType != "LIMIT" {
		t.Errorf("OrdType = %q, want \"LIMIT\"", e.OrdType)
	}
	// A CANCEL references the order it cancels, so OrigOrderID is populated here.
	if want := wantSession + "_0_1845275_O"; e.OrigOrderID != want {
		t.Errorf("OrigOrderID = %q, want %q", e.OrigOrderID, want)
	}
	if e.BarrierEpochNs != 1785425690308110021 {
		t.Errorf("BarrierEpochNs = %d, want 1785425690308110021", e.BarrierEpochNs)
	}
	// smp_id is the LAST positional field, so it is the one most likely to be lost to
	// an arity mismatch — and a lost id reads as SMPIDNone, i.e. "unconstrained",
	// which would silently disable self-match prevention rather than fail loudly.
	if e.SMPID != 4 {
		t.Errorf("SMPID = %d, want 4", e.SMPID)
	}

	// A MARKET/SELL event elsewhere in the batch proves the enum decoding is not just
	// matching the first event's values by luck.
	var sawMarket bool
	for _, ev := range b.Events {
		if ev.OrdType == "MARKET" {
			sawMarket = true
			if ev.Side != "SELL" && ev.Side != "BUY" {
				t.Errorf("MARKET event has Side = %q", ev.Side)
			}
		}
	}
	if !sawMarket {
		t.Error("fixture should contain a MARKET order; enum decoding is under-tested without one")
	}

	// The ids must VARY across the batch. A fixture where every event carried the same
	// id would pass the single-field check above while the producer-side rotation was
	// broken — which is exactly the state a stale controller binary produced: the field
	// was present on the wire but every value was SMPIDNone.
	ids := map[uint32]bool{}
	for _, ev := range b.Events {
		ids[ev.SMPID] = true
		if ev.SMPID == SMPIDNone {
			t.Errorf("event %q carries no SMP id; the correctness scenario seeds 8", ev.OrderID)
		}
		if ev.SMPID >= 8 {
			t.Errorf("SMPID = %d, outside the seeded range 0..7", ev.SMPID)
		}
	}
	if len(ids) < 2 {
		t.Errorf("all %d events share one SMP id (%v); the rotation is not reaching the wire", len(b.Events), ids)
	}
}

// TestOrderSentBatchV2IntoEvents checks the hoisted envelope fields are stamped onto
// every reconstituted event, mirroring Rust's OrderSentBatchV2::into_events.
func TestOrderSentBatchV2IntoEvents(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(realProducerBatch)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	var b OrderSentBatchV2
	if err := msgpack.Unmarshal(raw, &b); err != nil {
		t.Fatalf("decode: %v", err)
	}

	events := b.IntoEvents()
	if len(events) != len(b.Events) {
		t.Fatalf("IntoEvents returned %d events, want %d", len(events), len(b.Events))
	}
	for i, e := range events {
		if e.SessionID != b.SessionID {
			t.Errorf("event %d SessionID = %q, want the envelope's %q", i, e.SessionID, b.SessionID)
		}
		if e.SubmissionID != b.SubmissionID {
			t.Errorf("event %d SubmissionID = %q, want %q", i, e.SubmissionID, b.SubmissionID)
		}
		if e.WorkerID != b.WorkerID {
			t.Errorf("event %d WorkerID = %q, want %q", i, e.WorkerID, b.WorkerID)
		}
		if e.OrderID != b.Events[i].OrderID {
			t.Errorf("event %d OrderID = %q, want %q", i, e.OrderID, b.Events[i].OrderID)
		}
	}
}

// TestOrderSentBatchV1DecodeFailsOnV2Bytes documents the actual failure mode, so the
// regression is legible rather than folklore: the OLD named-map struct does not error
// on positional bytes — it silently produces a wrong SessionID, which is why a
// session-id filter dropped everything while nothing logged a decode error.
func TestOrderSentBatchV1DecodeFailsOnV2Bytes(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(realProducerBatch)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	var old OrderSentBatch
	err = msgpack.Unmarshal(raw, &old)
	if err == nil && old.SessionID == "019fb3a9-e275-70c7-8531-b0054ad6b035" {
		t.Fatal("the named-map struct decoded V2 bytes correctly; " +
			"if the producer reverted to a map envelope, OrderSentBatchV2 and its consumers must be revisited")
	}
}

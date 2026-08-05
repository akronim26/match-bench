// Package handler defines tests for the order_band field on createSlotRequest
// (bot-fleet-controller's exclusive per-session order-band lease, threaded
// through to the capture container as ORDER_BAND).
package handler

import (
	"encoding/json"
	"testing"

	"github.com/iicpc/schemas/topics"
)

func TestOrderBandDefaultsToUnsetWhenFieldOmitted(t *testing.T) {
	var req createSlotRequest
	if err := json.Unmarshal([]byte(`{"slot_id":"s1","image":"img","port":8080}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := req.orderBand(); got != topics.OrderBandUnset {
		t.Fatalf("orderBand() = %d, want OrderBandUnset (%d)", got, topics.OrderBandUnset)
	}
}

func TestOrderBandDefaultsToUnsetWhenFieldExplicitNull(t *testing.T) {
	var req createSlotRequest
	if err := json.Unmarshal([]byte(`{"slot_id":"s1","image":"img","port":8080,"order_band":null}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := req.orderBand(); got != topics.OrderBandUnset {
		t.Fatalf("orderBand() = %d, want OrderBandUnset (%d)", got, topics.OrderBandUnset)
	}
}

func TestOrderBandZeroIsDistinctFromOmitted(t *testing.T) {
	var req createSlotRequest
	if err := json.Unmarshal([]byte(`{"slot_id":"s1","image":"img","port":8080,"order_band":0}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := req.orderBand(); got != 0 {
		t.Fatalf("orderBand() = %d, want explicit band 0 (not the unset sentinel)", got)
	}
}

func TestOrderBandPassesThroughExplicitValue(t *testing.T) {
	var req createSlotRequest
	if err := json.Unmarshal([]byte(`{"slot_id":"s1","image":"img","port":8080,"order_band":3}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := req.orderBand(); got != 3 {
		t.Fatalf("orderBand() = %d, want 3", got)
	}
}

package topics

import (
	"reflect"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

func TestWorkloadSpecDecodesLegacySinglePayload(t *testing.T) {
	payload := []byte(`{
		"session_id":"sess-1",
		"submission_id":"sub-1",
		"target_host":"algo-sess-1.sandbox.svc.cluster.local",
		"target_port":9898,
		"protocol":"FIX",
		"worker_index":0,
		"worker_count":1,
		"global_seed":42,
		"tasks":[
			{"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000}
		]
	}`)

	var spec WorkloadSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		t.Fatalf("decode workload spec: %v", err)
	}
	if len(spec.Targets) != 0 {
		t.Fatalf("expected no targets on legacy payload, got %v", spec.Targets)
	}
	if spec.Tasks[0].TargetIdx != 0 {
		t.Fatalf("expected default target_idx 0, got %d", spec.Tasks[0].TargetIdx)
	}
	resolved := spec.ResolvedTargets()
	if len(resolved) != 1 || resolved[0].Protocol != "FIX" || resolved[0].Port != 9898 {
		t.Fatalf("unexpected resolved targets: %+v", resolved)
	}
}

func TestWorkloadSpecDecodesMultiTargetPayload(t *testing.T) {
	payload := []byte(`{
		"session_id":"sess-1",
		"submission_id":"sub-1",
		"target_host":"algo-sess-1.sandbox.svc.cluster.local",
		"target_port":9898,
		"protocol":"FIX",
		"targets":[
			{"protocol":"FIX","port":9898},
			{"protocol":"REST","port":8080},
			{"protocol":"WS","port":8080}
		],
		"worker_index":0,
		"worker_count":1,
		"global_seed":42,
		"tasks":[
			{"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000,"target_idx":2}
		]
	}`)

	var spec WorkloadSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		t.Fatalf("decode workload spec: %v", err)
	}
	if len(spec.Targets) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(spec.Targets))
	}
	if spec.Tasks[0].TargetIdx != 2 {
		t.Fatalf("expected target_idx 2, got %d", spec.Tasks[0].TargetIdx)
	}
	resolved := spec.ResolvedTargets()
	got := resolved[spec.Tasks[0].TargetIdx]
	if got.Protocol != "WS" || got.Port != 8080 {
		t.Fatalf("unexpected resolved target: %+v", got)
	}
}

func TestBandPartitionSameOrderSamePartition(t *testing.T) {
	order := "01890dd2-71f3-7abc-9def-0123456789ab_42_99_O"
	sent := BandPartition(2, order, 24, 6)
	acked := BandPartition(2, order, 24, 6)
	if sent != acked {
		t.Fatalf("band_partition not deterministic: %d vs %d", sent, acked)
	}
	if sent < 0 || sent >= 24 {
		t.Fatalf("partition %d out of range", sent)
	}
}

func TestBandPartitionConfinesBandToItsRange(t *testing.T) {
	n := int32(24)
	bandWidth := int32(6)
	for band := uint32(0); band < 4; band++ {
		bandStart := int32(band) * bandWidth
		for i := 0; i < 500; i++ {
			p := BandPartition(band, fmt.Sprintf("order_%d", i), n, bandWidth)
			if p < bandStart || p >= bandStart+bandWidth {
				t.Fatalf("partition %d escaped band %d range [%d, %d)", p, band, bandStart, bandStart+bandWidth)
			}
		}
	}
}

func TestBandPartitionExclusiveAcrossLeasedBands(t *testing.T) {
	n := int32(24)
	bandWidth := int32(6)
	ranges := make([]map[int32]bool, 4)
	for band := uint32(0); band < 4; band++ {
		seen := map[int32]bool{}
		for i := 0; i < 1000; i++ {
			seen[BandPartition(band, fmt.Sprintf("o_%d_%d", band, i), n, bandWidth)] = true
		}
		ranges[band] = seen
	}
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			for p := range ranges[i] {
				if ranges[j][p] {
					t.Fatalf("band %d and band %d overlap at partition %d", i, j, p)
				}
			}
		}
	}
}

func TestSessionBandPartitionSameOrderSamePartition(t *testing.T) {
	session := "01890dd2-71f3-7abc-9def-0123456789ab"
	order := "01890dd2-71f3-7abc-9def-0123456789ab_42_99_O"
	a := SessionBandPartition(session, order, 24, 8)
	b := SessionBandPartition(session, order, 24, 8)
	if a != b {
		t.Fatalf("session_band_partition not deterministic: %d vs %d", a, b)
	}
}

func TestOrderBandUnsetSentinel(t *testing.T) {
	if OrderBandUnset != math.MaxUint32 {
		t.Fatalf("OrderBandUnset must be math.MaxUint32, got %d", OrderBandUnset)
	}
	payload := []byte(`{
		"session_id":"sess-1",
		"submission_id":"sub-1",
		"target_host":"algo-sess-1.sandbox.svc.cluster.local",
		"target_port":8080,
		"protocol":"FIX",
		"worker_index":0,
		"worker_count":1,
		"global_seed":42,
		"order_band":4294967295,
		"tasks":[]
	}`)
	var spec WorkloadSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if spec.OrderBand != OrderBandUnset {
		t.Fatalf("expected unset sentinel, got %d", spec.OrderBand)
	}
}

func TestPortForProtocolMatchesPlatformPolicy(t *testing.T) {
	if PortForProtocol("FIX") != PortFIX {
		t.Fatalf("FIX port mismatch")
	}
	if PortForProtocol("REST") != PortHTTPWS {
		t.Fatalf("REST port mismatch")
	}
	if PortForProtocol("WS") != PortHTTPWS {
		t.Fatalf("WS port mismatch")
	}
	if PortFIX != 9898 || PortHTTPWS != 8080 {
		t.Fatalf("platform port constants drifted")
	}
}

// TestParseProtocols pins the protocol-declaration grammar (2026-08-02):
// a single protocol, "ALL" (alias for FIX,REST,WS), or an ordered
// comma-separated combination. Order is MEANINGFUL — the first element is the
// submission's primary protocol, the one pass-1 correctness runs on.
func TestParseProtocols(t *testing.T) {
	cases := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{"FIX", []string{"FIX"}, false},
		{"REST", []string{"REST"}, false},
		{"WS", []string{"WS"}, false},
		{"ALL", []string{"FIX", "REST", "WS"}, false},
		{"FIX,REST", []string{"FIX", "REST"}, false},
		{"REST,WS", []string{"REST", "WS"}, false},
		// Order preserved: REST-first combo grades pass-1 on REST.
		{"REST,FIX", []string{"REST", "FIX"}, false},
		{"ws,fix", []string{"WS", "FIX"}, false}, // case-insensitive
		{" FIX , WS ", []string{"FIX", "WS"}, false},
		{"FIX,FIX", nil, true},  // duplicate
		{"FIX,ALL", nil, true},  // ALL only stands alone
		{"HTTP", nil, true},     // unknown
		{"", nil, true},
		{",", nil, true},
	}
	for _, c := range cases {
		got, err := ParseProtocols(c.in)
		if c.wantErr != (err != nil) {
			t.Errorf("ParseProtocols(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if !c.wantErr && !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseProtocols(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

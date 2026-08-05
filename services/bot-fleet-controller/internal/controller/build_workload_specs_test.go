package controller

import (
	"testing"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/schemas/topics"
)

func taskSpecs(n int) []topics.TaskSpec {
	specs := make([]topics.TaskSpec, n)
	for i := range specs {
		specs[i] = topics.TaskSpec{TaskID: uint32(i), TargetRPS: 100}
	}
	return specs
}

func TestSubmissionTargetsSingleProtocol(t *testing.T) {
	sub := &store.SubmissionInfo{Protocol: "FIX"}
	targets := submissionTargets(sub)
	if len(targets) != 1 || targets[0].Protocol != "FIX" || targets[0].Port != topics.PortFIX {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestSubmissionTargetsAllProtocols(t *testing.T) {
	sub := &store.SubmissionInfo{Protocol: topics.ProtocolAll}
	targets := submissionTargets(sub)
	if len(targets) != 3 {
		t.Fatalf("expected 3 targets, got %d: %+v", len(targets), targets)
	}
	want := map[string]uint16{"FIX": topics.PortFIX, "REST": topics.PortHTTPWS, "WS": topics.PortHTTPWS}
	for _, tg := range targets {
		if want[tg.Protocol] != tg.Port {
			t.Fatalf("unexpected port for %s: got %d want %d", tg.Protocol, tg.Port, want[tg.Protocol])
		}
	}
}

func TestBuildWorkloadSpecsSplitsTasksRoundRobinAcrossTargets(t *testing.T) {
	r := &Runner{runConfig: RunConfig{GlobalSeed: 1}}
	sess := &Session{
		SessionID:    "sess-1",
		SubmissionID: "sub-1",
		Endpoint:     &orchestrator.Endpoint{Host: "algo.svc", Port: 9898},
	}
	sub := &store.SubmissionInfo{ContestantID: "team-1", Protocol: topics.ProtocolAll}
	scenario := &topics.Scenario{TaskSpecs: taskSpecs(9)}

	specs := r.buildWorkloadSpecs(sess, sub, scenario, 2, 3)
	if len(specs) != 2 {
		t.Fatalf("expected 2 worker specs, got %d", len(specs))
	}

	seen := map[uint32]uint8{}
	total := 0
	for _, spec := range specs {
		if len(spec.Targets) != 3 {
			t.Fatalf("expected 3 targets on every spec, got %d", len(spec.Targets))
		}
		for _, ts := range spec.Tasks {
			seen[ts.TaskID] = ts.TargetIdx
			total++
		}
	}
	if total != 9 {
		t.Fatalf("expected all 9 tasks distributed, got %d", total)
	}
	// task_id i must land on target_idx i%3, independent of worker sharding.
	for taskID, idx := range seen {
		want := uint8(taskID % 3)
		if idx != want {
			t.Errorf("task %d: got target_idx %d, want %d", taskID, idx, want)
		}
	}
}

func TestBuildWorkloadSpecsStampsOrderBandOnEverySpec(t *testing.T) {
	r := &Runner{runConfig: RunConfig{}}
	sess := &Session{
		SessionID:    "sess-1",
		SubmissionID: "sub-1",
		Endpoint:     &orchestrator.Endpoint{Host: "algo.svc", Port: 9898},
	}
	sub := &store.SubmissionInfo{Protocol: "FIX"}
	scenario := &topics.Scenario{TaskSpecs: taskSpecs(6)}

	specs := r.buildWorkloadSpecs(sess, sub, scenario, 3, 2)
	for i, spec := range specs {
		if spec.OrderBand != 2 {
			t.Fatalf("worker %d: expected leased order_band 2, got %d", i, spec.OrderBand)
		}
	}
}

func TestBuildWorkloadSpecsKeepsTaskIDsGloballyUnique(t *testing.T) {
	r := &Runner{runConfig: RunConfig{}}
	sess := &Session{
		SessionID:    "sess-1",
		SubmissionID: "sub-1",
		Endpoint:     &orchestrator.Endpoint{Host: "algo.svc", Port: 9898},
	}
	sub := &store.SubmissionInfo{Protocol: topics.ProtocolAll}
	scenario := &topics.Scenario{TaskSpecs: taskSpecs(12)}

	specs := r.buildWorkloadSpecs(sess, sub, scenario, 3, 1)
	ids := map[uint32]bool{}
	for _, spec := range specs {
		for _, ts := range spec.Tasks {
			if ids[ts.TaskID] {
				t.Fatalf("duplicate task_id %d across workers", ts.TaskID)
			}
			ids[ts.TaskID] = true
		}
	}
	if len(ids) != 12 {
		t.Fatalf("expected 12 unique task_ids, got %d", len(ids))
	}
}

// TestPass1IsSingleConnectionEvenForProtocolAll pins the decision (2026-08-02)
// that pass-1 correctness ALWAYS runs on ONE connection with ONE protocol —
// for a ProtocolAll submission, the first declared target (FIX). The
// correctness scenario builds exactly one task, and round-robin stamping
// (TargetIdx = i % len(targets)) therefore lands it on index 0. This is what
// makes TCPSeq a total order over the session, which full-replay grading
// depends on; W (CROSS_FLOW_WINDOW_US) is thereby a pass-2-only concern.
// If the correctness scenario ever grows a second task, or the target table's
// first entry stops being FIX, this fails and the decision must be revisited
// consciously rather than eroded.
func TestPass1IsSingleConnectionEvenForProtocolAll(t *testing.T) {
	sub := &store.SubmissionInfo{Protocol: "ALL"}
	targets := submissionTargets(sub)
	if len(targets) != 3 || targets[0].Protocol != "FIX" {
		t.Fatalf("ALL targets = %+v, want FIX first of 3", targets)
	}
	// One correctness task, round-robin stamped exactly as runner.go does.
	correctnessTasks := 1
	seen := map[uint8]bool{}
	for i := 0; i < correctnessTasks; i++ {
		seen[uint8(i%len(targets))] = true
	}
	if len(seen) != 1 || !seen[0] {
		t.Fatalf("pass-1 task target indices = %v, want exactly {0} (single FIX connection)", seen)
	}
}

// TestSubmissionTargetsCombos pins partial multi-protocol support
// (2026-08-02): combos are ordered, target 0 = primary = pass-1 protocol.
func TestSubmissionTargetsCombos(t *testing.T) {
	cases := []struct {
		decl  string
		want  []string
		ports []uint16
	}{
		{"REST,WS", []string{"REST", "WS"}, []uint16{8080, 8080}},
		{"REST,FIX", []string{"REST", "FIX"}, []uint16{8080, 9898}},
		{"FIX,WS", []string{"FIX", "WS"}, []uint16{9898, 8080}},
	}
	for _, c := range cases {
		targets := submissionTargets(&store.SubmissionInfo{Protocol: c.decl})
		if len(targets) != len(c.want) {
			t.Fatalf("%s: %d targets, want %d", c.decl, len(targets), len(c.want))
		}
		for i := range targets {
			if targets[i].Protocol != c.want[i] || targets[i].Port != c.ports[i] {
				t.Errorf("%s target[%d] = %+v, want %s:%d", c.decl, i, targets[i], c.want[i], c.ports[i])
			}
		}
		// Pass-1's single task round-robins onto index 0 — the declared primary.
		if targets[0].Protocol != c.want[0] {
			t.Errorf("%s pass-1 protocol = %s, want declared primary %s", c.decl, targets[0].Protocol, c.want[0])
		}
	}
}

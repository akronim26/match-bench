package main

import (
	"encoding/json"
	"testing"

	"github.com/iicpc/correctness-validator/internal/source"
)

// TestSessionModeAuto pins the two-pass routing: all-max-rate sessions (the
// pass-1 correctness scenario) get full replay, anything else gets invariants,
// and explicit VALIDATOR_MODE values force globally.
func TestSessionModeAuto(t *testing.T) {
	v := &validator{mode: "auto"}
	if got := v.sessionMode(source.SessionMeta{MaxRate: true}); got != "full" {
		t.Fatalf("auto+maxRate = %q, want full", got)
	}
	if got := v.sessionMode(source.SessionMeta{MaxRate: false}); got != "invariants" {
		t.Fatalf("auto+scale = %q, want invariants", got)
	}
	v.mode = "full"
	if got := v.sessionMode(source.SessionMeta{MaxRate: false}); got != "full" {
		t.Fatalf("forced full ignored: %q", got)
	}
	v.mode = "invariants"
	if got := v.sessionMode(source.SessionMeta{MaxRate: true}); got != "invariants" {
		t.Fatalf("forced invariants ignored: %q", got)
	}
}

// TestWorkloadSpecMaxRateDetection pins the wire-level signal: target_rps==0 on
// every task marks max-rate; any positive rate or an empty task list does not.
func TestWorkloadSpecMaxRateDetection(t *testing.T) {
	detect := func(raw string) bool {
		var spec workloadSpecOrderBand
		if err := json.Unmarshal([]byte(raw), &spec); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		maxRate := len(spec.Tasks) > 0
		for _, task := range spec.Tasks {
			if task.TargetRPS != 0 {
				maxRate = false
				break
			}
		}
		return maxRate
	}
	if !detect(`{"session_id":"s","tasks":[{"target_rps":0}]}`) {
		t.Fatal("single max-rate task not detected")
	}
	if detect(`{"session_id":"s","tasks":[{"target_rps":0},{"target_rps":1000}]}`) {
		t.Fatal("mixed rates must not be max-rate")
	}
	if detect(`{"session_id":"s","tasks":[]}`) {
		t.Fatal("empty task list must not be max-rate")
	}
	if detect(`{"session_id":"s"}`) {
		t.Fatal("absent tasks must not be max-rate")
	}
}

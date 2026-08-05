// Package main defines tests for main test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/store"
)

// TestCheckTimeoutConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckTimeoutConfig(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		settle  time.Duration
		wantErr bool
	}{
		{"valid budget", 60 * time.Second, 10 * time.Second, false},
		{"zero settle delay", 60 * time.Second, 0, false},
		{"zero timeout", 0, 0, true},
		{"negative timeout", -time.Second, 0, true},
		{"timeout equals settle", 10 * time.Second, 10 * time.Second, true},
		{"timeout below settle", 5 * time.Second, 10 * time.Second, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkTimeoutConfig(c.timeout, c.settle)
			if (err != nil) != c.wantErr {
				t.Errorf("checkTimeoutConfig(%v, %v) = %v, wantErr=%v", c.timeout, c.settle, err, c.wantErr)
			}
		})
	}
}

// TestTimeoutRecordShape performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestTimeoutRecordShape(t *testing.T) {
	rec := timeoutRecord("sess-timeout", 42)

	if rec.Status != store.StatusTimedOut {
		t.Errorf("Status = %q, want %q", rec.Status, store.StatusTimedOut)
	}
	if rec.SessionID != "sess-timeout" || rec.ComputedAtNS != 42 {
		t.Errorf("identity fields wrong: %+v", rec)
	}
	if rec.ContestantID != "" {
		t.Errorf("ContestantID = %q, want empty (the drain never completed, so it is unknown)", rec.ContestantID)
	}
	if rec.Report.TotalFills != 0 || rec.Report.PhantomFills != 0 {
		t.Errorf("fabricated fills in timeout placeholder: %+v", rec.Report)
	}
	if rec.Report.ViolationCount() != 0 || len(rec.Report.Violations) != 0 {
		t.Errorf("timeout placeholder carries violations: %+v", rec.Report)
	}
	if got := rec.Report.CorrectnessScore(); got != 1.0 {
		t.Errorf("CorrectnessScore() = %v, want 1.0 for the 0/0 placeholder", got)
	}
}

// TestInstanceGroupIsPerPod pins the band-learning consumer's group id to the
// pod identity. The band consumer must NOT share a consumer group across
// replicas: a shared group splits workload.assignments partitions between
// pods, so each replica learns only a subset of sessions' bands — and a
// replica validating a session whose band it never saw falls back to
// MaxRate=false, silently grading a pass-1 correctness session in invariants
// mode. Unique group per pod = every replica reads the whole topic.
func TestInstanceGroupIsPerPod(t *testing.T) {
	if got := instanceGroup("correctness-validator-band", "validator-7f9c"); got != "correctness-validator-band-validator-7f9c" {
		t.Errorf("instanceGroup with pod name = %q", got)
	}
	// No pod identity (local dev, tests): base group unchanged.
	if got := instanceGroup("correctness-validator-band", ""); got != "correctness-validator-band" {
		t.Errorf("instanceGroup without pod name = %q", got)
	}
}

// Package store defines tests for rungroup integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestIntegration_RecomputeRunGroupStatus performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_RecomputeRunGroupStatus(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the run-group rollup integration test")
	}
	ctx := context.Background()
	st, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC()
	sub := fmt.Sprintf("sub-h5-%d", now.UnixNano())
	grp := fmt.Sprintf("rg-h5-%d", now.UnixNano())
	a := grp + "-A"
	b := grp + "-B"
	g := RunGroupMeta{RunGroupID: grp, SubmissionID: sub, ContestantID: "c", Status: "requested", CreatedAt: now, UpdatedAt: now}
	children := []RunMeta{
		{SessionID: a, SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-1", Status: "requested", CreatedAt: now, UpdatedAt: now},
		{SessionID: b, SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-2", Status: "requested", CreatedAt: now, UpdatedAt: now},
	}
	if err := st.InsertRunGroupWithChildren(ctx, g, children); err != nil {
		t.Fatalf("insert group: %v", err)
	}

	if err := st.UpdateRunStatus(ctx, a, "failed", "boom"); err != nil {
		t.Fatalf("update A failed: %v", err)
	}
	if err := st.RecomputeRunGroupStatus(ctx, grp); err != nil {
		t.Fatalf("recompute (1): %v", err)
	}
	gm, err := st.GetRunGroup(ctx, grp)
	if err != nil {
		t.Fatalf("get group (1): %v", err)
	}
	if gm.Status == "failed" {
		t.Fatalf("H5: group went 'failed' on first child failure while sibling still in flight (status=%q)", gm.Status)
	}
	if gm.Status != "running" {
		t.Fatalf("group status with one failed + one requested = %q, want running", gm.Status)
	}

	if err := st.UpdateRunStatus(ctx, b, "completed", ""); err != nil {
		t.Fatalf("update B completed: %v", err)
	}
	if err := st.RecomputeRunGroupStatus(ctx, grp); err != nil {
		t.Fatalf("recompute (2): %v", err)
	}
	gm, err = st.GetRunGroup(ctx, grp)
	if err != nil {
		t.Fatalf("get group (2): %v", err)
	}
	if gm.Status != "failed" {
		t.Fatalf("group status with all terminal + one failed = %q, want failed", gm.Status)
	}
}

// TestIntegration_ListRunGroupsNilFilter performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_ListRunGroupsNilFilter(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the run-group list integration test")
	}
	ctx := context.Background()
	st, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC()
	contestant := fmt.Sprintf("c-listnil-%d", now.UnixNano())
	sub := fmt.Sprintf("sub-listnil-%d", now.UnixNano())
	grp := fmt.Sprintf("rg-listnil-%d", now.UnixNano())
	g := RunGroupMeta{RunGroupID: grp, SubmissionID: sub, ContestantID: contestant, Status: "requested", CreatedAt: now, UpdatedAt: now}
	children := []RunMeta{
		{SessionID: grp + "-A", SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-1", Status: "requested", CreatedAt: now, UpdatedAt: now},
	}
	if err := st.InsertRunGroupWithChildren(ctx, g, children); err != nil {
		t.Fatalf("insert group: %v", err)
	}

	tests := []struct {
		name          string
		submissionIDs []string
		wantGroups    int
	}{
		{name: "nil filter returns the group", submissionIDs: nil, wantGroups: 1},
		{name: "empty filter returns the group", submissionIDs: []string{}, wantGroups: 1},
		{name: "matching filter returns the group", submissionIDs: []string{sub}, wantGroups: 1},
		{name: "non-matching filter returns nothing", submissionIDs: []string{"sub-other"}, wantGroups: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups, err := st.ListRunGroups(ctx, RunGroupListFilter{
				ContestantID:  contestant,
				SubmissionIDs: tt.submissionIDs,
			})
			if err != nil {
				t.Fatalf("ListRunGroups: %v", err)
			}
			if len(groups) != tt.wantGroups {
				t.Fatalf("ListRunGroups returned %d groups, want %d", len(groups), tt.wantGroups)
			}
			if tt.wantGroups == 1 && groups[0].RunGroupID != grp {
				t.Fatalf("ListRunGroups returned group %q, want %q", groups[0].RunGroupID, grp)
			}
		})
	}
}

// TestIntegration_ClaimNextRunInGroup pins sequential-within-group dispatch
// (2026-08-02): claims hand out a group's `requested` runs one at a time in
// session_id (= scenario) order, flip each to `queued`, and return nil once
// exhausted. Concurrent/redelivered claims can never dispatch a run twice.
func TestIntegration_ClaimNextRunInGroup(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the claim integration test")
	}
	ctx := context.Background()
	st, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC()
	sub := fmt.Sprintf("sub-claim-%d", now.UnixNano())
	grp := fmt.Sprintf("rg-claim-%d", now.UnixNano())
	// session_id order = dispatch order; first child enters as queued (it is
	// dispatched directly by StartBenchmark).
	children := []RunMeta{
		{SessionID: grp + "-1", SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-correctness", Status: "queued", CreatedAt: now, UpdatedAt: now},
		{SessionID: grp + "-2", SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-ramp", Status: "requested", CreatedAt: now, UpdatedAt: now},
		{SessionID: grp + "-3", SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-spike", Status: "requested", CreatedAt: now, UpdatedAt: now},
	}
	g := RunGroupMeta{RunGroupID: grp, SubmissionID: sub, ContestantID: "c", Status: "requested", CreatedAt: now, UpdatedAt: now}
	if err := st.InsertRunGroupWithChildren(ctx, g, children); err != nil {
		t.Fatalf("insert group: %v", err)
	}

	first, err := st.ClaimNextRunInGroup(ctx, grp)
	if err != nil || first == nil || first.SessionID != grp+"-2" {
		t.Fatalf("claim 1 = %+v, %v; want session %s-2 (the queued first child is never re-claimed)", first, err, grp)
	}
	second, err := st.ClaimNextRunInGroup(ctx, grp)
	if err != nil || second == nil || second.SessionID != grp+"-3" {
		t.Fatalf("claim 2 = %+v, %v; want session %s-3", second, err, grp)
	}
	third, err := st.ClaimNextRunInGroup(ctx, grp)
	if err != nil || third != nil {
		t.Fatalf("claim 3 = %+v, %v; want nil (exhausted)", third, err)
	}
}

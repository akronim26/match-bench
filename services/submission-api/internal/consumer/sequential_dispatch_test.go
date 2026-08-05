// Package consumer tests for sequential-within-group dispatch (2026-08-02):
// a run group's scenarios execute one at a time — each terminal session
// (completed OR failed; continue-on-fail) dispatches the group's next
// scenario, so a 4-scenario group holds at most one order band at a time.
package consumer

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/iicpc/schemas/topics"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
)

type fakeStore struct {
	RunStatusStore
	next     []*store.RunMeta // popped per claim
	claims   int
	resets   []string
	claimErr error
}

func (f *fakeStore) ClaimNextRunInGroup(_ context.Context, _ string) (*store.RunMeta, error) {
	f.claims++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.next) == 0 {
		return nil, nil
	}
	n := f.next[0]
	f.next = f.next[1:]
	return n, nil
}

func (f *fakeStore) UpdateRunStatus(_ context.Context, sessionID, status, _ string) error {
	if status == topics.RunStatusRequested {
		f.resets = append(f.resets, sessionID)
	}
	return nil
}

type fakePub struct {
	published []publisher.BenchmarkMeta
	err       error
}

func (f *fakePub) PublishBenchmarkRequested(_ context.Context, m publisher.BenchmarkMeta) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, m)
	return nil
}

func consumerWith(st *fakeStore, pub *fakePub) *BenchmarkStatusConsumer {
	return &BenchmarkStatusConsumer{pg: st, pub: pub, log: slog.Default()}
}

func msgFor(status string) topics.BenchmarkStatusUpdated {
	return topics.BenchmarkStatusUpdated{
		SessionID: "sess-1", RunGroupID: "grp-1", Status: status,
	}
}

func TestTerminalStatusesAdvanceTheGroup(t *testing.T) {
	// Continue-on-fail: BOTH terminal outcomes dispatch the next scenario.
	for _, status := range []string{topics.RunStatusCompleted, topics.RunStatusFailed} {
		st := &fakeStore{next: []*store.RunMeta{{
			SessionID: "sess-2", SubmissionID: "sub", ContestantID: "c",
			RunGroupID: "grp-1", ScenarioID: "scen-ramp",
		}}}
		pub := &fakePub{}
		if ok := consumerWith(st, pub).advanceRunGroup(context.Background(), msgFor(status)); !ok {
			t.Fatalf("%s: advance reported failure", status)
		}
		if len(pub.published) != 1 || pub.published[0].SessionID != "sess-2" {
			t.Fatalf("%s: published = %+v, want exactly sess-2", status, pub.published)
		}
	}
}

func TestExhaustedGroupPublishesNothing(t *testing.T) {
	st := &fakeStore{} // no next run
	pub := &fakePub{}
	if ok := consumerWith(st, pub).advanceRunGroup(context.Background(), msgFor(topics.RunStatusCompleted)); !ok {
		t.Fatal("exhausted group must still report success (commit the message)")
	}
	if len(pub.published) != 0 {
		t.Fatalf("published %+v for an exhausted group", pub.published)
	}
}

func TestPublishFailureResetsClaimAndBlocksCommit(t *testing.T) {
	st := &fakeStore{next: []*store.RunMeta{{SessionID: "sess-2", RunGroupID: "grp-1"}}}
	pub := &fakePub{err: errors.New("kafka down")}
	if ok := consumerWith(st, pub).advanceRunGroup(context.Background(), msgFor(topics.RunStatusCompleted)); ok {
		t.Fatal("publish failure must block the commit so redelivery retries")
	}
	if len(st.resets) != 1 || st.resets[0] != "sess-2" {
		t.Fatalf("claimed run not reset to requested: resets=%v", st.resets)
	}
}

func TestClaimFailureBlocksCommit(t *testing.T) {
	st := &fakeStore{claimErr: errors.New("db down")}
	if ok := consumerWith(st, &fakePub{}).advanceRunGroup(context.Background(), msgFor(topics.RunStatusCompleted)); ok {
		t.Fatal("claim failure must block the commit")
	}
}

func TestNonTerminalStatusesAreNotTerminal(t *testing.T) {
	for _, s := range []string{topics.RunStatusRequested, topics.RunStatusQueued,
		topics.RunStatusDeploying, topics.RunStatusRunning, topics.RunStatusWaitingReady} {
		if terminalRunStatus(s) {
			t.Errorf("%s wrongly terminal", s)
		}
	}
	if !terminalRunStatus(topics.RunStatusCompleted) || !terminalRunStatus(topics.RunStatusFailed) {
		t.Error("completed/failed must be terminal")
	}
}

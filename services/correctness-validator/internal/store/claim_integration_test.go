// Package store defines tests for claim integration test.
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

	"github.com/iicpc/correctness-validator/internal/validate"
)

// TestIntegration_SaveClaimIdempotent performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_SaveClaimIdempotent(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the Save-claim integration test")
	}
	ctx := context.Background()
	st, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	sid := fmt.Sprintf("claim-%d", time.Now().UnixNano())
	rec := Record{
		SessionID:    sid,
		ContestantID: "team-claim",
		Report:       validate.Report{TotalFills: 4, ValidFills: 2, PhantomFills: 1, Overfills: 1},
		SentCount:    1000,
		AckedCount:   950,
		MatchedCount: 940,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}

	ins1, err := st.Save(ctx, rec)
	if err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if !ins1 {
		t.Fatal("first Save must report inserted=true (claimed)")
	}

	ins2, err := st.Save(ctx, rec)
	if err != nil {
		t.Fatalf("second Save: %v", err)
	}
	if ins2 {
		t.Fatal("M26: second Save for the same session must report inserted=false (already claimed)")
	}

	ev, ok, err := st.LoadScore(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("LoadScore = (%v, ok=%v, err=%v)", ev, ok, err)
	}
	if ev.ContestantID != "team-claim" || ev.TotalFills != 4 || ev.ValidFills != 2 {
		t.Errorf("LoadScore round-trip wrong: %+v", ev)
	}
	if ev.SentCount != 1000 || ev.AckedCount != 950 || ev.MatchedCount != 940 {
		t.Errorf("LoadScore counts = sent %d acked %d matched %d, want 1000/950/940", ev.SentCount, ev.AckedCount, ev.MatchedCount)
	}
}

// TestIntegration_SaveScoredOverwritesTimeout performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_SaveScoredOverwritesTimeout(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the Save-overwrite integration test")
	}
	ctx := context.Background()
	st, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	sid := fmt.Sprintf("status-%d", time.Now().UnixNano())

	if status, exists, err := st.SummaryStatus(ctx, sid); err != nil || exists || status != "" {
		t.Fatalf("SummaryStatus(missing) = (%q, %v, %v), want (\"\", false, nil)", status, exists, err)
	}

	placeholder := Record{
		SessionID:    sid,
		Status:       StatusTimedOut,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	claimed, err := st.Save(ctx, placeholder)
	if err != nil {
		t.Fatalf("Save placeholder: %v", err)
	}
	if !claimed {
		t.Fatal("first Save of the timed_out placeholder must report claimed=true")
	}
	if status, exists, err := st.SummaryStatus(ctx, sid); err != nil || !exists || status != StatusTimedOut {
		t.Fatalf("SummaryStatus after placeholder = (%q, %v, %v), want (%q, true, nil)", status, exists, err, StatusTimedOut)
	}

	if claimed, err := st.Save(ctx, placeholder); err != nil || claimed {
		t.Fatalf("repeat placeholder Save = (claimed=%v, err=%v), want (false, nil)", claimed, err)
	}

	scored := Record{
		SessionID:    sid,
		ContestantID: "team-status",
		Status:       StatusScored,
		Report: validate.Report{
			TotalFills: 4, ValidFills: 3, Overfills: 1,
			Violations: []validate.Violation{{
				Type: validate.Overfill, OrderID: "T1", ReportedQty: 5, ReportedPrice: 100,
				Detail: "cumulative reported 15 exceeds order qty 10",
			}},
		},
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	claimed, err = st.Save(ctx, scored)
	if err != nil {
		t.Fatalf("Save scored over placeholder: %v", err)
	}
	if !claimed {
		t.Fatal("scored Save over a timed_out placeholder must report claimed=true (caller publishes the real score)")
	}
	if status, exists, err := st.SummaryStatus(ctx, sid); err != nil || !exists || status != StatusScored {
		t.Fatalf("SummaryStatus after overwrite = (%q, %v, %v), want (%q, true, nil)", status, exists, err, StatusScored)
	}
	ev, ok, err := st.LoadScore(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("LoadScore = (%v, ok=%v, err=%v)", ev, ok, err)
	}
	if ev.ContestantID != "team-status" || ev.TotalFills != 4 || ev.ValidFills != 3 {
		t.Errorf("LoadScore after overwrite = %+v, want team-status 4/3", ev)
	}
	// The overwrite must carry the per-category counters too, not just the totals:
	// they are the whole breakdown now that the per-violation table is gone, so a
	// scored row that replaces a timed_out one while leaving stale category counts
	// behind would render a breakdown belonging to a different attempt.
	var overfillsAfter int64
	if err := st.pool.QueryRow(ctx,
		"SELECT overfills FROM correctness_summary WHERE session_id=$1", sid).Scan(&overfillsAfter); err != nil {
		t.Fatalf("read category counters after overwrite: %v", err)
	}
	if overfillsAfter != int64(scored.Report.Overfills) {
		t.Errorf("overfills after overwrite = %d, want %d", overfillsAfter, scored.Report.Overfills)
	}

	if claimed, err := st.Save(ctx, scored); err != nil || claimed {
		t.Fatalf("repeat scored Save = (claimed=%v, err=%v), want (false, nil)", claimed, err)
	}
	if claimed, err := st.Save(ctx, placeholder); err != nil || claimed {
		t.Fatalf("placeholder Save over scored = (claimed=%v, err=%v), want (false, nil)", claimed, err)
	}
	ev, ok, err = st.LoadScore(ctx, sid)
	if err != nil || !ok || ev.TotalFills != 4 || ev.ValidFills != 3 {
		t.Errorf("scored row mutated by late placeholder: (%+v, ok=%v, err=%v)", ev, ok, err)
	}
}

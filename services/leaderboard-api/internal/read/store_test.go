// Package read defines tests for store test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package read

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestLeaderboardOrderByAllowlist performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardOrderByAllowlist(t *testing.T) {
	got := leaderboardOrderBy("p99", "desc")
	if !strings.HasPrefix(got, "disqualified ASC, p99_at_peak_ns DESC") {
		t.Fatalf("order = %q", got)
	}

	injected := leaderboardOrderBy("p99;DROP TABLE scores", "desc;DROP")
	if strings.Contains(injected, ";") || !strings.HasPrefix(injected, "disqualified ASC, rank ASC") {
		t.Fatalf("unsafe fallback order = %q", injected)
	}
}

// TestLeaderboardOrderByDisqualifiedAlwaysLeads performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardOrderByDisqualifiedAlwaysLeads(t *testing.T) {
	cases := []struct {
		name string
		sort string
		ord  string
	}{
		{name: "default", sort: "", ord: ""},
		{name: "rank", sort: "rank", ord: "asc"},
		{name: "peak_tps_desc", sort: "peak_tps", ord: "desc"},
		{name: "peak_sustained_tps_asc", sort: "peak_sustained_tps", ord: "asc"},
		{name: "p99", sort: "p99", ord: ""},
		{name: "p99_at_peak", sort: "p99_at_peak", ord: "desc"},
		{name: "p99_ns_at_peak_tps", sort: "p99_ns_at_peak_tps", ord: "asc"},
		{name: "spike_recovery", sort: "spike_recovery", ord: ""},
		{name: "spike_recovery_ns", sort: "spike_recovery_ns", ord: "desc"},
		{name: "total_score", sort: "total_score", ord: ""},
		{name: "total_correctness", sort: "total_correctness", ord: "asc"},
		{name: "correctness", sort: "correctness", ord: "desc"},
		{name: "team_name", sort: "team_name", ord: ""},
		{name: "computed_at", sort: "computed_at", ord: "desc"},
		{name: "hyphen_alias", sort: "peak-tps", ord: "desc"},
		{name: "unknown_falls_back", sort: "disqualified", ord: "asc"},
		{name: "injection_falls_back", sort: "peak_tps;--", ord: "desc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := leaderboardOrderBy(tc.sort, tc.ord)
			if !strings.HasPrefix(got, "disqualified ASC, ") {
				t.Fatalf("sort=%q order=%q produced %q without leading disqualified ASC", tc.sort, tc.ord, got)
			}
		})
	}
}

// TestRankedSubqueryOrderLeadsWithDisqualified performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRankedSubqueryOrderLeadsWithDisqualified(t *testing.T) {
	if !strings.HasPrefix(rankedOrder, "disqualified ASC") {
		t.Fatalf("ranked subquery order must lead with disqualified ASC, got %q", rankedOrder)
	}
	wantOrder := []string{
		"disqualified ASC",
		"peak_sustained_tps DESC",
		"p99_at_peak_ns ASC",
		"spike_recovery_ns ASC",
		"total_correctness DESC",
		"run_group_id ASC",
	}
	last := -1
	for _, term := range wantOrder {
		i := strings.Index(rankedOrder, term)
		if i <= last {
			t.Fatalf("term %q missing or out of order in %q", term, rankedOrder)
		}
		last = i
	}
}

// TestLeaderboardCursorRoundTrip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardCursorRoundTrip(t *testing.T) {
	cursor := encodeLeaderboardCursor(125)
	offset, err := decodeLeaderboardCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if offset != 125 {
		t.Fatalf("offset = %d, want 125", offset)
	}
}

// TestLeaderboardCursorRejectsInvalidInput performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardCursorRejectsInvalidInput(t *testing.T) {
	if _, err := decodeLeaderboardCursor("not-base64"); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
}

// TestCacheableLeaderboardOnlyDefaultTopRank performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCacheableLeaderboardOnlyDefaultTopRank(t *testing.T) {
	if !cacheableLeaderboard(LeaderboardQuery{Limit: 50}) {
		t.Fatal("default top leaderboard should be cacheable")
	}
	if cacheableLeaderboard(LeaderboardQuery{Limit: 50, Sort: "p99"}) {
		t.Fatal("custom sorted leaderboard should not be cacheable")
	}
	if cacheableLeaderboard(LeaderboardQuery{Limit: 50, ContestantID: "c1"}) {
		t.Fatal("filtered leaderboard should not be cacheable")
	}
	if !cacheableLeaderboard(LeaderboardQuery{Limit: 50, Scenario: "constant"}) {
		t.Fatal("scenario-only leaderboard should be cacheable")
	}
}

// TestLeaderboardCacheKeyIncludesScenario ensures per-scenario boards do not
// collide in cache: distinct scenarios must produce distinct keys, and the
// empty scenario stays on the shared "all" key.
func TestLeaderboardCacheKeyIncludesScenario(t *testing.T) {
	all := leaderboardCacheKey(LeaderboardQuery{Limit: 50})
	constant := leaderboardCacheKey(LeaderboardQuery{Limit: 50, Scenario: "constant"})
	ramp := leaderboardCacheKey(LeaderboardQuery{Limit: 50, Scenario: "ramp"})
	if all == constant || constant == ramp {
		t.Fatalf("scenario keys must differ: all=%q constant=%q ramp=%q", all, constant, ramp)
	}
	if all != leaderboardCacheKey(LeaderboardQuery{Limit: 50, Scenario: ""}) {
		t.Fatal("empty scenario must map to the shared aggregate key")
	}
}

// TestIsUndefinedTable performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIsUndefinedTable(t *testing.T) {
	if !isUndefinedTable(&pgconn.PgError{Code: "42P01"}) {
		t.Fatal("undefined_table was not recognized")
	}
	if isUndefinedTable(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("non-undefined table error was recognized")
	}
}

// TestLeaderboardQueriesSelectJitterColumns is a no-Postgres regression check
// that both the paged Leaderboard() query and the single-row scoreForRunGroup()
// query select every jitter_* column, in the order LeaderboardRow.Scan expects
// (there is no sqlmock/pgxmock harness in this package to exercise the scan
// against a live driver, so this pins the SQL text instead).
func TestLeaderboardQueriesSelectJitterColumns(t *testing.T) {
	jitterCols := "jitter_p50_us, jitter_p99_us, jitter_p999_us, jitter_max_us, jitter_inversion_rate"
	if strings.Count(leaderboardQuerySQL, jitterCols) != 2 {
		t.Fatalf("expected jitter column list %q to appear twice (inner + outer select) in Leaderboard query", jitterCols)
	}
	if strings.Count(scoreForRunGroupQuerySQL, jitterCols) != 2 {
		t.Fatalf("expected jitter column list %q to appear twice (inner + outer select) in scoreForRunGroup query", jitterCols)
	}
}

// TestLeaderboardRowJitterJSONRoundTrip covers the struct-mapping/marshal
// logic for the new jitter fields on LeaderboardRow: the JSON tags used by
// the API response must round-trip losslessly, including the zero value
// (no recorded inversions), matching the CorrectnessScoreEvent convention.
func TestLeaderboardRowJitterJSONRoundTrip(t *testing.T) {
	want := LeaderboardRow{
		RunGroupID:    "rg-1",
		JitterP50US:   12.5,
		JitterP99US:   88.25,
		JitterP999US:  150,
		JitterMaxUS:   300,
		JitterInvRate: 0.002,
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got LeaderboardRow
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != want {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, want)
	}

	zero := LeaderboardRow{RunGroupID: "rg-2"}
	raw, err = json.Marshal(zero)
	if err != nil {
		t.Fatalf("marshal zero: %v", err)
	}
	if !strings.Contains(string(raw), `"jitter_p99_us":0`) {
		t.Fatalf("zero jitter must be emitted (no omitempty), got %s", raw)
	}
}

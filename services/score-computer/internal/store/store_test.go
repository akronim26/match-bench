// Package store defines tests for store test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package store

import (
	"strings"
	"testing"
)

// TestRankOrderByLeadsWithDisqualified performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRankOrderByLeadsWithDisqualified(t *testing.T) {
	if !strings.HasPrefix(rankOrderBy, "disqualified ASC") {
		t.Fatalf("ranking order must lead with disqualified ASC, got %q", rankOrderBy)
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
		i := strings.Index(rankOrderBy, term)
		if i <= last {
			t.Fatalf("term %q missing or out of order in %q", term, rankOrderBy)
		}
		last = i
	}
}

// TestScoresSortIndexMatchesRankOrder performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestScoresSortIndexMatchesRankOrder(t *testing.T) {
	if strings.Contains(createTableSQL, "idx_scores_sort_v2 ON") {
		t.Fatal("createTableSQL still creates the DQ-blind idx_scores_sort_v2")
	}
	if !strings.Contains(createTableSQL, "disqualified ASC, peak_sustained_tps DESC") {
		t.Fatal("scores sort index does not lead with disqualified ASC")
	}
}

// TestCreateTableSQLTelemetryCompletenessSchema performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCreateTableSQLTelemetryCompletenessSchema(t *testing.T) {
	migrations := []string{
		"ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS sent_count BIGINT",
		"ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS acked_count BIGINT",
		"ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS matched_count BIGINT",
		"ALTER TABLE scores ADD COLUMN IF NOT EXISTS incomplete_telemetry BOOLEAN NOT NULL DEFAULT false",
		"ALTER TABLE scoring_config ADD COLUMN IF NOT EXISTS min_coverage DOUBLE PRECISION NOT NULL DEFAULT 0.90",
	}
	for _, m := range migrations {
		if !strings.Contains(createTableSQL, m) {
			t.Errorf("createTableSQL missing idempotent migration %q", m)
		}
	}
	alterIdx := strings.Index(createTableSQL, "ADD COLUMN IF NOT EXISTS min_coverage")
	seedIdx := strings.Index(createTableSQL, "INSERT INTO scoring_config")
	if alterIdx == -1 || seedIdx == -1 || alterIdx > seedIdx {
		t.Errorf("min_coverage migration must precede the scoring_config seed (alter at %d, seed at %d)", alterIdx, seedIdx)
	}
	if !strings.Contains(createTableSQL, "min_coverage)") || !strings.Contains(createTableSQL, "0.90)") {
		t.Error("scoring_config seed does not include the 0.90 min_coverage default")
	}
}

// TestCreateTableSQLJitterSchema verifies the jitter columns are present on
// both score_progress (nullable, populated per-session by RecordCorrectness)
// and scores (NOT NULL DEFAULT 0, populated per-run-group by SaveScore), and
// that both are added via idempotent migrations so pre-existing tables
// upgrade cleanly.
func TestCreateTableSQLJitterSchema(t *testing.T) {
	jitterCols := []string{"jitter_p50_us", "jitter_p99_us", "jitter_p999_us", "jitter_max_us", "jitter_inversion_rate"}

	scoreProgressIdx := strings.Index(createTableSQL, "CREATE TABLE IF NOT EXISTS score_progress")
	scoringConfigIdx := strings.Index(createTableSQL, "CREATE TABLE IF NOT EXISTS scoring_config")
	scoresIdx := strings.Index(createTableSQL, "CREATE TABLE IF NOT EXISTS scores")
	if scoreProgressIdx == -1 || scoringConfigIdx == -1 || scoresIdx == -1 {
		t.Fatal("expected all three tables in createTableSQL")
	}
	scoreProgressBlock := createTableSQL[scoreProgressIdx:scoringConfigIdx]
	scoresBlock := createTableSQL[scoresIdx:]

	for _, col := range jitterCols {
		if !strings.Contains(scoreProgressBlock, col+" ") {
			t.Errorf("score_progress table definition missing column %q", col)
		}
		if !strings.Contains(scoreProgressBlock, "ADD COLUMN IF NOT EXISTS "+col+" DOUBLE PRECISION;") {
			t.Errorf("score_progress missing idempotent migration for %q", col)
		}
		if !strings.Contains(scoresBlock, col+" ") {
			t.Errorf("scores table definition missing column %q", col)
		}
		if !strings.Contains(scoresBlock, "ADD COLUMN IF NOT EXISTS "+col+" DOUBLE PRECISION NOT NULL DEFAULT 0;") {
			t.Errorf("scores missing idempotent migration for %q with DEFAULT 0", col)
		}
	}
}

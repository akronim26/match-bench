// Package score defines tests for score test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package score

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/iicpc/schemas/topics"
)

const wave = DefaultWaveDurationNS

// TestWaveScheduleDerivesRPSFromTaskIntervals performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestWaveScheduleDerivesRPSFromTaskIntervals(t *testing.T) {
	got := WaveSchedule([]topics.TaskSpec{
		{TargetRPS: 1000, StartOffsetNs: 0, DurationNs: 2 * wave},
		{TargetRPS: 2000, StartOffsetNs: wave, DurationNs: wave},
		{TargetRPS: 600, StartOffsetNs: wave / 2, DurationNs: wave},
	}, wave)
	want := []WaveOffer{
		{WaveIndex: 0, OfferedRPS: 1300},
		{WaveIndex: 1, OfferedRPS: 3300},
	}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wave %d = %#v want %#v", i, got[i], want[i])
		}
	}
}

// TestComputePassingClimbStopsAtFirstFail performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputePassingClimbStopsAtFirstFail(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Metrics = []MetricRow{
		{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0.001},
		{WaveIndex: 1, P99NS: 700_000, ErrorRate: 0.001},
		{WaveIndex: 2, P99NS: 1_500_000, ErrorRate: 0.001},
		{WaveIndex: 3, P99NS: 300_000, ErrorRate: 0.001},
	}
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disqualified {
		t.Fatalf("unexpected dq: %#v", res)
	}
	if res.PeakSustainedTPS != 20_000 || res.P99AtPeakNS != 700_000 {
		t.Fatalf("peak=(%d,%d), want (20000,700000)", res.PeakSustainedTPS, res.P99AtPeakNS)
	}
	if len(res.Waves) != 2 || res.Waves[1].Passed || res.Waves[1].Reason != "p99_latency" {
		t.Fatalf("bad wave walk: %#v", res.Waves)
	}
}

// TestComputeMixedZeroAndPositiveTargetRPSUsesOnlyGradedWaves checks that
// TargetRPS==0 tasks (the uncapped max-rate sentinel) overlapping the same
// ramp as throughput-graded (TargetRPS>0) tasks don't suppress the graded
// waves' PeakSustainedTPS computation.
func TestComputeMixedZeroAndPositiveTargetRPSUsesOnlyGradedWaves(t *testing.T) {
	in := baseInput()
	in.Sessions[2].TaskSpecs = append(in.Sessions[2].TaskSpecs,
		topics.TaskSpec{TargetRPS: 0, StartOffsetNs: 0, DurationNs: 3 * wave})
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.MaxRateOnly {
		t.Fatalf("mixed scenario must not be flagged MaxRateOnly: %#v", res)
	}
	if res.PeakSustainedTPS != 30_000 {
		t.Fatalf("peak=%d, want 30000 (unaffected by zero-target task)", res.PeakSustainedTPS)
	}
}

// TestComputeAllZeroTargetRPSIsMaxRateOnly checks that a ramp session made
// entirely of TargetRPS==0 tasks (correctness-pass-1, not throughput-graded)
// doesn't zero-fail the scenario: MaxRateOnly must be set and PeakSustainedTPS
// derived from measured TPS1S rather than the (nonexistent) offered rate.
func TestComputeAllZeroTargetRPSIsMaxRateOnly(t *testing.T) {
	in := baseInput()
	in.Sessions[2].TaskSpecs = []topics.TaskSpec{
		{TargetRPS: 0, StartOffsetNs: 0, DurationNs: 3 * wave},
	}
	in.Sessions[2].Metrics = []MetricRow{
		{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0, TPS1S: 12_345},
		{WaveIndex: 1, P99NS: 500_000, ErrorRate: 0, TPS1S: 54_321},
	}
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disqualified {
		t.Fatalf("all-max-rate scenario must not be disqualified by the peak gate: %#v", res)
	}
	if !res.MaxRateOnly {
		t.Fatalf("want MaxRateOnly=true, got %#v", res)
	}
	if res.PeakSustainedTPS != 54_321 {
		t.Fatalf("peak=%d, want 54321 (max observed TPS1S)", res.PeakSustainedTPS)
	}
	if len(res.Waves) != 0 {
		t.Fatalf("want no throughput-gated waves recorded, got %#v", res.Waves)
	}
}

// TestComputeCorrectnessDQ performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeCorrectnessDQ(t *testing.T) {
	in := baseInput()
	in.Sessions[0].Correct.ValidFills = 900
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Disqualified || res.PeakSustainedTPS != 30_000 || res.DisqualificationCode == "" {
		t.Fatalf("want dq with measured peak retained, got %#v", res)
	}
}

// TestComputeAbsentWaveFails performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeAbsentWaveFails(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Metrics = []MetricRow{
		{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0},
		{WaveIndex: 1, P99NS: 500_000, ErrorRate: 0},
	}
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.PeakSustainedTPS != 20_000 || len(res.Waves) != 2 || res.Waves[1].Reason != "missing_metrics" {
		t.Fatalf("unexpected absent-wave result: %#v", res)
	}
}

// TestComputeDeterministicJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeDeterministicJSON(t *testing.T) {
	in := baseInput()
	a, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatalf("not deterministic:\n%s\n%s", ja, jb)
	}
}

// TestComputeNoRampDisqualifiedReturnsResult performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeNoRampDisqualifiedReturnsResult(t *testing.T) {
	in := baseInput()
	in.Sessions = in.Sessions[:2]
	in.Sessions[0].Correct.ValidFills = 800
	res, err := Compute(in)
	if err != nil {
		t.Fatalf("rampless DQ run-group must score, not error: %v", err)
	}
	if !res.Disqualified || res.DisqualificationCode != "correctness_below_threshold" {
		t.Fatalf("want dq result, got %#v", res)
	}
	if res.PeakSustainedTPS != 0 {
		t.Fatalf("rampless run-group should not retain a peak: %#v", res)
	}
}

// TestComputeNoRampWithoutDQReturnsError performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeNoRampWithoutDQReturnsError(t *testing.T) {
	in := baseInput()
	// Drop ONLY the ramp. Truncating the slice also removed the pass-1 session, which is
	// now the sole source of correctness -- the group then scored 0, disqualified, and
	// Compute returned nil instead of the error this test exists to check.
	kept := in.Sessions[:0]
	for _, s := range in.Sessions {
		if s.Scenario != "ramp" {
			kept = append(kept, s)
		}
	}
	in.Sessions = kept
	if _, err := Compute(in); !errors.Is(err, ErrMissingRampSession) {
		t.Fatalf("err = %v, want ErrMissingRampSession", err)
	}
}

// TestComputeIncompleteTelemetrySkipsViolationDQ performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeIncompleteTelemetrySkipsViolationDQ(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Correct.ViolationCount = 1
	in.Sessions[2].Correct.SentCount = 1000
	in.Sessions[2].Correct.AckedCount = 900
	in.Sessions[2].Correct.MatchedCount = 850
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disqualified || res.DisqualificationCode != "" {
		t.Fatalf("high-ratio ramp must not be disqualified: %#v", res)
	}
	if !res.IncompleteTelemetry {
		t.Fatalf("IncompleteTelemetry not set: %#v", res)
	}
	if res.IncompleteTelemetryReason == "" {
		t.Fatalf("reason missing from score detail: %#v", res)
	}
	if res.PeakSustainedTPS != 30_000 {
		t.Fatalf("throughput waves must still be scored: %#v", res)
	}
}

// TestComputeCoverageAtThresholdKeepsViolationDQ performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeCoverageAtThresholdKeepsViolationDQ(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Correct.ValidFills = 850
	in.Sessions[2].Correct.SentCount = 1000
	in.Sessions[2].Correct.AckedCount = 960
	in.Sessions[2].Correct.MatchedCount = 950
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Disqualified || res.DisqualificationCode != "session_correctness_below_threshold" {
		t.Fatalf("a ramp below the correctness threshold must be disqualified: %#v", res)
	}
	if res.IncompleteTelemetry || res.IncompleteTelemetryReason != "" {
		t.Fatalf("flag must stay false at/above the coverage threshold: %#v", res)
	}
}

// TestComputeIncompleteTelemetryKeepsCorrectnessGates performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeIncompleteTelemetryKeepsCorrectnessGates(t *testing.T) {
	in := baseInput()
	// Degrade the PASS-1 session: it is the only source of the aggregate correctness
	// score now, so degrading a pass-2 session would leave the aggregate at 1.0 and
	// this test would silently stop exercising the gate it is named after.
	for i := range in.Sessions {
		if in.Sessions[i].Scenario == ScenarioCorrectness {
			in.Sessions[i].Correct.ValidFills = 800
		}
	}
	in.Sessions[2].Correct.SentCount = 1000
	in.Sessions[2].Correct.MatchedCount = 100
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Disqualified || res.DisqualificationCode != "correctness_below_threshold" {
		t.Fatalf("correctness-ratio gate must survive the completeness flag: %#v", res)
	}
	if !res.IncompleteTelemetry {
		t.Fatalf("IncompleteTelemetry not set: %#v", res)
	}
}

// TestComputeUnknownCoverageNotGated performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestComputeUnknownCoverageNotGated(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Correct.ValidFills = 850
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.IncompleteTelemetry || res.IncompleteTelemetryReason != "" {
		t.Fatalf("unknown coverage must not flag the run: %#v", res)
	}
	if !res.Disqualified || res.DisqualificationCode != "session_correctness_below_threshold" {
		t.Fatalf("ramp below correctness threshold must disqualify: %#v", res)
	}
}

// TestConfigWithDefaultsMinCoverage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestConfigWithDefaultsMinCoverage(t *testing.T) {
	cases := []struct {
		name string
		in   float64
		want float64
	}{
		{"zero falls back", 0, DefaultMinCoverage},
		{"negative falls back", -1, DefaultMinCoverage},
		{"above one falls back", 1.5, DefaultMinCoverage},
		{"NaN falls back", math.NaN(), DefaultMinCoverage},
		{"valid kept", 0.8, 0.8},
		{"one kept", 1, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{MinCoverage: c.in}.WithDefaults()
			if cfg.MinCoverage != c.want {
				t.Errorf("MinCoverage = %v, want %v", cfg.MinCoverage, c.want)
			}
		})
	}
}

// TestSortResultsDisqualifiedRanksLast performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSortResultsDisqualifiedRanksLast(t *testing.T) {
	results := []Result{
		{RunGroupID: "dq", PeakSustainedTPS: 1_000_000, P99AtPeakNS: 1, TotalCorrectness: 1, Disqualified: true},
		{RunGroupID: "slow", PeakSustainedTPS: 10, P99AtPeakNS: 900_000, TotalCorrectness: 0.99},
		{RunGroupID: "fast", PeakSustainedTPS: 50_000, P99AtPeakNS: 400_000, TotalCorrectness: 1},
	}
	SortResults(results)
	if results[len(results)-1].RunGroupID != "dq" {
		t.Fatalf("dq with retained peak must rank below every clean result: %#v", results)
	}
	if results[0].RunGroupID != "fast" || results[1].RunGroupID != "slow" {
		t.Fatalf("clean results lost their relative order: %#v", results)
	}
}

// TestSortResultsTiebreak performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSortResultsTiebreak(t *testing.T) {
	results := []Result{
		{RunGroupID: "b", PeakSustainedTPS: 20, P99AtPeakNS: 10, SpikeRecoveryNS: 5, TotalCorrectness: 0.999},
		{RunGroupID: "a", PeakSustainedTPS: 20, P99AtPeakNS: 5, SpikeRecoveryNS: 100, TotalCorrectness: 1},
		{RunGroupID: "c", PeakSustainedTPS: 30, P99AtPeakNS: 50, SpikeRecoveryNS: 1, TotalCorrectness: 0.99},
	}
	SortResults(results)
	if results[0].RunGroupID != "c" || results[1].RunGroupID != "a" || results[2].RunGroupID != "b" {
		t.Fatalf("bad order: %#v", results)
	}
}

// TestAggregateCorrectnessDoesNotOverflow performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAggregateCorrectnessDoesNotOverflow(t *testing.T) {
	got := aggregateCorrectness([]Session{
		{Scenario: ScenarioCorrectness, Correct: Correctness{ValidFills: math.MaxUint64, TotalFills: math.MaxUint64}},
		{Scenario: ScenarioCorrectness, Correct: Correctness{ValidFills: math.MaxUint64, TotalFills: math.MaxUint64}},
	})
	if got != 1 {
		t.Fatalf("correctness = %v, want 1", got)
	}
}

// TestAggregateCorrectnessIgnoresPass2 pins the rule that only the full-replay pass-1
// session decides correctness. Pass 2 grades book-free invariants, where an engine that
// fills every order unconditionally scores a perfect 1.0 -- measured at exactly that for
// the echo engine against 0.62 for the same binary in full mode. Pooling the two let the
// pass that cannot tell those apart outvote the one that can, weighted by fill count.
func TestAggregateCorrectnessIgnoresPass2(t *testing.T) {
	got := aggregateCorrectness([]Session{
		{Scenario: ScenarioCorrectness, Correct: Correctness{ValidFills: 50, TotalFills: 100}},
		{Scenario: "constant", Correct: Correctness{ValidFills: 10_000, TotalFills: 10_000}},
	})
	if got != 0.5 {
		t.Fatalf("correctness = %v, want 0.5 (pass-1 only, not diluted by a perfect pass-2)", got)
	}
}

// TestAggregateCorrectnessWithoutPass1IsZero: a group never graded against the reference
// book has no correctness evidence, and must not read as perfect.
func TestAggregateCorrectnessWithoutPass1IsZero(t *testing.T) {
	got := aggregateCorrectness([]Session{
		{Scenario: "constant", Correct: Correctness{ValidFills: 10_000, TotalFills: 10_000}},
	})
	if got != 0 {
		t.Fatalf("correctness = %v, want 0 when no pass-1 session exists", got)
	}
}

// TestAggregateJitterTakesWorstSessionPerPercentile verifies aggregateJitter
// reduces per-session jitter to the max across sessions, independently per
// field (not the totals from whichever session has the single worst p99).
func TestAggregateJitterTakesWorstSessionPerPercentile(t *testing.T) {
	p50, p99, p999, maxUS, invRate := aggregateJitter([]Session{
		{Correct: Correctness{JitterP50US: 5, JitterP99US: 40, JitterP999US: 80, JitterMaxUS: 100, JitterInvRate: 0.01}},
		{Correct: Correctness{JitterP50US: 10, JitterP99US: 20, JitterP999US: 90, JitterMaxUS: 50, JitterInvRate: 0.05}},
	})
	if p50 != 10 || p99 != 40 || p999 != 90 || maxUS != 100 || invRate != 0.05 {
		t.Fatalf("aggregateJitter = (%v,%v,%v,%v,%v), want (10,40,90,100,0.05)", p50, p99, p999, maxUS, invRate)
	}
}

// TestAggregateJitterZeroWhenNoSessions verifies the zero-inversions default
// (no recorded jitter anywhere) round-trips as all-zero, matching the
// CorrectnessScoreEvent convention.
func TestAggregateJitterZeroWhenNoSessions(t *testing.T) {
	p50, p99, p999, maxUS, invRate := aggregateJitter(nil)
	if p50 != 0 || p99 != 0 || p999 != 0 || maxUS != 0 || invRate != 0 {
		t.Fatalf("aggregateJitter(nil) = (%v,%v,%v,%v,%v), want all zero", p50, p99, p999, maxUS, invRate)
	}
}

// TestComputePropagatesJitterIntoResult verifies Compute() surfaces the
// aggregated jitter fields onto Result, so score-computer's SaveScore has
// them available to persist alongside the rest of the run-group score row.
func TestComputePropagatesJitterIntoResult(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Correct.JitterP50US = 12.5
	in.Sessions[2].Correct.JitterP99US = 88.25
	in.Sessions[2].Correct.JitterP999US = 150
	in.Sessions[2].Correct.JitterMaxUS = 300
	in.Sessions[2].Correct.JitterInvRate = 0.002
	res, err := Compute(in)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if res.JitterP50US != 12.5 || res.JitterP99US != 88.25 || res.JitterP999US != 150 ||
		res.JitterMaxUS != 300 || res.JitterInvRate != 0.002 {
		t.Fatalf("Result jitter fields not propagated: %+v", res)
	}
}

// TestWaveScheduleBoundsOverflowedTaskEnd performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestWaveScheduleBoundsOverflowedTaskEnd(t *testing.T) {
	got := WaveSchedule([]topics.TaskSpec{
		{TargetRPS: 1, StartOffsetNs: math.MaxUint64 - 1, DurationNs: math.MaxUint64},
	}, 1)
	if len(got) != 0 {
		t.Fatalf("overflowed late task should not allocate or wrap into early waves: %#v", got[:min(len(got), 3)])
	}
}

// baseInput performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func baseInput() Input {
	correct := Correctness{ValidFills: 1000, TotalFills: 1000}
	return Input{
		RunGroupID:   "rg",
		SubmissionID: "sub",
		ContestantID: "contestant",
		Sessions: []Session{
			{SessionID: "constant", Scenario: "constant", Correct: correct},
			{SessionID: "spike", Scenario: "spike", Correct: correct},
			{
				SessionID: "ramp",
				Scenario:  "ramp",
				Correct:   correct,
				TaskSpecs: []topics.TaskSpec{
					{TargetRPS: 10_000, StartOffsetNs: 0, DurationNs: wave},
					{TargetRPS: 20_000, StartOffsetNs: wave, DurationNs: wave},
					{TargetRPS: 30_000, StartOffsetNs: 2 * wave, DurationNs: wave},
				},
				Metrics: []MetricRow{
					{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0},
					{WaveIndex: 1, P99NS: 700_000, ErrorRate: 0},
					{WaveIndex: 2, P99NS: 900_000, ErrorRate: 0},
				},
			},
			// LAST on purpose: several tests address sessions positionally
			// (in.Sessions[2] is the ramp), so this must not shift their indices.
			// Correctness now comes only from the pass-1 session; without one the group
			// scores 0 and every fixture here would disqualify for a reason unrelated to
			// what it actually tests.
			{SessionID: "correctness", Scenario: ScenarioCorrectness, Correct: correct},
		},
	}
}

// TestP99AtPeakIsStableNotWorstSecond pins the 2026-08-02 ranking decision:
// the TPS tiebreak uses the STABLE p99 (median of the peak wave's per-second
// p99s) — the same summary the pass/fail gate uses — not the wave's single
// worst second, which is dominated by connection-setup/warmup noise and had
// been deciding ties despite its own "diagnostic" comment.
func TestP99AtPeakIsStableNotWorstSecond(t *testing.T) {
	in := baseInput()
	// Three seconds within the first JUDGED wave (the climb starts at wave
	// index 1; wave 0 is baseline): median 500us, worst second 900us.
	in.Sessions[2].Metrics = []MetricRow{
		{WaveIndex: 1, P99NS: 100_000, ErrorRate: 0.001},
		{WaveIndex: 1, P99NS: 500_000, ErrorRate: 0.001},
		{WaveIndex: 1, P99NS: 900_000, ErrorRate: 0.001},
	}
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.P99AtPeakNS != 500_000 {
		t.Fatalf("P99AtPeakNS = %d, want 500000 (stable/median), not the 900000 worst second", res.P99AtPeakNS)
	}
}

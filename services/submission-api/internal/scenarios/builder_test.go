// Package scenarios defines tests for builder test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package scenarios

import (
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

// sumRPSAt performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sumRPSAt(specs []topics.TaskSpec, t time.Duration) uint64 {
	var total uint64
	for _, s := range specs {
		start := time.Duration(s.StartOffsetNs)
		end := start + time.Duration(s.DurationNs)
		if t >= start && t < end {
			total += uint64(s.TargetRPS)
		}
	}
	return total
}

// TestConstantScenario_FlatBaseline performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestConstantScenario_FlatBaseline(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "constant")

	want := uint64(DefaultConfig().ConstantTotalRPS)
	for _, at := range []time.Duration{0, 30 * time.Second, 59 * time.Second} {
		got := sumRPSAt(c.TaskSpecs, at)
		if got != want {
			t.Errorf("constant: RPS at t=%v = %d, want %d", at, got, want)
		}
	}
}

// TestSpikeScenario_Shape performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSpikeScenario_Shape(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	s := find(t, rows, "spike")

	cases := []struct {
		at   time.Duration
		want uint64
	}{
		{at: 0, want: 10_000},
		{at: 24 * time.Second, want: 10_000},
		{at: 25 * time.Second, want: 50_000},
		{at: 30 * time.Second, want: 50_000},
		{at: 34 * time.Second, want: 50_000},
		{at: 35 * time.Second, want: 10_000},
		{at: 59 * time.Second, want: 10_000},
	}
	for _, c := range cases {
		got := sumRPSAt(s.TaskSpecs, c.at)
		if got != c.want {
			t.Errorf("spike: RPS at t=%v = %d, want %d", c.at, got, c.want)
		}
	}
}

// TestRampScenario_Staircase performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRampScenario_Staircase(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	r := find(t, rows, "ramp")

	perWave := uint64(DefaultConfig().RampPeakRPS / rampWaveCount)
	for wave := 0; wave < rampWaveCount; wave++ {
		at := time.Duration(wave)*rampWaveCadence + 1*time.Second
		expected := perWave * uint64(wave+1)
		got := sumRPSAt(r.TaskSpecs, at)
		if got != expected {
			t.Errorf("ramp: RPS at t=%v (after wave %d) = %d, want %d",
				at, wave, got, expected)
		}
	}

	peak := perWave * uint64(rampWaveCount)
	got := sumRPSAt(r.TaskSpecs, 179*time.Second)
	if got != peak {
		t.Errorf("ramp: RPS at t=179s = %d, want peak %d", got, peak)
	}
}

// TestConfig_CustomDurationAndRPS performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestConfig_CustomDurationAndRPS(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConstantDuration = 300 * time.Second
	cfg.ConstantTotalRPS = 50000
	cfg.SpikePeakRPS = 200000

	rows, err := BuildAll(cfg)
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "constant")

	if c.DurationNs != uint64((300 * time.Second).Nanoseconds()) {
		t.Errorf("constant duration = %d ns, want 300s", c.DurationNs)
	}
	for _, at := range []time.Duration{0, 150 * time.Second, 299 * time.Second} {
		if got := sumRPSAt(c.TaskSpecs, at); got != 50000 {
			t.Errorf("constant@50k: RPS at t=%v = %d, want 50000", at, got)
		}
	}

	s := find(t, rows, "spike")
	pre := cfg.SpikePreWindow
	if got := sumRPSAt(s.TaskSpecs, pre-time.Second); got != 50000 {
		t.Errorf("spike pre-burst RPS = %d, want 50000", got)
	}
	if got := sumRPSAt(s.TaskSpecs, pre+time.Second); got != 200000 {
		t.Errorf("spike burst RPS = %d, want 200000", got)
	}
}

// TestConfig_Validation performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestConfig_Validation(t *testing.T) {
	bad := []Config{
		func() Config { c := DefaultConfig(); c.ConstantDuration = 0; return c }(),
		func() Config { c := DefaultConfig(); c.ConstantTotalRPS = 0; return c }(),
		func() Config { c := DefaultConfig(); c.SpikePeakRPS = 1000; return c }(),
	}
	for i, c := range bad {
		if _, err := BuildAll(c); err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}

// TestConfig_ValidationRejectsMixNotSummingTo100 performs the package-specific
// operation described by its name. It protects QoL-6: the lifted population mix
// must still sum to 100, matching the pre-lift hardcoded 60/25/15 invariant.
func TestConfig_ValidationRejectsMixNotSummingTo100(t *testing.T) {
	c := DefaultConfig()
	c.MixHFTPct = 60
	c.MixRetailPct = 25
	c.MixInstitutionalPct = 10 // sums to 95, not 100

	if _, err := BuildAll(c); err == nil {
		t.Fatal("expected validation error for a mix that does not sum to 100")
	}
}

// TestConfig_ValidationRejectsActionPctOver100 guards the lifted per-profile
// action mixes (QoL-6): each must stay within 0-100.
func TestConfig_ValidationRejectsActionPctOver100(t *testing.T) {
	c := DefaultConfig()
	c.HFTMarketPct = 101

	if _, err := BuildAll(c); err == nil {
		t.Fatal("expected validation error for an action pct over 100")
	}
}

// TestConfig_MixOverrideChangesTaskActionMix verifies overriding the lifted
// Config fields actually reaches the emitted TaskSpecs (QoL-6's whole point: the
// per-protocol budget split edits these same call sites).
func TestConfig_MixOverrideChangesTaskActionMix(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HFTMarketPct = 77
	cfg.HFTCancelPct = 3
	cfg.HFTReplacePct = 1

	rows, err := BuildAll(cfg)
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "constant")
	found := false
	for _, ts := range c.TaskSpecs {
		if ts.Profile != "hft" {
			continue
		}
		found = true
		if ts.MarketPct != 77 || ts.CancelPct != 3 || ts.ReplacePct != 1 {
			t.Fatalf("hft task action mix = %d/%d/%d, want 77/3/1", ts.MarketPct, ts.CancelPct, ts.ReplacePct)
		}
	}
	if !found {
		t.Fatal("expected at least one hft task at the default constant RPS budget")
	}
}

// TestConfig_MixOverrideChangesPopulationSplit verifies the lifted MixHFTPct etc.
// actually drive botCountsForBudget, not just decorative fields.
func TestConfig_MixOverrideChangesPopulationSplit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MixHFTPct = 100
	cfg.MixRetailPct = 0
	cfg.MixInstitutionalPct = 0

	rows, err := BuildAll(cfg)
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "constant")
	for _, ts := range c.TaskSpecs {
		if ts.Profile != "hft" {
			t.Fatalf("expected only hft tasks at mix=100/0/0, found %q", ts.Profile)
		}
	}
}

// TestConfigFromEnv_ReadsMixAndActionPcts covers the env-var wiring QoL-6 adds
// alongside the existing RPS knobs.
func TestConfigFromEnv_ReadsMixAndActionPcts(t *testing.T) {
	t.Setenv("MIX_HFT_PCT", "50")
	t.Setenv("MIX_RETAIL_PCT", "30")
	t.Setenv("MIX_INSTITUTIONAL_PCT", "20")
	t.Setenv("HFT_MARKET_PCT", "9")
	t.Setenv("RETAIL_REPLACE_PCT", "0")
	t.Setenv("INSTITUTIONAL_CANCEL_PCT", "12")

	cfg := ConfigFromEnv()
	if cfg.MixHFTPct != 50 || cfg.MixRetailPct != 30 || cfg.MixInstitutionalPct != 20 {
		t.Fatalf("mix = %d/%d/%d, want 50/30/20", cfg.MixHFTPct, cfg.MixRetailPct, cfg.MixInstitutionalPct)
	}
	if cfg.HFTMarketPct != 9 {
		t.Fatalf("HFTMarketPct = %d, want 9", cfg.HFTMarketPct)
	}
	if cfg.RetailReplacePct != 0 {
		t.Fatalf("RetailReplacePct = %d, want 0 (explicit env override, not the fallback default)", cfg.RetailReplacePct)
	}
	if cfg.InstitutionalCancelPct != 12 {
		t.Fatalf("InstitutionalCancelPct = %d, want 12", cfg.InstitutionalCancelPct)
	}
}

// TestConfigFromEnv_UnsetMixFallsBackToDefaults ensures ConfigFromEnv without any
// of the new env vars reproduces the pre-lift hardcoded 60/25/15 population mix.
func TestConfigFromEnv_UnsetMixFallsBackToDefaults(t *testing.T) {
	cfg := ConfigFromEnv()
	def := DefaultConfig()
	if cfg.MixHFTPct != def.MixHFTPct || cfg.MixRetailPct != def.MixRetailPct || cfg.MixInstitutionalPct != def.MixInstitutionalPct {
		t.Fatalf("mix = %d/%d/%d, want defaults %d/%d/%d",
			cfg.MixHFTPct, cfg.MixRetailPct, cfg.MixInstitutionalPct,
			def.MixHFTPct, def.MixRetailPct, def.MixInstitutionalPct)
	}
}

// find performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCorrectnessScenario_SingleMaxRateTask(t *testing.T) {
	cfg := DefaultConfig()
	rows, err := BuildAll(cfg)
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "correctness")

	if len(c.TaskSpecs) != 1 {
		t.Fatalf("expected exactly 1 task, got %d", len(c.TaskSpecs))
	}
	task := c.TaskSpecs[0]
	if task.TargetRPS != 0 {
		t.Errorf("expected TargetRPS=0 (max-rate sentinel), got %d", task.TargetRPS)
	}
	if task.Profile != "hft" {
		t.Errorf("expected hft profile, got %q", task.Profile)
	}
	if c.DurationNs != uint64(cfg.CorrectnessDuration.Nanoseconds()) {
		t.Errorf("DurationNs = %d, want %d", c.DurationNs, cfg.CorrectnessDuration.Nanoseconds())
	}
	if task.MarketPct != cfg.HFTMarketPct || task.CancelPct != cfg.HFTCancelPct || task.ReplacePct != cfg.HFTReplacePct {
		t.Errorf("correctness task action mix should match HFT mix, got %+v", task)
	}
}

func TestCorrectnessScenario_DurationFromEnv(t *testing.T) {
	t.Setenv("CORRECTNESS_DURATION_S", "90")
	cfg := ConfigFromEnv()
	if cfg.CorrectnessDuration != 90*time.Second {
		t.Fatalf("expected 90s from env, got %v", cfg.CorrectnessDuration)
	}
}

func TestCorrectnessScenario_DefaultDuration(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.CorrectnessDuration != 45*time.Second {
		t.Fatalf("expected default 45s, got %v", cfg.CorrectnessDuration)
	}
}

func find(t *testing.T, rows []ScenarioRow, name string) ScenarioRow {
	t.Helper()
	for _, r := range rows {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("scenario %q not found", name)
	return ScenarioRow{}
}

// TestCorrectnessScenarioSeedsSMPIDs pins the seeding that makes pass 1 scoreable at
// all. Without rotating SMP ids, the correctness scenario's single task gives every
// order one participant identity, the reference book flags every fill as a self-trade,
// and a CORRECT engine scores 0 while one that refuses to trade scores 1.0.
func TestCorrectnessScenarioSeedsSMPIDs(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "correctness")
	if len(c.TaskSpecs) != 1 {
		t.Fatalf("correctness must stay a single task (one connection => total TCPSeq order), got %d", len(c.TaskSpecs))
	}
	if got := c.TaskSpecs[0].SMPIDCount; got < 2 {
		t.Errorf("correctness SMPIDCount = %d; must be >= 2 or every match is a self-match", got)
	}
	if got := c.TaskSpecs[0].SMPIDCount; got != correctnessSMPIDCount {
		t.Errorf("correctness SMPIDCount = %d, want %d", got, correctnessSMPIDCount)
	}

	// Scale scenarios must NOT carry SMP ids: SMP is not graded in pass 2, and omitting
	// the field keeps their wire frames byte-identical to pre-SMP output.
	for _, name := range []string{"constant", "spike", "ramp"} {
		s := find(t, rows, name)
		for i, ts := range s.TaskSpecs {
			if ts.SMPIDCount != 0 {
				t.Errorf("%s task %d SMPIDCount = %d, want 0", name, i, ts.SMPIDCount)
				break
			}
		}
	}
}

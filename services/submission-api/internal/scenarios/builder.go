// Package scenarios implements builder behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package scenarios

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/iicpc/schemas/topics"
)

const (
	rpsPerHFT           uint32 = 1000
	rpsPerRetail        uint32 = 5
	rpsPerInstitutional uint32 = 300
)

const (
	rampWaveCount   = 9 // 9 waves; peak ≈ RampPeakRPS held at the top of the ramp
	rampWaveCadence = 20 * time.Second
)

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Config struct {
	ConstantDuration    time.Duration
	SpikeDuration       time.Duration
	RampDuration        time.Duration
	CorrectnessDuration time.Duration

	ConstantTotalRPS uint32
	SpikePeakRPS     uint32
	RampPeakRPS      uint32

	SpikePreWindow   time.Duration
	SpikeBurstWindow time.Duration

	// Population mix: share of the RPS budget assigned to each bot profile.
	// Must sum to 100 (validate()).
	MixHFTPct           uint32
	MixRetailPct        uint32
	MixInstitutionalPct uint32

	// Per-profile action mix: percentage of a task's orders that are market
	// orders / cancels / replaces (the remainder are new limit orders).
	HFTMarketPct  uint8
	HFTCancelPct  uint8
	HFTReplacePct uint8

	RetailMarketPct  uint8
	RetailCancelPct  uint8
	RetailReplacePct uint8

	InstitutionalMarketPct  uint8
	InstitutionalCancelPct  uint8
	InstitutionalReplacePct uint8
}

// DefaultConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func DefaultConfig() Config {
	return Config{
		ConstantDuration:    60 * time.Second,
		SpikeDuration:       60 * time.Second,
		RampDuration:        180 * time.Second,
		CorrectnessDuration: 45 * time.Second,
		ConstantTotalRPS:    10000,
		SpikePeakRPS:     50000,
		RampPeakRPS:      90000,
		SpikePreWindow:   25 * time.Second,
		SpikeBurstWindow: 10 * time.Second,

		MixHFTPct:           60,
		MixRetailPct:        25,
		MixInstitutionalPct: 15,

		HFTMarketPct:  10,
		HFTCancelPct:  30,
		HFTReplacePct: 10,

		RetailMarketPct:  65,
		RetailCancelPct:  5,
		RetailReplacePct: 0,

		InstitutionalMarketPct:  20,
		InstitutionalCancelPct:  0,
		InstitutionalReplacePct: 0,
	}
}

// ConfigFromEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ConfigFromEnv() Config {
	c := DefaultConfig()
	c.ConstantDuration = envSeconds("CONSTANT_DURATION_S", c.ConstantDuration)
	c.SpikeDuration = envSeconds("SPIKE_DURATION_S", c.SpikeDuration)
	c.RampDuration = envSeconds("RAMP_DURATION_S", c.RampDuration)
	c.CorrectnessDuration = envSeconds("CORRECTNESS_DURATION_S", c.CorrectnessDuration)
	c.ConstantTotalRPS = envUint32("CONSTANT_TOTAL_RPS", c.ConstantTotalRPS)
	c.SpikePeakRPS = envUint32("SPIKE_PEAK_RPS", c.SpikePeakRPS)
	c.RampPeakRPS = envUint32("RAMP_PEAK_RPS", c.RampPeakRPS)
	c.SpikePreWindow = envSeconds("SPIKE_PREWINDOW_S", c.SpikePreWindow)
	c.SpikeBurstWindow = envSeconds("SPIKE_BURST_S", c.SpikeBurstWindow)

	c.MixHFTPct = envUint32("MIX_HFT_PCT", c.MixHFTPct)
	c.MixRetailPct = envUint32("MIX_RETAIL_PCT", c.MixRetailPct)
	c.MixInstitutionalPct = envUint32("MIX_INSTITUTIONAL_PCT", c.MixInstitutionalPct)

	c.HFTMarketPct = envUint8("HFT_MARKET_PCT", c.HFTMarketPct)
	c.HFTCancelPct = envUint8("HFT_CANCEL_PCT", c.HFTCancelPct)
	c.HFTReplacePct = envUint8("HFT_REPLACE_PCT", c.HFTReplacePct)

	c.RetailMarketPct = envUint8("RETAIL_MARKET_PCT", c.RetailMarketPct)
	c.RetailCancelPct = envUint8("RETAIL_CANCEL_PCT", c.RetailCancelPct)
	c.RetailReplacePct = envUint8("RETAIL_REPLACE_PCT", c.RetailReplacePct)

	c.InstitutionalMarketPct = envUint8("INSTITUTIONAL_MARKET_PCT", c.InstitutionalMarketPct)
	c.InstitutionalCancelPct = envUint8("INSTITUTIONAL_CANCEL_PCT", c.InstitutionalCancelPct)
	c.InstitutionalReplacePct = envUint8("INSTITUTIONAL_REPLACE_PCT", c.InstitutionalReplacePct)

	return c
}

// validate applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c Config) validate() error {
	if c.ConstantDuration <= 0 || c.SpikeDuration <= 0 || c.RampDuration <= 0 || c.CorrectnessDuration <= 0 {
		return fmt.Errorf("scenario durations must be positive (constant=%v spike=%v ramp=%v correctness=%v)",
			c.ConstantDuration, c.SpikeDuration, c.RampDuration, c.CorrectnessDuration)
	}
	if c.ConstantTotalRPS == 0 || c.SpikePeakRPS == 0 || c.RampPeakRPS == 0 {
		return fmt.Errorf("scenario RPS budgets must be positive (constant=%d spikePeak=%d rampPeak=%d)",
			c.ConstantTotalRPS, c.SpikePeakRPS, c.RampPeakRPS)
	}
	if c.SpikePeakRPS < c.ConstantTotalRPS {
		return fmt.Errorf("spike peak RPS %d must be >= baseline RPS %d", c.SpikePeakRPS, c.ConstantTotalRPS)
	}
	if c.SpikePreWindow+c.SpikeBurstWindow > c.SpikeDuration {
		return fmt.Errorf("spike pre-window %v + burst %v exceed spike duration %v",
			c.SpikePreWindow, c.SpikeBurstWindow, c.SpikeDuration)
	}
	if c.RampDuration <= time.Duration(rampWaveCount-1)*rampWaveCadence {
		return fmt.Errorf("ramp duration %v too short for %d waves at %v cadence",
			c.RampDuration, rampWaveCount, rampWaveCadence)
	}
	if sum := c.MixHFTPct + c.MixRetailPct + c.MixInstitutionalPct; sum != 100 {
		return fmt.Errorf("population mix must sum to 100, got %d (hft=%d retail=%d institutional=%d)",
			sum, c.MixHFTPct, c.MixRetailPct, c.MixInstitutionalPct)
	}
	for _, pct := range []uint8{
		c.HFTMarketPct, c.HFTCancelPct, c.HFTReplacePct,
		c.RetailMarketPct, c.RetailCancelPct, c.RetailReplacePct,
		c.InstitutionalMarketPct, c.InstitutionalCancelPct, c.InstitutionalReplacePct,
	} {
		if pct > 100 {
			return fmt.Errorf("action mix percentages must be 0-100, got %d", pct)
		}
	}
	return nil
}

// BuildAll performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func BuildAll(cfg Config) ([]ScenarioRow, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	constID, err := newUUID()
	if err != nil {
		return nil, err
	}
	spikeID, err := newUUID()
	if err != nil {
		return nil, err
	}
	rampID, err := newUUID()
	if err != nil {
		return nil, err
	}
	correctnessID, err := newUUID()
	if err != nil {
		return nil, err
	}

	return []ScenarioRow{
		{
			ScenarioID: constID,
			Name:       "constant",
			SortOrder:  1,
			DurationNs: uint64(cfg.ConstantDuration.Nanoseconds()),
			TaskSpecs:  buildConstantTasks(cfg, 0, 0, cfg.ConstantDuration, cfg.ConstantTotalRPS),
		},
		{
			ScenarioID: spikeID,
			Name:       "spike",
			SortOrder:  2,
			DurationNs: uint64(cfg.SpikeDuration.Nanoseconds()),
			TaskSpecs:  buildSpikeTasks(cfg),
		},
		{
			ScenarioID: rampID,
			Name:       "ramp",
			SortOrder:  3,
			DurationNs: uint64(cfg.RampDuration.Nanoseconds()),
			TaskSpecs:  buildRampTasks(cfg),
		},
		{
			ScenarioID: correctnessID,
			Name:       "correctness",
			SortOrder:  4,
			DurationNs: uint64(cfg.CorrectnessDuration.Nanoseconds()),
			TaskSpecs:  buildCorrectnessTasks(cfg),
		},
	}, nil
}

// buildCorrectnessTasks builds the pass-1 single-connection, max-rate,
// full-book-replay correctness gate scenario (docs/multi-contestant-audit.md
// §5, P-F pass 1): exactly one task, no pacer (TargetRPS: 0 is the max-rate
// sentinel honored by bot-fleet's write loops), single connection so TCPSeq
// gives an unambiguous total order, HFT action mix.
//
// SMPIDCount: 8 is what makes this pass scoreable. One task means one connection,
// which is deliberate — TCPSeq then totally orders every message. But it also means
// every order would share a single participant identity, so the reference book's
// self-trade check fired on EVERY fill: a correct engine scored 0 while an engine
// that refused to trade produced no fills and scored 1.0. Rotating 8 self-match
// prevention ids across orders on that one connection restores real cross-participant
// matching (the broker model: one session, many participants) without giving up the
// deterministic wire order. Scale scenarios leave this 0 — SMP is not graded in pass
// 2, and omitting the field keeps their frames byte-identical to pre-SMP output.
// correctnessSMPIDCount is how many self-match-prevention ids the correctness
// scenario's single task rotates through. Must be >= 2 for any matching to occur at
// all; 8 gives a realistic book without making self-crossing rare enough to go
// untested (roughly 1 in 8 potential matches is self-crossing).
const correctnessSMPIDCount = 8

func buildCorrectnessTasks(cfg Config) []topics.TaskSpec {
	return []topics.TaskSpec{
		{
			TaskID:        0,
			Profile:       "hft",
			TargetRPS:     0,
			StartOffsetNs: 0,
			DurationNs:    uint64(cfg.CorrectnessDuration.Nanoseconds()),
			MarketPct:     cfg.HFTMarketPct,
			CancelPct:     cfg.HFTCancelPct,
			ReplacePct:    cfg.HFTReplacePct,
			SMPIDCount:    correctnessSMPIDCount,
		},
	}
}

// ScenarioRow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ScenarioRow struct {
	ScenarioID string
	Name       string
	SortOrder  int
	DurationNs uint64
	TaskSpecs  []topics.TaskSpec
}

// botCountsForBudget performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func botCountsForBudget(cfg Config, totalRPS uint32) (hft, retail, inst, realisedRPS uint32) {
	hft = (totalRPS * cfg.MixHFTPct / 100) / rpsPerHFT
	retail = (totalRPS * cfg.MixRetailPct / 100) / rpsPerRetail
	inst = (totalRPS * cfg.MixInstitutionalPct / 100) / rpsPerInstitutional
	realisedRPS = hft*rpsPerHFT + retail*rpsPerRetail + inst*rpsPerInstitutional
	return hft, retail, inst, realisedRPS
}

// buildConstantTasks performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildConstantTasks(cfg Config, startTaskID uint32, startOffset, duration time.Duration, totalRPS uint32) []topics.TaskSpec {
	hft, retail, inst, _ := botCountsForBudget(cfg, totalRPS)
	return buildLayer(cfg, startTaskID, startOffset, duration, hft, retail, inst)
}

// buildSpikeTasks performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildSpikeTasks(cfg Config) []topics.TaskSpec {
	baseline := buildConstantTasks(cfg, 0, 0, cfg.SpikeDuration, cfg.ConstantTotalRPS)

	extraHFT, extraRetail, extraInst, _ := botCountsForBudget(cfg, cfg.SpikePeakRPS-cfg.ConstantTotalRPS)
	spike := buildLayer(
		cfg,
		uint32(len(baseline)),
		cfg.SpikePreWindow,
		cfg.SpikeBurstWindow,
		extraHFT, extraRetail, extraInst,
	)

	out := make([]topics.TaskSpec, 0, len(baseline)+len(spike))
	out = append(out, baseline...)
	out = append(out, spike...)
	return out
}

// buildRampTasks performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildRampTasks(cfg Config) []topics.TaskSpec {
	perWaveRPS := cfg.RampPeakRPS / rampWaveCount
	out := make([]topics.TaskSpec, 0)

	for wave := 0; wave < rampWaveCount; wave++ {
		startOffset := time.Duration(wave) * rampWaveCadence
		duration := cfg.RampDuration - startOffset
		if duration <= 0 {
			continue
		}
		hft, retail, inst, _ := botCountsForBudget(cfg, perWaveRPS)
		layer := buildLayer(cfg, uint32(len(out)), startOffset, duration, hft, retail, inst)
		out = append(out, layer...)
	}
	return out
}

// buildLayer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildLayer(cfg Config, startTaskID uint32, startOffset, duration time.Duration,
	hftBots, retailBots, institutionalBots uint32) []topics.TaskSpec {
	total := hftBots + retailBots + institutionalBots
	tasks := make([]topics.TaskSpec, 0, total)
	id := startTaskID

	for i := uint32(0); i < hftBots; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        id,
			Profile:       "hft",
			TargetRPS:     rpsPerHFT,
			StartOffsetNs: uint64(startOffset.Nanoseconds()),
			DurationNs:    uint64(duration.Nanoseconds()),
			MarketPct:     cfg.HFTMarketPct,
			CancelPct:     cfg.HFTCancelPct,
			ReplacePct:    cfg.HFTReplacePct,
		})
		id++
	}
	for i := uint32(0); i < retailBots; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        id,
			Profile:       "retail",
			TargetRPS:     rpsPerRetail,
			StartOffsetNs: uint64(startOffset.Nanoseconds()),
			DurationNs:    uint64(duration.Nanoseconds()),
			MarketPct:     cfg.RetailMarketPct,
			CancelPct:     cfg.RetailCancelPct,
			ReplacePct:    cfg.RetailReplacePct,
		})
		id++
	}
	for i := uint32(0); i < institutionalBots; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        id,
			Profile:       "institutional",
			TargetRPS:     rpsPerInstitutional,
			StartOffsetNs: uint64(startOffset.Nanoseconds()),
			DurationNs:    uint64(duration.Nanoseconds()),
			MarketPct:     cfg.InstitutionalMarketPct,
			CancelPct:     cfg.InstitutionalCancelPct,
			ReplacePct:    cfg.InstitutionalReplacePct,
		})
		id++
	}

	return tasks
}

// newUUID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newUUID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("uuid v7: %w", err)
	}
	return id.String(), nil
}

// envSeconds performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envSeconds(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

// envUint32 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envUint32(key string, def uint32) uint32 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return def
	}
	return uint32(n)
}

// envUint8 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
// Unlike envUint32, 0 is a valid override (several action-mix percentages default to
// 0, e.g. RetailReplacePct), so only a parse error or out-of-range value falls back.
func envUint8(key string, def uint8) uint8 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 8)
	if err != nil {
		return def
	}
	return uint8(n)
}

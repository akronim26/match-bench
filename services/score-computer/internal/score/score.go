// Package score implements score behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package score

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/iicpc/schemas/topics"
)

const (
	DefaultCorrectnessDQThreshold = 0.95
	DefaultMaxErrorRate           = 0.01
	DefaultMaxP99NS               = uint64(1_000_000)
	DefaultWaveDurationNS         = uint64(20_000_000_000)
	DefaultMaxScheduledWaves      = uint64(10_000)
	DefaultMinCoverage            = 0.90
)

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Config struct {
	CorrectnessDQThreshold float64
	MaxErrorRate           float64
	MaxP99NS               uint64
	WaveDurationNS         uint64
	MinCoverage            float64
}

// WithDefaults applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c Config) WithDefaults() Config {
	if !isPositiveFinite(c.CorrectnessDQThreshold) || c.CorrectnessDQThreshold > 1 {
		c.CorrectnessDQThreshold = DefaultCorrectnessDQThreshold
	}
	if !isPositiveFinite(c.MaxErrorRate) {
		c.MaxErrorRate = DefaultMaxErrorRate
	}
	if c.MaxP99NS == 0 {
		c.MaxP99NS = DefaultMaxP99NS
	}
	if c.WaveDurationNS == 0 {
		c.WaveDurationNS = DefaultWaveDurationNS
	}
	if !isPositiveFinite(c.MinCoverage) || c.MinCoverage > 1 {
		c.MinCoverage = DefaultMinCoverage
	}
	return c
}

// isPositiveFinite performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isPositiveFinite(v float64) bool {
	return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}

// Correctness groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Correctness struct {
	SessionID      string
	ValidFills     uint64
	TotalFills     uint64
	ViolationCount uint64
	SentCount      uint64
	AckedCount     uint64
	MatchedCount   uint64
	// P-G jitter (docs/multi-contestant-audit.md §5), mirrored from
	// CorrectnessScoreEvent. Zero means no recorded inversions.
	JitterP50US   float64
	JitterP99US   float64
	JitterP999US  float64
	JitterMaxUS   float64
	JitterInvRate float64
}

// MetricRow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type MetricRow struct {
	WaveIndex int
	P99NS     uint64
	TPS1S     float64
	ErrorRate float64
}

// Session groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Session struct {
	SessionID  string
	Scenario   string
	TaskSpecs  []topics.TaskSpec
	Correct    Correctness
	Metrics    []MetricRow
	DurationNS uint64
}

// Input groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Input struct {
	RunGroupID   string
	SubmissionID string
	ContestantID string
	TeamName     string
	Sessions     []Session
	Config       Config
}

// WaveResult groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type WaveResult struct {
	WaveIndex  int    `json:"wave_index"`
	OfferedRPS uint64 `json:"offered_rps"`
	P99NS      uint64 `json:"p99_ns"`
	Passed     bool   `json:"passed"`
	Reason     string `json:"reason,omitempty"`
}

// Result groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Result struct {
	RunGroupID                string  `json:"run_group_id"`
	SubmissionID              string  `json:"submission_id"`
	ContestantID              string  `json:"contestant_id"`
	TeamName                  string  `json:"team_name"`
	PeakSustainedTPS          uint64  `json:"peak_sustained_tps"`
	P99AtPeakNS               uint64  `json:"p99_at_peak_ns"`
	SpikeRecoveryNS           uint64  `json:"spike_recovery_ns"`
	TotalCorrectness          float64 `json:"total_correctness"`
	Disqualified              bool    `json:"disqualified"`
	DisqualificationCode      string  `json:"disqualification_code,omitempty"`
	IncompleteTelemetry       bool    `json:"incomplete_telemetry"`
	IncompleteTelemetryReason string  `json:"incomplete_telemetry_reason,omitempty"`
	// Jitter* is the worst-case (max across sessions) P-G jitter for the
	// run-group. Zero means no recorded inversions in any session.
	JitterP50US   float64      `json:"jitter_p50_us"`
	JitterP99US   float64      `json:"jitter_p99_us"`
	JitterP999US  float64      `json:"jitter_p999_us"`
	JitterMaxUS   float64      `json:"jitter_max_us"`
	JitterInvRate float64      `json:"jitter_inversion_rate"`
	Waves         []WaveResult `json:"waves,omitempty"`
	// MaxRateOnly is true when every ramp task has TargetRPS==0 (the uncapped
	// max-rate sentinel used by correctness-pass-1 scenarios, which are not
	// throughput-graded). PeakSustainedTPS is then derived from measured TPS1S
	// samples rather than an offered rate, and downstream scoring must not
	// penalize a resulting zero/low peak as a failed throughput gate.
	MaxRateOnly bool `json:"max_rate_only,omitempty"`
}

var ErrMissingRampSession = errors.New("missing ramp session")

// Compute performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Compute(in Input) (Result, error) {
	cfg := in.Config.WithDefaults()
	res := Result{
		RunGroupID:       in.RunGroupID,
		SubmissionID:     in.SubmissionID,
		ContestantID:     in.ContestantID,
		TeamName:         in.TeamName,
		TotalCorrectness: aggregateCorrectness(in.Sessions),
	}
	res.JitterP50US, res.JitterP99US, res.JitterP999US, res.JitterMaxUS, res.JitterInvRate = aggregateJitter(in.Sessions)

	for i := range in.Sessions {
		c := in.Sessions[i].Correct
		if c.SentCount == 0 {
			continue
		}
		coverage := min(float64(c.MatchedCount)/float64(c.SentCount), 1.0)
		if coverage < cfg.MinCoverage {
			res.IncompleteTelemetry = true
			res.IncompleteTelemetryReason = fmt.Sprintf(
				"session %s telemetry coverage %.4f (matched %d / sent %d) below min_coverage %g",
				in.Sessions[i].SessionID, coverage, c.MatchedCount, c.SentCount, cfg.MinCoverage)
			break
		}
	}

	var ramp *Session
	disqualificationCode := ""
	if res.TotalCorrectness < cfg.CorrectnessDQThreshold {
		disqualificationCode = "correctness_below_threshold"
	}
	for i := range in.Sessions {
		s := &in.Sessions[i]
		if s.Correct.TotalFills > 0 {
			sessionCorrectness := float64(s.Correct.ValidFills) / float64(s.Correct.TotalFills)
			sessionCorrectness = min(sessionCorrectness, 1.0)
			if sessionCorrectness < cfg.CorrectnessDQThreshold && disqualificationCode == "" {
				disqualificationCode = "session_correctness_below_threshold"
			}
		}
		if s.Scenario == "ramp" {
			ramp = s
		}
	}
	if ramp == nil {
		if disqualificationCode != "" {
			res.Disqualified = true
			res.DisqualificationCode = disqualificationCode
			return res, nil
		}
		return res, ErrMissingRampSession
	}

	res.SpikeRecoveryNS = spikeRecoveryNS(in.Sessions, cfg.WaveDurationNS)

	// TargetRPS==0 is the uncapped max-rate sentinel for correctness-pass-1
	// scenarios, which are not throughput-graded. If every ramp task uses it,
	// there's no offered rate to gate on, so fall back to measured TPS1S
	// samples (already collected per-wave) rather than zero-failing the whole
	// scenario for having no throughput-graded waves at all.
	maxRateOnly := len(ramp.TaskSpecs) > 0
	for _, t := range ramp.TaskSpecs {
		if t.TargetRPS != 0 {
			maxRateOnly = false
			break
		}
	}

	if maxRateOnly {
		res.MaxRateOnly = true
		var peak float64
		for _, m := range ramp.Metrics {
			if m.TPS1S > peak {
				peak = m.TPS1S
			}
		}
		res.PeakSustainedTPS = uint64(math.Round(peak))
	} else {
		schedule := WaveSchedule(ramp.TaskSpecs, cfg.WaveDurationNS)
		metrics := summarizeMetrics(ramp.Metrics)
		for _, wave := range schedule {
			if wave.WaveIndex == 0 {
				continue
			}
			wr := WaveResult{WaveIndex: wave.WaveIndex, OfferedRPS: wave.OfferedRPS}
			m, ok := metrics[wave.WaveIndex]
			if !ok || m.Count == 0 {
				wr.Passed = false
				wr.Reason = "missing_metrics"
				res.Waves = append(res.Waves, wr)
				break
			}
			wr.P99NS = m.StableP99NS
			switch {
			case m.MaxErrorRate > cfg.MaxErrorRate:
				wr.Reason = "error_rate"
			case m.StableP99NS > cfg.MaxP99NS:
				wr.Reason = "p99_latency"
			default:
				wr.Passed = true
				res.PeakSustainedTPS = wave.OfferedRPS
				// StableP99NS, not MaxP99NS (decision 2026-08-02): this field
				// is the TPS tiebreak in SortResults, and the wave's single
				// worst second is dominated by connection-setup/warmup noise —
				// ties were being broken by whose startup hiccup was smaller.
				// The median is the same summary the gate two cases above
				// judges, so gate and ranking now agree on what latency means.
				// MaxP99NS remains computed as the diagnostic it claims to be.
				res.P99AtPeakNS = m.StableP99NS
			}
			res.Waves = append(res.Waves, wr)
			if !wr.Passed {
				break
			}
		}
	}
	if disqualificationCode != "" {
		res.Disqualified = true
		res.DisqualificationCode = disqualificationCode
	}
	return res, nil
}

// aggregateCorrectness performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
// ScenarioCorrectness is the pass-1 scenario: one task on one connection, replayed
// against the reference book in full mode. It is the ONLY source of the published
// correctness score.
const ScenarioCorrectness = "correctness"

// aggregateCorrectness returns the correctness of the PASS-1 session alone.
//
// It used to pool ValidFills/TotalFills across every session in the run-group, which made
// the published number a fill-weighted blend of full-replay and invariants grading. That
// is not a stricter or a looser rule, it is a meaningless one: the two passes answer
// different questions. Pass 2 grades book-free invariants, so an engine that fills every
// order unconditionally scores 1.0 there -- measured at exactly 1.0 for the echo engine,
// against 0.62 for the same binary in full mode. Pass-2 sessions also carry far more fills
// than the single-connection pass-1 run, so the blend was DOMINATED by the pass that cannot
// tell a correct engine from a fill-everything one, and it fed both the disqualification
// gate and the leaderboard ranking.
//
// Pass-2 correctness is still computed, stored per session and available as a metric; it
// simply does not decide anything.
//
// With no pass-1 session in the group there is nothing to certify, so this returns 0 rather
// than falling back to whatever sessions exist — a group that was never graded against the
// book must not read as perfect.
func aggregateCorrectness(sessions []Session) float64 {
	var valid, total float64
	for _, s := range sessions {
		if s.Scenario != ScenarioCorrectness {
			continue
		}
		valid += float64(s.Correct.ValidFills)
		total += float64(s.Correct.TotalFills)
	}
	if total == 0 {
		return 0
	}
	return min(valid/total, 1.0)
}

// aggregateJitter reduces per-session P-G jitter to the worst-case value
// across the run-group's sessions (max of each percentile independently).
func aggregateJitter(sessions []Session) (p50, p99, p999, maxUS, invRate float64) {
	for _, s := range sessions {
		c := s.Correct
		p50 = math.Max(p50, c.JitterP50US)
		p99 = math.Max(p99, c.JitterP99US)
		p999 = math.Max(p999, c.JitterP999US)
		maxUS = math.Max(maxUS, c.JitterMaxUS)
		invRate = math.Max(invRate, c.JitterInvRate)
	}
	return p50, p99, p999, maxUS, invRate
}

// WaveOffer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type WaveOffer struct {
	WaveIndex  int
	OfferedRPS uint64
}

// WaveSchedule performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func WaveSchedule(tasks []topics.TaskSpec, waveDurationNS uint64) []WaveOffer {
	if waveDurationNS == 0 {
		waveDurationNS = DefaultWaveDurationNS
	}
	var maxEnd uint64
	for _, t := range tasks {
		if end := saturatingAdd(t.StartOffsetNs, t.DurationNs); end > maxEnd {
			maxEnd = end
		}
	}
	if maxEnd == 0 {
		return nil
	}
	waves64 := ((maxEnd - 1) / waveDurationNS) + 1
	if waves64 > DefaultMaxScheduledWaves {
		waves64 = DefaultMaxScheduledWaves
	}
	waves := int(waves64)
	out := make([]WaveOffer, 0, waves)
	for wave := 0; wave < waves; wave++ {
		start := uint64(wave) * waveDurationNS
		end := saturatingAdd(start, waveDurationNS)
		var offered float64
		for _, t := range tasks {
			if t.TargetRPS == 0 {
				// Uncapped max-rate sentinel (correctness-pass-1): not throughput-graded,
				// excluded from the offered-rate gate rather than treated as offered=0.
				continue
			}
			taskStart := t.StartOffsetNs
			taskEnd := saturatingAdd(t.StartOffsetNs, t.DurationNs)
			overlap := overlapNS(start, end, taskStart, taskEnd)
			if overlap == 0 {
				continue
			}
			offered += float64(t.TargetRPS) * (float64(overlap) / float64(waveDurationNS))
		}
		if offered <= 0 {
			continue
		}
		out = append(out, WaveOffer{WaveIndex: wave, OfferedRPS: uint64(math.Round(offered))})
	}
	return out
}

// saturatingAdd performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func saturatingAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

// overlapNS performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func overlapNS(aStart, aEnd, bStart, bEnd uint64) uint64 {
	start := max(aStart, bStart)
	end := min(aEnd, bEnd)
	if end <= start {
		return 0
	}
	return end - start
}

// MetricSummary groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type MetricSummary struct {
	Count        int
	MaxP99NS     uint64 // worst single second (diagnostic; retained for visibility)
	StableP99NS  uint64 // median of per-second p99 — the gate metric (see below)
	MaxErrorRate float64
	p99Samples   []uint64
}

// summarizeMetrics performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func summarizeMetrics(rows []MetricRow) map[int]MetricSummary {
	out := make(map[int]MetricSummary)
	for _, row := range rows {
		m := out[row.WaveIndex]
		m.Count++
		m.MaxP99NS = max(m.MaxP99NS, row.P99NS)
		m.p99Samples = append(m.p99Samples, row.P99NS)
		if row.ErrorRate > m.MaxErrorRate {
			m.MaxErrorRate = row.ErrorRate
		}
		out[row.WaveIndex] = m
	}
	for idx, m := range out {
		m.StableP99NS = medianU64(m.p99Samples)
		out[idx] = m
	}
	return out
}

// medianU64 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func medianU64(samples []uint64) uint64 {
	if len(samples) == 0 {
		return 0
	}
	s := append([]uint64(nil), samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

// spikeRecoveryNS performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func spikeRecoveryNS(sessions []Session, waveDurationNS uint64) uint64 {
	if waveDurationNS == 0 {
		waveDurationNS = DefaultWaveDurationNS
	}
	var spike *Session
	for i := range sessions {
		if sessions[i].Scenario == "spike" {
			spike = &sessions[i]
			break
		}
	}
	if spike == nil {
		return 0
	}
	sum := summarizeMetrics(spike.Metrics)
	if len(sum) == 0 {
		return 0
	}
	waves := make([]int, 0, len(sum))
	for w := range sum {
		waves = append(waves, w)
	}
	sort.Ints(waves)

	baseline := sum[waves[0]].MaxP99NS
	if baseline == 0 {
		return 0
	}
	threshold := uint64(float64(baseline) * 1.10)

	peakWave, peakP99 := waves[0], sum[waves[0]].MaxP99NS
	for _, w := range waves {
		if sum[w].MaxP99NS > peakP99 {
			peakP99, peakWave = sum[w].MaxP99NS, w
		}
	}
	if peakP99 <= threshold {
		return 0
	}
	for _, w := range waves {
		if w > peakWave && sum[w].MaxP99NS <= threshold {
			return uint64(w-peakWave) * waveDurationNS
		}
	}
	last := waves[len(waves)-1]
	return uint64(last-peakWave+1) * waveDurationNS
}

// SortResults performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func SortResults(results []Result) {
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.Disqualified != b.Disqualified {
			return b.Disqualified
		}
		if a.PeakSustainedTPS != b.PeakSustainedTPS {
			return a.PeakSustainedTPS > b.PeakSustainedTPS
		}
		if a.P99AtPeakNS != b.P99AtPeakNS {
			return a.P99AtPeakNS < b.P99AtPeakNS
		}
		if a.SpikeRecoveryNS != b.SpikeRecoveryNS {
			return a.SpikeRecoveryNS < b.SpikeRecoveryNS
		}
		if a.TotalCorrectness != b.TotalCorrectness {
			return a.TotalCorrectness > b.TotalCorrectness
		}
		return a.RunGroupID < b.RunGroupID
	})
}

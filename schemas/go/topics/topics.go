// Package topics defines shared schema contracts for topics.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package topics

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	TopicSubmissionBuildRequested = "submission.build.requested"
	TopicSubmissionStatusUpdated  = "submission.status.updated"
	TopicBenchmarkRequested       = "benchmark.requested"
	TopicBenchmarkStatusUpdated   = "benchmark.status.updated"
	TopicWorkloadAssignments      = "workload.assignments"
	TopicBarrier                  = "barrier"
	TopicBotReady                 = "bot.ready"
	TopicWorkloadFailed           = "workload.failed"
	TopicOrdersSent               = "orders.sent"
	TopicOrdersAcked              = "orders.acked"
	TopicScoresCorrectness        = "scores.correctness"
	TopicLeaderboardUpdates       = "leaderboard.updates"
	TelemetryPriceScale           = uint64(1_000_000_000)

	// PortFIX is the platform-mandated port for FIX. Contestants do not choose
	// this; the eBPF capture filter hardcodes it. See
	// docs/tps-improvement-plan.md §7.3 port policy.
	PortFIX = uint16(9898)
	// PortHTTPWS is the platform-mandated port shared by REST and WS. See
	// docs/tps-improvement-plan.md §7.3 port policy.
	PortHTTPWS = uint16(8080)
)

// ProtocolAll is the declaration alias for all three transports. Kept for
// back-compat; new combos are declared explicitly ("FIX,REST"). It only
// stands alone — "FIX,ALL" is rejected.
const ProtocolAll = "ALL"

// ParseProtocols normalizes a submission's protocol declaration into its
// ordered element list: one protocol, ProtocolAll (= FIX,REST,WS), or a
// comma-separated combination. Order is MEANINGFUL and preserved: the first
// element is the submission's primary protocol — the one the single-connection
// pass-1 correctness run uses (decision 2026-08-02). Elements are
// case-insensitive, trimmed, must be unique, and each must be FIX, REST or WS.
func ParseProtocols(decl string) ([]string, error) {
	if strings.TrimSpace(strings.ToUpper(decl)) == ProtocolAll {
		return []string{"FIX", "REST", "WS"}, nil
	}
	parts := strings.Split(decl, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.ToUpper(p))
		switch p {
		case "FIX", "REST", "WS":
		default:
			return nil, fmt.Errorf("invalid protocol element %q in %q", p, decl)
		}
		if _, dup := seen[p]; dup {
			return nil, fmt.Errorf("duplicate protocol %q in %q", p, decl)
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty protocol declaration")
	}
	return out, nil
}

// PortForProtocol returns the platform-mandated port for a protocol string
// ("FIX" | "REST" | "WS").
func PortForProtocol(protocol string) uint16 {
	if protocol == "FIX" {
		return PortFIX
	}
	return PortHTTPWS
}

// OrderBandUnset is the sentinel for WorkloadSpec.OrderBand meaning
// "unassigned — fall back to hash-derived banding via SessionBandPartition".
// Mirrors Rust's ORDER_BAND_UNSET (u32::MAX): valid bands are always small
// (< num_partitions), leaving math.MaxUint32 permanently free as a sentinel
// in both languages.
const OrderBandUnset = uint32(math.MaxUint32)

// SMPIDNone is the sentinel for OrderSentEvent.SMPID meaning "this order carried NO
// self-match prevention id on the wire" — no FIX tag 7928, no smp_id JSON key. Such an
// order is UNCONSTRAINED: an engine may match it against anything, including another
// order with no id.
//
// Mirrors Rust's SMP_ID_NONE (u32::MAX). Critically NOT 0 — 0 is a valid SMP id, and
// conflating "absent" with "id 0" would make every id-less pass-2 order look like one
// participant, which under skip-and-continue would stop an engine matching anything.
const SMPIDNone = uint32(math.MaxUint32)

func fnv1a64(s string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= 0x100000001b3
	}
	return hash
}

// BandPartition maps (band, orderID) onto a Kafka partition using an
// EXCLUSIVELY leased band (see bot-fleet-controller's band lease allocator),
// rather than a hash-derived one. base = band*bandWidth; orderID is hashed
// only within [base, base+bandWidth). Mirrors Rust's band_partition exactly
// (same FNV-1a 64-bit hash) so Go readers (correctness-validator) and Rust
// producers (bot-fleet, ebpf-latency) agree on partition placement.
func BandPartition(band uint32, orderID string, numPartitions, bandWidth int32) int32 {
	if numPartitions <= 1 {
		return 0
	}
	if bandWidth < 1 {
		bandWidth = 1
	}
	if bandWidth > numPartitions {
		bandWidth = numPartitions
	}
	base := int32(int64(band) * int64(bandWidth))
	within := int32(fnv1a64(orderID) % uint64(bandWidth))
	return (base + within) % numPartitions
}

// SessionBandPartition mirrors Rust's session_band_partition: hash-derived
// band selection (band = hash(sessionID) % numBands). DEPRECATED fallback —
// exclusive per-session bands are now controller-leased (see BandPartition);
// this remains only so band-unaware messages (WorkloadSpec.OrderBand ==
// OrderBandUnset) keep working during rollout / back-compat.
func SessionBandPartition(sessionID, orderID string, numPartitions, bandWidth int32) int32 {
	if numPartitions <= 1 {
		return 0
	}
	if bandWidth < 1 {
		bandWidth = 1
	}
	if bandWidth > numPartitions {
		bandWidth = numPartitions
	}
	numBands := (numPartitions + bandWidth - 1) / bandWidth
	if numBands < 1 {
		numBands = 1
	}
	band := int32(fnv1a64(sessionID) % uint64(numBands))
	base := band * bandWidth
	within := int32(fnv1a64(orderID) % uint64(bandWidth))
	return (base + within) % numPartitions
}

// SubmissionBuildRequested groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SubmissionBuildRequested struct {
	SubmissionID string    `json:"submission_id"`
	ContestantID string    `json:"contestant_id"` // reserved; empty until OAuth is added
	ArtifactPath string    `json:"artifact_path"` // MinIO object path: submissions/{id}/artifact.zip
	Language     string    `json:"language"`      // cpp | rust | go
	Protocol     string    `json:"protocol"`      // FIX | REST | WS
	Port         int       `json:"port"`          // port the algorithm listens on
	BuildType    string    `json:"build_type"`    // cmake | cargo | go
	BuildTarget  string    `json:"build_target"`  // binary name declared in benchmark.yaml
	TeamName     string    `json:"team_name"`
	SHA256       string    `json:"sha256"`
	RequestedAt  time.Time `json:"requested_at"`
}

const (
	StatusUploaded  = "uploaded"
	StatusBuilding  = "building"
	StatusScanned   = "scanned"
	StatusSBOMReady = "sbom_ready"
	StatusReady     = "ready"
	StatusFailed    = "failed"
)

const (
	RunStatusRequested    = "requested"     // submission-api accepted the click
	RunStatusQueued       = "queued"        // controller decoded benchmark.requested, awaiting a dispatch slot
	RunStatusDeploying    = "deploying"     // controller is allocating a sandbox slot
	RunStatusWaitingReady = "waiting_ready" // workload published, fanning in bot.ready
	RunStatusBarrierFired = "barrier_fired" // barrier event published
	RunStatusRunning      = "running"       // bots actively firing
	RunStatusCompleted    = "completed"     // controller: terminal success
	RunStatusFailed       = "failed"        // controller: terminal failure
)

// BenchmarkRequested groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkRequested struct {
	SessionID    string    `json:"session_id"`    // UUID v7, minted by submission-api
	SubmissionID string    `json:"submission_id"` // referenced submission (must be 'ready')
	ContestantID string    `json:"contestant_id"` // reserved; empty until OAuth
	RunGroupID   string    `json:"run_group_id"`  // parent group; shared across the run-group's sessions
	ScenarioID   string    `json:"scenario_id"`   // which row in scenarios table this session runs
	RequestedAt  time.Time `json:"requested_at"`
}

// BenchmarkStatusUpdated groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkStatusUpdated struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"` // included for consumers that index by submission
	RunGroupID   string    `json:"run_group_id"`  // parent group; allows the frontend / SSE to correlate sibling sessions
	Status       string    `json:"status"`        // one of RunStatus* constants above
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// CorrectnessScoreEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type CorrectnessScoreEvent struct {
	SessionID        string  `json:"session_id"`
	ContestantID     string  `json:"contestant_id"`
	ValidFills       uint64  `json:"valid_fills"`
	TotalFills       uint64  `json:"total_fills"`
	CorrectnessScore float64 `json:"correctness_score"` // valid_fills / total_fills
	ViolationCount   uint64  `json:"violation_count"`
	ComputedAtNS     uint64  `json:"computed_at_ns"`
	SentCount        uint64  `json:"sent_count"`    // orders.sent events drained for the session
	AckedCount       uint64  `json:"acked_count"`   // orders.acked events drained (post-dedup)
	MatchedCount     uint64  `json:"matched_count"` // distinct orders present in both streams
	// P-G jitter (docs/multi-contestant-audit.md §5): cross-flow processing-order
	// inversion magnitude distribution, in microseconds. Zero-valued (all-zero) in
	// full-replay mode and for sessions with no recorded inversions.
	JitterP50US   float64 `json:"jitter_p50_us"`
	JitterP99US   float64 `json:"jitter_p99_us"`
	JitterP999US  float64 `json:"jitter_p999_us"`
	JitterMaxUS   float64 `json:"jitter_max_us"`
	JitterInvRate float64 `json:"jitter_inversion_rate"` // inversions / total processed orders
	// T7-stream health (pass-2 invariants mode): orders that escaped
	// processing-order grading, and whether the result is flagged unreliable.
	T7ReorderLate uint64 `json:"t7_reorder_late"`
	T7Anomalies   uint64 `json:"t7_anomalies"`
	ResultTainted bool   `json:"result_tainted"`
	TaintReason   string `json:"taint_reason,omitempty"`
}

// LeaderboardUpdateEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type LeaderboardUpdateEvent struct {
	RunGroupID           string  `json:"run_group_id"`
	SubmissionID         string  `json:"submission_id"`
	ContestantID         string  `json:"contestant_id"`
	TeamName             string  `json:"team_name"`
	Rank                 int64   `json:"rank"`
	RankDelta            int64   `json:"rank_delta"`
	PeakSustainedTPS     uint64  `json:"peak_sustained_tps"`
	P99NSAtPeakTPS       uint64  `json:"p99_ns_at_peak_tps"`
	SpikeRecoveryNS      uint64  `json:"spike_recovery_ns"`
	TotalCorrectness     float64 `json:"total_correctness"`
	Disqualified         bool    `json:"disqualified"`
	DisqualificationCode string  `json:"disqualification_code,omitempty"`
	UpdatedAtNS          uint64  `json:"updated_at_ns"`
}

// SubmissionStatusUpdated groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SubmissionStatusUpdated struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"` // uploaded | building | scanned | sbom_ready | ready | failed
	Message      string    `json:"message"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// WorkloadSpec groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type WorkloadSpec struct {
	SessionID        string       `json:"session_id"`
	SubmissionID     string       `json:"submission_id"`
	ContestantID     string       `json:"contestant_id"`
	TargetHost       string       `json:"target_host"` // IP of contestant pod
	TargetPort       uint16       `json:"target_port"`
	Protocol         string       `json:"protocol"` // FIX | REST | WS
	Targets          []TargetSpec `json:"targets,omitempty"`
	WorkerIndex      uint32       `json:"worker_index"`
	WorkerCount      uint32       `json:"worker_count"` // Total worker pods
	GlobalSeed       uint64       `json:"global_seed"`
	FIXVersion       string       `json:"fix_version"`
	ConnectTimeoutMS uint64       `json:"connect_timeout_ms"`
	WriteTimeoutMS   uint64       `json:"write_timeout_ms"`
	// PublishedAtUnixNS is the unix-nanosecond timestamp stamped by the
	// controller at publish time, used by bot-fleet workers to detect and
	// skip stale specs left behind by a session that released its
	// partition lease before a worker attached. Zero value (unset) means
	// consumers must treat it as NOT stale.
	PublishedAtUnixNS uint64 `json:"published_at_unix_ns"`
	// OrderBand is the exclusively-leased partition band index for this
	// session's orders.sent/orders.acked traffic (band N covers partitions
	// [N*bandWidth, (N+1)*bandWidth)). OrderBandUnset means unassigned —
	// fall back to hash-derived SessionBandPartition for back-compat with
	// band-unaware producers. Zero value would collide with a real band 0,
	// so callers decoding older payloads MUST default-fill this to
	// OrderBandUnset themselves (Go's zero value for uint32 is 0, not the
	// sentinel — unlike Rust's serde default). See controller runner.go
	// buildWorkloadSpecs, which is the only production constructor.
	OrderBand uint32     `json:"order_band"`
	Tasks     []TaskSpec `json:"tasks"` // this worker's slice of the scenario's task list
}

// TargetSpec names one protocol+port a workload can dispatch tasks to. Ports
// are platform-mandated (see PortForProtocol), not contestant-chosen.
type TargetSpec struct {
	Protocol string `json:"protocol"` // FIX | REST | WS
	Port     uint16 `json:"port"`
}

// ResolvedTargets returns Targets if populated, otherwise a single-entry
// slice built from the legacy Protocol/TargetPort fields, so callers never
// have to special-case old messages.
func (w WorkloadSpec) ResolvedTargets() []TargetSpec {
	if len(w.Targets) > 0 {
		return w.Targets
	}
	return []TargetSpec{{Protocol: w.Protocol, Port: w.TargetPort}}
}

// TaskSpec groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type TaskSpec struct {
	TaskID        uint32 `json:"task_id"`
	Profile       string `json:"profile"`         // hft | retail | institutional
	TargetRPS     uint32 `json:"target_rps"`      // orders per second, constant for this task's lifetime
	StartOffsetNs uint64 `json:"start_offset_ns"` // relative to barrier epoch
	DurationNs    uint64 `json:"duration_ns"`     // how long this task fires
	MarketPct     uint8  `json:"market_pct"`
	CancelPct     uint8  `json:"cancel_pct"`
	ReplacePct    uint8  `json:"replace_pct"`
	// TargetIdx indexes the owning WorkloadSpec's Targets (or its single
	// resolved legacy target when Targets is empty). Defaults to 0.
	TargetIdx uint8 `json:"target_idx"`
	// SMPIDCount is how many distinct self-match-prevention ids this task rotates
	// through (smp = seq % SMPIDCount). The correctness scenario sets 8 so a
	// single-connection run still produces cross-participant matching; scale
	// scenarios leave it 0.
	//
	// 0 (and 1) mean "no SMP id": the bot omits the field from the wire entirely —
	// no FIX tag 7928, no smp_id JSON key — so pass-2 frames stay byte-identical to
	// pre-SMP output, and a contestant can distinguish "no id" from "id 0". An order
	// without an SMP id is unconstrained and matches normally.
	SMPIDCount uint32 `json:"smp_id_count,omitempty"`
}

// Scenario groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Scenario struct {
	ScenarioID string     `json:"scenario_id"`
	Name       string     `json:"name"`        // constant | spike | ramp
	DurationNs uint64     `json:"duration_ns"` // wall-time of the session
	TaskSpecs  []TaskSpec `json:"task_specs"`  // full task list — sharded across worker pods by the controller
}

// BarrierEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BarrierEvent struct {
	SessionID            string `json:"session_id"`
	TargetEpochUnixNanos uint64 `json:"target_epoch_unix_nanos"`
}

// ReadySignal groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ReadySignal struct {
	SessionID        string `json:"session_id"`
	SubmissionID     string `json:"submission_id"`
	WorkerID         string `json:"worker_id"`
	WorkerIndex      uint32 `json:"worker_index"`
	WorkerCount      uint32 `json:"worker_count"`
	TaskCount        uint32 `json:"task_count"`
	ConnectedCount   uint32 `json:"connected_count"`
	ReadyAtUnixNanos uint64 `json:"ready_at_unix_nanos"`
}

// OrderSentBatch groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type OrderSentBatch struct {
	SessionID string           `json:"session_id" msgpack:"session_id"`
	WorkerID  string           `json:"worker_id" msgpack:"worker_id"`
	Events    []OrderSentEvent `json:"events" msgpack:"events"`
}

// OrderSentEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type OrderSentEvent struct {
	SessionID      string `json:"session_id" msgpack:"session_id"`
	SubmissionID   string `json:"submission_id" msgpack:"submission_id"`
	WorkerID       string `json:"worker_id" msgpack:"worker_id"`
	TaskID         uint32 `json:"task_id" msgpack:"task_id"`
	OrderID        string `json:"order_id" msgpack:"order_id"`
	TargetSendTSNS uint64 `json:"target_send_ts_ns" msgpack:"target_send_ts_ns"`
	SendTSNS       uint64 `json:"send_ts_ns" msgpack:"send_ts_ns"`
	RecvDoneTSNS   uint64 `json:"recv_done_ts_ns" msgpack:"recv_done_ts_ns"`
	TimedOut       bool   `json:"timed_out" msgpack:"timed_out"`
	Price          uint64 `json:"price" msgpack:"price"`
	Qty            uint64 `json:"qty" msgpack:"qty"`
	Side           string `json:"side" msgpack:"side"`                 // BUY | SELL
	PayloadType    string `json:"payload_type" msgpack:"payload_type"` // NEW | CANCEL | REPLACE — lets the validator/ingester separate cancels from new orders (cancel throughput).
	OrdType        string `json:"ord_type" msgpack:"ord_type"`         // LIMIT | MARKET (FIX tag 40) — distinguishes market from limit new orders, which share payload_type=NEW.
	OrigOrderID    string `json:"orig_order_id" msgpack:"orig_order_id"`
	BarrierEpochNs uint64 `json:"barrier_epoch_ns" msgpack:"barrier_epoch_ns"`
	// SMPID is the self-match-prevention id this order was sent under, as assigned
	// by the bot. SMPIDNone means the order carried no SMP id on the wire and is
	// unconstrained. Read from HERE rather than from the eBPF capture: acked events
	// are joined to sent events by order id, and the bot is the authority on what it
	// assigned, so the capture never needs to parse FIX tag 7928.
	// SMPID is the self-match-prevention id, or SMPIDNone when the order carried no
	// id on the wire.
	//
	// CAUTION: the Go zero value of this field is 0, which is a VALID id — so a
	// zero-valued OrderSentEvent built in code (rather than decoded from the wire)
	// reads as "participant 0" unless it explicitly sets SMPIDNone. Decoded events
	// always carry an explicit value, so this only bites synthetic construction; see
	// pipeline.AssembleOrder, which normalises it.
	SMPID uint32 `json:"smp_id" msgpack:"smp_id"`
}

// OrderSentEventFields is the per-order wire payload of the POSITIONAL orders.sent
// batch envelope (OrderSentBatchV2). It is the Go mirror of Rust's
// `OrderSentEventFields` in schemas/rust/src/lib.rs, and FIELD ORDER IS THE WIRE
// CONTRACT — the encoding is a msgpack array, not a map, so a reordered field here
// silently decodes into the wrong destination rather than failing.
//
// session_id / submission_id / worker_id are absent by design: they are identical for
// every event in a batch and are hoisted into the envelope.
type OrderSentEventFields struct {
	_msgpack       struct{} `msgpack:",as_array"`
	TaskID         uint32
	OrderID        string
	TargetSendTSNS uint64
	SendTSNS       uint64
	RecvDoneTSNS   uint64
	TimedOut       bool
	Price          uint64
	Qty            uint64
	Side           string // BUY | SELL
	PayloadType    string // NEW | CANCEL | REPLACE
	OrdType        string // LIMIT | MARKET
	OrigOrderID    string
	BarrierEpochNs uint64
	// APPENDED LAST on purpose: this struct is POSITIONAL msgpack (see the
	// msgpack:",as_array" tag), so field order IS the wire contract and a new field
	// may only go at the end. Must mirror Rust's OrderSentEventFields exactly.
	SMPID uint32
}

// OrderSentBatchV2 is the positional-msgpack envelope the bot-fleet producer actually
// writes to orders.sent (Rust `OrderSentBatchV2Ref`). Mirror of Rust's
// `OrderSentBatchV2`; field order IS the wire contract.
//
// WHY THIS EXISTS: the producer moved orders.sent to this hoisted, positional format,
// and telemetry-ingester was updated with it — but the correctness-validator, the OTHER
// orders.sent consumer, kept decoding the old named-map OrderSentBatch. Positional
// bytes decoded as a named map yield a garbage SessionID, so the validator's
// session-id filter rejected EVERY batch: it reported sent=0 while the topic held
// ~600k records, which in turn made matched=0 and left every correctness score
// meaningless. Any future change to the wire format must update both consumers;
// TestOrderSentBatchV2WireContract pins the layout so drift fails a test instead of a
// benchmark.
type OrderSentBatchV2 struct {
	_msgpack     struct{} `msgpack:",as_array"`
	SessionID    string
	SubmissionID string
	WorkerID     string
	Events       []OrderSentEventFields
}

// IntoEvents reconstitutes full OrderSentEvents from the hoisted envelope, mirroring
// Rust's OrderSentBatchV2::into_events, so consumers that operate on the per-event
// struct need not know about the wire layout.
func (b OrderSentBatchV2) IntoEvents() []OrderSentEvent {
	out := make([]OrderSentEvent, 0, len(b.Events))
	for _, f := range b.Events {
		out = append(out, OrderSentEvent{
			SessionID:      b.SessionID,
			SubmissionID:   b.SubmissionID,
			WorkerID:       b.WorkerID,
			TaskID:         f.TaskID,
			OrderID:        f.OrderID,
			TargetSendTSNS: f.TargetSendTSNS,
			SendTSNS:       f.SendTSNS,
			RecvDoneTSNS:   f.RecvDoneTSNS,
			TimedOut:       f.TimedOut,
			Price:          f.Price,
			Qty:            f.Qty,
			Side:           f.Side,
			PayloadType:    f.PayloadType,
			OrdType:        f.OrdType,
			OrigOrderID:    f.OrigOrderID,
			BarrierEpochNs: f.BarrierEpochNs,
			SMPID:          f.SMPID,
		})
	}
	return out
}

// OrderAckedBatch groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type OrderAckedBatch struct {
	SessionID    string            `json:"session_id" msgpack:"session_id"`
	ContestantID string            `json:"contestant_id" msgpack:"contestant_id"`
	Events       []OrderAckedEvent `json:"events" msgpack:"events"`
}

// OrderAckedEvent groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type OrderAckedEvent struct {
	SessionID           string `json:"session_id" msgpack:"session_id"`
	ContestantID        string `json:"contestant_id" msgpack:"contestant_id"`
	OrderID             string `json:"order_id" msgpack:"order_id"`
	SrcIP               uint32 `json:"src_ip" msgpack:"src_ip"`
	SrcPort             uint16 `json:"src_port" msgpack:"src_port"`
	TCPSeq              uint32 `json:"tcp_seq" msgpack:"tcp_seq"`
	T3XDPIngressNS      uint64 `json:"t3_xdp_ingress_ns" msgpack:"t3_xdp_ingress_ns"`
	T7XDPEgressNS       uint64 `json:"t7_xdp_egress_ns" msgpack:"t7_xdp_egress_ns"`
	PodServiceTimeNS    uint64 `json:"pod_service_time_ns" msgpack:"pod_service_time_ns"`
	ExecType            string `json:"exec_type" msgpack:"exec_type"`
	FillQty             uint64 `json:"fill_qty" msgpack:"fill_qty"`
	FillPrice           uint64 `json:"fill_price" msgpack:"fill_price"` // fixed-point, scaled by TelemetryPriceScale
	OrigOrderID         string `json:"orig_order_id" msgpack:"orig_order_id"`
	ReorderingDetected  bool   `json:"reordering_detected" msgpack:"reordering_detected"`
	RetransmissionCount uint32 `json:"retransmission_count" msgpack:"retransmission_count"`
}

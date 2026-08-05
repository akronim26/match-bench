// Package controller implements runner behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
)

const DefaultMaxTasksPerWorker = 1000

// RunConfig groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RunConfig struct {
	GlobalSeed       uint64
	FIXVersion       string
	ConnectTimeoutMS uint64
	WriteTimeoutMS   uint64

	DeployDeadline   time.Duration
	ReadyDeadline    time.Duration
	BarrierSafetyGap time.Duration

	MaxTasksPerWorker int
	// CapacityWaitTimeout bounds the blocking pre-scale gate: how long a session
	// waits for the bot-fleet consumer group to have one member per shard before
	// giving up. Zero disables the gate (publish immediately, the pre-gate
	// behaviour). Must exceed the fleet's realistic scale-up time — pod schedule +
	// image pull + Kafka group join — or sessions fail while KEDA is still working.
	CapacityWaitTimeout time.Duration
	// CapacityPollInterval is how often the gate re-reads group membership.
	CapacityPollInterval time.Duration
	// WorkerRPSCapacity is one bot-fleet worker's sustainable send rate in orders
	// per second, used as the throughput ceiling when sizing a session's shard
	// count. Environment-specific and therefore configuration: measured
	// single-worker ceilings range from ~50k/s (local loopback) to 600-790k/s
	// (drain, telemetry off). Zero disables the ceiling, leaving shard count
	// determined by task count alone (the pre-rate-awareness behaviour).
	WorkerRPSCapacity uint64

	// LeaseAcquireTimeout bounds how long a session blocks waiting for free
	// workload.assignments partitions before admission fails outright. This
	// is the cross-session capacity gate: validateWorkerCapacity used to
	// check one session's worker count against the partition count with no
	// notion of partitions other sessions already held.
	LeaseAcquireTimeout time.Duration
}

// Runner groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Runner struct {
	sessions   *SessionManager
	store      *store.Store
	orch       *orchestrator.Client
	producer   *Producer
	leases     *PartitionLeaseAllocator
	bandLeases *PartitionLeaseAllocator
	runConfig  RunConfig
	log        *slog.Logger
	// capacityProbe reports bot-fleet consumer-group capacity for the blocking
	// pre-scale gate. Nil disables the gate — which is what every existing test
	// constructing a Runner directly gets, so none of them need a live broker.
	capacityProbe CapacityProbe
}

// SetCapacityProbe installs the pre-scale gate's capacity probe. Set at wiring time in
// main rather than passed to NewRunner, which already takes eight arguments and is
// constructed in several tests that must not need a broker.
func (r *Runner) SetCapacityProbe(p CapacityProbe) {
	r.capacityProbe = p
}

// NewRunner performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewRunner(sessions *SessionManager, st *store.Store, orch *orchestrator.Client, producer *Producer, leases *PartitionLeaseAllocator, bandLeases *PartitionLeaseAllocator, runConfig RunConfig, log *slog.Logger) *Runner {
	return &Runner{
		sessions:   sessions,
		store:      st,
		orch:       orch,
		producer:   producer,
		leases:     leases,
		bandLeases: bandLeases,
		runConfig:  runConfig,
		log:        log,
	}
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) Run(parent context.Context, req topics.BenchmarkRequested) {
	log := r.log.With(
		"session_id", req.SessionID,
		"submission_id", req.SubmissionID,
		"run_group_id", req.RunGroupID,
		"scenario_id", req.ScenarioID,
	)

	// Panic backstop: registered before any resource acquisition so it runs
	// LAST during unwind — after runSession's defers have already released the
	// slot, both leases, and the session entry. Without this, a recovered
	// panic (consumer.dispatch swallows it to protect other sessions) leaves
	// the run permanently non-terminal: the offset was committed at accept, so
	// nothing ever redelivers it. publishFailure uses a detached context
	// internally, so it works even when parent is already cancelled.
	defer func() {
		if p := recover(); p != nil {
			metrics.Counter("controller_session_panics_total", "Session goroutines recovered from a panic.", nil, 1)
			log.Error("session panicked", "panic", p)
			r.publishFailure(parent, req, fmt.Sprintf("session panicked: %v", p), log)
		}
	}()

	// Fail closed: the offset was committed at dispatch acceptance, so there
	// is no redelivery to fall back on — proceeding on a store error risks
	// double-running a session whose prior completion we simply couldn't see
	// (transient Postgres blip during a rebalance redelivery window). Failing
	// the run is user-retryable; a duplicate run is not recoverable.
	if status, serr := r.store.RunStatus(parent, req.SessionID); serr != nil {
		r.publishFailure(parent, req, "run-status precheck failed: "+serr.Error(), log)
		return
	} else if status == topics.RunStatusCompleted || status == topics.RunStatusFailed {
		log.Info("benchmark.requested for an already-terminal run; skipping redelivery", "status", status)
		return
	}

	scenario, err := r.store.LoadScenario(parent, req.ScenarioID)
	if err != nil {
		log.Error("load scenario failed", "error", err)
		r.publishFailure(parent, req, "load scenario: "+err.Error(), log)
		return
	}
	if scenario == nil || len(scenario.TaskSpecs) == 0 {
		r.publishFailure(parent, req, "scenario has zero tasks", log)
		return
	}

	totalRPS := totalTargetRPS(scenario.TaskSpecs)
	workerCount := computeWorkerCountForLoad(
		len(scenario.TaskSpecs), totalRPS,
		r.runConfig.MaxTasksPerWorker, r.runConfig.WorkerRPSCapacity,
	)
	// shard_reason names the binding ceiling so an unexpected worker_count is
	// diagnosable from one log line instead of by re-deriving both ceilings.
	shardReason := "tasks"
	if workerCount > computeWorkerCount(len(scenario.TaskSpecs), r.runConfig.MaxTasksPerWorker) {
		shardReason = "rate"
	}
	log = log.With(
		"scenario_name", scenario.Name,
		"total_tasks", len(scenario.TaskSpecs),
		"total_target_rps", totalRPS,
		"worker_count", workerCount,
		"shard_reason", shardReason,
	)
	metrics.Counter("sessions_started_total", "Benchmark sessions started by scenario.", metrics.Labels("scenario_name", scenario.Name), 1)

	sess := &Session{
		SessionID:     req.SessionID,
		SubmissionID:  req.SubmissionID,
		ContestantID:  req.ContestantID,
		RunGroupID:    req.RunGroupID,
		WorkerCount:   workerCount,
		ReadyReceived: make(map[uint32]topics.ReadySignal),
		readyCh:       make(chan topics.ReadySignal, int(workerCount)*2+1),
		CreatedAt:     time.Now().UTC(),
	}

	ctx, cancel := context.WithCancel(parent)
	sess.cancel = cancel
	defer cancel()

	if _, existed := r.sessions.Add(sess); existed {
		log.Info("session already tracked — ignoring duplicate benchmark.requested")
		return
	}
	defer r.sessions.Drop(sess.SessionID)

	// Both-or-block: a session must hold BOTH its workload.assignments
	// partition leases AND its exclusive order band lease, or neither. They
	// share one leaseCtx/timeout window so a session can't sit half-admitted
	// (holding partitions but no band, or vice versa) while waiting out a
	// second full LEASE_ACQUIRE_TIMEOUT for the other resource.
	leaseCtx, leaseCancel := context.WithTimeout(ctx, r.runConfig.LeaseAcquireTimeout)
	leases, err := r.leases.Acquire(leaseCtx, sess.SessionID, int(workerCount))
	if err != nil {
		leaseCancel()
		r.publishFailure(parent, req, "acquire partition leases: "+err.Error(), log)
		return
	}
	bands, err := r.bandLeases.Acquire(leaseCtx, sess.SessionID, 1)
	leaseCancel()
	if err != nil {
		r.leases.Release(sess.SessionID)
		r.publishFailure(parent, req, "acquire order band lease: "+err.Error(), log)
		return
	}
	orderBand := uint32(bands[0])
	// Release order: band lease is stale-tail-guarded independently by the
	// validator (session-id filter on every read event) and by time-window
	// pruning of old sessions' offsets, so releasing it here — even while a
	// straggler order from this session is still in flight on the wire — is
	// bounded-risk: the next session leased into this band can only ever be
	// misread as this session's traffic within that guard window, never
	// silently forever. Same reasoning already governs partition lease
	// release timing.
	defer r.leases.Release(sess.SessionID)
	defer r.bandLeases.Release(sess.SessionID)

	r.runSession(ctx, sess, scenario, workerCount, leases, orderBand, log)
}

// runSession applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) runSession(
	ctx context.Context,
	sess *Session,
	scenario *topics.Scenario,
	workerCount uint32,
	leases []int,
	orderBand uint32,
	log *slog.Logger,
) {
	sessionStart := time.Now()
	result := "failed"
	defer func() {
		labels := metrics.Labels("scenario_name", scenario.Name, "result", result)
		metrics.Counter("sessions_completed_total", "Benchmark sessions completed by scenario and result.", labels, 1)
		metrics.Histogram("session_duration_seconds", "Benchmark session duration in seconds.", labels, metrics.SinceSeconds(sessionStart))
	}()
	stageStart := time.Now()
	sub, err := r.store.GetSubmission(ctx, sess.SubmissionID)
	recordSessionStage("load_submission", stageStart, err)
	if err != nil {
		r.fail(ctx, sess, "lookup submission: "+err.Error(), log)
		return
	}
	if sub == nil {
		r.fail(ctx, sess, "submission not found", log)
		return
	}
	if sub.ImageRef == "" {
		r.fail(ctx, sess, "submission has no built image ref", log)
		return
	}

	r.transition(ctx, sess, topics.RunStatusDeploying, "allocating sandbox slot", log)
	image := sub.ImageRef
	stageStart = time.Now()
	if _, err := r.orch.CreateSlot(ctx, sess.SessionID, sess.ContestantID, image, submissionPorts(sub), orderBand); err != nil {
		recordSessionStage("create_slot", stageStart, err)
		r.fail(ctx, sess, "create slot: "+err.Error(), log)
		return
	}
	recordSessionStage("create_slot", stageStart, nil)
	sess.SlotID = sess.SessionID
	// Guard the slot from here on: every return path below (fail, panic, or
	// normal completion) releases it exactly once. Manual releaseSlot calls
	// at each early-return site used to be required and were easy to miss —
	// e.g. a panic between here and the end of runSession would leak the
	// sandbox slot with no cleanup. This defer covers all of them.
	defer r.releaseSlot(sess, log)

	stageStart = time.Now()
	slot, err := r.orch.WaitForReady(ctx, sess.SessionID, r.runConfig.DeployDeadline, 500*time.Millisecond)
	recordSessionStage("wait_slot_ready", stageStart, err)
	if err != nil || slot.State != orchestrator.StateReady {
		msg := "slot did not become ready"
		if err != nil {
			msg = msg + ": " + err.Error()
		} else {
			msg = msg + ": " + slot.Message
		}
		r.fail(ctx, sess, msg, log)
		return
	}
	sess.Endpoint = &slot.Endpoint

	specs := r.buildWorkloadSpecs(sess, sub, scenario, workerCount, orderBand)

	// Pre-scale gate — BLOCKING. Wait until the bot-fleet consumer group actually has
	// one member per shard, and has finished rebalancing, before publishing anything.
	//
	// This used to be log-only, which left a real hazard: publishing worker_count specs
	// to a smaller fleet means a pod receives more specs than it can run concurrently
	// and leaves the surplus UNCOMMITTED, so when KEDA's new pod joins, the rebalance
	// re-delivers those specs and a shard runs TWICE. A local 3-shard run reproduced
	// exactly that — worker_index 0 and 1 each prepared twice, and the session
	// delivered 142k orders against an expected 324k because the duplicate runs began
	// after the barrier epoch had passed.
	//
	// The group is read via Kafka DescribeGroups rather than Deployment readyReplicas:
	// group membership is what determines whether a spec can be received at all (a pod
	// that has not joined yet cannot), and it needs no Kubernetes RBAC.
	log.Info("pre-scale gate: leased-partition demand",
		"session_worker_count", workerCount,
		"total_leased_partitions", r.leases.LeasedCount(),
	)
	if r.capacityProbe != nil {
		if err := awaitCapacity(
			ctx, r.capacityProbe, int(workerCount),
			r.runConfig.CapacityWaitTimeout, r.runConfig.CapacityPollInterval, log,
		); err != nil {
			recordSessionStage("await_capacity", stageStart, err)
			r.fail(ctx, sess, "bot-fleet capacity: "+err.Error(), log)
			return
		}
	}

	stageStart = time.Now()
	if err := r.producer.PublishWorkloadSpec(ctx, specs, leases); err != nil {
		recordSessionStage("publish_workload", stageStart, err)
		r.fail(ctx, sess, "publish workload specs: "+err.Error(), log)
		return
	}
	recordSessionStage("publish_workload", stageStart, nil)
	r.transition(ctx, sess, topics.RunStatusWaitingReady, "fanning in ready signals", log)

	stageStart = time.Now()
	if err := r.awaitReady(ctx, sess, log); err != nil {
		recordSessionStage("await_ready", stageStart, err)
		r.fail(ctx, sess, err.Error(), log)
		return
	}
	recordSessionStage("await_ready", stageStart, nil)

	barrierEpochNs := uint64(time.Now().Add(r.runConfig.BarrierSafetyGap).UnixNano())
	stageStart = time.Now()
	if err := r.producer.PublishBarrier(ctx, sess.SessionID, barrierEpochNs); err != nil {
		recordSessionStage("publish_barrier", stageStart, err)
		r.fail(ctx, sess, "publish barrier: "+err.Error(), log)
		return
	}
	recordSessionStage("publish_barrier", stageStart, nil)
	r.transition(ctx, sess, topics.RunStatusBarrierFired, "barrier published", log)
	r.transition(ctx, sess, topics.RunStatusRunning, "bots firing", log)

	totalDuration := time.Duration(scenario.DurationNs) + r.runConfig.BarrierSafetyGap
	select {
	case <-time.After(totalDuration):
	case <-ctx.Done():
		log.Info("context cancelled during run; marking failed and cleaning up")
		failCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r.fail(failCtx, sess, "controller shutdown during run", log)
		return
	}

	r.transition(ctx, sess, topics.RunStatusCompleted, "run completed", log)
	result = "completed"
}

// computeWorkerCount performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func computeWorkerCount(totalTasks, maxTasksPerWorker int) uint32 {
	if totalTasks <= 0 {
		return 1
	}
	if maxTasksPerWorker <= 0 {
		maxTasksPerWorker = DefaultMaxTasksPerWorker
	}
	count := (totalTasks + maxTasksPerWorker - 1) / maxTasksPerWorker
	return uint32(count)
}

// totalTargetRPS sums target_rps across a scenario's task specs — the scenario's
// aggregate order rate, and the input to the throughput ceiling below. Accumulates
// in uint64: 1000 tasks at high per-task rates overflow a uint32 sum.
func totalTargetRPS(specs []topics.TaskSpec) uint64 {
	var total uint64
	for i := range specs {
		total += uint64(specs[i].TargetRPS)
	}
	return total
}

// computeWorkerCountForLoad sizes a session's shard count against BOTH of the
// worker's real limits and takes whichever demands more:
//
//   - tasks / maxTasksPerWorker  — the memory and file-descriptor bound. One task is
//     one TCP connection plus a pending map, so this caps per-pod footprint.
//   - rps / workerRPSCapacity    — the throughput bound. A pod can only pace so many
//     orders per second regardless of how few connections carry them.
//
// Sharding on task count alone (the previous behaviour) silently under-provisions
// whenever load is concentrated: 1000 HFT bots at 1000 rps is 1M orders/s and fits
// the task ceiling exactly, so it sharded to ONE worker and the run delivered a
// fraction of its scenario. Task count is a proxy for the wrong quantity.
//
// workerRPSCapacity == 0 disables the throughput ceiling, reproducing the old
// task-count-only result exactly — so an unset WORKER_RPS_CAPACITY changes nothing.
// The value is environment-specific (measured single-worker ceilings on this branch
// span ~50k/s on local loopback to 600-790k/s draining with telemetry off), which is
// why it is configuration rather than a constant.
func computeWorkerCountForLoad(totalTasks int, totalRPS uint64, maxTasksPerWorker int, workerRPSCapacity uint64) uint32 {
	byTasks := computeWorkerCount(totalTasks, maxTasksPerWorker)
	if workerRPSCapacity == 0 || totalRPS == 0 {
		return byTasks
	}
	byRate := (totalRPS + workerRPSCapacity - 1) / workerRPSCapacity
	if byRate > uint64(byTasks) {
		return uint32(byRate)
	}
	return byTasks
}

// transition applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) transition(ctx context.Context, sess *Session, status, message string, log *slog.Logger) {
	sess.Status = status
	sess.Message = message
	evt := topics.BenchmarkStatusUpdated{
		SessionID:    sess.SessionID,
		SubmissionID: sess.SubmissionID,
		RunGroupID:   sess.RunGroupID,
		Status:       status,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := r.producer.PublishStatus(ctx, evt); err != nil {
		log.Error("publish status failed", "status", status, "error", err)
	} else {
		metrics.Counter("session_transitions_total", "Session transitions published by status.", metrics.Labels("status", status), 1)
		log.Info("session transition", "status", status, "message", message)
	}
}

// fail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) fail(_ context.Context, sess *Session, message string, log *slog.Logger) {
	log.Error("session failed", "message", message)
	pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.transition(pubCtx, sess, topics.RunStatusFailed, message, log)
}

// publishFailure applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) publishFailure(_ context.Context, req topics.BenchmarkRequested, message string, log *slog.Logger) {
	pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	evt := topics.BenchmarkStatusUpdated{
		SessionID:    req.SessionID,
		SubmissionID: req.SubmissionID,
		RunGroupID:   req.RunGroupID,
		Status:       topics.RunStatusFailed,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := r.producer.PublishStatus(pubCtx, evt); err != nil {
		log.Error("publish early failure status", "error", err, "message", message)
	}
}

// releaseSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) releaseSlot(sess *Session, log *slog.Logger) {
	if sess.SlotID == "" {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.orch.DeleteSlot(releaseCtx, sess.SlotID); err != nil {
		log.Error("release slot failed", "slot_id", sess.SlotID, "error", err)
	}
}

// awaitReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) awaitReady(ctx context.Context, sess *Session, log *slog.Logger) error {
	deadline := time.NewTimer(r.runConfig.ReadyDeadline)
	defer deadline.Stop()

	for uint32(len(sess.ReadyReceived)) < sess.WorkerCount {
		select {
		case sig := <-sess.readyCh:
			sess.ReadyReceived[sig.WorkerIndex] = sig
			log.Info("ready signal received",
				"worker_index", sig.WorkerIndex,
				"connected", sig.ConnectedCount,
				"total_received", len(sess.ReadyReceived),
				"expected", sess.WorkerCount,
			)
		case <-deadline.C:
			if len(sess.ReadyReceived) == 0 {
				metrics.Counter("ready_none_total", "Sessions with no ready signals before deadline.", nil, 1)
				return errors.New("no ready signals before deadline")
			}
			// Partial fan-in is a hard failure, not a degraded mode: a
			// contestant benchmarked with fewer bot-fleet workers than
			// leased silently receives a fraction of intended TPS, which
			// is an integrity failure for a competitive benchmark, not
			// something to proceed through quietly.
			metrics.Counter("ready_partial_total", "Sessions with partial ready fan-in.", nil, 1)
			return fmt.Errorf(
				"ready deadline expired with partial fan-in: received %d of %d worker ready signals",
				len(sess.ReadyReceived), sess.WorkerCount,
			)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	metrics.Counter("ready_signals_total", "Ready fan-in completions by result.", metrics.Labels("result", "full"), 1)
	return nil
}

// submissionTargets returns the TargetSpec table for a submission — one
// target per declared protocol, in DECLARATION ORDER (topics.ParseProtocols:
// single protocol, "ALL", or an ordered combo like "REST,WS"). Order matters:
// target 0 is the primary protocol, and the single-task pass-1 correctness
// scenario lands on it via round-robin stamping — so a "REST,FIX" submission
// is correctness-graded over REST. An unparseable declaration (cannot happen
// past submission validation; defensive for hand-inserted rows) falls back to
// treating the string as a single protocol, preserving old behavior.
func submissionTargets(sub *store.SubmissionInfo) []topics.TargetSpec {
	parts, err := topics.ParseProtocols(sub.Protocol)
	if err != nil {
		parts = []string{sub.Protocol}
	}
	targets := make([]topics.TargetSpec, 0, len(parts))
	for _, p := range parts {
		targets = append(targets, topics.TargetSpec{Protocol: p, Port: topics.PortForProtocol(p)})
	}
	return targets
}

// submissionPorts returns every distinct port the sandbox pod must expose for a
// submission, derived from the SAME target table the bots connect through.
//
// Deriving it from submissionTargets rather than from sub.Port is the point: those two
// must agree by construction. When they did not, a ProtocolAll pod exposed only
// sub.Port, the bots dialled all three targets, and every REST and WS task timed out
// against a port the pod had never opened — while FIX worked perfectly, which made it
// look like a load or concurrency problem for far longer than it should have.
//
// REST and WS share PortHTTPWS, so the list is de-duplicated: a repeated ContainerPort
// is rejected by the API server.
func submissionPorts(sub *store.SubmissionInfo) []int {
	targets := submissionTargets(sub)
	ports := make([]int, 0, len(targets))
	seen := make(map[int]struct{}, len(targets))
	for _, t := range targets {
		p := int(t.Port)
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		ports = append(ports, p)
	}
	return ports
}

// buildWorkloadSpecs applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Runner) buildWorkloadSpecs(
	sess *Session,
	sub *store.SubmissionInfo,
	scenario *topics.Scenario,
	workerCount uint32,
	orderBand uint32,
) []topics.WorkloadSpec {
	targets := submissionTargets(sub)

	tasksByWorker := make([][]topics.TaskSpec, workerCount)
	for i := range tasksByWorker {
		tasksByWorker[i] = make([]topics.TaskSpec, 0)
	}
	// Round-robin across declared targets first, so each task's target_idx
	// is stable regardless of worker sharding; keeps the RPS budget split
	// even across protocols and preserves globally-unique task_ids (§7.3
	// rationale 1 — no ClOrdID collisions across protocol sets).
	for i, ts := range scenario.TaskSpecs {
		ts.TargetIdx = uint8(i % len(targets))
		shard := uint32(i) % workerCount
		tasksByWorker[shard] = append(tasksByWorker[shard], ts)
	}

	// Legacy protocol/target_port mirror the first declared target so old
	// consumers (and the Rust Protocol enum, which has no "ALL" variant)
	// always see a valid single-protocol fallback; workers with the Shape A
	// change read Targets/TargetIdx instead and ignore these.
	legacyTarget := targets[0]
	specs := make([]topics.WorkloadSpec, 0, workerCount)
	for i := uint32(0); i < workerCount; i++ {
		specs = append(specs, topics.WorkloadSpec{
			SessionID:         sess.SessionID,
			SubmissionID:      sess.SubmissionID,
			ContestantID:      sub.ContestantID,
			TargetHost:        sess.Endpoint.Host,
			TargetPort:        legacyTarget.Port,
			Protocol:          legacyTarget.Protocol,
			Targets:           targets,
			WorkerIndex:       i,
			WorkerCount:       workerCount,
			GlobalSeed:        r.runConfig.GlobalSeed,
			FIXVersion:        r.runConfig.FIXVersion,
			ConnectTimeoutMS:  r.runConfig.ConnectTimeoutMS,
			WriteTimeoutMS:    r.runConfig.WriteTimeoutMS,
			PublishedAtUnixNS: uint64(time.Now().UnixNano()),
			OrderBand:         orderBand,
			Tasks:             tasksByWorker[i],
		})
	}
	return specs
}

// recordSessionStage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordSessionStage(stage string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("stage", stage, "result", result)
	metrics.Counter("session_stage_total", "Benchmark session stages by result.", labels, 1)
	metrics.Histogram("session_stage_duration_seconds", "Benchmark session stage duration in seconds.", labels, metrics.SinceSeconds(start))
}

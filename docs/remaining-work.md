# Remaining work

Status as of 2026-07-28, branch `feat/bot-tps`. Everything on the adversarial-review
list is closed (32 fixed / 1 mitigated / 1 accepted — see
`docs/branch-review-findings.md`); what remains is verification, a few small features,
the Kafka topology exercise, and a deliberately-parked EKS bucket. Strategy: test
everything possible on a local cluster (k3s/kind); touch EKS only when a task is
physically impossible locally.

---

## A. Code prerequisites (small; unblock the e2e track)

1. ~~**Per-session validator mode selection.**~~ **DONE (72cb924)** — `VALIDATOR_MODE` is a global env var;
   the two-pass design needs the validator to choose full-replay (pass-1
   `correctness` scenario) vs invariants (pass-2 scale scenarios) per session.
   Cleanest source: learn scenario kind from `workload.assignments` the same way the
   validator already learns the session's order band (BandCache pattern), or stamp it
   on the benchmark status event. Prerequisite for B2.
2. **Echo-engine template rendering.** The echo contestant still renders execution
   reports with the pre-P2 `format!` builder and unbatched writes; measured locally
   as roughly half of the pacing knee (300k/s with echo vs 500-700k/s drain). Any
   measurement-capacity claim is echo-capped until this gets the P2 treatment
   (`execution_report_frame`).
3. **Frontend badge for `ResultTainted`** (optional). The taint flag reaches the
   score event; surface it on the leaderboard row so a flagged pass-2 result is
   visibly provisional.
   **Jitter is NOT part of this item — corrected 2026-08-01.** It is already rendered:
   `LeaderboardRow.tsx` shows `jitter_p99_us` (with tests for the present and absent
   cases) and leaderboard-api exposes all four percentiles plus the inversion rate, with
   real values stored (`p50=65.536us p99=2097.152us inversion_rate=0.4973`). Those are
   exact powers of two — 2^16 and 2^21 ns — so they are coarse HDR bucket boundaries, not
   precise figures. What is genuinely missing is jitter on the RUN DETAIL page: nothing
   under `frontend/src/components/run/` references it, so a run shows correctness,
   throughput and latency but not the ordering quality that W is calibrated against.

## B. Local-cluster verification track (k3s/kind; eBPF and KEDA both run locally)

1. ~~**Two-concurrent-sessions e2e**~~ **MOSTLY DONE** — `deploy-local/b1-two-sessions.sh`
   on local k3s. Verified: parallel controller dispatch, both sessions in `running` at
   the same instant, 2 sandbox slots + 2 capture pods live together, exclusive partition
   and order-band leases held then released, per-session latency rows with no
   cross-contestant rows, per-session scoring (session A: sent 216,211 / matched 212,415
   / 239,916 fills graded).
   **Still outstanding:** the live-SSE smoke (Redis → poll loop → `live_metrics` →
   frontend) is NOT covered — the script asserts on Postgres/Timescale rows, never the
   frontend. Two-live-leaderboard-tiles is therefore also unverified.
   Getting here required fixing four platform bugs (see "Bugs found by the local track"
   below), so the run was worth far more than the assertions it now passes.
2. ~~**Two-pass flow e2e.**~~ **DONE (2026-07-31), both halves.**
   - Pass 1 — `deploy-local/b2-two-pass.sh`, 11/11. The correct book scores 0.9833 and
     QUALIFIES; the acker scores 0.0444 and is DISQUALIFIED. This had never passed: the
     acker used to score a clean 1.0. Getting here needed the whole validator and eBPF
     workstream below.
   - Pass 2 — `deploy-local/b2-pass2.sh`, 10/10, on `constant` (204 tasks = 204 flows,
     60s). Book-free grading selected correctly for a paced scenario, order accounting
     exact (scored 216,197 + capture_gaps 10 = sent 216,207), lost-order and capture-gap
     reporting live, jitter histogram fed, taint clean. Duplicate-ack suppression dropped
     3,869 redeliveries.
   **Still outstanding:** the taint path is only proven in its *clean* direction. Forcing
   it to fire (tiny `VALIDATOR_T7_ANOMALY_CAP_MS`, or a low `VALIDATOR_LATE_TAINT_RATE`)
   is untested outside unit tests, so "not tainted" is trustworthy and "tainted" is not
   yet. Jitter-on-the-leaderboard is also still unverified — it reaches the score event,
   but the leaderboard row renders it; the run detail page does not (see A3).
3. ~~**Mixed-protocol run.**~~ **DONE (2026-07-31).** Both forms pass:
   `deploy-local/b3-mixed3.sh` (3 tasks, one per protocol) and
   `deploy-local/b3-mixed-protocol.sh` (single-protocol grading, then 204 tasks as
   `ProtocolAll`). Phase 1 grades the SAME engine over each transport and asserts the
   scores agree: FIX 0.99445 / REST 0.99633 / WS 0.99636, spread 0.0019. Phase 2 ran the
   full 204 tasks with `matched + capture_gaps == sent` (216,204 + 0) and no task dropped
   at connect.
   Getting here needed three platform fixes, none of which were on this list and every one
   of which would have hit real contestants:
   - **WebSocket submissions scored 0.** bot-fleet sent `WsMessage::Binary` (opcode 0x2)
     while any conventionally-written engine handles TEXT (0x1) — including this repo's own
     reference engine. 90,496 orders sent, ZERO answered, while FIX and REST on the same
     engine scored 0.994/0.996. The bot now sends TEXT.
   - **`ProtocolAll` submissions could not connect at all.** sandbox-orchestrator has always
     accepted a `ports` array; bot-fleet-controller only ever sent a single `port`, so the
     pod exposed one port while the bots dialled three targets. Every REST and WS task timed
     out — identically at 3 tasks and at 204, which is what ruled out the "connect storm"
     reading. Ports are now derived from `submissionTargets()` so the two cannot drift.
   - **A deadlock in the reference engine**: frames arriving in the same TCP segment as the
     WS upgrade sat unprocessed until more data arrived, which never came from a client
     waiting on responses.
   **Still outstanding:** phase-2 grading of a multi-protocol submission is dominated by
   cross-flow ordering — see B6, which this run showed is not a pass-2-only problem.
4. ~~**Stalled-peer harness.**~~ **DONE (2026-08-01), 5/5 —**
   `deploy-local/b4-stalled-peer.sh` + `deploy-local/stall-sink` (drains 256KB per
   connection, then stops reading with the socket held open; `STALL_AFTER_BYTES=0`
   wedges immediately). Run against the pass-1 correctness scenario: 14,272 orders
   sent, then 45 consecutive seconds of send_rate=0 with inflight pinned — and the
   session still completed on schedule, zero worker restarts, final inflight 0.
   One finding corrected the claim itself: the platform's defense is the
   **inflight-cap throttle** (sends stop at 64 unacked per task; writes stay
   non-blocking, `max_write_block_ms` ~0 throughout), not a blocking-write drain
   deadline. Two measurement traps recorded in the harness: prometheus counters are
   cumulative across sessions, and the ingester writes NO metrics row for a
   zero-response session (`aggregate.rs` gates on a non-empty service_time
   histogram) — `offered=0` in Timescale is the EXPECTED shape of a full stall.

   **Recovery semantics reviewed and ACCEPTED as-is (2026-08-01).** The stall
   self-resumes at two levels: acks shrinking the pending map wake the cap-wait
   loop via `notify`, and a peer that resumes reading unblocks `write_all` through
   plain TCP flow control. Mid-run, the recovery window is the remainder of the
   run — deliberate (a per-write timeout was tried and collapsed offered load,
   see the comment at `worker.rs` `write_all`), and the right grading semantics:
   stalled seconds cost score automatically. Past `task_end + 5s`
   (`RESPONSE_TIMEOUT_NS`, the drain grace) the write is abandoned, the task
   exits, and the last-tick sweep marks everything pending `timed_out` —
   discarded for good; a peer recovering later gets nothing. One structural
   detail worth knowing when reading stall telemetry: a batch blocked inside
   `write_all` has no expiry entries (those are pushed only after a successful
   write), so the watchdog cannot time it out mid-run — which is why a full
   stall shows inflight pinned at exactly one batch (64) rather than decaying.
   The worker frees for the next assignment afterward: proven empirically by
   consecutive B4/B2 runs on the same pods with zero restarts.
5. ~~**KEDA session-count trigger.**~~ **DONE, and superseded by a better signal** —
   `deploy-local/b5-autoscale-shards.sh`, all assertions green on local k3s. Rather than
   session COUNT, the ScaledObject scales on `iicpc_controller_demanded_workers`
   (Σ worker_count over in-flight sessions), which is correct for one 500k/s session as
   well as for ten small ones; session count is not. Verified end to end: rate-aware
   shard sizing (`shard_reason=rate`, 5400 rps → 3 shards), KEDA scale 1→3, complete
   ready fan-in, every shard executed EXACTLY once, all shards sharing one barrier
   epoch, delivered volume 234,307 vs 234,000 expected. KEDA is installed on the local
   cluster (helm, `keda` namespace). Design + rationale: `docs/rate-aware-sharding-plan.md`.
6. **W calibration.** Reference engine vs a deliberately-naive (thread-per-conn,
   no ingress ordering) engine; compare jitter distributions; set the published
   cross-flow window W where they separate. Fully local.
   **First real measurement is in (B2 pass 2, 2026-07-31), and W is wrong by roughly
   30x.** On 204 flows at 3.6k orders/s the CORRECT reference engine recorded an
   inversion rate of 0.478 and a jitter p99 of ~16.8ms, against
   `CROSS_FLOW_WINDOW_US = 500` (0.5ms). It scored 0.797: ~20% of its orders were
   flagged, essentially all of it cross-flow ordering (lost=0, missed=0, overfills=0).
   204 epoll-served connections do not drain in kernel-arrival order, and W is currently
   set far below the scale of that effect.
   Two caveats on the number. It is a log2-bucket estimate — p99 reported as 16777.216us
   is exactly 2^24 ns, a bucket boundary, so the true value is somewhere in
   [8.4ms, 16.8ms). And every jitter figure recorded BEFORE 2026-07-31 came from a
   capture that silently dropped 25.6% of responses, biased toward uncoalesced
   (low-load) ones — i.e. biased low. Any W calibrated on pre-fix data is invalid.
   This still needs the naive-engine comparison to find where the distributions
   SEPARATE: widening W to ~20-50ms would stop punishing correct engines but also
   forgives real queue-jumps, which is why the audit's option B (replay by T7-derived
   processing order, correcting the replay rather than forgiving the symptom) remains
   the better fix.
   **W is NOT a pass-2-only problem (2026-07-31).** A `ProtocolAll` submission runs three
   concurrent flows, so pass 1 loses its single-connection guarantee too. Measured on the
   3-task mixed run: score 0.8446 with **314,686 of 314,687 violations being
   `time_violations`** — price violations, missed fills, self-trades and lost orders were
   all ZERO, and 314,686/2,024,475 = 15.54% accounts for the score exactly. The rationale
   recorded for deleting `AGGRESSIVE_FILL_TOLERANCE_US` ("pass 1 runs one task on one
   connection, so TCPSeq totally orders the session") holds only for single-protocol
   submissions. Whatever is decided for W has to cover multi-flow replay in BOTH passes.
   **Also invalidating for calibration:** every latency and jitter figure is now measured in
   a deliberately de-optimised network regime (MTU 1500 instead of 9001, GRO/GSO/TSO off)
   that the capture requires — see `docs/grading-network-regime.md`. Pass-2 jitter p99 moved
   4.19ms -> 33.55ms when the capture stopped truncating, most of which is the metric
   becoming honest rather than the system slowing down. W must be calibrated inside that
   regime, and cannot later be reused against numbers taken with offloads on.

## C. Kafka topology exercise (wants a running local cluster to measure against)

1. ~~**Single topic ownership.**~~ **DONE (2026-08-01, 1ec926b).** The recorded
   diagnosis was slightly off: BOTH creators asked for `replication-factor 3` and
   `min.insync.replicas 2`, so they agreed with each other and both were unsatisfiable
   on this one-broker cluster. The app path also DISCARDED the per-topic result vector
   `create_topics()` returns, so it reported success having created nothing. The live
   broker shows `orders.sent` at RF=1 / `min.insync.replicas=1` and no init Job present,
   so something else created the topics — had the app path won that race with
   `min.insync.replicas=2` on RF=1, every produce would have failed with
   `NOT_ENOUGH_REPLICAS`. Both now default to 1 and read
   `KAFKA_TOPIC_REPLICATION_FACTOR` / `KAFKA_MIN_INSYNC_REPLICAS`; `ensure_topics` fails
   loudly on anything except `TopicAlreadyExists`.
   **Still open from the original item:** the app and the Job remain two independent
   creators racing on `--if-not-exists`. They now agree on replication, but retention
   values are still defined in two places.
2. **Per-topic sizing table** from measured inventory (msg rate, msg size, consumer
   parallelism, replay-window need, per local runs): partitions, replication factor,
   retention, compression per topic — replacing the arbitrary 3/24 two-tier split.
3. **Retention by semantics.** Control/barrier topics are consumed in seconds (hours
   of retention, not 24h); `orders.*` retention = the allowed ingester lag/replay
   window (couple with the `auto.offset.reset=latest` footgun fix); scores belong in
   Postgres, not 30-day Kafka.
4. **`benchmark.requested` → 1 partition** if strict global queue FIFO is wanted
   (tiny volume; 3 partitions only approximate FIFO).
5. **Partition count vs bands revisit.** 24 partitions / width-6 / 4 exclusive bands
   was the interim call; revisit alongside the uniform-lease idea (every session
   gets exactly 6 workload partitions → starvation class disappears, session size
   capped) and whether 32 partitions buys anything.
6. **Disk/compression plan** (from the sizing discussion): gp3 throughput is
   provisioned separately from volume size — default 125 MB/s was the likely
   producer-backpressure culprit; target ~500 MB/s per broker for lz4/RF=2 or
   250 for zstd/RF=1 when back on EKS. zstd-1 on `orders.*` is already shipped.

## D. EKS-only (parked; touch only when unavoidable)

- Karpenter node autoscaling (sessions → pods → nodes).
- Graviton (c7g) botworker pool: 1 vCPU = 1 physical core, bot-fleet is pure Rust,
  no eBPF on those nodes; needs multi-arch images + arm64 node group only.
- gp3 throughput provisioning per C6.
- cpuset NodeConfig conflict (`reservedSystemCPUs` vs EKS-managed kubelet args) —
  the static-CPU-manager behavior itself can be rehearsed on k3s first.
- Frame-pointer image builds for on-cluster profiling.
- **Final benchmark numbers — last, after everything above.** Baseline to beat:
  747,911 orders/s single-node drain (pre-P1/P2); local 4-thread drain reached
  2.2–2.6M/s; EKS expectation ~1.1–1.4M/s per c6i.xlarge node.

## Bugs found by the local-cluster track (2026-07-30)

None of these were on the list; all were found by actually running B1/B5 on k3s. Each
was invisible to the existing test suite, and the last four were invisible in a way
worth understanding.

1. **libclang missing from every Rust builder image.** This branch enabled rdkafka's
   `zstd` feature (zstd-1 on `orders.*`), which pulls zstd-sys → bindgen → dlopen
   libclang at build time. `bot-fleet`, `telemetry-ingester`, `ebpf-latency` and
   `contestant-echo` Dockerfiles all lacked it. **This broke the EKS image build too** —
   `infra/Makefile` builds the same Dockerfiles — so `e2e/01-images.sh` could not have
   worked either. A host `cargo check` passes because developer machines have clang.
2. **Three controller lease metrics were silent no-ops.** `libs/go/metrics` exports only
   names present in a pre-declared catalog; `controller_leased_partitions`,
   `controller_leased_order_bands` and `controller_admission_blocked_total` were never
   declared, so every write was dropped and counted as `unregistered_metric`. The lease
   observability the branch built did not exist at runtime. Now declared, plus
   `metrics.RegistryErrorCount()` and a test — verified to fail when the catalog entry
   is removed — so the trap is mechanically caught for any service.
3. **`runs` rows are never created for Kafka-triggered sessions.** The row is INSERTed
   only by submission-api's HTTP `StartBenchmark`; the controller and submission-api
   only ever UPDATE it. Publishing `benchmark.requested` directly (what `e2e/run.sh` and
   the local scripts do) creates nothing, so score-computer rejected every correctness
   event with `lookup run <session>: no rows in result set` and no session was ever
   scored. Worked around in the harness; the platform gap is real and unfixed.
4. **The worker executed one workload at a time.** `worker.rs` awaited `run_workload`
   inline in the consume loop, so two concurrent sessions landing on one pod serialized
   regardless of shard size — B1's load phases ran back-to-back. Now spawned onto a
   `JoinSet` with one workload per partition and a `MAX_CONCURRENT_WORKLOADS` cap.
5. **Shard count ignored order rate.** `computeWorkerCount` divided task count by
   `MAX_TASKS_PER_WORKER` only, so 1000 HFT bots at 1000 rps — 1M orders/s — sharded to
   ONE worker and the run silently delivered a fraction of its scenario. Now the max of
   the task ceiling and a new rate ceiling (`WORKER_RPS_CAPACITY`).
6. **Concurrent workloads competed for the barrier** (a regression introduced by #4).
   The barrier consumer group was keyed per pod, so two concurrent workloads became
   competing members of one group and only one was assigned the barrier partition — the
   other never fired and its shard's load silently vanished (58% delivered). Now keyed
   per concurrency SLOT: distinct per concurrent workload, still free of `session_id`
   (which would leak broker-side groups, the reason it was per-pod originally).
7. **The validator could not decode `orders.sent` at all.** The producer writes the
   POSITIONAL `OrderSentBatchV2` envelope (session/submission/worker hoisted);
   telemetry-ingester was updated for it, but the correctness-validator kept decoding
   the named-map `OrderSentBatch`. Positional bytes read as a named map do not error —
   they yield a garbage `SessionID`, so the session filter dropped EVERY batch: the
   validator reported `sent=0` against a topic holding ~600k records, `matched=0`
   followed, and **every local correctness score was meaningless**. Fixed by adding the
   Go V2 mirror; `sent 0 → 216,211`, `matched 0 → 212,415` on the next run.

Two lessons worth keeping:

- **Go-to-Go round-trip tests cannot verify a cross-language wire contract.** Four
  validator tests encoded the superseded named-map format and passed throughout bug #7;
  one even asserted the producer used `to_vec_named`, true only before V2. They were not
  merely stale, they were *masking* the bug. `schemas/go/topics/wire_contract_test.go`
  now pins the layout against real captured producer bytes.
- **Assertions must fail closed.** Three separate checks passed vacuously on absent
  data: `matched(0) <= sent(0)` while the validator saw nothing, and
  `distinct_barrier_epochs <= 1` when zero epochs were logged. All now require the
  evidence to exist before comparing it.

## Validator: the batch path is deleted (2026-07-31)

**The problem.** The validator carried two full implementations of pass-1 grading: the
batch `validate.Run` (buffer the whole session, replay the book, compare in a second
pass) and the streaming `StreamValidator` (single-pass, finalize each order as it leaves
the book). Only the streaming one ran in production — `main.go` calls
`source.StreamSession` for both modes — and the batch one survived purely as the thing
nine equivalence tests compared against. That is worse than dead code: every new check
had to be written into both paths and kept scoring-identical, and the two disagreed
structurally (batch summed `engine.Fills()` after processing everything, stream
accumulated `DrainFills()` per order, so they diverged whenever one order was both maker
and taker across separate `Apply` calls). Under-reporting could not be made a violation
without that divergence failing every equivalence test on a field neither path was
really wrong about.

**The decision.** Delete the batch path entirely — `validate.Run`, `source.DrainSession`
and its collectors, `pipeline.Run`/`Assemble`/`Counts` — and keep the streaming path as
the single implementation. Tests that exercised real grading behavior were ported onto
`StreamValidator` rather than deleted; only the equivalence scaffolding itself is gone.
`TestUnderReportIsValid` was deleted with it: it asserted that reporting fewer fills than
the reference is legitimate, came from commit `cf9dae8` ("validator and dockerfile
fixes") with no rationale, and is contradicted by the audit. If the reference matched an
order, the contestant owes an execution report for it; silence is a dropped fill.

**What this fixed on the way through.** Removing the second implementation exposed four
defects that the equivalence tests had been hiding, because they compared the two paths
to each other rather than to the specification:

1. **Streaming could not detect a self-match at all.** The reference book applies
   skip-and-continue, so a same-SMP pair produces NO trade — and the streaming path's
   only self-trade check read the engine's *trades*, which meant it could never fire.
   Batch had a second check against the book's RESTING state; streaming did not, so
   every contestant self-match was scored as a generic "reference engine produced no
   fill" price violation, saying nothing about the rule actually broken. The resting
   check now lives in `scoreOrder`, backed by `book.RestingSMPMatch` (O(price level),
   where the batch version copied the whole book per fill).
2. **`Time` and `CancelReplaceLoss` were unreachable in production.** Queue-jump
   detection scanned only orders still resting, but the victim of a jump has almost
   always departed by the time the jumper is scored — it was consumed by the very match
   under dispute. Every time-priority breach was therefore downgraded to a price
   violation on the live path. `shortedIndex` now keeps the orders that departed
   *under-reported* (bounded ring, 64Ki entries, level-indexed) as queue-jump evidence.
   Membership is narrower than batch's whole-session scan on purpose: earlier in the
   queue + the reference filled it + the contestant did not report it, all three, is what
   separates a priority breach from a merely fabricated fill.
3. **`ScoredFills` underflowed.** Stream mode computed `TotalFills - PhantomFills` on the
   premise that phantoms were counted into `TotalFills`. They are not — `AddPhantom`
   increments `PhantomFills` alone — so any session with more phantom fills than real
   ones wrapped this uint64 to ~1.8e19. The equivalence tests never compared the field.
4. **Duplicate acks were counted twice.** The batch collector deduped redelivered
   `orders.acked` events on (order, exec type, T7); the streaming join never did, so a
   producer retry inflated cumulative reported qty and manufactured overfill violations
   against an innocent engine. Ported into the join as `pendingOrder.addAck` — a linear
   scan over the handful of acks held for that order, so there is no per-order map
   allocated 445k times a session.

**Then a fifth, found by asking whether every class was actually reachable: neither mode
could see a DROPPED order.** `emitReady` pushes an order to the validator only once it
has at least one ack (`stream.go`), so an order the contestant never answered was
discarded by the join. Full mode therefore never counted it in `ScoredOrders` — an engine
that answered nothing at all scored 1.0 through the empty-session guard, exactly like the
acker did before `MissedFill` — and invariants mode's lost-order branch, which lives in
`Apply`, could only ever fire for an order that reached `Apply`, i.e. one whose responses
failed the T7 sanity gate. `LostOrder`/`LostCancel` were effectively dead in production in
both modes.

Silently dropping orders and blindly ACKing them are the two ways to game pass 1;
closing only the second would have left the gate as easy to walk around as before. The
join now classifies each pending order (`pendingOrder.outcome`) and reports the
unanswered ones through a new `AddLost` callback on both validators. A dropped order does
NOT enter the reference book: T3 is captured from the *response*, so an unanswered order
has no ingress timestamp and no place in the replay timeline. It is graded standalone —
counted into `ScoredOrders`, flagged, dirtied — which is sufficient, because the failure
is unconditional: no response is wrong whatever the book would have done.

That work also exposed a **scoring bug in invariants mode's denominator**: `applied` was
incremented ABOVE the lost-order branch, while `Finish` computes
`ScoredOrders = applied + LostOrders + LostCancels` on the premise that lost orders
returned before being counted. Every lost order therefore sat in the denominator twice,
and an engine that answered nothing scored **0.5 instead of 0**. `applied` now increments
only when an order actually enters the T7 reorder window — which is also the right
denominator for the taint rate, since an order with no T7 says nothing about whether the
T7 stream is trustworthy.

Plus one defect in the book itself, found while making self-match detection live: a
**repriced order lost its SMP identity**. `replace()` rebuilt the resting order without
carrying `smpID`/`hasSMP`, so every repriced order was silently exempt from self-match
prevention — the reference would match it against a same-id aggressor that a compliant
contestant had correctly skipped, and then score the contestant for the missing fill.
The correctness scenario uses the HFT action mix, which includes replaces, so this was
live. The re-inserted order now inherits identity from the order it replaces.

**`AGGRESSIVE_FILL_TOLERANCE_US` is deleted, not merely unread.** It accepted a fill the
reference never produced when opposite liquidity had been resting within ±tolerance of
the aggressor's `EffectiveT3` — a hedge against the replay interleaving differently from
what a live engine could observe. It does not apply: pass 1 runs exactly ONE task
(`buildCorrectnessTasks`), the worker opens one TCP connection per task
(`connect_tasks`), so the session is a single flow, `TCPSeq` totally orders it,
`CrossFlowTie` never engages, and the reference replays in wire order. An engine that
processes in read order reproduces the reference exactly — there is no interleaving to
forgive. Pass 2 is book-free and never consulted it. It was implemented only in the
deleted batch path, making the documented env knob a production no-op, and the
per-order availability windows feeding it (`book.avail`) grew O(session) on the live
streaming path — an unbounded allocation in a validator whose whole design premise is
bounded memory. `model.ParticipantOf`, its last caller, went with it.

**Violation-class reachability is now pinned by tests, not by reading.** Three of these
defects were classes that existed, were counted, were stored — and could never fire.
`class_coverage_test.go` provokes every class full mode owns and asserts three things
each: it fires, its exact counter and `ViolationCount()` both move, and it dirties its
order so the score actually drops. That last one is the failure shape the original
order-level score change was written for. Every assertion was mutation-verified by
disabling each mechanism in turn and confirming the corresponding test fails. Adding a
`ViolationType` without wiring it now fails a test rather than being discovered a session
later.

If the epoll-ordering problem (honest stock-socket engines scoring ~45%) is revisited,
it needs a bounded per-level availability structure and a deliberate grading decision —
not this knob. That work is the validator redesign, and it belongs to pass 2's
cross-flow window, not to pass 1.

## The capture loses nothing (2026-07-31)

`capture_gaps` is **0**. A 1,844,352-order pass-1 run had every single order matched, and
a correct engine scored **0.9943**. It started the day at 0.5999 with 25.6% of responses
missing.

Two separate defects, found in sequence.

**1. The BPF program discarded everything past 1536 bytes of a coalesced packet.** The tc
egress hook runs before GSO segmentation, so it sees the whole pre-segmentation skb — up
to 64KiB when the engine batches responses — and `capture_len` clamped to `COPY_CAP`,
dropping ~514 execution reports per truncated packet as "a sampled loss, fine for a
latency distribution". True for a histogram, fatal for correctness. Fixed by clamping
`gso_max_size`/`gso_max_segs` on the capture interface, which bounds skb construction
UPSTREAM of the hook (`ethtool -K gso off` cannot: it governs on-wire framing only).
25.6% loss → 0.2%.

**2. The FIX framer manufactured the rest of the loss on well-formed input.** Two bugs
compounding:

- `frame_fix` treated "not enough bytes yet" as "malformed" — a buffer holding exactly
  `8=FIX.4.2\x01` plus a byte or two is the head of a good message that has not finished
  arriving, and it was discarded. The remainder then began mid-message.
- Resync searched for a bare `8=`, which is not a message boundary: tag 38 (OrderQty)
  renders as `38=`, as do 58/108/118. So recovery landed inside the NEXT message and
  walked forward field by field, consuming that one too.

Together they fired whenever a TCP segment boundary landed within ~2 bytes of a
BeginString — about one boundary per segment over ~7 messages, i.e. the ~0.2% of orders
that went missing. Fixed: wait when short, resync on `8=FIX`.

**3. And a latent defect the fix exposed: the reassembler could not advance past a hole.**
`next_seq` only moves through contiguous data, and `drain_hold` only fills from `next_seq`,
so one missing segment stalled that flow for the rest of the session — the old crude
resync had been masking it by consuming bytes. Hold overflow made it worse: it cleared
every segment queued behind the hole AND left `next_seq` pinned, so the overflow repeated
forever. Fixed with explicit gap recovery: after 4 pushes with no progress, or on hold
overflow, the hole is declared lost and `next_seq` jumps to the earliest held segment.
Cost is bounded to the missing bytes.

Measured after: `stream_gap_bytes` = **2** for a whole session, so there were essentially
no genuine holes at all — every byte the kernel captured was there, and the entire loss
was self-inflicted.

**What made this findable: an offline property harness** (`framing_property.rs`).
Generate real FIX traffic, replay it through `Reassembler` + `frame_fix` under arbitrary
segmentation, reordering, duplication and a permanently dropped segment, and assert every
message is recovered exactly once. It reproduced both shipped defects in 20ms and found a
third nobody had seen (a reordered stream START makes the reassembler adopt the wrong
origin and lose the head — documented, not fixed, since the capture attaches before the
connection opens).

Four cluster hypotheses were confidently wrong before that harness existed — CPU
starvation, ring-buffer overflow, Kafka producer queue, capture teardown race. Each cost a
build/import/run cycle. The framer and reassembler are pure functions; they should never
have been debugged in a cluster.

**Answered along the way:** broker headroom is a non-question. `producer_inflight` stayed
0 and `undelivered_at_exit` 0 — a single broker (RF=1, 24 partitions, 4 CPU limit, 20Gi
local-path) is nowhere near the constraint at these rates. Two real bugs were fixed there
anyway: the capture never flushed its Kafka producer queue on shutdown (enqueue is
fire-and-forget, so "flushed" meant "enqueued"), and the capture Job's
`terminationGracePeriodSeconds` was 5, which would have SIGKILLed any drain mid-flight.

**Residual, accepted:** 0.9943, i.e. 10,583 dirty orders of 1.84M — 7,004 missed fills,
3,450 price violations, 532 time violations. These are genuine grading findings now, not
capture artifacts. Leading suspect for the missed fills is the matcher's 5-second idle
eviction: a resting maker is acked immediately, marked answered, reaped as routine GC, and
its FILL arrives later when someone finally trades against it — unmatched and dropped
(`unmatched_response` = 11,837 with requests fully framed). **Decision: not pursuing this
unless the rate grows at higher throughput.**

### Parked: capture scaling beyond the correctness rate

Pass-1 runs at ~40k orders/s. The stated target is 400-500k/s, and the capture has
structural limits that this workstream did not touch. Recorded so they are not
rediscovered under load; deliberately NOT acted on until EKS shows whether they bite.

- Everything after the ring buffer is single-threaded: decode, reassembly, FIX framing and
  matching all run in one tokio task.
- The ring buffer is 64MB ≈ 43k records — about 30ms of headroom at 500k/s, versus ~12x
  that at the correctness rate. `ringbuf_dropped` was 0 all day, but that margin shrinks
  linearly.
- Up to 1536 bytes per packet are copied to userspace purely to re-parse FIX there
  (~100MB/s at 500k/s).
- The matcher holds a `HashMap<String, _>` entry per in-flight order, keyed by ClOrdID —
  ~2.5M live entries at 500k/s with the 5s idle window.

~~If these bite, the shape of the fix is parsing in the kernel to emit fixed-size records,
or sharding userspace workers per flow.~~ **Superseded (2026-08-01): kernel-side parse is
dead by design** — messages do not align with packet boundaries and in-kernel stream
reassembly is not possible; it was attempted early in the project and failed for exactly
this reason. The full plan for these limits — offline criterion ceiling measurement,
ordered fixes (memory+ring pairing, matcher key, `CAPTURE_CAP` 9029, conditional per-flow
sharding via flow-hash-steered ring buffers) — is `docs/capture-ringbuf-drops.md` §6.

## Opened by the 2026-07-31 grading/capture work

1. **`correctness_summary` cannot say WHY an engine failed.** No columns for
   `missed_fills`, `lost_orders`, `lost_cancels`, `capture_gaps` or `tainted` — the score
   persists, the reasons do not. Worse, `time_violations`, `self_trades` and
   `cancel_replace_loss` are computed by iterating `Report.Violations`, which is capped at
   `maxViolationExamplesPerType` (100) per class, so those three columns silently max out
   at 100 on any real run. The exact counters exist on the Report; they just are not the
   ones being stored. Pre-existing, and it blocks any useful contestant-facing feedback.
2. ~~**The taint flag is only proven clean.**~~ It fired for real on 2026-07-31, on a
   session where the capture lost 93.8% of responses: the score was published flagged with
   the reason and the count, instead of presented as authoritative. Both directions of the
   signal are now observed.
3. **Pass-1 residual violations are unexplained.** The reference book scores 0.9833 with
   34,743 violations (1.7% of orders). These are real findings now, not capture
   artifacts, so 0.983 should not be assumed to be the ceiling until they are attributed.
4. **Every latency number recorded before 2026-07-31 is optimistic.** The capture was
   publishing ~74% of responses, biased toward uncoalesced (low-load) ones, and then a
   further ~0.2% was lost to the framer. Everything in `deploy-bench/` predates both fixes
   and should be re-measured before any of it is quoted.
5. **`gso_max_segs 1` cost: measured, not detectable.** Four max-rate 45s runs, mean
   1,816,640 orders before the clamp vs 1,811,136 after (-0.3%, inside the 1.4% spread
   between the two pre-clamp runs). Execution reports are ~130 bytes, so GSO was batching
   tiny messages for syscall economy rather than moving bulk bytes. It is a
   per-packet-overhead tax, so re-measure if per-node rates climb toward the D-bucket
   targets. Applies only to the contestant's capture interface.

## Opened by the 2026-07-31 mixed-protocol / capture-CPU work

1. **The capture had no CPU telemetry, and the first three CPU measurements were all wrong.**
   Now fixed and covered by tests, but recorded because each is a trap that will recur:
   `process_cpu_percent` read 970% (~10 cores) purely as a sampling artefact — samples driven
   by a `tokio::time::interval` with default `Burst` missed-tick behaviour collapse to ~1ms
   under load, and against 10ms `/proc` accounting that yields 0% or ~1000% from the same
   healthy process; the throttling counter read `/sys/fs/cgroup/cpu.stat`, which under
   `HostPID: true` with no private cgroup mount is the host ROOT cgroup and can only ever
   report `nr_throttled: 0`; and a hand-read of `/proc/1/stat` measured the host's systemd,
   not the capture. **There is still no trustworthy CPU figure for the capture** — the fixed
   sampler produces the first one on the next loaded run. Splitting it by thread name
   (`rdk:*` = Kafka produce and zstd, versus `tokio-rt-worker`/`iicpc-ebpf-late` = drain,
   reassembly, framing, matching) is what says which half to optimise.
2. **Ring-buffer drops were host CPU starvation, not a capture limit.** The same max-rate
   REST workload dropped 23,436 records (3.2% capture_gaps, session tainted) on a busy node
   and 0 on a quiet one while decoding MORE records (4,185,669 vs 4,112,553). Kafka was ruled
   out (`producer_inflight` 24) and CFS throttling was 0. The capture's CPU **request** was
   200m against a contestant pinned Guaranteed at 4 — a 20:1 CFS weight disadvantage on a
   node where the contestant is graded on throughput and will saturate its cores by design.
   Raised to 2. This applies to EKS, not only to a laptop: a dedicated sandbox node does not
   remove the contention, it concentrates it between exactly those two pods. Full
   investigation, including the hypotheses the data killed:
   `docs/capture-ringbuf-drops.md`.
3. **`CAPTURE_CAP` = 1536 is what forces MTU 1500 platform-wide**, costing ~6x the packet
   count for the same bytes. Raising it to >= 9029 restores jumbo frames and fits the per-CPU
   map limit; going further would let GSO/GRO stay on entirely. Nobody has measured what the
   current tax costs, and the fairness argument for keeping it ("uniform, therefore neutral")
   is weaker than it looks because the tax is proportional to packet count and so scales with
   each engine's I/O behaviour. Plan: `docs/capture-cap-plan.md`.
4. **`cargo test -p <crate>` is not a sufficient gate.** `telemetry-ingester` (three sites,
   including the `inject` BINARY) and a bot-fleet example had not compiled since `b0655fb`
   added `smp_id`, because the suite that was being run never covered them.
   **`cargo test --workspace` is the gate.**

## Local cluster state left behind (2026-08-01)

Not backlog items, but the next session will waste time rediscovering them.

- **Code committed but NOT deployed locally.** `sandbox-orchestrator` carries the capture
  CPU request raise (200m -> 2) and `score-computer` carries pass-1-only correctness
  (b0b790c); neither image was rebuilt/imported, so the RUNNING orchestrator still requests
  200m and the RUNNING score-computer still pools pass-1 and pass-2 into
  `total_correctness`. Rebuild + `sudo k3s ctr images import` + rollout before trusting a
  leaderboard number or re-testing ring-buffer drops.
- **A CoreDNS workaround is installed and must never reach EKS.** ConfigMap
  `coredns-custom` in `kube-system` NXDOMAINs `*.*.iitr.ac.in` so Alpine builds resolve.
  The campus resolver answers the search-domain-expanded form (`dl-cdn.alpinelinux.org.
  iitr.ac.in`) with NOERROR-and-no-records instead of NXDOMAIN, and musl stops there rather
  than trying the absolute name — which broke every contestant Kaniko build. It survives
  k3s restarts. VPC DNS behaves correctly, so this is local-only scaffolding.
- **Throwaway scenarios seeded:** `cpu4`, `cpu4x41k`, `smoke`, `mixed3`. Since
  `StartBenchmark` fans out over every row in the table, a browser-triggered submission
  currently runs eight sessions including two max-rate ones. `mixed3` is worth keeping for
  B3 re-runs; the other three are disposable.
- **The node IP moves.** It changed three times in one session (WiFi DHCP), each time
  breaking the cluster, the registry push endpoint (spawner env) and the pull config
  (`/etc/rancher/k3s/registries.yaml`) independently. A dummy interface plus `--node-ip`
  would end this permanently; `up-dev.sh` already warns when the installed registries.yaml
  no longer matches.

## The second EKS bring-up (2026-08-03) — first end-to-end success, and what it cost

**Outcome: the platform ran end to end on EKS for the first time.** A contestant zip
went upload -> Kaniko build -> ECR -> scheduled slot -> capture -> bots -> telemetry ->
validation -> score -> leaderboard, with graphs rendering. Reference engine scored
**0.9921** (local FIX reference: 0.99445) and the FIX acker was correctly disqualified at
**0.0417**, so grading discriminates on EKS. A `protocol: ALL` submission also completed.

Seven defects had to be fixed to get there. **Six were invisible to local k3s by
construction, and five of those are the same class:** the local path (`up-dev.sh`,
`e2e/02-bootstrap.sh`, `infra/Makefile`) injects something imperatively that the
`kubectl apply -k overlays/eks-contest` path never creates. That class is the single
biggest source of EKS-only failure and every instance below is now declared in the tree.

### Fixed and committed

1. **`CAPTURE_IMAGE` / `SPAWNER_IMAGE` were never stamped.** Both travel as ENV VARS,
   which kustomize's `images:` transformer cannot rewrite, so EKS ran the stale public
   `ghcr.io/agrawalx/*:demo` while everything else ran the pushed tag. The capture image
   predated `ccf0f80`, so it carried `CAPTURE_CAP=1536` and a compiled-in
   `CAPTURE_CLAMP_MTU` default of 1500 — which is the *entire* explanation of the
   "capture clamps MTU 9001 -> 1500" mystery (nothing in the tree sets that var) and the
   prime suspect for `orders.acked = 0`. `SPAWNER_IMAGE` is the `fetch` initContainer of
   every build Job, so the build pipeline ran stale code too. Fixed with a
   `replacements:` block that derives both tags from the already-stamped image fields
   (one source of truth, cannot drift) plus a fail-closed guard in `push-images.sh` that
   refuses the push if any `ghcr.io/` ref survives the render or either env var does not
   resolve to the pushed tag.
2. **orchestrator -> algo dial denied by NetworkPolicy.** Recorded as a ProtocolAll bug
   in `slot.go`; it was neither. `sandbox-isolation` allowed the ingress half of the dial
   and nothing allowed the egress half, and its catch-all `0.0.0.0/0` EXCEPTs 10.0.0.0/8,
   i.e. every pod IP. Single-protocol runs looked fine only because `slot.go` dials the
   EXTRA port, so a FIX-only slot never dials at all. Egress rule added, scoped to 9898 +
   8080, verified before/after from the orchestrator's netns.
3. **Kafka did not tolerate its own node's taint.** Terraform creates a dedicated,
   tainted kafka node group; nothing tolerated it, so Kafka scheduled onto a general node
   and the broker node ran nothing while Kafka competed with Postgres/Timescale/validator.
   Fixed in `patch-kafka-single-broker.yaml`. Must be set BEFORE first apply — EBS PVCs
   are AZ-bound, so moving Kafka later can strand `kafka-logs` and force a topic wipe.
4. **`auth-api` Service missing from the tree -> frontend CrashLoopBackOff.** The frontend
   templates its nginx upstreams from env and nginx resolves every upstream at
   CONFIG-PARSE time, so an unresolvable name is a hard `[emerg] host not found in
   upstream` and the container never starts — regardless of whether anything ever calls
   `/api/auth/`. `up-dev.sh` had always injected a placeholder Service; the kustomize tree
   had not. Service (not Deployment) now declared in `k8s/kustomization.yaml`.
5. **IRSA never annotated -> the build pipeline could not create ECR repos.** The AWS SDK
   fell through its whole credential chain to EC2 IMDS and found nothing; the submission
   went `failed`. `enable_spawner_irsa` now `true` in `contest.tfvars`, with the two-apply
   ordering documented there (the SA does not exist during the first apply).
6. **Validator OOMKilled at 2Gi, in a poison loop.** See the dedicated section below.
7. **Fixture bug: the drain acker killed itself.** `accept().await?` in
   `e2e/contestant-echo` propagated any transient accept error out of `main`, taking the
   listener with it — so a 500-task connect storm ended the process after 3 seconds and
   read as a platform throughput ceiling. Now logs and continues, with an EMFILE/ENFILE
   backoff. The reference CLOB already did this correctly (`match` on accept), which is
   why it degraded gracefully instead of vanishing.

Also corrected: the bring-up guide told you to create secrets BEFORE the overlay, but
nothing creates the six namespaces except the overlay, so `create-secrets.sh` fails on a
fresh cluster. Namespaces must be applied first; doing so also means no pod ever enters
the `CreateContainerConfigError` state that step existed to avoid.

### Validator memory — measured, and the eviction question settled

2Gi OOMKilled every validator pod. Because the OOM lands before the Kafka offset is
committed, the pod restarts and re-reads the SAME session: a poison loop that never
drains, which KEDA makes strictly worse by scaling to max so every replica loops. 14
session-completion events were stuck behind it. Raising the limit drained all 14 with
zero restarts.

**Measured peak: 5.2 GiB for one 3.8M-order pass-1 session** (limit now 8Gi). Where it
goes, measured not estimated:

- The reorder buffer holds up to `2*REORDER_WINDOW` = 2,097,152 assembled orders before
  releasing anything (`Push` releases at `2*window`, not `window`). At a measured
  **283 B/order** that is ~594 MB, and it is FIXED. Consequence worth knowing: for any
  session SMALLER than 2.1M orders, `release()` never fires and the whole session is
  buffered — the streaming validator degenerates to batch behaviour below that threshold,
  which is the failure the batch path was deleted for.
- The rest scales with the orders RESTING in the reference book (`v.pending`, `v.ref`).

**Eviction is not missing and this is not a leak.** Filled, cancelled and never-resting
orders are finalized and deleted as they leave the book (`DrainEvicted` -> `finalize`),
and `book.Forget` exists solely to bound the streaming path ("Batch never calls it").
Resting orders *cannot* be evicted — a later order may still match them, and they must be
scored when they leave. So validator memory is **O(peak book depth), not O(session)**, and
it is scenario-controlled rather than contestant-controlled (the book replays the bot's
flow, so no submission can inflate it). 8Gi is a ceiling sized against one measurement,
not a bound: re-measure peak book depth per scenario once contest scenarios are fixed.

### The "120k/s stall" — explained: it is the RESPONSE_TIMEOUT cliff

Reproduced with three unrelated engines (Rust CLOB, Go REST echo, tokio drain acker), so
never contestant-specific. Ruled out by direct measurement, each with evidence:

| candidate | verdict |
|---|---|
| bot send capacity | not it — FIX drain-sink sustained **200,001/s** for a full 60s |
| mutual TCP flow-control stall | not it — 200 sockets stay ESTABLISHED with small, *churning* queues |
| capture overload | not it — `ringbuf_dropped` / `acked_dropped` were 0 |
| engine crash | not it — listener stays up, sockets stay established |
| bandwidth shaping | not it — `bandwidth` is absent from the CNI chain (see below) |

The actual mechanism, caught second-by-second on a 100-task ProtocolAll run:

```
16:03:53  80,058 tps  p50 4,706 ms  err 0.000
16:03:54  72,992 tps  p50 4,920 ms  err 0.005   <- first timeouts
16:03:55  21,455 tps  p50 5,125 ms  err 0.765   <- p50 crosses 5,000 ms
16:03:56       0 tps                err 1.000
```

Offered load above engine capacity builds a standing queue; latency climbs; the moment
the queue exceeds **5 seconds of work** every response arrives after its deadline, the
bot's watchdog times them all out, and delivered tps reads **0** with `error_rate` 1.0 —
while the engine is still processing at full rate (queues still churning, 100 sockets
still established). Only the *time to reach* the cliff differs: ~30s at mild overload
(100k offered vs ~90k capacity), ~4s at heavy overload (200k offered vs ~120k delivered),
never for drain-sink (no responses, so nothing can be late).

**Grading consequence, needs a decision:** a submission that is merely slightly too slow
currently scores as though it died completely — 0 throughput, 100% errors — rather than
"90k with high latency". That is a ranking decision, not a bug, but it is not the
behaviour anyone would choose on purpose.

**Not fully explained:** in one acker run the capture's `xdp_packets`/`tc_packets` froze
and the bot's `sent_rate` went to 0, which the cliff does not account for (there the
engine kept working and sockets kept moving). Possibly a second effect at higher backlog
rates. Treat that specific case as open.

### Throughput numbers measured (all bounded by the caveat below)

- Platform, with a minimal FIX responder: **336,698 orders/s at 0.03-0.05 ms p50**, one
  bot worker pod, zero capture drops. So the ~80k both real engines cap at is the
  ENGINE, not the bot, capture or network.
- Bot send ceiling per c7g.xlarge (drain-sink, no acks): **200,001/s sustained exactly**;
  at a 1M/s target it peaked 901k and decayed to ~460k; at 2M/s across two workers both
  were **OOMKilled**. That OOM is an artefact of the control, not the bot: with nothing
  responding, every order stays pending for `RESPONSE_TIMEOUT`, so a worker tracks
  `rate x 5s` live entries (5M at 1M/s) on a 6.6Gi node. A responding engine holds
  `rate x actual latency` (~100 entries). **M1 is therefore still unmeasured** and
  `botworker_max_size` still cannot be fixed from this.
- The 104 ms "service_time" seen on `correctness` is **queue depth, not engine cost**:
  Little's Law gives 10,000 inflight / 94,735 per s = 105.6 ms vs 104.4 measured, and
  `service_time ~= round_trip` (103.9 vs 104.4) leaves only ~0.5 ms of network. `t3` is
  stamped by the eBPF ingress hook on packet arrival, so it legitimately includes kernel
  socket backlog. Latency from the unpaced `correctness` scenario is meaningless as a
  latency figure.

### Unfixed bugs discovered — none of these are addressed

1. **A REST acker scores 1.0000.** `smoke-rest-echo` — no order book, returns
   `exec_type=2` (FILL) at the order's own price for every request including cancels and
   replaces — produced `valid_fills == total_fills`, **zero violations**, and a perfect
   score on 1.89M real orders. The identical cheat over FIX scores 0.0417. **Grading is
   protocol-dependent and the REST path does not discriminate at all.** Contest-fatal,
   and invisible because it presents as a flawless submission. `remaining-work` records
   the acker-scores-1.0 defect as fixed; that fix holds for FIX only. First place to look
   is `frame_http_ws`, the one capture path `session-handoff.md` §3 flags as having no
   property coverage, then the validator's JSON fill decoding.
2. **Empty sessions score 1.0000 instead of failing closed.** Every session with
   `sent_count = 0` / `status = timed_out` published a perfect score — 8 of 14 backlogged
   sessions. A session the validator saw no data for must not be scoreable.
3. **KEDA cannot scale the bot fleet — circular dependency.** The controller's pre-scale
   gate refuses to publish `workload.assignments` until members == shards; the committed
   ScaledObject scales on *lag on that same topic*. No publish -> no lag -> no scale ->
   the gate times out after 90s and the session fails. **Any session needing more shards
   than the current replica count fails at cold start, permanently** (observed: ramp
   needed 5, had 2). The prometheus/`iicpc_controller_demanded_workers` design that §B5
   describes as validated is applied ONLY by `deploy-local/b5-autoscale-shards.sh`
   inline — it was never committed to `k8s/benchmark/bot-fleet/scaledobject.yaml`. The
   gauge is emitted and nothing consumes it. Note a second, independent problem behind
   it: 1-pod-per-node (required podAntiAffinity + 3 CPU of 3.6 allocatable) means N
   shards needs N nodes, and node provisioning (~2 min) loses to the 90s gate anyway.
4. **Contestant bandwidth shaping is inert.** `ALGO_EGRESS_BANDWIDTH` /
   `ALGO_INGRESS_BANDWIDTH` are set to 100M and `slot.go` stamps
   `kubernetes.io/{egress,ingress}-bandwidth` on every algo pod, but the `bandwidth`
   plugin is **not in the CNI chain** (`aws-cni -> egress-cni -> host-local -> portmap`;
   the binary is present in `/opt/cni/bin`, unused). The platform believes it rate-limits
   contestants to 100 Mbit and does not. That is a fairness assumption that silently does
   not hold.
5. **Go submissions are silently capped at Go 1.23.** The build template pins
   `golang:1.23-alpine` with `GOTOOLCHAIN=local`, so any `go >= 1.24` in a contestant's
   `go.mod` fails with `go.mod requires go >= 1.25 (running go 1.23.12)` and no guidance.
   Repo fixtures already declare 1.25, so this is not hypothetical.
6. **A failed build permanently poisons its sha256.** `submit.go:114` dedups on sha256
   without checking status, so re-uploading identical bytes returns the FAILED submission
   and explicitly skips the build. Recovering required deleting the row from Postgres. A
   contestant hitting a transient build failure can never retry the same artifact.
   Related to, but distinct from, the cross-contestant 404 trap already recorded.
7. **Owner-scoped 404 hides submissions across identities.** With auth off, identity comes
   from the token `sub`; a submission made as one identity returns `submission not found`
   to another, including for `/benchmark`. Already recorded as a dedup trap; observed here
   as a plain operational footgun.
8. **`drain-sink` cannot be used as a load-ceiling control as committed.** It hardcodes
   `Port: 8080`, which forces `protocol: REST`, and HTTP/1.1 cannot reuse a connection
   without a response — so it degenerates to ~10 connections at 10k/s. A FIX variant on
   9898 is required (`deploy-local/drain-sink-fix.zip`, untracked).
9. **`submission-api`'s Service maps its `metrics` port to 8080**, the app's HTTP port,
   not 9090 — so that scrape target is wrong.
10. **Cluster-autoscaler adding sandbox nodes is unvalidated.** Slots scheduled onto
    autoscaler-added nodes failed during this session; the netpol fix (item 2 above) is
    the likely explanation since it was AZ-independent, but node-scaling was never
    re-tested after the fix. Worth confirming before relying on sandbox autoscaling.

### Found by the first `production/` bring-up (2026-08-04)

**11. A slot's capture can be denied its node, permanently, and the session
silently produces no data.** Two concurrent submissions; one session out of four
finished `completed` with `acked_count = 0`, zero telemetry rows and no score.
The contestant sees a completed run with no result and no error anywhere.

Measured, exact:

```
Warning OutOfcpu  capture-019fccfc-bf25-7e52-...
  Node didn't have enough resource: cpu, requested: 2000, used: 6190, capacity: 7910
Warning BackoffLimitExceeded  job/capture-019fccfc-bf25-7e52-...
```

Timeline on the node (`c6i.2xlarge`, 7910m allocatable), one submission per node
throughout — the 1-slot-per-node property held, this is an overlap in TIME:

```
13:35:46  algo(constant)       Scheduled            holds 4000m
13:35:49  capture(constant)    Started              holds 2000m
13:36:55  both sent Killing    algo grace 30s -> frees 13:37:25
                               capture grace 60s -> frees ~13:37:55
13:36:58  algo(correctness)    FailedScheduling
13:37:06                       TriggeredScaleUp
13:37:25  algo(correctness)    Scheduled  <- the instant the old ALGO's 30s expired
13:37:28  capture(correctness) OutOfcpu   <- old CAPTURE still draining (33s of 60s)
13:37:29                       BackoffLimitExceeded -> permanent
```

Arithmetic at 13:37:28: new algo 4000m + OLD capture 2000m + DaemonSets ~190m =
**6190m**, exactly the reported figure. Free 1720m against 2000m needed, short by
280m.

Root cause: **a slot's 6000m does not release atomically.** The algo pod takes
the default 30s grace; the capture is given 60s explicitly (`slot.go:596`) so its
Kafka producer can drain — which is correct and must not simply be shortened,
since SIGKILL mid-flush loses whole batches of `orders.acked`. So for **30
seconds after every session there is a window where a node looks schedulable for
a new slot but cannot host that slot's capture**. Three decisions compose into
the failure:

- the capture is placed by explicit `NodeName` (it must share the algo's netns),
  which **bypasses the scheduler entirely** — it either fits instantly or dies;
- nothing reserves the capture's 2000m when the ALGO is scheduled, so the
  scheduler admits a slot onto a node that cannot actually hold it;
- `backoffLimit: 0` (`slot.go:588`) makes one transient rejection permanent.

The cluster autoscaler behaved correctly and was simply too late — it fired at
13:37:06 and delivered nodes at 13:37:43, while the scheduler had already placed
the algo on the old node at 13:37:25.

Fix options, none obviously right: reserve the pair (make the algo request the
combined footprint, or use a placeholder pod); allow `backoffLimit: 1` with a
guard so a retried capture that missed the session start is discarded rather
than trusted; or align the grace periods. The first is the only one that removes
the race rather than narrowing it.

**Related fragility worth recording separately: 1-slot-per-node is emergent, not
declared.** There is no `podAntiAffinity` on algo pods (verified on a live pod:
`.spec.affinity` is empty). Isolation comes only from resource requests — algo
4000m/8Gi + capture 2000m/2Gi against 7910m/~14.4Gi means a second slot cannot
fit. But the BASE default is `ALGO_CPU=2`; only `overlays/eks-contest` raises it
to 4. On any deployment without that patch two slots fit on one node and the
fairness property silently disappears, with nothing failing to signal it.

**12. `envUint32` cannot express zero — every numeric scenario knob silently
ignores `0`.** `builder.go:410`:

```go
n, err := strconv.ParseUint(v, 10, 32)
if err != nil || n == 0 { return def }   // a parsed ZERO is treated as "unset"
```

Setting `MIX_RETAIL_PCT=0` and `MIX_INSTITUTIONAL_PCT=0` left them at their
defaults 25 and 15, the mix summed to 140, validation rejected it, and
submission-api CrashLooped on every start with `population mix must sum to 100,
got 140`. Affects `CONSTANT_TOTAL_RPS`, `SPIKE_PEAK_RPS`, `RAMP_PEAK_RPS` and all
three mix percentages. For a percentage, zero is a legitimate value and there is
no way to express it.

Consequence in practice: a "100% HFT" workload is unreachable. The closest is
98/1/1, which at 100k adds 200 retail bots — 1,000 rps but **two-thirds of the
task count**, and task count is what decides shard count. Fix: only fall back
when `os.Getenv` returns `""`.

**13. Bring-up was not idempotent against its own teardown.** Teardown
`state rm`s the results bucket (it carries `prevent_destroy`), leaving it alive
in AWS but absent from state; the next apply then fails with
`BucketAlreadyExists` because S3 names are globally unique. Fixed in
`production/01-cluster.sh`, which re-imports it before applying. Recorded because
the same shape applies to anything else ever protected by `prevent_destroy`.

### Corrections to previously recorded diagnoses

- §7a/b/c of `eks-bringup-handoff.md` all had the wrong cause; corrected in place.
- The NetworkPolicy theory for `orders.acked = 0` was confounded: the "known-good"
  `e2e/02-bootstrap.sh` runs also injected the correct images, so image version and
  netpol scope varied together and nothing isolated the policies.
- `bot-fleet-controller`'s one-off `45364d4-t180` pin is safe to overwrite: `45364d4` IS
  an ancestor of HEAD and `defaultSlotHTTPTimeout = 180 * time.Second` is in the tree.
  `context deadline exceeded` on a slot is the overall readiness deadline, not that
  timeout.
- Every capture measurement taken through `overlays/eks-contest` before this session is
  invalid (unknown binary). `deploy-bench` numbers are unaffected — `up-full.sh` injects
  `CAPTURE_IMAGE` correctly.

## Housekeeping

- ~25 commits on `feat/bot-tps` unpushed: `git push fork feat/bot-tps`.
- Eventually: PR to main; ARCHITECTURE.md "Where this is going" chapter will need a
  refresh once B/C land (it describes some of this as future work that is now done).

## What is left, in priority order (2026-08-01)

B1, B2 (both passes), B3 (both forms) and the contestant submission path all pass on
local k3s and were re-run — not inherited — after this session's fixes. What follows is
everything still open, ordered by what would hurt most if it stayed broken.

### 1. Two ranking decisions that change what contestants are graded on

- **W (B6).** ~~No longer a pass-2-only question: a `ProtocolAll` submission runs three
  concurrent flows in pass 1~~ **Corrected 2026-08-02: pass-1 is single-connection by
  construction for EVERY submission, including ProtocolAll.** The correctness scenario
  builds exactly one task (`buildCorrectnessTasks`) and round-robin target stamping
  (`runner.go` TargetIdx = i % len(targets)) lands it on target 0 — FIX, one flow. The
  15.5%-time-violation measurement predates this shape and should not drive W. Decision
  (2026-08-02): keep it this way — one protocol, one connection, TCPSeq totally orders
  pass 1; pinned by `TestPass1IsSingleConnectionEvenForProtocolAll`. **W is therefore a
  pass-2-only calibration**, measured in the MTU-9001 regime (pre-change numbers are
  invalid): reference engine vs a deliberately-naive engine, set W where the
  distributions separate; fully local except the final number.
- ~~**`P99AtPeakNS` is the WORST SINGLE SECOND**~~ **DECIDED + FIXED (2026-08-02):**
  the TPS tiebreak now uses `StableP99NS` (median of the peak wave's per-second p99s) —
  the same summary the pass/fail gate judges, so gate and ranking agree on what latency
  means and ties are no longer broken by connection-setup/warmup noise. `MaxP99NS`
  stays computed as the diagnostic its comment always claimed. Pinned by
  `TestP99AtPeakIsStableNotWorstSecond` (which also documents that the climb judges
  waves from index 1; wave 0 is baseline).

### 2. Make the run-group shape match the scoring rule

**Decided 2026-08-01: a run-group is one `correctness` run plus one `ramp` run.**
Correctness comes from the pass-1 session alone (b0b790c); the ramp supplies latency and
TPS. Pass-2 invariants correctness is still recorded per session as a metric and decides
nothing — book-free grading cannot separate a correct engine from one that fills every
order (measured: the echo scored 1.0 in invariants mode against 0.62 in full mode).

That shape closes two edges by construction rather than by code: `aggregateCorrectness`
always has a pass-1 session to read, and `score.Compute` never returns
`ErrMissingRampSession`, so every group produces a `scores` row. The old asymmetry —
disqualified groups published while passing ones showed "pending" forever — cannot arise
once every group contains a ramp.

**Sequential-within-group dispatch is DONE (2026-08-02)** — StartBenchmark publishes
only the first scenario; the status consumer dispatches the next on each terminal
session (continue-on-fail). A 4-scenario group holds at most one order band at a time,
so 4 contestants' groups run concurrently. Unit + live-DB integration tested.

**What still has to change for that to be what actually runs:**
- `StartBenchmark` fans out over EVERY row returned by `ListScenarios`, so the scenario
  table (or the trigger) has to be reduced to exactly these two. The local table currently
  holds eight, including throwaways seeded during testing (`cpu4`, `cpu4x41k`, `smoke`,
  `mixed3`), and a browser-triggered submission runs all of them.
- Dropping `spike` from the group removes the only source of `SpikeRecoveryNS`
  (`score.go` matches `Scenario == "spike"`). Either accept that the metric goes away or
  keep a spike session in the group.

### 3. ~~B4 — stalled-peer harness~~ DONE (2026-08-01), 5/5 — see section B item 4.

### 4. A2 — echo-engine template rendering

No longer just a theoretical cap. Measured 2026-08-01: 4 flows paced at 41k/s each against
the echo delivered ~20k orders/s and left 360,410 orders evicted unanswered
(`framed_requests − framed_responses` matches `evicted_idle` almost exactly), versus ~46k
orders/s against the reference book engine. The echo is now the slowest thing in the loop
and actively blocks load testing, not just capacity claims.

### 5. Contestant-facing feedback and display

- **A submission deduped against ANOTHER contestant's upload is a permanent dead end.**
  Observed 2026-08-01: uploading `deploy-local/smoke-rest-echo.zip` from the browser
  returned HTTP 200 with an existing submission id, and every following
  `GET /submissions/{id}` returned 404, so the page showed "queued" forever. Nothing was
  broken and nothing was built — the build was correctly SKIPPED because that sha256 was
  already built — the caller simply cannot read what it was handed.
  Three individually-sound decisions compose into the trap: `/submit` dedups on sha256
  GLOBALLY (`FindBySHA256`); `GetSubmission` is owner-scoped and returns 404 rather than 403
  when `meta.ContestantID != contestantID` (`handler/submission.go`); and
  `claimOrResolveOwner` (`handler/ownership.go`) only binds an owner when the row is
  UNOWNED, so an already-owned row is handed back unchanged. It also leaks existence: the
  404 exists to hide other users' submissions, yet `/submit` discloses one by returning its
  id. Two contestants submitting the same starter template hit this identically and the
  second is stuck permanently.
  Fix: scope dedup to `(sha256, contestant_id)` so identical bytes from different users
  become separate submissions — that keeps the cheap re-run for your OWN resubmission,
  which is what the dedup comment is actually after. Frontend should also distinguish 404
  from pending so a future mismatch surfaces an error instead of hanging.

- **The run page presents pass-1 and pass-2 correctness as the same kind of number, and
  they are not.** The structure is already right — one page per run-group (one group per
  benchmark trigger), with every scenario session inside it, served by `GetRunGroup`. The
  header already shows the deciding metric: `RunHeader` reads
  `run.score.total_correctness`, which since b0b790c is the pass-1 full-replay score.
  The problem is that `SessionCards` renders each session's `correctness_score` with the
  same word and the same format, including pass-2 sessions graded book-free. An engine
  that fills every order unconditionally therefore renders **1.00 on the constant/ramp
  cards next to 0.62 in the header** (both measured on the echo engine), with nothing on
  the page explaining the difference — a reader would reasonably conclude the header is
  wrong. Wanted: label the header as full-replay correctness and say it decides; mark the
  pass-2 figures as invariants-graded and non-deciding, or stop calling them correctness
  at all (they are closer to "invariant violations observed"); and present the
  `correctness` session as the SOURCE of the header number rather than as a peer of the
  scale scenarios.
  The "Pending" state needs no work — it disappears once every group contains a ramp,
  per the decided run-group shape in section 2.

- `correctness_summary` now carries every violation class and the breakdown sums exactly;
  what remains is surfacing it usefully.
- **HDR chart vs latency timeseries disagree** and both are right: `LatencyTimeline.tsx`
  drops `WARMUP_MS = 2000`, while `histForSession` merges each wave's cumulative
  histogram including warmup. Measured 12.71ms in the HDR against 0.30-0.68ms across the
  visible timeseries. Either exclude the same warmup window or label the HDR as
  whole-session.
- **A3** taint badge — reaches the score event, nothing shows it. Jitter is already on the
  leaderboard row; what is missing is jitter on the RUN DETAIL page. Note jitter exists
  ONLY for pass-2 scenarios: `jitterHistogram` lives in the validator's `invariants.go`
  because P-G jitter measures cross-flow processing-order inversion, and pass 1 runs a
  single connection where TCP sequence totally orders the session — the metric is
  undefined there, not merely unmeasured. `aggregateJitter` then takes the worst value
  across sessions PER PERCENTILE independently, so the published number is a per-percentile
  max rather than any one session's distribution. Consequence of the pass-1-only
  correctness decision: W is calibrated entirely from passes that no longer decide
  correctness, while the penalty W causes now also lands on pass-1 multi-protocol runs,
  where jitter itself is never measured. Nothing shows
  them.
- **`runs` rows are never created for Kafka-triggered sessions.** Only submission-api's
  HTTP `StartBenchmark` INSERTs them; every harness in `deploy-local/` works around it by
  inserting rows itself.

### 6. Live-SSE — mostly observed working; only the concurrent case is open

Downgraded 2026-08-01. The live path was watched working during this session's runs: the
HDR histograms updated in the browser repeatedly WHILE a benchmark was in progress, which
exercises the whole chain — telemetry-ingester → Redis → poll loop → `live_metrics` → SSE
→ frontend render. That was a direct observation, not an assertion, but it is far stronger
evidence than the automated gates provide: B1 asserts on Postgres/Timescale rows and never
touches the UI.

What is still genuinely unverified is the CONCURRENT case: two sessions rendering as two
live tiles at once without cross-contamination. B1 proves the backend keeps two sessions
isolated (separate slots, leases, latency rows, no cross-contestant rows); nobody has
watched two tiles update side by side. That is the only part worth a deliberate test, and
it needs a human looking at the page during a B1 run rather than a script.

### 7. Kafka topology C2-C6

Per-topic sizing, retention by semantics, `benchmark.requested` partitioning, the
partition/band revisit, and the disk/compression plan. Wants a running local cluster to
measure against, and none of it is urgent. C1 is done (see section C).

### 8. Capture throughput — parked until EKS, with instrumentation ready

The capture is NOT CPU-bound at any load this laptop can produce: peak 86% of ONE core
with zero ring-buffer drops and zero CFS throttling. `iicpc-ebpf-late` (the single tokio
pipeline task) dominates at roughly 5x all rdkafka threads combined, so sharding that task
is the lever if and when it is needed. Cost tracks FLOW COUNT and burst shape rather than
record count: 1 -> 4 flows more than doubled peak CPU (33% -> 75%) for 13% more orders,
while records decoded FELL 40% because fuller packets carry more messages each.

Three attempts could not exceed ~46k orders/s delivered — bounded by the single-node veth
wall and by A2 — so the ceiling is an EKS measurement. Any per-record extrapolation from
local runs is invalid; the earlier "~3 cores at >150k delivered" note in
`sandbox-orchestrator/internal/k8s/slot.go` does not reconcile with these numbers and
should be treated as unverified until re-measured there.

### D. EKS-only (unchanged)

Karpenter, Graviton botworker pool, gp3 throughput, the cpuset NodeConfig conflict,
frame-pointer builds, and the final benchmark numbers — last, after everything above.

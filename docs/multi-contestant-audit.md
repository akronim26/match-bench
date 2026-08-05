# Multi-Contestant Audit: Concurrency, Replay, Live Leaderboard, Cost, Validator Ordering

Five goals frame this audit, as stated in the workflow brief:

- **G1** — run concurrent, multi-contestant, multi-protocol benchmarks at max TPS.
- **G2** — per-contestant correctness replay.
- **G3** — live leaderboard; the Redis sink feeding it is suspected to be a dead path.
- **G4** — cost-minimized nodes at scale.
- **G5** — the correctness validator produces false violations from epoll-vs-capture-order mismatch.

Six subsystems were audited with file:line evidence, then 14 of the resulting claims were adversarially re-verified. Corrections from that verification pass are folded into the subsystem text below (each flagged inline). A target-architecture roadmap follows as the final section.

---

## 1. Controller load path (G1): submission-api → bot-fleet-controller → workload.assignments → bot-fleet workers → KEDA

### What exists today

The controller runs sessions strictly serially. `StartBenchmarkRequested`'s fetch/commit loop calls `runner.Run(ctx, req)` synchronously (`services/bot-fleet-controller/internal/controller/consumer.go:89`), and `Run` blocks for the full scenario duration via `time.After(totalDuration)` (`runner.go:208-218`) before the next Kafka message is even fetched. `main.go:83` starts exactly one consumer goroutine; `deployment.yaml:14` sets `replicas: 1`. `benchmark.requested` has 3 partitions (`ops/kafka/create-topics.sh:37`) but they buy nothing behind one blocking consumer.

Downstream state is already concurrency-ready: `SessionManager` is a mutex-guarded per-session map with per-session ready dispatch (`session.go:38-108`, `consumer.go:125`), and Shape A (FIX+REST+WS) round-robins tasks across per-session multi-port sandbox slots (`runner.go:345-379`, `slot.go:141-158`).

`workload.assignments` partition routing is session-agnostic: `workerIndexBalancer.Balance` maps `partition = worker_index % numPartitions` (`producer.go:71-86`), and every session's worker specs restart at index 0 (`runner.go:388-397`). Two concurrent sessions' worker-0 specs therefore land on the same partition. `validateWorkerCapacity` (`producer.go:90-101`) checks only one session's worker count against the partition count — there is no cross-session accounting anywhere.

Each bot-fleet worker pod processes exactly one workload spec end-to-end: `worker.rs:174-215` awaits `run_workload(...)` inline before committing. A second spec queued behind it on a shared partition waits the full run duration, then misses its barrier: the barrier consumer group is per-worker not per-session (`worker.rs:402`, test `worker.rs:2194-2198`), and `wait_for_barrier` commits every barrier message it reads (`kafka.rs:307-342`), so the reused offset is already past the barrier and the worker times out after `BARRIER_WAIT=120s` (`worker.rs:48`), silently dropping the workload.

KEDA scales `bot-fleet-worker` 2..50 on `workload.assignments` lag (threshold 1), but only 24 partitions can be consumed — effective global concurrency ceiling is 24 worker-specs across *all* sessions regardless of replica count (`scaledobject.yaml:16-27` vs `ops/kafka/create-topics.sh:39`). KEDA pre-scale also races a 30s `READY_DEADLINE`, and `awaitReady` proceeds on partial fan-in (`runner.go:317-327`), silently under-driving a contestant's offered TPS.

### Corrections from verification

The concurrent-session claim, the routing-collision claim, the missing-cross-session-accounting claim, and the one-spec-at-a-time claim were all **CONFIRMED** with no material correction. One nuance surfaced: `validateWorkerCapacity` does block same-session partition oversubscription at publish time — the unguarded failure mode is specifically *concurrent sessions*, where session A's worker 0 and session B's worker 0 both map to partition 0 and neither the capacity guard nor KEDA prevents the resulting missed-barrier drop. This sharpens rather than weakens the gap: the platform's core promise (concurrent contestants) is exactly the scenario the current code has no defense for.

### Gaps

| Goal | Gap | Evidence |
|---|---|---|
| G1 | No session-level concurrency in the controller — `runner.Run` blocks the whole loop for one session's duration | `consumer.go:89`, `runner.go:208-218`, `deployment.yaml` replicas:1 |
| G1 | Partition routing collides across sessions (`worker_index % partitions`, no session term) | `producer.go:71-86`, `worker.rs:174-215` |
| G1 | No global partition/slot budget or admission control across sessions | `producer.go:90-101` |
| G1 | KEDA scale-up is reactive and races the ready deadline; partial fan-in silently proceeds | `runner.go:317-327`, `producer.go:142-146` |
| G1 | `maxReplicaCount=50` exceeds the 24-partition consumption ceiling; conversely 24 partitions cap total concurrent worker-specs platform-wide | `scaledobject.yaml:17` vs `create-topics.sh:39` |
| G4 | `minReplicaCount=2` keeps workers warm at all times with no scale-to-zero-with-pre-scale hook | `scaledobject.yaml:16` |

### Proposals

1. **Concurrent session dispatch** — dispatch `runner.Run` into a per-session goroutine behind a `MAX_CONCURRENT_SESSIONS` semaphore, commit `benchmark.requested` offset on admission. `SessionManager` needs no change. ~100 LoC. Keep `replicas: 1` until partition leasing (below) exists — scaling replicas first turns "blocked" into "second session fails at ReadyDeadline."
2. **Partition leasing replaces modulo routing** (breaking) — a lease allocator (Postgres table, or controller-local bitmap while replicas=1) hands out free partitions per session on admission, stamps each spec's Kafka message explicitly, releases on completion. Admission blocks in a bounded queue when free partitions run out — this *is* the global capacity check `validateWorkerCapacity` never was. Also fix the barrier group to be per-(session, worker), and stop `wait_for_barrier` committing offsets past unmatched barriers.
3. **Pre-scale gate + hard partial-ready failure** — check `readyReplicas` against leased-partition demand before publishing specs, with its own deadline; make partial fan-in a hard failure by default (`PARTIAL_READY_POLICY=fail`) since a silently under-driven benchmark is an integrity failure, not a degraded mode.
4. **Scheduler/admission layer** — only if demand becomes bursty: a thin Postgres-backed QUEUED-status queue in front of `benchmark.requested`, drained at the rate free partitions + free sandbox capacity allow. New component, deferred.

---

## 2. Sandbox slots + eBPF capture multi-tenancy (G1, G4)

### What exists today

Sandbox slots are one pod + Service per contestant, multi-port capable, Guaranteed QoS (2 CPU / 1Gi), readiness gated on port-0 probe plus TCP-dial of remaining ports (`slot.go:141-177,369-456,678-687`). eBPF capture is one privileged, hostPID Job per slot, node-pinned to the algo pod's node, owner-ref'd and garbage-collected (`slot.go:533-655`). Capture attaches *inside* the target pod's own network namespace via `/proc/<pid>/ns/net` (resolved by scanning `/proc/*/cgroup` for pod UID + container ID), so fixed capture ports 9898/8080 never collide across contestants — per-contestant capture isolation is inherent (`ebpf-latency/src/netns.rs:14-47`, `main.rs:416-424,501-531`). Captured `orders.acked` events are co-partitioned by `order_id` with session-prefixed order IDs, so N concurrent contestants share the topic without key collisions (`main.rs:278-313`, `bot-fleet/src/telemetry.rs:332`).

Capture resources are Burstable, request 200m / limit 4 CPU — the limit was raised from 2 to 4 after CFS throttling at 2 cores caused ring-buffer drops above ~150k acked/s (`slot.go:659-674`).

Current topology hard-caps concurrency at ~1 contestant: exactly one sandbox node (c6i.2xlarge, min=max=desired=1, tainted), and the EKS bench path even runs with `CAPTURE_ENABLED=false` (`bench.tfvars`, `deploy-bench/eks-up.sh:95`). cpuset static pinning is disabled on EKS entirely — `reservedSystemCPUs` conflicts with EKS default kube/system-reserved and blocks node join (`main.tf:173-198`, `variables.tf:206-215`). There is no capacity admission control anywhere in the slot path — `CreateSlot` validates ports/IDs only and creates pods unconditionally; excess requests surface only as `StateCreating` until a 3600s deadline (`handler/slot.go:64-138`, `k8s/slot.go:181-202,691-720`).

### Gaps

| Goal | Gap | Evidence |
|---|---|---|
| G1 | Sandbox pool fixed at 1 node, no admission control: a hot contestant uses ~6 of 8 vCPU (2 algo + up to 4 capture); two hot contestants co-scheduled reproduce the exact CFS-throttle→ringbuf-drop failure the CPU-limit bump fixed | `bench.tfvars`, `slot.go:659-674`, `handler/slot.go:64-138` |
| G1/G4 | Capture's 200m request vs ~3-4 core real demand makes the scheduler blind to actual packing math; one-slot-per-node currently holds only by accident of pool sizing | `slot.go:660-673` |
| G1 | `ensureCapture` pins to the algo pod's node with no headroom check | `slot.go:545-558` |
| G4 | cpuset pinning (contest-fairness) dead on EKS until the NodeConfig conflict is resolved | `main.tf:173-198`, `variables.tf:206-215` |
| G4 | No per-contestant cost story; capture must colocate with the algo pod (hostPID + same-node netns entry), so sandbox nodes always carry both workloads — cannot move capture to cheaper Graviton botworker economics | `netns.rs:14-16`, `slot.go:545-548` |
| G1 | Fixed 24 `orders.acked` partitions shared by all contestants; at N contestants the shared topic/2-broker tier becomes the aggregate bottleneck before capture does | `main.rs:142-143,111` |

### Proposals

1. **1-slot-per-node contract, made explicit** — `requiredDuringScheduling` podAntiAffinity on the algo label, or raise the capture Job's CPU request to its real ~3-core demand so scheduler math reflects reality. Small change; removes the silent-throttle failure mode as the pool grows.
2. **Autoscaled sandbox pool, N-for-N** — min=0/max=N via cluster-autoscaler or Karpenter on the tainted `pool=sandbox` group; keep c6i.2xlarge as the unit (proven fidelity floor). Add a 429+retry-after admission check in `handler/slot.go` when pending slots exceed pool capacity. Spot is explicitly **not** recommended here — mid-run reclaim kills a contestant's benchmark, unlike botworkers.
3. **Graviton for botworkers only, sandbox stays x86** — capture's baked BPF object needs an arm64 CO-RE build before Graviton sandbox is safe; defer that, ship Graviton/Spot on botworker now since it dominates node count at scale.
4. **Per-contestant Kafka budget for `orders.acked`** — size partitions ≥ 24×⌈N/2⌉ and brokers to the measured per-contestant ~150k/s ≈ 50MB/s; captures already read partition count at startup so a resize needs only a capture restart, which slot lifecycle already gives per run.
5. **Fix the cpuset NodeConfig conflict** — replace `reservedSystemCPUs` with `kubeReserved`/`systemReserved` quantities (not mutually exclusive with `cpuManagerPolicy: static`), validate on a throwaway node. Config-only; unblocks contest-fairness pinning.

---

## 3. Telemetry + Kafka scale (G1, G3)

### What exists today

Bot-fleet has exactly one telemetry aggregator task per worker pod; `record()` is lossless backpressure — it blocks when the channel is full, so telemetry drain rate directly throttles order generation for that contestant (`bot-fleet/src/telemetry.rs:61-72,93-101`). The aggregator drains in 4096-slices, batches per destination partition (max 1000 events/batch), and encodes with `rmp_serde::to_vec_named` (self-describing msgpack, field names repeated per event) (`telemetry.rs:128,229,317`).

Both `orders.sent` and `orders.acked` co-partition by FNV-1a hash of `order_id` — a single busy session spreads across all 24 partitions by design, so every ingester replica processes every session (`schemas/rust/src/lib.rs:41-52`, spread test `telemetry.rs:391-402`). Topics: 24 partitions, RF=3, min.insync=2, retention 6h, 1MiB max message, no compression configured anywhere (`k8s/data/kafka/topic-init-job.yaml:42-59`).

Ingester replicas form one consumer group subscribed to both topics; correct sent/acked joining depends on librdkafka's default assignor giving the same partition number of both topics to the same replica — no explicit strategy is set (`kafka.rs:13-28`). Per-session isolation *within* a replica is by key, not ownership: aggregator windows key on `(session_id, wave)`, with wave derived deterministically from `barrier_epoch_ns` (consumer-independent, tested) (`aggregate.rs:176-205,449-471`). A 2-stage rollup merges per-shard partial HDR rows into canonical metrics via idempotent UPSERT, sealed after a 3s lag (`rollup.rs:20-30,145-245`).

**G3 sink status**: the ingester's `RedisSink` *is* called — `flush()` HSETs `contestant:{contestant}:{session}:{wave}` hashes every snapshot tick (`ingester.rs:130`, `redis_sink.rs:60-65`). But nothing reads them: leaderboard-api's only Redis usage is a 2s JSON cache over its own Postgres queries (`leaderboard-api/internal/read/cache.go:41-69`); a repo-wide grep for `contestant:` and for HGET/HGETALL hits only `redis_sink.rs` and its own integration test. Writes are per-row awaited HSETs with no pipelining and no TTL, and rows with empty `contestant_id` are silently skipped.

The ingester's main loop is single-task: one `select!` interleaves Kafka recv, snapshot flush, and both TimescaleDB and Redis writes inline — a slow DB write stalls consumption for all sessions (`ingester.rs:48-71,108-137`).

### Corrections from verification

Several claims needed correction after adversarial checking:

- **"Never called" is wrong.** `RedisSink::connect` runs unconditionally at startup (a connect failure aborts the whole service) and `redis.write(&snaps)` fires on every flush tick. The accurate framing: the sink's writes are write-only — real data lands in Redis every second, but has **zero production readers**. The only reader anywhere in the repo is the ingester's own integration test (`tests/integration.rs:182,246,499`), a write-verification self-test, not a consumer.
- **The empty-`contestant_id` skip is not at `redis_sink.rs:36`.** `Aggregator::snapshot()` (`aggregate.rs:291`) gates emission on both non-empty `service_time` *and* non-empty `contestant_id`; since `service_time` samples are only recorded in `observe_acked`, a pre-ack window never produces a `Snapshot` row at all — `redis_sink.rs:36` is unreachable defense-in-depth. The real consequence is worse than "skipped": sent-side counters (offered, timed-out) for the interval before a session's first ack are accumulated then unconditionally reset at `aggregate.rs:319-324` with no row ever emitted — that interval's data is **permanently dropped** from both Timescale and Redis, not deferred. Fix belongs in `aggregate.rs` (carry `contestant_id` in the sent-event envelope, or defer the reset), not in `redis_sink.rs`.
- **The wire-bloat factor is 1.84×, not 2-3×.** Measured directly with the repo's own `full_chunk_stays_under_broker_message_ceiling` fixture: a 1000-event named-msgpack chunk is 383,934 bytes vs 208,906 bytes tuple-encoded. Any capacity math should use 1.84×, and 3× is arithmetically impossible for this schema (fixed per-event overhead of 175 bytes against ~207-382 byte events).
- **The assignor-reliance claim is confirmed but underspecified.** librdkafka's default is the *list* `"range,roundrobin"`, not "range" alone — range wins only while all group members run identical config; a mixed-config member could flip the group to roundrobin, which does *not* preserve cross-topic co-assignment, making the reliance more fragile than originally stated. Currently moot at single-replica deployment, but a latent hazard exactly at the point of scaling for G1/G3.

### Gaps

| Goal | Gap | Evidence |
|---|---|---|
| G1 | Single aggregator + single producer per bot-worker is the telemetry drain ceiling; lossless backpressure means telemetry saturation throttles order generation | `telemetry.rs:63,97` |
| G1 | Named-msgpack wire bloat is 1.84× (not 2-3×) — real but smaller multiplier against the 445k/s durable Kafka ceiling | `telemetry.rs:229`, measured |
| G1 | N sessions share all 24 partitions with no session-awareness; one hot contestant delays every replica's snapshot cadence | `lib.rs:41-52`, `telemetry.rs:391-402` |
| G1 | Sent/acked join correctness across replicas depends implicitly on the assignor default; no static membership, no checkpointed join state | `kafka.rs:13-28`, `ingester.rs:36` |
| G1 | One message per `select!` wake, flush inline — head-of-line blocking becomes the replica ceiling before CPU does | `ingester.rs:48-71,108-137` |
| G3 | Live leaderboard has no production Redis reader; `leaderboard.updates` topic exists but the ingester never produces to it | `redis_sink.rs:60-65` vs `cache.go:41-69` |
| G3 | Redis writes unpipelined, no TTL, unbounded key growth; early-session waves silently dropped (corrected above: dropped permanently, at `aggregate.rs:319-324`, not merely deferred) | `redis_sink.rs:33-54` |
| G4 | 445k events/s single-broker durable ceiling (RF=3/min.insync=2, default gp3 125MB/s); no compression configured anywhere | `topic-init-job.yaml:42-45` |

### Proposals

1. **P5: shard the bot-worker aggregator** into M≈4 shards keyed by `partition_for(order_id) % M`, each with its own channel/batcher/producer — multiplies drain throughput ~M× while preserving per-shard lossless backpressure. Already tracked as P5 in `docs/tps-improvement-plan.md:170-173`. ~1-2 days.
2. **Compact wire format + compression** — tuple encoding (`rmp_serde::to_vec`) behind a batch-version byte, hoist constant fields (`session_id`, `submission_id`, `worker_id`, `barrier_epoch_ns`) into the batch envelope, `compression.type=lz4`. Measured 1.84× from encoding alone, more with lz4; raises `MAX_EVENTS_PER_BATCH` toward the 1MiB ceiling. Cross-language schema change (Rust + Go + validator decode), one coordinated deploy.
3. **Session-affine partition bands** — `base = FNV(session_id) % P_total`, hash within a per-session band. Preserves the sent/acked co-partition contract; ingester replicas then own disjoint session sets and scale with session count. Breaking, versioned contract (also feeds correctness-validator's replay scan, Section 4).
4. **Harden the ingester consumer group** — explicit `assign()` per replica from `(ordinal, replica_count)` via StatefulSet + `group.instance.id`; disable auto-commit, commit after each snapshot flush.
5. **Decouple consume from flush** — split into a consume task (bounded channel, batch recv) and a flush task; pipeline Redis writes, add a TTL (~24h) on `contestant:*` keys.
6. **Close the G3 loop** — either (a) have ingester `flush()` additionally produce a compact snapshot to the already-declared `leaderboard.updates` topic and consume it in leaderboard-api's existing SSE path (preferred, no new infra), or (b) have leaderboard-api read the existing `contestant:*` hashes directly. Either way, fix the contestant_id-carry bug so wave 0 stops being dropped.
7. **Kafka data-plane sizing** — keep per-broker leader throughput under ~60% of the re-measured (post-compression) ceiling; raise `orders.*` partitions to 8×N_max_sessions; provision gp3 at ≥500MB/s/16k IOPS explicitly (125MB/s default is the suspected wall); keep RF=3/min.insync=2 for `orders.*` since replay is correctness evidence (G2) — do not trade durability there.

---

## 4. Frontend + leaderboard + Redis (G3)

### What exists today

A live-leaderboard push path already exists end-to-end via Kafka+SSE, *not* Redis: score-computer publishes `LeaderboardUpdateEvent` to `leaderboard.updates` (`score-computer/internal/worker/worker.go:90-105`, `internal/publisher/kafka.go:30`); leaderboard-api consumes it and broadcasts over an SSE broker at `/api/events` (`leaderboard-api/internal/consumer/consumer.go:40,61`, `main.go:96`); the frontend subscribes via `EventSource` and patches the react-query cache with a 400ms row flash (`frontend/src/hooks/useSSE.ts:63-72`, `useLeaderboard.ts:80-103`). SSE clients get an initial snapshot (limit=100) then per-run update events; slow clients (16-message channel buffer) are dropped (`sse/broker.go:47-56,71,86-94`).

Update cadence today: SSE `update` events fire only when score-computer finishes scoring a whole run-group — once per contestant run, at the end. There is no mid-run leaderboard event (`score-computer/main.go:63-65`, `worker.go:55-105`).

The write side of the Redis story is live (Section 3), but as established there, nothing reads it. A second, independent dead design also exists: score-computer's Redis client defines `ZScore`/`ZAdd`/`ZRevRank` against `LEADERBOARD_KEY=leaderboard:global`, but none of it is invoked — `worker.score()` computes rank from Postgres (`RankForRunGroup`), Redis is only `Ping`'d for readiness, and `rankDelta` is hardcoded to `0` (`score-computer/internal/redis/redis.go:49-80`, `worker.go:82-87`, `config.go:42`).

The frontend read path is react-query GET `/api/leaderboard` with an optional scenario filter, resolved via an EXISTS join to runs+scenarios (no schema change, matching the uncommitted light-theme redesign) (`frontend/src/api/leaderboard.ts:15-29`, `leaderboard-api/internal/read/store.go:89-91,144-147`). Mid-run metrics for polling already exist too: the ingester writes per-(session,wave) snapshots to TimescaleDB every flush, served at `/api/charts/{session_id}` and `/api/live` (`ingester.rs:122-128`, `leaderboard-api/main.go:92-94`, `store.go:216-218,413-432`).

### Gaps

| Goal | Gap | Evidence |
|---|---|---|
| G3 | No live in-run leaderboard: SSE only carries final per-run-group scores; the per-wave Redis snapshots are orphaned (no reader), so nothing shows contestants moving during a run | `redis_sink.rs` write-only; `consumer.go:40`; `worker.go` publishes once per run-group |
| G3 | `rank_delta` is always 0, so the frontend's rank-movement affordance is inert | `worker.go:86`, `useLeaderboard.ts:41` |
| G3 | SSE update patches ignore the scenario filter: an update applies to whatever cache is active regardless of tab match, and appended rows bypass server-side rank ordering/limit | `useLeaderboard.ts:44-50` |
| G3 | Dead code invites confusion: score-computer's abandoned ZSET leaderboard design coexists with the Postgres-authoritative one | `redis.go:47-80`, `config.go:42` |
| G3 | 2s response cache means final-score polling can briefly disagree with the uncached SSE reader | `main.go:59` vs `:60-62` |

### Proposals

1. **Wire the orphaned Redis snapshots into a mid-run 'live runs' SSE stream** — ingester additionally writes a `live:{session_id}:latest` pointer key so readers never guess wave index; leaderboard-api runs a 1s loop reading latest snapshots for `ActiveRuns` sessions and broadcasts a new `live_metrics` SSE event on the existing broker. No new service, no new topic — reuses the existing write path and closes the dead sink. ~1-2 days.
2. **Fix final-score push client correctness** — add `scenario` to `LeaderboardUpdateEvent`; client drops updates not matching the active tab, re-sorts/truncates after patching; compute `rank_delta` from `RankForRunGroup` before/after `SaveScore` instead of the hardcoded 0. ~0.5-1 day.
3. **Delete the dead score-computer ZSET path** — remove `ZAdd`/`ZScore`/`ZRevRank`/`LEADERBOARD_KEY`, document that Postgres is ranking-authoritative and Redis is (a) ingester live snapshots and (b) leaderboard-api's response cache. ~1 hour + doc.
4. **Optional: mid-run provisional score ticker** — publish `partial=true` `LeaderboardUpdateEvent`s from `score_progress` rows (per-scenario partial correctness already lands there) as they arrive; render dimmed on the frontend. Worth it only for runs >1-2 min.

Latency budget for the mid-run stream: order → capture → ingester flush (1s) → Redis → 1s poll → SSE ≈ 2-3s worst case; final scores stay event-driven at run completion.

---

## 5. correctness-validator: per-contestant replay (G2) and epoll-vs-capture-order false violations (G5)

### What exists today

The validator's replay order is a strict total order — `(EffectiveT3, Flow, TCPSeq)` — applied identically by both the batch path (`replay/order.go:42-51`) and the streaming path's `Reorderer` (`source/reorder.go:61`, doc at `:10-11`). `EffectiveT3` is a per-flow head-of-line promotion of T3 (kernel-capture ingress time from eBPF): within a flow, sort by TCPSeq and take a running max of `T3Ns`. Cross-flow interleaving is decided purely by these capture timestamps, not by anything the contestant process did (`order.go:23-38`, `source/reorder.go:78-96`, `model/order.go:97`).

Ordering enters scoring at exactly two places: the reference book consumes orders in replay order, so cross-connection reorder changes which reference fills exist (`internal/book/book.go:181`, `validate/stream.go:50-52`); and queue-jump detection compares engine arrival sequences to emit Time/CancelReplaceLoss violations (`validate/stream.go:175-200`, `validate/validate.go:219-228`).

The only epoll-order forgiveness anywhere is `CrossFlowTie` with `TieToleranceNs = 100` — orders on different flows within 100ns aren't counted as queue-jumpers (`replay/order.go:14,71-76`). 100ns is orders of magnitude below real epoll cross-connection reordering (µs-ms), so it excuses essentially nothing.

By violation type: Time and CancelReplaceLoss are directly ordering-derived. All three Price branches (`r==nil`, price-not-produced, `cum > ref.qty`) depend on reference-book state built in replay order and are therefore epoll-sensitive (`validate/stream.go:133-169`). Overfill (own-qty only) and Phantom (existence only) are ordering-insensitive.

`AGGRESSIVE_FILL_TOLERANCE_US` is wired only into the batch path (`validate.Run`) and is a no-op in the production streaming path: `StreamValidator.scoreOrder` never consults `AggressiveFillToleranceNs`, and `main.go` uses only the streaming validator (`validate/validate.go:117-137,286`, `validate/stream.go:123-171`, `main.go:205-217`).

> **Update (2026-07-31).** Resolved by deletion. The batch path and the tolerance knob are both gone; grading is strict. Pass 1 runs one task on one connection, so it has no cross-flow interleaving to forgive, and pass 2 is book-free. See `docs/remaining-work.md` §"Validator: the batch path is deleted".

Per-session replay (G2) works by full-topic scan + in-decoder filter: `StreamSession` opens a reader per partition of *both* `orders.sent` and `orders.acked`, discarding batches whose SessionID mismatches after decode (`source/stream.go:86-122,288,298`). Session start offset is pruned only by decoding a UUIDv7 timestamp from the session ID minus 60s margin (`source/drain.go:269-299`); non-UUIDv7 IDs fall back to reading each partition from `FirstOffset`. `VALIDATOR_CONCURRENCY` (default 4) runs that many parallel full-topic scans, so broker read amplification is O(N × window) and per-session memory is a 1M-order reorder window (`DefaultReorderWindow = 1<<20`) plus join buffer plus live book (`main.go:59,90-92`, `source/stream.go:29-30,117-122`). ContestantID is derived from the first ack event seen in the session, giving per-contestant replay for free as long as sessions are 1:1 with contestants (`stream.go:192-194`, `main.go:223-255`). The batch path (`DrainSession` + `validate.Run`) still exists but is memory-unbounded — the path that previously OOMed; streaming replaced it in production.

### Corrections from verification

- **SelfTrade is also epoll-order-sensitive** — the audit's original claim listed only Overfill/Phantom as ordering-insensitive alongside a claim that all three Price branches were the sensitive set. Verification found `r.selfPrices` (used by the SelfTrade branch, `stream.go:139-142`) is populated from the *reference* engine's trades, gated on same-participant maker/taker during EffectiveT3-order replay (`stream.go:63-68`). A divergent processing order can create or destroy a reference self-trade, flipping a fill's classification. **Corrected set: only Overfill and Phantom are ordering-insensitive; Time, CancelReplaceLoss, all three Price branches, and SelfTrade are all epoll-order-sensitive.**
- **A second (also-dead) tolerance mechanism exists.** `CrossFlowTie`'s 100ns is confirmed as the only forgiveness on the *production* path, but `AggressiveFillToleranceNs` is a second, independent cross-connection forgiveness mechanism defined in the batch code (`validate.go:123-137`, waives Price violations when opposite-side liquidity from a different participant overlapped within tolerance) — it is simply unreachable in production, same root cause as above. If either the batch path or the env knob is ever reactivated, restated correctly: the claim of "100ns is the *only* forgiveness" becomes false without any streaming-code change.
- **The epoll-vs-capture-order mismatch itself, and its consequence (stock-socket engines score ~45%), were CONFIRMED** with one wording correction: the mechanism is not epoll-specific — the C++ reference/contestant engine uses per-connection reader threads, not epoll, and hits the identical cross-connection nondeterminism. Within a single TCP connection there is no mismatch (TCPSeq ordering agrees); false violations are strictly cross-connection.

### Gaps

| Goal | Gap | Evidence |
|---|---|---|
| G5 | Strict cross-connection total order asserts an order the contestant never saw; only a 100ns tie window forgives it, essentially nothing | `order.go:14,42-51`, `validate/stream.go:133-200` |
| G5 | Streaming path has no tolerance knob at all — `AggressiveFillToleranceNs` exists only in the dead batch path | `stream.go:123-171` vs `validate.go:117-137` |
| G2 | N concurrent sessions cause N full re-scans of both topics; no session/contestant partition keying or shared demux | `stream.go:117-122,288,298`, `main.go:59` |
| G2 | Offset pruning depends on UUIDv7 session IDs; any other format falls back to unbounded `FirstOffset` scan | `drain.go:279-299` |

### Proposals

1. **P-A (cheapest): widen the cross-flow tie tolerance** to an epoll-scale env-configurable window (e.g. `EPOLL_TIE_TOLERANCE_US`, 200-1000µs). Hours of work; immediately collapses false Time/CancelReplaceLoss. Does not fix Price/SelfTrade (reference-book divergence untouched); too wide a window also excuses real cross-flow queue jumps.
2. **P-B (recommended): a processing-order oracle from T7 ack egress.** Define `ProcessedTS(order) = min T7` over its responses — an upper bound on when the contestant finished ingesting it (T7 is already captured per response). Replay cross-flow by `(ProcessedTS, Flow, TCPSeq)`, keeping per-flow TCPSeq as a hard invariant via the same monotone-promotion machinery already used for EffectiveT3. This is the only proposal that fixes all four ordering-sensitive violation classes (Time, CancelReplaceLoss, Price, SelfTrade) at once, because it corrects the replay order itself rather than forgiving individual symptoms. Anti-gaming: reject `ProcessedTS < T3` (impossible), cap `ProcessedTS - T3`; incentives already oppose ack-delaying since latency scoring uses the same t7. ~1-2 days.
3. **P-C (strongest, most expensive): partial-order / best-legal-schedule validation.** Treat same-flow TCPSeq as authoritative; cross-flow orders within an epoll window form equivalence classes; on a would-be violation, bounded local search over legal linear extensions, accept if any produces the observed fill. Zero false positives by construction and no trust in contestant timestamps, but requires book-engine snapshot/rewind surgery and worst-case CPU blowups on hot price levels. 1-2 weeks; keep behind a flag as an appeals backstop.
4. **P-D (protocol option): contestant-reported ingest sequence** — echo a monotone ingest seq per order, cross-check against TCP order and T3 bound. Ground truth but requires a schema change across bot-worker templates, every contestant engine, and eBPF parse. v2 material.
5. **P-E (G2): single-pass session demux** — replace per-session `StreamSession` invocations with one shared drain per (topic,partition) window that demuxes decoded batches by SessionID into per-session bounded lanes, each feeding its own unchanged Reorderer+StreamValidator. Cheaper alternative: key `orders.sent`/`orders.acked` producers by `session_id` so a session maps to a known partition subset (ties into Section 3's partition-band proposal).

6. **P-F (chosen): two-pass validation — single-connection correctness gate, then
   T7-order multi-connection replay.**

   True cross-connection arrival order is unknowable without a serialization point —
   even a perfect engine cannot recover it. Within the simultaneity window no "correct"
   order exists: the engine is the matching venue, not a trader with P&L, so its
   serialization choice among genuinely simultaneous orders is as legitimate as any.
   That reframes the problem: prove the matching logic honest where order is
   unambiguous, then only check *consistency* where order is inherently ambiguous.

   **Pass 1 — single-connection saturating correctness run.** One task, one connection,
   max-rate mode (no pacer: `target_rps=0` sentinel makes the existing catch-up loop
   send back-to-back, saturating one core; `BOT_WRITE_BATCH` coalescing is fine because
   TCPSeq gives the exact total order on a single flow — t3 granularity is irrelevant
   for sequence). Total order is unambiguous, replay needs zero trust, every violation
   class is meaningful. This is the graded correctness score.

   **Pass 2 — multi-connection run, replay in min-T7 order** (the P-B machinery, now
   justified by the gate). `ProcessedTS(order) = min T7` over its responses — a kernel
   egress timestamp the contestant cannot forge; only the *ordering* of acks is
   contestant-influenced, and pass 1 has already proven the matching logic honest.
   Hard checks that remain trust-free in pass 2: per-flow TCPSeq as an inviolable
   invariant, `T7 ≥ T3` and a capped `T7 − T3` window as anti-gaming bounds, and the
   ordering-insensitive violation classes (Overfill, per-flow Time) — which
   also catch cross-connection *concurrency bugs* (lock races, lost updates, torn
   state) that a single-connection run cannot exercise. An engine with a real
   concurrency bug fails pass 2 because its fills are inconsistent with any
   self-consistent serialization, including its own ack order.

   Why the gate fixes P-B's circularity: standalone P-B grades an engine against its
   own chosen order — a malicious engine could pick favorable orders and always look
   consistent. Gated P-B only extends trust *after* price-time-priority compliance is
   proven under unambiguous ordering, and the residual freedom (bias within the
   simultaneity window) confers no advantage to an engine that has no position in the
   market it matches.

7. **P-G: ordering jitter as a graded metric.** Real venues guarantee only per-session
   FIFO; cross-session ordering is a measured jitter property, and the market ranks
   venues on how small and flat that window is (deterministic/FPGA gateways exist to
   compress it; CME's "gateway roulette" era is the cautionary tale). Contestant
   engines can be scored the same way, from data already captured:

   - **Arrival order** = t3 (kernel ingress timestamp at the pod NIC, eBPF —
     contestant-untouchable). **Effective processing order** = sequence of first
     responses, min t7 per order (also kernel-stamped; the sequence is the engine's
     market-visible serialization).
   - For each cross-flow pair processed in inverted arrival order, the inversion
     magnitude is the pair's t3 gap. **Engine jitter = the distribution of inversion
     magnitudes** (p50/p99/max in µs + inversion rate). An ingress-timestamp-ordered
     engine scores near zero; a naive thread-per-connection engine shows ms tails.
   - Compute in the pass-2 book-free validator: it already consumes both streams
     joined by order_id in a reorder window — sliding window sorted by t3, compare t3
     rank vs t7 rank, emit inversion magnitudes into an HDR histogram per
     (session, wave) → existing rollup → TimescaleDB → one new frontend chart beside
     match-latency. No new components.
   - Honesty: both timestamps ours; gaming by delaying acks to fake ingress ordering
     costs the latency score 1:1, and pass 1 has already proven the matching logic.
     Egress-path noise adds a small equal-for-everyone floor. Per-flow FIFO breaches
     remain violations, never jitter.
   - This also resolves how to pick W: keep a modest fixed W for pass/fail priority
     grading, and rank engines on the jitter distribution itself — engines are not
     just forgiven inside the window, they are scored on how little of it they use.
     Leaderboard becomes: correctness %, latency p99, throughput, **jitter p99**.

**Violation coverage matrix (ACCEPTED DESIGN):**

| Violation | Definition | Pass 1 (single-conn, full book replay) | Pass 2 (full-scale, book-free) |
|---|---|---|---|
| Overfill | fills exceed order qty | exact | exact (own qty only) |
| Lost order/cancel | arrived on TCP, never acted on | exact | exact (per-order accounting) |
| Time (per-flow) | same-connection FIFO breach | exact | exact (TCPSeq per flow) |
| Time (cross-flow) | queue-jump across connections | exact (single flow — moot) | vs t3 within published window W |
| CancelReplaceLoss | cancel/replace vs fill precedence wrong | exact | per-flow exact; cross-flow under W |
| Price (r==nil / price-not-produced / cum>ref.qty) | fill impossible per reference book | exact | not checked — pass 1 owns matching logic |
| SelfTrade | matched participant against own resting order | exact | not checked — same |
| Latency (t7−t3), TPS | graded metrics | measured, not graded | graded |
| Jitter (P-G) | t3-gap distribution of cross-flow inversions | ~zero by construction | graded, 4th leaderboard metric |

Phantom-fill is REMOVED as a scored violation class (decision 2026-07-16): every acked
event is joined against `orders.sent` by order_id before it ever reaches scoring, so a
fabricated fill cannot enter the fill set — it fails the join and is already surfaced
by the `unmatched_responses` counter. Scoring it again was double-counting the join.

Rationale for the drop-list: Price/SelfTrade test matching *logic*, identical code under 1
or 1000 connections — pass 1 exercises it exactly under max pressure. Multi-connection
load adds *concurrency* failure modes (drops, duplicates, overfills, per-flow misorder),
all of which pass 2's book-free checks catch at full strength for the full duration —
including sustained-load degradation (the t=34s class).

Recommendation (final): **P-A immediately as the interim knob; P-F two-pass as the
structural fix (pass 1: short single-connection saturating run, full book replay, the
graded correctness score; pass 2: full-duration multi-connection run, book-free
invariant checks — per-flow FIFO, Overfill, lost orders — plus cross-flow
priority vs t3 with published window W); P-G jitter as the fourth graded metric,
computed in pass 2; P-C behind a flag as the appeals path.** Standalone P-B (ungated)
remains withdrawn; the expensive reference-book replay is deleted from full-scale runs
entirely rather than sampled (a first-N-seconds sample would have missed the known
t=34s engine-degradation class).

---

## 6. Infra + cost audit (G4): k8s manifests, KEDA, node pools, Kafka topology, gp3

### What exists today

The checked-in 2M/s bench tier is 8 fixed-size on-demand x86 nodes (44 vCPU): 2× m6i.2xlarge general, 2× m6i.xlarge Kafka, 1× c6i.2xlarge sandbox, 3× c6i.xlarge botworker (`bench/bench.tfvars:1-49`). All pools are `AL2023_x86_64_STANDARD`, on-demand, min=max=desired (`infra/terraform/main.tf:122-260`). KEDA scales `bot-fleet-worker` 2..50 on `workload.assignments` lag, threshold 1 (`addons.tf:65-70`, `scaledobject.yaml:16-27`) — but `deployment.yaml` also hardcodes `replicas: 5` in the same file KEDA manages, fighting for ownership on every apply, and sized resources (1 CPU req / 3 CPU limit, 6Gi mem limit) can double-pack on a 4-vCPU node.

Kafka topic ownership is split three ways: the checked-in `topic-init-job.yaml` creates 24-partition topics at RF=3/min.insync=2; `bench/kafka-bench.sh` seds the same job to 96 partitions RF=1 and rewrites the StatefulSet to 2 replicas; the checked-in StatefulSet itself says `replicas: 3` with a hardcoded 3-voter quorum. Consumers hardcode conflicting partition-count defaults (`ebpf-latency` defaults to 24; `kafka-bench.sh` imperatively sets 96 via `kubectl set env`). gp3 sizing similarly diverges across three definitions.

`bot-fleet-worker` pods need a privileged netshoot init-container for MTU/offload tuning — but this exists because sandbox-side eBPF capture truncates frames at 1536B; it does not apply to botworker nodes themselves, so nothing architecturally blocks ARM/Graviton for that pool.

Measured botworker capacity is ~64k orders/s per c6i.xlarge node in the last EKS run — pre catch-up-pacing fix; `bench.tfvars` claims ~800k/s per node, a 12× spread, unvalidated on EKS since the pacing fix landed.

### Corrections from verification

None of this subsystem's specific claims were in the 14-item adversarial verification pass; they stand as audited, carrying the explicit unvalidated-measurement caveat noted above (the 64k vs 800k/s spread) as the dominant unknown for any cost sizing.

### Gaps

| Goal | Gap | Evidence |
|---|---|---|
| G4 | KEDA scales pods 2..50 but the botworker node group is fixed-size — no cluster-autoscaler/Karpenter exists; beyond ~3 pods per pool, extra replicas are Pending | `scaledobject.yaml:16-17` vs `bench.tfvars:43-45`; `addons.tf` has only KEDA |
| G4 | `lagThreshold=1` on `workload.assignments` scales on a start-of-run pulse, not sustained load — the fleet cools down and scales in mid-run | `scaledobject.yaml:19,26` |
| G4 | Everything on-demand; botworker load-gen (stateless, re-runnable) is the ideal Spot workload and the largest scaling cost term, and it's untouched | no `capacity_type=SPOT` anywhere in `main.tf` |
| G4 | Dual/triple topic ownership: checked-in 24-partition/RF=3 topic-init is incompatible with the checked-in 2-broker pool (RF=3 topic creation fails with 2 brokers) | `topic-init-job.yaml:42` vs `bench.tfvars:31-34` |
| G4 | Sandbox pool is min=max=1; nothing scales it or maps sessions to nodes at the infra layer | `bench.tfvars:36-39` |
| G4 | `replicas: 5` fights KEDA ownership on every apply; sizing can double-pack a node | `deployment.yaml:14,99-104` vs `scaledobject.yaml:14-16` |
| G4 | Kafka/gp3 cost is fixed regardless of load, no scale-to-zero path | `bench.tfvars:31-34`, `kafka-bench.sh:29-30,71` |

### Proposals

1. **Per-contestant cost model.** Fixed base (general + Kafka + gp3) ≈ $28/day. Sandbox is 1 irreducible c6i.2xlarge/contestant ($0.34/hr, cpuset floor). Botworker share = `ceil(target_tps / per_node_tps) × $0.17/hr` (c6i.xlarge) — at the claimed 800k/s/node that's ~$0.68/hr marginal at ~1M/s target; at the last *measured* 64k/s/node the same target needs 16 nodes ($2.72/hr). **Re-measure per-node TPS post pacing-fix before sizing anything.**
2. **Cheapest botworker shape: c7g Graviton Spot.** A second node group, ARM, Spot, same taint/label. c7g.xlarge = 4 physical cores at ~$0.06-0.07/hr Spot vs c6i.xlarge's 2 cores+HT at $0.17 on-demand — roughly 4-5× TPS/$. No eBPF runs on botworker nodes, so no kernel-feature risk; needs a multi-arch Rust build. Per-contestant marginal drops to ~$0.45/hr (sandbox + 1-2 c7g Spot nodes).
3. **Autoscaling: sessions → pods (KEDA) → nodes (Karpenter).** Replace the lag trigger with a session-derived trigger (Postgres/Prometheus scaler on active-session count), `minReplicaCount 0`. Install Karpenter (absent today) with NodePools for `pool=botworker` (c7g Spot) and `pool=sandbox` (c6i.2xlarge OD, scale 0..N). Drop `replicas: 5` so KEDA solely owns replica count; set cpu request ~3 so one worker packs per 4-vCPU node.
4. **Single topic ownership + declarative bench overlay.** Kustomize base (24 partitions, RF=1, matching the actual 2-broker transient-data posture — today's checked-in RF=3 cannot even create topics on that cluster) + bench overlay (96 partitions, gp3-bench PVC, 8 ingester replicas), replacing the imperative sed/kubectl-set-env pipeline. One ConfigMap publishing `ORDERS_PARTITIONS` to every consumer.

---

## Verification verdicts

Fourteen claims from the six subsystem audits were adversarially re-checked against source. Corrections that change the subsystem narrative are already folded into the sections above; this table is the complete ledger.

| Claim | Verdict | Evidence |
|---|---|---|
| Controller runs sessions strictly serially (`consumer.go:89` sync call, `runner.go:208` blocks for scenario duration, single consumer goroutine) | CONFIRMED | `consumer.go:89`, `runner.go:114,208-218`, `main.go:83`; minor line-precision note: blocking select is `runner.go:209-218` |
| `workerIndexBalancer` routes by `worker_index` only, so two sessions' worker-0 specs collide on the same partition | CONFIRMED | `producer.go:71-86`, `runner.go:388-397` |
| All three Price-violation branches depend on reference-book state and are epoll-sensitive; Overfill/Phantom/SelfTrade are not | PARTIAL | Price branches confirmed epoll-sensitive; **SelfTrade is also epoll-sensitive** (`stream.go:63-68,139-142`) — only Overfill and Phantom are truly insensitive |
| `AggressiveFillToleranceNs` is never read by the streaming validator; production uses only the streaming path, so the env knob is a no-op | CONFIRMED | `validate.go:117,124,132,286` vs `stream.go` (no references); `main.go:205-217` |
| `OrderSentEvent` carries no `contestant_id`, so pre-first-ack snapshots are skipped by `redis_sink.rs:36` | PARTIAL | Outcome confirmed (no pre-ack Redis rows), **mechanism wrong**: gated by `Aggregator::snapshot()` at `aggregate.rs:291`, not `redis_sink.rs:36`; real bug is permanent data loss of pre-ack sent-side counters at `aggregate.rs:319-324`, not a deferred skip |
| `validateWorkerCapacity` checks only the current session against total partitions; no cross-session accounting exists | CONFIRMED | `producer.go:90-101,133`; mitigated today only because the controller's serial execution prevents concurrent sessions from ever reaching this code path simultaneously |
| The `contestant:{c}:{s}:{w}` Redis hashes have zero readers anywhere in the repo | PARTIAL | Zero **production** readers confirmed; the ingester's own integration test does read them (`tests/integration.rs:182,246,499`) — a self-test, not a consumer |
| The Redis sink feeding the leaderboard is never called at runtime (dead path) | PARTIAL | **"Never called" is refuted** — it runs on every flush tick unconditionally; the dead half is the **read** side: zero production consumers of the hashes it writes |
| A 1000-event named-msgpack chunk is ~2-3× larger than tuple encoding | PARTIAL | Measured empirically with the repo's own fixture: **1.84×** (383,934 vs 208,906 bytes), not 2-3×; direction confirmed, magnitude corrected |
| Cross-topic sent/acked co-assignment relies on librdkafka's default range assignor with no explicit strategy set | CONFIRMED | `kafka.rs:14-23` sets no strategy; default is the list `"range,roundrobin"` (not "range" alone) — more fragile than stated, but moot at today's single-replica deployment |
| A bot-fleet worker handles one workload spec at a time, so partition sharing means serial execution and missed barriers | CONFIRMED | `worker.rs:174-215,220-271,402`, `producer.go:90-99`'s own error text acknowledges the failure mode; unguarded specifically for *concurrent sessions*, not same-session oversubscription |
| Strict (EffectiveT3, Flow, TCPSeq) replay ordering mismatches contestant epoll processing order, causing false price/time violations | CONFIRMED | `order.go:42-51`, `stream.go:203`, reference engine's own comment documents the mechanism (`deploy-local/correct-engine/src/main.go:20-31`); minor wording note — the C++ engine uses per-connection threads, not epoll, same mechanism |
| `CrossFlowTie`'s 100ns tolerance is the ONLY cross-connection ordering forgiveness anywhere in the scoring path | PARTIAL | True for the executed production path; **a second mechanism** (`AggressiveFillToleranceNs`) exists in the dead batch code — restate as "only forgiveness in the production streaming path" |
| Running multiple benchmark sessions concurrently is blocked or unsafe today | CONFIRMED | Two independent blockers: controller's single synchronous consumer loop (`consumer.go:89`, `replicas:1`), and the shared `workload.assignments` topic with one-workload-at-a-time workers and no cross-session admission (`worker.rs:174-215`, `producer.go:92-96`) |

---

## Target architecture and roadmap

*Status: design proposal. Branch context: `feat/bot-tps`. Breaking changes are allowed and taken where they simplify the target state.*

### 0. Where we are and why the platform is single-contestant today

The platform benchmarks exactly one contestant at a time, enforced by two independent mechanisms, not one:

1. **Controller serialization.** `main.go:83` starts a single consumer goroutine; `consumer.go:89` calls `runner.Run(ctx, req)` synchronously inside the fetch/commit loop, and `runner.go:209-218` sleeps through the entire scenario duration (`time.After(totalDuration)`) before the next `benchmark.requested` message is even fetched. With `replicas: 1` (`k8s/benchmark/bot-fleet-controller/deployment.yaml:14`), session N+1 waits for session N's full lifecycle. The 3 partitions on `benchmark.requested` (`ops/kafka/create-topics.sh:37`) buy nothing behind one blocking consumer.
2. **Session-agnostic worker routing.** Even if the controller dispatched concurrently, `producer.go:71-86` maps each workload spec to partition `worker_index % numPartitions` — worker 0 of *every* session lands on partition 0, because `runner.go:388-397` restarts worker indexes at 0 per session. Bot-fleet workers run one spec at a time for the full run duration (`worker.rs:174-215`: `run_workload(...).await` inline, commit after), so a second session's spec queued on an occupied partition misses its barrier. Worse: the barrier consumer group is per-worker, not per-session (`worker.rs:402`, test at `worker.rs:2194-2198`), and `wait_for_barrier` commits every barrier message it reads (`kafka.rs:307-342`), so a late worker finds its offset already past its barrier and times out after `BARRIER_WAIT` 120s (`worker.rs:48`), dropping the workload. `validateWorkerCapacity` (`producer.go:90-101`) guards only a single session against the 24-partition topic (`ops/kafka/create-topics.sh:39`); there is no cross-session accounting anywhere.

Everything downstream is already concurrency-ready: `SessionManager` is a mutex-guarded per-session map with per-session ready dispatch (`session.go:38-108`), sandbox slots are per-session pods with distinct IPs (`sandbox-orchestrator/internal/k8s/slot.go:141-158`), telemetry aggregation keys on `(session_id, wave)` with waves derived deterministically from `barrier_epoch_ns` (`aggregate.rs:176-205, 449-471`), and order IDs are session-prefixed so shared Kafka topics never collide on keys (`telemetry.rs:332`). The work is in the execution path, not the data model.

Shape A multi-protocol is done: a submission declaring `ALL` gets FIX+REST+WS `TargetSpec`s, tasks round-robined across targets (`runner.go:345-379`), sandbox slots are multi-port with readiness dialing every port (`slot.go:141-177, 241-245`), and capture derives transport per packet from server port (9898→FIX, else HTTP/WS), both ports always captured (`docs/tps-improvement-plan.md:294-306`, `slot.go:45`).

### 1. Target architecture: N contestants × 3 protocols at max TPS

#### 1.1 Shape

Per active contestant (a "session"):

- **1 sandbox node** (c6i.2xlarge, x86, On-Demand) carrying the algo pod (Guaranteed 2 CPU / 1Gi, `slot.go:678-687`, `deployment.yaml:46-49`) plus its node-pinned eBPF capture Job (hostPID, setns into the pod netns, `netns.rs:14-47`, `slot.go:533-655`). Capture must colocate with the algo pod (it enters `/proc/<pid>/ns/net`), so the sandbox node is the irreducible per-contestant unit. One contestant per node is made explicit with required podAntiAffinity on the algo label (topologyKey `kubernetes.io/hostname`) and by raising the capture Job's CPU *request* from 200m to 3 — its real hot demand; the 200m request vs 4-CPU limit (`slot.go:659-674`) currently leaves the scheduler blind and would pack two captures onto one node, reproducing the CFS-throttle→ringbuf-drop failure the 2→4 limit bump fixed.
- **1-2 botworker nodes** (target: c7g.xlarge Graviton Spot) running bot-fleet workers driving FIX+REST+WS simultaneously via Shape A targets. Spot is safe here (stateless, re-runnable load) and forbidden on sandbox nodes (mid-run reclaim kills the contestant's benchmark).
- **A partition lease** of W partitions on `workload.assignments` (Section 2).
- **A shared slice** of the Kafka telemetry plane and ingester replicas (Section 3).

Shared fixed base: 2× general nodes (controller, orchestrator, APIs, Postgres/Timescale, Redis, frontend), a Kafka broker pool, ingester replicas, one validator deployment.

#### 1.2 Cost model

Honest caveat first: per-node botworker throughput is the dominant unknown. Last measured on EKS: ~64k orders/s per c6i.xlarge — *before* the catch-up pacing fix landed (the tokio-timer pacer capped each task at ~1k/s; the fix is 65-75× locally). `bench/bench.tfvars:14` claims ~800k/s/node, a 12× spread against the measurement. **Re-measure one node post-fix before buying anything** — the drain-mode sweep in `deploy-bench/` is the harness. The model below uses per-node TPS `R` as a parameter.

| Component | Unit | $/hr (us-east-1) | Scaling |
|---|---|---|---|
| Fixed base: 2× m6i.2xlarge general | node | 0.384 ea | constant |
| Kafka brokers: m6i.xlarge | node | 0.192 | +1 per ~1M events/s post-compression (Section 3.4) |
| gp3 500MB/s+16k IOPS volume | vol | ~0.025-0.03 | per broker |
| Sandbox: c6i.2xlarge On-Demand | node | 0.34 | 1 per active contestant, scale 0..N |
| Botworker: c7g.xlarge Spot | node | ~0.06-0.07 | ceil(target_tps / R) per contestant |
| Botworker fallback: c6i.xlarge OD | node | 0.17 | min 0, Spot-drought only |

Per-contestant marginal at target ~1M/s offered, assuming R lands near the claimed range on Graviton: **~$0.34 (sandbox) + ~$0.07-0.14 (1-2 c7g Spot) ≈ $0.45-0.50/hr**, paid only while the run is active (sandbox and botworker pools scale to zero — Section 2.4). Idle-cluster cost falls to the general+Kafka base, ~$28/day at the 2-broker tier. If R measures at 64k/s the same target costs 16 nodes/contestant and the plan is not viable until the pacing fix is validated on EKS — this is the first task in Phase 0.

c7g.xlarge rationale: 4 physical cores vs c6i.xlarge's 2 cores+HT, at ~$0.145 OD / ~$0.06 Spot — roughly 4-5× TPS/$. bot-fleet is pure Rust with no eBPF or x86 dependency on botworker nodes (capture lives on sandbox nodes; the netshoot MTU/offload initContainer at `k8s/benchmark/bot-fleet/deployment.yaml:38-56` exists for the *sandbox-side* capture's 1536B frame truncation and doesn't constrain botworker arch). Needs a multi-arch image (`docker buildx`, `aarch64-unknown-linux-gnu`) and one ARM TPS validation run. Sandbox stays x86: contestant binaries and the baked BPF object (`slot.go:39`) are x86; Graviton sandbox is deferred until an arm64 CO-RE build is verified.

Two per-contestant fidelity items ride along:

- **cpuset pinning** is dead on EKS (NodeConfig `reservedSystemCPUs` conflicts with EKS default kube/system-reserved and blocks node join, `infra/terraform/main.tf:173-198`, `variables.tf:206-215`). Fix by expressing reservations as `kubeReserved`/`systemReserved` quantities alongside `cpuManagerPolicy: static` in nodeadm config, validated on a throwaway node. Config-only; `slot.go:119-127` already validates integer cores.
- **Capture on EKS is unvalidated**: every EKS bench run so far used `CAPTURE_ENABLED=false` (`deploy-bench/eks-up.sh:95`). The XDP DRV→SKB fallback (`main.rs:501-531`) must be exercised on VPC-CNI veth before contest day.

### 2. Multi-contestant orchestration and routing

Four changes, in dependency order. No new service for moderate N; a thin scheduler appears only at the point stated in Section 7.

#### 2.1 Concurrent session dispatch in the controller

`StartBenchmarkRequested` dispatches `runner.Run` into a per-session goroutine behind a semaphore (`MAX_CONCURRENT_SESSIONS`), committing the `benchmark.requested` offset on admission. `SessionManager` already isolates state and routes ready signals by session (`session.go:95-108`, `consumer.go:125`); the duplicate-session guard already exists (`session.go:51-61`). Keep `replicas: 1` until 2.2's lease store exists — scaling replicas *before* fixing routing converts "blocked" into "second session fails at ReadyDeadline", because both replicas would stamp colliding partitions. ~100 LoC.

#### 2.2 Partition leasing replaces modulo routing (breaking)

Replace `workerIndexBalancer` with an explicit lease allocator: a Postgres table (or controller-local bitmap while replicas=1) over the `workload.assignments` partitions. On admission, lease `worker_count` free partitions; stamp each spec's Kafka message with its leased partition explicitly (drop the balancer); release on session completion/failure/expiry (the expiry watchdog already exists for run lifecycle). Admission blocks in a bounded queue when free partitions < worker_count — this *is* the global capacity check that `validateWorkerCapacity` never was. Raise the topic to 64+ partitions and align `maxReplicaCount` (`scaledobject.yaml:17` currently 50 vs a 24-partition consumption ceiling — replicas 25-50 can never take a partition).

Also fix the barrier group: make it per-(session, worker) or stop `wait_for_barrier` committing offsets past barriers it hasn't matched (`kafka.rs:340`), so a re-used worker can still see its own barrier.

#### 2.3 Pre-scale gate + hard partial-ready failure

Before publishing specs, the controller checks bot-fleet `readyReplicas` (k8s API) against total leased-partition demand across in-flight sessions, with a pre-scale deadline separate from `READY_DEADLINE`. Today KEDA reacts to lag only after specs are published, racing the 30s deadline (`producer.go:142-146` documents the race), and `awaitReady` proceeds on partial fan-in (`runner.go:317-327`) — a contestant silently benchmarked at a fraction of intended TPS is an integrity failure, not a degraded mode. Default `PARTIAL_READY_POLICY=fail`.

#### 2.4 Sessions → pods → nodes autoscaling

There is no node autoscaler at all today — `addons.tf:65-70` installs only KEDA, all four pools are min=max=desired (`bench.tfvars:22-49`), so KEDA scale-out beyond ~3 botworker pods just makes Pending pods. And the KEDA trigger itself is wrong: lag on `workload.assignments` (`scaledobject.yaml:26`, lagThreshold 1) is a start-of-run pulse — assignments are consumed once at session start while load continues, so cooldownPeriod 120s scales the fleet in mid-run.

Target: (a) KEDA trigger becomes session-derived — postgres scaler on `count(running sessions) × workers_per_session`, minReplicaCount 0; (b) Karpenter NodePools for `pool=botworker` (c7g Spot, consolidation on) and `pool=sandbox` (c6i.2xlarge OD, scale 0..N), fixed pools remain only for general+Kafka; (c) delete `replicas: 5` from the bot-fleet Deployment (it fights KEDA's HPA on every apply) and set cpu request ~3 so exactly one worker packs per 4-vCPU node; (d) sandbox-orchestrator gains a capacity admission check returning 429+retry-after when pending slots exceed the NodePool limit (`handler/slot.go:64-138` currently creates unconditionally and Pending pods surface only as `StateCreating` until the 3600s deadline); (e) `READY_DEADLINE`/deploy deadline budgets for ~90-120s scale-from-zero node join.

### 3. Telemetry and Kafka scaling

The telemetry path is load-bearing for offered TPS, not just observability: `TelemetrySink.record()` blocks losslessly when the channel is full (`telemetry.rs:93-101`), so telemetry drain rate directly throttles order generation. `BOT_DISABLE_TELEMETRY` exists precisely because a single broker can't absorb per-order telemetry at drain rates (`telemetry.rs:38-41`). Known single-broker durable ceiling: ~445k events/s at RF=3/min.insync=2 on default gp3 (125MB/s).

#### 3.1 P5: shard the per-worker aggregator

One aggregator task + one producer per worker pod (`telemetry.rs:61-72`) is the per-contestant drain ceiling. Split into M≈4 shards, routed by `partition_for(order_id, num_partitions) % M` so per-partition batching stays coherent; each shard owns its mpsc channel, `PartitionBatcher`, and rdkafka producer. Lossless backpressure is preserved per shard; drain multiplies ~M×. Already planned as P5 in `docs/tps-improvement-plan.md:170-173`.

#### 3.2 Positional msgpack + envelope hoisting + compression (breaking wire change)

`rmp_serde::to_vec_named` repeats all 16 field names in every event (`telemetry.rs:229`, schema at `schemas/rust/src/lib.rs:189-208`). Measured with the exact `full_chunk_stays_under_broker_message_ceiling` fixture (`telemetry.rs:326-345, 407`): a 1000-event named chunk is 383,934 bytes vs 208,906 tuple-encoded — **1.84×**, not the 2-3× sometimes assumed; size all capacity math on 1.84×. Changes, shipped together behind a batch-version byte:

- tuple (positional) encoding via `rmp_serde::to_vec`;
- hoist per-batch constants (`session_id`, `submission_id`, `worker_id`, `barrier_epoch_ns`) out of events into the batch envelope;
- `compression.type=lz4` on producers and topics (nothing sets compression anywhere today, `topic-init-job.yaml:42-45`);
- raise `MAX_EVENTS_PER_BATCH` from 1000 toward the 1MiB ceiling (~4-5k events post-compaction, `telemetry.rs:317, 419`).

This is a coordinated change across `schemas/rust`, `schemas/go`, bot-fleet, ebpf-latency, ingester, and validator decode — the named-msgpack contract is a cross-language invariant. Net effect: ~2.5-3.5× effective event rate per broker for zero new nodes.

#### 3.3 Session-affine partition bands (breaking)

`partition_for = FNV1a(order_id) % 24` spreads every session across every partition by design (`lib.rs:41-52`, spread test `telemetry.rs:391-402`), so every ingester replica processes every session and one hot contestant delays all snapshot cadences. Change to: `base = FNV(session_id) % P_total`, order hashed within a band of `P_s` partitions from base. Sent/acked co-partitioning survives (both sides compute the same function from fields already in every event); wave bucketing is already replica-independent via `barrier_epoch_ns`. Ingester replicas then own disjoint session sets and scale with session count. Version the contract — validator replay reads by partition and gains directly (Section 4).

#### 3.4 Ingester hardening

- **Explicit partition assignment, not group magic.** The sent/acked join requires partition i of both topics on the same replica; today that holds only because librdkafka's default strategy list `"range,roundrobin"` elects range in a homogeneous group (`kafka.rs:13-28` sets no strategy). Range is an eager assignor: every rebalance revokes everything, and any partition that moves strands the old replica's in-memory join state — with `enable.auto.commit=true` + `auto.offset.reset=latest` that's silent data loss. Single-replica today, so this is a latent hazard that fires exactly when we scale. Fix: StatefulSet + `group.instance.id`, each replica `assign()`s the same computed partition list on both topics from (ordinal, replica_count); disable auto-commit, commit after each snapshot flush.
- **Decouple consume from flush.** The single `select!` loop takes one message per wake and awaits Timescale+Redis writes inline (`ingester.rs:48-71, 108-137`); slow storage stalls consumption for all sessions. Split into consume task (batch recv into bounded channel) and flush task; pipeline Redis writes (`redis::pipe`) and add a TTL (~24h) on `contestant:*` keys (currently unbounded, per-row awaited HSETs, `redis_sink.rs:33-54`).
- **Fix the pre-first-ack data drop.** `contestant_id` arrives only on acked events (`aggregate.rs:230-248`); `snapshot()` refuses to emit rows with empty `contestant_id` or empty service_time (`aggregate.rs:291`), and then resets sent-side counters unconditionally (`aggregate.rs:319-324`) — an interval's offered/timeout data before the session's first ack is permanently dropped from both Timescale and Redis. Fix in `aggregate.rs`: carry `contestant_id` in the workload spec → `OrderSentEvent` envelope (the controller knows it), or defer counter reset until a row is emitted.

#### 3.5 Broker/disk sizing

Sent+acked ≈ 2 events/order; per contestant at ~150k acked/s ≈ 50MB/s pre-compression. Provisioning rules: keep per-broker leader throughput under ~60% of the *re-measured post-compression* ceiling; gp3 volumes explicitly at ≥500MB/s / 16k IOPS (verify the disk wall with log-flush metrics before paying — 125MB/s default is the suspect); RF=3/min.insync=2 stays for orders.* because replay is correctness evidence (G2) — do not trade to acks=1/RF=2. Ballpark: 3 brokers handle 4-6 concurrent ~100k/s sessions post-compression.

**Single topic ownership** (prerequisite hygiene): today three sources disagree — checked-in `topic-init-job.yaml` (24 partitions, RF=3, which *cannot even create topics* on the checked-in 2-broker bench pool), `kafka-bench.sh`'s sed to 96/RF=1, and ebpf-latency's code default `ORDERS_PARTITIONS=24` (`main.rs:111`) which silently mispartitions on a 96-partition cluster if the env isn't set imperatively. Replace with kustomize base+bench overlays and one ConfigMap publishing `ORDERS_PARTITIONS` to bot-fleet, sandbox-orchestrator, and the capture job template; collapse the two divergent gp3-bench StorageClasses into one.

### 4. Per-contestant replay pipeline (G2)

Replay already works per session: `StreamSession` opens a reader per partition of both topics, joins sent/acked, reorders in a bounded window, streams into the validator, and publishes a per-session `CorrectnessScoreEvent` (`source/stream.go:86-122`, `main.go:223-255`). ContestantID comes from the first acked event (`stream.go:192-194`), fine while sessions are 1:1 with contestants. Three scaling defects:

1. **O(N × topic) re-scan.** Every validation opens *all* partitions of both topics and discards non-matching batches after decode (`stream.go:288, 298`). N concurrent sessions ⇒ N full scans. Fix, in order of preference: (a) Section 3.3's session bands mean a session lives on a known partition subset — `StreamSession` opens only those (infra-only once bands ship); (b) if band width still makes decode the bottleneck, a shared demux: one drain per (topic, partition) window routing decoded batches by SessionID into per-session bounded lanes, each lane owning its Reorderer+StreamValidator unchanged.
2. **Memory.** `DefaultReorderWindow = 1<<20` orders (`stream.go:29-30`) × `VALIDATOR_CONCURRENCY` (default 4) is hundreds of MB per session for jitter that is orders of magnitude smaller. Shrink via the existing `REORDER_WINDOW` env after measuring real reorder depth.
3. **Offset pruning assumes UUIDv7 session IDs** (`drain.go:279-299`); anything else falls back to reading from FirstOffset. Make UUIDv7 a validated invariant at session creation, or persist (session → start offsets) at run start.

The batch path (`DrainSession` + `validate.Run`) is memory-unbounded and already OOMed at 445k/885k orders; it is unreachable from production `main.go`. Delete it in Phase 3 once the tolerance semantics it uniquely holds are ported (Section 6).

> **Update (2026-07-31).** Deleted. The tolerance was NOT ported — it was deleted too, as it forgives cross-flow interleaving that pass 1 (one task, one connection) does not have. What the batch path did uniquely hold, and what WAS ported, is self-match detection against the book's resting state: without it the streaming path could not detect a self-match at all.

### 5. Live leaderboard end-to-end (G3)

Ground truth: the push skeleton **already exists** — score-computer publishes `LeaderboardUpdateEvent` to `leaderboard.updates` (`worker.go:90-105`), leaderboard-api consumes and broadcasts over SSE at `/api/events` (`consumer.go:40,61`, `main.go:96`), the frontend patches the react-query cache with a row flash (`useSSE.ts:63-72`, `useLeaderboard.ts:80-103`). What's missing is *mid-run* data and payload correctness.

The ingester's RedisSink writes `contestant:{cid}:{sid}:{wave}` hashes on every flush tick (`ingester.rs:130`, `redis_sink.rs:60-65`) — but nothing in production reads them; leaderboard-api's Redis is only a 2s JSON cache of its own Postgres queries (`cache.go:41-68, 105-115`), and score-computer's ZSET methods (`ZAdd/ZScore/ZRevRank`, `LEADERBOARD_KEY`) are never invoked — rank comes from Postgres `RankForRunGroup` with `rankDelta` hardcoded 0 (`worker.go:82-87`). So: the data for a live view is already flowing into Redis every second, only the read side is missing, and a dead third design (the ZSET) is confusing the picture.

Target design:

1. **Mid-run stream (new read path, no new infra).** Ingester additionally writes `live:{session_id}:latest` (newest wave snapshot) so readers never guess wave indexes. leaderboard-api runs a 1s loop (matching flush cadence): for sessions in `ActiveRuns`, read latest snapshots, broadcast SSE event `live_metrics` `{contestant_id, session_id, tps_1s, p99_ns, error_rate, wave_index}` on the existing broker (which already handles fan-out and slow-client drops, `sse/broker.go:47-56`). Frontend adds a `live_metrics` listener feeding a live-runs panel / per-row sparkline.
2. **Final-score push fixes.** Add `scenario` to `LeaderboardUpdateEvent`; client drops updates not matching the active tab and re-sorts/truncates after patching (`useLeaderboard.ts:44-50` currently appends unsorted regardless of scenario). Compute `rank_delta` from RankForRunGroup before/after SaveScore.
3. **Delete the score-computer ZSET path** and document: Postgres is ranking-authoritative; Redis is (a) ingester live snapshots, (b) leaderboard-api response cache. Two half-built Redis leaderboard designs is how the read path got lost.
4. Optional: provisional mid-run *score* ticker from `score_progress` rows (per-scenario partial correctness already lands there, `store.go:216-218`), published as `partial=true` events, rendered dimmed. Only worth it for runs >1-2 min.

Latency budget: order → capture → ingester flush (1s tick) → Redis → 1s poll → SSE ≈ 2-3s worst case for TPS/latency movement; final scores remain event-driven at run completion.

### 6. Validator ordering: cross-connection processing order vs capture order (G5)

#### 6.1 The problem, precisely

The validator replays all orders in a strict total order (EffectiveT3, Flow, TCPSeq) — `replay/order.go:42-51`, with the streaming Reorderer emitting the identical order (`reorder.go:61`). T3 is kernel XDP-ingress time (`stream.go:203`). The reference book is built by consuming orders in that order (`stream.go:50-52`, `book.go:181`), and queue-jump detection compares engine arrival seqs (`stream.go:175-200`).

But no contestant processes in kernel-ingress order across connections. epoll delivers per-connection FIFO with arbitrary cross-connection interleave; per-connection reader threads (the C++ engine, `fix_server.cpp:12-15,442,538`) have the same property. Within one connection there is no mismatch (TCPSeq agrees); across connections, a perfectly price/time-correct contestant diverges from capture order by µs-ms. The reference engine in `deploy-local/correct-engine/src/main.go:20-31` documents this exact mechanism about itself.

Blast radius by violation type: **Time** and **CancelReplaceLoss** are directly ordering-derived (queueJump). All three **Price** branches compare against reference-book fills (`stream.go:144-165`: no-reference-fill, price-not-produced, cum>ref-qty) and are ordering-sensitive. **SelfTrade** is *also* ordering-sensitive — `r.selfPrices` is populated from reference-engine trades gated on same-participant maker/taker (`stream.go:63-68, 139-142`), so replay divergence can create or destroy a reference self-trade. Only **Overfill** (own-qty arithmetic, `stream.go:134-137`) and **Phantom** (sent-set membership, `source/stream.go:161-163`) are ordering-insensitive.

Existing mitigations are ineffective: the only forgiveness on the production path is `CrossFlowTie` with `TieToleranceNs = 100` (`order.go:14`) — 100ns against µs-ms skew excuses nothing. `AGGRESSIVE_FILL_TOLERANCE_US` is wired from env (`main.go:60`) but read only by batch `validate.Run` (`validate.go:117-137`), which nothing in production calls; `StreamValidator.scoreOrder` implements only tolerance==0 semantics. Consequence: honest stock-socket engines (both C++ and Go) score ~45%, and high scores currently require kernel-bypass networking that makes processing order match ingress order.

> **Update (2026-07-31).** Two corrections to this section. (1) `AGGRESSIVE_FILL_TOLERANCE_US` is deleted, so it is no longer even a candidate mitigation — the diagnosis stands, the escape hatch does not. (2) The **SelfTrade** class is no longer ordering-sensitive in the way described above: `r.selfPrices` is gone. The reference book applies skip-and-continue, so a same-SMP pair produces no trade at all and the trade-derived check could never fire; detection now reads the book's RESTING state (`book.RestingSMPMatch`). Scope note: this whole section is about MULTI-flow replay, i.e. pass 2. Pass 1 is single-task/single-connection, where TCPSeq totally orders the session and none of this applies.

#### 6.2 Candidates

**A — Widen the cross-flow tie window** (`EPOLL_TIE_TOLERANCE_US`, ~200-1000µs, threaded through `CrossFlowTie`).
Hours of work; immediately collapses false Time/CancelReplaceLoss. Does nothing for Price/SelfTrade (reference-book divergence is untouched). Too-wide windows also excuse *real* cross-flow queue jumps inside the window. Ship as the interim knob, not the fix.

**B — Contestant-processing-order oracle from T7 ack egress.**
The capture already records per-response `T7XDPEgressNS`. Define `ProcessedTS(order) = min T7` over its responses — the first byte the contestant emitted about the order, an upper bound on when it finished ingesting it. Replay cross-flow by `(ProcessedTS, Flow, TCPSeq)`, keeping per-flow TCPSeq as a hard invariant with monotone promotion per flow — the existing EffectiveT3 promotion machinery (`reorder.go:78-96`) is reusable verbatim with T7 substituted for T3. The reference book is then built in an order the contestant *actually realized*, fixing Time, CancelReplaceLoss, Price, and SelfTrade false positives at once. Orders with zero responses fall back to T3. Anti-gaming: flag `ProcessedTS < T3` (impossible) and cap `ProcessedTS − T3`; incentives already oppose ack-delaying because latency is scored from the same t7. Residual weakness: contestants whose ack-emission order differs from internal ingest order (deep pipelining) are mis-timed. ~1-2 days including parametrized reorder tests.

**C — Partial-order / best-legal-schedule validation.**
Same-flow TCPSeq authoritative; cross-flow orders within window W form equivalence classes; on a would-be violation, bounded search over linear extensions (swap cross-flow neighbors within W, capped K resimulations) and accept the fill if any legal schedule produces it. Zero false positives by construction and zero trust in contestant timestamps — but requires book-engine snapshot/rewind surgery, worst-case CPU blowups on hot price levels, and scores become schedule-existence claims that are hard to explain on a leaderboard. 1-2 weeks. Keep behind a flag as the escalation path.

**D — Protocol-level contestant-reported ingest sequence** (echoed monotone seq per order, cross-checked against TCP order and T3 within a bound). Ground truth, but a schema change across bot-fleet templates, all contestant engines, eBPF parse, and validator — every contestant must implement it. v2 material.

#### 6.3 Recommendation

**Ship B as the fix, A as the interim knob, C as the audit/appeals backstop.**

B is the only proposal at small cost that repairs *all four* ordering-sensitive violation classes, because it corrects the replay order itself rather than forgiving individual symptoms. It needs no capture change, no protocol change, no engine surgery — a sort-key swap plus promotion reuse; the reorderer, join, and StreamValidator are untouched. Its trust assumption (T7 reflects processing order) is bounded by the anti-gaming checks and by the latency-score incentive, and its known blind spot (internal pipelining reordering ack emission) is exactly what D closes later if it matters in practice. A ships first because it is hours of work and immediately de-noises Time violations while B is in test. C is reserved for disputed scores: run the schedule-existence check offline on appeal rather than inline on every order. Acceptance test for B: the stock-socket C++ and Go engines, which embody correct matching under epoll/thread ordering, must score near-100% on price/time classes; kernel-bypass engines must not regress.

Also in this workstream: port a tolerance hook into `StreamValidator.scoreOrder` or delete `AGGRESSIVE_FILL_TOLERANCE_US` entirely — a documented env knob that is a production no-op is worse than absent. **Done 2026-07-31: deleted.** Porting it was rejected — it would have needed a bounded per-level availability structure (the existing one grew O(session)), and it forgives the wrong thing: a fill the reference never produced, rather than the replay order that made it look wrong. B above corrects the order itself, which is the actual defect.

### 7. New components — and everything that stays

Only **one** new deployable and **one** new infra addon are justified. Everything else lands inside existing services because the seams already exist:

- Controller concurrency, leasing, pre-scale gating: extensions of bot-fleet-controller — `SessionManager` and the producer are the natural owners of session and partition state.
- Telemetry sharding, wire format, ingester assignment/flush split: internal to bot-fleet / schemas / telemetry-ingester.
- Live leaderboard: the SSE broker, the Redis writes, and the frontend hook all exist; only a read loop and an event type are added to leaderboard-api.
- Replay demux and T7 ordering: internal to correctness-validator's `source/` and `replay/` packages.

New:

1. **Karpenter** (infra addon). No node autoscaler exists; KEDA scales pods only. Cluster-autoscaler on fixed ASGs could substitute, but Karpenter's consolidation and scale-from-zero per tainted pool map directly onto "one sandbox node per active contestant, zero when idle" — the cost model's core mechanism. Nothing existing can absorb this: it is a control-plane capability, not a service feature.
2. **run-scheduler** (thin, deferred until demand exceeds the partition/sandbox budget). A Postgres-backed QUEUED-status queue drained by the controller at the rate free partitions + free sandbox capacity allow, exposing queue position via `benchmark.status.updated`. Why not the controller as-is: admission under contention needs durable ordering and user-visible queue position across controller restarts and replicas; Kafka lag is opaque to contestants and unfair under bursts. Why not KEDA/Karpenter: they scale capacity, they don't arbitrate fairness. Until contest-start burst demand actually exceeds capacity, the semaphore + lease queue from 2.1/2.2 suffices — build the scheduler when the queue becomes user-visible product surface.

Explicitly rejected: a separate "live-metrics service" (leaderboard-api already brokers SSE), a per-session Kafka-topic-per-contestant scheme (session bands on shared topics preserve the co-partition contract with none of the topic-churn), and Spot sandbox nodes (reclaim kills a run).

### 8. Phased migration plan

Breaking changes are batched into Phase 2 (wire format + partitioning together, one flag day — both sides of every contract deploy together).

**Phase 0 — Measure and de-risk (days).** Re-measure per-node botworker TPS on EKS post pacing-fix (drain sweep in `deploy-bench/`); the cost model is parameterized on this number. Validate capture-on-EKS (XDP/SKB on VPC-CNI) — it has never run there. Validate the nodeadm cpuset config on a throwaway node. Ship G5 interim: env-configurable tie tolerance (6.2-A).

**Phase 1 — Concurrency correctness (week 1-2).** Controller goroutine dispatch + semaphore (2.1). Partition lease allocator + explicit-partition publish, delete modulo balancer (2.2). Barrier-group session fix. Pre-scale gate + `PARTIAL_READY_POLICY=fail` (2.3). Sandbox anti-affinity + honest capture CPU request (1.1). Exit criterion: 2-3 concurrent sessions on a manually-scaled cluster, all barriers hit, zero partial fan-ins, capture drop-free.

**Phase 2 — Data-plane scale, one coordinated flag day (week 2-4).** Versioned positional-msgpack batches + envelope hoisting + lz4 (3.2). Session-affine partition bands in `partition_for`, both producers + validator reader (3.3, feeds 4.1). Kustomize topic ownership + `ORDERS_PARTITIONS` ConfigMap (3.5). P5 aggregator sharding (3.1). Ingester: static assignment, manual commits, consume/flush split, pre-ack-drop fix (3.4). Broker pool + gp3 provisioning per re-measured ceiling. Exit criterion: 2 sessions at max TPS with telemetry ON, no `record()` backpressure throttling, ingester restart mid-run loses ≤1 snapshot interval.

**Phase 3 — Validator and replay (parallel with Phase 2; independent).** T7 processing-order oracle behind `REPLAY_ORDER=processed` (6.3), acceptance-tested against the honest cpp/go engines. Band-scoped partition reads in `StreamSession`; shrink reorder window; UUIDv7 invariant. Delete the batch path and the dead tolerance knob (or port tolerance to the stream path first). Exit criterion: stock-socket engines score near-100% on ordering-sensitive classes; N=3 concurrent replays without full-topic scans.

**Phase 4 — Autoscaling and cost (week 4-5).** Karpenter + NodePools (sandbox 0..N OD, botworker c7g Spot + OD fallback). KEDA postgres session trigger, minReplica 0, drop `replicas: 5`. Multi-arch bot-fleet image + one ARM TPS validation. Orchestrator 429 admission. Deadline budgets for scale-from-zero. Exit criterion: idle cluster = base cost only; a session request cold-starts sandbox+botworker nodes and completes inside deadlines.

**Phase 5 — Live leaderboard + polish (week 5-6).** `live:{session}:latest` pointer + leaderboard-api 1s read loop + `live_metrics` SSE + frontend panel (5.1). Scenario-scoped update patching, real `rank_delta`, delete the ZSET dead code (5.2-5.3). Optional provisional-score ticker. run-scheduler only if contest-registration numbers demand it. Exit criterion: full dress rehearsal — N≥3 contestants, 3 protocols each, live board moving mid-run, per-contestant replay scores published, cost telemetry matching the Section 1.2 model.

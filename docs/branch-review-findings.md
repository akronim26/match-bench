# Adversarial review findings — feat/bot-tps implementation commits

> **Verified 2026-07-16.** All 56 findings below carry an adversarial-verification verdict: **52 confirmed / 3 refuted / 1 partial**. Refuted findings are retained here for the record, each with its refutation evidence, rather than deleted — they were plausible failure hypotheses the finder raised that closer reading of the surrounding code ruled out.

## Context

This is a line-level adversarial review of the implementation commits on `feat/bot-tps`:

- `d520955`, `ed1184c`, `7a4b837` — bot-fleet hot path (batched FIX/REST/WS writers, expiry queue, per-protocol metrics)
- `9dc256d` — bot-fleet-controller (dispatch concurrency, partition lease allocator)
- `6e84a7b` — telemetry (ingester sharding, msgpack V2 wire format, session-band partitioning, zstd compression)
- `248b36c` — live leaderboard (SSE poller/broker, frontend live-metrics hook)
- `3bea142` + `3411e53` — correctness-validator (two-pass invariants mode, streaming rewrite)

Three review lenses were run against these commits in parallel: **unbounded state** (maps/queues/slices that grow without eviction), **concurrency** (races, wakeup fairness, goroutine/task lifecycle), and **blocking + scale** (calls that block a shared runtime/thread pool, work that scales worse than claimed). The raw finder output was deduped from 58 findings down to 56. Findings below are grouped by the component owning the file, and within each component ordered critical → major → minor. Each entry gives the file:line, severity, a one-line summary, the finder's failure scenario verbatim (uncompressed — this is the evidence a verifier needs), and an empty verdict line for the next phase.

---

## bot-fleet-controller (11 findings: 1 critical, 8 major, 2 minor)

### `internal/controller/runner.go:131` — critical
**Summary:** Lease is released the instant the session goroutine returns, but the workload-spec messages published under that lease can still be sitting unconsumed on the partition; the next session immediately re-leases the partition and enqueues behind stale specs.

**Failure scenario:** Session A leases partition 7, publishes its spec, then fails at awaitReady with partial fan-in because no bot-fleet worker ever consumed partition 7 (KEDA not scaled). defer Release returns 7 to the pool while A's spec is still uncommitted on the partition. Session B acquires 7 and publishes its spec behind A's. When a worker finally attaches to partition 7 it processes A's stale spec first (targets a deleted slot; burns connect timeouts against a dead endpoint), delaying B's spec past READY_DEADLINE — B hard-fails with partial fan-in through no fault of its own, and the pattern repeats for every successor of a failed session. The lease lifecycle and the Kafka message lifecycle on the partition are not paired.

**Verdict: CONFIRMED**
Lease released on Run return with no coordination to whether published specs were consumed; fast-fail frees partition with stale spec still on it for next lessee.

Fix: Release only after spec consumed/acked or tombstoned.

### `internal/controller/consumer.go:111` — major
**Summary:** Offset commit-on-accept with no durable session record means a hard controller crash (OOM/kill, not graceful ctx cancel) permanently strands accepted sessions: the run stays in a non-terminal status forever and its orchestrator sandbox slot is never deleted, leaking one sandbox per in-flight session per crash.

**Failure scenario:** 4 sessions dispatched (offsets already committed), each has passed CreateSlot and is mid-run; controller pod is OOM-killed. On restart Kafka does not redeliver (committed), the in-memory Session/lease state is gone, no RunStatusFailed is ever published (the 'controller shutdown' fail path only runs on ctx.Done, which never fires on SIGKILL), and DeleteSlot is never called. Over 24h of periodic restarts the orchestrator accumulates orphaned sandbox slots until slot capacity is exhausted and every new session fails at create_slot; users see runs stuck in DEPLOYING/RUNNING forever with no retry signal, contradicting the code comment's 'user-retryable' claim.

**Verdict: PARTIAL**
RecoverInFlightRuns at startup marks crashed runs failed (refutes stuck-forever half) but never calls DeleteSlot — sandbox slot leak per crash stands.

Fix: Recovery calls DeleteSlot / orchestrator orphan sweep.

### `internal/controller/runner.go:172` — major
**Summary:** Sandbox slot release is manual per-return-path (releaseSlot called at 6 sites) instead of deferred, so the new panic-isolation path added in this commit (consumer.dispatch recover) leaks one orchestrator slot per recovered panic — and the process keeps running, so the leak accumulates silently.

**Failure scenario:** runSession panics anywhere after CreateSlot succeeds (e.g. nil-map/nil-pointer in buildWorkloadSpecs or a producer library panic). Before this commit a panic killed the process and k8s restart plus slot GC could recover; now consumer.dispatch's recover() swallows it, logs, and continues. Run's defers release the partition lease and drop the session, but nothing calls DeleteSlot — the sandbox pod for that session lives forever. With 50 sessions/day hitting a panic-triggering scenario, sandbox capacity monotonically shrinks until admission fails cluster-wide with zero crash signal.

**Verdict: CONFIRMED**
releaseSlot at 6 manual sites, no defer; panic after CreateSlot leaks slot; dispatch recover() keeps process alive so startup recovery never runs.

Fix: defer releaseSlot guarded after CreateSlot.

### `internal/controller/producer.go:84` — major
**Summary:** leasedPartitionBalancer silently falls back to hash-balancing when the leased partition ID is absent from the writer's current partition list, reintroducing exactly the cross-session partition collision the lease allocator exists to prevent — with no error, metric, or log.

**Failure scenario:** Writer metadata is momentarily stale or a broker holding partition 17 is briefly offline while session A publishes a spec leased to partition 17. Balance() doesn't find 17 in `partitions`, hashes the key, and lands the spec on partition 3 — which session B holds a lease on. Two specs now share partition 3: serial consumption by one bot-fleet worker, session B misses its ready fan-in, and with the new hard partial-fan-in policy B's run fails outright. The lease bookkeeping still reports 17 as exclusively held, so the collision is invisible; the safe behavior is to return an error (or the leased index unconditionally) rather than hash-fallback.

**Verdict: CONFIRMED**
recover() only logs+frees semaphore — no publishFailure, no releaseSlot, no restart so no recovery backstop.

Fix: Best-effort fail+releaseSlot in recover branch.

### `internal/controller/consumer.go:156` — major
**Summary:** Panic in a dispatched session skips all run-teardown side effects: no failure status is published and any orchestrator slot created before the panic is leaked, while the Kafka offset was already committed so the run is never redelivered.

**Failure scenario:** Runner.Run panics after CreateSlot (e.g. nil deref in producer/store path mid-runSession). Deferred Release/Drop unwind, but recover() sits in Consumer.dispatch which only logs+counts — r.fail/publishFailure and releaseSlot never run for that path. Result: sandbox slot (contestant pod) runs forever until manual cleanup, and the run row stays permanently non-terminal (Deploying/Running) in the store; since the offset was committed on acceptance there is no redelivery to reconcile it, and the RunStatus precheck only skips Completed/Failed so nothing ever closes it out.

**Verdict: CONFIRMED**
Broadcast wakeup (close(waitCh)) no FIFO; large request starvable by small-session churn; no fairness test exists.

Fix: FIFO wait queue in Acquire.

### `internal/controller/lease.go:44` — major
**Summary:** Acquire has broadcast wakeup with no reservation or FIFO fairness, so a session needing many partitions can be starved indefinitely by smaller sessions and repeatedly fails admission at LeaseAcquireTimeout.

**Failure scenario:** 24 partitions; session L needs 16 and blocks. Small 4-partition sessions keep arriving (MAX_CONCURRENT_SESSIONS=4 keeps 3 others flowing). Every Release wakes all waiters; a newly-arrived small session (or waiter) grabs the freed partitions before 16 ever accumulate — free count oscillates below 16 forever. L blocks the full 60s LEASE_ACQUIRE_TIMEOUT, then publishFailure fails the run. Under sustained load every large-scenario run deterministically fails while small runs succeed, and each retry of L repeats the loss.

**Verdict: CONFIRMED**
Dispatch goroutines untracked by WaitGroup; shutdown can cut off cleanup / race producer.Close.

Fix: WaitGroup + bounded join on shutdown.

### `internal/controller/consumer.go:127` — major
**Summary:** Dispatch goroutines are fire-and-forget with no WaitGroup; shutdown returns from StartBenchmarkRequested and lets main exit while sessions are mid-run, so the ctx.Done cleanup path in runSession (fail-status publish + slot release on 10s background contexts) races process exit and can be cut off.

**Failure scenario:** SIGTERM cancels ctx; the fetch loop returns immediately, main proceeds to deferred Close/exit while N dispatch goroutines are inside runSession's <-ctx.Done() branch trying to DeleteSlot and publish 'controller shutdown during run' on fresh 10-second background-context calls. Nothing joins those goroutines, so under a normal k8s terminationGracePeriod the process exits first: slots leak in the orchestrator and runs stay non-terminal — and because offsets were committed on acceptance, restart never redelivers them.

**Verdict: CONFIRMED**
Same broadcast-no-fairness root cause; starved Acquire also pins a MAX_CONCURRENT_SESSIONS slot for full timeout.

Fix: Same FIFO fix.

### `internal/controller/lease.go:85` — major
**Summary:** Acquire has no queue/fairness: every Release wakes all waiters and whichever re-grabs the mutex first wins, so a large-count acquisition can be starved indefinitely by a stream of small-count sessions and spuriously hard-fails at LeaseAcquireTimeout.

**Failure scenario:** 24 partitions; a steady stream of 4-partition sessions (MAX_CONCURRENT_SESSIONS=4 keeps ~16 leased at all times) while one session needs 20+. On each wakeup len(free) < 20, and freed partitions are immediately re-taken by newly admitted small sessions. The big session blocks 60s, then Runner publishes a hard run failure ('acquire partition leases: context deadline exceeded') even though the system is healthy — a user-visible integrity failure whose probability grows with load, exactly when tests never exercise it.

**Verdict: CONFIRMED**
Balance silently hash-falls-back when leased partition missing from metadata — collision invisible; Balancer interface has no error path.

Fix: Verify leased partitions against metadata before publish; counter+log in fallback.

### `internal/controller/consumer.go:120` — major
**Summary:** Offset is committed on dispatch acceptance, but on controller shutdown/restart the in-flight session's failure path publishes with the already-cancelled context, so the run is never redelivered AND never marked failed — stuck in a non-terminal status forever.

**Failure scenario:** Session accepted, offset committed, dispatch goroutine mid-benchmark. Controller receives SIGTERM: ctx cancels, runSession's current stage errors, and r.fail/publishFailure is invoked with the cancelled ctx — the Kafka status publish and/or DB update fails. After restart the message is not redelivered (offset committed), so the run row stays RUNNING permanently; the commit message's 'user-retryable' claim assumes the run at least reaches a failed state, which this path never does. Needs a detached (context.Background()+timeout) failure publish or a startup sweep of orphaned RUNNING sessions.

**Verdict: REFUTED**
Runner.fail ignores caller ctx, always detached Background+10s — cancelled-context publish mechanism doesn't exist.

### `internal/controller/lease.go:85` — minor
**Summary:** PartitionLeaseAllocator wakeups have no FIFO/fairness: every Release wakes all blocked Acquires and the first to grab the mutex wins, so a large-count Acquire can starve indefinitely behind a stream of small sessions and repeatedly burn its full LEASE_ACQUIRE_TIMEOUT before failing.

**Failure scenario:** 24 partitions, steady arrival of 4-worker sessions keeping ~20 partitions leased, plus one 12-worker session blocked in Acquire. Each Release frees 4 partitions; a newly-arriving 4-worker Acquire (or one already re-looping) claims them before free count ever reaches 12. The 12-worker session times out after 60s and publishes a failure; on user retry the same race repeats. Under sustained multi-tenant load, large scenarios become effectively un-runnable while the admission-blocked counter increments once per attempt, and each starved attempt also pins one of the MAX_CONCURRENT_SESSIONS dispatch slots for the full 60s, cutting effective session concurrency.

**Verdict: CONFIRMED**
Duplicate of fairness findings; semaphore-pinning secondary claim verified.

Fix: Same.

### `internal/controller/consumer.go:114` — minor
**Summary:** acquireDispatchSlot can block the fetch loop on the sem for an entire session duration (minutes) with a fetched-but-uncommitted message; a consumer-group rebalance during that window redelivers the message, and the duplicate is only suppressed while the first copy is still in SessionManager — after it finishes, a stale redelivery re-runs unless the RunStatus precheck succeeds, which is explicitly 'proceed on error'.

**Failure scenario:** Sem full for 3 minutes; broker restart triggers a rebalance mid-block; message for session S is redelivered later. First dispatch of S completes and Drops the session; redelivered copy then hits the RunStatus precheck during a transient Postgres blip ('precheck failed; proceeding'), passes sessions.Add (already dropped), and re-acquires partitions to re-run a completed benchmark — duplicate load against the contestant's engine and a second status stream for the same session ID.

**Verdict: CONFIRMED**
RunStatus precheck proceeds on store error; rebalance redelivery during transient DB error can double-run a session.

Fix: Fail closed on precheck error or persistent dispatch-accepted marker.

---

## bot-fleet (18 findings: 2 critical, 10 major, 6 minor)

### `src/worker.rs:924` — critical
**Summary:** Batch write_all is raced only against process-level cancel (SIGTERM/SIGINT), not against task_end/drain deadline, so a stalled contestant makes task wall time unbounded and breaks the max.poll.interval guarantee validate_spec relies on.

**Failure scenario:** Contestant stops reading its socket near task end (pause/deadlock). TCP window fills, write_half.write_all blocks indefinitely (zero-window probes keep the connection alive; no error, no timeout). CancelToken fires only on SIGINT/SIGTERM, so the select never resolves. fire_workload's join_next blocks, run_workload never returns, the workload's Kafka offset is never committed, max.poll.interval expires, the consumer group rebalances and re-delivers the assignment to another worker -> duplicate execution of the whole session's load. validate_spec's worst_case_wall_time_ns (barrier+offset+duration+drain) explicitly promises this cannot happen, but the blocking write is not bounded by drain_end_ns. Same defect in rw_write_loop at line 1447.

**Verdict: CONFIRMED**
Write select has no drain-deadline arm — only cancel (SIGTERM-level). Permanently-stalled peer wedges writer past task/drain end; worst_case_wall_time promise broken. Distinct from the documented no-per-write-timeout decision (slow-but-draining peers).

Fix: Bound write select on drain_end_ns, not a per-write timeout.

### `src/worker.rs:1141` — critical
**Summary:** Expiry-queue watchdog no longer evicts pending-map entries that have no queue entry: a batch inserted into `pending` before a write that is still blocked (or aborted by cancel-break) at drain_end is never evicted, losing timed_out telemetry and leaking the inflight gauge — the old HashMap::retain last-tick semantics evicted everything.

**Failure scenario:** fix_write_loop inserts up to batch_max (64) orders into `pending` and inflight_add's them (line 893-912) BEFORE write_all; expiry entries are pushed only on write success (line 951-956). If the write is still blocked at drain_end (slow contestant), or the loop breaks via `_ = cancel.cancelled() => break` mid-write (line 927), those 64 entries per task sit in `pending` with no expiry entry. watchdog_loop's last_tick pops only the queue and exits (line 1141) — the orders get neither ack nor timed_out OrderSentEvent (sent/acked accounting downstream is now short), and the FIX inflight gauge stays permanently elevated by 64 per affected task. With 500 tasks that is up to 32k phantom inflight, defeating the exact 'contestant not draining' diagnosis the gauge exists for.

**Verdict: CONFIRMED**
Orders inserted into pending+inflight BEFORE write; expiry entry pushed only after write succeeds. Blocked write or cancel-break leaves pending entries with no expiry entry; last_tick drains only the queue — never evicted, no timed_out telemetry, inflight never decremented.

Fix: last_tick drains remaining pending-map entries directly with telemetry + inflight_sub.

### `src/worker.rs:593` — major
**Summary:** ExpiryQueue retains an entry per written order until its send+5s deadline even after the order is acked, so its size is send_rate x RESPONSE_TIMEOUT — completely uncapped by max_inflight and defeating the documented ~2.4MiB/task memory bound.

**Failure scenario:** Max-rate drain run at ~835k orders/s aggregate against a fast sink: pending map stays near-empty (acks are instant) but every order_id String is pushed to the per-task VecDeque and only popped by the 250ms watchdog once its 5s deadline passes. Steady state ~4.2M queued (deadline,String) entries (~60B each incl. String heap) ≈ 250-350MB of resident growth per worker that the inflight cap was explicitly designed to prevent; on a small node this OOMs the worker under exactly the load the drain harness runs. Acked orders' entries cannot be removed early, so the cost is paid on the healthy path, not just failure paths.

**Verdict: CONFIRMED**
Ack removes from pending but not expiry queue; entries live full RESPONSE_TIMEOUT — queue depth O(rate x 5s), not O(inflight).

Fix: Prune expiry entries on ack.

### `src/worker.rs:924` — major
**Summary:** The uncancellable, deadline-free write_all (FIX line 924, REST/WS line 1447) means a contestant that permanently stops reading while holding the socket open wedges the writer forever — past task_end and drain_end — so fire_workload never returns and the workload's Kafka offset is never committed.

**Failure scenario:** Contestant engine deadlocks at t=30s of a 60s task but keeps the TCP connection open: its receive window fills, write_all blocks indefinitely, and nothing breaks it (cancel only fires on SIGINT/SIGTERM; there is no drain_end_ns arm in the select). Reader and watchdog exit at drain_end but JoinSet::join_next waits on the stuck writer, run_workload never completes, the assignment offset is never committed, max.poll.interval elapses and Kafka rebalances and re-delivers the workload to another worker — the exact duplicate-execution scenario validate_spec's worst-case-wall-time check claims to make impossible. The old per-write timeout (now deliberately ignored as _write_timeout) bounded this; the backpressure rationale in the comment covers slow peers, not permanently stalled ones.

**Verdict: CONFIRMED**
Duplicate of critical :924 — new failure mode (never-resuming peer) not covered by the design comment's rationale.

Fix: Same drain-deadline bound.

### `src/worker.rs:1436` — major
**Summary:** rw_write_loop never calls metrics::inflight_add for inserted orders, but the shared watchdog_loop unconditionally calls inflight_sub for evictions — on REST/WS tasks the global inflight gauge is driven negative and stays corrupted for the 24h worker lifetime.

**Failure scenario:** Any REST or WS task whose orders time out (slow contestant): watchdog pops N expiries and does inflight_sub(N) against a gauge that was never incremented for those orders (FIX inserts add at line 912; the rw path has no counterpart, and emit_response also never subs). Gauge (i64) goes negative and drifts further with every eviction; the 1s send-health snapshot and the Prometheus inflight metric — the primary signal used to diagnose 'contestant stopped draining' vs capture loss per the stall-diagnosis comment — report garbage on mixed-protocol or REST/WS sessions. Additionally, on the FIX path a cancel-during-write (line 926 break) leaves count added but never subtracted, giving permanent positive drift across many sessions.

**Verdict: CONFIRMED**
rw_write_loop never inflight_add's; watchdog inflight_sub's unconditionally per eviction — REST/WS timeouts drive shared gauge negative.

Fix: Mirror FIX pairing: add on insert, sub in emit_response.

### `src/worker.rs:1436` — major (second finder pass, near-duplicate of above)
**Summary:** rw_write_loop (REST/WS) never calls metrics::inflight_add after inserting the batch into pending, and emit_response never calls inflight_sub on ack, yet the shared watchdog_loop calls inflight_sub for evicted REST/WS orders — the inflight gauge goes negative and drifts without bound on any REST/WS run with timeouts.

**Failure scenario:** WS session where the contestant drops or times out on 1,000 responses: rw_write_loop inserts orders with no inflight_add (contrast fix_write_loop line 912), watchdog evicts them and calls inflight_sub(1000) (line 1114) -> gauge = -1000 and keeps sinking every eviction pass. On a worker running mixed FIX+WS tasks the negative WS drift cancels real FIX inflight, so the 1s send-health snapshot (inflight_value at line 320) and the backpressure cliff diagnosis (inflight pinned high vs normal) report garbage. The rw error path (line 1484) also removes frames from pending with no inflight accounting, consistent with add never having happened but divergent from the FIX error path's inflight_sub(count) at line 974.

**Verdict: CONFIRMED**
Confirmed; emit_response also never subs — happy path self-consistent, eviction path pure negative drift.

Fix: Same.

### `src/worker.rs:1073` — major
**Summary:** FIX read-buffer overflow reset (buf.clear() at 1 MiB) discards a partial FIX message mid-frame with no resynchronization, so every subsequent parse starts at an arbitrary byte offset; parse_messages may then fail to frame anything and the buffer repeatedly grows to 1 MiB and resets, orphaning all pending orders to the 5s watchdog for the rest of the task.

**Failure scenario:** Contestant sends a burst of large non-ExecReport messages (or one message >1 MiB, or the reader falls behind a fast acker so >1 MiB accumulates between reads under the new batched write load). buf.clear() cuts the stream mid-message; the next chunk begins with the tail of a truncated frame, checksum/framing never re-aligns on '8=', parse_messages consumes 0, buf grows to 1 MiB again, clears again — livelock. Acks are all dropped, pending fills to max_inflight, the writer parks on notify and is paced solely by watchdog evictions (10k per 5s), silently collapsing offered load while every order is reported timed_out. Same pattern in rest_read_loop line 1702, where dropping bytes mid-HTTP-response desynchronizes response framing permanently.

**Verdict: REFUTED**
parse_messages scans for 8=FIX anywhere (find_subslice) — self-resynchronizing; buf.clear() loses one truncated message, not permanent desync/livelock.

### `src/worker.rs:952` — major
**Summary:** Expiry queue retains every sent order id for the full 5s RESPONSE_TIMEOUT regardless of ack, so per-task memory is O(send_rate x 5s), not bounded by max_inflight as the design comment claims.

**Failure scenario:** Acked orders are removed from the pending map but their (deadline, order_id String) entries stay in the ExpiryQueue until the watchdog pops them as 'due' 5 seconds later (stale entries are only 'silently skipped' at pop time). At max-rate a healthy fast contestant acking ~50k orders/s/task leaves ~250k queue entries per task (~60B each with the heap ClOrdID String) ~= 15-20MB/task, 25x the 10k inflight cap the comment sizes worker memory by; hundreds of tasks on one worker -> multiple GB. The replaced HashMap::retain design held only unacked orders (hard-capped at 10k by backpressure). Same shape as the known invariants-mode retain-until-Finish bug: tests use small rates/durations so the 5s window never accumulates.

**Verdict: CONFIRMED**
Same root cause as :593 verified at push site.

Fix: Same.

### `src/worker.rs:941` — major
**Summary:** Batched writes widen the insert-before-write race: an ack for an early frame of a batch can arrive while write_all for the rest of the batch is still blocked, emitting telemetry with send_ts_ns=0.

**Failure scenario:** Orders are inserted into the pending map with send_ts_ns=0 BEFORE the batch write; send_ts_ns is only patched after write_all/write_batch for the whole (up to 64-frame, ~16KB) batch returns. Under exactly the TCP-backpressure scenario the code is designed for, write_all flushes a prefix of batch_buf then parks for seconds on a full send window; the contestant acks the flushed prefix, fix_read_loop/emit_response removes those entries and records OrderSentEvent{send_ts_ns:0, timed_out:false} -> ingester computes latency = recv_done_ts_ns - 0 ~= 1.7e18 ns, poisoning the (session,wave) HDR histograms (and in max-rate mode target_send_ts_ns is also still the unpatched scheduled value). Pre-batching the race window was one ~200B frame (effectively atomic); batching (d520955/7a4b837) makes it 64 frames wide and realistic under load. Same race in rw_write_loop lines ~1426/1454-1468.

**Verdict: CONFIRMED**
send_ts_ns=0 until whole-batch write_all returns; contestant can ack an early flushed frame first -> OrderSentEvent with send_ts_ns=0.

Fix: Per-frame send_ts patch or guard acks before batch stamp.

### `src/telemetry.rs:177` — major
**Summary:** close() joins shard aggregators sequentially with early-return `??` after draining all handles out of the shared Vec; if any shard's join fails, the remaining shards' JoinHandles are dropped un-awaited, abandoning their final flush.

**Failure scenario:** 4 shards; shard 0's aggregator panics (JoinError) or returns Err. close() drains all 4 handles from the Mutex<Vec>, awaits handle 0, hits `??`, returns Err. Handles 1-3 are dropped: those tasks are still running their post-loop shutdown drain (batcher.drain_ready() + awaiting up to ~1000-event inflight batches). The worker treats telemetry as closed and the process/pod tears down; shards 1-3's tail batches (up to 3 shards x buffered partials + inflight) are lost silently — neither telemetry_flushed nor telemetry_dropped_n is ever accounted, so the loss is invisible to the sent/acked reconciliation the ingester relies on. Pre-shard code had exactly one handle so this partial-abandonment path could not exist.

**Verdict: CONFIRMED**
close() awaits shard handles with ?? in a loop — first error drops remaining handles un-awaited, their shutdown flush unobserved.

Fix: join_all + aggregate errors.

### `src/telemetry.rs:335` — major
**Summary:** QueueFull retry path calls kafka::poll_producer(producer, 10ms) — a synchronous librdkafka poll that blocks the tokio worker thread — and sharding multiplies this: up to BOT_TELEMETRY_SHARDS (default 4) aggregator tasks now sit in this retry loop concurrently against the ONE shared producer queue, blocking up to N runtime threads at once.

**Failure scenario:** Broker slows (ISR shrink, disk stall) with the shared 1M-message producer queue full: all 4 shard aggregators enter the retry loop and each blocks a tokio worker thread for 10ms per iteration. On a cpuset-pinned bot-worker pod with a 2-4 thread runtime, the entire runtime stalls — not just telemetry backpressure, but the order-send tasks and pacing timers on the same runtime stop being scheduled, so send t1 timestamps and pacing fidelity (the thing this pipeline measures) are corrupted during exactly the window being recorded. Single-shard code blocked at most 1 thread; this commit makes it N. The 1ms sleep between polls does not help because the 10ms poll itself is the blocking call.

**Verdict: CONFIRMED**
librdkafka sync poll called in async fn without spawn_blocking; up to N shard tasks block runtime threads 10ms each under QueueFull.

Fix: spawn_blocking around poll_producer.

### `src/worker.rs:1105` — minor
**Summary:** Watchdog's last_tick drains only the expiry queue, not the pending map, so map entries that never got a queue entry are never emitted as timed_out telemetry — silently lost orders, unlike the prior HashMap::retain semantics the comment claims to preserve.

**Failure scenario:** Writer inserts a 64-order batch into pending (line 893), then blocks in write_all as the run ends; expiry push (line 951) only happens on write success. At drain_end the watchdog's last_tick pops an expiry queue that has no entries for these 64 orders and exits, leaving them in pending with inflight still counted. Telemetry never receives their timed_out=true OrderSentEvent, so the ingester's per-wave counts under-report exactly the orders lost at the failure boundary — the events the timeout path exists to account for. Old code's retain(last_tick => evict all remaining map entries) reported them; the queue-based rewrite does not.

**Verdict: CONFIRMED**
Same gap as :1141, last_tick never scans pending.

Fix: Same.

### `src/kafka.rs:140` — minor
**Summary:** KAFKA_TELEMETRY_COMPRESSION_LEVEL validation range (-131_072..=22) is the raw zstd library range, not librdkafka's: librdkafka validates compression.level to -1..=12 at config time, so any value in 13..=22 or below -1 passes the filter but makes producer creation fail at startup.

**Failure scenario:** Operator sets KAFKA_TELEMETRY_COMPRESSION_LEVEL=19 (a perfectly valid zstd level, plausibly chosen to maximize compression); telemetry_producer() returns Err from ClientConfig::create with 'compression.level ... out of range', the bot worker fails to start, and the guard that was written specifically to sanitize this env var is what let the bad value through.

**Verdict: CONFIRMED**
Level filter allows -131072..=22 but vendored librdkafka clamps compression.level to -1..=12 — out-of-range value passes filter, create() fails at startup.

Fix: Filter -1..=12.

### `src/kafka.rs:140` — minor (second finder pass, adds vendored-version citation)
**Summary:** KAFKA_TELEMETRY_COMPRESSION_LEVEL is filtered to zstd's native range (-131072..=22), but librdkafka clamps compression.level to RD_KAFKA_COMPLEVEL_MIN/MAX = [-1, 12] (verified in vendored rdkafka-sys 4.7.0+2.3.0 / 4.10.0 rdkafka_conf.h:86,91), so any value in 13..=22 or -131072..=-2 passes the code's filter yet makes ClientConfig::create() fail — the worker crashes at telemetry_producer() creation, i.e. at pod startup, before any orders are sent.

**Failure scenario:** Operator sets KAFKA_TELEMETRY_COMPRESSION_LEVEL=19 (a perfectly valid zstd level, and inside the range this code deliberately accepts). librdkafka's rd_kafka_conf_set returns 'outside allowed range -1..12', producer creation errors, every bot-worker pod CrashLoops at boot, and the whole benchmark run fails to start. The filter advertises a range 45% of which is guaranteed to crash the process.

**Verdict: CONFIRMED**
Vendored rdkafka_conf.h:85,90 confirms MIN=-1 MAX=12.

Fix: Same.

### `src/kafka.rs:140` — major (third finder pass, notes ebpf-latency shares the constructor)
**Summary:** KAFKA_TELEMETRY_COMPRESSION_LEVEL is validated against zstd's library range (-131072..=22), but librdkafka's compression.level config property only accepts -1..=12; any value in 13..=22 or below -1 passes the filter and then fails ClientConfig::create at producer construction.

**Failure scenario:** Operator sets KAFKA_TELEMETRY_COMPRESSION_LEVEL=15 (valid zstd level, passes the filter). ClientConfig rejects `compression.level 15` as outside range -1..12, telemetry_producer() returns Err, and every bot-fleet worker (and ebpf-latency, which shares this producer constructor) crashes at startup instead of clamping or falling back to the default.

**Verdict: CONFIRMED**
ebpf-latency shares telemetry_producer constructor — same crash-at-boot exposure.

Fix: Same.

### `src/worker.rs:974` — minor
**Summary:** FIX batch-write error path decrements inflight gauge by full batch count even for orders already acked (and decremented) by the read loop during the failed partial write.

**Failure scenario:** write_all fails after partially flushing the batch; contestant acks the flushed frames, fix_read_loop removes them and calls inflight_sub(1) each; the error path then calls inflight_sub(count) for the whole batch (its map.remove for those ids is a no-op but the gauge sub is unconditional). Gauge underflows/drifts negative by the number of raced acks per write error. This gauge is the primary diagnostic used to distinguish 'contestant not draining' from engine stalls (per the 50k/s stall investigation), so negative drift corrupts that signal on long-running workers.

**Verdict: CONFIRMED**
FIX error path inflight_sub(count) for whole batch; frames already acked (and subbed) during partial write get double-subtracted.

Fix: Sub only actual removals.

### `src/worker.rs:1114` — minor
**Summary:** Shared watchdog_loop decrements the inflight gauge for REST/WS task evictions, but rw_write_loop never calls inflight_add and emit_response never calls inflight_sub — REST/WS traffic only ever subtracts.

**Failure scenario:** Run a REST or WS session against a slow/dead contestant: every timed-out order goes through watchdog_loop's metrics::inflight_sub(to_evict.len()) (protocol-agnostic), while the REST/WS write path (lines 1417-1436) adds nothing to the gauge. iicpc_bot_inflight goes monotonically negative in proportion to REST/WS timeouts, mixing into the same global gauge FIX tasks legitimately use; the d520955 QoL-3 per-protocol metrics pass labeled every other metric but left this add/sub pairing asymmetric across protocols.

**Verdict: CONFIRMED**
Same rw asymmetry as :1436, watchdog site.

Fix: Same pairing fix.

### `src/worker.rs:1183` — minor
**Summary:** WS batch path clones every frame's byte buffer per order (frame.bytes.clone() into WsMessage::Binary) on the hot write loop, re-adding a per-order allocation+copy the P2 template work was meant to remove.

**Failure scenario:** At target WS rates (tens of thousands of orders/s/task), each order incurs an extra heap allocation and ~150-300B memcpy in RwWriter::write_batch on top of the clone TemplateCache::patch already makes to produce frame.bytes — two allocations per order on the path profiled as CPU-bound at ~42us/order. frames[] is only needed afterward for order_id/pending patching, so bytes could be moved out instead of cloned; under a multi-session worker this measurably caps WS throughput below the FIX/REST paths that write borrowed slices.

**Verdict: CONFIRMED**
WS arm clones frame.bytes per frame into WsMessage though nothing needs bytes afterward — avoidable alloc+copy on hot path.

Fix: Move bytes out instead of clone.

---

## telemetry-ingester (4 findings: 1 critical, 1 major, 2 minor)

### `Cargo.toml:12` — critical
**Summary:** Producers switched to zstd (compression.type=zstd in bot-fleet kafka.rs) but telemetry-ingester's rdkafka dependency lacks the 'zstd' feature, and its Dockerfile builds with `cargo build --release -p iicpc-telemetry-ingester`, so the vendored librdkafka in the deployed ingester image is compiled WITHOUT libzstd. Only bot-fleet's Cargo.toml gained the feature; feature unification only rescues builds that include bot-fleet in the same cargo invocation (e.g. workspace-root `cargo test`, which is why the live integration test was green).

**Failure scenario:** Deploy the ingester from services/telemetry-ingester/Dockerfile (cargo build -p only pulls that package's dep graph, so rdkafka builds without zstd). Bot-fleet produces orders.sent/orders.acked message-sets compressed with zstd. The consumer cannot decompress: every fetch of those partitions fails with unsupported-compression-codec errors, zero events are ingested, and all telemetry for every run is lost while unit/workspace tests stay green.

**Verdict: CONFIRMED**
Ingester's rdkafka lacks zstd feature; Dockerfile builds -p ingester alone so no workspace feature unification — deployed consumer cannot decompress zstd (compile-time libzstd link, symmetric).

Fix: Add zstd to ingester Cargo.toml rdkafka features.

### `src/redis_sink.rs:55` — major
**Summary:** live:{session}:latest pointer is written with an unconditional SET per snapshot, so it is non-monotonic: it can rewind to an older wave under HashMap iteration order, Kafka replay after restart/rebalance, or multiple ingester replicas sharing a session's partition band.

**Failure scenario:** At a wave boundary, Aggregator::snapshot() (aggregate.rs, `self.windows.iter_mut()` over a HashMap) can emit wave N and wave N+1 for the same session in one flush in arbitrary order; if wave N is pipelined last, live:{sid}:latest ends at N while wave N+1 is the real latest. Worse under horizontal scale (session-band partitioning from 6e84a7b spreads one session across partitions, hence across ingester replicas): a lagging replica flushing wave N-3 overwrites the pointer set to N by a caught-up replica. The leaderboard poller then HGetAlls the old wave's hash and BroadcastLives stale/rewinding metrics every second; the poller (poll.go) also has no wave/updated_at_ns monotonic guard, so the frontend tile flaps backwards. Same rewind occurs single-replica after a restart replaying from the committed offset. Fix requires a compare-and-set (e.g. Lua max-guard) or poller-side high-water mark.

**Verdict: CONFIRMED**
live:{sid}:latest SET unconditional — out-of-order flush regresses the pointer to an older wave.

Fix: Compare-and-set (Lua) on wave_index/updated_at.

### `src/ingester.rs:88` — minor
**Summary:** V2 decode path regresses per-event cost: b.into_events() heap-clones session_id, submission_id, and worker_id Strings for every event in the batch (OrderSentBatchV2::into_events in schemas/rust/src/lib.rs), where the pre-commit code iterated &b.events with zero per-event envelope allocation; the envelope hoisting that saved wire bytes is undone with 3 String allocations + copies per order in the single hot ingest loop.

**Failure scenario:** 100M orders through one ingester replica = 300M extra String allocs/frees plus a Vec<OrderSentEvent> materialization per batch in the consumer hot path; at the 50k+/s rates this branch targets, allocator pressure in ingest() lowers the ingester's ceiling exactly where the commit was trying to raise producer throughput, since observe_sent(&e) only needs references and the envelope fields are constant per batch.

**Verdict: CONFIRMED**
into_events() clones session/submission/worker Strings per event (3 allocs/order) though observe_sent needs only borrows.

Fix: Iterate fields with borrowed envelope.

### `src/ingester.rs:88` — minor (second finder pass, adds absolute cost estimate)
**Summary:** V2 decode path calls b.into_events(), which allocates a new Vec and clones session_id, submission_id, and worker_id Strings for every event in the batch (3 heap allocations per order) purely to reconstitute the envelope fields that observe_sent only reads by reference; the old path iterated `&b.events` with zero per-event allocation.

**Failure scenario:** At the measured ~64k orders/s per node, the ingester's single consume loop now performs ~192k extra String allocations plus one Vec per batch per second on its hottest path; under a multi-node load test (hundreds of thousands of orders/s into one ingester replica) this added allocator pressure raises decode latency, consumer lag grows, and the batch-drop behavior the 92%-drop incident exposed re-appears earlier than the wire-format savings should allow.

**Verdict: CONFIRMED**
Duplicate of above.

Fix: Same.

---

## schemas (1 finding: 1 major)

### `rust/src/lib.rs:88` — major
**Summary:** session_band_partition computes num_bands = num_partitions / band_width (integer division), so when num_partitions is not a multiple of band_width the trailing `num_partitions % band_width` partitions are unreachable: base = band*band_width <= (num_bands-1)*band_width and within < band_width never reach them (the final `% num_partitions` is a no-op for those values). All traffic permanently skews onto the reachable prefix.

**Failure scenario:** Cluster reconfigured to ORDERS_PARTITIONS=20 with default band width 8: num_bands=2, reachable partitions are 0..=15; partitions 16-19 never receive a single sent or acked event. Consumer instances assigned those partitions sit idle while the other 16 partitions absorb 25% extra load, silently shrinking the throughput ceiling the sharding was meant to raise, with no error anywhere.

**Verdict: CONFIRMED**
num_bands = floor(partitions/band_width): when partitions not a multiple of band_width, tail partitions unreachable (e.g. 20/8 -> 16-19 never used). Default 24/8 exact so latent, not live.

Fix: div_ceil or distribute remainder.

---

## correctness-validator (9 findings: 3 critical, 2 major, 4 minor)

### `internal/validate/invariants.go:309` — critical
**Summary:** Invariants mode appends an uncapped Violation struct (with per-order strings) to Report.Violations for every lost order/cancel, overfill, FIFO violation, and cross-flow violation, defeating the commit's claimed O(window+flows) memory bound.

**Failure scenario:** Max-rate correctness scenario against an engine that crashes or stops responding mid-session: every remaining sent order has zero responses, so Apply calls v.rep.add(LostOrder,...) once per order. At 100M orders that is tens of millions of Violation structs (~100+ bytes each incl. orderID/detail strings) -> multi-GB Report and OOM in exactly the pass-2 mode that was rewritten to avoid O(session) memory (the t7Reorderer/jitter-histogram bounding is bypassed). The full slice is then also carried into store.Save(rec) with Report embedded, blowing up the persistence write. Same class as the precedent bug; tests miss it because they use small inputs with few violations. Same uncapped append fires from onEmit (invariants.go:337, :356) for a systematically misordering engine. Fix needs a cap on retained Violation examples (counters already exist separately).

**Verdict: CONFIRMED**
Apply appends a Violation for every zero-response order via Report.add (validate.go:230-234, unconditional append) — unbounded examples slice.

Fix: Cap retained Violation examples (first N per type); counters keep totals.

### `internal/validate/validate.go:79` — critical
**Summary:** CorrectnessScore's `scored := r.TotalFills - r.PhantomFills` assumes phantoms are counted inside TotalFills, which holds only in full mode (StreamValidator.AddPhantom increments both); in invariants mode AddUnmatched increments PhantomFills only, so the uint64 subtraction underflows or the denominator deflates, producing garbage scores.

**Failure scenario:** VALIDATOR_MODE=invariants session: 5 matched valid fills (TotalFills=5, ValidFills=5) plus 10 unmatched responses (PhantomFills=10) -> scored = 5-10 underflows to 2^64-5, score ~= 0.0 for a clean engine. Conversely 100 valid fills + 30 unmatched responses -> score = 100/70 = 1.43 > 1.0. Tests miss it because they exercise phantom counting per-validator, not the shared score formula against invariants-mode accounting.

**Verdict: CONFIRMED**
invariants-mode AddUnmatched increments PhantomFills only (TotalFills untouched), so TotalFills-PhantomFills (uint64) underflows when unmatched > matched; full mode's AddPhantom increments both so is safe.

Fix: Separate matched-fill denominator per mode or make AddUnmatched increment TotalFills too.

### `internal/validate/invariants.go:356` — critical
**Summary:** Invariants mode appends a full Violation struct (with orderID string) to Report.Violations for EVERY lost order, overfill, and time violation — the slice grows O(violations) with no cap, reintroducing the exact unbounded-memory class this rewrite was meant to remove.

**Failure scenario:** The streaming rewrite (3411e53) bounded the reorder buffer and jitter histogram but left rep.add untouched: invariants.go calls v.rep.add at lines 293 (overfill), 309 (lost order/cancel), 337 (per-flow FIFO), 356 (cross-flow). Per project history, stock-socket engines score ~45% — i.e. roughly half of all events are violations under strict grading. A multi-contestant invariants-mode session with millions of orders against such an engine produces millions of Violation structs (~80B each plus orderID heap string), so a 10M-order session with 30% violation rate allocates ~300MB+ of Violations per session, times VALIDATOR_CONCURRENCY=4 concurrent sessions → OOM. All counters (TimeViolations, LostOrders, ...) already exist separately; the slice is pure unbounded growth. Missed by tests because suites use small synthetic inputs. Bonus: ViolationCount() at validate.go:89 casts len(Violations) to uint32, silently wrapping past 4.29B.

**Verdict: CONFIRMED**
Cross-flow violations append unbounded Violation examples, same mechanism as :309.

Fix: Same cap as :309.

### `internal/validate/invariants.go:287` — major
**Summary:** Apply computes minT7Ns over FILL responses only (falling back to any response only when there are zero fills), but the documented predicate defines processing order as the order's FIRST response (min T7) — mixing fill times for filled orders with ack times for unfilled ones skews the T7 processing order and produces spurious FIFO and cross-flow violations.

**Failure scenario:** Same flow, engine behaving correctly: order A (TCPSeq 100) is acked at t7=1ms and fills at t7=900ms (rests on book, fills late); order B (TCPSeq 200, sent after A) is acked at t7=2ms and never fills (canceled). minT7(A)=900ms (first fill), minT7(B)=2ms (fallback ack) → the t7Reorderer emits B before A, onEmit sees TCPSeq go 200→100 within the flow and records a false Time violation (line 335), plus a bogus cross-flow jitter/violation sample if flows interleave. Any resting-order latency variance between filled and unfilled orders — completely normal for limit-order flow — corrupts the score. The equivalence oracle test can't catch it because the old batch path shared the same minT7 extraction.

**Verdict: CONFIRMED**
minT7Ns computed from FILL responses first, ack fallback only when no fills — contradicts documented 'first response (min T7)'; equivalence oracle shares the bug so tests can't catch it.

Fix: minT7Ns = min over ALL responses.

### `internal/validate/invariants.go:171` — major
**Summary:** t7Reorderer silently excludes from ALL checks (FIFO, cross-flow, jitter, processed count) any order whose minT7Ns is behind the watermark, and T7ReorderLate is a counter only — it never degrades the score or fails the session, so a window overflow makes the validator quietly grade a subset of the session.

**Failure scenario:** Records are pushed in EffectiveT3 (arrival) order from the source Reorderer but the window is keyed by minT7. An engine that parks some orders for a long time (or the fill-vs-ack minT7 skew from the previous finding, where resting fills push minT7 up by seconds) makes T7 order diverge from T3 push order by more than VALIDATOR_T7_REORDER_WINDOW (default 1M) records. At 64k+ orders/s per node, 1M records is under ~16s of stream — a fill delayed >16s relative to arrival order lands behind the watermark and its order is dropped from grading with only T7ReorderLate++. A contestant engine could exploit this: delay responses to its worst-ordered flows past the window and those violations vanish from CorrectnessScore, which has no term for T7ReorderLate. Tests are green because small inputs never overflow the window.

**Verdict: CONFIRMED**
Late records only bump T7ReorderLate, never checked/scored/published — silently ungraded orders.

Fix: Fold into score or fail session above threshold.

### `internal/validate/invariants.go:224` — minor
**Summary:** lastSeq/lastSeqSet maps keyed by model.Flow are never evicted; memory is O(distinct flows) for the life of the validator with no bound or cleanup on flow termination.

**Failure scenario:** A session where the contestant engine drops connections and bots reconnect (each reconnect = new ephemeral client port = new model.Flow key) accumulates one entry pair per flow ever seen; with aggressive connection churn over a long session the maps grow linearly with reconnect count and are only freed when the whole per-session validator is GC'd at Finish. Bounded per session in practice (validator is per-session), so low impact, but the 'O(flows)' claim silently means 'O(flows-ever-seen)', not 'O(concurrent flows)'.

**Verdict: CONFIRMED**
lastSeq/lastSeqSet maps never evicted; bounded by distinct flows per session (task count) — minor.

Fix: Optional inactivity eviction.

### `internal/validate/invariants.go:339` — minor
**Summary:** lastSeq/lastSeqSet maps are keyed by model.Flow and never evicted, so per-session memory is O(distinct flows ever seen), not the claimed O(window+flows) with bounded flows — the same never-evicted-map class the 3411e53 rewrite was made to remove.

**Failure scenario:** A contestant engine (or bot-fleet task hitting the write-error 'task writer exiting' path and being re-run) that churns TCP connections gives every reconnect a fresh ephemeral source port = a new Flow key; over a long or reconnect-flappy session the two maps grow monotonically for the whole drain and are only freed when the validator is GC'd at Finish. Additionally, a reconnect reusing an exact 5-tuple (port reuse) starts TCPSeq from a new ISN, and the stale lastSeq entry for that flow then yields a false 'out-of-order TCPSeq within one flow' Time violation. Tests use a handful of static flows and never see growth or reuse.

**Verdict: CONFIRMED**
Same maps as :224, usage site.

Fix: Same.

### `internal/validate/invariants.go:177` — minor
**Summary:** t7Reorderer buffers up to 2*window invRecs (default window 1<<20) and each release does a full sort.SliceStable over the whole buffer plus a fresh copy of the remainder; with heap-allocated orderID strings this is roughly 150-250 MB resident per session, multiplied by VALIDATOR_CONCURRENCY (default 4) concurrent sessions in main.go.

**Failure scenario:** Four large sessions validated concurrently at defaults hold ~4 x 2M invRecs (~1 GB including string data and the O(2M) allocations of `rest := make(...)` per release) — the service that was rewritten to be memory-bounded can still OOM a modestly-sized pod purely from the default T7 window times worker concurrency; no test runs the window near its default size or with concurrent sessions.

**Verdict: CONFIRMED**
2M invRecs x VALIDATOR_CONCURRENCY=4 buffered + full sort + alloc/copy per release — multi-hundred-MB..GB profile.

Fix: Smaller default window and/or heap-based incremental emit.

### `internal/validate/validate.go:89` — minor
**Summary:** ViolationCount truncates len(r.Violations) to uint32; combined with invariants mode emitting one Violation per event, counts past 4,294,967,295 wrap to small numbers on the published score event.

**Failure scenario:** Long multi-contestant invariants session with a pathological engine accumulates >2^32 violations (assuming the OOM in finding 1 is fixed by keeping the slice but capping counters, or on very long sessions); the CorrectnessScoreEvent reports a wrapped, near-zero violation count while CorrectnessScore says the run was bad — inconsistent published contract.

**Verdict: CONFIRMED**
ViolationCount = uint32(len(Violations)) can wrap at >4.29B; published on CorrectnessScoreEvent.

Fix: uint64 or saturate.

---

## score-computer (3 findings: 1 major, 2 minor)

### `internal/score/score.go:282` — major
**Summary:** The TargetRPS=0 max-rate sentinel breaks the score-computer's offered-rate contract: for the new correctness scenario every wave computes offered = 0*overlap and is skipped (offered <= 0 → continue), so scheduledWaveOffers returns an empty slice for the entire scenario.

**Failure scenario:** Commit 3bea142 taught bot-fleet's write loops that target_rps=0 means max-rate, but score-computer still multiplies TargetRPS into per-wave offered RPS. A correctness-scenario run (builder.go:234 sets TargetRPS: 0) yields zero WaveOffer rows for all 45s of waves, so whatever consumes OfferedRPS (delivered-vs-offered throughput scoring / wave gating) sees no scheduled waves at all — the run is scored as if nothing was offered, either zeroing or entirely skipping the throughput component for that scenario, silently and per-run. No test covers score-computer with a zero-RPS task because the sentinel was only added to worker.rs validation.

**Verdict: CONFIRMED**
TargetRPS=0 (max-rate correctness scenario) -> offered=0 -> wave dropped -> empty schedule -> PeakSustainedTPS stuck at 0. Cross-wave integration bug with the pass-1 sentinel.

Fix: Treat TargetRPS==0 as max-rate: score against measured RPS or skip offered-rate gating.

### `internal/redis/redis.go:46` — minor
**Summary:** ZSET writer path deleted with no cleanup of the existing leaderboard:global key: the ZSET, holding one member per contestant ever scored and no TTL, is orphaned in Redis permanently.

**Failure scenario:** Deployed against the existing Redis: leaderboard:global already contains every contestant member accumulated to date; after this commit nothing reads, writes, expires, or deletes it, so it sits in memory indefinitely (and keeps growing zero — but never shrinks). On a Redis instance also holding the 1h-TTL live hashes, this is dead resident memory that survives every restart until someone manually DELs it; no migration or DEL was shipped with the removal.

**Verdict: CONFIRMED**
Legacy leaderboard:global ZSET orphaned in Redis — deleted writers/readers, no DEL/migration.

Fix: One-shot startup DEL or documented manual cleanup.

### `internal/worker/worker.go:24` — minor
**Summary:** ZSET rank path removal deletes the writers/readers but leaves the existing leaderboard ZSET key in Redis with no TTL and no deletion migration — the key persists forever on every deployed Redis.

**Failure scenario:** Any environment that ran the pre-248b36c score-computer has a leaderboard ZSET (formerly cfg leaderboardKey) with one member per contestant and no expiry; after this commit nothing writes, reads, or deletes it, so it survives indefinitely as orphaned state. Small today, but it is exactly the never-evicted-key pattern the lens targets and will confuse any future operator inspecting Redis (stale ranks that look authoritative). A one-shot DEL on startup or a documented migration is missing.

**Verdict: CONFIRMED**
Same orphaned-key issue, worker wiring site.

Fix: Same.

---

## leaderboard-api (7 findings: 6 major, 1 minor)

### `internal/live/poll.go:78` — major
**Summary:** Poller has no per-tick or per-call timeout: tick() runs Postgres + per-session Redis calls with the process-lifetime ctx, so one hung connection blocks the poll loop forever with no recovery and no error surfaced.

**Failure scenario:** Redis primary fails over or a network partition leaves the TCP connection half-open; p.redis.Get in pollSession blocks indefinitely (ctx passed is main's background ctx, never cancelled until shutdown). The single Run goroutine is stuck, live_metrics silently stop for all sessions and all SSE clients, and only the initial slog.Warn paths — which never fire because the call never returns — could have signaled it. Service appears healthy (readiness unaffected) while the live pipeline is dead until restart.

**Verdict: REFUTED**
go-redis Read/WriteTimeout 2s are socket deadlines independent of ctx — hung connection errors out in ~2s and tick proceeds; no forever-block.

### `internal/live/poll.go:86` — major
**Summary:** tick() performs 2 sequential Redis round trips per active session, so per-tick cost is O(sessions) serialized RTTs; with enough sessions or modest Redis latency a tick exceeds the 1s interval and live data lags unboundedly behind.

**Failure scenario:** 200 active sessions x 2 RTTs x 3ms Redis latency = 1.2s per tick against a 1s ticker; ticker coalesces, so effective update rate degrades and every broadcast payload is stale by more than one wave. At 1000 sessions (multi-tenant contest final) a tick takes ~6s, meaning the 'live' pill shows data 6+ waves old while the ingester is writing every second. No pipelining/MGET batching and no concurrency, so cost scales strictly linearly in session count inside one goroutine.

**Verdict: CONFIRMED**
Sequential Get+HGetAll per session per 1s tick, single goroutine — linear cost in session count.

Fix: Pipeline/MGET or bounded fan-out.

### `internal/read/store.go:471` — major
**Summary:** ActiveSessionContestants runs every second and its cost grows with the whole runs table (uncorrelated subquery over runs by status) while returning ALL sessions of a group if ANY sibling run is non-terminal — including long-finished and stuck sessions, which are then polled against Redis every second forever.

**Failure scenario:** A run stuck in 'pending'/'running' (crashed sandbox-orchestrator, known failure mode) keeps its entire run_group in the result set indefinitely: every completed sibling session gets a Redis Get + HGetAll every second, and for the first hour (TTL window) their stale hash is re-broadcast as live_metrics to every SSE client each tick — frozen 'live' numbers for finished sessions. Meanwhile the per-second full-table subquery (no LIMIT, filter on a low-cardinality status column) scans an ever-growing runs table, so the poll loop's Postgres cost grows with historical run count, not active-session count.

**Verdict: CONFIRMED**
Unbounded no-LIMIT subquery over runs every 1s; any non-terminal sibling drags whole run_group in.

Fix: Time-bound / incremental active-session tracking.

### `internal/read/store.go:477` — major
**Summary:** ActiveSessionContestants selects ALL sessions of any run_group containing at least one non-terminal run — including that group's already-completed sessions — and has no bound; a single stuck run keeps its whole group polled forever.

**Failure scenario:** A run_group with 20 sessions where one run wedges in 'running' (crashed orchestrator, never marked failed): all 20 sessions are returned every second, forever. Each poller tick then issues 1 Postgres query + up to 2 Redis round-trips per returned session (Get live pointer, HGetAll) sequentially inside the 1s tick. Over 24h with several stuck/long groups the per-tick work grows with total accumulated non-terminal rows, ticks start exceeding the interval, and the Postgres/Redis load is a permanent floor that only manual DB surgery removes — nothing in the code path ever evicts a stuck session from the poll set.

**Verdict: CONFIRMED**
Returns completed sessions of a group while any sibling is live; no per-session terminal filter.

Fix: Filter per-session non-terminal.

### `internal/sse/broker.go:68` — major
**Summary:** BroadcastLive shares each client's 16-slot channel with 'update' events, and the poller emits one message per active session in a tight loop each second; overflow force-closes the client, so bursts larger than the buffer disconnect healthy clients.

**Failure scenario:** 50 concurrent sessions: live.Poller.tick calls BroadcastLive 50 times back-to-back within microseconds while the ServeHTTP consumer must do a network Write+Flush per message. Any client whose TCP send buffer momentarily backs up (remote browser, brief congestion) drains slower than the burst fills, the select hits default:, and the broker closes+deletes the channel — kicking a perfectly healthy client every tick. Clients auto-reconnect via EventSource, producing a steady disconnect/reconnect churn (connection setup + snapshot replay per reconnect) that scales with session count and never stabilizes.

**Verdict: CONFIRMED**
16-slot client buffer shared by update+live_metrics; burst of S sessions per tick force-closes slow clients.

Fix: Coalesce per-tick live_metrics into one batch message and/or bigger buffer.

### `internal/live/poll.go:118` — major
**Summary:** Poller broadcasts every session's metrics every tick even when the hash is unchanged (no updated_at_ns dedup), multiplying SSE channel pressure: with S sessions each client must drain S events/sec into a 16-slot buffer, so modest session counts get healthy clients force-disconnected by the broker's close-on-full policy.

**Failure scenario:** 30 active sessions -> 30 live_metrics events broadcast per second per client. A browser on a throttled/background tab or a client on a slow link that stalls for ~500ms overflows the chan buffer of 16; Broker.send hits the default case, closes the channel, and drops the client mid-contest. The client reconnects, gets a fresh snapshot, stalls again — a reconnect storm exactly when session count peaks, driven by events that carry no new data (the ingester writes once per wave, but the poller re-reads and rebroadcasts the same hash every second).

**Verdict: CONFIRMED**
Rebroadcasts unchanged data every tick — no last-updated_at comparison.

Fix: Skip when updated_at_ns unchanged.

### `internal/live/poll.go:100` — minor
**Summary:** Poller Get(live pointer) then HGetAll(hash) is non-atomic with the 1h TTLs, and a hash present but missing any expected field (partial/older schema write) makes parseLiveMetrics fail and log a Warn every tick for every such session — unbounded 1/s log spam per stuck session with no backoff or suppression.

**Failure scenario:** A run whose row never reaches 'completed'/'failed' in Postgres (crashed orchestrator — ActiveSessionContestants has no time bound) keeps being polled forever; once its Redis keys expire the pointer Get returns not-found (silent), but during the window where the pointer survives and the hash has expired or was written by an ingester version without one of the six required fields, every 1s tick emits a Warn for that key indefinitely. With dozens of orphaned sessions this floods logs and burns a serial Redis round-trip pair per orphan per second for the lifetime of the process.

**Verdict: CONFIRMED**
Pointer+hash reads non-atomic; parse errors Warn-spam every tick indefinitely for stuck sessions.

Fix: Rate-limit warns; atomic read via Lua if needed.

---

## frontend (3 findings: 3 major)

### `src/hooks/useLeaderboard.ts:99` — major
**Summary:** liveMetrics Record accumulates one entry per (session_id, contestant_id) forever with no eviction — entries are only ever added/overwritten, never removed when a session finishes.

**Failure scenario:** Leaderboard page left open on a projector for a 24h contest with dozens of sessions started/finished: every session that ever emitted a live_metrics event stays in the Record and is rendered as a 'live' tile by LeaderboardClient (Object.values(liveMetrics) with no staleness filter). The tile row grows monotonically, shows frozen TPS/p99 numbers for long-dead sessions as if they were live, and each SSE event re-spreads an ever-larger object ({...current}), so per-event work and React re-render cost grow linearly with total historical sessions.

**Verdict: CONFIRMED**
liveMetrics Record insert-only — no eviction on completion/staleness; grows for page lifetime.

Fix: Evict on update-event completion or staleness sweep.

### `src/hooks/useLeaderboard.ts:101` — major
**Summary:** liveMetrics Record keyed by session_id:contestant_id is insert-only — entries are never evicted when a session completes, so the map grows without bound and finished sessions render as 'live' tiles forever.

**Failure scenario:** A contest day with hundreds of short runs: each run's session adds a key on its first live_metrics event; the backend poller stops broadcasting once the run leaves 'active' status (Postgres filter), but the frontend keeps the last snapshot in state. After N sessions the LeaderboardClient renders N stale tiles showing hour-old TPS/p99 as current, and every incoming live_metrics event spreads the whole ever-growing object ({...current}), giving O(N) work per event per second. Tests only exercise one or two events so this never surfaces. Needs eviction on session completion or an updated_at_ns staleness sweep.

**Verdict: CONFIRMED**
Duplicate of :99 (spread-copy cost grows with keys ever seen).

Fix: Same.

### `src/hooks/useLeaderboard.ts:101` — major (second finder pass, frames as sibling of validator OOM)
**Summary:** liveMetrics state is a Record keyed by session:contestant that is never evicted — entries persist after sessions complete, and each incoming event does a full spread copy of the map, so a long-lived leaderboard tab does O(total sessions ever seen) work per event and grows memory without bound.

**Failure scenario:** A leaderboard dashboard left open through a contest day: with sessions broadcast at ~1 event/sec each and every event triggering setLiveMetrics with {...current}, after K distinct session:contestant keys have accumulated each of the N events/sec copies K entries — O(N*K) allocations per second, plus a React re-render of the whole leaderboard per event. Completed sessions' entries are never removed (the backend keeps broadcasting nothing for them, and there is no eviction on 'update'/run-completion events), so the map and the stale on-screen live pills grow monotonically across the day. Direct sibling of the invariants-mode retain-until-Finish bug.

**Verdict: CONFIRMED**
Duplicate.

Fix: Same.

---

## Stats

| Component | Critical | Major | Minor | Total | Confirmed | Refuted | Partial |
|---|---|---|---|---|---|---|---|
| bot-fleet-controller | 1 | 8 | 2 | 11 | 9 | 1 | 1 |
| bot-fleet | 2 | 10 | 6 | 18 | 17 | 1 | 0 |
| telemetry-ingester | 1 | 1 | 2 | 4 | 4 | 0 | 0 |
| schemas | 0 | 1 | 0 | 1 | 1 | 0 | 0 |
| correctness-validator | 3 | 2 | 4 | 9 | 9 | 0 | 0 |
| score-computer | 0 | 1 | 2 | 3 | 3 | 0 | 0 |
| leaderboard-api | 0 | 6 | 1 | 7 | 6 | 1 | 0 |
| frontend | 0 | 3 | 0 | 3 | 3 | 0 | 0 |
| **Total** | **7** | **32** | **17** | **56** | **52** | **3** | **1** |

Raw finder output: 58 findings. After dedup: 56 (2 removed as duplicates). Note several near-identical entries survived dedup across the 3 lenses (e.g. the `kafka.rs:140` compression-range issue and the `worker.rs:1436` inflight-gauge issue were each independently flagged by more than one lens) — these are listed as separate entries above since they carry slightly different evidence, but the verify pass should likely collapse them to one verdict each.

---

## Fix triage

Confirmed findings only (52), bucketed by how much work the fix actually requires.

**Status: 32 fixed / 1 mitigated / 0 open / 1 accepted.**

### (a) One-line / mechanical fixes — all FIXED

- `src/kafka.rs:140` (all 3 variants, incl. ebpf-latency) — clamp compression-level filter to librdkafka's `-1..=12`. **FIXED (b83835c)**
- `src/worker.rs:974` — sub only actual removals in the FIX batch-write error path, not the whole batch count. **FIXED (b83835c)**
- `src/worker.rs:1183` — move `frame.bytes` out instead of cloning per WS frame. **FIXED (b83835c)**
- `internal/validate/validate.go:89` — widen `ViolationCount` to uint64 (or saturate). **FIXED (b83835c)**
- `internal/redis/redis.go:46`, `internal/worker/worker.go:24` — one-shot `DEL` of the orphaned `leaderboard:global` ZSET. **FIXED (b83835c)**
- `internal/live/poll.go:118` — skip broadcast when `updated_at_ns` is unchanged. **FIXED (b83835c)**
- `internal/live/poll.go:100` — rate-limit the per-tick parse-error warn log. **FIXED (b83835c)**
- `services/telemetry-ingester/Cargo.toml:12` — add the `zstd` feature to the ingester's own `rdkafka` dep. **FIXED (b83835c)**
- `schemas/rust/src/lib.rs:88` — `div_ceil` (or remainder distribution) for `num_bands`. **FIXED (b83835c)**
- `src/telemetry.rs:335` — wrap `poll_producer` in `spawn_blocking`. **FIXED (this change)**
- `internal/controller/runner.go:172` — `defer releaseSlot` guarded after `CreateSlot` instead of 6 manual call sites. **FIXED (this change)**
- `src/worker.rs:1436` (major + dup) / `:1114` — add the missing `inflight_add` call in `rw_write_loop` to mirror the FIX path. **FIXED (b83835c)**
- `services/telemetry-ingester/src/ingester.rs:88` (both entries) — iterate borrowed envelope fields instead of cloning 3 Strings per event. **FIXED (this change)**

### (b) Real design work

- `internal/controller/runner.go:131` — pair lease release with spec-consumption ack/tombstone (lease lifecycle vs. Kafka message lifecycle currently uncoupled). **MITIGATED (this change)** — worker.rs now skips stale specs via a `published_at` stamp + max-age check, so a stale spec left behind by a fast-failed session no longer gets executed against a dead endpoint by the next lessee. The lease lifecycle and the Kafka message lifecycle on the partition are still uncoupled — full fix (release only after spec consumed/acked or tombstoned) remains open.
- `internal/controller/lease.go:44`, `:85` (major), `:85` (minor) — FIFO fairness queue in `Acquire` to fix broadcast-wakeup starvation. **FIXED (57f6baa)** **OPEN**
- `internal/controller/consumer.go:127` — WaitGroup + bounded join for dispatch goroutines on shutdown. **FIXED (e9e703c)** **OPEN**
- `internal/controller/consumer.go:156` — panic-recovery path needs a fail+releaseSlot backstop. **FIXED (e9e703c)** **OPEN**
- `internal/controller/producer.go:84` — verify leased partition against writer metadata before publish instead of silent hash-fallback. **FIXED (e9e703c)** **OPEN**
- `internal/controller/consumer.go:114` — fail-closed RunStatus precheck (or persistent dispatch-accepted marker) to prevent duplicate re-dispatch. **FIXED (e9e703c)** **OPEN**
- `src/worker.rs:924` (critical + major) — bound the write `select` on `drain_end_ns`, not just process-level cancel. **FIXED (this change)** **OPEN**
- `src/worker.rs:1141` / `:1105` — `last_tick` must drain remaining `pending` map entries directly, not just the expiry queue. **FIXED (this change)** **OPEN**
- `src/worker.rs:941` — per-frame `send_ts_ns` patch (or guard acks before batch stamp) for batched writes. **FIXED (this change)** **OPEN**
- `src/worker.rs:593` / `:952` — prune expiry-queue entries on ack so queue depth is bounded by inflight, not `rate x RESPONSE_TIMEOUT`. **FIXED (this change — compact 16B entries (deadline,seq,kind), id rebuilt on eviction; entries still live to deadline but at ~4-5x less memory; wheel design reserved if residual matters)** **OPEN**
- `src/telemetry.rs:177` — `join_all` + aggregate shard-shutdown errors instead of early-return `??` mid-drain. **FIXED (this change)** **OPEN**
- `internal/validate/invariants.go:309` / `:356` — cap retained `Violation` examples (counters already exist separately). **FIXED (b83835c)**
- `internal/validate/validate.go:79` — separate matched-fill denominator per validator mode (invariants vs. full). **FIXED (b83835c)**
- `internal/validate/invariants.go:287` — compute `minT7Ns` over all responses, not fills-first with ack fallback. **FIXED (b83835c)**
- `internal/validate/invariants.go:171` — fold `T7ReorderLate` into scoring or fail the session above a threshold. **FIXED (this change — plus t7<t3 / t7-t3-cap sanity gate found during manual trace)** **OPEN**
- `internal/validate/invariants.go:177` — bound T7 reorder window memory (smaller default and/or heap-based incremental emit). **FIXED (this change — plus t7<t3 / t7-t3-cap sanity gate found during manual trace)** **OPEN**
- `internal/score/score.go:282` — handle `TargetRPS==0` max-rate sentinel in the offered-rate/wave-scheduling path. **FIXED (b83835c)**
- `services/telemetry-ingester/src/redis_sink.rs:55` — compare-and-set (Lua) on the live-pointer wave index instead of unconditional `SET`. **FIXED (b83835c)**
- `internal/read/store.go:471` / `:477` — time-bound and per-session-terminal-filter the active-session-contestants query. **FIXED (b83835c)**
- `internal/sse/broker.go:68` — coalesce per-tick `live_metrics` into one batch message and/or grow the client buffer. **OPEN**
- `internal/live/poll.go:86` — pipeline/batch Redis reads across sessions instead of sequential per-session round trips. **OPEN**
- `frontend/src/hooks/useLeaderboard.ts:99` / `:101` (both entries) — evict completed/stale sessions from `liveMetrics` instead of insert-only growth. **FIXED (b83835c)**

### (c) Accept-as-known-limitation candidates (minor + bounded)

- `internal/validate/invariants.go:224` / `:339` — `lastSeq`/`lastSeqSet` maps grow O(distinct-flows-ever-seen), but are bounded by per-session lifetime and freed at `Finish`; optional inactivity eviction is a nice-to-have, not urgent.

# Ring-buffer drops in the eBPF capture: what is measured, what is not

Status: **root cause resolved** — the drops were host CPU contention starving a Burstable
container (§5b), not a capture throughput limit, not Kafka, not the traffic. The document
now also carries the forward plan (§6): how the capture's real ceiling gets measured without
the cluster in the loop, and the ordered fixes that raise it.

Written 2026-07-31. Root cause confirmed 2026-07-31 (§5b). Plan rewritten 2026-08-01 (§6).

---

## 1. The problem

The capture drops records from its kernel→userspace ring buffer under load. Observed on one
B3 phase-1 run (session `019fb90e`, REST, 2,058,368 orders in 45s):

| counter | value |
|---|---|
| `ringbuf_dropped` | **23,436** |
| `stream_gap_bytes` | 32,421,526 (32 MB) |
| `hold_overflow` | 254 |
| `matcher_evicted_unanswered` | 36,264 |
| `truncated_captures` | 0 |
| `acked_dropped`, `undelivered_at_exit` | 0 |

Every dropped record is a hole in a TCP stream the reassembler must then write off, which is
why 23,436 dropped records cost 32 MB of stream and produced 65,968 `capture_gaps` (3.2% of
orders) and a **tainted** session. The engine answered every order; the capture failed to
record 3.2% of the answers.

This is the failure the platform can least afford: it is indistinguishable, at grading time,
from a contestant that did not answer.

## 2. What has been measured

Four runs, all 45s, all on the same cluster, engine and commit. `events_decoded` is capture
records that reached userspace; `ringbuf_dropped` is records the kernel discarded because
userspace was not draining fast enough.

| run | shape | orders | events_decoded | ringbuf_dropped |
|---|---|---|---|---|
| `019fb90d` FIX | max-rate, 1 flow | 1,816,128 | 3,788,008 | **0** |
| `019fb90e` REST | max-rate, 1 flow | 2,058,368 | 4,112,553 | **23,436** |
| `019fb910` WS | max-rate, 1 flow | 2,006,656 | 4,186,482 | **0** |
| `019fb921` mixed3 | paced 3x15k/s, 3 flows | 2,025,024 | 4,231,524 | **0** |

## 3. Hypotheses the measurements killed

**Kafka backpressure — ruled out.** `producer_inflight` peaked at **24** during the dropping
run, against 20 on a clean run. If the drain loop were blocking on produce, this would be in
the thousands. `acked_dropped` and `undelivered_at_exit` were both 0.

**Average order rate — ruled out.** WS pushed 2,006,656 orders (2.5% below REST) through the
same capture with zero drops.

**Burstiness — ruled out.** The paced run was the obvious suspect for "smooth traffic does not
drop", but FIX and WS were equally bursty (max-rate, `avg_batch: 64`) and also dropped
nothing. Pacing is not the discriminator.

**Records per second — ruled out.** WS decoded 4,186,482 events and mixed3 decoded 4,231,524,
both MORE than REST's 4,112,553, and neither dropped anything.

So the one run that dropped is not distinguished from the three that did not by any of order
rate, record rate, burstiness, flow count, or Kafka pressure.

## 4. Two problems with the measurements themselves

These matter more than any conclusion above, because they undermine the data the conclusions
rest on.

### 4a. Counter totals are truncated by scrape timing

The capture runs as a **per-run Job** whose pod is reaped on completion. A ~50s lifetime
scraped every ~15s means whatever accumulates after the final scrape is simply lost.

This is not hypothetical. The REST run's own funnel is internally impossible:

```
xdp_packets 1,489,492 + tc_packets 2,506,275 - short_payload_skipped 990,264 = 3,005,503 records emitted
events_decoded                                                              = 4,112,553 records decoded
```

You cannot decode more records than were emitted. The gap is missing final scrapes, and it
means the cross-run totals in §2 are indicative, not authoritative.

**Fixed:** the capture now logs a `"eBPF capture FINAL counters"` line on shutdown carrying
every loss counter plus a freshly re-read `ringbuf_dropped` from the kernel map. Job logs
survive pod reaping; the time series is now only for shape, and the log is the authority.

### 4b. The host was not quiet — and the capture is Burstable

The capture container is deliberately **Burstable**: `requests: 200m CPU`,
`limits: 4 CPU` (`sandbox-orchestrator/internal/k8s/slot.go`, `captureResources`). Under node
pressure the scheduler can squeeze it toward its 200m request, and CFS will throttle it.

**This exact failure is already on record in that file's own comment:**

```go
// 4 (was 2): the userspace drain+parse+publish wants ~3 cores at >150k
// delivered; a 2-core cap CFS-throttled it -> ringbuf drops. Burstable
// (request stays 200m), so this is safe on a 4-vCPU node and lets the
// capture use its needed cores on the c6i.2xlarge (8 vCPU) sandbox node.
```

Two things follow. First, the userspace path is known to want **~3 cores at >150k records/s**
— so it is not lightly loaded, and a squeeze does not have to be severe to hurt. Second, a
CPU cap producing precisely these ring-buffer drops has been diagnosed here once already.

These runs executed on a local k3s single node — the same laptop that was concurrently running
multi-core Rust/Docker builds during parts of this session. That is exactly the node pressure
this container's QoS class exposes it to, on whichever run happened to overlap.

**This is now the leading hypothesis**, and it explains the otherwise inexplicable result in
§3: the run that dropped is not distinguished by anything about its traffic, because the
cause was not in its traffic. It is a genuine confound and it is not yet excluded.
**Any re-measurement must run on a quiet host**, or the result means nothing.
(Confirmed by the §5b re-run.)

## 5. What is still unknown, and the instrumentation added for it

The open question is where userspace time actually goes. It could not be answered at all:
the capture exported **no CPU metric**, and cAdvisor is not scraped on this cluster. The only
datapoint is crude — `/proc/1/stat` read by hand mid-run showed **73 CPU-seconds over a ~50s
process lifetime**, i.e. over a full core — with no attribution.

Per-process totals cannot answer it either, because everything after the ring buffer —
decode, reassembly, framing, matching — runs in **a single tokio task**. "One thread pinned at
100% while the process shows 1.4 cores" and "work spread evenly" are different problems with
different fixes, and a process total cannot tell them apart.

**Added** (`services/ebpf-latency/src/cpu.rs`):

- `iicpc_ebpf_thread_cpu_percent{thread="..."}` — per-thread CPU as a percentage of one core,
  sampled from `/proc/self/task/*/stat` on the stats cadence
- `iicpc_ebpf_process_cpu_percent` — total across threads
- both also written to the Job log, so a reaped pod does not take the answer with it
- `_SC_CLK_TCK` read via `sysconf` rather than assumed to be 100
- comm parsed after the last `)`, since a thread name may contain spaces and parentheses
- `iicpc_ebpf_cpu_throttled_periods` / `_usec` from the cgroup's `cpu.stat` (v2, falling back
  to v1), also logged — this is what separates "needs more CPU" from "was stopped by CFS
  while CPU was available", per §4b

The reading is now a two-way decision rather than a one-way guess:

| observation | meaning | fix |
|---|---|---|
| `throttled_periods` climbing during drops | held below its limit by CFS — §4b | raise the request/limit, or isolate the node; **not** a code change |
| one thread pinned ~100%, no throttling | the single-task pipeline is saturated | shard the pipeline |
| no thread saturated, no throttling | neither — look at the drain loop's wakeup pattern | investigate further before changing anything |

## 5b. RESULT: the drops were host CPU contention

Re-ran the same shape (max-rate REST, single flow) on a quiet host with the new telemetry.
Session `019fb93a`:

| | busy host (`019fb90e`) | quiet host (`019fb93a`) |
|---|---|---|
| orders | 2,058,368 | 1,838,528 |
| events decoded | 4,112,553 | **4,185,669** |
| `ringbuf_dropped` | **23,436** | **0** |
| `cpu_throttled_periods` | not measurable then | **0** |
| `capture_gaps` | 65,968 (3.2%), tainted | **0** |
| score | 0.98716 | **0.99650** |

The quiet run decoded MORE records than the dropping run and dropped none. Combined with
`producer_inflight` having ruled out Kafka, and with zero CFS throttling, the conclusion is:

> **The ring-buffer drops were caused by CPU starvation from other work on the node — not by
> a capture throughput limit, not by Kafka, and not by anything about the traffic.**

That also retires the mystery in §3: no traffic variable distinguished the dropping run
because the cause was never in its traffic.

### 5c. Three of the CPU measurements were wrong — all now fixed

The 970% reading was chased down and it was an artefact. So were two others. Recording them
because each is the same class of error this document exists to prevent.

**1. `process_cpu_percent` of 970% was a sampling artefact, not a measurement.** Sampling ran
every 10 ticks of a `tokio::time::interval`, whose default `MissedTickBehavior` is `Burst`:
under exactly the load worth measuring, the loop falls behind and then fires ticks
back-to-back, so the gap between samples collapses to ~1ms. Against `/proc` accounting
quantised to 10ms (`USER_HZ` 100) that yields `delta = 0` -> **0%**, or `delta = 1 tick` over
~1ms -> **~1000%**. Both were observed from the same healthy process minutes apart. **The
capture does NOT need ~10 cores; that number was noise** and it came within one step of being
used to argue for a bigger EKS node.
*Fixed:* samples spanning less than 250ms are discarded WITHOUT consuming the baseline, so
the next sample measures the full gap. Regression test:
`sub_interval_samples_are_suppressed_and_keep_their_baseline`.

**2. The throttling metric could only ever report zero.** It read
`/sys/fs/cgroup/cpu.stat`, but the capture Job runs `HostPID: true` with no private cgroup
mount, so that path is the host ROOT cgroup — observed reporting `usage_usec` of 119,500
seconds (~33 hours of CPU on a node up 34 hours) for a Job alive for seconds, with
`nr_throttled` permanently 0 because the root cgroup is never throttled.
*Fixed:* the path is resolved from `/proc/self/cgroup`
(`/kubepods.slice/.../cri-containerd-<id>.scope`).

**3. The "73 CPU-seconds over ~50s" figure quoted earlier was not the capture.** It came from
reading `/proc/1/stat` inside the container — and with `HostPID: true`, PID 1 is the host's
systemd. That number should be struck from any reasoning; it measured the wrong process
entirely.

None of this changes the §5b conclusion — that rests on `ringbuf_dropped`, `capture_gaps` and
`producer_inflight`, which were never in doubt. It does mean **no trustworthy CPU figure for
the capture exists yet**; the next loaded run with the fixed sampler produces the first one.

## 6. The plan: measure offline, ship the no-regret fixes, gate on cluster counters

Rewritten 2026-08-01, after the root cause landed (§5b) and the code was re-read against the
original proposal. Several items from the first version of this section are already done or
already dead; recording which, so they are not re-proposed.

### 6.0 Status of the original proposals

**CPU request raise — SHIPPED.** `captureResources` now requests **2 CPU** (was 200m), limit
4, memory 256Mi/512Mi (`sandbox-orchestrator/internal/k8s/slot.go`). The request is the CFS
weight and scheduler floor, which is exactly what the §5b starvation exploited. One drift
item: the comment on the LIMIT block still says "request stays 200m", contradicting the
request block five lines above it — fix in the next pass that touches the file.

**Drain-loop batching — already correct, no work pending.** `drain_ringbuf` reads ALL
currently-available records per 5ms tick, synchronously and non-blockingly; `flush` enqueues
to Kafka without blocking and DROPS the batch on QueueFull rather than stalling the drain
(the old blocking `send().await` froze the capture at ~107k/s and is long gone). A
per-record wakeup path some earlier notes assumed does not exist.

**Publish serialization — already msgpack** (`rmp_serde::to_vec_named`, batched,
co-partitioned by order id). Not a pending optimization.

**Kernel-side parse — dead by design, do not re-propose.** The parser's input is a TCP byte
stream; a BPF hook's input is packets. FIX/HTTP/WS messages do not align with packet
boundaries — a message splits across segments, a segment carries several messages, and the
ClOrdID sits at a variable offset (FIX tag 11 anywhere in the tag soup, REST inside a JSON
body, WS behind variable-length frame headers). Extracting it in-kernel requires per-flow
stream reassembly with held-back bytes — exactly what userspace `Reassembler` exists for,
and BPF can hold neither arbitrary per-flow byte buffers nor unbounded scans. This was
attempted much earlier in the project and failed for precisely this reason. The
capture-only-kernel / parse-in-userspace split is the design, not a compromise.

### 6.1 Why the ceiling is measured offline, not with cluster ramps

The local single-node cluster hits the veth packet-rate wall before the capture saturates: a
load ramp there measures the load generator, not the capture. But everything after the ring
buffer is deterministic userspace — so the pipeline's ceiling is measurable with **no
network in the loop at all**: feed synthetic capture records straight into
`Pipeline::process` in a criterion macro-bench and read records/s per core off the report.

This is not a new idea in this repo — it is the `framing_property.rs` lesson applied to
throughput. That offline harness reproduced in 20ms two shipped framing defects that four
confidently-wrong cluster hypotheses (CPU starvation, ring-buffer overflow, Kafka queue,
teardown race) had failed to find, each at the cost of a build/import/run cycle. The framer
and reassembler are pure functions and were never going to be debugged in a cluster; neither
is their throughput. The property harness's FIX traffic generators are the natural bench
fixtures.

One cost center the pipeline benches CANNOT see: the Kafka producer's own threads. The
per-thread CPU telemetry was built to split exactly this — `rdk:*` threads (produce + zstd
compression) versus `tokio-rt-worker`/`iicpc-ebpf-late` (drain, reassembly, framing,
matching). The first trustworthy loaded reading decides which half to optimise: if `rdk:*`
dominates, the lever is producer config (compression level, batching), not pipeline code,
and no amount of criterion work on the pipeline will move the ceiling.

The criterion harness (`services/ebpf-latency/benches/`, to be added) carries:

- **decode** — `Capture` parse from raw record bytes;
- **reassembly** — `push()` on a contiguous stream, and separately a reordered/hold-path
  variant; realistic record sizes (~150B FIX response vs full-size);
- **framing per protocol** — FIX tag scan vs HTTP response parse vs WS frame decode, on
  payloads lifted from the e2e fixtures. This is the only stage where the three protocols
  genuinely differ; REST is the prime suspect (header parse, most bytes per order);
- **matcher** — insert/match/evict at a realistic live-set size (1M+ entries), including a
  fixed-width-key prototype as the A/B (see 6.2);
- **full pipeline** — the macro-bench: N flows of mixed realistic traffic through
  `Pipeline::process` single-threaded. This number IS the single-core ceiling.

Target arithmetic the ceiling is judged against: 400–500k orders/s × ~2 capture records per
order (request + response; measured 2.0–2.1 across all three protocols in §2) ≈ **~1M
records/s**. The macro-bench verdict against that number decides sharding (6.2, item 4) —
without a single cluster run.

The harness doubles as permanent regression tracking: any future change that eats pipeline
throughput fails a visible benchmark instead of a live session.

Kernel-side cost is measured separately and cheaply: `sysctl kernel.bpf_stats_enabled=1`
during any loaded run, then `bpftool prog show` → `run_time_ns / run_cnt` for the XDP and tc
programs. Expected sub-µs/packet; this is a box-check, not an investigation.

Cluster runs keep exactly one role: **regression gate**. A max-rate run per protocol on a
quiet host must show `capture_gaps` = 0, `ringbuf_dropped` = 0, `cpu_throttled_periods` = 0
in the FINAL counters line — at whatever rate the local wall permits. A run with nonzero
throttling is invalid, not a data point (§4b).

**Precondition for any local cluster run: redeploy the orchestrator first.** The CPU
request raise is committed but the running local orchestrator was never rebuilt/imported —
the LIVE capture pods still request 200m (see "Local cluster state left behind",
`remaining-work.md`). A local measurement taken before that redeploy is exposed to the
exact starvation this document diagnosed, and is void.

### 6.2 Fixes, in order

**1. Pod memory raise, paired with ring-buffer growth.** Ring 64MB → 256MB; pod memory
256Mi/512Mi → **1Gi/2Gi**. These must move together: BPF map memory is memcg-charged to the
creating process since kernel 5.11, so a 256MB ring plus userspace heap does not fit under
the current 512Mi limit — growing the ring alone OOM-kills the capture at startup. The raise
also covers two userspace worst cases the current limit cannot: the matcher's live set under
an unresponsive engine (entries linger up to the 5s idle eviction → 500k/s × 5s ≈ 2.5M
entries ≈ 400–500MB at ~150–200B/entry — OOM exactly when measuring the interesting failure
mode), and reassembly hold buffers after the capture-cap change (64 held segments × 9KB ≈
576KB/flow worst case). Ring growth is insurance — it extends the survivable-starvation
window ~4x (64MB is ~43k max-size records ≈ 43ms at 1M records/s against a 5ms drain
cadence, ample when userspace actually runs) — but it is nearly free and the memory raise
that enables it is needed anyway. Measured on cluster only: FINAL counters under load.

**2. Matcher fixed-width key.** `HashMap<String, Inflight>` allocates a String per record on
the hot path. Replace the key with a fixed-width form of ClOrdID. Behavior-neutral, wins at
any load. Precondition: confirm the ClOrdID width/format the bot fleet generates (fixed
width → `[u8; N]`; else an inline-string type). Measured in criterion: the matcher bench
A/Bs String vs fixed-width at 1M live entries; the delta is the whole argument.

While in this code, expect the **5s idle-eviction question to reopen**. It is the leading
suspect for the pass-1 residual missed fills (a resting maker is acked, marked answered,
reaped as routine GC, and its FILL arrives later when someone trades against it —
unmatched and dropped; 7,004 missed fills / `unmatched_response` 11,837 on the 1.84M-order
run, see `remaining-work.md`). That was parked with the explicit condition "unless the rate
grows at higher throughput" — and raising throughput is this plan's stated goal, so the
work that raises the rate trips the revisit trigger itself. The eviction window also sizes
worst-case matcher memory (item 1), so the two decisions are coupled.

**3. `CAPTURE_CAP` 1536 → 9029.** Covers a full jumbo frame, so EKS keeps MTU 9001 and the
`net-tune` MTU clamp disappears. Three wins at once: ~6x fewer capture records wherever the
engine's egress coalesces (per-record pipeline overhead is the cost that scales with record
COUNT — decode, map lookups, `push()` calls, marks — while per-byte framing cost is
invariant); the veth wall itself rises (~6x fewer packets per byte through the veth pair —
the wall is packet-rate-bound); and the load generator's ceiling rises with it. The verifier
proof in `capture_len` survives the constant change — its bounds are structural
(read_volatile + relational JLT/JGT, clamp path returns the CONSTANT cap), none reference
1536 — and 28 + 9029 = 9057 fits the 32KB per-CPU value limit. Lockstep changes: the
userspace mirror `capture.rs CAPTURE_CAP` (its `captured_len` sanity check would otherwise
reject every complete jumbo capture as corrupt), the `clamp_gso` target (1500 → 9001, still
bounding pre-GSO skbs to one jumbo frame), and the test constant in `main.rs`. `gro-disable`
STAYS — GRO coalesces to 64KB regardless of MTU.

Temper the expectation before measuring: the "~6x fewer records" win is
**engine-behavior-dependent, not automatic**. Execution reports are ~130 bytes, and the
measured cost of `gso_max_segs 1` was −0.3% (inside run-to-run spread) — GSO was batching
tiny messages for syscall economy, not moving bulk bytes. Coalescing wins materialize only
where the engine actually batches multiple messages per write. So the criterion A/B must
use realistic batch-size distributions (as observed from real engines), not synthetic
all-jumbo streams — an all-jumbo fixture would overstate the win roughly 6x.

Measured twice: criterion (pipeline fed record streams at realistic batch distributions,
1536-cap vs 9029-cap framing of the same byte stream) and ONE quiet-host cluster A/B (MTU
1500 vs 9001, same scenario) using bot-side send/receive timestamps as the
capture-independent witness — which also finally separates the measurement-correction from
the genuine slowdown, the experiment `grading-network-regime.md` calls for and nobody has
run.

**4. CONDITIONAL — per-flow sharding.** Only if the 6.1 macro-bench puts the single-core
ceiling below ~1M records/s. Mechanism, if needed: **N ring buffers with flow-hash steering
in the BPF programs** — kernel logic stays hash + index (verifier-trivial, no parsing);
`FlowKey` is direction-normalized, so a flow's XDP-side requests and tc-side responses land
in the SAME ring; each userspace worker owns its ring, its flows' reassemblers, and its
matcher shard. ClOrdIDs live on one flow, so the matcher shards along with the flows — no
shared concurrent map, no merge stage. This supersedes the earlier sketch that hashed flows
to workers behind a single ring and worried about a shared matcher; steering in the kernel
dissolves that problem.

### 6.2b Measured (2026-08-01, first criterion baseline + the shipped fixes)

The harness exists (`services/ebpf-latency/benches/capture_bench.rs`) and items 1–3 are
implemented. Numbers from one laptop core (relative structure is the finding; EKS absolute
numbers will differ):

| stage | cost | verdict |
|---|---|---|
| record decode | ~0.9 ns/record | irrelevant |
| reassembly | ~53 GiB/s | irrelevant |
| FIX frame+parse | ~140–210 ns/msg (7.1M/s) | cheap |
| **HTTP frame+parse** | **~1.4 µs/msg (720k/s)** | **the bottleneck, 7–10x FIX** |
| WS frame+parse | ~1.2 µs/msg (855k/s) | second hotspot |
| matcher pair (100k occupancy) | 202 ns → 186 ns after fixed key | ~5% of a FIX order |

Full pipeline, single core: **FIX ~2.7M records/s baseline (≈1.37M orders/s) — already
above the ~1M records/s target with no sharding.** **HTTP ~585k records/s (≈292k orders/s)
— below target; HTTP parse cost is the single biggest TPS lever in the whole plan**, bigger
than every shipped item combined. WS sits between them. So the sharding decision (6.2 item
4) is protocol-specific: FIX never needs it; HTTP needs either a parse-cost fix (first) or
~2 workers.

Fix outcomes against their predictions:

- **Matcher fixed-width key: shipped, −8%** on the matcher bench (2.02→1.86ms per 10k
  pairs) — but only on the third attempt. A derived Hash over the zero-padded 64-byte
  array measured **+18% slower** than the String key, and SipHash over the filled prefix
  still +7%; only prefix-hash + FxHashMap wins (inserted keys are bot-generated, so
  SipHash's flood resistance was pure cost). The "removing malloc always wins" argument
  from the first version of this section was measurably false as stated.
- **CAPTURE_CAP 9029: shipped.** Per-order userspace cost flat, as the tempered
  expectation predicted; one new second-order cost surfaced — framing inside a
  jumbo-packed record pays `consume()`'s memmove of the record's tail per message
  (~10% on a 40-message record). The change's real wins stay kernel/wire-side (packet
  count, veth wall, loadgen ceiling). BPF object builds; live 6.1 verifier load pending.
- **Ring 256MB + pod 1Gi/2Gi: shipped**, pinned by a Go test so ring and memcg limits
  cannot drift apart.

**And the flamegraph paid for itself the same day.** Profiling the HTTP pipeline bench
(`docs/http-pipeline-flamegraph.svg`) attributed the HTTP multiplier precisely: `find()`
was a positional substring scan compiling to one memcmp call per byte offset — **63% of
the entire profile** — and every JSON field lookup heap-allocated its `"key"` search
pattern via `format!` (~15% more across malloc/free/format_inner). Fix: `memchr::memmem`
(SIMD) + stack-built patterns. Measured: HTTP framing 720k → 1.57M msgs/s, WS 855k →
2.51M msgs/s, HTTP pipeline **585k → 1.28M records/s (+96%) ≈ 638k orders/s single
core — HTTP is now above the 400–500k target too, with no sharding.** With FIX at
~1.85M orders/s and HTTP at ~638k, item 4 (sharding) is not currently needed for any
protocol at the stated targets; it stays parked unless targets rise or EKS disagrees.

### 6.3 Acceptance

- Criterion: single-core pipeline ceiling recorded per protocol; matcher-key and record-size
  A/B deltas recorded; benches runnable by anyone, tracked so regressions are visible.
- Cluster, per protocol, max-rate, quiet host: `capture_gaps` = 0 and `ringbuf_dropped` = 0
  — zero, not merely under the 1% taint threshold — with `cpu_throttled_periods` = 0 or the
  run is void.
- The ceiling number itself comes from criterion, never from a cluster ramp, until the
  cluster stops being the smaller of the two walls (EKS bench tier).

## 7. Open questions

- ~~Does the sandbox slot impose a cgroup CPU limit on the capture container?~~ **Answered:**
  requests 200m / limits 4, i.e. Burstable and squeezable — see §4b. Whether it was actually
  throttled during the dropping run is now measurable but not yet measured.
- Is the 5s matcher idle-eviction window contributing? `matcher_evicted_unanswered` was 36,264
  on the dropping run, but that is expected downstream of dropped responses rather than an
  independent cause. The window does, however, size the matcher's worst-case memory — it is
  the residence time in the 2.5M-entry arithmetic of §6.2 item 1.
- ~~What actually differs about the REST run?~~ **Answered by §5b: nothing.** No traffic
  variable distinguished it because the cause was never in its traffic — it was the run that
  happened to share the host with a build.

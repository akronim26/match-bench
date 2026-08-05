# Bot-Worker TPS Improvement Plan

Goal: raise per-node order generation throughput on EKS, where bot-workers run as pods
on dedicated `botworker` nodes and each run offers load to one contestant. This plan
covers all three protocols (FIX, REST/HTTP, WebSocket). Multi-contestant routing/mesh
work is explicitly out of scope here — this is "more TPS for one run at a time."

---

## 1. Evidence base

### 1.1 Profile (docs/bot-worker-flamegraph.svg)

CPU flamegraph of a bot-worker at high single-task rate, telemetry off. Bot-fleet source
has changed only ~3 lines since this profile was taken (commits `bf37802`, `fc42ab6`),
so it remains representative. Sample shares:

| Frame | % of samples | Meaning |
|---|---|---|
| `fix_write_loop` | 42.3 | dominant task |
| `render_frame` / `build_frame` | ~25 | frame construction per order |
| `format!` machinery (`format_inner` + `core::fmt::write` + `Display` + `write_str`) | ~17–20 | string formatting |
| allocator (`realloc` 6.5 + `malloc` ~5 + `cfree` ~4) | ~15 | per-order String growth/free |
| send syscall path (`__send` → `tcp_sendmsg` → `ip_output`) | 10.4 | wire cost (already batched, see §1.3) |
| inline softirq net_rx | 7.7 | offloads-off tax (GSO/TSO/GRO disabled by design for eBPF capture fidelity) |
| `watchdog_loop` (incl. `HashMap::retain` 4.9) | 7.4 | pending-map expiry scan |
| `TaskGenerator::next` | 5.6 | order generation |
| `_raw_spin_lock` (kernel) | 1.0 | negligible |
| futex / userspace mutex frames | **0** | no lock contention |

Conclusions:
- The worker is **CPU-bound in userspace string formatting + allocation**, roughly
  30–35% of all cycles.
- **No lock contention.** The per-task `PendingMap` (`worker.rs:549`) is shared by only
  that task's write/read/watchdog loops; zero futex frames in the profile. Proposals to
  split into 4 single-threaded pinned pods (vs 1 pod on 4 cores) would buy low single
  digits at best — not pursued.
- The 51.10% `tokio::runtime::blocking::pool::Inner::run` stack is **not** work on the
  blocking pool: tokio's multi-thread runtime launches its worker threads through that
  internal chain (identical sample counts confirm it's the same stack). There is no
  `spawn_blocking`/`block_on`/`std::thread` anywhere in bot-fleet source. No action.

### 1.2 Current validated ceilings (ARCHITECTURE §"Benchmarking")

- Generation, telemetry off (drain sink): **~600–790 k orders/s per c6i.xlarge node**;
  peak 747,911/s in `deploy-bench/drain-raw-tps.tsv`.
- Telemetry on (single worker → 1 broker): **~445 k/s** (lossless backpressure).
- Node scaling measured 2.02× at 2 nodes; workers share no state.

### 1.3 Already fixed — do not re-attack

- Per-task tokio-timer pacer cap (~1 k/s) → catch-up pacing (65–75×), both write loops.
- `push_resting` O(n) `Vec::remove(0)` → `VecDeque`.
- One `write` syscall per order on the FIX path → `BOT_WRITE_BATCH=64` coalescing of
  already-due backlog (`worker.rs:768–888`); kernel syscall share 27% → 2%.
- Single-node veth wall: local-only artifact, gone on EKS (separate node pools, real NIC).
- Telemetry serialization: msgpack + batched drain + per-partition batching + lossless
  backpressure; drops ~0%.

---

## 2. The two structural problems

### 2.1 Triple-render: every order builds FIX + JSON + REST, sends one

`build_frame` (`services/bot-fleet/src/fix.rs:254–293`) constructs, per order:
1. FIX body + `finalize_fix` (BodyLength + checksum),
2. the JSON payload,
3. the full HTTP request string wrapping that JSON,

and stores all three in `OrderFrame` (`fix`, `rest`, `ws_bytes`) — but the write loop
sends exactly one of them depending on `spec.protocol`. The waste is acknowledged in
code (`fix.rs:515–517`) and quantifiable with the existing ignored bench:

```
cargo test -p iicpc-bot-fleet --lib serialization_cost -- --ignored --nocapture
```

### 2.2 `format!`-per-order instead of template-and-patch

Every frame is rebuilt from scratch with `format!` (multiple `String` allocs, `Display`
formatting, realloc growth) even though per task almost everything is constant: session
id, bot id, sender/target comp IDs, symbol, side names, HTTP header block. The codebase
already proves the alternative: `OrderFrame::patch_timestamp` patches tag 52 in place at
1.6% CPU. Extending that pattern to the whole frame removes the ~30–35% formatting +
allocator burn.

### 2.3 REST/WS paths are second-class vs FIX

The FIX write loop got the batching/coalescing work; the shared REST/WS loop
(`rw_write_loop`, `worker.rs:1219`) did not:

| Issue | Where | Cost |
|---|---|---|
| No write batching — one awaited `write_order` per order | `worker.rs:1102–1110` | 1 syscall/order (REST); FIX equivalent was 27% kernel CPU before batching |
| WS `SinkExt::send` per message → tungstenite write **+ flush** per order | `worker.rs:1105–1108` | sink wakeup + syscall per order |
| `frame.ws_bytes.clone()` per order, then tungstenite re-allocates the frame and does the masking XOR pass on yet another buffer | `worker.rs:1106` | 2–3 extra allocs + full-payload copy per order |
| Response parse uses full `serde_json::from_slice` + String alloc per response | `clordid_from_json`, `worker.rs:1389–1397` | avoidable; ebpf-latency solved the same problem with a hand-rolled key scan (`parse.rs::json_string`) |

Fine as-is in the REST/WS loop: catch-up pacing (identical `select!` pattern),
blocking-write backpressure, in-flight cap, and request pipelining (the write loop never
awaits responses; the read loop resolves them via the pending map).

---

## 3. Work items, ranked

### P1 — Single-protocol render (all protocols)

Thread the spec's protocol into frame construction; build only the needed
representation. `OrderFrame` becomes single-payload (`bytes: Vec<u8>` + `payload_type`);
`tag52_offset` kept only for FIX. Call chain to touch: `render_frame` (`worker.rs:678`)
→ `order_frame`/`market_frame`/`cancel_frame`/`replace_frame` → `build_frame` (`fix.rs`).

Estimated: ser-cost roughly halved for FIX runs, more for REST/WS; +15–25% node TPS.

### P2 — Template-and-patch FIX + JSON

Per task, pre-render one frame per `FrameKind` at task start. Per order, patch in place
into a reusable `Vec<u8>`:
- seq digits (`34=`, ClOrdID suffix), qty, price — itoa-style digit writes;
- checksum by delta (FIX checksum = byte sum mod 256, so patching bytes adjusts it in O(patched bytes));
- timestamp via the existing `patch_timestamp` mechanism.

**Fixed-width (zero-padded) numeric fields** make this clean: constant field widths per
`FrameKind` ⇒ constant BodyLength ⇒ `9=` never changes ⇒ template offsets truly static.
Seq width rollover (10→100→…) re-renders the template, ~4 times per task lifetime.

**Scope guard:** ClOrdID (`11=`) format stays exactly `sess_bot_seq_K`, unpadded — it is
the join key across telemetry, the eBPF matcher, and the validator. Pad only
`38=`/`44=`/`34=` (and JSON `qty`/`price`).

Estimated: +20–30% on top of P1.

### P2′ — Templates for REST and WS wire frames

- **REST:** header block is constant except `Content-Length`. With fixed-width JSON
  numerics, Content-Length is constant per kind ⇒ the entire header is static; patch
  digits in the JSON body only.
- **WS:** pre-build the **entire wire frame** (2-byte header + 4-byte mask key + masked
  payload) into a reusable buffer and write raw bytes to the underlying stream,
  bypassing tungstenite for data frames (keep tungstenite for the handshake and
  ping/pong). Client masking is RFC-required, but the mask key may be constant per
  connection (no intermediaries in this deployment), so the masked payload is itself a
  patchable template: patch digit bytes, XOR only the patched bytes with the mask.

### P3 — Watchdog expiry without full-map scans (~5% CPU)

`watchdog_loop`'s `HashMap::retain` scans the whole pending map per tick (4.9% of
samples). Replace with insertion-ordered expiry (per-task `VecDeque<(deadline,
order_id)>` — orders are inserted in send order, so deadlines are monotonic; pop from
the front) or lengthen the tick.

### P3′ — Batch REST/WS writes like FIX

Apply the FIX `BOT_WRITE_BATCH` pattern to `rw_write_loop`, coalescing only
**already-due** backlog orders (preserves the coordinated-omission contract — `target_send_ts_ns`
is captured before any sleep, unchanged):
- REST: concatenate N pipelined requests into one `write_all` (HTTP/1.1 pipelining).
- WS: with P2′ raw frames, plain buffer concat + one `write_all`; otherwise
  `feed()`×N + single `flush()`.

### P4 — Cheap response parse + generator cleanup

- Replace `serde_json::from_slice` in `clordid_from_json` with a `"cl_ord_id"` key scan
  (same approach as ebpf-latency `parse.rs::json_string`); reuse the response buffer.
- Revisit `TaskGenerator::next` (5.6%) after P2 — its order-id `format!` cost should be
  absorbed by prefix-patching; measure before doing more.

### P5 — Telemetry-on gap (pipeline, not worker; separate track)

Worker fixes raise the telemetry-**off** ceiling; the on/off gap (445 k vs 790 k) closes
via the already-identified levers: T1 aggregator sharding (M drain tasks per worker,
partition-sharded — today one `run_aggregator` task per worker) and the 2-broker Kafka
tier. Do after P1/P2: msgpack serialization also burns worker CPU, so worker headroom
helps here too.

### P6 — Opportunistic quality-of-life (accepted; do inside the files already touched)

| # | Where | Change |
|---|---|---|
| QoL-1 | `worker.rs:811, 1257` | Replace the 1 ms `sleep` poll in the in-flight backpressure loop (`while pending.len() >= max_inflight`) with a `tokio::sync::Notify` signaled by the read loop on ack/removal. Removes up to 1k wakeups/s per saturated task; reacts to drain instantly. |
| QoL-2 | `worker.rs:61, 774` → `config.rs` | Move the scattered `LazyLock` env knobs (`BOT_MAX_INFLIGHT_PER_TASK`, `BOT_WRITE_BATCH`) into `Config::from_env` so they sit next to the documented knobs and become testable. |
| QoL-3 | `worker.rs` metrics | Per-protocol labels on `orders_sent` / `order_write_error` / write-latency / slip. **Export through the worker's Prometheus metrics endpoint so Grafana can chart per-transport rates** — mandatory once one worker mixes protocols, else a stall can't be attributed to a transport. |
| QoL-4 | `fix.rs` (and other touched files) | Delete the boilerplate doc comments ("performs the module-specific operation described by its name") in files being rewritten; keep contract comments only where behavior is non-obvious (checksum-delta patching earns one). |
| QoL-5 | `fix.rs:521` | Promote the ignored `serialization_cost_breakdown` test into a criterion bench with FIX/REST/WS variants — the standing regression guard for P1/P2/P2′. |
| QoL-6 | `builder.go:20–41` | Lift the hardcoded population mix (`mixHFTPct` 60/25/15) and per-profile action mixes (`hftMarketPct` etc.) into `Config` + `ConfigFromEnv`, alongside the existing RPS knobs — the per-protocol budget split edits these call sites anyway. |
| QoL-7 | orchestrator `slot.go` | With multi-port slots: readiness probe must gate on **both** listeners (9898 and 8080) — a contestant that binds one but not the other fails `WaitForReady`, not mid-run. |

### Deferred QoL (documented, not scheduled)

- **`spec_version` on `WorkloadSpec`** + cross-language round-trip fixture test (Go
  encode → Rust decode) for the hand-synced schema mirrors. Worth doing at the *next*
  wire-breaking change if not this one.
- **WS control-frame audit**: bypassing tungstenite for data frames must not starve
  ping/pong/close handling — verify during P2′ implementation rather than as separate
  work.
- **Scenario config log line** at submission-api startup (config hash/values) so a
  stale-env reseed is diagnosable.
- **Per-run seed override** in `BenchmarkRequested` (fallback `runConfig.GlobalSeed`)
  for exact replay of a specific run without controller redeploy.
- **`validateWorkerCapacity` failure surfacing** as an explicit
  `benchmark.status.updated` failure reason — first thing a bigger run hits at the
  24-partition cap.
- **Single-source the protocol→port table** shared by `capturablePorts`, Service port
  lists, and docs (fixed-port policy §7.3 enables it).
- **Merge contestant variants** (echo/drain/matching) into one binary behind `--mode`
  flags to cut the e2e matrix maintenance.
- **Template-render the echo contestant's execution reports.** Measured (local task
  sweep, 2026-07-15): the echo's per-order reply — old `format!` builder + one
  unbatched write per response — moved the worker's pacing knee from ~500-700k/s
  (drain) down to ~300k/s (echo). Since the echo is the sink for
  `measure-capacity-sweep.sh`, an unoptimized echo caps what the measurement pipeline
  *appears* able to sustain. Same fix as P2, applied to
  `execution_report_frame`.

### Explicit non-actions

| Idea | Why not |
|---|---|
| 4 pods × 1 pinned core vs 1 pod × 4 cores | zero lock contention in profile; work-stealing is not the bottleneck; expect low single digits |
| More write batching on FIX | already done; kernel share is ~2% |
| Per-write timeouts | deliberately removed; blocking write is the backpressure mechanism (ramp-collapse fix) |
| veth wall | local-only artifact, documented S2, absent on EKS |

---

## 4. Constraints verified

### 4.1 eBPF is offset-agnostic — fixed-width padding is safe

- The **kernel** program is content-blind: it computes L3/L4 header bounds and bulk-copies
  up to `CAPTURE_CAP = 1536` payload bytes (`ebpf.rs:125–126, 231, 255`). The BPF
  verifier constraint applies to the **copy length** only (`ARG_CONST_SIZE`,
  `ebpf.rs:38–44`), already satisfied by the constant clamp. No payload field offsets
  exist in kernel code.
- **Userspace** parsing is offset-independent: `frame_fix` walks `8=`/`9=BodyLength`;
  `parse_fix` splits on SOH and scans tags; `parse_uint` accepts leading zeros (pure
  digit loop). `frame_ws` handles masked frames generically (constant mask key
  irrelevant). `frame_http` frames by Content-Length and already consumes back-to-back
  pipelined messages from the reassembler stream.
- `tag52_offset` is bot-fleet's own patch offset, computed per frame
  (`fix.rs::find_tag52_offset`) — ours to control.

### 4.2 Offloads-off environment

GSO/TSO/GRO are disabled by DaemonSet + initContainer (required for eBPF capture
fidelity; see ARCHITECTURE §3f). Consequences for this plan: batching reduces syscalls
and sink wakeups but **not** wire packets/s (still ~orders×size/MSS); per-segment t3
stamping is preserved; orders coalesced into the same TCP segment share a bit-identical
t3 (per-segment timestamp resolution — accepted).

---

## 5. Cross-codebase impact ledger

| Component | Change required | Breaking? |
|---|---|---|
| bot-fleet `fix.rs` / `worker.rs` | all of §3 | wire change limited to zero-padded numeric fields |
| eBPF kernel (`ebpf.rs`) | none | — |
| eBPF userspace (`parse.rs`) | none | — |
| telemetry-ingester | none — keys on `order_id` string, never parses qty/price from wire | — |
| correctness-validator | **document only** (full redesign planned anyway): zero-padded `38=`/`44=` values must compare equal to unpadded; ClOrdID format unchanged | flag for redesign |
| contestant spec / sample engines (echo, cpp, go, e2e matching engine) | must tolerate zero-padded numerics (standard atoi does) **and** HTTP/1.1 pipelining (multiple in-flight requests on one keep-alive connection, in-order responses) — verify sample engines loop-on-buffer during P3′ | doc + verify |
| deploy-bench scripts | none; used for verification | — |

---

## 6. Verification protocol

1. **Tests first, per step** (project TDD rule). Existing frame/parse tests keep the
   contract; extend the ignored `serialization_cost_breakdown` bench (`fix.rs:521`) with
   REST-only and WS-only variants and record ns/order before/after each of P1/P2/P2′.
2. `cargo test -p iicpc-bot-fleet` green after each step.
3. **Local re-profile** (deploy-local, drain contestant, `BOT_DISABLE_TELEMETRY=1`;
   privileged — run manually):
   ```
   sudo perf record -F 997 -g -p $(pgrep -f iicpc-bot-fleet) -- sleep 30
   sudo perf script | stackcollapse-perf.pl | flamegraph.pl > docs/bot-worker-flamegraph-v2.svg
   ```
4. **EKS**: `deploy-bench/drain-scale-sweep.sh` on a single botworker node before/after.
   Number to beat: **747,911/s peak** (`deploy-bench/drain-raw-tps.tsv`). Repeat for
   REST and WS protocol runs (previously unmeasured at this granularity — capture a
   per-protocol baseline first).

## 7. Related track: mixed-protocol runs (FIX + REST + WS to one contestant)

Separate from raw TPS but touching the same code paths, so planned here to avoid
conflicting rework. Goal: one benchmark run offers all three protocols to the same
contestant simultaneously.

### 7.1 Finding: eBPF is NOT the blocker — everything upstream is

The capture side already supports mixed protocols per run:

- `ebpf.rs:32–34` captures **both** ports at all times: 9898 → FIX, 8080 → HTTP/WS,
  filtering on both and tagging each packet's `server_port` independently.
- `capture.rs:128–132` derives `Transport` **per packet** from `server_port`, not per
  capture session.
- `pipeline.rs` reassemblers are keyed per `(FlowKey, Direction)`; a mixed FIX + HTTP/WS
  packet stream is already handled correctly.
- sandbox-orchestrator's `capturablePorts` already whitelists `{8080, 9898}`
  (`slot.go:43`).
- REST and WS already share port 8080 and the same `Transport::HttpWs` tag; framing
  distinguishes them by payload shape (`parse.rs:164`), not port. Only FIX needs its own
  port.

### 7.2 Blockers (all upstream of capture)

| Where | Problem |
|---|---|
| `schemas/rust/src/lib.rs:81` + `schemas/go/topics/topics.go:127` | `WorkloadSpec` carries ONE `protocol` + `target_port` for the whole spec; `TaskSpec` has no protocol field |
| `services/bot-fleet-controller/internal/controller/runner.go:355–369` | controller stamps the single submission-level `sub.Protocol` / `sess.Endpoint.Port` onto every worker spec |
| `services/submission-api/internal/scenarios/builder.go` | scenario builder never varies protocol per task; RPS budget is not split across transports |
| `services/bot-fleet/src/worker.rs:1644–1676` | `TargetClient::connect` opens one transport per spec; all tasks share it |
| `e2e/contestant-matching-engine/src/main.rs:219` | reference engine binds ONE listener/port |
| `services/sandbox-orchestrator/internal/k8s/slot.go:139–455` + `handler/slot.go` | `CreateSlot` and pod/service specs are single-port (`port int`); need `ports []int` with multiple `ContainerPort`/`ServicePort` entries |

### 7.3 Change list — DECIDED: Shape A (per-task protocol)

Two shapes were considered:

**A. Per-task protocol (schema change) — CHOSEN.** Add a `targets:
Vec<TargetSpec{protocol, port}>` table to `WorkloadSpec` and a `target_idx: u8` to
`TaskSpec`. Builder tags each task with a transport and splits the RPS budget; worker
resolves the connection per task (the per-task loop structure already exists in
`connect_tasks`, `worker.rs:432`). Touches `schemas/rust/src/lib.rs:81–118`,
`schemas/go/topics/topics.go:127–149`, builder, runner, worker.

**B. Multiple WorkloadSpec sets (no TaskSpec change) — REJECTED.** Controller emits one
spec set per protocol sharing the session + barrier epoch.

Why A wins:

1. **ClOrdID collisions.** `order_id = sess_task_seq_K` has no protocol component, and
   task generators are seeded `global_seed ^ task_id`. Shape B duplicates task_ids
   across protocol sets ⇒ identical deterministic order streams ⇒ colliding order ids
   (breaks the telemetry join, eBPF matcher, validator). B is only safe via task-id
   range offsetting — an out-of-schema convention two components must forever agree on.
   A is collision-free by construction: one flat task list, task_ids already unique.
2. **Worker/partition budget.** B triples worker count per run against the hard
   `worker_count > partition_count` guard (24 partitions on `workload.assignments`),
   cutting max run size to ~8 workers per protocol. A keeps
   `worker_count = ceil(tasks/1000)` with protocols mixed inside one worker's slice.
3. **Fits P1.** The single-protocol render work makes protocol a per-call parameter
   anyway; per-task protocol slots in without rework.
4. **RPS accounting.** A splits the budget explicitly per task; B hides it in "how many
   spec sets at what scale" and forces spike/ramp wave shapes to be replicated and kept
   in sync per set.
5. **Future-proof.** The `targets` vec is exactly where multi-contestant
   `{protocol, host, port}` entries slot in later; B dead-ends there.

### Port policy: mandated, not contestant-chosen

The eBPF kernel program hardcodes its port filter (`FIX_PORT=9898`,
`HTTP_WS_PORT=8080`, `ebpf.rs:32–34`) and classifies transport by
`server_port == 9898 → Fix else HttpWs` (`capture.rs:128–132`). Supporting arbitrary
contestant-declared ports would require a per-packet BPF map lookup for a port→transport
table — complexity with no benefit. Decision:

- **Ports are platform constants**: 9898 = FIX, 8080 = HTTP **and** WS.
- `benchmark.yaml` declares which **protocols** the engine supports; it does not choose
  ports. submission-api validates the manifest and rejects non-conforming port
  declarations at submit time.
- HTTP and WS **share 8080 by design** — WS begins as an HTTP GET Upgrade handshake on
  the same connection, so one listener serving both is the standard model. The capture
  side already disambiguates per reassembled message by shape (`looks_like_http`,
  `parse.rs:177–180`); WS frame headers (0x81/0x82…) can never collide with ASCII HTTP
  method prefixes.
- Consequence: `capturablePorts`, netpol rules, and slot Service port lists stay static
  — no per-submission port plumbing anywhere.

Either way, also required:
- **Contestant side:** reference engine (and contestant spec) must listen on 9898 (FIX)
  and 8080 (REST + WS shape-multiplexed) simultaneously.
- **sandbox-orchestrator:** multi-port slot API + pod/service specs (`ports []int`).
- **eBPF / telemetry-ingester:** no changes. Ingester joins on `order_id` and is
  protocol-agnostic; the `payload_type` field already distinguishes transports in
  telemetry events.

Interaction with the TPS work above: P1 (single-protocol render) keys off the **task's**
protocol (`spec.targets[task.target_idx].protocol`), not the spec's — the P1 signature
takes protocol as a per-call parameter (it already does: `render_frame` receives it from
the caller), so Shape A slots in without rework.

---

## 8. Expected outcome

If formatting + allocation are the ~30–35% the profile shows, and REST/WS gain FIX-parity
batching, the compounded target is **~1.1–1.3 M orders/s per node telemetry-off** for FIX
(from ~790 k), with REST/WS improving by a larger factor from their lower baselines
(unmeasured — establish baseline in §6.4). Error bars stay wide until the step-3
re-profile.

Order of work: P1 → P2 → P2′ → P3/P3′ → P4 → (separate track) P5. P6 QoL items land
inside whichever step touches their file (QoL-1/2/3 with P1, QoL-4/5 with P2, QoL-6 with
the Shape A builder work, QoL-7 with the multi-port orchestrator work).

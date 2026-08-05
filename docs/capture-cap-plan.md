# Plan: the capture cap, the network tax, and what to do about it

Status: **proposal, not started.** Written 2026-07-31 to be reviewed later. Nothing here is
in flight; the current behaviour is the config workaround described under "The problem".

---

## 1. The problem

### What the system does today

Every benchmark run executes on a deliberately de-optimised network path:

| setting | value | applied by |
|---|---|---|
| pod `eth0` MTU | 1500 (clamped down from the EKS default of 9001) | bot-fleet `net-tune` initContainer; the capture's `mtu::clamp_to` on the sandbox pod |
| GRO / GSO / TSO / LRO on pod `eth0` | off | bot-fleet `net-tune` initContainer; the capture's `disable_offloads` |
| GRO / GSO / TSO / LRO on sandbox **host** interfaces | off, re-applied every 2s | `k8s/sandbox/gro-disable-daemonset.yaml` |
| `gso_max_size` / `gso_max_segs` on the capture interface | 1500 / 1 | the capture's `clamp_gso` |

### Why it does that

The eBPF capture copies at most `CAPTURE_CAP` = 1536 bytes of any packet
(`services/ebpf-latency/src/capture.rs`, mirrored in `src/ebpf.rs`). Bytes past that are
never captured, and the responses inside them are indistinguishable from responses the
contestant never sent — they are scored as unanswered orders against an engine that in fact
answered them.

Three independent mechanisms produce payloads larger than the cap, and all three had to be
suppressed:

1. **Jumbo frames.** EKS pods default to MTU 9001, so a single un-coalesced frame already
   exceeds 1536.
2. **GRO on receive.** The XDP program attaches in GENERIC mode on veth, which runs AFTER
   GRO. GRO merges consecutive segments of a flow into skbs of up to 64KB, re-creating the
   problem even at MTU 1500.
3. **GSO on transmit.** The tc egress hook runs in `__dev_queue_xmit`, BEFORE segmentation,
   so it observes the pre-segmentation skb — up to 64KB when the engine batches responses.
   Note `ethtool -K gso off` was measured NOT to fix this; only `gso_max_size` /
   `gso_max_segs` bound skb construction early enough.

Measured consequences of leaving these open: 25.6% of all responses lost to GSO truncation,
and a separate 2.2% lost to GRO super-frames (B2 pass 2: `capture_gaps` 4763 of 216,210,
`truncated_captures` 1218). Both went to exactly 0 once suppressed.

**The governing invariant, which every option below is a variation on:**

> `CAPTURE_CAP` must be >= the largest payload the hook actually observes.
> tc egress observes the pre-GSO skb (bounded by `gso_max_size`, else 64KB).
> Generic-mode XDP observes the post-GRO skb (bounded by GRO, else 64KB).

So there are exactly three families of fix: raise the cap, shrink the maximum at the hook,
or stop needing the payload at all.

### The cost being paid

Roughly 6x the packet count for the same bytes, plus software segmentation. Per packet the
system pays an skb allocation, a full trip through netfilter/tc/qdisc, a driver doorbell,
and on receive a NAPI poll and its own trip up the TCP stack. CPU-per-byte rises, softirq
load rises, and the bottleneck shifts from bytes/s toward **packets/s**. Under burst that
means deeper queues, so tail latency degrades more than median.

### What is NOT known

**The size of the tax has never been measured.** Removing truncation and de-optimising the
network landed in the same change, so every before/after number mixes two effects:

1. **A measurement correction, which is not a regression.** Truncation did not drop
   responses uniformly — it chopped the tails off coalesced packets, and packets coalesce
   precisely when the engine is busy and slow. The old numbers were computed on a sample
   with the slow responses systematically deleted.
2. **A genuine slowdown** from 6x packets and software segmentation.

Observed on B2 pass 2 (204 flows, reference engine, same cluster and commit): jitter p99
4,194,304 ns -> 33,554,432 ns, inversion_rate 0.4688 -> 0.4913, score 0.809 -> 0.791, while
`capture_gaps` went 4763 -> 0. It has been asserted that (1) dominates. That is an inference
from the truncation mechanism, **not a measurement**, and the score delta cannot be
attributed to either cause as things stand.

---

## 2. Why this matters

**Ranking integrity — the main reason.** The tax is not a uniform multiplier. It is
proportional to **packet count**, and engines differ in packets emitted per order, so it
scales each engine by its own I/O behaviour rather than by a shared constant. That means it
can **reorder** close results: an engine with fast matching but naive per-message writes
beats a slower engine with good batching when packets are cheap (MTU 9001 + GSO), and can
lose to it when packets are the scarce resource (MTU 1500, GSO off). Large gaps will not
invert; close ones can, along that specific axis.

**It changes what the contest rewards.** Turning off GSO removes precisely the mechanism
that most rewarded response batching (one large skb, hardware does the splitting), so the
regime compresses the spread between batch-optimised and naive engines and shifts the reward
toward per-packet efficiency. Existing evidence that stock-socket engines cluster around
~45% regardless of language suggests I/O already dominates matching logic in these scores;
this regime pushes further that way. For a matching-engine competition, that should be a
deliberate choice, not an inherited side effect of a 1536-byte buffer.

**Throughput ceiling.** Packets/s is the binding constraint in this regime, so the headroom
for the stated high-rate ambitions is being spent on packet overhead rather than on work.

**What is genuinely unaffected.** Pass-1 correctness grading does not depend on absolute
latency at all, and contestants cannot observe or change MTU or offloads — the contestant
container runs `Capabilities: Drop ALL`, `AllowPrivilegeEscalation: false`,
`SeccompProfile: RuntimeDefault`. The tax is therefore uniform in setting and un-gameable,
which is what makes "fair, just document it" a defensible position for pass-2 latency and
TPS. This plan exists because it is defensible, not because it is optimal.

---

## 3. Prerequisite: measure the tax before spending effort on it

Do this first. Tier 1 below is real eBPF verifier work, and nobody currently knows whether it
buys 3% or 40%.

**Experiment.** bot-fleet records its own send and receive timestamps and is an independent
witness, so the network tax can be measured with the capture entirely out of the loop:

1. Disable the capture for the run.
2. Run the same scenario twice: once with MTU 9001 and offloads on, once with MTU 1500 and
   offloads off.
3. Compare bot-side latency (median and tail) and achieved TPS.

That isolates the genuine slowdown from the measurement correction and yields a single
number: what the regime costs.

**Second experiment, for the ranking question.** Run a response-batching engine and a
per-message engine under both regimes and compare **the gap between them**, not the absolute
figures. If the gap barely moves, the fairness argument is fully vindicated and the rest of
this plan can be closed. If the gap moves materially, Tier 1 becomes a ranking-integrity
issue rather than a performance nicety.

---

## 4. The approaches

### Tier 0 — raise the ceiling without touching eBPF at all

None of these change the grading regime; they reduce what it costs.

- **cpuset isolation plus IRQ/softirq steering away from engine cores.** Most of the tax is
  softirq CPU contending with the matching engine on the same cores. Separating them means
  the 6x packet count largely stops hurting the engine, with no MTU change whatsoever.
  Cheapest real win available and the natural first move regardless of what else is chosen.
- **Shard the userspace pipeline.** Decode, reassembly, framing and matching currently all
  run in a single tokio task, so everything after the ring buffer is single-threaded. Per-flow
  sharding. No BPF risk at all.
- **Grow the ring buffer.** 64MB is about 43k records, roughly 30ms of headroom at 500k/s.
- **Drop the string keys in the matcher.** It keys `HashMap<String, Inflight>` on ClOrdID,
  which at high rate means millions of live entries with per-entry allocation.

Risk: low. Payoff: unknown until the Tier 0 measurement, but this is the only group that
carries essentially no correctness risk.

### Tier 1 — raise `CAPTURE_CAP` to 9029 and restore MTU 9001 (recommended)

Covers a full jumbo frame, so the MTU clamp becomes unnecessary and the 6x packet inflation
disappears.

What makes this cheaper than it first appears:

- `SCRATCH` is a `PerCpuArray<CaptureRecord>`, and per-CPU map values are bounded by
  `PCPU_MIN_UNIT_SIZE` (32KB). A 28-byte header plus a 9001-byte payload is ~9057 bytes,
  comfortably inside that.
- Emission already writes only the used prefix (`from_raw_parts(rec, 28 + cap)`), so a larger
  cap costs **scratch memory, not ring-buffer bandwidth** — small packets still produce small
  records.
- Userspace decode already honours `captured_len`, so it needs no change.

Still required afterwards: GRO stays off, and the gso clamp is raised from 1500 to 9001.
Retired: the `net-tune` MTU clamp and the packet inflation.

Risk: moderate, concentrated in the verifier bounds work in `capture_len`. The comments there
document hard-won constraints — the length must stay verifier-trivial (`umin`/`umax` provable),
and clamping via a saturating min or an AND-mask was already found to break that. Expect the
effort to be in the verifier, not in the struct change.

### Tier 2 — allow GSO and GRO to stay on

Requires covering ~64KB payloads, which will **not** fit a `PerCpuArray`. The route is
`bpf_ringbuf_reserve()` written into directly, skipping `SCRATCH`. Reserve size must be a
verifier constant, so the pattern is reserve-max / commit-actual, giving 64MB / 64KB = 1024
in-flight slots. Retires the DaemonSet and every clamp; the engine then runs a stock tuned
path.

Cheaper half worth testing on its own: **re-test native/driver-mode XDP attach on veth.** The
DaemonSet's own comment states native attach fails there, but if it can be made to work, XDP
runs BEFORE GRO and the DaemonSet retires for the receive side, leaving only the egress
clamp. That is a small experiment with a large payoff and should be tried before committing
to the full Tier 2 restructure.

Risk: high. Changes the emission path that everything downstream depends on.

### Tier 3 — parse in the kernel and emit fixed-size records

Stop copying payload at all: extract ClOrdID and timestamps in BPF and emit ~64-byte records.
This dissolves the cap question permanently, cuts copy volume by roughly 25x, and removes the
userspace framing cost in the same stroke — addressing the single-threaded pipeline concern
at its source rather than by sharding around it.

FIX is tractable (a bounded scan for tag `11=`, with several records emitted per packet).
HTTP/WS is materially harder — masking, chunked encoding, fragmentation — and would
realistically stay on the copy path, meaning both paths must then be maintained.

Risk: highest. Payoff: highest. This is the "right" long-term shape if capture throughput
ever becomes the binding constraint.

---

## 5. Suggested sequence

1. **Measure** (section 3). Both experiments. Cheap, and it determines whether anything below
   is worth doing.
2. **Tier 0.** Low risk, independent of every decision, useful regardless of outcome.
3. **Re-test native XDP attach on veth.** Small experiment, potentially retires half the
   DaemonSet requirement.
4. **Tier 1**, if and only if the measurement shows the tax is material or the two-engine gap
   moves. This is the recommended stopping point: jumbo restored, packet inflation gone,
   GRO-off as the only remaining tax, capture still provably correct.
5. **Tier 2 / Tier 3** only if capture throughput becomes the binding constraint at target
   rates.

---

## 6. Invariants any change must preserve

- Every contestant response must be captured. A partially captured response is worse than an
  obviously missing one, because it grades as an unanswered order against an engine that
  answered.
- Contestants must remain unable to observe or influence the regime. `Drop ALL` capabilities
  is what currently guarantees this, and it also blocks AF_XDP and DPDK — which matters,
  because AF_XDP TX would skip the tc egress hook via `dev_direct_xmit` and make an engine's
  responses invisible rather than merely faster. Do not relax those capabilities.
- Any before/after comparison must state whether the capture was truncating, because a
  truncating capture flatters tail latency by deleting the slow samples.
- Changes to the framer or reassembler belong in `services/ebpf-latency/src/framing_property.rs`
  first, not in a cluster run.

## 7. Open questions to resolve during review

- Does raising `CAPTURE_CAP` to 9029 keep the ring buffer's effective headroom acceptable at
  target rates? Fewer, larger records for the same byte volume should help rather than hurt,
  but this is reasoning, not a measurement.
- Does native-mode XDP attach genuinely fail on veth in this environment, or was that
  conclusion drawn under different conditions?
- Is the sandbox pod's own `disable_offloads` still needed once the host DaemonSet is running,
  or is it now redundant belt-and-braces?
- Should the contest deliberately reward per-packet efficiency? The current regime does so by
  accident. If that is the intent, it should be stated to contestants; if it is not, Tier 1
  partially undoes it.

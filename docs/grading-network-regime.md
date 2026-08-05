# The network regime submissions are graded in

Every benchmark run executes on a deliberately de-optimised network path, paid identically
by every submission. This document records what the regime is, why it exists, and what may
and may not be concluded from numbers measured inside it.

**This is a workaround, not a law of nature.** The tax is not the price of capturing every
response — it is the price of capturing with a 1536-byte cap, and that cap is a design
parameter (see "Open questions"). It was chosen because it was fast and provably correct
while the capture was actively losing a quarter of all responses. It should not be treated
as settled.

**The size of the tax has never been measured.** Removing truncation and de-optimising the
network happened in the same change, so every before/after number below mixes the two. See
"Separating measurement from slowdown".

## What the regime is

On both the load-generator side and the contestant side:

| setting | value | applied by |
|---|---|---|
| pod `eth0` MTU | **1500** (clamped down from EKS default 9001) | bot-fleet `net-tune` initContainer; capture's `mtu::clamp_to` on the sandbox pod |
| GRO / GSO / TSO / LRO on pod `eth0` | **off** | bot-fleet `net-tune` initContainer; capture's `disable_offloads` |
| GRO / GSO / TSO / LRO on sandbox **host** interfaces | **off**, re-applied every 2s | `k8s/sandbox/gro-disable-daemonset.yaml` |
| `gso_max_size` / `gso_max_segs` on the capture interface | **1500 / 1** | capture's `clamp_gso` |

## Why

The eBPF capture copies at most `CAPTURE_CAP` = 1536 bytes of any packet
(`services/ebpf-latency/src/capture.rs`). Anything past that is not captured, and the
responses inside it are indistinguishable from responses the contestant never sent.

Three separate mechanisms produce oversized payloads, and all three had to be closed:

1. **Jumbo frames.** EKS pods default to MTU 9001, so a *single un-coalesced frame* already
   exceeds the cap. Closed by clamping MTU to 1500 at both ends.
2. **GRO on the receive path.** Generic-mode XDP attaches on veth and therefore runs AFTER
   GRO, which re-coalesces MTU-sized frames into super-frames of up to 64KB — re-creating
   the problem even at MTU 1500. Closed by the `gro-disable` DaemonSet on host interfaces;
   the capture's own `disable_offloads` covers only the pod netns, never the host root
   netns.
3. **GSO on the transmit path.** The tc egress hook runs in `__dev_queue_xmit`, BEFORE GSO
   segmentation, so it sees the pre-segmentation skb — up to 64KB when the engine batches
   responses. `ethtool -K gso off` does NOT fix this; it governs on-wire framing only.
   Closed by clamping `gso_max_size`/`gso_max_segs`, which bound skb construction upstream
   of the hook.

Measured consequences of leaving these open: 25.6% of responses lost to GSO truncation, and
a separate 2.2% lost to GRO super-frames (2026-07-31, B2 pass 2: `capture_gaps` 4763 of
216,210, `truncated_captures` 1218). Both went to exactly **0** once closed.

## The cost

Roughly 6x the packet count for the same bytes, plus software segmentation — so more
per-packet CPU, more interrupts, and more queueing delay than a tuned production path.

Directly measured on B2 pass 2 (204 flows, reference engine, same cluster, same commit):

| | truncating capture | complete capture |
|---|---|---|
| `capture_gaps` | 4763 (2.2%) | **0** |
| jitter p99 | 4,194,304 ns | 33,554,432 ns |
| inversion_rate | 0.4688 | 0.4913 |

## Separating measurement from slowdown

Two distinct effects are folded into the table above, and they have never been separated:

1. **A measurement correction, which is not a regression at all.** Truncation did not drop
   responses at random: it dropped the tails of coalesced packets, and packets coalesce
   precisely when the engine is busy and slow. The old p99 was computed on a sample with the
   slow responses systematically deleted. Restoring them raises p99 without anything having
   become slower — the metric simply stopped flattering itself. (Both values are exact
   powers of two, coarse HDR buckets at that magnitude, so read the table as "a few
   buckets", not a precise 8x.)
2. **A genuine slowdown**: ~6x the packet count plus software segmentation.

Which dominates is **unknown**. It has been asserted that (1) dominates; that is an
inference from the truncation mechanism, not a measurement. The 0.809 -> 0.791 score change
is likewise contaminated by both and cannot be attributed to either.

**How to settle it** (not yet done): bot-fleet records its own send/receive timestamps and
is an independent witness, so the tax can be measured without the capture in the loop. Run
the same scenario with the capture disabled, offloads+MTU on versus off, and compare
bot-side latency and TPS. That isolates (2) and says whether the tax is 3% or 40% — which is
what decides whether raising `CAPTURE_CAP` is worth the verifier work.

## Uniform in setting, not identical in effect

The regime is the same for everyone, so it does not scramble the ranking. But it is not a
neutral scaling either: at MTU 9001 with GSO, an engine that batches large responses gains a
great deal, and at MTU 1500 with GSO off that advantage is capped. The regime therefore
compresses the differences between batching strategies and shifts the reward toward
per-packet efficiency. That is a defensible thing to grade on — it should just be a
deliberate choice rather than a side effect of the capture's buffer size.

## Why this is fair, and what it still constrains

**Fair for ranking.** The regime is identical for every submission, applied by platform
manifests rather than anything a contestant controls. Pass 1 grades correctness against a
reference book and does not depend on absolute latency at all. Pass 2 ranks latency and
throughput *comparatively*, so a uniform handicap shifts all competitors together and does
not reorder them.

**Not fair to quote as platform capability.** Absolute latency and TPS figures measured here
understate what the same engines would do on a tuned path at MTU 9001 with offloads on.
Do not publish them as "what this hardware can do", and do not compare them against numbers
gathered in any other regime.

**W must be calibrated inside this regime.** `CROSS_FLOW_WINDOW_US` is being set from EKS
measurements (B6). Those measurements will carry this tax, which is correct — it is the
regime contestants are graded in — but it means W cannot later be reused against numbers
taken with offloads on.

## Open questions

- **Restoring jumbo frames — the cheap 80%.** `CAPTURE_CAP` = 1536 is what forces the MTU
  clamp, and it is not a kernel limit: it is sized to the per-CPU scratch map (`ebpf.rs`:
  28 + 1536 = 1564 <= 1568 value_size). Raising it to >= 9029 covers a full jumbo frame, so
  EKS keeps MTU 9001 and the `net-tune` clamp disappears. `SCRATCH` is a `PerCpuArray`, and
  per-CPU map values are bounded by `PCPU_MIN_UNIT_SIZE` (32KB), so 9029 fits with room to
  spare. Plausibly *helps* the userspace pipeline too: same total bytes copied, ~6x fewer
  capture records, hence less per-record overhead and more effective ring-buffer headroom
  in bytes. `gro-disable` is still required (GRO coalesces to 64KB regardless of MTU).
- **Removing the tax entirely — the harder version.** Covering pre-GSO skbs and GRO
  super-frames needs ~64KB records, which will NOT fit a per-CPU array. That means writing
  directly into a `bpf_ringbuf_reserve()` rather than staging through `SCRATCH`, after which
  GSO/TSO/GRO could be left on and the DaemonSet retired. More restructuring, and both
  routes touch the verifier-sensitive bounds work in `capture_len` that the comments there
  warn about. A third option is parsing in the kernel and emitting fixed-size records, which
  removes the copy question altogether.
- ~~Kernel-bypass submissions may not be taxed equally.~~ **Answered: they cannot bypass.**
  AF_XDP TX would skip the tc egress hook (`dev_direct_xmit`), making a bypassing engine's
  responses invisible to the capture rather than merely faster — but the contestant
  container runs with `Capabilities: Drop ALL`, `AllowPrivilegeEscalation: false` and
  `SeccompProfile: RuntimeDefault` (`services/sandbox-orchestrator/internal/k8s/slot.go`).
  That removes `CAP_NET_RAW` (needed to open an AF_XDP socket) and `CAP_BPF`/`CAP_SYS_ADMIN`
  (needed to load the XDP redirect program), and seccomp blocks `bpf()` independently. DPDK
  additionally needs VFIO/UIO and hugepages. So every submission goes through the kernel
  stack, the capture sees all of it, and the tax really is uniform.

  Note that **io_uring is NOT kernel bypass** — it is an async syscall interface, and its
  packets traverse the full stack including tc. io_uring engines are fully visible and
  fully taxed. Two things worth confirming about it, since it is the main remaining
  optimisation path: whether `RuntimeDefault` seccomp permits `io_uring_setup`/`_enter`/
  `_register` under the runtime in use, and that gVisor is not enabled for graded runs —
  runsc does not implement io_uring at all, so it would silently deny that path.
- **DaemonSet coverage on EKS is unverified live.** `gro-disable` selects `pool: sandbox`.
  Confirm at bring-up that its pod count equals the sandbox node count, and that its 2s
  re-apply loop keeps up with ENIs the VPC CNI attaches over time.

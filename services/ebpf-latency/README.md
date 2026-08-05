# iicpc-ebpf-latency

Wire-to-wire latency capture for the algo pod. Measures `t3` (request ingress,
XDP) and `t7` (response egress, tc) per FIX ClOrdID and publishes
`OrderAckedBatch` records to `orders.acked`.

## Architecture: dumb kernel, smart userspace

The kernel program (`src/ebpf.rs`) does the **minimum**: parse the eth/ip/tcp
headers, stamp `bpf_ktime_get_ns`, and copy the TCP payload of every
request/response segment into a ring buffer as a `CaptureRecord`. It does **no**
protocol parsing or matching — so the BPF verifier surface is tiny and the
captured bytes can have any (contestant-controlled) layout.

The userspace binary (`src/main.rs` + modules) does everything else:

- **`capture.rs`** — decode the `CaptureRecord` ring-buffer ABI.
- **`reassembly.rs`** — per-`(flow, direction)` TCP stream reassembly. Handles
  coalescing (several messages in one segment) and straddling (one message split
  across segments), and attributes each message's timestamp to the segment
  carrying its first byte.
- **`parse.rs`** — frame messages (FIX by BodyLength; HTTP by Content-Length; WS
  by frame length) and extract fields by scanning, with **no fixed offsets**.
- **`matcher.rs`** — match responses to requests by ClOrdID and emit **one event
  per response** (ACK, then each partial fill), all sharing the request's `t3`.
- **`pipeline.rs`** — ties the above together; `main.rs` drains the ring,
  converts the monotonic stamps to realtime, runs the pipeline, and publishes.

XDP is used for ingress because it fires before the kernel network stack, giving
the most faithful `t3`. tc egress is the only option for `t7`.

## Clocks

The kernel stamps `CLOCK_MONOTONIC` (`bpf_ktime_get_ns`). Userspace samples a
`realtime - monotonic` offset once at startup and adds it to every stamp, so
`t3`/`t7` land in the same **CLOCK_REALTIME** domain as the bot fleet's
`t0/t1/r9`, and `t7 - t3` stays exact (the offset cancels).

## Deployment notes

- Disable segmentation offload on the algo veth so tc egress sees per-MTU
  segments (no 64 KB GSO super-segments) and `captured_len == payload_len`:
  `ethtool -K <veth> tso off gso off gro off lro off`. Pairs with `TCP_NODELAY`
  on the bot for a clean `t3`. The `TRUNCATED_CAPTURES` counter stays 0 when this
  is set. The loader runs this itself at attach time (best-effort).
- The loader also clamps the capture interface MTU to `CAPTURE_CLAMP_MTU`
  (default 9001 since `ccf0f80`, `0` disables) at attach time, via raw `SIOCGIFMTU`/`SIOCSIFMTU`
  ioctls (no iproute2 needed in the image). Offloads off only bounds segments by
  the MTU — on EKS the pod veth inherits the node ENI's 9001-byte jumbo MTU, so
  a single full-MTU segment would still exceed the kernel's 1536-byte
  `CAPTURE_CAP` and force a lossy flow reset. The clamp only ever lowers the
  MTU (old -> new is logged) and is best-effort: failure logs loudly but never
  aborts the capture.
- Shutdown: SIGTERM (how Kubernetes stops the per-slot Job) and SIGINT both
  flush the buffered `orders.acked` tail and exit 0, so teardown loses no
  events and the Job completes Succeeded.
- In Kubernetes, set `EBPF_NETNS_PATH=/proc/<algo-pid>/ns/net` and
  `EBPF_IFACE=eth0`; the loader enters that netns to attach (the offload
  disable and MTU clamp run inside it too).

## CaptureRecord ABI

Fixed 28-byte header + `captured_len` payload bytes (variable-length record):

```c
struct capture_record {
    __u64 timestamp_ns;   // bpf_ktime_get_ns (CLOCK_MONOTONIC; userspace -> realtime)
    __u32 client_ip;      // flow identity (bot side), host order, both directions
    __u32 tcp_seq;        // this segment's starting sequence
    __u32 payload_len;    // full on-wire TCP payload length
    __u16 client_port;    // flow identity
    __u16 server_port;    // 9898 (FIX) or 8080 (REST/WS)
    __u16 captured_len;   // bytes copied (== min(payload_len, 1536))
    __u8  direction;      // 0 = request (XDP ingress), 1 = response (tc egress)
    __u8  _pad;
    __u8  payload[captured_len];
};
```

## Required environment

- `SESSION_ID`, `CONTESTANT_ID`, `EBPF_IFACE`, `EBPF_OBJECT_PATH`, `KAFKA_BROKERS`

## Optional environment

- `ORDERS_ACKED_TOPIC` (default `orders.acked`)
- `EBPF_NETNS_PATH`, `EBPF_XDP_INGRESS_PROGRAM`, `EBPF_TC_EGRESS_PROGRAM`,
  `EBPF_RINGBUF_MAP`, `EBPF_FLUSH_INTERVAL_MS`, `EBPF_BATCH_SIZE`
- `CAPTURE_CLAMP_MTU` (default `9001` — the EKS jumbo MTU, which fits one
  `CAPTURE_CAP`-sized record since `ccf0f80`; `1500` is the rollback lever; `0`
  disables the attach-time MTU clamp; valid range otherwise 68–65535)

## Tests

Fast, unprivileged unit tests (capture/reassembly/parse/matcher/pipeline):

```bash
cargo test -p iicpc-ebpf-latency
```

Build the BPF object:

```bash
cargo +nightly build --release -p iicpc-ebpf-latency --lib \
  --target bpfel-unknown-none --features ebpf -Z build-std=core
```

Real eBPF integration test — loads the program (the BPF verifier runs here),
attaches to a netns veth, drives FIX/REST/WS round-trips (including partial fills
and two pipelined orders), and asserts the events produced by the real userspace
pipeline. Needs root + `bpf-linker`/nightly (or `EBPF_OBJECT_PATH`):

```bash
IICPC_REAL_EBPF_STRICT=1 sudo -E env "PATH=$PATH" \
  cargo test -p iicpc-ebpf-latency --test real_ebpf -- --ignored --nocapture
```

MTU clamp integration test — creates a real veth pair at the EKS jumbo default
(9001) and verifies the `SIOCSIFMTU` clamp lowers it to 1500, never raises a
smaller MTU, and honors `CAPTURE_CLAMP_MTU=0`. Needs root + iproute2:

```bash
IICPC_REAL_EBPF_STRICT=1 sudo -E env "PATH=$PATH" \
  cargo test -p iicpc-ebpf-latency --test mtu_clamp -- --ignored --nocapture
```

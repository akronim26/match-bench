# Build context is the repository root (COPY paths are repo-relative).

# Stage 1 — compile the kernel-side BPF object.
# Needs the nightly toolchain + rust-src (for -Z build-std=core) and bpf-linker
# (which links against LLVM). The object is target/bpfel-unknown-none/release/
# libiicpc_ebpf_latency.so and is baked into the runtime image below.
FROM rust:bookworm AS bpf-builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends llvm clang libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN rustup toolchain install nightly --component rust-src \
    && cargo install bpf-linker --locked
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
# telemetry-ingester is a workspace member, so its manifest must be present for
# cargo to load the workspace — even though this -p selection never compiles it.
# A new workspace member added to the root Cargo.toml must also be copied here.
COPY services/telemetry-ingester services/telemetry-ingester
RUN cargo +nightly build --release -p iicpc-ebpf-latency --lib \
    --target bpfel-unknown-none --features ebpf -Z build-std=core

# Stage 2 — compile the userspace binary. It pulls in iicpc-bot-fleet -> rdkafka
# with the `cmake-build` feature (vendored librdkafka 2.3.0 built from source), so
# this stage needs cmake + a C/C++ toolchain on top of librdkafka-dev/pkg-config.
# (The BPF stage above never compiles rdkafka — bot-fleet is gated to the non-BPF
# target in ebpf-latency/Cargo.toml — so it needs none of this.)
#
# clang + libclang-dev come along for rdkafka's `zstd` feature (zstd-1 compression
# on orders.*), which pulls zstd-sys -> bindgen -> dlopen(libclang) at build time.
# Stage 1 already installs them for bpf-linker; this stage needs its own copy.
FROM rust:bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential librdkafka-dev pkg-config clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
COPY services/telemetry-ingester services/telemetry-ingester
RUN cargo build --release -p iicpc-ebpf-latency

# Stage 3 — runtime. Runs as root (needs CAP_BPF/CAP_NET_ADMIN/CAP_SYS_ADMIN +
# setns into the algo pod's netns); the Job's securityContext grants those.
FROM debian:bookworm-slim
# ethtool: the capture disables GSO/TSO/GRO/LRO on the algo veth at attach time so
# iproute2: `ip link set ... gso_max_size/gso_max_segs` bounds skb construction upstream
# of the tc egress hook. ethtool alone is NOT enough — the hook runs before GSO
# segmentation, so it sees 64KiB coalesced skbs whatever the offload flags say, and
# everything past the 1536-byte capture cap is lost (measured: 25.6% of orders lost every
# response record they had).
# the hooks see one MTU-sized packet per message (avoids CAPTURE_CAP truncation).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 ethtool iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-ebpf-latency /usr/local/bin/iicpc-ebpf-latency
COPY --from=bpf-builder /app/target/bpfel-unknown-none/release/libiicpc_ebpf_latency.so \
     /opt/iicpc/ebpf/libiicpc_ebpf_latency.so
ENV EBPF_OBJECT_PATH=/opt/iicpc/ebpf/libiicpc_ebpf_latency.so
ENTRYPOINT ["/usr/local/bin/iicpc-ebpf-latency"]

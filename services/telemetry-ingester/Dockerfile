# Build context is the repository root (COPY paths are repo-relative).
# Rust 1.96 (not 1.79): workspace deps now use edition 2024 (e.g. idna_adapter
# ≥1.2.2, rust-version 1.86), which a 1.79 cargo cannot parse. `-bookworm` is kept
# for parity with the other Rust images (Debian Bookworm toolchain libs).
FROM rust:1.96-bookworm AS builder
WORKDIR /app

# rdkafka is built with the `cmake-build` feature (vendored librdkafka), so the
# builder needs cmake + a C/C++ toolchain. librdkafka-dev/pkg-config are kept for
# parity with the other Rust service images.
#
# clang + libclang-dev are for the `zstd` rdkafka feature (zstd-1 compression on
# orders.*): it pulls zstd-sys, whose build script runs bindgen, which dlopens
# libclang at build time. Without it the build dies with "Unable to find libclang"
# — a host build hides this whenever the developer happens to have clang installed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential librdkafka-dev pkg-config clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# The whole Cargo workspace must be present for cargo to resolve it — copy every
# member, not just this crate.
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
COPY services/telemetry-ingester services/telemetry-ingester
RUN cargo build --release -p iicpc-telemetry-ingester

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-telemetry-ingester /usr/local/bin/iicpc-telemetry-ingester
COPY --from=builder /app/target/release/rollup /usr/local/bin/telemetry-rollup
# Synthetic pipeline-load injector (benchmark tool; run as a Job with command override).
COPY --from=builder /app/target/release/inject /usr/local/bin/inject
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/iicpc-telemetry-ingester"]

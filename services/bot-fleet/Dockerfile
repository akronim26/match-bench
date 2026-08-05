FROM rust:1.96-bookworm AS builder
WORKDIR /app

# Rust 1.96 (bumped from 1.79): workspace dependencies now use edition 2024
# (e.g. idna_adapter ≥1.2.2, rust-version 1.86), which a 1.79 cargo cannot parse.
#
# rdkafka uses the `cmake-build` feature — librdkafka 2.3.0 is built from the
# vendored rdkafka-sys source (see bot-fleet/Cargo.toml), NOT the system package —
# so the builder needs cmake + a C/C++ toolchain. Because the build is vendored the
# system librdkafka version is irrelevant; librdkafka-dev/pkg-config and the
# Bookworm base are kept only for parity with the other Rust service images.
#
# clang + libclang-dev are for the `zstd` rdkafka feature (zstd-1 compression on
# orders.*): it pulls zstd-sys, whose build script runs bindgen, which dlopens
# libclang at build time. Without it the build dies with "Unable to find libclang"
# — a host build hides this whenever the developer happens to have clang installed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential librdkafka-dev pkg-config clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the ENTIRE Cargo workspace. Cargo loads every member listed in the root
# Cargo.toml when it resolves, so all member manifests must be present even though
# `-p iicpc-bot-fleet` compiles only this crate (a missing member fails workspace
# load). Only this crate + its deps are actually compiled, so the other members'
# build-time deps (e.g. telemetry-ingester's vendored librdkafka) are never pulled
# in. The shared root Cargo.lock keeps the build reproducible. NOTE: a new
# workspace member added to Cargo.toml must also be added here.
COPY Cargo.toml Cargo.lock* ./
COPY libs/rust libs/rust
COPY schemas/rust schemas/rust
COPY services/bot-fleet services/bot-fleet
COPY services/ebpf-latency services/ebpf-latency
COPY services/telemetry-ingester services/telemetry-ingester
RUN cargo build --release -p iicpc-bot-fleet

FROM debian:bookworm-slim
# librdkafka1 is the shared library the binary linked against at build
# time. Without it the binary aborts at startup with "cannot open shared
# object file: librdkafka.so.1".
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iicpc-bot-fleet /usr/local/bin/iicpc-bot-fleet
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/iicpc-bot-fleet"]

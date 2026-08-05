# Contestant stand-in for the e2e: the in-repo fix_echo_server example, which
# ACKs every NewOrderSingle (35=8, OrdStatus=New). Built from repo root:
#   docker build -f services/bot-fleet/contestant-echo.Dockerfile -t <ref> .
FROM rust:1.96-bookworm AS builder
# clang + libclang-dev: this compiles iicpc-bot-fleet, whose rdkafka carries the
# `zstd` feature (zstd-1 on orders.*) -> zstd-sys -> bindgen -> dlopen(libclang).
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
RUN cargo build --release -p iicpc-bot-fleet --example fix_echo_server

FROM debian:12-slim
COPY --from=builder /app/target/release/examples/fix_echo_server /usr/local/bin/fix_echo_server
# Bind all interfaces on the capturable port 9898 (the eBPF program filters
# 9898/8080) so the orchestrator's TCP readiness probe + the bots can reach it.
ENV BIND=0.0.0.0:9898
EXPOSE 9898
ENTRYPOINT ["/usr/local/bin/fix_echo_server"]

# The reference contestant (e2e/contestant-matching-engine) as a prebuilt IMAGE.
#
# In production this crate arrives as a submission ZIP and the build-worker generates
# its Dockerfile and builds it in-cluster with Kaniko. That path is worth testing on
# its own, but it is not what the two-concurrent-sessions run (B1) is testing —
# folding an in-cluster build into that run just adds a second failure source to a
# test about controller dispatch, leases and slot isolation. So B1 registers already
# `ready` submissions pointing at images built here.
#
# Build from the crate directory (it is a standalone crate, NOT a workspace member):
#   docker build -f deploy-local/contestant-matching-engine.Dockerfile \
#     -t iicpc/contestant-matching-engine:dev1 e2e/contestant-matching-engine
FROM rust:1.96-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src src
RUN cargo build --release --bin matching_engine

FROM debian:12-slim
COPY --from=builder /app/target/release/matching_engine /usr/local/bin/matching_engine
# Port 9898 is platform-mandated for FIX (the eBPF capture filters 9898/8080 only —
# see schemas PORT_FIX). Bind all interfaces so the orchestrator's TCP readiness
# probe and the bots can reach it.
ENV BIND=0.0.0.0:9898
# Both platform-mandated ports: this engine is dual-listener (FIX on 9898, REST and
# WS shape-multiplexed on 8080 — WS starts as an HTTP Upgrade on the same socket),
# which is what the mixed-protocol ProtocolAll run needs.
EXPOSE 9898 8080
ENTRYPOINT ["/usr/local/bin/matching_engine"]

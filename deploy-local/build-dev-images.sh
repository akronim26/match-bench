#!/usr/bin/env bash
# Build every platform image FROM THE WORKING TREE, tagged iicpc/<svc>:$TAG.
#
# Why not deploy-local/build-images.sh: that one tags ghcr.io/agrawalx/<svc>:demo,
# the same reference the committed manifests carry. The published :demo images are
# stale (they predate this branch), and reusing the tag makes "did this pod get my
# code?" unanswerable. A distinct local tag + an explicit `set image` per deployment
# (see up-dev.sh) removes the ambiguity: nothing can silently fall back to a public
# stale image, because iicpc/* does not exist on any registry.
#
# Order: Go services first (seconds each), Rust last (minutes each) — a typo in the
# Go set surfaces before the long tail.
#
#   TAG=dev1 deploy-local/build-dev-images.sh
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="${TAG:-dev1}"

bld() {
  echo "=== build $2  ($(date +%H:%M:%S)) ==="
  docker build -q -f "$1" -t "$2" "${3:-.}"
}

# Go services (repo-root context; each Dockerfile COPYs only schemas/go, libs/go
# and its own service dir — .dockerignore keeps target/ out of the context).
bld services/submission-api/Dockerfile        iicpc/submission-api:$TAG
bld services/build-worker/Dockerfile          iicpc/spawner:$TAG
bld services/sandbox-orchestrator/Dockerfile  iicpc/sandbox-orchestrator:$TAG
bld services/bot-fleet-controller/Dockerfile  iicpc/bot-fleet-controller:$TAG
bld services/correctness-validator/Dockerfile iicpc/correctness-validator:$TAG
bld services/auth-api/Dockerfile              iicpc/auth-api:$TAG
bld services/score-computer/Dockerfile        iicpc/score-computer:$TAG
bld services/leaderboard-api/Dockerfile       iicpc/leaderboard-api:$TAG

# Frontend (frontend/ context, has its own .dockerignore)
bld frontend/Dockerfile                       iicpc/frontend:$TAG frontend

# Rust services (repo-root context; each compiles the whole workspace member set
# it COPYs, so these are the slow ones)
bld services/bot-fleet/Dockerfile             iicpc/bot-fleet:$TAG
bld services/telemetry-ingester/Dockerfile    iicpc/telemetry-ingester:$TAG
bld services/ebpf-latency/Dockerfile          iicpc/ebpf-latency:$TAG

echo "=== ALL IMAGES BUILT ($TAG) ==="
docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' | grep "^iicpc/.*:$TAG" | sort

#!/usr/bin/env bash
# Build the two contestant images the local verification runs use.
#
#   contestant-matching-engine — a correct price-time-priority book (dual listener:
#     FIX 9898 + REST/WS 8080). Expected to QUALIFY (correctness >= 0.95).
#   contestant-echo — ACKs every order, no book. Expected to be DISQUALIFIED on
#     correctness. Two contestants with *different* verdicts is the point: it makes
#     "scores are isolated per session" an observable claim rather than an assumption.
#
#   TAG=dev1 deploy-local/build-contestants.sh
# then import them into k3s (root):
#   TAG=dev1 sudo -E deploy-local/import-dev-images.sh
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="${TAG:-dev1}"

echo "=== build iicpc/contestant-matching-engine:$TAG ==="
docker build -f deploy-local/contestant-matching-engine.Dockerfile \
  -t "iicpc/contestant-matching-engine:$TAG" e2e/contestant-matching-engine

echo "=== build iicpc/contestant-echo:$TAG ==="
docker build -f services/bot-fleet/contestant-echo.Dockerfile \
  -t "iicpc/contestant-echo:$TAG" .

docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' | grep "^iicpc/contestant.*:$TAG"

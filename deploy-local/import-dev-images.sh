#!/usr/bin/env bash
# Import the locally built iicpc/*:$TAG images into k3s's containerd.
#
# k3s does not use the Docker daemon, so a `docker build` result is invisible to it.
# There is no registry in the loop: the images are streamed straight into containerd's
# image store, and the manifests' IfNotPresent pull policy then finds them. Because
# iicpc/* exists on no registry, a failed import shows up immediately as ErrImagePull
# rather than as a silent fall back to a stale public image.
#
# Needs root (containerd's socket). Run:
#   TAG=dev1 sudo -E deploy-local/import-dev-images.sh
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="${TAG:-dev1}"

IMAGES=(
  iicpc/submission-api:$TAG
  iicpc/spawner:$TAG
  iicpc/sandbox-orchestrator:$TAG
  iicpc/bot-fleet-controller:$TAG
  iicpc/correctness-validator:$TAG
  iicpc/auth-api:$TAG
  iicpc/score-computer:$TAG
  iicpc/leaderboard-api:$TAG
  iicpc/frontend:$TAG
  iicpc/bot-fleet:$TAG
  iicpc/telemetry-ingester:$TAG
  iicpc/ebpf-latency:$TAG
)

# Contestant images are optional here: the platform comes up without them, and they
# are only needed for the terminal run flow (b1-two-sessions.sh). Import whichever
# have been built so the run does not need a second root step.
for c in iicpc/contestant-matching-engine:$TAG iicpc/contestant-echo:$TAG; do
  docker image inspect "$c" >/dev/null 2>&1 && IMAGES+=("$c")
done

missing=()
for i in "${IMAGES[@]}"; do
  docker image inspect "$i" >/dev/null 2>&1 || missing+=("$i")
done
if [ ${#missing[@]} -gt 0 ]; then
  echo "!! not built yet: ${missing[*]}"
  echo "!! run: TAG=$TAG deploy-local/build-dev-images.sh"
  exit 1
fi

# One save | import for the whole set: the images share base layers, so a single
# stream is far smaller and faster than twelve.
echo ">> saving ${#IMAGES[@]} images and importing into k3s containerd..."
docker save "${IMAGES[@]}" | k3s ctr images import --digests=true -

echo ">> imported:"
k3s ctr images ls -q | grep "^docker.io/iicpc/.*:$TAG" || {
  echo "!! nothing matched docker.io/iicpc/*:$TAG in containerd"; exit 1; }

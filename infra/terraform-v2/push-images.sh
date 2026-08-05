#!/usr/bin/env bash
# infra/terraform-v2/push-images.sh — build + push every service image to ECR
# and stamp the immutable tag into the eks-contest overlay.
#
#   ./push-images.sh [tag]        default tag: git short sha (immutable repos:
#                                 re-pushing an existing tag FAILS, by design —
#                                 new code means a new tag)
#
# bot-fleet is the ONE dual-arch build (linux/amd64 + linux/arm64 manifest
# list) and REQUIRES a docker-container buildx builder — the default docker
# driver cannot do multi-platform (bitten 2026-08-02):
#   docker buildx create --name multiarch --driver docker-container
# plus binfmt/QEMU once per machine:
#   docker run --privileged --rm tonistiigi/binfmt --install arm64
# Everything else is x86-only and uses plain docker build+push (reuses the
# daemon layer cache). Includes the measurement FIXTURES (drain-sink,
# stall-sink) — platform tooling, not contestants; real contestant images are
# produced ONLY by the build pipeline from submitted zips (decided
# 2026-08-02; push-contestants.sh deleted for that reason).
set -euo pipefail
cd "$(dirname "$0")/../.."

REGION="${REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
TAG="${1:-$(git rev-parse --short HEAD)}"
OVERLAY="overlays/eks-contest/kustomization.yaml"

echo ">> registry $REGISTRY  tag $TAG"
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"

# Reuse the same Dockerfile->image mapping as deploy-local/build-dev-images.sh.
# Format: <dockerfile> <repo> [context]
x86_builds() {
  cat <<EOF
services/submission-api/Dockerfile        iicpc/submission-api
services/build-worker/Dockerfile          iicpc/spawner
services/sandbox-orchestrator/Dockerfile  iicpc/sandbox-orchestrator
services/bot-fleet-controller/Dockerfile  iicpc/bot-fleet-controller
services/correctness-validator/Dockerfile iicpc/correctness-validator
services/auth-api/Dockerfile              iicpc/auth-api
services/score-computer/Dockerfile        iicpc/score-computer
services/leaderboard-api/Dockerfile       iicpc/leaderboard-api
frontend/Dockerfile                       iicpc/frontend frontend
services/telemetry-ingester/Dockerfile    iicpc/telemetry-ingester
services/ebpf-latency/Dockerfile          iicpc/ebpf-latency
deploy-local/drain-sink/Dockerfile        iicpc/drain-sink deploy-local/drain-sink
deploy-local/stall-sink/Dockerfile        iicpc/stall-sink deploy-local/stall-sink
EOF
}

while read -r df repo ctx; do
  [ -n "$df" ] || continue
  echo "=== amd64 build+push $repo:$TAG  ($(date +%H:%M:%S)) ==="
  docker build -f "$df" -t "$REGISTRY/$repo:$TAG" "${ctx:-.}"
  docker push "$REGISTRY/$repo:$TAG"
done < <(x86_builds)

echo "=== MULTI-ARCH build+push iicpc/bot-fleet:$TAG (amd64+arm64) ==="
docker buildx build --builder multiarch --platform linux/amd64,linux/arm64 \
  -f services/bot-fleet/Dockerfile -t "$REGISTRY/iicpc/bot-fleet:$TAG" --push .

echo ">> stamping tag $TAG into $OVERLAY"
sed -i "s/newTag: .*/newTag: \"$TAG\"/" "$OVERLAY"

# Verify the stamp actually reaches everything, then FAIL rather than deploy a
# stale image. The `images:` transformer only rewrites container image FIELDS;
# two first-party images travel as env vars instead (CAPTURE_IMAGE ->
# capture Jobs, SPAWNER_IMAGE -> build-Job fetch initContainer) and both
# silently shipped ghcr :demo to EKS on 2026-08-03. The overlay now derives
# them via `replacements:`; this asserts that stays true.
echo ">> verifying every first-party reference in $OVERLAY resolves to $TAG"
RENDER="$(kubectl kustomize overlays/eks-contest)"

# Any surviving ghcr.io ref is a first-party image the overlay failed to map.
if printf '%s\n' "$RENDER" | grep -n "ghcr.io/"; then
  echo "!! the refs above are unmapped first-party images — add them to \`images:\`" >&2
  echo "!! (if the ref is an env var, \`images:\` cannot reach it: add a \`replacements:\` entry)" >&2
  exit 1
fi

# Env-var-carried images: assert the resolved tag is this push's tag.
check_env_image() {  # <env name> <expected repo suffix>
  got="$(printf '%s\n' "$RENDER" | grep -A1 "name: $1\$" | grep 'value:' | awk '{print $2}')"
  case "$got" in
    */"$2:$TAG") echo ">> $1 OK: $got" ;;
    *) echo "!! $1 did not stamp: got '${got:-<missing>}', want */$2:$TAG" >&2; exit 1 ;;
  esac
}
check_env_image CAPTURE_IMAGE iicpc/ebpf-latency
check_env_image SPAWNER_IMAGE iicpc/spawner

echo
echo "############ PUSHED $TAG ############"
echo ">> commit the overlay stamp, then: kubectl apply -k overlays/eks-contest"

#!/usr/bin/env bash
# production/02-images.sh — build every service image, push to ECR, stamp the
# overlay, and REFUSE to proceed if the stamp did not land everywhere.
#
# Rewritten from infra/terraform-v2/push-images.sh rather than calling it, so
# production/ carries no dependency on the legacy path. Four properties of the
# original are load-bearing and are preserved deliberately:
#
#   1. ECR repos are IMMUTABLE. Re-pushing an existing tag FAILS, so a partial
#      push must be recovered with a NEW tag, never a retry. Checked up front
#      instead of discovered halfway through.
#   2. bot-fleet is the ONE multi-arch image (amd64 + arm64 manifest list) and
#      needs a docker-container buildx builder; the default driver cannot do it.
#   3. The tag is the git short sha, so "did this pod get my code" is answerable
#      by construction — but only if the tree is clean, which is now checked.
#   4. THE STAMP GUARD. Two images (CAPTURE_IMAGE, SPAWNER_IMAGE) travel as env
#      vars, which kustomize's `images:` transformer cannot rewrite. On
#      2026-08-03 that shipped a stale public :demo capture to EKS and the
#      platform graded nothing while looking healthy. The guard fails the push
#      if any ghcr.io ref survives the render, or if either env var does not
#      resolve to the tag just pushed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/aws.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

# <dockerfile> <repo> [build context]
x86_builds() {
  cat <<'EOF'
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

main() {
  phase "Images"
  need docker; need aws; need kubectl; need git

  local registry tag
  registry="$(ecr_registry)"
  tag="${1:-$(git -C "$ROOT" rev-parse --short HEAD)}"
  info "registry=$registry tag=$tag"

  # A dirty tree means the tag names a commit the images do not contain, which
  # defeats the whole point of tagging by sha. Warn rather than block: config
  # and doc edits are routine and do not change image contents.
  if [ -n "$(git -C "$ROOT" status --porcelain -- services/ frontend/ libs/ schemas/ 2>/dev/null)" ]; then
    warn "uncommitted changes under services/ frontend/ libs/ schemas/"
    warn "tag '$tag' will not describe what is in these images"
  fi

  step "ecr login"
  ecr_login || die "ecr login failed"
  ok "authenticated to $registry"

  step "immutability precheck"
  # ECR rejects a re-push of an existing tag, so find out now rather than after
  # six successful pushes have left the registry half-updated.
  local df repo ctx clash=()
  while read -r df repo ctx; do
    [ -n "$df" ] || continue
    ecr_tag_exists "$repo" "$tag" && clash+=("$repo:$tag")
  done < <(x86_builds)
  ecr_tag_exists "iicpc/bot-fleet" "$tag" && clash+=("iicpc/bot-fleet:$tag")
  if [ ${#clash[@]} -gt 0 ]; then
    die "tag '$tag' already exists in ECR (${clash[*]}). Repos are IMMUTABLE — re-run with a new tag, e.g. $0 ${tag}-r2"
  fi
  ok "tag '$tag' is free in every repo"

  step "build + push (amd64)"
  while read -r df repo ctx; do
    [ -n "$df" ] || continue
    info "$repo"
    docker build -q -f "$ROOT/$df" -t "$registry/$repo:$tag" "$ROOT/${ctx:-.}" >/dev/null \
      || die "build failed: $repo ($df)"
    docker push -q "$registry/$repo:$tag" >/dev/null \
      || die "push failed: $repo:$tag"
    ok "$repo:$tag"
  done < <(x86_builds)

  step "build + push (multi-arch: bot-fleet)"
  # Graviton botworker nodes pull arm64 from the same tag via the manifest list.
  docker buildx build --builder multiarch --platform linux/amd64,linux/arm64 \
    -f "$ROOT/services/bot-fleet/Dockerfile" \
    -t "$registry/iicpc/bot-fleet:$tag" --push "$ROOT" >/dev/null \
    || die "multi-arch build failed for bot-fleet (is the 'multiarch' buildx builder running?)"
  ok "iicpc/bot-fleet:$tag (amd64+arm64)"

  step "stamp the overlay"
  # Writing the tag into the declarative source, which is then applied. This is
  # not imperative configuration — it is how the overlay learns which build to
  # run, and `apply -k` remains the only thing that talks to the cluster.
  sed -i "s/newTag: .*/newTag: \"$tag\"/" "$ROOT/$OVERLAY/kustomization.yaml"
  ok "stamped $OVERLAY/kustomization.yaml"

  step "stamp guard"
  local render
  render="$(kubectl kustomize "$ROOT/$OVERLAY")" || die "overlay does not render after stamping"

  # Any surviving ghcr.io ref is a first-party image the overlay failed to map.
  local ghcr
  ghcr="$(printf '%s\n' "$render" | grep -n 'ghcr.io/' || true)"
  if [ -n "$ghcr" ]; then
    printf '%s\n' "$ghcr" | sed 's/^/      /'
    fail "unmapped first-party image refs above — add them to \`images:\`, or a \`replacements:\` entry if the ref is an env var"
  else
    ok "no ghcr.io refs survive the render"
  fi

  # The two env-carried images. This is the check that would have caught the
  # stale :demo capture on 2026-08-03.
  local var want got
  for var in CAPTURE_IMAGE:iicpc/ebpf-latency SPAWNER_IMAGE:iicpc/spawner; do
    want="${var#*:}"; var="${var%%:*}"
    # `awk ... exit` rather than `| head -1`: head closes the pipe after one
    # line, awk takes SIGPIPE, and pipefail turns that into a failed command
    # substitution under `set -e`. Same hazard as `| grep -q`.
    got="$(grep -A1 "name: $var\$" <<<"$render" | grep 'value:' | awk '{print $2; exit}')"
    case "$got" in
      */"$want:$tag") ok "$var -> $got" ;;
      *) fail "$var did not stamp: got '${got:-<missing>}', want */$want:$tag" ;;
    esac
  done

  finish "Images"
}

main "$@"

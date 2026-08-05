#!/usr/bin/env bash
# production/00-preflight.sh — fail before spending money, not mid-nodegroup.
#
# Everything here is read-only. It exists because each check below has, at least
# once, been discovered the expensive way: after `terraform apply` started
# billing, or worse, after a deploy silently targeted the wrong cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/aws.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

# Standard on-demand vCPU needed by the baseline shape:
#   2x m6i.2xlarge (16) + 1x m6i.xlarge (4) + 1x c6i.2xlarge (8) + 2x c7g.xlarge (8) = 36
# 48 is the requested floor, leaving headroom for a rolling node replacement.
: "${STD_VCPU_NEED:=48}"

main() {
  phase "Preflight"

  step "tooling"
  need aws     "https://docs.aws.amazon.com/cli/"
  need kubectl
  need terraform
  need docker
  need python3
  need zip
  ok "aws, kubectl, terraform, docker, python3, zip"

  step "identity"
  local acct
  acct="$(aws_account_id)" || die "no AWS credentials for profile '$AWS_PROFILE'"
  [ -n "$acct" ] || die "no AWS credentials for profile '$AWS_PROFILE'"
  info "profile=$AWS_PROFILE account=$acct region=$AWS_REGION"
  # The default profile on a dev machine is routinely a different account in a
  # different region. Building the wrong cluster is silent until teardown.
  ok "credentials resolve"

  step "quota"
  # ONE Standard quota (L-1216C47A) covers x86 AND Graviton — c7g is C-family,
  # and there is no separate Graviton quota. L-DB2E81BA ("G and VT") is GPU
  # instances; requesting it is a wasted support ticket.
  local std
  std="$(standard_vcpu_quota)"
  if awk -v h="$std" -v n="$STD_VCPU_NEED" 'BEGIN{exit !(h+0 < n+0)}'; then
    fail "Standard on-demand vCPU quota is ${std%.*}, need >= $STD_VCPU_NEED (Service Quotas -> EC2 -> L-1216C47A)"
  else
    ok "Standard on-demand vCPU quota ${std%.*} >= $STD_VCPU_NEED"
  fi

  step "instance types offered in $AWS_REGION"
  local t
  for t in m6i.2xlarge m6i.xlarge c6i.2xlarge c7g.xlarge; do
    if [ "$(instance_type_offered "$t")" = "0" ]; then
      fail "$t is not offered in $AWS_REGION"
    else
      ok "$t"
    fi
  done

  step "multi-arch build support"
  # bot-fleet is the ONE dual-arch image (amd64 + arm64 manifest list) and the
  # default docker driver cannot do multi-platform builds.
  if docker buildx inspect multiarch >/dev/null 2>&1; then
    ok "buildx builder 'multiarch' exists"
  else
    fail "buildx builder 'multiarch' missing — docker buildx create --name multiarch --driver docker-container"
  fi
  if [ -e /proc/sys/fs/binfmt_misc/qemu-aarch64 ]; then
    ok "arm64 binfmt registered"
  else
    fail "arm64 binfmt missing — docker run --privileged --rm tonistiigi/binfmt --install arm64"
  fi
  docker info >/dev/null 2>&1 && ok "docker daemon reachable" || fail "docker daemon not reachable"

  step "declarative assets present"
  local p
  for p in "$TF_DIR/$TFVARS" "$OVERLAY/kustomization.yaml" "$SMOKE_FIXTURE"; do
    [ -e "$ROOT/$p" ] && ok "$p" || fail "missing: $p"
  done
  # backend.hcl is gitignored and per-account; without it `terraform init` has
  # no state backend and would silently start a fresh local state.
  [ -e "$ROOT/$TF_DIR/backend.hcl" ] \
    && ok "$TF_DIR/backend.hcl" \
    || fail "missing: $TF_DIR/backend.hcl (cp backend.hcl.example backend.hcl and fill in the bucket)"

  step "stale port-forwards"
  # A forward binds to whatever context was active when it launched, so one left
  # over from a previous session is a silent tunnel into the WRONG cluster —
  # you read the wrong leaderboard and believe it.
  local stale
  stale="$(pgrep -af 'kubectl.*port-forward' 2>/dev/null | grep -c . || true)"
  if [ "${stale:-0}" -gt 0 ]; then
    warn "$stale kubectl port-forward process(es) already running:"
    pgrep -af 'kubectl.*port-forward' 2>/dev/null | sed 's/^/      /' || true
    warn "kill them before trusting anything on localhost"
  else
    ok "no pre-existing port-forwards"
  fi

  finish "Preflight"
}

main "$@"

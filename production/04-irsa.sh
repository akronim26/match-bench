#!/usr/bin/env bash
# production/04-irsa.sh — the second terraform apply, now that the SA exists.
#
# Why this is a separate phase and not part of 01: terraform's
# kubernetes_annotations resource PATCHES an existing ServiceAccount and has no
# depends_on. build-spawner is created by the platform manifests (phase 03), so
# on a fresh cluster the first apply has nothing to annotate — 01 therefore runs
# with -var enable_spawner_irsa=false, and this phase runs without the override
# so the committed `true` in contest.tfvars takes effect.
#
# Skipping this is not subtle in its consequences but IS subtle in its symptom:
# the spawner's ECR CreateRepository falls through the ENTIRE AWS credential
# chain down to EC2 IMDS, finds no role, and the submission goes `failed` with
# no build Job ever created. That cost a submission on 2026-08-03.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/k8s.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

main() {
  phase "IRSA"
  need terraform; need kubectl
  require_context "$CLUSTER_NAME"

  step "precondition"
  kubectl -n build get sa build-spawner >/dev/null 2>&1 \
    || die "ServiceAccount build/build-spawner does not exist — run 03-platform.sh first"
  ok "build/build-spawner exists"

  step "terraform apply (irsa enabled)"
  terraform -chdir="$ROOT/$TF_DIR" apply \
    -var-file="$TFVARS" \
    -input=false -auto-approve \
    || die "terraform apply failed"
  ok "applied"

  step "annotation"
  local arn
  arn="$(sa_annotation build build-spawner 'eks\.amazonaws\.com/role-arn')"
  if [ -n "$arn" ]; then
    ok "build-spawner -> $arn"
  else
    die "build-spawner still has no eks.amazonaws.com/role-arn — check enable_spawner_irsa in $TFVARS"
  fi

  step "restart spawner"
  # Required, not cosmetic: the web-identity env (AWS_ROLE_ARN,
  # AWS_WEB_IDENTITY_TOKEN_FILE) is injected by the mutating webhook at POD
  # CREATION. A spawner already running when the SA was annotated does not pick
  # it up, and keeps failing exactly as if the annotation were absent.
  kubectl -n build rollout restart deploy/spawner >/dev/null || die "rollout restart failed"
  kubectl -n build rollout status deploy/spawner --timeout=180s >/dev/null \
    || die "spawner did not come back up"
  ok "spawner restarted"

  step "credentials injected"
  # Read the POD, never the Deployment. The IRSA env (AWS_ROLE_ARN,
  # AWS_WEB_IDENTITY_TOKEN_FILE, AWS_REGION, ...) is added by the EKS pod
  # identity mutating webhook at POD CREATION, so it exists only on the pod
  # spec — the Deployment's container env is untouched and always empty of
  # AWS_*. Checking the Deployment produces a false negative on a cluster where
  # IRSA is working perfectly, which is exactly what it did on the first run.
  local pod pod_env role_env token_env
  pod="$(kubectl -n build get pods -l app=spawner \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [ -n "$pod" ] || die "no spawner pod found after rollout"
  pod_env="$(kubectl -n build get pod "$pod" \
    -o 'jsonpath={range .spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}' 2>/dev/null || true)"
  role_env="$(grep -m1 '^AWS_ROLE_ARN=' <<<"$pod_env" | cut -d= -f2- || true)"
  token_env="$(grep -m1 '^AWS_WEB_IDENTITY_TOKEN_FILE=' <<<"$pod_env" | cut -d= -f2- || true)"
  [ -n "$role_env" ]  && ok "AWS_ROLE_ARN=$role_env"              || fail "AWS_ROLE_ARN not injected into the spawner pod"
  [ -n "$token_env" ] && ok "AWS_WEB_IDENTITY_TOKEN_FILE present" || fail "AWS_WEB_IDENTITY_TOKEN_FILE not injected"

  finish "IRSA"
}

main "$@"

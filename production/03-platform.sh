#!/usr/bin/env bash
# production/03-platform.sh — namespaces, secrets, the overlay, topics.
#
# The order below is not arbitrary. Namespaces MUST precede secrets: nothing
# else in the tree creates the six namespaces, and creating a namespaced Secret
# into a missing namespace fails outright. The published bring-up guide used to
# say "secrets before the overlay", which only ever worked because the overlay
# had already been applied once — which is exactly how 20 pods ended up in
# CreateContainerConfigError.
#
# Applying namespaces first also means no pod ever ENTERS that error state,
# rather than entering it and recovering.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/k8s.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

NAMESPACES=(platform data build sandbox benchmark observability)

main() {
  phase "Platform"
  need kubectl
  require_context "$CLUSTER_NAME"

  step "namespaces"
  local ns args=()
  for ns in "${NAMESPACES[@]}"; do
    args+=(-f "$ROOT/k8s/$ns/namespace.yaml")
  done
  kubectl apply "${args[@]}" >/dev/null || die "namespace apply failed"
  for ns in "${NAMESPACES[@]}"; do ok "namespace/$ns"; done

  step "secrets"
  "$ROOT/production/secrets/load.sh" || die "secret creation failed"

  step "platform manifests"
  # The ONLY thing that configures the cluster. Never `set env`, never `patch`,
  # never a sed-mutated manifest piped to apply — if a deployment needs a value,
  # it is declared in k8s/ or overlays/ and arrives through this one command.
  #
  # stderr is inspected rather than counted: `apply` reports rejected resources
  # on stderr while still exiting 0, so `grep -c` on the output hides them.
  local out
  out="$(kubectl apply -k "$ROOT/$OVERLAY" 2>&1)" || die "overlay apply failed:\n$out"
  local bad
  bad="$(printf '%s\n' "$out" | grep -iE 'error|invalid|forbidden' || true)"
  if [ -n "$bad" ]; then
    printf '%s\n' "$bad" | sed 's/^/      /'
    die "overlay applied with rejected resources"
  fi
  ok "applied $OVERLAY ($(printf '%s\n' "$out" | grep -c . ) objects)"

  step "kafka topics"
  # Auto-create is OFF by design, so without this Job every publish silently
  # goes nowhere and the whole pipeline reports zeros with no error anywhere.
  if kubectl -n data wait --for=condition=complete job/kafka-topic-init --timeout=300s >/dev/null 2>&1; then
    ok "kafka-topic-init complete"
  else
    fail "kafka-topic-init did not complete within 300s"
    dim "debug: kubectl -n data logs job/kafka-topic-init"
    dim "note: Jobs are immutable — to re-run, delete it first, then re-apply the overlay"
  fi

  step "data plane"
  # Everything else depends on these, and services that dial Kafka or Postgres
  # at boot will CrashLoop until they are up. That churn is normal startup
  # ordering, not a fault — which is why the wait is here and not a failure.
  local sts
  for sts in kafka postgres timescaledb minio redis; do
    if kubectl -n data rollout status "statefulset/$sts" --timeout=300s >/dev/null 2>&1; then
      ok "statefulset/$sts"
    else
      fail "statefulset/$sts not ready within 300s"
    fi
  done

  step "application rollouts"
  wait_rollouts platform  submission-api leaderboard-api frontend
  wait_rollouts build     spawner
  wait_rollouts sandbox   sandbox-orchestrator
  wait_rollouts benchmark bot-fleet-controller bot-fleet-worker correctness-validator \
                          score-computer telemetry-ingester telemetry-rollup
  wait_rollouts observability grafana prometheus loki

  finish "Platform"
}

main "$@"

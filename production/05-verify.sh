#!/usr/bin/env bash
# production/05-verify.sh — gates, not suggestions. Non-zero exit on any failure.
#
# Every check here caught a real defect on 2026-08-03. The theme running through
# them: a cluster can be entirely healthy by every ordinary measure — all pods
# Running, all rollouts complete — and still grade nothing, because the two
# images that matter travel as env vars and the orchestrator cannot reach the
# contestant. "Green pods" is not the property we need; these are.
#
# Read-only throughout. Safe to re-run at any time against a live cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/k8s.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

gate_health() {
  step "gate 1: health"
  local unhealthy
  unhealthy="$(pods_not_healthy)"
  if [ -n "$unhealthy" ]; then
    printf '%s\n' "$unhealthy" | sed 's/^/      /'
    fail "$(printf '%s\n' "$unhealthy" | grep -c .) pod(s) not Running/Succeeded"
  else
    ok "every pod Running or Succeeded"
  fi

  # Restart counts are a softer signal: the services that dial Kafka/Postgres at
  # boot legitimately restart a few times during startup ordering. A count that
  # is still CLIMBING is the real problem, which a single sample cannot see —
  # so this warns rather than fails.
  local restarting
  restarting="$(kubectl get pods -A --no-headers 2>/dev/null \
    | awk '$5+0 > 3 {print "      "$1"/"$2" restarts="$5}' || true)"
  [ -n "$restarting" ] && { warn "pods with >3 restarts:"; printf '%s\n' "$restarting"; } || ok "no pod above 3 restarts"

  # Captured, not piped into `grep -q`: grep exits on the first match, the
  # producer takes SIGPIPE, and `set -o pipefail` reports the pipeline as
  # failed — so the condition reads false even when the match succeeded.
  local succeeded
  succeeded="$(kubectl -n data get job kafka-topic-init -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
  if [ "$succeeded" = "1" ]; then
    ok "kafka-topic-init succeeded"
  else
    fail "kafka-topic-init has not succeeded — auto-create is off, so every publish goes nowhere"
  fi
}

gate_placement() {
  step "gate 2: placement"
  local node pods

  # The sandbox node is the measurement surface: anything sharing it competes
  # with the contestant for the cores it is graded on.
  for node in $(kubectl get nodes -l pool=sandbox -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    pods="$(pods_on_node "$node" | grep -v '/gro-disable-' || true)"
    if [ -n "$pods" ]; then
      printf '%s\n' "$pods" | sed 's/^/      /'
      warn "sandbox node $node carries pods besides gro-disable (expected during a run: algo + capture)"
    else
      ok "sandbox node clear except gro-disable"
    fi
  done

  # Kafka on its own dedicated, tainted node. Nothing tolerated that taint until
  # 2026-08-03, so Kafka silently ran on a general node and competed with
  # Postgres/Timescale/the validator while its own node sat idle.
  for node in $(kubectl get nodes -l pool=kafka -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    pods="$(pods_on_node "$node" || true)"
    if grep -qx 'data/kafka-0' <<<"$pods"; then
      ok "kafka-0 on the kafka node"
    else
      fail "kafka-0 is NOT on the kafka node — it is competing for general-node cores"
    fi
  done

  # gro-disable must cover every sandbox node or cross-node GRO coalescing
  # produces capture gaps that read as contestant failures.
  local sandbox_nodes gro
  sandbox_nodes="$(nodes_in_pool sandbox)"
  gro="$(kubectl -n sandbox get pods -l app=gro-disable --no-headers 2>/dev/null | grep -c . || true)"
  if [ "${gro:-0}" -eq "${sandbox_nodes:-0}" ] && [ "${gro:-0}" -gt 0 ]; then
    ok "gro-disable on all $gro sandbox node(s)"
  else
    fail "gro-disable pods=$gro but sandbox nodes=$sandbox_nodes"
  fi

  local bw workers
  bw="$(nodes_in_pool botworker)"
  workers="$(kubectl -n benchmark get pods -l app=bot-fleet-worker --no-headers 2>/dev/null | grep -c . || true)"
  if [ "${workers:-0}" -le "${bw:-0}" ]; then
    ok "bot-fleet-worker $workers pod(s) on $bw botworker node(s)"
  else
    # Required antiaffinity means a worker per node; more workers than nodes
    # leaves the surplus permanently Pending.
    fail "$workers bot-fleet-worker pods but only $bw botworker nodes — antiaffinity leaves the surplus Pending"
  fi
}

gate_images() {
  step "gate 3: image identity (the one that matters)"
  # Read off the LIVE Deployments, not the manifests. CAPTURE_IMAGE and
  # SPAWNER_IMAGE are env vars, which kustomize's `images:` transformer cannot
  # rewrite — on 2026-08-03 both silently kept their base ghcr.io/*:demo values
  # while every other component ran the pushed tag. The cluster looked perfect
  # and graded nothing.
  local want_tag capture spawner
  want_tag="$(grep -m1 'newTag:' "$ROOT/$OVERLAY/kustomization.yaml" | tr -d ' "' | cut -d: -f2)"
  info "overlay tag: $want_tag"

  capture="$(deploy_env sandbox sandbox-orchestrator CAPTURE_IMAGE)"
  spawner="$(deploy_env build spawner SPAWNER_IMAGE)"

  local pair name val
  for pair in "CAPTURE_IMAGE|$capture|iicpc/ebpf-latency" "SPAWNER_IMAGE|$spawner|iicpc/spawner"; do
    IFS='|' read -r name val repo <<<"$pair"
    case "$val" in
      *ghcr.io*)          fail "$name is still a ghcr.io ref: $val" ;;
      *:demo)             fail "$name points at the mutable :demo tag: $val" ;;
      */"$repo:$want_tag") ok "$name -> $val" ;;
      "")                 fail "$name is not set on the live Deployment" ;;
      *)                  fail "$name = '$val' does not match */$repo:$want_tag" ;;
    esac
  done
}

gate_irsa() {
  step "gate 4: IRSA"
  local arn
  arn="$(sa_annotation build build-spawner 'eks\.amazonaws\.com/role-arn')"
  if [ -n "$arn" ]; then
    ok "build-spawner -> $arn"
  else
    fail "build-spawner has no role-arn annotation — ECR CreateRepository will fall through to IMDS and every build will fail"
  fi
}

gate_config() {
  step "gate 5: config that only exists if the overlay declared it"
  # These three existed ONLY as Go defaults until 2026-08-03 and were injected
  # imperatively by every harness. If they are absent here, something is
  # deploying from a stale tree.
  local v val
  for v in "sandbox|sandbox-orchestrator|SANDBOX_NODE_POOL|sandbox" \
           "benchmark|bot-fleet-controller|READY_DEADLINE|120s" \
           "benchmark|correctness-validator|VALIDATION_TIMEOUT_MS|300000" \
           "platform|submission-api|DEFAULT_CONTESTANT_ID|$SMOKE_CONTESTANT"; do
    IFS='|' read -r ns dep key want <<<"$v"
    val="$(deploy_env "$ns" "$dep" "$key")"
    [ "$val" = "$want" ] && ok "$dep $key=$val" || fail "$dep $key='$val', expected '$want'"
  done
}

main() {
  phase "Verify"
  need kubectl
  require_context "$CLUSTER_NAME"
  gate_health
  gate_placement
  gate_images
  gate_irsa
  gate_config
  finish "Verify"
}

main "$@"

#!/usr/bin/env bash
# B5 — rate-aware sharding + KEDA autoscaling + multi-shard barrier alignment.
#
# One session whose ORDER RATE (not task count) demands more shards than one worker
# can serve. Proves the whole chain:
#
#   * the controller sizes worker_count from total_target_rps, not task count
#     (shard_reason=rate in its log), which the old divisor could not do — 1000 HFT
#     bots at 1000 rps is 1M orders/s and used to shard to ONE worker
#   * KEDA scales bot-fleet-worker up to meet that demand
#   * every shard is consumed EXACTLY ONCE across all pods — the assertion that
#     catches a rebalance re-delivering an uncommitted spec to a second pod, which
#     would double that shard's load and silently corrupt the measurement
#   * all pods receive the SAME barrier epoch, so shards start together
#   * delivered orders match rate x duration, catching both a duplicated shard and a
#     dropped one
#
# Cheap by construction: rather than generating >50k/s (the local loopback wall), it
# LOWERS WORKER_RPS_CAPACITY so a small scenario fans out. Same code path a 200k/s
# scenario takes on EKS, at 6k/s on a laptop.
#
# Usage: TAG=dev1 deploy-local/b5-autoscale-shards.sh [scenario]   (default: spike)
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"
TAG="${TAG:-dev1}"
SCENARIO="${1:-spike}"
RUN_TIMEOUT="${RUN_TIMEOUT:-900}"
# Capacity low enough that the chosen scenario needs >= 3 shards. Deliberately not a
# realistic value — the point is to exercise the fan-out path, not to model a pod.
RPS_CAP="${RPS_CAP:-2000}"
WANT_SHARDS="${WANT_SHARDS:-3}"

IMAGE="$(contestant_image contestant-echo)"

echo "############ B5: rate-aware sharding + autoscaling (scenario=$SCENARIO) ############"

# ── 0. preflight ─────────────────────────────────────────────────────────────
echo "== preflight =="
kubectl get crd scaledobjects.keda.sh >/dev/null 2>&1 || {
  echo "!! KEDA not installed. Install it first:"
  echo "!!   helm repo add kedacore https://kedacore.github.io/charts && helm repo update"
  echo "!!   helm install keda kedacore/keda -n keda --create-namespace --wait"
  exit 1; }
require_image "$IMAGE"

total_rps=$(psql_val "SELECT sum((t->>'target_rps')::bigint) FROM scenarios, jsonb_array_elements(task_specs) t WHERE name='$SCENARIO';")
# Expected ORDERS is sum(rate x that task's OWN duration), not peak_rate x scenario
# duration. Shaped scenarios have cohorts that fire for only part of the window —
# `spike` is 204 tasks at 3600 rps for 60 s PLUS 102 burst tasks at 1800 rps for just
# 10 s from t=25, so peak x duration (5400 x 60 = 324k) overstates the truth (234k) by
# 38% and can never be delivered by any healthy run.
expect_orders=$(psql_val "SELECT sum((t->>'target_rps')::bigint * ((t->>'duration_ns')::bigint/1000000000)) FROM scenarios, jsonb_array_elements(task_specs) t WHERE name='$SCENARIO';")
tasks=$(psql_val "SELECT jsonb_array_length(task_specs) FROM scenarios WHERE name='$SCENARIO';")
SCENARIO_ID=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='$SCENARIO';")
duration_s=$(psql_val "SELECT duration_ns/1000000000 FROM scenarios WHERE name='$SCENARIO';")
[ -n "$SCENARIO_ID" ] || { echo "!! scenario '$SCENARIO' not seeded"; exit 1; }

expect_shards=$(( (total_rps + RPS_CAP - 1) / RPS_CAP ))
echo "   scenario=$SCENARIO tasks=$tasks total_rps=$total_rps duration=${duration_s}s"
echo "   WORKER_RPS_CAPACITY=$RPS_CAP -> expected shards=$expect_shards (want >= $WANT_SHARDS)"
echo "   expected orders=$expect_orders (sum of rate x per-task duration)"
if [ "$expect_shards" -lt "$WANT_SHARDS" ]; then
  echo "!! this scenario would not fan out. Lower RPS_CAP or pick a higher-rate scenario."
  exit 1
fi

# ── 1. configure the rate ceiling + KEDA on the demand gauge ─────────────────
echo "== configuring controller rate ceiling and KEDA =="
kubectl -n benchmark set env deploy/bot-fleet-controller WORKER_RPS_CAPACITY=$RPS_CAP >/dev/null
kubectl -n benchmark rollout status deploy/bot-fleet-controller --timeout=180s >/dev/null

# Scale on the controller's DEMAND gauge, not on Kafka lag. Demand is a declaration
# available before any spec is published; lag only appears after specs land on
# partitions nobody is consuming, which is too late to prevent the under-provisioned
# publish that causes a partial ready fan-in.
kubectl apply -f - >/dev/null <<YAML
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: bot-fleet-worker
  namespace: benchmark
spec:
  scaleTargetRef:
    name: bot-fleet-worker
  minReplicaCount: 1
  maxReplicaCount: 8
  pollingInterval: 5
  cooldownPeriod: 60
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 0
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.observability.svc.cluster.local:9090
        metricName: iicpc_controller_demanded_workers
        query: max(iicpc_controller_demanded_workers)
        threshold: "1"
YAML
echo "   ScaledObject applied (prometheus trigger on iicpc_controller_demanded_workers)"

# ── 2. register the submission and its run rows ──────────────────────────────
STAMP="$(date +%s)"
SUB="b5-$STAMP"; CONTESTANT="b5-echo"
SESSION="$(new_session_id)"; GROUP="$(cat /proc/sys/kernel/random/uuid)"
echo "== registering submission $SUB, session $SESSION =="
psql_exec "INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
 VALUES ('$SUB','$CONTESTANT','b5-$STAMP','rust','FIX',9898,'b5-echo','n/a','$IMAGE','ready');"
psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
 VALUES ('$GROUP','$SUB','$CONTESTANT','running');"
psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
 VALUES ('$SESSION','$SUB','$CONTESTANT','$GROUP','$SCENARIO_ID','requested');"

NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kafka_produce benchmark.requested "$GROUP" \
  "{\"session_id\":\"$SESSION\",\"submission_id\":\"$SUB\",\"contestant_id\":\"$CONTESTANT\",\"run_group_id\":\"$GROUP\",\"scenario_id\":\"$SCENARIO_ID\",\"requested_at\":\"$NOW\"}"

# ── 3. observe the scale-up and the run ──────────────────────────────────────
echo "== running (timeout ${RUN_TIMEOUT}s) =="
PEAK_REPLICAS=0
deadline=$(( $(date +%s) + RUN_TIMEOUT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  reps=$(kubectl -n benchmark get deploy bot-fleet-worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  reps="${reps:-0}"
  if [ "$reps" -gt "$PEAK_REPLICAS" ]; then PEAK_REPLICAS=$reps; fi
  status=$(psql_val "SELECT status FROM runs WHERE session_id='$SESSION';")
  printf "\r   status=%-14s ready_workers=%s peak=%s   " "${status:-none}" "$reps" "$PEAK_REPLICAS"
  case "$status" in completed|failed) break ;; esac
  sleep 5
done
echo

# ── 4. assertions ────────────────────────────────────────────────────────────
echo "== assertions =="
status=$(psql_val "SELECT status FROM runs WHERE session_id='$SESSION';")
[ "$status" = completed ] && ok "session completed" || fail "session status='$status' (want completed)"

# The controller must attribute the shard count to the RATE ceiling. Without this the
# test could pass on a task-count fan-out and prove nothing about rate awareness.
ctl=$(kubectl -n benchmark logs deploy/bot-fleet-controller --tail=3000 2>/dev/null | grep "$SESSION" || true)
got_shards=$(echo "$ctl" | grep -oE '"worker_count":[0-9]+' | head -1 | cut -d: -f2)
reason=$(echo "$ctl" | grep -oE '"shard_reason":"[a-z]+"' | head -1 | cut -d'"' -f4)
logged_rps=$(echo "$ctl" | grep -oE '"total_target_rps":[0-9]+' | head -1 | cut -d: -f2)
echo "   controller: worker_count=${got_shards:-?} shard_reason=${reason:-?} total_target_rps=${logged_rps:-?}"
[ "${got_shards:-0}" -ge "$WANT_SHARDS" ] \
  && ok "sharded into ${got_shards} (>= $WANT_SHARDS)" \
  || fail "worker_count=${got_shards:-none}, want >= $WANT_SHARDS"
[ "$reason" = rate ] \
  && ok "shard count attributed to the RATE ceiling" \
  || fail "shard_reason='${reason:-none}', want 'rate' (a task-count fan-out proves nothing here)"
[ "${logged_rps:-0}" = "$total_rps" ] \
  && ok "controller summed total_target_rps correctly ($logged_rps)" \
  || fail "controller logged total_target_rps=${logged_rps:-none}, scenario has $total_rps"

[ "$PEAK_REPLICAS" -ge "${got_shards:-$WANT_SHARDS}" ] \
  && ok "KEDA scaled workers to $PEAK_REPLICAS (>= ${got_shards} shards)" \
  || fail "peak ready workers = $PEAK_REPLICAS, need >= ${got_shards} to serve every shard"

# Ready fan-in must be complete: the controller only publishes the barrier after all
# worker_count ready signals arrive, so a partial fan-in fails the run outright.
fanin=$(echo "$ctl" | grep -oE '"total_received":[0-9]+' | tail -1 | cut -d: -f2)
[ "${fanin:-0}" = "${got_shards:-0}" ] \
  && ok "ready fan-in complete ($fanin of ${got_shards})" \
  || fail "ready fan-in reached ${fanin:-0} of ${got_shards}"

# THE rebalance-duplicate assertion. Each worker_index must be prepared exactly once
# across ALL pods. A redelivered uncommitted spec shows up here as a count of 2.
echo "   per-shard execution count across all worker pods:"
dupes=0; missing=0
declare -A seen
for p in $(kubectl -n benchmark get pods -l app=bot-fleet-worker -o jsonpath='{.items[*].metadata.name}'); do
  for idx in $(kubectl -n benchmark logs "$p" --tail=5000 2>/dev/null \
      | grep "$SESSION" | grep '"message":"preparing workload"' \
      | grep -oE '"worker_index":[0-9]+' | cut -d: -f2); do
    seen[$idx]=$(( ${seen[$idx]:-0} + 1 ))
  done
done
for i in $(seq 0 $(( ${got_shards:-1} - 1 ))); do
  c=${seen[$i]:-0}
  echo "     worker_index=$i prepared ${c}x"
  [ "$c" -gt 1 ] && dupes=$((dupes+1))
  [ "$c" -eq 0 ] && missing=$((missing+1))
done
[ "$dupes" -eq 0 ] \
  && ok "no shard executed twice (no rebalance duplicate)" \
  || fail "$dupes shard(s) executed more than once — a rebalance re-delivered an uncommitted spec"
[ "$missing" -eq 0 ] \
  && ok "every shard executed at least once" \
  || fail "$missing shard(s) never executed — that share of the load was silently dropped"

# One barrier epoch for the whole session: shards must start together, or the
# scenario's shape is smeared across pods.
#
# FAIL-CLOSED on zero. `distinct <= 1` alone would pass when the log line is absent
# entirely — which it was until `barrier received` was added to worker.rs, making this
# assertion silently vacuous. Require one epoch reported by EVERY shard.
epoch_lines=$(kubectl -n benchmark logs -l app=bot-fleet-worker --tail=5000 2>/dev/null \
  | grep "$SESSION" | grep -c '"message":"barrier received"' || true)
epochs=$(kubectl -n benchmark logs -l app=bot-fleet-worker --tail=5000 2>/dev/null \
  | grep "$SESSION" | grep '"message":"barrier received"' \
  | grep -oE '"barrier_epoch_ns":[0-9]+' | cut -d: -f2 | sort -u | wc -l)
echo "   barrier: ${epoch_lines} shard(s) reported, ${epochs} distinct epoch(s)"
if [ "${epoch_lines:-0}" -ne "${got_shards:-0}" ]; then
  fail "only ${epoch_lines} of ${got_shards} shards reported a barrier epoch"
elif [ "${epochs:-0}" -eq 1 ]; then
  ok "all ${got_shards} shards shared ONE barrier epoch"
else
  fail "$epochs distinct barrier epochs across ${got_shards} shards — shards did not start together"
fi

# Delivered volume. Catches a duplicated shard (over) and a dropped one (under).
sent=$(kubectl -n benchmark logs -l app=bot-fleet-worker --tail=5000 2>/dev/null \
  | grep "$SESSION" | grep '"message":"workload completed"' \
  | grep -oE '"sent":[0-9]+' | cut -d: -f2 | awk '{s+=$1} END{print s+0}')
expect="$expect_orders"
lo=$(( expect * 90 / 100 )); hi=$(( expect * 110 / 100 ))
echo "   delivered=${sent} expected=${expect} (accepting ${lo}..${hi})"
if [ "${sent:-0}" -ge "$lo" ] && [ "${sent:-0}" -le "$hi" ]; then
  ok "delivered volume within 10% of sum(rate x per-task duration)"
else
  fail "delivered ${sent}, expected ~${expect} — suspect a duplicated or dropped shard"
fi

stale=$(kubectl -n benchmark logs -l app=bot-fleet-worker --tail=5000 2>/dev/null \
  | grep -c "skipping stale workload assignment" || true)
[ "${stale:-0}" -eq 0 ] \
  && ok "no stale workload specs skipped" \
  || fail "${stale} stale spec(s) skipped — a shard waited longer than WORKLOAD_SPEC_MAX_AGE_S"

echo
echo "session: $SESSION"
if [ "$ASSERT_FAILURES" -eq 0 ]; then
  echo "############ B5 PASSED ############"
else
  echo "############ B5: $ASSERT_FAILURES assertion(s) FAILED ############"
  exit 1
fi

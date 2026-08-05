#!/usr/bin/env bash
# B4 — the stalled-peer harness. Runs the pass-1 correctness scenario against
# stall-sink: a "contestant" that accepts the connection, drains 256KB, then
# stops reading while keeping the socket open. The bot's send buffer fills and
# every later write hits EAGAIN forever — the wedged-engine failure mode that
# neither the echo (reads, never replies) nor the drain (always reads) can
# produce.
#
# Proves, per docs/remaining-work.md B4:
#   1. drain-deadline write exit — the session reaches a terminal state on
#      schedule instead of hanging on a blocked write;
#   2. watchdog last-tick pending sweep — iicpc_bot_inflight returns to 0;
#   3. accounting closure — orders were offered and every one of them ended
#      in a terminal state (nothing acked: the sink never writes, so
#      offered ≈ errors in the ingester rollup);
#   4. the worker survives (no restarts/OOM).
#
# Usage: TAG=dev1 deploy-local/b4-stalled-peer.sh
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"

TAG="${TAG:-dev1}"
SINK_IMAGE="$(contestant_image stall-sink)"
RUN_TIMEOUT="${RUN_TIMEOUT:-300}"
SETTLE_S="${SETTLE_S:-30}"   # post-run wait for watchdog sweep + telemetry flush
ASSERT_FAILURES=0

echo "############ B4: stalled-peer — wedged-engine invariants ############"

# ── preflight ────────────────────────────────────────────────────────────────
echo "== preflight =="
for d in benchmark/bot-fleet-controller benchmark/bot-fleet-worker sandbox/sandbox-orchestrator; do
  ns=${d%/*}; n=${d#*/}
  ready=$(kubectl -n "$ns" get deploy "$n" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  [ "${ready:-0}" -ge 1 ] || { echo "!! $d not ready — run deploy-local/up-dev.sh"; exit 1; }
done

if [ "$HARNESS_ENV" = eks ]; then
  require_image "$SINK_IMAGE"
else
  docker image inspect "$SINK_IMAGE" >/dev/null 2>&1 || {
    echo ">> building $SINK_IMAGE"
    docker build -q -f deploy-local/stall-sink/Dockerfile -t "$SINK_IMAGE" deploy-local/stall-sink
  }
fi
echo "!! ensure the image is imported into k3s (idempotent, needs root):"
echo "!!   docker save $SINK_IMAGE | sudo k3s ctr images import -"
if [ "${SKIP_IMPORT_WAIT:-0}" != 1 ]; then
  read -r -p ">> press enter once imported (SKIP_IMPORT_WAIT=1 to skip this prompt) " _ || true
fi

SCEN=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='correctness';" || true)
[ -n "$SCEN" ] || { echo "!! 'correctness' scenario not seeded (see b2 preflight for reseed)"; exit 1; }
echo "   correctness scenario=$SCEN"

# ── trigger ──────────────────────────────────────────────────────────────────
STAMP="$(date +%s)"
SUB="b4-stall-$STAMP"; SESS="$(new_session_id)"; GRP="$(cat /proc/sys/kernel/random/uuid)"
psql_exec "INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
 VALUES ('$SUB','b4-stall','b4-stall-$STAMP','rust','FIX',9898,'b4-stall','n/a','$SINK_IMAGE','ready');" >/dev/null
psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
 VALUES ('$GRP','$SUB','b4-stall','running');" >/dev/null
psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
 VALUES ('$SESS','$SUB','b4-stall','$GRP','$SCEN','requested');" >/dev/null
kafka_produce benchmark.requested "$GRP" \
  "{\"session_id\":\"$SESS\",\"submission_id\":\"$SUB\",\"contestant_id\":\"b4-stall\",\"run_group_id\":\"$GRP\",\"scenario_id\":\"$SCEN\",\"requested_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
echo "   stall-sink -> session $SESS"

# ── wait for terminal state (assertion 1 lives here: it must NOT hang) ──────
ctl_status() {
  { kubectl -n benchmark logs deploy/bot-fleet-controller --tail=3000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session transition"' \
      | grep -oE '"status":"[a-z_]+"' | tail -1 | cut -d'"' -f4; } 2>/dev/null || true
}
echo "== running (timeout ${RUN_TIMEOUT}s — exceeding it means the write path wedged) =="
deadline=$(( $(date +%s) + RUN_TIMEOUT )); st=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  st=$(ctl_status "$SESS")
  printf "\r   session=%-14s " "${st:-none}"
  case "$st" in completed|failed) break ;; esac
  sleep 5
done
echo
echo "   settling ${SETTLE_S}s (watchdog sweep + telemetry flush)"
sleep "$SETTLE_S"

# ── assertions ───────────────────────────────────────────────────────────────
# All session-scoped, read from the worker's structured logs. Two sources that
# looked right and are not: prometheus counters are cumulative across sessions
# (and prometheus-client appends _total), and the ingester writes NO metrics
# row for a session with zero responses (aggregate.rs gates the row on a
# non-empty service_time histogram) — so "offered=0 in Timescale" is the
# EXPECTED shape of a full stall, not an accounting failure.
echo "== assertions =="

wlog() { kubectl -n benchmark logs deploy/bot-fleet-worker --tail=6000 2>/dev/null \
           | grep "\"session_id\":\"$SESS\""; }

[ "$st" = completed ] \
  && ok "session reached terminal state 'completed' within ${RUN_TIMEOUT}s (no write-path hang)" \
  || fail "session status='$st' after ${RUN_TIMEOUT}s — write path wedged (B4 claim 1 FAILS)"

restarts=$(kubectl -n benchmark get pods -l app=bot-fleet-worker \
  -o jsonpath='{range .items[*]}{range @.status.containerStatuses[*]}{.restartCount}{"\n"}{end}{end}' \
  | awk '{s+=$1} END {print s+0}')
[ "$restarts" -eq 0 ] \
  && ok "no bot-fleet-worker restarts (no OOM/panic under stall)" \
  || fail "bot-fleet-worker restartCount=$restarts"

sent=$(wlog | grep '"message":"workload completed"' | grep -oE '"sent":[0-9]+' | tail -1 | cut -d: -f2)
[ "${sent:-0}" -gt 0 ] \
  && ok "bots sent $sent orders before the stall took hold" \
  || fail "no 'workload completed' with sent>0 — the run never engaged the sink"

# The stall itself must be visible: send snapshots with the throttle engaged —
# rate pinned to 0 while inflight sits at the per-task cap. This is the
# platform's actual defense (inflight-cap throttle; writes stay non-blocking,
# max_write_block_ms ~0), not a blocking-write deadline.
stalled_snaps=$(wlog | grep '"message":"bot send snapshot"' \
  | grep '"send_rate_per_s":0' | grep -vc '"inflight":0' || true)
[ "${stalled_snaps:-0}" -ge 3 ] \
  && ok "stall observed: $stalled_snaps snapshots with rate=0 while inflight pinned (throttle engaged)" \
  || fail "stall never observed in send snapshots — sink did not wedge the flow"

final_inflight=$(wlog | grep '"message":"bot send snapshot"' \
  | grep -oE '"inflight":[0-9]+' | tail -1 | cut -d: -f2)
[ "${final_inflight:-1}" -eq 0 ] \
  && ok "final snapshot inflight=0 (every pending order swept; accounting closed)" \
  || fail "final inflight=${final_inflight:-?} — pending orders leaked (B4 claims 2/3 FAIL)"

echo
if [ "$ASSERT_FAILURES" -eq 0 ]; then
  echo "############ B4 PASSED ############"
else
  echo "############ B4 FAILED ($ASSERT_FAILURES assertion(s)) ############"
  exit 1
fi

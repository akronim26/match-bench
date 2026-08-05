#!/usr/bin/env bash
# production/06-smoke.sh — one reference contestant, end to end, asserted.
#
# This is the phase that distinguishes "the cluster is up" from "the platform
# works". Everything before it can pass while the platform grades nothing: on
# 2026-08-03 every pod was Running and every rollout complete, and the capture
# was a stale binary publishing no orders.acked at all.
#
# The fixture is a contestant KNOWN to qualify (0.9921 on EKS), so a low score
# means the platform is broken rather than the engine. That is the whole point
# of using a reference rather than a throughput contestant here.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/k8s.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

trap pf_cleanup EXIT

API="http://127.0.0.1:${PORT_SUBMISSION_API}"
TOKEN=""

# Identity comes from the token's `sub` claim when auth is off. It MUST match
# DEFAULT_CONTESTANT_ID on submission-api, or the owner-scoped 404 makes the
# benchmark trigger unreachable — the submission uploads fine and then every
# subsequent call answers "submission not found".
mint_token() {
  local payload
  payload="$(printf '{"sub":"%s"}' "$SMOKE_CONTESTANT" | basenc --base64url | tr -d '=')"
  TOKEN="eyJhbGciOiJub25lIn0.${payload}."
}

api() { curl -sS -H "Authorization: Bearer $TOKEN" "$@"; }

jqf() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('$1',''))" 2>/dev/null; }

psql_platform() {
  # Unlike e2e/lib.sh's psql_exec, this does NOT end in `|| true`: a query that
  # ERRORS must be distinguishable from a query that returns zero rows.
  # Conflating them turns a broken harness into a false assertion failure.
  kubectl -n data exec -i postgres-0 -c postgres -- \
    psql -U iicpc -d iicpc -t -A -v ON_ERROR_STOP=1 -c "$1"
}

main() {
  phase "Smoke"
  need kubectl; need curl; need python3; need basenc
  require_context "$CLUSTER_NAME"
  [ -f "$ROOT/$SMOKE_FIXTURE" ] || die "fixture not found: $SMOKE_FIXTURE"

  mint_token
  port_forward_bg platform submission-api "${PORT_SUBMISSION_API}:80"

  step "upload"
  local resp sid
  resp="$(api -F "file=@$ROOT/$SMOKE_FIXTURE" "$API/submit")" || die "upload failed"
  sid="$(printf '%s' "$resp" | jqf submission_id)"
  [ -n "$sid" ] || die "no submission_id in response: $resp"
  info "submission_id=$sid"
  info "protocol=$(printf '%s' "$resp" | jqf protocol) port=$(printf '%s' "$resp" | jqf port)"
  ok "uploaded"

  step "build (Kaniko -> ECR)"
  local st deadline=$(( SECONDS + 600 ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    st="$(api "$API/submissions/$sid" | jqf status)"
    case "$st" in
      ready)  ok "build reached 'ready'"; break ;;
      failed) die "build FAILED — kubectl -n build logs job/build-$sid... ; check spawner IRSA (phase 04)" ;;
    esac
    sleep 10
  done
  [ "$st" = "ready" ] || die "build did not reach 'ready' within 600s (last status: ${st:-unknown})"

  # The build Job runs with no nodeSelector and no toleration, so it belongs on
  # a general node. If it landed elsewhere the pool taints are wrong.
  local build_node
  build_node="$(kubectl -n build get pods -l job-name --no-headers -o wide 2>/dev/null \
    | grep -m1 "$sid" | awk '{print $7}' || true)"
  [ -n "$build_node" ] && dim "build ran on $build_node"

  step "trigger benchmark"
  local rg
  rg="$(api -X POST "$API/submissions/$sid/benchmark" | jqf run_group_id)"
  [ -n "$rg" ] || die "no run_group_id — if this says 'submission not found', SMOKE_CONTESTANT does not match DEFAULT_CONTESTANT_ID"
  info "run_group_id=$rg"
  ok "benchmark requested"

  step "watch placement (algo + capture must share a node)"
  # The capture Job is pinned to the algo pod's node by explicit NodeName. A
  # split means the capture is in the wrong netns and sees no packets, which
  # presents downstream as orders.acked = 0.
  local algo_node="" cap_node="" i
  for i in $(seq 1 60); do
    algo_node="$(kubectl -n sandbox get pods --no-headers -o wide 2>/dev/null | awk '/^algo-/{print $7; exit}')"
    cap_node="$(kubectl -n sandbox get pods --no-headers -o wide 2>/dev/null | awk '/^capture-/{print $7; exit}')"
    [ -n "$algo_node" ] && [ -n "$cap_node" ] && break
    sleep 5
  done
  if [ -n "$algo_node" ] && [ "$algo_node" = "$cap_node" ]; then
    ok "algo and capture co-located on $algo_node"
  elif [ -n "$algo_node" ]; then
    fail "algo on '$algo_node' but capture on '${cap_node:-<none>}' — capture is in the wrong netns"
  else
    warn "did not observe the algo pod (it may have completed before sampling)"
  fi

  step "orchestrator can reach the contestant"
  # Both contestant ports, from inside the orchestrator's own netns. Until
  # 2026-08-03 nothing permitted this egress and slots simply never became
  # ready — recorded for months as a ProtocolAll bug in slot.go, when in fact
  # every port failed and single-protocol runs only worked because slot.go
  # dials the EXTRA port, so a FIX-only slot never dials at all.
  local algo_pod algo_ip orch dbg
  algo_pod="$(kubectl -n sandbox get pods --no-headers 2>/dev/null | awk '/^algo-/{print $1; exit}')"
  if [ -n "$algo_pod" ]; then
    algo_ip="$(kubectl -n sandbox get pod "$algo_pod" -o jsonpath='{.status.podIP}' 2>/dev/null)"
    orch="$(kubectl -n sandbox get pod -l app=sandbox-orchestrator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    if [ -n "$algo_ip" ] && [ -n "$orch" ]; then
      kubectl -n sandbox debug "$orch" --image=busybox:1.36 --target=sandbox-orchestrator -q --attach=false \
        -- sh -c "nc -w 5 -zv $algo_ip 9898; nc -w 5 -zv $algo_ip 8080" >/dev/null 2>&1 || true
      sleep 12
      dbg="$(kubectl -n sandbox get pod "$orch" -o jsonpath='{range .status.ephemeralContainerStatuses[*]}{.name}{"\n"}{end}' 2>/dev/null | tail -1)"
      local dial
      dial="$(kubectl -n sandbox logs "$orch" -c "$dbg" 2>/dev/null || true)"
      if grep -q 'open' <<<"$dial"; then
        ok "orchestrator reaches the contestant"
      else
        fail "orchestrator cannot reach the contestant — check the egress rule in k8s/sandbox/sandbox-orchestrator/network-policy.yaml"
        printf '%s\n' "$dial" | sed 's/^/      /'
      fi
    fi
  else
    dim "no live algo pod to dial (session already finished)"
  fi

  step "await terminal state"
  local terminal=0
  deadline=$(( SECONDS + 900 ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local pending
    pending="$(psql_platform "select count(*) from runs where run_group_id='$rg' and status not in ('completed','failed','timed_out');" || echo "?")"
    [ "$pending" = "0" ] && { terminal=1; break; }
    sleep 15
  done
  [ "$terminal" -eq 1 ] || fail "sessions did not reach a terminal state within 900s"
  psql_platform "select coalesce(s.name,'?')||' | '||r.status||' | '||left(r.message,50) from runs r left join scenarios s on s.scenario_id=r.scenario_id where r.run_group_id='$rg';" \
    | sed 's/^/      /' || true

  step "assert grading"
  # orders.acked = 0 is the signature of a stale capture image: the session runs,
  # telemetry flows, and nothing is ever matched.
  local acked score
  acked="$(psql_platform "select coalesce(max(acked_count),0) from correctness_summary cs join runs r on r.session_id=cs.session_id where r.run_group_id='$rg';" || echo 0)"
  if [ "${acked:-0}" -gt 0 ]; then
    ok "orders.acked observed ($acked)"
  else
    fail "orders.acked is 0 — the capture published nothing (stale image? wrong netns?)"
  fi

  # Grade on the PASS-1 (`correctness`) session only.
  #
  # Two reasons this is not `max(score)` across the run group. First, pass-2
  # scenarios (constant/ramp/spike) are graded BOOK-FREE in invariants mode and
  # are explicitly non-deciding -- an overloaded pass-2 run scores near zero
  # because every order past the 5s RESPONSE_TIMEOUT counts as lost, which says
  # nothing about engine correctness. Observed here: constant scored 0.09 with
  # 2.54M lost of 6.00M sent while correctness scored 0.9928 on the same engine.
  # Second, `max()` silently grades whichever session happens to have a row.
  #
  # And a run reaching a TERMINAL state does not mean it has been SCORED: the
  # validator settles for SETTLE_DELAY_MS and then replays the whole session, so
  # the summary row arrives well after `runs.status` goes terminal. Querying
  # immediately caught only the earlier-finishing session on the first run.
  step "await pass-1 scoring"
  local scored=0 deadline2=$(( SECONDS + 600 )) n
  while [ "$SECONDS" -lt "$deadline2" ]; do
    n="$(psql_platform "select count(*) from correctness_summary cs
           join runs r on r.session_id=cs.session_id
           left join scenarios s on s.scenario_id=r.scenario_id
          where r.run_group_id='$rg' and s.name='correctness' and cs.status='scored';" || echo 0)"
    [ "${n:-0}" -ge 1 ] && { scored=1; break; }
    sleep 10
  done
  [ "$scored" -eq 1 ] || fail "the correctness session was never scored within 600s of finishing"

  score="$(psql_platform "select coalesce(max(correctness_score),0) from correctness_summary cs
             join runs r on r.session_id=cs.session_id
             left join scenarios s on s.scenario_id=r.scenario_id
            where r.run_group_id='$rg' and s.name='correctness' and cs.status='scored';" || echo 0)"
  info "pass-1 correctness score: $score"
  if awk -v s="$score" -v m="$SMOKE_MIN_SCORE" 'BEGIN{exit !(s+0 >= m+0)}'; then
    ok "score $score >= $SMOKE_MIN_SCORE (reference engine qualifies)"
  else
    fail "score $score < $SMOKE_MIN_SCORE — a KNOWN-GOOD engine failed to qualify, so grading is wrong"
  fi

  # Pass-2 scores are reported for visibility, never asserted on.
  psql_platform "select '      pass-2 '||coalesce(s.name,'?')||': score='||round(cs.correctness_score::numeric,4)
                        ||' sent='||cs.sent_count||' lost='||cs.lost_orders
                   from correctness_summary cs
                   join runs r on r.session_id=cs.session_id
                   left join scenarios s on s.scenario_id=r.scenario_id
                  where r.run_group_id='$rg' and coalesce(s.name,'') <> 'correctness';" 2>/dev/null || true

  # A session that scored 1.0 on zero orders is the fail-open bug, not a pass.
  local vacuous
  vacuous="$(psql_platform "select count(*) from correctness_summary cs join runs r on r.session_id=cs.session_id where r.run_group_id='$rg' and cs.sent_count=0 and cs.correctness_score>0;" || echo 0)"
  [ "${vacuous:-0}" -eq 0 ] && ok "no session scored above 0 on zero orders" \
    || warn "$vacuous session(s) scored >0 with sent_count=0 (known open bug: empty sessions fail open)"

  step "assert telemetry"
  # `runs` lives in the platform Postgres and `metrics` in Timescale, so they
  # cannot be joined in one query — resolve the session ids first, then count
  # rows per session.
  local rows sessions total=0 s
  sessions="$(psql_platform "select session_id from runs where run_group_id='$rg';" || true)"
  for s in $sessions; do
    rows="$(kubectl -n data exec -i timescaledb-0 -c timescaledb -- \
      psql -U iicpc -d metrics -t -A -c "select count(*) from metrics where session_id='$s';" 2>/dev/null || echo 0)"
    total=$(( total + ${rows:-0} ))
  done
  if [ "$total" -gt 0 ]; then
    ok "$total telemetry row(s) — the graphs have data"
  else
    fail "no rows in metrics for this run group — the frontend charts will be empty"
  fi

  finish "Smoke"
}

main "$@"

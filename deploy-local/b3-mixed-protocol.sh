#!/usr/bin/env bash
# B3 — multi-protocol capture, end to end.
#
# The same CORRECT matching engine (dual listener: FIX 9898 + REST/WS 8080) is graded once
# per protocol, then once with all three live simultaneously. The engine is identical in
# every run, so any difference in score or capture loss is the PLATFORM's, not the
# contestant's — which is the only way to tell a framing bug from a bad engine.
#
# Why this gate exists: until now only FIX had ever run in anger. The HTTP/WS framer has
# strictly more state (Content-Length, chunked bodies, WebSocket masking, three length
# encodings, an HTTP->WS transition mid-stream) and had no coverage against arbitrary TCP
# segmentation at all. Property tests now cover the pure framing
# (services/ebpf-latency/src/framing_property.rs); this covers everything they cannot —
# real sockets, real segmentation, the reassembler, the matcher and the validator.
#
# Phase 1 uses `correctness` (1 task, single connection, max rate, full reference-book
# replay) so each protocol gets a real GRADE.
#
# Phase 2 must use a MULTI-TASK scenario. ProtocolAll round-robins tasks across targets
# (`ts.TargetIdx = i % len(targets)` in the controller), so a 1-task scenario would only
# ever exercise target 0 — FIX — and would report a green "ALL" run having never sent a
# single WebSocket frame. `constant` (204 tasks) spreads across all three.
#
# Usage: TAG=dev1 deploy-local/b3-mixed-protocol.sh
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"
TAG="${TAG:-dev1}"
RUN_TIMEOUT="${RUN_TIMEOUT:-600}"
VALIDATE_WAIT="${VALIDATE_WAIT:-420}"
DQ_THRESHOLD="${DQ_THRESHOLD:-0.95}"
GAP_PCT_MAX="${GAP_PCT_MAX:-1.0}"

BOOK_IMAGE="$(contestant_image contestant-matching-engine)"

echo "############ B3: mixed-protocol capture (FIX / REST / WS / ALL) ############"

# ── preflight ────────────────────────────────────────────────────────────────
echo "== preflight =="
for d in benchmark/bot-fleet-controller benchmark/bot-fleet-worker \
         benchmark/correctness-validator sandbox/sandbox-orchestrator; do
  ns=${d%/*}; n=${d#*/}
  ready=$(kubectl -n "$ns" get deploy "$n" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  [ "${ready:-0}" -ge 1 ] || { echo "!! $d not ready — run deploy-local/up-dev.sh"; exit 1; }
done
require_image "$BOOK_IMAGE"

SCEN_C=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='correctness';")
SCEN_K=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='constant';")
[ -n "$SCEN_C" ] || { echo "!! the 'correctness' scenario is not seeded"; exit 1; }
[ -n "$SCEN_K" ] || { echo "!! the 'constant' scenario is not seeded"; exit 1; }
NTASK=$(psql_val "SELECT jsonb_array_length(task_specs) FROM scenarios WHERE name='constant';")
echo "   correctness=$SCEN_C   constant=$SCEN_K (tasks=$NTASK)"
# Fail closed: a single-task phase-2 scenario silently tests FIX three times.
if [ "${NTASK:-0}" -lt 3 ]; then
  echo "!! 'constant' has $NTASK task(s). ProtocolAll round-robins tasks across targets, so"
  echo "!! fewer than 3 tasks cannot exercise FIX + REST + WS. Phase 2 would be vacuous."
  exit 1
fi

ctl_status() {
  { kubectl -n benchmark logs deploy/bot-fleet-controller --tail=4000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session transition"' \
      | grep -oE '"status":"[a-z_]+"' | tail -1 | cut -d'"' -f4; } 2>/dev/null || true
}
val() { # val <session> <field>
  { kubectl -n benchmark logs -l app=correctness-validator --tail=6000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session validated"' \
      | grep -oE "\"$2\":[0-9.]+" | tail -1 | cut -d: -f2; } 2>/dev/null || true
}

# run_one <label> <protocol> <port> <scenario> -> echoes session id
run_one() {
  local label="$1" proto="$2" port="$3" scen="$4"
  local stamp sub sess grp
  stamp="$(date +%s%N)"
  sub="b3-$label-$stamp"; sess="$(new_session_id)"; grp="$(cat /proc/sys/kernel/random/uuid)"
  psql_exec "INSERT INTO submissions
    (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
   VALUES ('$sub','b3-$label','$sub','rust','$proto',$port,'b3-$label','n/a','$BOOK_IMAGE','ready');" >/dev/null
  psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
   VALUES ('$grp','$sub','b3-$label','running');" >/dev/null
  psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
   VALUES ('$sess','$sub','b3-$label','$grp','$scen','requested');" >/dev/null
  kafka_produce benchmark.requested "$grp" \
    "{\"session_id\":\"$sess\",\"submission_id\":\"$sub\",\"contestant_id\":\"b3-$label\",\"run_group_id\":\"$grp\",\"scenario_id\":\"$scen\",\"requested_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
  echo "$sess"
}

wait_for() { # wait_for <session>
  local s="$1" deadline
  deadline=$(( $(date +%s) + RUN_TIMEOUT ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    case "$(ctl_status "$s")" in completed|failed) break ;; esac
    sleep 5
  done
  deadline=$(( $(date +%s) + VALIDATE_WAIT ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    [ -n "$(val "$s" score)" ] && break
    sleep 5
  done
}

# assert_session <label> <session> <require_score>
assert_session() {
  local label="$1" s="$2" require_score="$3"
  local st score sent matched gaps
  st=$(ctl_status "$s")
  [ "$st" = completed ] && ok "$label session completed" \
    || { fail "$label session status='$st' (want completed)"; return; }

  score=$(val "$s" score); sent=$(val "$s" sent)
  matched=$(val "$s" matched); gaps=$(val "$s" capture_gaps)

  # Fail closed on every absent value: an empty read must never look like a pass.
  [ -n "$score" ] && ok "$label was validated (score=$score)" \
    || { fail "$label was never validated — no score logged"; return; }
  if [ -z "$sent" ] || [ "$sent" = 0 ]; then
    fail "$label observed sent=0 — the validator saw no orders, so every number below is vacuous"
    return
  fi
  ok "$label validator observed $sent sent orders"

  # THE multi-protocol assertion. The engine answers every order on every protocol it
  # serves, so a shortfall here is the CAPTURE failing to frame one of them — which is
  # exactly how a WebSocket framing bug presents: an engine that looks like it went silent.
  if [ -n "$matched" ] && [ -n "$gaps" ]; then
    pct=$(awk -v g="$gaps" -v s="$sent" 'BEGIN{printf "%.3f", (s>0? 100*g/s : 100)}')
    awk -v p="$pct" -v m="$GAP_PCT_MAX" 'BEGIN{exit !(p<=m)}' \
      && ok "$label capture_gaps=$gaps of $sent (${pct}%) within ${GAP_PCT_MAX}%" \
      || fail "$label capture_gaps=$gaps of $sent (${pct}%) — the capture is losing responses on this protocol"
    ok "$label matched=$matched"
  else
    fail "$label matched/capture_gaps absent — cannot tell framing loss from a silent engine"
  fi

  if [ "$require_score" = yes ]; then
    awk -v s="$score" -v t="$DQ_THRESHOLD" 'BEGIN{exit !(s>=t)}' \
      && ok "$label QUALIFIES (score $score >= $DQ_THRESHOLD)" \
      || fail "$label scored $score, below $DQ_THRESHOLD — the SAME engine qualifies over FIX, so this is the platform's fault"
  fi
}

# ── phase 1: one protocol at a time, graded ──────────────────────────────────
echo "== phase 1: single-protocol grading (scenario=correctness) =="
declare -A S1
for spec in "fix:FIX:9898" "rest:REST:8080" "ws:WS:8080"; do
  label="${spec%%:*}"; rest="${spec#*:}"; proto="${rest%%:*}"; port="${rest##*:}"
  S1[$label]=$(run_one "$label" "$proto" "$port" "$SCEN_C")
  echo "   $proto -> session ${S1[$label]}"
  # Serialized on purpose: correctness is a single-connection max-rate run, and two at
  # once would share a worker and skew the load each one sees.
  wait_for "${S1[$label]}"
done

echo "== phase 1 assertions =="
for label in fix rest ws; do
  assert_session "$label" "${S1[$label]}" yes
done

# Cross-protocol comparison. Same engine, same scenario — a large spread means the capture
# treats one transport worse than another.
sf=$(val "${S1[fix]}" score); sr=$(val "${S1[rest]}" score); sw=$(val "${S1[ws]}" score)
echo "   scores: FIX=${sf:-none} REST=${sr:-none} WS=${sw:-none}"
if [ -n "$sf" ] && [ -n "$sr" ] && [ -n "$sw" ]; then
  awk -v a="$sf" -v b="$sr" -v c="$sw" 'BEGIN{
    lo=a; hi=a; if(b<lo)lo=b; if(b>hi)hi=b; if(c<lo)lo=c; if(c>hi)hi=c;
    exit !(hi-lo <= 0.05)
  }' && ok "protocol scores agree within 0.05 (same engine, same scenario)" \
     || fail "protocol scores differ by more than 0.05 — the capture handles one transport worse than another"
else
  fail "could not compare protocol scores — at least one session produced none"
fi

# ── phase 2: all three protocols at once ─────────────────────────────────────
echo "== phase 2: ALL three protocols simultaneously (scenario=constant, $NTASK tasks) =="
# Stamp before phase 2 so the connect-drop check below counts only THIS run's failures.
# A plain --tail grep silently summed consecutive B3 runs and reported 136 drops for 68
# actual ones, which is the same class of error as a counter that measures the wrong thing.
PHASE2_TS="$(date -u +%Y-%m-%dT%H:%M:%S)"
SALL=$(run_one "all" "ALL" 9898 "$SCEN_K")
echo "   ALL -> session $SALL"
wait_for "$SALL"

echo "== phase 2 assertions =="
# Graded in invariants mode (constant is a scale scenario), so no correctness threshold —
# the claim under test is that the capture frames all three transports concurrently.
assert_session "all" "$SALL" no

sent=$(val "$SALL" sent); matched=$(val "$SALL" matched); gaps=$(val "$SALL" capture_gaps)
if [ -n "$sent" ] && [ -n "$matched" ] && [ -n "$gaps" ]; then
  # With tasks round-robined across three targets, roughly a third of orders ride each
  # transport. If the WebSocket framer were desyncing, ~2/3 of orders would still match
  # and the shortfall would be far larger than the 1% gap budget — so this is the
  # assertion that actually proves all three transports were captured, not just FIX.
  awk -v s="$sent" -v m="$matched" -v g="$gaps" 'BEGIN{exit !(m+g==s)}' \
    && ok "matched($matched) + capture_gaps($gaps) = sent($sent) — every order accounted for across all three transports" \
    || fail "matched($matched) + capture_gaps($gaps) != sent($sent) — orders vanished on at least one transport"
else
  fail "ALL session did not report sent/matched/capture_gaps"
fi

# A transport that contributes NOTHING makes every assertion above pass vacuously: if all
# WS tasks are dropped at connect, the surviving FIX/REST orders still reconcile exactly.
# That is not hypothetical — the first B3 run passed phase 2 this way, with sent=80,169
# against the 216,206 this same scenario produces when every task connects.
drop_lines() {
  kubectl -n benchmark logs -l app=bot-fleet-worker --tail=20000 2>/dev/null \
    | grep '"message":"task connect failed; dropping"' \
    | awk -v since="$PHASE2_TS" '{
        if (match($0, /"timestamp":"[^"]+"/)) {
          ts = substr($0, RSTART+13, RLENGTH-14)
          if (ts >= since) print
        }
      }'
}
drops=$(drop_lines | wc -l)
if [ "${drops:-0}" -eq 0 ]; then
  ok "no tasks dropped at connect — every declared transport actually carried load"
else
  fail "$drops task(s) were dropped at connect — at least one transport carried NO load, so the reconciliation above is vacuous"
  drop_lines | grep -oE '"error":"[^"]+"' | sort | uniq -c | sed 's/^/     /'
fi

echo
echo "sessions: FIX=${S1[fix]} REST=${S1[rest]} WS=${S1[ws]} ALL=$SALL"
if [ "${ASSERT_FAILURES:-0}" -eq 0 ]; then
  echo "############ B3 PASSED ############"
else
  echo "############ B3: ${ASSERT_FAILURES} assertion(s) FAILED ############"
  exit 1
fi

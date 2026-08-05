#!/usr/bin/env bash
# B2 second half — the pass-2 (invariants) leg of the two-pass flow.
#
# Pass 1 (b2-two-pass.sh) grades a single-connection session against a reference book.
# Pass 2 grades a MULTI-connection scale scenario with no book at all: per-flow FIFO,
# cross-flow processing order against arrival time, lost orders, overfill — plus the
# jitter distribution and the taint flag that says "this result ran on a sample".
#
# Everything here had only unit coverage. In particular the lost-order and capture-gap
# reporting from the streaming source into InvariantsValidator has never executed against
# a real session, and pass 1 has already shown that a class can be implemented, counted,
# stored and still be unreachable in production.
#
# Usage: TAG=dev1 deploy-local/b2-pass2.sh
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"
TAG="${TAG:-dev1}"
SCENARIO="${SCENARIO:-constant}"
RUN_TIMEOUT="${RUN_TIMEOUT:-900}"
VALIDATE_WAIT="${VALIDATE_WAIT:-420}"
BOOK_IMAGE="$(contestant_image contestant-matching-engine)"

echo "############ B2 pass 2: invariants-mode grading of a scale scenario ############"

# ── preflight ────────────────────────────────────────────────────────────────
echo "== preflight =="
for d in benchmark/bot-fleet-controller benchmark/bot-fleet-worker \
         benchmark/correctness-validator sandbox/sandbox-orchestrator; do
  ns=${d%/*}; n=${d#*/}
  ready=$(kubectl -n "$ns" get deploy "$n" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  [ "${ready:-0}" -ge 1 ] || { echo "!! $d not ready — run deploy-local/up-dev.sh"; exit 1; }
done
require_image "$BOOK_IMAGE"

SCEN=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='$SCENARIO';")
[ -n "$SCEN" ] || { echo "!! scenario '$SCENARIO' is not seeded"; exit 1; }
TASKS=$(psql_val "SELECT jsonb_array_length(task_specs) FROM scenarios WHERE name='$SCENARIO';")
DUR=$(psql_val "SELECT duration_ns/1000000000 FROM scenarios WHERE name='$SCENARIO';")
echo "   scenario=$SCENARIO id=$SCEN duration=${DUR}s tasks=$TASKS"
# Pass 2 exists to grade CROSS-flow ordering. One task is one connection is one flow,
# which makes the cross-flow checks and the jitter distribution vacuous.
if [ "${TASKS:-0}" -lt 2 ]; then
  echo "!! $SCENARIO has $TASKS task(s): a single flow cannot exercise cross-flow ordering,"
  echo "!! and every assertion below about jitter/inversions would pass vacuously."
  exit 1
fi

# ── trigger ──────────────────────────────────────────────────────────────────
STAMP="$(date +%s)"
sub="b2p2-$STAMP"; sess="$(new_session_id)"; grp="$(cat /proc/sys/kernel/random/uuid)"
psql_exec "INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
 VALUES ('$sub','b2p2','$sub','rust','FIX',9898,'b2p2','n/a','$BOOK_IMAGE','ready');" >/dev/null
psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
 VALUES ('$grp','$sub','b2p2','running');" >/dev/null
psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
 VALUES ('$sess','$sub','b2p2','$grp','$SCEN','requested');" >/dev/null
kafka_produce benchmark.requested "$grp" \
  "{\"session_id\":\"$sess\",\"submission_id\":\"$sub\",\"contestant_id\":\"b2p2\",\"run_group_id\":\"$grp\",\"scenario_id\":\"$SCEN\",\"requested_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
echo "   session -> $sess"

# ── wait ─────────────────────────────────────────────────────────────────────
ctl_status() {
  { kubectl -n benchmark logs deploy/bot-fleet-controller --tail=4000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session transition"' \
      | grep -oE '"status":"[a-z_]+"' | tail -1 | cut -d'"' -f4; } 2>/dev/null || true
}
echo "== running (timeout ${RUN_TIMEOUT}s) =="
deadline=$(( $(date +%s) + RUN_TIMEOUT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  s=$(ctl_status "$sess")
  printf "\r   status=%-14s " "${s:-none}"
  case "$s" in completed|failed) break ;; esac
  sleep 5
done
echo

val() { # val <field>
  { kubectl -n benchmark logs -l app=correctness-validator --tail=6000 2>/dev/null \
      | grep "\"session_id\":\"$sess\"" | grep '"msg":"session validated"' \
      | grep -oE "\"$1\":(\"[^\"]*\"|[0-9.eE+-]+|true|false)" | tail -1 | cut -d: -f2- | tr -d '"'; } 2>/dev/null || true
}
echo "   waiting up to ${VALIDATE_WAIT}s for validation..."
vdl=$(( $(date +%s) + VALIDATE_WAIT ))
while [ "$(date +%s)" -lt "$vdl" ]; do
  [ -n "$(val score)" ] && break
  sleep 5
done

# ── assertions ───────────────────────────────────────────────────────────────
echo "== assertions =="
mode=$(val mode); score=$(val score); sent=$(val sent); matched=$(val matched)
lost=$(val lost_orders); lostc=$(val lost_cancels); gaps=$(val capture_gaps)
scored=$(val scored_orders); dirty=$(val dirty_orders)
late=$(val t7_reorder_late); anom=$(val t7_anomalies)
tainted=$(val tainted); reason=$(val taint_reason)
jp99=$(val jitter_p99_us); jrate=$(val jitter_inversion_rate)

echo "   mode=$mode score=$score scored_orders=$scored dirty=$dirty"
echo "   sent=$sent matched=$matched lost_orders=$lost lost_cancels=$lostc capture_gaps=$gaps"
echo "   t7_late=$late t7_anomalies=$anom tainted=$tainted"
echo "   jitter p99=${jp99}us inversion_rate=$jrate"

[ "$(ctl_status "$sess")" = completed ] \
  && ok "session completed" || fail "session status='$(ctl_status "$sess")' (want completed)"
[ -n "$score" ] && ok "session was validated" || fail "session was never validated"

# THE assertion this script exists for: a scale scenario must be graded book-free.
[ "$mode" = "invariants" ] \
  && ok "graded in invariants mode (pass 2)" \
  || fail "mode='$mode', want invariants — a scale scenario must not be book-replayed"

# Fail closed: every number below is meaningless if the validator saw nothing.
if [ -z "$sent" ] || [ "$sent" = 0 ]; then
  fail "validator observed sent=0 — its verdict is vacuous"
else
  ok "validator observed $sent sent orders"
fi
if [ -z "$matched" ] || [ "$matched" = 0 ]; then
  fail "validator matched 0 orders"
else
  ok "validator matched $matched orders"
fi

# The order-level denominator must account for every order: graded + lost. Orders whose
# evidence the PLATFORM lost (capture_gaps) are excluded by design and must be reported
# separately, never silently folded into either side.
if [ -n "$scored" ] && [ -n "$sent" ]; then
  accounted=$(( scored + gaps ))
  awk -v a="$accounted" -v s="$sent" 'BEGIN{exit !(a<=s)}' \
    && ok "scored_orders($scored) + capture_gaps($gaps) = $accounted <= sent($sent)" \
    || fail "accounted $accounted exceeds sent $sent — orders are being double-counted"
fi

# Pass 2 is the mode that owns lost orders. A reference engine answers everything, so
# lost should be ~0 here; what is being proven is that the counter is WIRED, which pass 1
# showed cannot be assumed. A non-trivial rate means the engine really did drop orders.
if [ -n "$lost" ] && [ -n "$sent" ]; then
  awk -v l="$lost" -v s="$sent" 'BEGIN{exit !(l/s < 0.02)}' \
    && ok "lost_orders=$lost is under 2% of sent (reference engine answers its orders)" \
    || fail "lost_orders=$lost of $sent — either the engine dropped orders or the lost path is misfiring"
fi

# Capture health. Above 1% the validator taints the result; this asserts the CAPTURE is
# healthy, independently of whether the taint logic fired.
if [ -n "$gaps" ] && [ -n "$sent" ]; then
  awk -v g="$gaps" -v s="$sent" 'BEGIN{exit !(g/s < 0.01)}' \
    && ok "capture_gaps=$gaps is under 1% of sent" \
    || fail "capture_gaps=$gaps of $sent — the capture is losing responses again (check iicpc_ebpf_truncated_captures)"
fi

# Jitter is the P-G deliverable and only exists in this mode. A multi-flow scale run at
# rate WILL produce cross-flow inversions; all-zero means the histogram is not wired.
if [ -n "$jrate" ]; then
  awk -v r="$jrate" 'BEGIN{exit !(r>0)}' \
    && ok "cross-flow jitter recorded (inversion_rate=$jrate, p99=${jp99}us)" \
    || fail "inversion_rate=0 across $matched multi-flow orders — the jitter histogram is not being fed"
fi

# A healthy run must NOT be tainted, or the flag means nothing when it does fire.
if [ "$tainted" = "true" ]; then
  fail "result tainted on a healthy run: $reason"
else
  ok "result not tainted (t7_late=$late t7_anomalies=$anom)"
fi

echo
echo "session: $sess"
if [ "$ASSERT_FAILURES" -eq 0 ]; then
  echo "############ B2 pass 2 PASSED ############"
else
  echo "############ B2 pass 2: $ASSERT_FAILURES assertion(s) FAILED ############"
  exit 1
fi

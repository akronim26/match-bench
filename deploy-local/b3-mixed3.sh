#!/usr/bin/env bash
# B3 (minimal) — all three protocols live on ONE contestant, one task each.
#
# The full B3 phase 2 uses the 204-task `constant` scenario, where every REST and WS task
# times out at connect (68 + 68, while all 68 FIX tasks succeed) — so its reconciliation
# passes vacuously over FIX-only traffic and proves nothing about mixed protocols. That
# connect storm is a separate, unresolved issue.
#
# This runs the smallest workload that answers the actual question: three tasks, which the
# controller round-robins as `TargetIdx = i % len(targets)` — exactly one task on FIX, one
# on REST, one on WS, against a single `ProtocolAll` submission. No connect storm, no
# ambiguity about which transports carried load.
#
# Rate is CAPPED rather than max (`target_rps: 0`). Three max-rate tasks would push ~3x a
# phase-1 session and re-trip the capture's ring buffer, which would confound "do all three
# transports work together" with "does the capture keep up". 15k/s x 3 keeps total load in
# the same envelope as one max-rate single-protocol run (~45k/s).
#
# Usage: TAG=dev1 deploy-local/b3-mixed3.sh
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"
TAG="${TAG:-dev1}"
RUN_TIMEOUT="${RUN_TIMEOUT:-600}"
VALIDATE_WAIT="${VALIDATE_WAIT:-420}"
RPS="${RPS:-15000}"
BOOK_IMAGE="$(contestant_image contestant-matching-engine)"

echo "############ B3 (minimal): FIX + REST + WS, one task each ############"

require_image "$BOOK_IMAGE"

# ── seed the 3-task scenario (idempotent) ────────────────────────────────────
# `|| true` is required, not defensive noise: psql_val pipes through `grep -vE '^Defaulted'`,
# which exits 1 on an empty result, and under `set -euo pipefail` that aborts the script.
# This query is EXPECTED to return nothing on the first run.
SCEN=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='mixed3';" || true)
if [ -z "$SCEN" ]; then
  SCEN="$(cat /proc/sys/kernel/random/uuid)"
  specs='['
  for i in 0 1 2; do
    [ "$i" -gt 0 ] && specs="$specs,"
    specs="$specs{\"profile\":\"hft\",\"task_id\":$i,\"cancel_pct\":30,\"market_pct\":10,\"target_idx\":0,\"target_rps\":$RPS,\"duration_ns\":45000000000,\"replace_pct\":10,\"smp_id_count\":8,\"start_offset_ns\":0}"
  done
  specs="$specs]"
  psql_exec "INSERT INTO scenarios (scenario_id, name, duration_ns, task_specs, sort_order)
             VALUES ('$SCEN','mixed3',45000000000,'$specs'::jsonb,99);" >/dev/null
  echo "   seeded scenario mixed3=$SCEN (3 tasks @ ${RPS}/s each)"
else
  echo "   scenario mixed3=$SCEN (already seeded)"
fi

NT=$(psql_val "SELECT jsonb_array_length(task_specs) FROM scenarios WHERE scenario_id='$SCEN';" || true)
[ "${NT:-0}" -eq 3 ] || { echo "!! mixed3 has $NT tasks, need exactly 3 (one per protocol)"; exit 1; }

# ── run one ProtocolAll session ──────────────────────────────────────────────
STAMP="$(date +%s)"
sub="b3m-$STAMP"; sess="$(new_session_id)"; grp="$(cat /proc/sys/kernel/random/uuid)"
psql_exec "INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
 VALUES ('$sub','b3m','$sub','rust','ALL',9898,'b3m','n/a','$BOOK_IMAGE','ready');" >/dev/null
psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
 VALUES ('$grp','$sub','b3m','running');" >/dev/null
psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
 VALUES ('$sess','$sub','b3m','$grp','$SCEN','requested');" >/dev/null
START_TS="$(date -u +%Y-%m-%dT%H:%M:%S)"
kafka_produce benchmark.requested "$grp" \
  "{\"session_id\":\"$sess\",\"submission_id\":\"$sub\",\"contestant_id\":\"b3m\",\"run_group_id\":\"$grp\",\"scenario_id\":\"$SCEN\",\"requested_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
echo "   session -> $sess"

ctl_status() {
  { kubectl -n benchmark logs deploy/bot-fleet-controller --tail=4000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session transition"' \
      | grep -oE '"status":"[a-z_]+"' | tail -1 | cut -d'"' -f4; } 2>/dev/null || true
}
val() {
  { kubectl -n benchmark logs -l app=correctness-validator --tail=6000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session validated"' \
      | grep -oE "\"$2\":[0-9.a-z]+" | tail -1 | cut -d: -f2; } 2>/dev/null || true
}

echo "== running =="
dl=$(( $(date +%s) + RUN_TIMEOUT ))
while [ "$(date +%s)" -lt "$dl" ]; do
  s=$(ctl_status "$sess"); printf "\r   status=%-14s" "${s:-none}"
  case "$s" in completed|failed) break ;; esac
  sleep 5
done
echo
dl=$(( $(date +%s) + VALIDATE_WAIT ))
while [ "$(date +%s)" -lt "$dl" ]; do [ -n "$(val "$sess" score)" ] && break; sleep 5; done

echo "== assertions =="
st=$(ctl_status "$sess")
[ "$st" = completed ] && ok "session completed" || fail "session status='$st' (want completed)"

score=$(val "$sess" score); sent=$(val "$sess" sent)
matched=$(val "$sess" matched); gaps=$(val "$sess" capture_gaps)
echo "   score=${score:-none} sent=${sent:-?} matched=${matched:-?} capture_gaps=${gaps:-?}"

[ -n "$score" ] && ok "session was validated" || fail "session was never validated"
if [ -z "$sent" ] || [ "$sent" = 0 ]; then
  fail "sent=0 — the validator saw no orders, so every number here is vacuous"
else
  ok "validator observed $sent sent orders"
fi

# No task may be dropped: with exactly one task per protocol, a single drop means a whole
# transport carried nothing and any reconciliation below is meaningless.
drop_lines() {
  kubectl -n benchmark logs -l app=bot-fleet-worker --tail=20000 2>/dev/null \
    | grep '"message":"task connect failed; dropping"' \
    | awk -v since="$START_TS" '{ if (match($0,/"timestamp":"[^"]+"/)) { ts=substr($0,RSTART+13,RLENGTH-14); if (ts>=since) print } }'
}
drops=$(drop_lines | wc -l)
[ "${drops:-0}" -eq 0 ] \
  && ok "no tasks dropped at connect — FIX, REST and WS all carried load" \
  || { fail "$drops of 3 task(s) dropped at connect — at least one transport carried nothing"; \
       # Through drop_lines, NOT a fresh grep: the unfiltered version printed 69/69 for a
       # run that dropped exactly 1 REST and 1 WS, because it summed every prior run's
       # failures. A breakdown that disagrees with the count above is worse than none.
       drop_lines | grep -oE '"error":"[^"]+"' | sort | uniq -c | sed 's/^/     /'; }

if [ -n "$sent" ] && [ -n "$matched" ] && [ -n "$gaps" ]; then
  awk -v s="$sent" -v m="$matched" -v g="$gaps" 'BEGIN{exit !(m+g==s)}' \
    && ok "matched($matched) + capture_gaps($gaps) = sent($sent) — every order accounted for across all three transports" \
    || fail "matched($matched) + capture_gaps($gaps) != sent($sent)"
  pct=$(awk -v g="$gaps" -v s="$sent" 'BEGIN{printf "%.3f", (s>0? 100*g/s : 100)}')
  awk -v p="$pct" 'BEGIN{exit !(p<=1.0)}' \
    && ok "capture_gaps ${pct}% within 1.0%" \
    || fail "capture_gaps ${pct}% — the capture lost responses with all three transports live"
fi

echo
echo "session: $sess   scenario: mixed3 ($SCEN)"
if [ "${ASSERT_FAILURES:-0}" -eq 0 ]; then
  echo "############ B3 (minimal) PASSED ############"
else
  echo "############ B3 (minimal): ${ASSERT_FAILURES} assertion(s) FAILED ############"
  exit 1
fi

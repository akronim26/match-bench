#!/usr/bin/env bash
# B2 / SMP plan phase 5 — the two-pass flow, end to end.
#
# Runs the SAME two contestants through the pass-1 `correctness` scenario (single
# connection, max rate, full reference-book replay, self-match prevention graded) and
# asserts the thing that has never been true on this platform:
#
#     a CORRECT matching engine qualifies, and an engine that only ACKs does not.
#
# Before this work pass 1 was inverted and had never been run. Its single task gave
# every order one participant identity, so the reference book flagged every fill as a
# self-trade: a correct engine scored 0, while an engine that refused to trade produced
# no fills, hit the ScoredFills==0 guard, and scored 1.0. Both defects are fixed —
# rotating SMP ids (FIX tag 7928 / JSON smp_id) restore cross-participant matching on
# one connection, and the score is now order-level so lost orders count against it.
#
# Usage: TAG=dev1 deploy-local/b2-two-pass.sh
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"
TAG="${TAG:-dev1}"
RUN_TIMEOUT="${RUN_TIMEOUT:-600}"
VALIDATE_WAIT="${VALIDATE_WAIT:-300}"

BOOK_IMAGE="$(contestant_image contestant-matching-engine)"
ECHO_IMAGE="$(contestant_image contestant-echo)"

echo "############ B2: two-pass flow — pass 1 correctness gate ############"

# ── preflight ────────────────────────────────────────────────────────────────
echo "== preflight =="
for d in benchmark/bot-fleet-controller benchmark/bot-fleet-worker \
         benchmark/correctness-validator sandbox/sandbox-orchestrator; do
  ns=${d%/*}; n=${d#*/}
  ready=$(kubectl -n "$ns" get deploy "$n" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  [ "${ready:-0}" -ge 1 ] || { echo "!! $d not ready — run deploy-local/up-dev.sh"; exit 1; }
done
if [ "$HARNESS_ENV" = eks ]; then
  # Contestant images are BUILD PRODUCTS on EKS (zip -> Kaniko -> ECR);
  # preflight checks the zips, not pre-pushed images.
  for z in reference-clob-all.zip smoke-rest-echo.zip; do
    [ -f "$REPO_ROOT/deploy-local/$z" ] || { echo "!! missing $REPO_ROOT/deploy-local/$z"; exit 1; }
  done
else
  for img in "$BOOK_IMAGE" "$ECHO_IMAGE"; do
    require_image "$img"
  done
fi
eks_sandbox_preflight

SCEN=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='correctness';")
[ -n "$SCEN" ] || {
  echo "!! the 'correctness' scenario is not seeded."
  echo "!! up-dev.sh sets SEED_SCENARIOS=correctness,constant,spike,ramp — reseed with:"
  echo "!!   kubectl -n platform set env deploy/submission-api SEED_SCENARIOS=correctness,constant,spike,ramp RESEED_SCENARIOS=true"
  exit 1; }
SMP=$(psql_val "SELECT coalesce(task_specs->0->>'smp_id_count','0') FROM scenarios WHERE name='correctness';")
DUR=$(psql_val "SELECT duration_ns/1000000000 FROM scenarios WHERE name='correctness';")
echo "   correctness scenario=$SCEN duration=${DUR}s smp_id_count=$SMP"
if [ "${SMP:-0}" -lt 2 ]; then
  echo "!! smp_id_count=$SMP — with fewer than 2 ids every order shares one identity and"
  echo "!! the whole book self-crosses, which is the defect this test exists to prove fixed."
  echo "!! The submission-api image is probably older than the SMP seeding change."
  exit 1
fi

# ── trigger both contestants through pass 1 ──────────────────────────────────
STAMP="$(date +%s)"
declare -A SESSION IMAGE_OF SUB_OF CONTESTANT_OF
IMAGE_OF[book]="$BOOK_IMAGE"
IMAGE_OF[echo]="$ECHO_IMAGE"

# EKS: contestants go through the REAL submission path (zip -> build-worker ->
# Kaniko -> ECR -> ready); the direct-insert bypass below stays local-only.
if [ "$HARNESS_ENV" = eks ]; then
  SUB_OF[book]="$(eks_submission "$REPO_ROOT/deploy-local/reference-clob-all.zip")"
  SUB_OF[echo]="$(eks_submission "$REPO_ROOT/deploy-local/smoke-rest-echo.zip")"
fi

for who in book echo; do
  sess="$(new_session_id)"; grp="$(cat /proc/sys/kernel/random/uuid)"
  SESSION[$who]="$sess"
  if [ "$HARNESS_ENV" = eks ]; then
    sub="${SUB_OF[$who]}"
    CONTESTANT_OF[$who]="$(psql_val "SELECT contestant_id FROM submissions WHERE submission_id='$sub';")"
  else
    sub="b2-$who-$STAMP"
    CONTESTANT_OF[$who]="b2-$who"
    psql_exec "INSERT INTO submissions
      (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
     VALUES ('$sub','b2-$who','b2-$who-$STAMP','rust','FIX',9898,'b2-$who','n/a','${IMAGE_OF[$who]}','ready');" >/dev/null
  fi
  psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
   VALUES ('$grp','$sub','${CONTESTANT_OF[$who]}','running');" >/dev/null
  psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
   VALUES ('$sess','$sub','${CONTESTANT_OF[$who]}','$grp','$SCEN','requested');" >/dev/null
  kafka_produce benchmark.requested "$grp" \
    "{\"session_id\":\"$sess\",\"submission_id\":\"$sub\",\"contestant_id\":\"${CONTESTANT_OF[$who]}\",\"run_group_id\":\"$grp\",\"scenario_id\":\"$SCEN\",\"requested_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
  echo "   $who -> session ${SESSION[$who]}"
  # Serialize: pass 1 is a single-task, single-connection run by design, and running
  # both at once would put two sessions on one worker and skew the max-rate load.
  sleep 2
done

# ── wait for both runs, then for validation ──────────────────────────────────
echo "== running (timeout ${RUN_TIMEOUT}s) =="
ctl_status() {
  { kubectl -n benchmark logs deploy/bot-fleet-controller --tail=3000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session transition"' \
      | grep -oE '"status":"[a-z_]+"' | tail -1 | cut -d'"' -f4; } 2>/dev/null || true
}
deadline=$(( $(date +%s) + RUN_TIMEOUT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  b=$(ctl_status "${SESSION[book]}"); e=$(ctl_status "${SESSION[echo]}")
  printf "\r   book=%-14s echo=%-14s " "${b:-none}" "${e:-none}"
  case "$b" in completed|failed) case "$e" in completed|failed) break ;; esac ;; esac
  sleep 5
done
echo

# Validation is asynchronous (settle delay + drain) and sessions are validated one at a
# time, so poll rather than read once.
val() { # val <session> <field>
  { kubectl -n benchmark logs -l app=correctness-validator --tail=5000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" | grep '"msg":"session validated"' \
      | grep -oE "\"$2\":[0-9.]+" | tail -1 | cut -d: -f2; } 2>/dev/null || true
}
echo "   waiting up to ${VALIDATE_WAIT}s for both sessions to be validated..."
vdl=$(( $(date +%s) + VALIDATE_WAIT ))
while [ "$(date +%s)" -lt "$vdl" ]; do
  if [ -n "$(val "${SESSION[book]}" score)" ] && [ -n "$(val "${SESSION[echo]}" score)" ]; then break; fi
  sleep 5
done

# ── assertions ───────────────────────────────────────────────────────────────
echo "== assertions =="
DQ_THRESHOLD="${DQ_THRESHOLD:-0.95}"

for who in book echo; do
  s="${SESSION[$who]}"
  [ "$(ctl_status "$s")" = completed ] \
    && ok "$who session completed" \
    || fail "$who session status='$(ctl_status "$s")' (want completed)"
done

book_score=$(val "${SESSION[book]}" score)
echo_score=$(val "${SESSION[echo]}" score)
book_fills=$(val "${SESSION[book]}" total_fills)
echo_fills=$(val "${SESSION[echo]}" total_fills)
book_sent=$(val "${SESSION[book]}" sent)
echo_sent=$(val "${SESSION[echo]}" sent)
book_viol=$(val "${SESSION[book]}" violations)
echo_viol=$(val "${SESSION[echo]}" violations)

echo "   book: score=${book_score:-none} fills=${book_fills:-?} sent=${book_sent:-?} violations=${book_viol:-?}"
echo "   echo: score=${echo_score:-none} fills=${echo_fills:-?} sent=${echo_sent:-?} violations=${echo_viol:-?}"

[ -n "$book_score" ] && ok "book was validated" || fail "book was never validated"
[ -n "$echo_score" ] && ok "echo was validated" || fail "echo was never validated"

# Fail closed: sent=0 means the validator saw no orders at all, in which case every
# score below is vacuous. This is exactly how the orders.sent decode bug hid.
for who in book echo; do
  v=$(val "${SESSION[$who]}" sent)
  if [ -z "$v" ] || [ "$v" = 0 ]; then
    fail "$who observed sent=0 — the validator saw no orders, so its score is meaningless"
  else
    ok "$who validator observed $v sent orders"
  fi
done

# THE headline assertion. A correct price-time-priority book, running the same scenario
# as an acker, must be distinguishable from it.
if [ -n "$book_score" ] && [ -n "$echo_score" ]; then
  awk -v b="$book_score" -v e="$echo_score" 'BEGIN{exit !(b>e)}' \
    && ok "book ($book_score) outscores echo ($echo_score)" \
    || fail "book ($book_score) did NOT outscore echo ($echo_score) — pass 1 cannot tell a correct engine from an acker"
fi

# Qualification gate, per scoring_config.correctness_dq_threshold.
if [ -n "$book_score" ]; then
  awk -v s="$book_score" -v t="$DQ_THRESHOLD" 'BEGIN{exit !(s>=t)}' \
    && ok "book QUALIFIES (score $book_score >= $DQ_THRESHOLD)" \
    || fail "book score $book_score is below the $DQ_THRESHOLD qualification threshold"
fi
if [ -n "$echo_score" ]; then
  awk -v s="$echo_score" -v t="$DQ_THRESHOLD" 'BEGIN{exit !(s<t)}' \
    && ok "echo DISQUALIFIED (score $echo_score < $DQ_THRESHOLD)" \
    || fail "echo scored $echo_score, at or above the threshold — an acker must not qualify"
fi

# The reference book applies skip-and-continue, so a contestant that implements SMP
# correctly must never produce a self-trade.
#
# Fail CLOSED on an absent value. This read an unlogged field for its whole life, so
# book_self was always empty and the assertion silently did nothing — the run reported
# 11/11 while never checking self-trades at all, and the stored column meanwhile sat at
# exactly 100 (the violation-example cap) with nobody looking.
book_self=$(val "${SESSION[book]}" self_trades)
if [ -z "$book_self" ]; then
  fail "self_trades was not reported — cannot assert the SMP rule, and an unasserted rule is an unenforced one"
elif [ "$book_self" = 0 ]; then
  ok "book produced no self-trades (SMP honoured on both sides)"
else
  fail "book produced $book_self self-trades — contestant and reference book disagree on SMP"
fi

# Attribute what a correct engine still loses. These are exact counters now, not counts
# of capped examples.
for f in price_violations time_violations cancel_replace_loss missed_fills overfills capture_gaps; do
  printf "   book %s=%s\n" "$f" "$(val "${SESSION[book]}" "$f")"
done

# A correct engine must actually TRADE. Zero fills would mean SMP is over-applied and
# the book is self-crossing everything — the pre-fix failure mode wearing a new mask.
if [ -n "$book_fills" ]; then
  [ "$book_fills" -gt 0 ] \
    && ok "book produced $book_fills fills (cross-participant matching works on one connection)" \
    || fail "book produced 0 fills — SMP is rejecting every match, which is the inverted pass-1 defect"
fi

echo
echo "sessions: book=${SESSION[book]} echo=${SESSION[echo]}"
if [ "$ASSERT_FAILURES" -eq 0 ]; then
  echo "############ B2 PASSED ############"
else
  echo "############ B2: $ASSERT_FAILURES assertion(s) FAILED ############"
  exit 1
fi

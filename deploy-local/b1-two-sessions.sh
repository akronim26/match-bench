#!/usr/bin/env bash
# B1 — two concurrent sessions, the highest-value item on the local verification list.
#
# Two DIFFERENT contestants are benchmarked at the same time against the same
# scenario, and the run has to keep them apart end to end:
#   * the controller admits both (parallel dispatch, no serialization)
#   * each holds its OWN exclusive workload.assignments partition lease and its OWN
#     exclusive orders.sent/acked band lease — the two never collide
#   * the orchestrator stands up two sandbox slots, each with its own capture pod
#   * the validator scores each session against only its own band
#   * the leaderboard/score rows come out per-session and DIFFERENT (a correct book
#     qualifies, an acker does not) — proving no cross-contamination
#
# Rates are deliberately low (see up-dev.sh): every one of those properties is
# structural, not load-dependent, and low rates keep the combined task count of BOTH
# sessions under MAX_TASKS_PER_WORKER, past which a shard silently goes unassigned.
#
# Usage:  TAG=dev1 deploy-local/b1-two-sessions.sh [scenario]     (default: constant)
set -euo pipefail
. "$(dirname "$0")/lib-local.sh"
cd "$REPO_ROOT"
TAG="${TAG:-dev1}"
SCENARIO="${1:-constant}"
RUN_TIMEOUT="${RUN_TIMEOUT:-600}"

A_IMAGE="$(contestant_image contestant-matching-engine)"
B_IMAGE="$(contestant_image contestant-echo)"

echo "############ B1: two concurrent sessions (scenario=$SCENARIO) ############"

# ── 0. preflight ─────────────────────────────────────────────────────────────
echo "== preflight =="
for d in benchmark/bot-fleet-controller benchmark/bot-fleet-worker \
         benchmark/correctness-validator benchmark/telemetry-ingester \
         benchmark/score-computer sandbox/sandbox-orchestrator; do
  ns=${d%/*}; n=${d#*/}
  ready=$(kubectl -n "$ns" get deploy "$n" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  [ "${ready:-0}" -ge 1 ] || { echo "!! $d not ready — run deploy-local/up-dev.sh first"; exit 1; }
done
# The contestant images have to be in k3s's *containerd*, not just in docker — but
# reading containerd needs root, and gating the run on a `sudo -n` would fail closed
# on any box without passwordless sudo even when the images are present. So check
# what can be checked unprivileged (docker) and let the pod's own ErrImagePull report
# a missed import; the run loop below surfaces it as slots never reaching Running.
for img in "$A_IMAGE" "$B_IMAGE"; do
  require_image "$img"
done
echo "   (images built; if a slot never leaves ContainerCreating, the k3s import was missed:"
echo "    TAG=$TAG sudo -E deploy-local/import-dev-images.sh)"
tasks=$(psql_val "SELECT jsonb_array_length(task_specs) FROM scenarios WHERE name='$SCENARIO';")
[ -n "$tasks" ] || { echo "!! scenario '$SCENARIO' not seeded"; exit 1; }
# benchmark.requested carries scenario_id, and the controller looks the scenario up
# by that id — NOT by name. submission-api seeds scenario_id as a UUID with the
# human name in a separate column, so publishing the bare name yields "scenario not
# found" and an instant failed run. (e2e/run.sh passes the name because the EKS
# suite seeds rows whose scenario_id IS the name.)
SCENARIO_ID=$(psql_val "SELECT scenario_id FROM scenarios WHERE name='$SCENARIO';")
[ -n "$SCENARIO_ID" ] || { echo "!! no scenario_id for '$SCENARIO'"; exit 1; }
maxtasks=$(kubectl -n benchmark get deploy bot-fleet-controller \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MAX_TASKS_PER_WORKER")].value}' 2>/dev/null)
maxtasks="${maxtasks:-1000}"
workers=$(kubectl -n benchmark get deploy bot-fleet-worker -o jsonpath='{.status.readyReplicas}')
echo "   scenario=$SCENARIO tasks/session=$tasks  x2 sessions = $((tasks*2))"
echo "   workers=$workers  MAX_TASKS_PER_WORKER=$maxtasks  capacity=$((workers*maxtasks))"
if [ $((tasks*2)) -gt $((workers*maxtasks)) ]; then
  echo "!! BOTH sessions together exceed worker capacity — a shard would go unassigned"
  echo "!! and the run would silently deliver a fraction of its load. Lower CONSTANT_RPS"
  echo "!! (up-dev.sh) or scale bot-fleet-worker out."
  exit 1
fi

# ── 1. register two ready submissions ────────────────────────────────────────
# Terminal flow (status='ready' + image_ref), not the zip/Kaniko path: B1 is about
# controller/lease/slot behaviour, and an in-cluster build would only add a second
# failure source. sha256 is UNIQUE in the submissions table, hence the distinct values.
STAMP="$(date +%s)"
A_SUB="b1a-$STAMP"; A_CONTESTANT="b1-book"
B_SUB="b1b-$STAMP"; B_CONTESTANT="b1-echo"
echo "== registering submissions =="
psql_exec "INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
 VALUES
  ('$A_SUB','$A_CONTESTANT','b1a-$STAMP','rust','FIX',9898,'b1-matching-engine','n/a','$A_IMAGE','ready'),
  ('$B_SUB','$B_CONTESTANT','b1b-$STAMP','rust','FIX',9898,'b1-echo','n/a','$B_IMAGE','ready');"

# ── 2. fire both benchmark.requested as close together as possible ───────────
A_SESSION="$(new_session_id)"; A_GROUP="$(cat /proc/sys/kernel/random/uuid)"
B_SESSION="$(new_session_id)"; B_GROUP="$(cat /proc/sys/kernel/random/uuid)"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "== triggering both sessions =="
echo "   A book: session=$A_SESSION group=$A_GROUP"
echo "   B echo: session=$B_SESSION group=$B_GROUP"

# Insert the run_groups + runs rows that submission-api's HTTP StartBenchmark would
# have created. Producing benchmark.requested straight to Kafka bypasses that handler,
# and NOTHING else ever INSERTs these rows — the controller and submission-api only
# ever UPDATE them. Without them, score-computer rejects every correctness event with
# "lookup run <session>: no rows in result set" and no session is ever scored, even
# though the validator produced correct verdicts.
psql_exec "INSERT INTO run_groups (run_group_id, submission_id, contestant_id, status)
 VALUES ('$A_GROUP','$A_SUB','$A_CONTESTANT','running'),
        ('$B_GROUP','$B_SUB','$B_CONTESTANT','running');"
psql_exec "INSERT INTO runs (session_id, submission_id, contestant_id, run_group_id, scenario_id, status)
 VALUES ('$A_SESSION','$A_SUB','$A_CONTESTANT','$A_GROUP','$SCENARIO_ID','requested'),
        ('$B_SESSION','$B_SUB','$B_CONTESTANT','$B_GROUP','$SCENARIO_ID','requested');"

# Separate run_groups: these are two independent contestants clicking "benchmark",
# which is the case the partition/band lease allocators exist for.
kafka_produce benchmark.requested "$A_GROUP" \
  "{\"session_id\":\"$A_SESSION\",\"submission_id\":\"$A_SUB\",\"contestant_id\":\"$A_CONTESTANT\",\"run_group_id\":\"$A_GROUP\",\"scenario_id\":\"$SCENARIO_ID\",\"requested_at\":\"$NOW\"}"
kafka_produce benchmark.requested "$B_GROUP" \
  "{\"session_id\":\"$B_SESSION\",\"submission_id\":\"$B_SUB\",\"contestant_id\":\"$B_CONTESTANT\",\"run_group_id\":\"$B_GROUP\",\"scenario_id\":\"$SCENARIO_ID\",\"requested_at\":\"$NOW\"}"

# ── 3. watch them run, and snapshot the concurrency evidence WHILE they run ──
# Peak concurrent slots/captures can only be observed live; after the run the pods
# are gone and the leases are released, so a post-hoc check would always read zero.
echo "== running (timeout ${RUN_TIMEOUT}s) =="

# live <status> — true while a session has not reached a terminal state.
live() { case "$1" in completed|failed) return 1 ;; *) return 0 ;; esac; }

# Session status comes from the CONTROLLER'S LOG, not the runs table. The runs row is
# INSERTed only by submission-api's HTTP StartBenchmark; every other writer (the
# controller's MarkRunFailed, submission-api's UpdateRunStatus) is an UPDATE. Driving
# a run by producing benchmark.requested straight to Kafka — what this script and
# e2e/run.sh both do — therefore never creates a row, and every status write silently
# affects zero rows. Reading `runs` here would report "none" forever.
# Empty output (no transition logged yet) is normal at the start of a run, so the
# whole pipeline is guarded: under `set -euo pipefail` a non-matching grep would
# otherwise abort the caller on its first poll.
ctl_status() {
  { kubectl -n benchmark logs deploy/bot-fleet-controller --tail=2000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" \
      | grep '"msg":"session transition"' \
      | grep -oE '"status":"[a-z_]+"' \
      | tail -1 | cut -d'"' -f4; } 2>/dev/null || true
}
# Controller metrics live on 8080 (the container's only declared port), not 9090.
CTL_POD="$(kubectl -n benchmark get pod -l app=bot-fleet-controller -o jsonpath='{.items[0].metadata.name}')"
ctl_metrics() {
  kubectl get --raw "/api/v1/namespaces/benchmark/pods/${CTL_POD}:8080/proxy/metrics" 2>/dev/null || true
}

PEAK_SLOTS=0; PEAK_CAPTURES=0; PEAK_LEASED_PARTS=0; PEAK_LEASED_BANDS=0
BOTH_LIVE=0; BOTH_FIRING=0
deadline=$(( $(date +%s) + RUN_TIMEOUT ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  # Label values come from sandbox-orchestrator/internal/k8s/slot.go: algo pods are
  # app=algo, the eBPF capture Job's pods are app=ebpf-capture, both stamped
  # app.kubernetes.io/managed-by=sandbox-orchestrator.
  slots=$(kubectl -n sandbox get pods -l app=algo --no-headers 2>/dev/null | grep -c Running || true)
  caps=$(kubectl -n sandbox get pods -l app=ebpf-capture --no-headers 2>/dev/null | grep -c Running || true)
  # `[ cond ] && assign` is NOT safe here: when the test is false the whole statement
  # returns non-zero, and under `set -e` that terminates the script mid-run — which is
  # exactly what happened on the first attempt (zero slots on iteration one killed the
  # poll loop silently, before a single assertion ran). Use explicit `if`.
  if [ "${slots:-0}" -gt "$PEAK_SLOTS" ]; then PEAK_SLOTS=$slots; fi
  if [ "${caps:-0}" -gt "$PEAK_CAPTURES" ]; then PEAK_CAPTURES=$caps; fi

  m="$(ctl_metrics || true)"
  if [ -n "$m" ]; then
    lp=$(prom_value "$m" iicpc_controller_leased_partitions)
    lb=$(prom_value "$m" iicpc_controller_leased_order_bands)
    if [ "${lp:-0}" -gt "$PEAK_LEASED_PARTS" ]; then PEAK_LEASED_PARTS=$lp; fi
    if [ "${lb:-0}" -gt "$PEAK_LEASED_BANDS" ]; then PEAK_LEASED_BANDS=$lb; fi
  fi

  a_status=$(ctl_status "$A_SESSION")
  b_status=$(ctl_status "$B_SESSION")
  # Two separate concurrency claims, and the weaker one alone is not enough.
  #   BOTH_LIVE  — both sessions non-terminal at the same instant. Satisfied even if
  #                one is merely waiting_ready while the other fires.
  #   BOTH_FIRING— both in `running` at the same instant, i.e. genuinely generating
  #                load together. This is the one that fails at replicas=1, because a
  #                worker awaits run_workload() inline and takes one session at a time.
  if [ -n "$a_status" ] && [ -n "$b_status" ] && live "$a_status" && live "$b_status"; then
    BOTH_LIVE=1
  fi
  if [ "$a_status" = running ] && [ "$b_status" = running ]; then BOTH_FIRING=1; fi
  printf "\r   A=%-12s B=%-12s slots=%s caps=%s leased(p/b)=%s/%s   " \
    "${a_status:-none}" "${b_status:-none}" "$slots" "$caps" "${lp:-?}" "${lb:-?}"
  case "$a_status" in completed|failed) case "$b_status" in completed|failed) break ;; esac ;; esac
  sleep 5
done
echo

# ── 4. assertions ────────────────────────────────────────────────────────────
echo "== assertions =="
a_status=$(ctl_status "$A_SESSION")
b_status=$(ctl_status "$B_SESSION")
[ "$a_status" = completed ] && ok "session A completed" || fail "session A status='$a_status' (want completed)"
[ "$b_status" = completed ] && ok "session B completed" || fail "session B status='$b_status' (want completed)"

[ "$BOTH_LIVE" = 1 ] && ok "both sessions non-terminal at the same instant" \
  || fail "never observed both sessions live at once — they may have serialized"
[ "$BOTH_FIRING" = 1 ] && ok "both sessions in 'running' at the same instant — load genuinely concurrent" \
  || fail "never observed both sessions firing at once (needs bot-fleet-worker replicas >= 2)"

[ "$PEAK_SLOTS" -ge 2 ] && ok "peak concurrent algo slots = $PEAK_SLOTS" \
  || fail "peak concurrent algo slots = $PEAK_SLOTS (want >= 2)"
[ "$PEAK_CAPTURES" -ge 2 ] && ok "peak concurrent capture pods = $PEAK_CAPTURES" \
  || fail "peak concurrent capture pods = $PEAK_CAPTURES (want >= 2)"

[ "$PEAK_LEASED_PARTS" -ge 2 ] && ok "peak leased workload partitions = $PEAK_LEASED_PARTS" \
  || fail "peak leased workload partitions = $PEAK_LEASED_PARTS (want >= 2 — one per session)"
[ "$PEAK_LEASED_BANDS" -ge 2 ] && ok "peak leased order bands = $PEAK_LEASED_BANDS" \
  || fail "peak leased order bands = $PEAK_LEASED_BANDS (want >= 2 — exclusive per session)"

m="$(ctl_metrics || true)"
after_p=$(prom_value "$m" iicpc_controller_leased_partitions)
after_b=$(prom_value "$m" iicpc_controller_leased_order_bands)
[ "${after_p:-1}" = 0 ] && ok "partition leases released after run" \
  || fail "partition leases still held after run: $after_p"
[ "${after_b:-1}" = 0 ] && ok "band leases released after run" \
  || fail "band leases still held after run: $after_b"

# Telemetry has to land per session and stay separated.
a_rows=$(ts_val "SELECT count(*) FROM metrics WHERE session_id='$A_SESSION';")
b_rows=$(ts_val "SELECT count(*) FROM metrics WHERE session_id='$B_SESSION';")
[ "${a_rows:-0}" -gt 0 ] && ok "session A latency rows = $a_rows" || fail "session A has no metrics rows"
[ "${b_rows:-0}" -gt 0 ] && ok "session B latency rows = $b_rows" || fail "session B has no metrics rows"
cross=$(ts_val "SELECT count(*) FROM metrics WHERE session_id='$A_SESSION' AND contestant_id<>'$A_CONTESTANT';")
[ "${cross:-0}" = 0 ] && ok "no cross-contestant rows under session A" \
  || fail "$cross rows under session A carry another contestant_id"

# Per-session VALIDATION, read from the validator rather than from score_progress.
#
# score_progress is written by score-computer, and score.Compute REQUIRES a `ramp`
# session in the run-group (ErrMissingRampSession, score.go:208) because peak sustained
# TPS is derived from the ramp. This test deliberately runs ONE scenario per contestant
# to keep two sessions concurrent and small, so its run-groups can never be scored —
# score-computer correctly logs "missing ramp session". Asserting on score_progress here
# would be asserting that a single-scenario run-group is scorable, which it is not by
# design.
#
# What this test can and should assert is that VALIDATION ran per session and kept the
# two sessions' streams apart. Those counters live on the validator's published
# correctness event; they reach score_progress only via a scorable run-group. Read them
# from the validator's own log, polling because validation is asynchronous (settle delay
# + drain) and sessions are validated one at a time.
SCORE_WAIT="${SCORE_WAIT:-240}"
echo "   waiting up to ${SCORE_WAIT}s for both sessions to be validated..."
# Guarded pipeline: under `set -euo pipefail` a grep that matches nothing exits 1,
# pipefail propagates it out of the command substitution, and set -e kills the script.
# "Not validated yet" is the NORMAL state while this loop polls, so an unguarded
# pipeline here aborts the run before any assertion prints — which is exactly what
# happened on the first attempt.
val_counter() { # val_counter <session> <field>
  { kubectl -n benchmark logs -l app=correctness-validator --tail=4000 2>/dev/null \
      | grep "\"session_id\":\"$1\"" \
      | grep '"msg":"session validated"' \
      | grep -oE "\"$2\":[0-9]+" \
      | tail -1 | cut -d: -f2; } 2>/dev/null || true
}
val_deadline=$(( $(date +%s) + SCORE_WAIT ))
while [ "$(date +%s)" -lt "$val_deadline" ]; do
  a_seen=$(val_counter "$A_SESSION" sent)
  b_seen=$(val_counter "$B_SESSION" sent)
  # `[ ... ] && break` is unsafe: when the test is false the whole statement returns
  # non-zero and set -e terminates the loop's script. Use an explicit if.
  if [ -n "$a_seen" ] && [ -n "$b_seen" ]; then break; fi
  sleep 5
done
a_corr=$(val_counter "$A_SESSION" score)
b_corr=$(val_counter "$B_SESSION" score)
[ -n "$(val_counter "$A_SESSION" sent)" ] && ok "session A was validated independently" \
  || fail "session A was never validated"
[ -n "$(val_counter "$B_SESSION" sent)" ] && ok "session B was validated independently" \
  || fail "session B was never validated"

# matched <= sent is the invariant that actually breaks under cross-band contamination:
# a session validated against another session's band would match orders it never sent.
# "A outscored B" would not catch that on its own.
for pair in "A:$A_SESSION" "B:$B_SESSION"; do
  who=${pair%%:*}; sid=${pair#*:}
  sent=$(val_counter "$sid" sent)
  matched=$(val_counter "$sid" matched)
  if [ -z "$sent" ] || [ -z "$matched" ]; then
    fail "session $who has no sent/matched counters"
  elif [ "$sent" -eq 0 ]; then
    # FAIL-CLOSED on zero. `matched <= sent` is trivially true at 0 <= 0, and that is
    # exactly what it reported while the validator could not decode orders.sent at all —
    # a vacuous pass hiding a total failure to observe the sent stream.
    fail "session $who observed sent=0 — the validator saw no sent events, so isolation is untested"
  elif [ "$matched" -gt "$sent" ]; then
    fail "session $who matched($matched) > sent($sent) — validation crossed bands"
  else
    ok "session $who matched($matched) <= sent($sent), sent>0 — no cross-band leakage"
  fi
done

# Each session's sent count should be close to its own offered load. Wildly unequal
# counts would suggest one session's stream bled into the other's band.
a_sent=$(val_counter "$A_SESSION" sent); b_sent=$(val_counter "$B_SESSION" sent)
if [ -n "$a_sent" ] && [ -n "$b_sent" ] && [ "$a_sent" -gt 0 ] && [ "$b_sent" -gt 0 ]; then
  ratio=$(awk -v a="$a_sent" -v b="$b_sent" 'BEGIN{printf "%.2f", (a>b? a/b : b/a)}')
  awk -v r="$ratio" 'BEGIN{exit !(r < 1.5)}' \
    && ok "per-session sent counts comparable (A=$a_sent B=$b_sent, ratio $ratio)" \
    || fail "per-session sent counts diverge (A=$a_sent B=$b_sent, ratio $ratio) — suspect band bleed"
fi

echo
echo "   -- reported, not asserted --"
echo "   correctness: A(book)=${a_corr:-none}  B(echo)=${b_corr:-none}"
echo "   NOTE: score_progress rows are NOT expected here — score.Compute needs a ramp"
echo "   session in the run-group, and this test runs one scenario per contestant."
if [ -n "$a_corr" ] && [ -n "$b_corr" ]; then
  if awk -v a="$a_corr" -v b="$b_corr" 'BEGIN{exit !(a>b)}'; then
    echo "   book > echo, as expected of a correct engine vs an acker"
  else
    echo "   NOTE: book did NOT outscore echo — not a B1 failure, chase separately"
  fi
fi

echo
echo "sessions:  A=$A_SESSION  B=$B_SESSION"
if [ "$ASSERT_FAILURES" -eq 0 ]; then
  echo "############ B1 PASSED ############"
else
  echo "############ B1: $ASSERT_FAILURES assertion(s) FAILED ############"
  exit 1
fi

#!/usr/bin/env bash
# Task-count sweep for the bot-worker pacing knee: max TPS subject to
#   (a) max_slip_ms < 1 send interval   (b) max_batch == 1  (distinct t1 per order)
# Modes:
#   single: 1 process, EXAMPLE_WORKER_THREADS=$THREADS (default 4)
#   split:  $PROCS processes x 1 tokio worker thread, each taskset-pinned to its own core
# Usage: ./local-task-sweep.sh [single|split|both] ; needs Kafka on localhost:9092.
set -euo pipefail

BIN="${BIN:-./target/release/examples/bot_worker_fix_roundtrip}"
MODE="${1:-both}"
STEPS=(${STEPS:-50 100 200 300 400 500 700 1000})
RPS="${RPS:-1000}"
DUR="${DUR:-20}"
THREADS="${THREADS:-4}"
PROCS="${PROCS:-4}"
OUT="${OUT:-deploy-bench/task-sweep-$(date +%s).tsv}"

echo -e "mode\ttasks_total\trate_med\trate_max\tavg_batch_max\tmax_batch\tmax_slip_ms\tcs_per_s" | tee "$OUT"

run_one() { # $1=tasks $2=threads $3=cpulist $4=bind $5=logfile
  local pin=()
  [ -n "$3" ] && pin=(taskset -c "$3")
  BOT_DISABLE_TELEMETRY=1 EXAMPLE_BENCH=1 EXAMPLE_TASKS="$1" EXAMPLE_RPS="$RPS" \
  EXAMPLE_DURATION_S="$DUR" EXAMPLE_ECHO_LATENCY_MS=0 EXAMPLE_DROP_EVERY="${DROP_EVERY:-0}" \
  BOT_MAX_INFLIGHT_PER_TASK="${MAX_INFLIGHT:-10000}" \
  EXAMPLE_WORKER_THREADS="$2" EXAMPLE_BIND="$4" \
  "${pin[@]}" "$BIN" >"$5" 2>&1 &
  echo $!
}

# Steady-state stats from snapshot lines: skip warmup (t_s<=2) and tail.
parse_log() { # $1=logfile -> "rate_med rate_max avg_batch_max max_batch max_slip"
  sed 's/\x1b\[[0-9;]*m//g' "$1" | awk '
    /bot send snapshot/ {
      for (i=1;i<=NF;i++) {
        split($i,kv,"=")
        if (kv[1]=="t_s") t=kv[2]
        if (kv[1]=="send_rate_per_s") r=kv[2]
        if (kv[1]=="avg_batch") { gsub(/"/,"",kv[2]); ab=kv[2] }
        if (kv[1]=="max_batch") mb=kv[2]
        if (kv[1]=="max_slip_ms") { gsub(/"/,"",kv[2]); ms=kv[2] }
      }
      if (t>2 && r>0) {
        rates[n++]=r
        if (ab>abmax) abmax=ab
        if (mb>mbmax) mbmax=mb
        if (ms>msmax) msmax=ms
      }
    }
    END {
      if (n==0) { print "0 0 0 0 0"; exit }
      for (i=0;i<n;i++) for (j=i+1;j<n;j++) if (rates[j]<rates[i]) { tmp=rates[i]; rates[i]=rates[j]; rates[j]=tmp }
      med=rates[int(n/2)]; mx=rates[n-1]
      printf "%d %d %.2f %d %.3f\n", med, mx, abmax, mbmax, msmax
    }'
}

cs_snap() { # $@=pids -> total ctxt switches across pids
  local total=0 v
  for pid in "$@"; do
    v=$(awk '/ctxt_switches/{s+=$2} END{print s+0}' /proc/"$pid"/task/*/status 2>/dev/null || echo 0)
    total=$(( total + v ))
  done
  echo "$total"
}
cs_rate() { # $@=pids -> switches/s summed over a concurrent 3s window
  local a b
  a=$(cs_snap "$@"); sleep 3; b=$(cs_snap "$@")
  echo $(( (b - a) / 3 ))
}

for T in "${STEPS[@]}"; do
  if [ "$MODE" = single ] || [ "$MODE" = both ]; then
    log=$(mktemp)
    pid=$(run_one "$T" "$THREADS" "" "127.0.0.1:9876" "$log")
    sleep 8; cs=$(cs_rate "$pid")
    while kill -0 "$pid" 2>/dev/null; do sleep 1; done
    read -r med mx ab mb ms <<<"$(parse_log "$log")"
    echo -e "single\t$T\t$med\t$mx\t$ab\t$mb\t$ms\t$cs" | tee -a "$OUT"
    rm -f "$log"
  fi
  if [ "$MODE" = split ] || [ "$MODE" = both ]; then
    pids=(); logs=()
    per=$(( T / PROCS )); [ "$per" -lt 1 ] && per=1
    for i in $(seq 0 $((PROCS-1))); do
      log=$(mktemp); logs+=("$log")
      pids+=("$(run_one "$per" 1 "$i" "127.0.0.1:$((9876+i))" "$log")")
      sleep 0.3
    done
    sleep 8; cs=$(cs_rate "${pids[@]}")
    for p in "${pids[@]}"; do while kill -0 "$p" 2>/dev/null; do sleep 1; done; done
    med=0; mx=0; ab=0; mb=0; ms=0
    for log in "${logs[@]}"; do
      read -r m1 m2 a1 b1 s1 <<<"$(parse_log "$log")"
      med=$((med+m1)); mx=$((mx+m2))
      ab=$(awk -v a="$ab" -v b="$a1" 'BEGIN{print (b>a)?b:a}')
      mb=$(( b1 > mb ? b1 : mb ))
      ms=$(awk -v a="$ms" -v b="$s1" 'BEGIN{print (b>a)?b:a}')
      rm -f "$log"
    done
    echo -e "split\t$((per*PROCS))\t$med\t$mx\t$ab\t$mb\t$ms\t$cs" | tee -a "$OUT"
  fi
done
echo "done -> $OUT"

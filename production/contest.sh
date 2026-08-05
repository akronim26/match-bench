#!/usr/bin/env bash
# production/contest.sh — clone -> deploy -> verify -> run, in one command.
#
# This file only SEQUENCES. Every phase is a standalone script that can be run
# by hand and re-run safely, which matters because a bring-up that fails at
# phase 5 should not require redoing phases 1-4 (phase 1 alone is 20 minutes and
# real money).
#
#   ./production/contest.sh                 # full run, provision -> smoke
#   ./production/contest.sh --from 03       # resume from the platform phase
#   ./production/contest.sh --only 05       # just re-run verification
#   ./production/contest.sh --teardown      # destroy (requires CONFIRM=yes)
#
# Phases and the ordering constraints they encode are explained in
# production/README.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"

PHASES=(00-preflight 01-cluster 02-images 03-platform 04-irsa 05-verify 06-smoke)

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

main() {
  local from="" only="" teardown=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --from)     from="$2"; shift 2 ;;
      --only)     only="$2"; shift 2 ;;
      --teardown) teardown=1; shift ;;
      -h|--help)  usage 0 ;;
      *)          printf 'unknown argument: %s\n\n' "$1" >&2; usage 1 ;;
    esac
  done

  if [ "$teardown" -eq 1 ]; then
    exec "$ROOT/production/99-teardown.sh"
  fi

  local started=0 p run=() label
  for p in "${PHASES[@]}"; do
    label="${p%%-*}"
    if [ -n "$only" ]; then
      [ "$label" = "$only" ] && run+=("$p")
      continue
    fi
    [ -n "$from" ] && [ "$started" -eq 0 ] && [ "$label" != "$from" ] && continue
    started=1
    run+=("$p")
  done

  if [ ${#run[@]} -eq 0 ]; then
    die "no phases selected (--from/--only must name one of: $(printf '%s ' "${PHASES[@]%%-*}"))"
  fi

  printf '%s╔══ contest.sh ══%s\n' "$_C_BLU" "$_C_RESET"
  printf '  phases: %s\n' "${run[*]}"
  local t0=$SECONDS

  for p in "${run[@]}"; do
    local script="$ROOT/production/$p.sh"
    [ -x "$script" ] || die "missing or non-executable phase: $script"
    if ! "$script"; then
      printf '\n%s╚══ FAILED at %s ══%s\n' "$_C_RED" "$p" "$_C_RESET" >&2
      printf '  fix, then resume:  %s --from %s\n' "$0" "${p%%-*}" >&2
      exit 1
    fi
  done

  local mins=$(( (SECONDS - t0) / 60 ))
  printf '\n%s╚══ complete in %dm ══%s\n' "$_C_GRN" "$mins" "$_C_RESET"

  # Only meaningful when the run actually reached the end.
  if [ -z "$only" ]; then
    cat <<'NEXT'

  The platform is deployed and a reference contestant has been graded.

  Watch it:
    kubectl -n platform     port-forward svc/frontend 3001:8080
    kubectl -n observability port-forward svc/grafana  3000:3000

  Tear it down (billing continues until you do):
    CONFIRM=yes ./production/contest.sh --teardown
NEXT
  fi
}

main "$@"

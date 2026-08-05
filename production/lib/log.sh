# production/lib/log.sh — output, failure accounting, and the run contract.
#
# Every phase script sources this. The rules it enforces:
#   - a phase that fails EXITS non-zero (contest.sh stops the sequence)
#   - a check that fails is RECORDED and the phase continues, so one run reports
#     every problem instead of one problem per run
#   - nothing is silently swallowed: there is no `|| true` idiom in this tree
#
# That last point is deliberate. e2e/assert.sh's psql_exec ends in `|| true`, so
# a query that ERRORS reads as "0 rows" and is reported as a failed assertion
# rather than a broken harness — the two are not the same and cost debugging
# time. Here, an error in a check is a distinct outcome from a failed check.

set -o pipefail

_C_RESET=$'\033[0m'; _C_RED=$'\033[31m'; _C_GRN=$'\033[32m'
_C_YEL=$'\033[33m'; _C_BLU=$'\033[34m'; _C_DIM=$'\033[2m'
if [ ! -t 1 ]; then _C_RESET=""; _C_RED=""; _C_GRN=""; _C_YEL=""; _C_BLU=""; _C_DIM=""; fi

# FAILURES accumulates human-readable descriptions across a phase.
FAILURES=()

phase() { printf '\n%s══ %s %s\n' "$_C_BLU" "$*" "$_C_RESET"; }
step()  { printf '%s→%s %s\n' "$_C_BLU" "$_C_RESET" "$*"; }
info()  { printf '  %s\n' "$*"; }
dim()   { printf '  %s%s%s\n' "$_C_DIM" "$*" "$_C_RESET"; }

ok()    { printf '  %sPASS%s %s\n' "$_C_GRN" "$_C_RESET" "$*"; }
warn()  { printf '  %sWARN%s %s\n' "$_C_YEL" "$_C_RESET" "$*"; }

# fail: record a failed CHECK and keep going.
fail() {
  printf '  %sFAIL%s %s\n' "$_C_RED" "$_C_RESET" "$*"
  FAILURES+=("$*")
}

# die: an unrecoverable phase error — stop immediately.
die() {
  printf '\n%sABORT%s %s\n' "$_C_RED" "$_C_RESET" "$*" >&2
  exit 1
}

# finish: call at the end of a phase. Exits non-zero if anything failed, and
# prints every failure again so the tail of the log is the summary.
finish() {
  local name="${1:-phase}"
  if [ ${#FAILURES[@]} -eq 0 ]; then
    printf '\n%s✓ %s: all checks passed%s\n' "$_C_GRN" "$name" "$_C_RESET"
    return 0
  fi
  printf '\n%s✗ %s: %d check(s) failed%s\n' "$_C_RED" "$name" "${#FAILURES[@]}" "$_C_RESET" >&2
  local f; for f in "${FAILURES[@]}"; do printf '    - %s\n' "$f" >&2; done
  exit 1
}

# need: assert a binary is present before we depend on it.
need() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1${2:+ ($2)}"
}

#!/usr/bin/env bash
# production/check-declarative.sh — the guard that stops this becoming e2e/.
#
# production/ exists because the previous "clone -> deploy -> run" path drifted
# into configuring the cluster imperatively, and five of seven defects in the
# 2026-08-03 bring-up were "the harness injects it, the overlay never created
# it". A rule enforced only by discipline decays; this one is checked.
#
# Two invariants:
#
#   1. Scripts ORCHESTRATE, never CONFIGURE. Anything a deployment needs is
#      declared in k8s/ or overlays/. Secrets are the one sanctioned exception
#      — they must not be in git.
#
#   2. production/ depends on NOTHING it is meant to outlive. It references
#      declarative assets (infra/terraform-v2/, k8s/, overlays/) and reimplements
#      everything procedural, so e2e/, deploy-bench/, deploy-local/ and
#      infra/terraform (v1) can be retired without touching this directory.
#
# Run standalone, or from CI.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"

# Only the scripts, not this file and not the docs (both legitimately NAME the
# banned patterns in order to explain them).
targets() {
  find "$ROOT/production" -type f -name '*.sh' \
    ! -name 'check-declarative.sh' -print
}

# scan <description> <extended-regex> [allow-file-basename]
scan() {
  local desc="$1" re="$2" allow="${3:-}" hits f
  hits=""
  while read -r f; do
    [ -n "$allow" ] && [ "$(basename "$f")" = "$allow" ] && continue
    # Strip comments before matching so an explanatory comment is not a hit.
    local m
    m="$(sed 's/#.*//' "$f" | grep -nE "$re" | sed "s|^|$(realpath --relative-to="$ROOT" "$f"):|" || true)"
    [ -n "$m" ] && hits+="$m"$'\n'
  done < <(targets)
  if [ -n "$hits" ]; then
    printf '%s' "$hits" | grep -v '^$' | sed 's/^/      /'
    fail "$desc"
  else
    ok "$desc"
  fi
}

main() {
  phase "Declarative guard"

  step "invariant 1: scripts orchestrate, never configure"
  scan "no 'kubectl set env'"        'kubectl[^|]*[[:space:]]set[[:space:]]+env'
  scan "no 'kubectl patch'"          'kubectl[^|]*[[:space:]]patch[[:space:]]'
  scan "no 'kubectl scale'"          'kubectl[^|]*[[:space:]]scale[[:space:]]'
  scan "no 'kubectl set image'"      'kubectl[^|]*[[:space:]]set[[:space:]]+image'
  scan "no 'kubectl annotate'"       'kubectl[^|]*[[:space:]]annotate[[:space:]]'
  # sed-mutated manifests piped into apply — the pattern that silently rewrote
  # the Kafka StatefulSet in both older harnesses.
  scan "no sed-mutated manifest piped to apply" 'sed[^|]*\|[[:space:]]*kubectl[[:space:]]+apply'
  # Heredoc manifests. `kubectl create secret ... | kubectl apply` is the
  # sanctioned secret path, so secrets/load.sh is exempt from this one only.
  scan "no inline heredoc manifests"  'cat[[:space:]]*<<.?(EOF|YAML).?[[:space:]]*\|[[:space:]]*kubectl' 'load.sh'

  step "invariant 2: no dependency on paths this replaces"
  # Referencing these at RUNTIME would make retiring them impossible, which is
  # the entire reason production/ was written from scratch rather than by
  # editing e2e/.
  scan "no reference to e2e/"                 '(^|[^-[:alnum:]_/])e2e/'
  scan "no reference to deploy-bench/"        'deploy-bench/'
  scan "no reference to deploy-local/ scripts" 'deploy-local/[a-z0-9-]+\.sh'
  # infra/terraform (v1) — but NOT infra/terraform-v2, which is a live asset.
  scan "no reference to infra/terraform (v1)" 'infra/terraform([^-]|$)'

  step "sanity"
  local n
  n="$(targets | grep -c . || true)"
  info "$n script(s) scanned"
  [ "${n:-0}" -ge 8 ] && ok "expected script count" \
    || fail "only $n scripts found — did the layout change?"

  finish "Declarative guard"
}

main "$@"

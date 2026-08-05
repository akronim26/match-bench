# production/lib/k8s.sh — cluster queries and waits.
#
# Read-only helpers plus rollout waiting. Nothing here mutates configuration:
# per production/README.md rule 2, config lives in k8s/ + overlays/, so this
# file must never grow a `set env`, `patch` or `scale`.

# ── context safety ───────────────────────────────────────────────────────────
# Local k3s and EKS are both in kubeconfig, and a phase run against the wrong
# one is the single most expensive mistake available. Every phase that touches
# the cluster calls this first.
require_context() {
  local want="$1" have
  have="$(kubectl config current-context 2>/dev/null)" \
    || die "no kubectl context set — run 01-cluster.sh first"
  case "$have" in
    *"$want"*) dim "context: $have" ;;
    *) die "kubectl context is '$have', expected one matching '$want'. Refusing to continue." ;;
  esac
}

# ── queries ──────────────────────────────────────────────────────────────────

# nodes_in_pool <pool> — node count carrying pool=<pool>, or role=<pool> as a
# fallback (terraform labels the general group `role`, not `pool`).
nodes_in_pool() {
  local pool="$1" n
  n="$(kubectl get nodes -l "pool=$pool" --no-headers 2>/dev/null | grep -c . || true)"
  if [ "${n:-0}" -eq 0 ]; then
    n="$(kubectl get nodes -l "role=$pool" --no-headers 2>/dev/null | grep -c . || true)"
  fi
  printf '%s' "${n:-0}"
}

# pods_not_healthy — every pod not Running/Completed, "ns/name status" per line.
# Uses jsonpath rather than parsing `get pods` columns: a RESTARTS value like
# "4 (3m ago)" splits into extra fields and shifts every column after it.
pods_not_healthy() {
  kubectl get pods -A \
    -o 'jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name} {.status.phase}{"\n"}{end}' \
    2>/dev/null | awk '$2 != "Running" && $2 != "Succeeded"'
}

# pod_node <ns> <label-selector> — node names hosting pods matching a selector.
pod_node() {
  kubectl -n "$1" get pods -l "$2" \
    -o 'jsonpath={range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -c . || true
}

# pods_on_node <node> — "ns/name" for every non-kube-system pod on a node.
pods_on_node() {
  kubectl get pods -A --field-selector "spec.nodeName=$1" \
    -o 'jsonpath={range .items[*]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}' \
    2>/dev/null | grep -vE '^(kube-system|keda)/' || true
}

# deploy_env <ns> <deploy> <VAR> — the value of an env var on a LIVE Deployment.
# Reading the live object, not the manifest, is the point: it is what catches a
# stale image ref or an env the overlay failed to stamp.
deploy_env() {
  kubectl -n "$1" get deploy "$2" \
    -o "jsonpath={.spec.template.spec.containers[*].env[?(@.name=='$3')].value}" 2>/dev/null
}

# sa_annotation <ns> <sa> <key>
sa_annotation() {
  kubectl -n "$1" get sa "$2" -o "jsonpath={.metadata.annotations.$3}" 2>/dev/null
}

# ── waits ────────────────────────────────────────────────────────────────────

# wait_rollouts <ns> <deploy...> — waits for each, accumulating failures instead
# of aborting on the first, and printing the debug command for anything that did
# not come up. Ported in preference to e2e's `|| true` version, which reports
# success for a deployment that never rolled out.
wait_rollouts() {
  local ns="$1"; shift
  local timeout="${ROLLOUT_TIMEOUT:-300s}" d bad=()
  for d in "$@"; do
    if kubectl -n "$ns" rollout status "deploy/$d" --timeout="$timeout" >/dev/null 2>&1; then
      ok "rollout $ns/$d"
    else
      bad+=("$d")
      fail "rollout $ns/$d did not complete within $timeout"
    fi
  done
  if [ ${#bad[@]} -gt 0 ]; then
    dim "debug: kubectl -n $ns describe deploy ${bad[*]}"
    dim "debug: kubectl -n $ns logs deploy/${bad[0]} --tail=50"
  fi
}

# wait_for <seconds> <description> <command...> — poll until the command
# succeeds. Returns non-zero on timeout; the CALLER decides fail vs die.
wait_for() {
  local timeout="$1" desc="$2"; shift 2
  local deadline=$(( SECONDS + timeout ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 5
  done
  return 1
}

# port_forward_bg <ns> <svc> <local:remote> — start a forward, record its PID in
# PF_PIDS for cleanup, and wait until the port actually accepts a connection.
#
# Forwards bind to whatever context was active when they launched, so a stale
# one from a previous session is a silent tunnel into the WRONG cluster. These
# are always started fresh and always torn down by pf_cleanup.
PF_PIDS=()
port_forward_bg() {
  local ns="$1" svc="$2" map="$3" local_port="${3%%:*}"
  kubectl -n "$ns" port-forward "svc/$svc" "$map" >/dev/null 2>&1 &
  PF_PIDS+=($!)
  local deadline=$(( SECONDS + 30 ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; then
      exec 3<&- 2>/dev/null || true
      dim "port-forward $ns/$svc -> localhost:$local_port"
      return 0
    fi
    sleep 1
  done
  die "port-forward to $ns/$svc ($map) never became reachable"
}

pf_cleanup() {
  local p
  for p in "${PF_PIDS[@]:-}"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done
  PF_PIDS=()
}

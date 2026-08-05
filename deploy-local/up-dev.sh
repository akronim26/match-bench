#!/usr/bin/env bash
# Single-node k3s bring-up of the FULL platform from LOCALLY BUILT images.
#
# Why this exists next to up.sh: up.sh is the clone-and-go path and pulls
# ghcr.io/agrawalx/*:demo. Those published images predate this branch, and up.sh
# itself has drifted from k8s/ (it scales `deployment/bot-fleet`, which was renamed
# to bot-fleet-worker; it predates telemetry-rollup and the pool nodeSelectors). This
# script is the verification path: current manifests + images built from the working
# tree by build-dev-images.sh, every deployment's image set explicitly so nothing can
# silently fall back to a stale public tag.
#
# Deltas from the EKS deploy (deploy-bench/eks-up.sh), all forced by one untainted
# 16-core / 15 GiB node:
#   - Kafka 1 broker, RF=1, min.insync=1 (EKS runs 2 brokers)
#   - bot-fleet-worker's `pool: botworker` nodeSelector stripped (no pools here)
#   - sandbox-orchestrator: RUNTIME_CLASS and SANDBOX_NODE_POOL unset (runc, no taint)
#   - eBPF capture ON (it works natively on k3s; this is the whole point of local-first)
#   - no gro-disable DaemonSet: local MTU is already 1500, and it targets pool=sandbox
#   - KEDA ScaledObject skipped (no KEDA installed yet — that's task B5)
#   - NetworkPolicies skipped, in-cluster registry for contestant images
#   - laptop-sized resource requests (see the `set resources` block)
#
# Prereqs: k3s running, kubectl context pointing at it, images built:
#   TAG=dev1 deploy-local/build-dev-images.sh
#   TAG=dev1 deploy-local/import-dev-images.sh   (needs root — imports into containerd)
# Then:
#   TAG=dev1 deploy-local/up-dev.sh
#   deploy-local/forward.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K="kubectl"
TAG="${TAG:-dev1}"
IMG="iicpc"          # local-only image namespace; never resolves to a registry

# Scenario load. Deliberately far below the EKS defaults: the local-cluster tasks
# (B1 two concurrent sessions, B2 two-pass) verify concurrency and correctness
# semantics — band leases, per-session slots, band-scoped validation, isolated
# scores — all of which behave identically at 4k/s and at 25k/s. Low rates also keep
# total tasks across BOTH sessions well under MAX_TASKS_PER_WORKER=1000, past which a
# shard goes unassigned and that share of the load is silently dropped.
CONSTANT_RPS="${CONSTANT_RPS:-4000}"
SPIKE_RPS="${SPIKE_RPS:-6000}"
RAMP_RPS="${RAMP_RPS:-6000}"
# Durations are NOT free parameters. The ramp is built as 9 waves on a 20s cadence,
# and submission-api refuses to boot ("ramp duration too short for 9 waves at 20s
# cadence") if RAMP_DURATION_S < 180 — a crash loop on the scenario seeder, not a
# validation error at request time. 180 is the floor, not a tuning choice.
RAMP_DURATION_S="${RAMP_DURATION_S:-180}"

NODE_IP="$(${K} get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
REG="${NODE_IP}:5000"
echo ">> node ${NODE_IP} | registry ${REG} | image tag ${TAG}"

# ---- in-cluster registry trust -------------------------------------------
# Contestant zips are built in-cluster (spawner/kaniko) and pushed to ${REG} over
# plain HTTP; containerd must be told to trust it. That file + a k3s restart are
# root, so this only regenerates it for the CURRENT node IP and warns — the node IP
# is this box's WiFi address and moves with the network.
cat > deploy-local/registries.yaml <<EOF
# k3s registry config — lets the node's containerd pull contestant images from
# the in-cluster registry over plain HTTP. Install to /etc/rancher/k3s/registries.yaml
# then restart k3s. NODE_IP must match the registry hostPort address used in up.sh.
#
# Regenerated for node ${NODE_IP}. This is the machine's *WiFi* address, so it
# changes with the network — up-dev.sh re-derives it on every run and warns when
# this file no longer matches what is installed.
mirrors:
  "${REG}":
    endpoint:
      - "http://${REG}"
configs:
  "${REG}":
    tls:
      insecure_skip_verify: true
EOF
if ! grep -q "\"${REG}\"" /etc/rancher/k3s/registries.yaml 2>/dev/null; then
  echo "!! /etc/rancher/k3s/registries.yaml does not trust ${REG}."
  echo "!! Platform comes up fine; SUBMITTING a contestant zip will fail to pull. Fix (root):"
  echo "!!   sudo cp deploy-local/registries.yaml /etc/rancher/k3s/registries.yaml && sudo systemctl restart k3s"
fi

# ---- dev credentials (local only) ----------------------------------------
PG_PW=devpass ; TS_PW=devpass
MINIO_AK=minioadmin ; MINIO_SK=minioadmin123
DB="postgres://iicpc:${PG_PW}@postgres.data.svc.cluster.local:5432/iicpc?sslmode=disable"
TSDB="postgres://iicpc:${TS_PW}@timescaledb.data.svc.cluster.local:5432/metrics?sslmode=disable"
KB="kafka.data.svc.cluster.local:9092"
REDIS_ADDR="redis.data.svc.cluster.local:6379"
REDIS_URL="redis://redis.data.svc.cluster.local:6379"

mk() { ${K} create secret generic "$1" -n "$2" "${@:3}" --dry-run=client -o yaml | ${K} apply -f - ; }
# apply a dir's manifests, skipping netpol / namespace / ECR variants / KEDA objects
applynp() {
  find "$1" -name '*.yaml' \
    ! -name '*network-policy*' ! -name 'namespace.yaml' \
    ! -name '*.ecr.yaml' ! -name 'scaledobject.yaml' ! -name '*ingress*' \
    -print0 | xargs -0 -I{} ${K} apply -f {}
}

echo "== 1. namespaces =="
for ns in data build platform sandbox benchmark observability; do ${K} apply -f k8s/$ns/namespace.yaml; done

echo "== 2. secrets =="
mk postgres-secret data --from-literal=password=$PG_PW
mk timescaledb-secret data --from-literal=password=$TS_PW
mk minio-secret data --from-literal=access-key=$MINIO_AK --from-literal=secret-key=$MINIO_SK
KID=$(python3 -c "import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes+uuid.uuid4().bytes[:4]).decode().rstrip('='))")
mk kafka-secret data --from-literal=cluster-id=$KID
mk submission-api-secret platform --from-literal=database-url="$DB" --from-literal=minio-access-key=$MINIO_AK --from-literal=minio-secret-key=$MINIO_SK
# submission-api hard-references GOOGLE_CLIENT_ID from auth-api-secret (optional:false)
# even with AUTH_REQUIRED=false, so the secret must exist or the container never starts.
${K} apply -f bootstrap/secrets-live/platform-auth-api.yaml 2>/dev/null \
  || mk auth-api-secret platform --from-literal=google-client-id=dummy
mk leaderboard-api-secret platform --from-literal=database-url="$DB" --from-literal=timescale-url="$TSDB" --from-literal=kafka-brokers="$KB" --from-literal=redis-addr="$REDIS_ADDR"
mk spawner-secret build --from-literal=database-url="$DB" --from-literal=minio-access-key=$MINIO_AK --from-literal=minio-secret-key=$MINIO_SK --from-literal=harbor-staging-endpoint="$REG" --from-literal=harbor-production-endpoint="$REG" --from-literal=harbor-user=anon --from-literal=harbor-password=anon
mk bot-fleet-controller-secret benchmark --from-literal=database-url="$DB"
mk correctness-validator-secret benchmark --from-literal=kafka-brokers="$KB" --from-literal=database-url="$DB"
mk score-computer-secret benchmark --from-literal=database-url="$DB" --from-literal=timescale-url="$TSDB" --from-literal=kafka-brokers="$KB" --from-literal=redis-addr="$REDIS_ADDR"
mk telemetry-ingester-secrets benchmark --from-literal=KAFKA_BROKERS="$KB" --from-literal=TIMESCALE_URL="$TSDB" --from-literal=REDIS_URL="$REDIS_URL"
mk grafana-admin observability --from-literal=admin-user=admin --from-literal=admin-password=admin

echo "== 3. in-cluster registry =="
${K} apply -f deploy-local/registry.yaml

echo "== 4. data tier (kafka: 1 broker / RF=1) =="
${K} apply -f k8s/data/postgres/service.yaml -f k8s/data/postgres/statefulset.yaml
${K} apply -f k8s/data/timescaledb/service.yaml -f k8s/data/timescaledb/statefulset.yaml
${K} apply -f k8s/data/minio/service.yaml -f k8s/data/minio/statefulset.yaml
${K} apply -f k8s/data/redis/service.yaml -f k8s/data/redis/statefulset.yaml
${K} apply -f k8s/data/kafka/service.yaml
# single broker: replicas 1, one quorum voter, RF/ISR 1. The manifest's podAntiAffinity
# (one broker per node) is satisfied trivially at replicas=1.
sed -e 's/replicas: 3/replicas: 1/' \
    -e 's#0@kafka-0.kafka.data.svc.cluster.local:9093,1@kafka-1.kafka.data.svc.cluster.local:9093,2@kafka-2.kafka.data.svc.cluster.local:9093#0@kafka-0.kafka.data.svc.cluster.local:9093#' \
    -e 's/value: "3"/value: "1"/g' -e 's/value: "2"/value: "1"/g' \
    k8s/data/kafka/statefulset.yaml | ${K} apply -f -
# Laptop sizing: the EKS request (1 cpu / 2Gi) plus everything else oversubscribes a
# 15 GiB box. Limits stay generous — they're a ceiling, not a reservation — so Kafka
# still gets page cache when it needs it.
${K} -n data set resources statefulset/kafka -c kafka \
  --requests=cpu=300m,memory=768Mi --limits=cpu=4,memory=4Gi >/dev/null

echo ">> waiting for data tier..."
${K} -n data rollout status statefulset/postgres --timeout=240s
${K} -n data rollout status statefulset/timescaledb --timeout=240s
${K} -n data rollout status statefulset/kafka --timeout=300s
${K} -n data rollout status statefulset/minio --timeout=180s || true
${K} -n data rollout status statefulset/redis --timeout=180s || true

echo "== 5. kafka topics (RF=1) =="
${K} -n data delete job/kafka-topic-init --ignore-not-found >/dev/null
sed -e 's/--replication-factor 3/--replication-factor 1/' \
    -e 's/min.insync.replicas=2/min.insync.replicas=1/' \
    k8s/data/kafka/topic-init-job.yaml | ${K} apply -f -
${K} -n data wait --for=condition=complete job/kafka-topic-init --timeout=240s \
  || ${K} -n data logs job/kafka-topic-init | tail -30

echo "== 6. observability (prometheus / grafana / loki) =="
applynp k8s/observability
${K} apply -f deploy-local/grafana-timescale-datasource.yaml
${K} -n observability rollout restart deployment/grafana >/dev/null 2>&1 || true

echo "== 7. build tier (spawner -> in-cluster registry) =="
${K} apply -f k8s/build/rbac.yaml
applynp k8s/build/spawner
${K} -n build set image deployment/spawner spawner=$IMG/spawner:$TAG
${K} -n build set env deployment/spawner \
  SPAWNER_IMAGE=$IMG/spawner:$TAG HARBOR_PROJECT=iicpc REGISTRY_INSECURE=true \
  HARBOR_STAGING_ENDPOINT="$REG" HARBOR_PRODUCTION_ENDPOINT="$REG"

echo "== 8. platform (submission-api, leaderboard-api, frontend; auth OFF) =="
applynp k8s/platform/submission-api
${K} -n platform set image deployment/submission-api submission-api=$IMG/submission-api:$TAG
# AUTH off: every visitor is DEFAULT_CONTESTANT_ID, which must match the frontend's
# NEXT_PUBLIC_DEFAULT_CONTESTANT_ID or a UI submission is owned by an identity that
# can't re-read it. RESEED_SCENARIOS rebuilds scenarios from these *_RPS values on
# every submission-api boot.
${K} -n platform set env deployment/submission-api \
  AUTH_REQUIRED=false DEFAULT_CONTESTANT_ID=echo-contestant \
  RESEED_SCENARIOS=true SEED_SCENARIOS=correctness,constant,spike,ramp \
  CONSTANT_TOTAL_RPS=$CONSTANT_RPS SPIKE_PEAK_RPS=$SPIKE_RPS RAMP_PEAK_RPS=$RAMP_RPS \
  CONSTANT_DURATION_S=60 SPIKE_DURATION_S=60 RAMP_DURATION_S=$RAMP_DURATION_S
applynp k8s/platform/leaderboard-api
${K} -n platform set image deployment/leaderboard-api leaderboard-api=$IMG/leaderboard-api:$TAG
# auth-api is not deployed (auth is off), but the frontend's nginx resolves its
# upstream at startup, so the Service name has to exist.
cat <<'YAML' | ${K} apply -f -
apiVersion: v1
kind: Service
metadata: { name: auth-api, namespace: platform }
spec:
  ports: [{ name: http, port: 8080, targetPort: 8080 }]
  selector: { app: auth-api-absent }
YAML
applynp k8s/platform/frontend
${K} -n platform set image deployment/frontend frontend=$IMG/frontend:$TAG

echo "== 9. sandbox orchestrator (runc, no pool, capture ON) =="
${K} apply -f k8s/sandbox/sandbox-orchestrator/rbac.yaml
applynp k8s/sandbox/sandbox-orchestrator
${K} -n sandbox set image deployment/sandbox-orchestrator sandbox-orchestrator=$IMG/sandbox-orchestrator:$TAG
# RUNTIME_CLASS-  : no gVisor locally (runc)
# SANDBOX_NODE_POOL- : without it the orchestrator stamps a pool=sandbox nodeSelector
#   on algo pods, which never schedules on this single untainted node.
# ALGO_CPU=2 / ALGO_MEMORY=2Gi: B1 runs TWO contestant slots at once, each with its
#   own capture pod, so per-slot sizing has to leave room for a second one.
${K} -n sandbox set env deployment/sandbox-orchestrator \
  RUNTIME_CLASS- SANDBOX_NODE_POOL- \
  ALGO_CPU=2 ALGO_MEMORY=2Gi \
  CAPTURE_ENABLED=true CAPTURE_IMAGE=$IMG/ebpf-latency:$TAG

echo "== 10. benchmark tier (ingester + rollup, validator, score-computer, controller, worker) =="
applynp k8s/benchmark/telemetry-ingester
${K} -n benchmark set image deployment/telemetry-ingester telemetry-ingester=$IMG/telemetry-ingester:$TAG
${K} -n benchmark set image deployment/telemetry-rollup    telemetry-rollup=$IMG/telemetry-ingester:$TAG
applynp k8s/benchmark/correctness-validator
${K} -n benchmark set image deployment/correctness-validator correctness-validator=$IMG/correctness-validator:$TAG
applynp k8s/benchmark/score-computer
${K} -n benchmark set image deployment/score-computer score-computer=$IMG/score-computer:$TAG
${K} apply -f k8s/benchmark/bot-fleet-controller/deployment.yaml
${K} -n benchmark set image deployment/bot-fleet-controller bot-fleet-controller=$IMG/bot-fleet-controller:$TAG
# READY_DEADLINE=120s: scenarios in a run-group serialize on the single worker, so a
# later scenario's ready fan-in has to outlast the previous run (30s default fails ramp).
${K} -n benchmark set env deployment/bot-fleet-controller \
  SANDBOX_ORCHESTRATOR_URL=http://sandbox-orchestrator.sandbox.svc.cluster.local:8080 \
  MAX_TASKS_PER_WORKER=1000 READY_DEADLINE=120s
applynp k8s/benchmark/bot-fleet
# The manifest pins the worker to the tainted botworker pool. There are no pools on a
# single node, so strip the selector (the toleration is harmless without a taint).
${K} -n benchmark patch deployment/bot-fleet-worker --type=json \
  -p '[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]' >/dev/null 2>&1 || true
${K} -n benchmark set image deployment/bot-fleet-worker bot-fleet-worker=$IMG/bot-fleet:$TAG
# TWO workers, not one. The worker's assignment loop (bot-fleet/src/worker.rs:203)
# awaits run_workload() to completion INLINE before receiving the next message, so a
# single worker executes exactly one session at a time. With replicas=1 two concurrent
# sessions still get distinct slots, captures and leases — but the second one's barrier
# cannot fire until the first one's workload returns, so the LOAD phases serialize and
# B1's concurrency claim is only half-tested. One worker per concurrent session.
${K} -n benchmark scale deployment/bot-fleet-worker --replicas=2

echo "== 11. laptop sizing =="
# Requests are what the scheduler reserves; the EKS numbers assume a dedicated node
# each. Limits are left alone except the worker's (6Gi is sized for 1000 stalled tasks
# at 10k in-flight — unreachable at these rates).
${K} -n benchmark set resources deployment/bot-fleet-worker -c bot-fleet-worker \
  --requests=cpu=500m,memory=512Mi --limits=cpu=4,memory=3Gi >/dev/null
${K} -n benchmark set resources deployment/telemetry-ingester -c telemetry-ingester \
  --requests=cpu=250m,memory=256Mi --limits=cpu=2,memory=1Gi >/dev/null
# The validator replays the full book in pass-1 (B2); 1Gi is the manifest default and
# is the first thing to OOM, so give it headroom without reserving it.
${K} -n benchmark set resources deployment/correctness-validator -c correctness-validator \
  --requests=cpu=250m,memory=256Mi --limits=cpu=2,memory=3Gi >/dev/null
${K} -n benchmark set env deployment/correctness-validator \
  VALIDATION_TIMEOUT_MS=300000 SETTLE_DELAY_MS=15000
for d in platform/submission-api platform/leaderboard-api platform/frontend; do
  ${K} -n ${d%/*} scale deployment/${d#*/} --replicas=1 >/dev/null 2>&1 || true
done

echo ">> waiting for rollouts..."
FAILED=""
for d in platform/submission-api platform/leaderboard-api platform/frontend \
         benchmark/telemetry-ingester benchmark/telemetry-rollup \
         benchmark/correctness-validator benchmark/score-computer \
         benchmark/bot-fleet-controller benchmark/bot-fleet-worker \
         sandbox/sandbox-orchestrator build/spawner \
         observability/grafana observability/prometheus; do
  ns=${d%/*}; name=${d#*/}
  ${K} -n $ns rollout status deployment/$name --timeout=180s || FAILED="$FAILED $d"
done

echo
if [ -n "$FAILED" ]; then
  echo "!! NOT READY:$FAILED"
  echo "!! kubectl -n <ns> describe deploy/<name>   /   kubectl -n <ns> logs deploy/<name>"
else
  echo "=== UP (tag $TAG). Next: deploy-local/forward.sh ==="
fi

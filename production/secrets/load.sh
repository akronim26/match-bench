#!/usr/bin/env bash
# production/secrets/load.sh — create the 13 application Secrets.
#
# Secrets are the ONE thing this path creates imperatively, which is correct:
# they must not live in git. Everything else is declared in k8s/ + overlays/.
#
# Contract, hazards and the upgrade path: production/secrets/schema.md.
#
# Rewritten from deploy-local/create-secrets.sh rather than calling it, so
# production/ has no dependency on deploy-local/ and can outlive it. Three
# behaviours from the original are load-bearing and are preserved deliberately:
#   - kafka-secret is create-once (cluster-id regeneration bricks the broker)
#   - auth-api-secret is create-once AND must exist even with auth off
#   - postgres/timescale passwords are never rotated against an existing volume
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "$ROOT/production/lib/log.sh"
# shellcheck source=../lib/k8s.sh
source "$ROOT/production/lib/k8s.sh"
# shellcheck source=../lib/aws.sh
source "$ROOT/production/lib/aws.sh"
# shellcheck source=../config/production.env
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

# ─────────────────────────────────────────────────────────────────────────────
# THE SEAM. Everything below this function reads variables and does not care
# where they came from. To move to AWS Secrets Manager / External Secrets,
# replace only this function.
#
# Current values are DEVELOPMENT credentials, by decision: auth is out of scope
# for this path, and an obviously-dev secret story is better than a half-built
# one. See schema.md.
# ─────────────────────────────────────────────────────────────────────────────
resolve_secrets() {
  PG_PW="devpass"
  TS_PW="devpass"
  MINIO_AK="minioadmin"
  MINIO_SK="minioadmin123"
  GRAFANA_USER="admin"
  GRAFANA_PW="admin"

  # Auth is disabled in this path; these exist because submission-api reads
  # google-client-id out of auth-api-secret unconditionally and will not start
  # without it. Guarded create-once below, so real credentials are never
  # clobbered once someone sets them.
  GOOGLE_CLIENT_ID="dummy"
  GOOGLE_CLIENT_SECRET="dummy"
  GOOGLE_REDIRECT_URIS="http://localhost:3000"

  # In-cluster DSNs. sslmode=disable is intra-cluster only.
  DB_URL="postgres://iicpc:${PG_PW}@postgres.data.svc.cluster.local:5432/iicpc?sslmode=disable"
  TSDB_URL="postgres://iicpc:${TS_PW}@timescaledb.data.svc.cluster.local:5432/metrics?sslmode=disable"
  KAFKA_BROKERS="kafka.data.svc.cluster.local:9092"
  REDIS_ADDR="redis.data.svc.cluster.local:6379"
  REDIS_URL="redis://redis.data.svc.cluster.local:6379"

  # Where Kaniko pushes contestant images. Derived from the live account so it
  # cannot drift from the cluster being deployed. A wrong value here sends every
  # contestant build to the wrong registry.
  REGISTRY="$(ecr_registry)"
  case "$REGISTRY" in
    *.dkr.ecr.*.amazonaws.com) : ;;
    *) die "could not resolve the ECR registry (got '$REGISTRY') — is AWS_PROFILE=$AWS_PROFILE valid?" ;;
  esac
}

# apply_secret <name> <namespace> <--from-literal...> — idempotent upsert.
apply_secret() {
  local name="$1" ns="$2"; shift 2
  kubectl create secret generic "$name" -n "$ns" "$@" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  ok "secret $ns/$name"
}

# apply_secret_once <name> <namespace> <--from-literal...> — create only if
# absent. For secrets where overwriting is destructive or unrecoverable.
apply_secret_once() {
  local name="$1" ns="$2"
  if kubectl -n "$ns" get secret "$name" >/dev/null 2>&1; then
    dim "secret $ns/$name already exists — left untouched (create-once)"
    return 0
  fi
  apply_secret "$@"
}

main() {
  phase "Secrets"
  need kubectl
  need aws
  require_context "$CLUSTER_NAME"

  # Namespaces must already exist — nothing else creates them, and creating a
  # namespaced Secret into a missing namespace fails. 03-platform.sh applies
  # them before calling this.
  local ns missing=()
  for ns in data platform build benchmark observability; do
    kubectl get ns "$ns" >/dev/null 2>&1 || missing+=("$ns")
  done
  [ ${#missing[@]} -eq 0 ] || die "namespaces missing: ${missing[*]} — apply them before secrets"

  resolve_secrets
  dim "registry: $REGISTRY"

  # data
  # Postgres/Timescale passwords are consumed only at PGDATA init. Rewriting
  # them against an existing volume desyncs every client from the database, so
  # an existing secret wins.
  apply_secret_once postgres-secret    data --from-literal=password="$PG_PW"
  apply_secret_once timescaledb-secret data --from-literal=password="$TS_PW"
  apply_secret      minio-secret       data \
    --from-literal=access-key="$MINIO_AK" --from-literal=secret-key="$MINIO_SK"
  # KRaft cluster identity: regenerating it against an initialised log dir
  # bricks the broker.
  apply_secret_once kafka-secret       data \
    --from-literal=cluster-id="$(kafka_cluster_id)"

  # platform
  apply_secret      submission-api-secret platform \
    --from-literal=database-url="$DB_URL" \
    --from-literal=minio-access-key="$MINIO_AK" \
    --from-literal=minio-secret-key="$MINIO_SK"
  apply_secret_once auth-api-secret       platform \
    --from-literal=google-client-id="$GOOGLE_CLIENT_ID" \
    --from-literal=google-client-secret="$GOOGLE_CLIENT_SECRET" \
    --from-literal=google-allowed-redirect-uris="$GOOGLE_REDIRECT_URIS"
  apply_secret      leaderboard-api-secret platform \
    --from-literal=database-url="$DB_URL" \
    --from-literal=timescale-url="$TSDB_URL" \
    --from-literal=kafka-brokers="$KAFKA_BROKERS" \
    --from-literal=redis-addr="$REDIS_ADDR"

  # build
  apply_secret spawner-secret build \
    --from-literal=database-url="$DB_URL" \
    --from-literal=minio-access-key="$MINIO_AK" \
    --from-literal=minio-secret-key="$MINIO_SK" \
    --from-literal=harbor-staging-endpoint="$REGISTRY" \
    --from-literal=harbor-production-endpoint="$REGISTRY" \
    --from-literal=harbor-user=anon \
    --from-literal=harbor-password=anon

  # benchmark
  apply_secret bot-fleet-controller-secret benchmark --from-literal=database-url="$DB_URL"
  apply_secret correctness-validator-secret benchmark \
    --from-literal=kafka-brokers="$KAFKA_BROKERS" --from-literal=database-url="$DB_URL"
  apply_secret score-computer-secret benchmark \
    --from-literal=database-url="$DB_URL" \
    --from-literal=timescale-url="$TSDB_URL" \
    --from-literal=kafka-brokers="$KAFKA_BROKERS" \
    --from-literal=redis-addr="$REDIS_ADDR"
  # UPPER-CASE keys — the ingester reads them as env names directly.
  apply_secret telemetry-ingester-secrets benchmark \
    --from-literal=KAFKA_BROKERS="$KAFKA_BROKERS" \
    --from-literal=TIMESCALE_URL="$TSDB_URL" \
    --from-literal=REDIS_URL="$REDIS_URL"

  # observability
  apply_secret grafana-admin observability \
    --from-literal=admin-user="$GRAFANA_USER" --from-literal=admin-password="$GRAFANA_PW"

  finish "Secrets"
}

# kafka_cluster_id — a 22-char base64url KRaft cluster id (16 random bytes).
kafka_cluster_id() {
  python3 -c "import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"
}

main "$@"

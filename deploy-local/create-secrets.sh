#!/usr/bin/env bash
# Create the application Secrets every deployment references. Extracted from
# up-dev.sh (2026-08-02) after the first EKS apply crash-looped 20 pods with
# CreateContainerConfigError: the manifests reference these Secrets but nothing
# in the kustomize tree creates them (correctly — secrets don't belong in git);
# locally up-dev.sh made them, on EKS nothing did. Idempotent (apply).
#
# Values are in-cluster DSNs + dev credentials — identical on local k3s and the
# validation-phase EKS cluster (auth-off posture; the contest-day account owns
# real credential management).
#
#   ./deploy-local/create-secrets.sh          # against current kubectl context
set -euo pipefail
K="${K:-kubectl}"

PG_PW=devpass ; TS_PW=devpass
MINIO_AK=minioadmin ; MINIO_SK=minioadmin123
DB="postgres://iicpc:${PG_PW}@postgres.data.svc.cluster.local:5432/iicpc?sslmode=disable"
TSDB="postgres://iicpc:${TS_PW}@timescaledb.data.svc.cluster.local:5432/metrics?sslmode=disable"
KB="kafka.data.svc.cluster.local:9092"
REDIS_ADDR="redis.data.svc.cluster.local:6379"
REDIS_URL="redis://redis.data.svc.cluster.local:6379"
# Registry endpoints for the spawner secret: on EKS the build-worker pushes to
# ECR via IRSA (ecr_aws.go); the harbor-* fields exist because the deployment
# env references them. REG defaults to the ECR registry when resolvable.
REG="${REG:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo local).dkr.ecr.${AWS_REGION:-us-east-1}.amazonaws.com}"

mk() { ${K} create secret generic "$1" -n "$2" "${@:3}" --dry-run=client -o yaml | ${K} apply -f - ; }

mk postgres-secret data --from-literal=password=$PG_PW
mk timescaledb-secret data --from-literal=password=$TS_PW
mk minio-secret data --from-literal=access-key=$MINIO_AK --from-literal=secret-key=$MINIO_SK
KID=$(python3 -c "import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes+uuid.uuid4().bytes[:4]).decode().rstrip('='))")
${K} -n data get secret kafka-secret >/dev/null 2>&1 || mk kafka-secret data --from-literal=cluster-id=$KID
mk submission-api-secret platform --from-literal=database-url="$DB" --from-literal=minio-access-key=$MINIO_AK --from-literal=minio-secret-key=$MINIO_SK
${K} -n platform get secret auth-api-secret >/dev/null 2>&1 \
  || mk auth-api-secret platform --from-literal=google-client-id=dummy \
       --from-literal=google-client-secret=dummy \
       --from-literal=google-allowed-redirect-uris=http://localhost:3000
mk leaderboard-api-secret platform --from-literal=database-url="$DB" --from-literal=timescale-url="$TSDB" --from-literal=kafka-brokers="$KB" --from-literal=redis-addr="$REDIS_ADDR"
mk spawner-secret build --from-literal=database-url="$DB" --from-literal=minio-access-key=$MINIO_AK --from-literal=minio-secret-key=$MINIO_SK --from-literal=harbor-staging-endpoint="$REG" --from-literal=harbor-production-endpoint="$REG" --from-literal=harbor-user=anon --from-literal=harbor-password=anon
mk bot-fleet-controller-secret benchmark --from-literal=database-url="$DB"
mk correctness-validator-secret benchmark --from-literal=kafka-brokers="$KB" --from-literal=database-url="$DB"
mk score-computer-secret benchmark --from-literal=database-url="$DB" --from-literal=timescale-url="$TSDB" --from-literal=kafka-brokers="$KB" --from-literal=redis-addr="$REDIS_ADDR"
mk telemetry-ingester-secrets benchmark --from-literal=KAFKA_BROKERS="$KB" --from-literal=TIMESCALE_URL="$TSDB" --from-literal=REDIS_URL="$REDIS_URL"
mk grafana-admin observability --from-literal=admin-user=admin --from-literal=admin-password=admin

echo "== secrets created/updated =="

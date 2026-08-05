#!/usr/bin/env bash
# production/01-cluster.sh — provision the cluster and point kubectl at it.
#
# Billing starts here (~$2/hour for the baseline shape).
#
# The `-var enable_spawner_irsa=false` override is REQUIRED on a fresh cluster
# and is not optional tidiness: infra/terraform-v2/tfvars/contest.tfvars sets it
# true (correct for every later apply), but kubernetes_annotations PATCHES an
# existing object and has no depends_on, and the build-spawner ServiceAccount is
# created by the platform manifests — which are applied two phases later. On a
# fresh cluster the first apply would fail with "ServiceAccount not found".
# 04-irsa.sh runs the second apply, without the override.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/k8s.sh"
source "$ROOT/production/lib/aws.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

main() {
  phase "Cluster"
  need terraform; need aws; need kubectl

  step "terraform init"
  terraform -chdir="$ROOT/$TF_DIR" init -backend-config=backend.hcl -input=false >/dev/null \
    || die "terraform init failed"
  ok "backend initialised"

  step "adopt an orphaned results bucket"
  # The results bucket carries prevent_destroy, so teardown cannot destroy it and
  # instead removes it from STATE (99-teardown.sh). The bucket therefore survives
  # in AWS while terraform forgets it — and the next apply tries to CREATE it and
  # fails with BucketAlreadyExists, because S3 bucket names are globally unique.
  #
  # That makes the bring-up non-idempotent against its own teardown, which is a
  # bug in the pair rather than in either half. Re-adopt it before apply. Import
  # rather than delete: the bucket is the one place run results are exported to,
  # and destroying it to satisfy a create would be exactly backwards.
  #
  # Only the bucket needs this. The versioning/public-access/lifecycle resources
  # are PUT operations against an existing bucket, so terraform creates them
  # cleanly whether or not the bucket predates this run.
  local bucket tf_state
  bucket="$(terraform -chdir="$ROOT/$TF_DIR" output -raw results_bucket 2>/dev/null || true)"
  [ -n "$bucket" ] || bucket="${CLUSTER_NAME}-results-$(aws sts get-caller-identity --query Account --output text 2>/dev/null)"
  # Capture first, then match with a here-string. NOT `state list | grep -q`:
  # grep -q exits on the first match, terraform then dies of SIGPIPE, and
  # `set -o pipefail` propagates that failure as the PIPELINE's status — so the
  # condition reads false even when the match succeeded, and this branch tried
  # to re-import an already-imported bucket.
  tf_state="$(terraform -chdir="$ROOT/$TF_DIR" state list 2>/dev/null || true)"
  if grep -qx 'aws_s3_bucket.results' <<<"$tf_state"; then
    dim "already tracked in state"
  elif aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    if terraform -chdir="$ROOT/$TF_DIR" import \
         -var-file="$TFVARS" -var enable_spawner_irsa=false \
         aws_s3_bucket.results "$bucket" >/dev/null 2>&1; then
      ok "imported orphaned bucket $bucket"
    else
      die "bucket $bucket exists but could not be imported — import it by hand, or delete it if it is empty"
    fi
  else
    dim "no pre-existing results bucket; terraform will create it"
  fi

  step "terraform apply (irsa deferred to 04)"
  info "this takes ~20 minutes and starts billing"
  terraform -chdir="$ROOT/$TF_DIR" apply \
    -var-file="$TFVARS" \
    -var enable_spawner_irsa=false \
    -input=false -auto-approve \
    || die "terraform apply failed"
  ok "cluster provisioned"

  step "kubeconfig"
  aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION" >/dev/null \
    || die "update-kubeconfig failed for $CLUSTER_NAME"
  require_context "$CLUSTER_NAME"

  step "node shape"
  # Asserted here rather than trusted: a node group that silently came up short
  # produces failures three phases later that look like application bugs.
  local pool want have total=0
  for pool in general kafka sandbox botworker; do
    case "$pool" in
      general)   want="$EXPECT_GENERAL_NODES" ;;
      kafka)     want="$EXPECT_KAFKA_NODES" ;;
      sandbox)   want="$EXPECT_SANDBOX_NODES" ;;
      botworker) want="$EXPECT_BOTWORKER_NODES" ;;
    esac
    have="$(nodes_in_pool "$pool")"
    total=$(( total + have ))
    if [ "$have" -eq "$want" ]; then
      ok "$pool: $have node(s)"
    else
      fail "$pool: $have node(s), expected $want"
    fi
  done
  info "total nodes: $total"

  step "node readiness"
  local notready
  notready="$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 != "Ready" {print $1}' || true)"
  if [ -n "$notready" ]; then
    fail "nodes not Ready: $(echo "$notready" | tr '\n' ' ')"
  else
    ok "all nodes Ready"
  fi

  step "terraform-managed in-cluster addons"
  # These come from terraform, not from the overlay. The gp3 StorageClass in
  # particular is a hard dependency of the next phase: every data PVC binds
  # through it, so a missing class leaves Kafka/Postgres/MinIO Pending forever.
  if kubectl get storageclass gp3 >/dev/null 2>&1; then
    ok "gp3 StorageClass"
  else
    fail "gp3 StorageClass missing — the data-plane PVCs cannot bind without it"
  fi

  finish "Cluster"
}

main "$@"

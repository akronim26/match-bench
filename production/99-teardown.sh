#!/usr/bin/env bash
# production/99-teardown.sh — destroy everything, then prove nothing is billing.
#
# Two steps exist purely because skipping them leaves billable resources running
# while `terraform destroy` reports success:
#
#   1. Helm releases in state HANG destroy — KEDA's uninstall times out against
#      a dying API server, and terraform still exits 0 with your nodes alive.
#   2. The results bucket carries prevent_destroy, so destroy FAILS until it is
#      released from state.
#
# And two classes of resource terraform does not own at all: PVC-backed EBS
# volumes (~$11/month if forgotten) and the per-submission ECR repos that
# build-worker creates at runtime.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/aws.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

CONFIRM="${CONFIRM:-}"

main() {
  phase "Teardown"
  need terraform; need aws

  if [ "$CONFIRM" != "yes" ]; then
    warn "This DESTROYS the '$CLUSTER_NAME' cluster and deletes its EBS volumes."
    warn "Anything not exported is lost — scores and telemetry live in Postgres/Timescale,"
    warn "which die with the cluster."
    die "refusing to run without CONFIRM=yes"
  fi

  step "release helm releases from state"
  # Not deleting them — removing them from state so destroy does not wait on an
  # uninstall that cannot complete against an API server being torn down.
  local releases
  releases="$(terraform -chdir="$ROOT/$TF_DIR" state list 2>/dev/null | grep 'helm_release' || true)"
  if [ -n "$releases" ]; then
    printf '%s\n' "$releases" | while read -r r; do
      terraform -chdir="$ROOT/$TF_DIR" state rm "$r" >/dev/null 2>&1 \
        && ok "state rm $r" || warn "could not state rm $r"
    done
  else
    dim "no helm releases in state"
  fi

  step "release the results bucket from state"
  # prevent_destroy is a deliberate safety gate on the one bucket that can hold
  # exported results. Releasing it from state leaves the BUCKET in place.
  #
  # Consequence, handled on the other side: the bucket then exists in AWS while
  # terraform has forgotten it, so the NEXT apply would try to create it and fail
  # with BucketAlreadyExists (S3 names are globally unique). 01-cluster.sh
  # re-imports it before applying, which is what keeps bring-up idempotent
  # against this teardown. Do not "fix" that by deleting the bucket here —
  # protecting it is the entire point of prevent_destroy.
  local b
  for b in aws_s3_bucket.results aws_s3_bucket_versioning.results \
           aws_s3_bucket_public_access_block.results \
           aws_s3_bucket_lifecycle_configuration.results; do
    terraform -chdir="$ROOT/$TF_DIR" state rm "$b" >/dev/null 2>&1 \
      && ok "state rm $b" || dim "$b not in state"
  done

  step "terraform destroy"
  terraform -chdir="$ROOT/$TF_DIR" destroy \
    -var-file="$TFVARS" -input=false -auto-approve \
    || die "terraform destroy failed — resolve, then re-run"
  ok "destroyed"

  step "per-submission ECR repos"
  local repos n=0 r
  repos="$(per_submission_repos)"
  if [ -n "$repos" ]; then
    for r in $repos; do
      aws ecr delete-repository --repository-name "$r" --force --region "$AWS_REGION" >/dev/null 2>&1 \
        && { ok "deleted $r"; n=$(( n + 1 )); } || warn "could not delete $r"
    done
    info "removed $n runtime-created repo(s)"
  else
    dim "none"
  fi

  step "orphaned EBS volumes"
  # Tag-filtered on purpose. The account-wide "every available volume" form
  # would also match volumes belonging to anything else in this region.
  local vols v
  vols="$(orphan_volumes)"
  if [ -n "$vols" ]; then
    for v in $vols; do
      aws ec2 delete-volume --volume-id "$v" --region "$AWS_REGION" >/dev/null 2>&1 \
        && ok "deleted $v" || warn "could not delete $v"
    done
  else
    dim "no volumes tagged to $CLUSTER_NAME"
  fi
  # PVCs provisioned through the gp3 class sometimes carry only CSIVolumeName,
  # so anything left is surfaced for a human rather than deleted automatically.
  local leftover
  leftover="$(aws ec2 describe-volumes --region "$AWS_REGION" \
    --filters Name=status,Values=available --query 'length(Volumes)' --output text 2>/dev/null || echo 0)"
  if [ "${leftover:-0}" -gt 0 ]; then
    warn "$leftover available volume(s) remain, untagged to this cluster — inspect before deleting:"
    untagged_available_volumes | sed 's/^/      /'
  fi

  step "final sweep"
  # Extended beyond the six originally documented: idle Elastic IPs bill, and
  # snapshots persist silently.
  local name q v2 failed=0
  while IFS='|' read -r name q; do
    v2="$(eval "$q" 2>/dev/null || echo '?')"
    if [ "$v2" = "0" ]; then
      ok "$name: 0"
    else
      fail "$name: $v2 (expected 0)"
      failed=1
    fi
  done <<EOF
eks clusters|aws eks list-clusters --region $AWS_REGION --query 'length(clusters)' --output text
running instances|aws ec2 describe-instances --region $AWS_REGION --filters Name=instance-state-name,Values=running,pending --query 'length(Reservations[].Instances[])' --output text
ebs volumes|aws ec2 describe-volumes --region $AWS_REGION --query 'length(Volumes)' --output text
nat gateways|aws ec2 describe-nat-gateways --region $AWS_REGION --filter Name=state,Values=available --query 'length(NatGateways)' --output text
ecr repositories|aws ecr describe-repositories --region $AWS_REGION --query 'length(repositories)' --output text
load balancers|aws elbv2 describe-load-balancers --region $AWS_REGION --query 'length(LoadBalancers)' --output text
idle elastic ips|aws ec2 describe-addresses --region $AWS_REGION --query 'length(Addresses[?AssociationId==null])' --output text
ebs snapshots|aws ec2 describe-snapshots --region $AWS_REGION --owner-ids self --query 'length(Snapshots)' --output text
EOF

  step "port-forwards"
  local pf
  pf="$(pgrep -af 'kubectl.*port-forward' 2>/dev/null | grep -c . || true)"
  [ "${pf:-0}" -eq 0 ] && ok "none running" \
    || warn "$pf port-forward(s) still running — kill them so no stale tunnel outlives the cluster"

  dim "kept by design: the terraform state bucket, its DynamoDB lock table, and the results bucket"
  finish "Teardown"
}

main "$@"

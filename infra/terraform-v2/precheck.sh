#!/usr/bin/env bash
# infra/terraform-v2/precheck.sh — fail BEFORE apply, not mid-nodegroup.
#
# Validates the traps that stalled previous bring-ups:
#   1. Standard on-demand vCPU quota (L-1216C47A) — covers A,C,D,H,I,M,R,T,Z,
#      which INCLUDES Graviton c7g (family letter decides the bucket, not the
#      processor — there is NO separate Graviton quota; L-DB2E81BA "G and VT"
#      is GPU graphics instances and irrelevant here). Baseline need: 40
#      (32 x86 + 2 arm botworkers), default 48 with headroom. Contest-day
#      (DIFFERENT account/profile): 56 x86 + 64 arm = 120 -> run STD_NEED=128.
#   2. AWS credentials + region sanity.
#   3. Instance-type availability in the region.
#
#   ./precheck.sh [region]
set -euo pipefail
REGION="${1:-us-east-1}"
STD_NEED="${STD_NEED:-48}"

fail=0

echo "== identity =="
aws sts get-caller-identity --output table --region "$REGION" || { echo "!! no AWS credentials"; exit 1; }

quota() { # quota <code> — current value or 0
  aws service-quotas get-service-quota \
    --service-code ec2 --quota-code "$1" \
    --region "$REGION" --query 'Quota.Value' --output text 2>/dev/null || echo 0
}

echo "== quotas =="
std=$(quota L-1216C47A)   # Running On-Demand Standard (A,C,D,H,I,M,R,T,Z) — incl. c7g
printf "   standard on-demand vCPU (x86 AND Graviton): %.0f (need >= %s)\n" "$std" "$STD_NEED"
awk -v have="$std" -v need="$STD_NEED" 'BEGIN{exit !(have+0 < need+0)}' && {
  echo "!! Standard quota too low — request an increase (Service Quotas -> EC2 -> L-1216C47A)"; fail=1; }

echo "== instance type availability in $REGION =="
for t in m6i.2xlarge m6i.xlarge c6i.2xlarge c7g.xlarge; do
  n=$(aws ec2 describe-instance-type-offerings --region "$REGION" \
      --filters "Name=instance-type,Values=$t" \
      --query 'length(InstanceTypeOfferings)' --output text 2>/dev/null || echo 0)
  if [ "${n:-0}" -ge 1 ]; then echo "   $t: available"; else echo "!! $t: NOT offered in $REGION"; fail=1; fi
done

if [ "$fail" -ne 0 ]; then
  echo
  echo "############ PRECHECK FAILED — fix the above before terraform apply ############"
  exit 1
fi
echo
echo "############ PRECHECK OK ############"

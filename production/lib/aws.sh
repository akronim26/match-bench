# production/lib/aws.sh — AWS queries used by preflight, images and teardown.
#
# Every call passes --region explicitly and relies on AWS_PROFILE from
# production.env. Nothing here creates or deletes: destructive AWS actions live
# in 99-teardown.sh where they can be read in one place.

aws_account_id() {
  aws sts get-caller-identity --query Account --output text --region "$AWS_REGION" 2>/dev/null
}

ecr_registry() {
  printf '%s.dkr.ecr.%s.amazonaws.com' "$(aws_account_id)" "$AWS_REGION"
}

ecr_login() {
  aws ecr get-login-password --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "$(ecr_registry)" >/dev/null 2>&1
}

# ecr_tag_exists <repo> <tag> — ECR repos are IMMUTABLE, so pushing an existing
# tag FAILS. Checking first turns that into a clear message instead of a partial
# push that leaves some repos on the new tag and some on the old.
ecr_tag_exists() {
  aws ecr describe-images --repository-name "$1" --image-ids "imageTag=$2" \
    --region "$AWS_REGION" >/dev/null 2>&1
}

# standard_vcpu_quota — L-1216C47A covers A,C,D,H,I,M,R,T,Z, which includes
# Graviton c7g. There is NO separate Graviton quota; L-DB2E81BA ("G and VT") is
# GPU instances and requesting it is a wasted support ticket.
standard_vcpu_quota() {
  aws service-quotas get-service-quota \
    --service-code ec2 --quota-code L-1216C47A \
    --region "$AWS_REGION" --query 'Quota.Value' --output text 2>/dev/null || printf '0'
}

instance_type_offered() {
  aws ec2 describe-instance-type-offerings --region "$AWS_REGION" \
    --filters "Name=instance-type,Values=$1" \
    --query 'length(InstanceTypeOfferings)' --output text 2>/dev/null || printf '0'
}

# ── teardown sweep helpers (read-only; the deletes live in 99-teardown.sh) ────

# orphan_volumes — PVC-backed EBS volumes survive the cluster (~$11/month if
# forgotten). Filtered by cluster tag rather than "every available volume in the
# region", which would also match anything unrelated in the account.
orphan_volumes() {
  aws ec2 describe-volumes --region "$AWS_REGION" \
    --filters Name=status,Values=available \
              "Name=tag:kubernetes.io/cluster/$CLUSTER_NAME,Values=owned" \
    --query 'Volumes[].VolumeId' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$' || true
}

# untagged_available_volumes — the fallback list, for the case where the tagged
# query is empty but volumes remain. PVCs provisioned through the gp3 class
# sometimes carry only CSIVolumeName. Printed for a human to inspect; never
# deleted automatically.
untagged_available_volumes() {
  aws ec2 describe-volumes --region "$AWS_REGION" \
    --filters Name=status,Values=available \
    --query 'Volumes[].{id:VolumeId,size:Size,az:AvailabilityZone,pvc:Tags[?Key==`kubernetes.io/created-for/pvc/name`]|[0].Value}' \
    --output table 2>/dev/null || true
}

# per_submission_repos — build-worker creates one ECR repo per submission at
# RUNTIME, so terraform does not own them and destroy leaves them behind.
per_submission_repos() {
  aws ecr describe-repositories --region "$AWS_REGION" \
    --query 'repositories[].repositoryName' --output text 2>/dev/null \
    | tr '\t' '\n' | grep -E 'iicpc/[0-9a-f-]{20,}' || true
}

# infra/terraform-v2/backend.tf — REMOTE state, non-negotiable.
#
# v1 kept terraform.tfstate on a laptop next to the source tree; for an
# ephemeral apply/destroy cluster that is how orphaned clusters happen. State
# lives in S3 with a DynamoDB lock. Backend blocks cannot interpolate
# variables, so the concrete values come from a partial-config file:
#
#   terraform init -backend-config=backend.hcl
#
# Copy backend.hcl.example -> backend.hcl (gitignored) and fill in the bucket
# you created once, out-of-band:
#   aws s3 mb s3://<your-tf-state-bucket> --region us-east-1
#   aws dynamodb create-table --table-name iicpc-tf-lock \
#     --attribute-definitions AttributeName=LockID,AttributeType=S \
#     --key-schema AttributeName=LockID,KeyType=HASH \
#     --billing-mode PAY_PER_REQUEST --region us-east-1

terraform {
  backend "s3" {}
}

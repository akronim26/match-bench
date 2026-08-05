# infra/terraform-v2/export.tf — the results bucket: the ONE thing that
# outlives the cluster. Ephemeral posture means Postgres/Timescale/leaderboard
# die with `terraform destroy`; the export Job (k8s manifest, IRSA role in
# irsa.tf) dumps them here first, and the runbook gates destroy on the export's
# completion marker object.
#
# prevent_destroy: `terraform destroy` intentionally FAILS while this bucket is
# in state. Destroy flow: run the export job, verify the marker, then
#   terraform state rm aws_s3_bucket.results   (bucket survives, unmanaged)
#   terraform destroy

locals {
  results_bucket = var.results_bucket_name != "" ? var.results_bucket_name : "${var.cluster_name}-results-${local.account_id}"
}

resource "aws_s3_bucket" "results" {
  bucket = local.results_bucket

  lifecycle {
    prevent_destroy = true
  }

  tags = var.tags
}

resource "aws_s3_bucket_versioning" "results" {
  bucket = aws_s3_bucket.results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket = aws_s3_bucket.results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

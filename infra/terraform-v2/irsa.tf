# infra/terraform-v2/irsa.tf — IAM roles for service accounts: EBS CSI, ALB
# controller, cluster-autoscaler, build-spawner (ECR push), results-export
# (S3 write — the destroy gate's writer).

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name             = "${var.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }

  tags = var.tags
}

module "alb_controller_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name                              = "${var.cluster_name}-alb-controller"
  attach_load_balancer_controller_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-load-balancer-controller"]
    }
  }

  tags = var.tags
}

module "cluster_autoscaler_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name                        = "${var.cluster_name}-cluster-autoscaler"
  attach_cluster_autoscaler_policy = true
  cluster_autoscaler_cluster_names = [var.cluster_name]

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:cluster-autoscaler"]
    }
  }

  tags = var.tags
}

# ── build-spawner: push contestant images to ECR ─────────────────────────────

data "aws_iam_policy_document" "spawner_ecr" {
  statement {
    sid       = "EcrAuth"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "EcrManageRepos"
    effect = "Allow"
    actions = [
      "ecr:CreateRepository",
      "ecr:DescribeRepositories",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EcrPush"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]
    resources = ["arn:aws:ecr:${var.region}:${local.account_id}:repository/iicpc/*"]
  }
}

resource "aws_iam_policy" "spawner_ecr" {
  name        = "${var.cluster_name}-spawner-ecr"
  description = "build-spawner: ECR auth + create/describe + push to iicpc/*"
  policy      = data.aws_iam_policy_document.spawner_ecr.json
  tags        = var.tags
}

module "spawner_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name = "${var.cluster_name}-build-spawner"
  role_policy_arns = {
    ecr = aws_iam_policy.spawner_ecr.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["build:build-spawner"]
    }
  }

  tags = var.tags
}

# SA annotation is gated exactly as in v1: the SA is created by the platform
# manifests, applied AFTER terraform. Set enable_spawner_irsa=true and re-apply
# once it exists, or annotate by hand (see v1 comment).
resource "kubernetes_annotations" "spawner_sa_irsa" {
  count = var.enable_spawner_irsa ? 1 : 0

  api_version = "v1"
  kind        = "ServiceAccount"
  metadata {
    name      = "build-spawner"
    namespace = "build"
  }
  annotations = {
    "eks.amazonaws.com/role-arn" = module.spawner_irsa.iam_role_arn
  }
  force = true
}

# ── results-export: the ONLY writer of the results bucket ────────────────────

data "aws_iam_policy_document" "results_export" {
  statement {
    sid    = "WriteResults"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.results.arn,
      "${aws_s3_bucket.results.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "results_export" {
  name        = "${var.cluster_name}-results-export"
  description = "results-export job: write DB dumps + leaderboard snapshots before destroy"
  policy      = data.aws_iam_policy_document.results_export.json
  tags        = var.tags
}

module "results_export_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = ">= 5.39.0, < 6.0.0"

  role_name = "${var.cluster_name}-results-export"
  role_policy_arns = {
    s3 = aws_iam_policy.results_export.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["platform:results-export"]
    }
  }

  tags = var.tags
}

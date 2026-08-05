# infra/terraform-v2/ecr.tf — per-service ECR repositories, IMMUTABLE tags.
# The overlays pin images by digest; immutability makes "did this pod get my
# code?" answerable by construction. bot-fleet's repo holds a multi-arch
# manifest list (amd64 + arm64) — same repo, arch resolved at pull.

resource "aws_ecr_repository" "service" {
  for_each = toset(var.service_images)

  name                 = "iicpc/${each.value}"
  image_tag_mutability = var.ecr_image_tag_mutability
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = var.tags
}

resource "aws_ecr_lifecycle_policy" "service" {
  for_each = aws_ecr_repository.service

  repository = each.value.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 14 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 14
        }
        action = { type = "expire" }
      }
    ]
  })
}

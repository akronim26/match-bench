# infra/terraform-v2/outputs.tf

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "ecr_repository_urls" {
  value = { for k, r in aws_ecr_repository.service : k => r.repository_url }
}

output "results_bucket" {
  value = aws_s3_bucket.results.bucket
}

output "results_export_role_arn" {
  value = module.results_export_irsa.iam_role_arn
}

output "spawner_role_arn" {
  value = module.spawner_irsa.iam_role_arn
}

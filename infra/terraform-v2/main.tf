# infra/terraform-v2/main.tf — contest-tier EKS cluster.
#
# Design: docs/eks-contest-deployment.md (2026-08-02). Four pools:
#   general   x2  m6i.2xlarge  — DBs, ingester, validator burst (4x2 CPU), APIs,
#                                observability. RAM slack = DB page cache, deliberate.
#   kafka     x2  m6i.xlarge   — one KRaft broker per node (anti-affinity), RF=1.
#   sandbox   1-4 c6i.2xlarge  — ONE contestant slot per node (algo 4CPU/8Gi
#                                Guaranteed + capture 2/4 CPU, 2/4Gi). x86: the BPF
#                                object is verifier-proven on x86_64/AL2023 kernel 6.1.
#                                sandbox_desired is the contest-day dial (1 -> 4).
#   botworker 2+  c7g.xlarge   — Graviton arm64, ONE worker pod per node (pod requests
#                                3 CPU of ~3.6 allocatable + podAntiAffinity in the
#                                manifest). KEDA scales pods; cluster-autoscaler
#                                (autoscaler tags below) follows with nodes.
#                                botworker_max_size comes from measurement M1.
#
# Jumbo frames: MTU stays at the EKS default 9001 EVERYWHERE — CAPTURE_CAP=9029
# (2026-08-01) removed the platform-wide 1500 clamp. There is no net-tune
# initContainer anymore; the gro-disable DaemonSet (k8s/sandbox) is still REQUIRED
# on sandbox nodes and is applied with the manifests, not here.

provider "aws" {
  region = var.region
  default_tags {
    tags = var.tags
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = var.account_id != "" ? var.account_id : data.aws_caller_identity.current.account_id
  azs        = slice(data.aws_availability_zones.available.names, 0, var.az_count)
}

data "aws_availability_zones" "available" {
  state = "available"
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = ">= 5.8.0, < 6.0.0"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  # Single NAT: ~$0.045/h + per-GB. Acceptable for an ephemeral cluster; ECR
  # pulls go through it, so the image pipeline should prefer few large pushes.
  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }

  tags = var.tags
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = ">= 20.24.0, < 21.0.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  enable_irsa = true

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  enable_cluster_creator_admin_permissions = true

  cluster_addons = {
    vpc-cni = {
      most_recent    = true
      before_compute = true
      configuration_values = jsonencode({
        # NetworkPolicy enforcement is a HARD gate: gVisor is off, so netpols
        # are the isolation boundary. Runbook step 5 verifies a denied probe.
        enableNetworkPolicy = "true"
        env = {
          ENABLE_PREFIX_DELEGATION = "true"
        }
      })
    }
    kube-proxy = {
      most_recent = true
    }
    coredns = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  eks_managed_node_groups = {
    general = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.general_instance_type]
      min_size       = var.general_min_size
      max_size       = var.general_max_size
      desired_size   = var.general_desired_size

      labels = {
        role = "general"
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.general_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }

    kafka = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.kafka_instance_type]
      min_size       = var.kafka_min_size
      max_size       = var.kafka_max_size
      desired_size   = var.kafka_desired_size

      labels = {
        pool = "kafka"
      }

      taints = {
        kafka = {
          key    = "kafka"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.kafka_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }

    sandbox = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.sandbox_instance_type]
      min_size       = var.sandbox_min_size
      max_size       = var.sandbox_max_size
      desired_size   = var.sandbox_desired_size

      labels = {
        pool = "sandbox"
      }

      taints = {
        sandbox = {
          key    = "sandbox"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.sandbox_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }

    botworker = {
      # Graviton: arm64 AMI. bot-fleet is the ONLY workload here and ships a
      # multi-arch image (docs/eks-contest-deployment.md 5b). No BPF on these
      # nodes.
      ami_type       = "AL2023_ARM_64_STANDARD"
      instance_types = [var.botworker_instance_type]
      min_size       = var.botworker_min_size
      max_size       = var.botworker_max_size
      desired_size   = var.botworker_desired_size

      labels = {
        pool = "botworker"
      }

      taints = {
        botworker = {
          key    = "botworker"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      # Cluster-autoscaler discovery: KEDA scales worker PODS on Kafka lag; a
      # pending pod (1-pod-per-node model) makes the autoscaler add a node
      # here. Only this group is autoscaled.
      tags = {
        "k8s.io/cluster-autoscaler/enabled"             = "true"
        "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.botworker_disk_size
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }
    }
  }

  tags = var.tags
}

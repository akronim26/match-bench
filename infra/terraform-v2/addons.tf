# infra/terraform-v2/addons.tf — cluster addons: gp3 default StorageClass, KEDA
# (pods: bot workers 2..50, validators 1..4), metrics-server, cluster-autoscaler
# (nodes: botworker group only), ALB controller (frontend ingress).
#
# Deliberately ABSENT vs v1: gVisor (decided off — netpols are the boundary and
# runsc lacks io_uring), and any privileged net-tune machinery (obsolete since
# CAPTURE_CAP=9029 restored jumbo frames).

resource "kubernetes_storage_class" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }
  storage_provisioner    = "ebs.csi.aws.com"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true
  # Delete, not Retain: ephemeral cluster — results leave via the export job,
  # never via orphaned EBS volumes that outlive a destroy and keep billing.
  reclaim_policy = "Delete"
  parameters = {
    type       = "gp3"
    fsType     = "ext4"
    iops       = "3000"
    throughput = "125"
  }

  depends_on = [module.eks]
}

resource "helm_release" "keda" {
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = "2.14.0"
  namespace        = "keda"
  create_namespace = true

  depends_on = [module.eks]
}

resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  version    = "3.12.1"
  namespace  = "kube-system"

  depends_on = [module.eks]
}

resource "helm_release" "cluster_autoscaler" {
  name       = "cluster-autoscaler"
  repository = "https://kubernetes.github.io/autoscaler"
  chart      = "cluster-autoscaler"
  version    = "9.37.0"
  namespace  = "kube-system"

  set {
    name  = "autoDiscovery.clusterName"
    value = var.cluster_name
  }
  set {
    name  = "awsRegion"
    value = var.region
  }
  set {
    name  = "rbac.serviceAccount.name"
    value = "cluster-autoscaler"
  }
  set {
    name  = "rbac.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = module.cluster_autoscaler_irsa.iam_role_arn
  }
  # Scale-down conservatively: a botworker node drained mid-run kills that
  # run's workers. 10m unneeded + utilization threshold keeps nodes through
  # inter-scenario gaps of a sequential group.
  set {
    name  = "extraArgs.scale-down-unneeded-time"
    value = "10m"
  }
  set {
    name  = "extraArgs.balance-similar-node-groups"
    value = "false"
  }

  depends_on = [module.eks, module.cluster_autoscaler_irsa]
}

resource "helm_release" "alb_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  version    = "1.8.1"
  namespace  = "kube-system"

  set {
    name  = "clusterName"
    value = var.cluster_name
  }
  set {
    name  = "region"
    value = var.region
  }
  set {
    name  = "vpcId"
    value = module.vpc.vpc_id
  }
  set {
    name  = "serviceAccount.create"
    value = "true"
  }
  set {
    name  = "serviceAccount.name"
    value = "aws-load-balancer-controller"
  }
  set {
    name  = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = module.alb_controller_irsa.iam_role_arn
  }

  depends_on = [module.eks, module.alb_controller_irsa]
}

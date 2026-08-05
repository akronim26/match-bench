# infra/terraform-v2/variables.tf — every knob of the contest-tier design.
# Values live in tfvars/contest.tfvars (baseline) and tfvars/contest-day.tfvars
# (sandbox 1 -> 4). Defaults here ARE the contest baseline so a bare plan is sane.

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "account_id" {
  description = "AWS account id; empty = derive from the caller identity."
  type        = string
  default     = ""
}

variable "cluster_name" {
  type    = string
  default = "iicpc-contest"
}

variable "kubernetes_version" {
  type    = string
  default = "1.30"
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "az_count" {
  type    = number
  default = 2
}

variable "tags" {
  type = map(string)
  default = {
    project = "iicpc"
    tier    = "contest"
  }
}

# ── general: DBs + ingester + validator burst + APIs + observability ─────────
variable "general_instance_type" {
  type    = string
  default = "m6i.2xlarge"
}
variable "general_min_size" {
  type    = number
  default = 2
}
variable "general_max_size" {
  type    = number
  default = 2
}
variable "general_desired_size" {
  type    = number
  default = 2
}
variable "general_disk_size" {
  type    = number
  default = 80
}

# ── kafka: dedicated brokers, one per node ───────────────────────────────────
variable "kafka_instance_type" {
  type    = string
  default = "m6i.xlarge"
}
variable "kafka_min_size" {
  type    = number
  default = 1
}
variable "kafka_max_size" {
  description = "Headroom for a 2nd broker if M3 shows one is not enough; raising this alone changes nothing until the overlay's broker patch and quorum voters follow."
  type        = number
  default     = 2
}
variable "kafka_desired_size" {
  description = "1 (2026-08-03): the eks-contest overlay runs a SINGLE broker, so a 2nd node sat idle at ~$0.19/h. Node count and broker count must move together — raise this only alongside patch-kafka-single-broker.yaml (replicas + quorum voters + internal-topic RFs). M3 decides whether one broker holds at the ramp target."
  type        = number
  default     = 1
}
variable "kafka_disk_size" {
  type    = number
  default = 120
}

# ── sandbox: one contestant slot per node; desired is the contest-day dial ───
variable "sandbox_instance_type" {
  type    = string
  default = "c6i.2xlarge"
}
variable "sandbox_min_size" {
  type    = number
  default = 1
}
variable "sandbox_max_size" {
  type    = number
  default = 4
}
variable "sandbox_desired_size" {
  type    = number
  default = 1
}
variable "sandbox_disk_size" {
  type    = number
  default = 60
}

# ── botworker: Graviton, 1 worker pod per node, autoscaled ───────────────────
variable "botworker_instance_type" {
  type    = string
  default = "c7g.xlarge"
}
variable "botworker_min_size" {
  type    = number
  default = 2
}
variable "botworker_max_size" {
  description = "MEASUREMENT OUTPUT (M1): ceil(worst-case aggregate / per-pod TPS) + 1. 8 is a pre-measurement placeholder ceiling for the autoscaler, NOT a sizing claim."
  type        = number
  default     = 8
}
variable "botworker_desired_size" {
  type    = number
  default = 2
}
variable "botworker_disk_size" {
  type    = number
  default = 40
}

# ── ECR ──────────────────────────────────────────────────────────────────────
variable "service_images" {
  description = "ECR repositories to create under iicpc/. bot-fleet is multi-arch (amd64+arm64)."
  type        = list(string)
  default = [
    "submission-api",
    "auth-api",
    "leaderboard-api",
    "frontend",
    "spawner",
    "sandbox-orchestrator",
    "bot-fleet-controller",
    "bot-fleet",
    "correctness-validator",
    "score-computer",
    "telemetry-ingester",
    "ebpf-latency",
    # Measurement fixtures (platform tooling, not contestants — contestant
    # repos are created by build-worker from real submissions):
    "drain-sink",
    "stall-sink",
  ]
}

variable "ecr_image_tag_mutability" {
  description = "IMMUTABLE: digests/tags may never be silently replaced — the overlays pin by digest."
  type        = string
  default     = "IMMUTABLE"
}

# ── results export (destroy gate) ────────────────────────────────────────────
variable "results_bucket_name" {
  description = "S3 bucket for the pre-destroy results export. Empty = <cluster_name>-results-<account_id>."
  type        = string
  default     = ""
}

# ── optional toggles ─────────────────────────────────────────────────────────
variable "enable_spawner_irsa" {
  description = "Annotate the build-spawner SA with its IRSA role. Requires the platform manifests to be applied first (the SA must exist)."
  type        = bool
  default     = false
}

# Contest-day shape: sandbox 1 -> 4 (four concurrent contestants, one slot per
# node). Apply over the same state:
#   terraform apply -var-file=tfvars/contest-day.tfvars
# Everything else inherits contest.tfvars-equal defaults from variables.tf.

region       = "us-east-1"
cluster_name = "iicpc-contest"

sandbox_desired_size = 4

# Botworker desired stays 2; KEDA + cluster-autoscaler grow the pool with load
# up to botworker_max_size (set from measurement M1 before contest day).
botworker_max_size = 8

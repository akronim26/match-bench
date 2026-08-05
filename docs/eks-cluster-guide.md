# EKS cluster — bring-up and teardown

Rewritten 2026-08-03 after the second bring-up, which was the first to run a
contestant end to end. Follow top to bottom. Every step here is either proven on
a live cluster or explicitly marked as unresolved.

This supersedes the step-by-step in `docs/eks-bringup-handoff.md`; that file is
kept for the narrative of what went wrong and why. Design authority remains
`docs/eks-contest-deployment.md`. Backlog and the full bug list are in
`docs/remaining-work.md`.

Baseline cost is roughly **$2/hour** from the moment `terraform apply` completes.

---

## 0. Rules that prevent the mistakes that cost hours

1. **`export AWS_PROFILE=iicpc` in every shell.** The default profile is a
   different account in a different region.
2. **Check your kubectl context before every command that matters.** Local k3s
   and EKS are both in kubeconfig: `kubectl config current-context`.
3. **Never `grep -c` the output of `kubectl apply`.** It hides rejected
   resources. Read the errors, or `grep -iE "error|invalid"`.
4. **Never machine-edit YAML with line-oriented scripts.** Two attempts have
   corrupted NetworkPolicies. Hand-edit, apply, read the result.
5. **Verify against live objects, not manifests.** A dial test against a deleted
   pod produces a confident, meaningless answer.
6. **Port-forwards bind to whatever context was active when they launched.** A
   stale forward from a previous session is a silent tunnel into the wrong
   cluster — you will read the wrong leaderboard and believe it. Kill them all
   before starting, and check `ps -eo pid,etime,args | grep port-forward`.

---

## 1. One-time per AWS account

```bash
export AWS_PROFILE=iicpc

# State backend (already exists on account 885232248981 — skip if present)
aws s3 mb s3://iicpc-tf-state-885232248981 --region us-east-1
aws dynamodb create-table --table-name iicpc-tf-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-1

cd infra/terraform-v2
cp backend.hcl.example backend.hcl     # fill in bucket name; gitignored

# Quota: ONE Standard quota covers x86 AND Graviton (c7g is C-family).
# L-DB2E81BA "G and VT" is GPU instances — do NOT request it.
./precheck.sh                          # baseline needs 48; contest-day STD_NEED=128

# Multi-arch build support (bot-fleet is amd64+arm64)
docker run --privileged --rm tonistiigi/binfmt --install arm64
docker buildx create --name multiarch --driver docker-container
```

---

## 2. Bring-up

The order below is not arbitrary. Three of its steps exist because doing them in
the obvious order fails:

- **images before namespaces** — ECR repos must exist, and terraform creates them
- **namespaces before secrets** — nothing else creates the six namespaces, so
  `create-secrets.sh` fails on a fresh cluster otherwise
- **terraform twice** — the IRSA annotation patches a ServiceAccount that only
  exists after the manifests are applied

### 2.1 Cluster (~20 min; billing starts here)

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2
terraform init -backend-config=backend.hcl
terraform apply -var-file=tfvars/contest.tfvars -var enable_spawner_irsa=false
```

The `-var enable_spawner_irsa=false` is **required on a fresh cluster**. The
tfvars file sets it true (that is correct for every later apply), but
`kubernetes_annotations` patches an existing object and the `build-spawner`
ServiceAccount does not exist yet.

```bash
aws eks update-kubeconfig --name iicpc-contest --region us-east-1
kubectl config current-context
kubectl get nodes -L pool,role
```

Expect **6 nodes**: 2 general, 1 kafka, 1 sandbox, 2 botworker. The general nodes
are labelled `role=general`, not `pool=general` — that inconsistency is
cosmetic, nothing selects on it.

Terraform also installs, inside the cluster: the `gp3` StorageClass, the EKS
addons (vpc-cni, kube-proxy, coredns, aws-ebs-csi-driver), and four helm releases
(KEDA, metrics-server, cluster-autoscaler, aws-load-balancer-controller). It does
**not** install any application workload — no `iicpc` namespace exists yet, and
that is expected.

### 2.2 Images

```bash
export AWS_PROFILE=iicpc
./infra/terraform-v2/push-images.sh          # 13 amd64 + bot-fleet multi-arch
```

Tags by git short sha. ECR repos are IMMUTABLE, so **a re-push of the same tag
fails** — if a push dies partway, recover with an explicit new tag rather than
retrying, e.g. `./push-images.sh $(git rev-parse --short HEAD)-r2`.

The script stamps the tag into the overlay and then **fails closed** if any
`ghcr.io/` reference survives the render, or if `CAPTURE_IMAGE`/`SPAWNER_IMAGE`
do not resolve to the tag just pushed. If it exits non-zero at that stage the
images pushed fine and only the overlay wiring is wrong — do not re-run.

Commit the stamp (optional, but it is what makes "did this pod get my code"
answerable): `git add overlays/ && git commit -m "chore(eks): stamp <sha>"`.

### 2.3 Namespaces, then secrets

```bash
kubectl apply -f k8s/platform/namespace.yaml -f k8s/data/namespace.yaml \
  -f k8s/build/namespace.yaml -f k8s/sandbox/namespace.yaml \
  -f k8s/benchmark/namespace.yaml -f k8s/observability/namespace.yaml

export AWS_PROFILE=iicpc
./deploy-local/create-secrets.sh
```

`AWS_PROFILE` is **not optional** for the secrets step: the script derives the
registry endpoint from `aws sts get-caller-identity`, and that value becomes
`harbor-staging-endpoint` / `harbor-production-endpoint` in `spawner-secret` —
i.e. where Kaniko pushes contestant images. Without the profile it silently
resolves to the wrong account's registry, or the literal string `local`.

Expect `== secrets created/updated ==` and 13 secrets.

### 2.4 Platform

```bash
kubectl apply -k overlays/eks-contest 2>&1 | grep -iE "error|invalid"   # MUST print nothing
kubectl -n data wait --for=condition=complete job/kafka-topic-init --timeout=300s
```

Kafka, Postgres, Timescale and MinIO must pull images and bind PVCs, so give it a
few minutes before treating `Pending`/`ContainerCreating` as failure. Services
that dial Kafka or Postgres at boot (validator, score-computer, telemetry-ingester,
controller, spawner) will CrashLoop a few times until those are Ready — that is
normal startup ordering, not a fault. What is not normal:
`CreateContainerConfigError` (a secret is genuinely missing).

If Kafka PVCs are ever wiped, **re-run the topic-init Job** — auto-create is off,
so every publish silently goes nowhere without it:

```bash
kubectl -n data delete job kafka-topic-init && kubectl apply -k overlays/eks-contest
```

### 2.5 IRSA, second apply

Now that `build-spawner` exists:

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2
terraform apply -var-file=tfvars/contest.tfvars          # no override this time
cd /home/yash/iicpc
kubectl -n build rollout restart deploy/spawner
```

The restart is required — the web-identity env is injected at pod creation, so a
running spawner will not pick up the annotation. Skipping this produces a
`failed` submission whose build Job is never created, with the AWS SDK reporting
`no EC2 IMDS role found`.

### 2.6 Port-forwards

One per terminal, foreground:

```bash
kubectl -n platform port-forward svc/submission-api 8088:80
kubectl -n platform port-forward svc/frontend 3001:8080
kubectl -n observability port-forward svc/grafana 3000:3000
```

Note `submission-api` maps `80 -> 8080`, so the local port must map to **80**.

---

## 3. Verification, before submitting anything

### 3.1 Health and placement

```bash
kubectl get pods -A --no-headers | awk '$4!="Running" && $4!="Completed"'   # empty
kubectl get pods -A -o custom-columns=NS:.metadata.namespace,POD:.metadata.name,NODE:.spec.nodeName --no-headers | sort -k3
```

| node | should hold |
|---|---|
| kafka | `kafka-0` and nothing else |
| sandbox | `gro-disable` only — kept clear for the contestant |
| botworker ×2 | one `bot-fleet-worker` each (1 pod/node by required antiAffinity) |
| general ×2 | everything else |

`gro-disable` pod count must equal sandbox node count.

### 3.2 The env that has broken bring-ups before

```bash
kubectl -n sandbox get deploy sandbox-orchestrator -o jsonpath='{..env[?(@.name=="CAPTURE_IMAGE")].value}{"\n"}'
kubectl -n build   get deploy spawner              -o jsonpath='{..env[?(@.name=="SPAWNER_IMAGE")].value}{"\n"}'
kubectl -n build   get sa build-spawner -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}{"\n"}'
```

Both images must be **ECR refs ending in the tag you pushed** — not `ghcr.io`,
not `:demo`. The SA must carry a role ARN. These three are the difference between
a working cluster and one that looks healthy and grades nothing.

### 3.3 The orchestrator can reach a contestant

Only meaningful with a live algo pod, so this is really a check to run the first
time a slot sticks in `deploying`:

```bash
IP=$(kubectl -n sandbox get pod <algo-pod> -o jsonpath='{.status.podIP}')
O=$(kubectl -n sandbox get pod -l app=sandbox-orchestrator -o jsonpath='{.items[0].metadata.name}')
kubectl -n sandbox debug $O --image=busybox:1.36 --target=sandbox-orchestrator -q --attach=false \
  -- sh -c "nc -w 5 -zv $IP 9898; nc -w 5 -zv $IP 8080"
# then read the ephemeral container's logs
```

Both must connect. If they time out, the orchestrator egress rule in
`k8s/sandbox/sandbox-orchestrator/network-policy.yaml` has regressed.

---

## 4. First run

Auth is off, so identity comes from the token's `sub` claim — and it matters:
submissions are owner-scoped and a mismatch returns **404 "submission not
found"**, not 403. Use the same identity to submit and to trigger.

```bash
API=http://localhost:8088
TOKEN="eyJhbGciOiJub25lIn0.$(printf '{"sub":"devuser"}' | basenc --base64url | tr -d '=')."

curl -s -H "Authorization: Bearer $TOKEN" -F file=@deploy-local/reference-clob-fix.zip $API/submit
# poll GET $API/submissions/<id> until status=ready   (Kaniko build + scan, ~60s)
curl -s -X POST -H "Authorization: Bearer $TOKEN" $API/submissions/<id>/benchmark
```

Going through HTTP `StartBenchmark` also creates the `runs` row — publishing to
Kafka directly does not, which is why every local harness inserts it by hand.

Watch, in order: build Job on a **general** node → `algo-<slot>` on the
**sandbox** node → `capture-<slot>` on **that same node** (pinned by explicit
`NodeName`; a different node means the capture is in the wrong netns) → graphs at
:3001 → score.

Reference numbers on EKS: reference CLOB **0.9921**, FIX acker **0.0417**.

**Contestant constraints worth knowing before blaming the platform:** Go
submissions must declare `go <= 1.23` (the build template pins
`golang:1.23-alpine` with `GOTOOLCHAIN=local`); `protocol: FIX` requires port
9898 and `REST`/`WS` port 8080; and a build that fails permanently poisons that
zip's sha256, because `/submit` dedups on hash without checking status — recover
by changing the zip or deleting the row.

---

## 5. Known-open bugs to expect

Full list and evidence in `docs/remaining-work.md`. The ones that will affect
what you see:

- **A REST engine that fills everything scores 1.0000.** Grading is
  protocol-dependent; the FIX path disqualifies the same cheat at 0.0417. Do not
  trust a REST score.
- **Sessions with `sent_count = 0` publish 1.0000** instead of failing closed.
- **KEDA cannot grow the bot fleet.** The controller will not publish
  `workload.assignments` until members == shards, and the committed ScaledObject
  scales on lag on that same topic. Any scenario needing more shards than the
  current replica count fails after 90s with `bot-fleet capacity: ... need N`.
  Keep scenarios within `tasks <= 1000 × current workers`, or pre-scale.
- **Throughput reads 0 when an engine is merely slow.** Once the standing queue
  exceeds the 5s `RESPONSE_TIMEOUT` every response is late, so delivered tps
  reads 0 and `error_rate` 1.0 while the engine is still working normally.
- **Contestant bandwidth limits do nothing** — the `bandwidth` CNI plugin is not
  in the chain, so `ALGO_EGRESS_BANDWIDTH` is inert.

---

## 6. Teardown

Two of these steps exist because skipping them leaves billable resources running
while `terraform destroy` reports success.

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2

# 1. Helm releases in state HANG destroy (KEDA's uninstall times out against a
#    dying API server) and terraform still exits 0 with nodes running.
terraform state list | grep helm_release | xargs -r -n1 terraform state rm

# 2. The results bucket has prevent_destroy. Export anything worth keeping first —
#    scores live in Postgres, which dies with the cluster.
terraform state rm aws_s3_bucket.results aws_s3_bucket_versioning.results \
  aws_s3_bucket_public_access_block.results aws_s3_bucket_lifecycle_configuration.results

terraform destroy -var-file=tfvars/contest.tfvars
```

Then clean what terraform does not own.

**Per-submission ECR repos** — build-worker creates one per submission at
runtime. Safe to delete at any time, including while destroy runs:

```bash
aws ecr describe-repositories --region us-east-1 --query 'repositories[].repositoryName' --output text \
  | tr '\t' '\n' | grep -E "iicpc/[0-9a-f-]{20,}" \
  | xargs -r -n1 -I{} aws ecr delete-repository --repository-name {} --force --region us-east-1
```

**PVC-backed EBS volumes** — these survive the cluster (~$11/month if
forgotten). Wait until destroy has finished: volumes detach as nodes terminate,
so an early sweep catches only some of them. **Inspect before deleting**, and
prefer the tagged filter — the untagged form matches every unattached volume in
the region, including anything unrelated:

```bash
aws ec2 describe-volumes --region us-east-1 \
  --filters Name=status,Values=available \
            Name=tag:kubernetes.io/cluster/iicpc-contest,Values=owned \
  --query 'Volumes[].{id:VolumeId,size:Size,az:AvailabilityZone}' --output table

aws ec2 describe-volumes --region us-east-1 \
  --filters Name=status,Values=available \
            Name=tag:kubernetes.io/cluster/iicpc-contest,Values=owned \
  --query 'Volumes[].VolumeId' --output text \
  | tr '\t' '\n' | xargs -r -n1 -I{} aws ec2 delete-volume --volume-id {} --region us-east-1
```

Expect ~5–6 (kafka-logs, postgres, timescaledb, minio, redis, prometheus). If the
tagged query is empty but volumes remain available, check their tags before
falling back to an untagged sweep — PVCs provisioned through the `gp3` class
occasionally carry only `CSIVolumeName`.

**Final sweep — every count must be 0:**

```bash
aws eks list-clusters --region us-east-1 --query 'length(clusters)' --output text
aws ec2 describe-instances --region us-east-1 --filters Name=instance-state-name,Values=running,pending --query 'length(Reservations[].Instances[])' --output text
aws ec2 describe-volumes --region us-east-1 --query 'length(Volumes)' --output text
aws ec2 describe-nat-gateways --region us-east-1 --filter Name=state,Values=available --query 'length(NatGateways)' --output text
aws ecr describe-repositories --region us-east-1 --query 'length(repositories)' --output text
aws elbv2 describe-load-balancers --region us-east-1 --query 'length(LoadBalancers)' --output text
```

Finally, kill the port-forwards so no stale tunnel outlives the cluster.

---

## 7. Local k3s is not a faithful preview

Each of these gaps has cost a live bring-up:

| | local k3s | EKS |
|---|---|---|
| Secrets | `up-dev.sh` creates them | `create-secrets.sh`, after namespaces |
| Namespaces | created by `up-dev.sh` | only by the overlay — apply them first |
| `auth-api` Service | injected by `up-dev.sh` | now declared in `k8s/kustomization.yaml` |
| `CAPTURE_IMAGE`/`SPAWNER_IMAGE` | injected by `up-dev.sh` | now derived via `replacements:` |
| Bot-fleet ScaledObject | `b5-autoscale-shards.sh` applies its own | committed one is the older lag trigger |
| NetworkPolicies | `applynp()` skips them | enforced by the VPC CNI node agent |
| Images | pre-imported into containerd | first pull per submission — hence 180s/300s timeouts |
| Nodes | one untainted node | tainted pools; `SANDBOX_NODE_POOL` matters |
| Validator memory | small sessions fit anything | 8Gi limit, 5.2 GiB measured peak |

The pattern in the first five rows is the same: **something the local path
injects imperatively that the overlay never created**. Five of the seven defects
in the last bring-up were exactly that. When something works locally and fails on
EKS, check this list before debugging the platform.

The highest-value follow-up remains making local run the same tree through the
same overlay mechanism, so these stop being discovered on a paid cluster.

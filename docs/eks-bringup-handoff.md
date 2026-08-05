# EKS bring-up — complete guide

> **SUPERSEDED for procedure (2026-08-03). Use `docs/eks-cluster-guide.md`.**
> That guide carries the corrected bring-up order (namespaces before secrets,
> terraform applied twice for IRSA), the verification steps, and the teardown.
> This file is kept for the narrative of what went wrong and why — the §7
> corrections below are the useful part, not the step list above them.

Rewritten 2026-08-03 after the first bring-up (cluster built, validated, torn
down). Self-contained: follow top to bottom. Every step here is either proven or
explicitly marked as unresolved.

- Design authority: `docs/eks-contest-deployment.md`
- What happened last time, and why: `docs/eks-bringup-findings.md`
- Backlog: `docs/remaining-work.md`

**Everything discovered in the first bring-up is committed** (13 fixes, tree
clean as of `d6bce55`). This guide folds them in as ordinary steps, so a fresh
run should not rediscover them.

---

## 0. Rules that prevent the mistakes that cost hours last time

1. **`export AWS_PROFILE=iicpc` in every shell.** The default profile is a
   different user in ap-south-1.
2. **Check your kubectl context before every harness run.** Local k3s and EKS
   are both in kubeconfig; `kubectl config current-context`.
3. **Never `grep -c` the output of `kubectl apply`.** It hides rejected
   resources. Read the errors, or `grep -iE "error|invalid"`.
4. **Never machine-edit YAML with line-oriented scripts.** Two attempts
   corrupted NetworkPolicies. Hand-edit, then apply and read the result.
5. **Verify against live objects.** A dial test against a deleted pod produces
   a confident, meaningless answer.

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

## 2. Bring up the cluster (~20 min, ~$2/h starts here)

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2
terraform init -backend-config=backend.hcl
terraform apply -var-file=tfvars/contest.tfvars
aws eks update-kubeconfig --name iicpc-contest --region us-east-1
kubectl get nodes -L pool          # expect 2 general, 2 kafka, 1 sandbox, 2 botworker
```

**b2 runs two contestants concurrently and needs TWO sandbox nodes** (one slot
per node by design). Terraform does NOT reconcile node-group `desired_size`
(the module defers to autoscalers), so scale via the EKS API:

```bash
NG=$(aws eks list-nodegroups --cluster-name iicpc-contest --region us-east-1 \
     --query 'nodegroups' --output text | tr '\t' '\n' | grep sandbox)
aws eks update-nodegroup-config --cluster-name iicpc-contest --nodegroup-name "$NG" \
  --scaling-config minSize=1,maxSize=4,desiredSize=2 --region us-east-1
```

## 3. Images

```bash
./infra/terraform-v2/push-images.sh    # 13 amd64 + bot-fleet multi-arch; stamps the overlay tag
git add overlays/ && git commit -m "chore(eks): stamp image tag <sha>"
```

Note: `overlays/eks-contest/kustomization.yaml` may still pin
`bot-fleet-controller` to a one-off tag (`45364d4-t180`) from the first
bring-up. A fresh push supersedes it — make sure the stamp applied to every
image, including that one.

## 4. Secrets — MUST run before the overlay

Nothing in the manifest tree creates application Secrets (correct — secrets
don't belong in git). Skipping this is what left 20 pods in
`CreateContainerConfigError` last time.

```bash
export AWS_PROFILE=iicpc
./deploy-local/create-secrets.sh       # idempotent; works against any context
```

## 5. Deploy the platform

```bash
kubectl apply -k overlays/eks-contest 2>&1 | grep -iE "error|invalid"   # MUST print nothing
kubectl get pods -A | grep -vE "Running|Completed"                      # MUST be empty
kubectl -n data wait --for=condition=complete job/kafka-topic-init --timeout=300s
```

If Kafka PVCs are ever wiped (voter-set changes require it), **re-run the
topic-init Job** — auto-create is off, and every publish silently goes nowhere
without topics:

```bash
kubectl -n data delete job kafka-topic-init && kubectl apply -k overlays/eks-contest
```

Port-forwards (detached so they survive):

```bash
setsid nohup kubectl -n observability port-forward svc/grafana 3000:3000 >/dev/null 2>&1 &
setsid nohup kubectl -n platform port-forward svc/frontend 3001:8080  >/dev/null 2>&1 &
```

## 6. Gates, in order — stop on the first failure

1. **gro-disable coverage**: pods == sandbox nodes (b2's preflight asserts it).
2. **Verifier**: the first capture Job must load its BPF programs on the EKS
   kernel (AL2023/6.1). Check capture pod logs for a clean attach; XDP falling
   back to skb mode is expected and fine.
3. **b2** — the platform gate. Contestants go through the REAL submission path
   (zip → Kaniko → ECR → `ready`), which is itself the build-pipeline test:

```bash
export AWS_PROFILE=iicpc
HARNESS_ENV=eks deploy-local/b2-two-pass.sh
```

Expect: book qualifies (~0.994), echo disqualified (~0.045), `capture_gaps=0`,
11/11. Reference numbers from local k3s with the same images.

4. Then `b2-pass2.sh`, `b4-stalled-peer.sh`, `b3-mixed3.sh`,
   `b3-mixed-protocol.sh`, `b5-autoscale-shards.sh` — all with
   `HARNESS_ENV=eks`. b1 is blocked on a decision: with auth off both
   submissions share `DEFAULT_CONTESTANT_ID`, so its cross-contestant isolation
   assertions cannot pass as written.

## 7. Open problems — expect these, and diagnose them THIS way

**All three of (a), (b) and (c) below are RESOLVED as of 2026-08-03**, and all
three had the wrong diagnosis recorded. They are kept here with the corrections
attached because the diagnostic recipes are still the right ones to reach for,
and because the wrong theories are worth not re-deriving. Summary:

| | recorded cause | actual cause |
|---|---|---|
| a | capture netns / Kafka QueueFull | stale `:demo` capture image (see c) |
| b | ProtocolAll dial in `slot.go` | NetworkPolicy egress gap, all ports |
| c | something sets `CAPTURE_CLAMP_MTU=1500` | stale image's compiled-in default |

**a) `orders.acked` stays 0 / no graphs — RESOLVED.** Root cause was (c): the
capture Job ran a stale public `ghcr.io/agrawalx/ebpf-latency:demo` because
`CAPTURE_IMAGE` is an env var the `images:` transformer cannot rewrite. With the
correctly-stamped image, `orders.acked` populates immediately and a reference
engine scores 0.9921 with `matched/sent = 95%`. The triage below is still the
right first move if it recurs. While a session is RUNNING (the capture Job is
reaped at session end — you get one window):

```bash
P=$(kubectl -n sandbox get pods --no-headers | grep capture | grep Running | awk '{print $1}' | head -1)
kubectl -n sandbox port-forward pod/$P 29090:9090 >/dev/null 2>&1 &
curl -s localhost:29090/metrics | grep -E "events_decoded|ringbuf_dropped|acked_dropped|tc_packets|throttled"
```

- `events_decoded` climbing, `acked_dropped` > 0 → capture sees traffic but the
  Kafka publish is dropping batches (producer QueueFull). Prime suspect.
- `events_decoded` ~0 → the capture is not seeing packets: wrong netns
  (`EBPF_ALGO_POD_UID` / container-id resolution) or packets dropped before the
  hook.
- `ringbuf_dropped` > 0 or `throttled` > 0 → CPU starvation (all clean locally).

**b) ProtocolAll slots never become ready — RESOLVED 2026-08-03. The diagnosis
below was wrong; it is a NetworkPolicy egress gap, not ProtocolAll.** Measured
from inside the orchestrator's own netns, BOTH 9898 and 8080 timed out, against
algo pods on two different nodes in two AZs — so the "extra port" was never the
issue. `sandbox-isolation` already permitted the INGRESS half of the dial (algo
pods accept `from podSelector app=sandbox-orchestrator`) but nothing permitted
the matching EGRESS, and the orchestrator is itself in the sandbox namespace, so
sandbox-isolation's egress applies to it too — whose catch-all `0.0.0.0/0`
EXCEPTs 10.0.0.0/8, i.e. every pod IP. The union of both policies denied it.

Single-protocol submissions only *looked* healthy because `slot.go` dials the
EXTRA port: a FIX-only slot performs no dial at all and is declared ready
without ever touching the contestant. ProtocolAll was simply the only code path
that exercised the broken one.

Fixed by the egress rule at the end of
`k8s/sandbox/sandbox-orchestrator/network-policy.yaml`. Verified before/after
from the orchestrator netns (timeout -> `open`), and a full `protocol: ALL`
submission then completed end to end. If it ever regresses, this is the test:

```bash
IP=$(kubectl -n sandbox get pod <algo-pod> -o jsonpath='{.status.podIP}')
O=$(kubectl -n sandbox get pod -l app=sandbox-orchestrator -o jsonpath='{.items[0].metadata.name}')
kubectl -n sandbox debug $O --image=busybox:1.36 --target=sandbox-orchestrator -q --attach=false \
  -- sh -c "nc -w 5 -zv $IP 9898; nc -w 5 -zv $IP 8080"
# then read the ephemeral container's logs
```

**Fallback if either blocks progress**: the previous working EKS runs
(`e2e/02-bootstrap.sh`) applied only the build namespace's policy. Deleting the
others reproduces that known-good config and unblocks measurement work — but
policies are the only isolation boundary now that gVisor is off, so this is
acceptable only while contestants are our own engines.

**c) Capture MTU — RESOLVED 2026-08-03, root cause was a stale image.** The
capture logged `clamped 9001 -> 1500` and the earlier diagnosis ("something
passes `CAPTURE_CLAMP_MTU=1500`") was wrong: nothing in the tree sets that var.
`captureJobSpec` (`slot.go:610`) passes nine env vars and none is the MTU;
`k8s/benchmark/ebpf-latency/job-template.yaml` has no such entry; there are no
initContainers anywhere in `k8s/`, `overlays/`, `e2e/` or `deploy-local/`; and
`gro-disable-daemonset.yaml` runs `ethtool -K` only, never touching MTU. The
1500 was the **old binary's compiled-in default**. `DEFAULT_CLAMP_MTU` became
9001 in `ccf0f80` (2026-08-01), but the capture image is not a container image
field — it is the `CAPTURE_IMAGE` env var on sandbox-orchestrator, which
kustomize's `images:` transformer cannot rewrite, and the overlay had no entry
for ebpf-latency anyway. So every capture Job on EKS pulled the public
`ghcr.io/agrawalx/ebpf-latency:demo` from the base manifest while the rest of
the platform ran ECR `418a2c0`. That image also carries `CAPTURE_CAP=1536`.

`SPAWNER_IMAGE` (the `fetch` initContainer of every build Job, `spawner.go:528`)
had the identical defect — base `ghcr.io/agrawalx/spawner:demo`.

Both are fixed: `overlays/eks-contest` now patches the two env vars to ECR refs
and derives their tags from the already-stamped image fields via a
`replacements:` block, so they cannot drift from the deploy. `push-images.sh`
now fails the push if any `ghcr.io/` ref survives the render, or if either env
var does not resolve to the pushed tag. Why local k3s never showed it: every
other bring-up path injects both vars explicitly (`up-dev.sh:168,210`,
`e2e/02-bootstrap.sh:35,80`, `deploy-bench/up-full.sh:46,63`,
`infra/Makefile:168`) — only `kubectl apply -k overlays/eks-contest` fell
through to the base defaults.

**This changes the priors on (a) and on the Kaniko 401.** The `:demo` capture
binary predates every recent ebpf-latency change, so re-test `orders.acked`
with a correctly-tagged capture *before* investigating netns targeting or
producer QueueFill. Likewise the `:demo` fetcher ignores `REGISTRY_PROVIDER=ecr`
if it predates that branch, and would clobber the mounted `/kaniko/.docker` ECR
config with Harbor creds (`fetcherEnv`, `spawner.go:710`) — a plausible
mechanism for the 401 that `patch-spawner-ecr.yaml` worked around.

## 8. Measurements (only after gates are green)

M1 per-pod Graviton loadgen TPS (fixes `botworker_max`), M2 capture ceiling,
M3 telemetry at ramp peak, M4 validator sessions/hour, M5 four-group rehearsal.
Details in `docs/eks-contest-deployment.md` §6b.

## 9. Teardown — and the two traps that leave money running

```bash
export AWS_PROFILE=iicpc && cd infra/terraform-v2

# 1. Helm releases in state HANG destroy (KEDA's uninstall times out against a
#    dying API server) and terraform still exits 0 with nodes running.
terraform state list | grep helm_release | xargs -r -n1 terraform state rm

# 2. The results bucket has prevent_destroy; release it (export first if the
#    run produced anything worth keeping).
terraform state rm aws_s3_bucket.results aws_s3_bucket_versioning.results \
  aws_s3_bucket_public_access_block.results aws_s3_bucket_lifecycle_configuration.results

terraform destroy -var-file=tfvars/contest.tfvars
```

Then clean what terraform does not own — **PVC-backed EBS volumes survive the
cluster** (~$11/month if forgotten), and build-worker creates ECR repos per
submission at runtime:

```bash
aws ec2 describe-volumes --region us-east-1 --query 'Volumes[?State==`available`].VolumeId' --output text \
  | tr '\t' '\n' | xargs -r -n1 -I{} aws ec2 delete-volume --volume-id {} --region us-east-1
aws ecr describe-repositories --region us-east-1 --query 'repositories[].repositoryName' --output text \
  | tr '\t' '\n' | grep -E "iicpc/[0-9a-f-]{20,}" \
  | xargs -r -n1 -I{} aws ecr delete-repository --repository-name {} --force --region us-east-1
```

Final sweep — every count must be 0:

```bash
for q in "eks list-clusters --query length(clusters)" ; do :; done
aws eks list-clusters --region us-east-1 --query 'length(clusters)' --output text
aws ec2 describe-instances --region us-east-1 --filters Name=instance-state-name,Values=running,pending --query 'length(Reservations[].Instances[])' --output text
aws ec2 describe-volumes --region us-east-1 --query 'length(Volumes)' --output text
aws ec2 describe-nat-gateways --region us-east-1 --filter Name=state,Values=available --query 'length(NatGateways)' --output text
aws ecr describe-repositories --region us-east-1 --query 'length(repositories)' --output text
aws elbv2 describe-load-balancers --region us-east-1 --query 'length(LoadBalancers)' --output text
```

## 10. Known environment differences to keep in mind

Local k3s is NOT a faithful preview, and each gap has bitten:

| | local k3s | EKS |
|---|---|---|
| Secrets | `up-dev.sh` creates them | `create-secrets.sh` (step 4) |
| NetworkPolicies | `applynp()` skips them; enforcement real if applied | applied by the overlay; VPC CNI semantics differ |
| Images | pre-imported into containerd; no cold pulls | first pull per submission — why timeouts are 180s/300s |
| Nodes | one untainted node | tainted pools; `SANDBOX_NODE_POOL` matters |
| Validator resources | 2 CPU/2Gi starves a laptop (use ~500m/768Mi req) | 2 CPU/2Gi correct |

The highest-value follow-up remains making local run the same tree through the
same overlay mechanism, so these stop being discovered on a paid cluster.

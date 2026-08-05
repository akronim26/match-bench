# How AWS-native is this stack, and should it be more so?

Written 2026-08-04, after the first `production/` bring-up. The short answer:
**the stack is deliberately EKS-hosted with a self-managed data plane, most of
that is the right call, and exactly one component should move.**

"AWS-native" is not a goal. The useful question is which specific properties you
want — durability past teardown, less operational surface, faster scaling — and
whether a managed service buys them without costing measurement fidelity or the
local-development path.

---

## 1. Where it stands today

**Already AWS-managed:**

| | service |
|---|---|
| control plane | EKS |
| compute | managed node groups (4 pools, tainted) |
| registry | ECR — 14 service repos + one per submission, created at runtime |
| identity | IAM + IRSA (`build-spawner`, `results-export`, EBS CSI, ALB, autoscaler) |
| storage | EBS gp3 via the CSI driver |
| network | VPC, subnets across 2 AZs, NAT |
| object store | S3 — but only for terraform state and the (empty) results bucket |
| addons | vpc-cni, kube-proxy, coredns, aws-ebs-csi-driver |

**Self-hosted in-cluster** (`k8s/data/`, `k8s/observability/`):

| component | shape today | AWS equivalent |
|---|---|---|
| Kafka | StatefulSet, **1 broker, RF=1**, KRaft, 20Gi PVC | MSK / MSK Serverless |
| Postgres | StatefulSet, replicas 1, 10Gi PVC | RDS / Aurora |
| TimescaleDB | StatefulSet, replicas 1, 20Gi PVC | RDS + partitioning, or Timestream |
| Redis | StatefulSet, replicas 1 | ElastiCache |
| MinIO | StatefulSet, 50Gi PVC | **S3** |
| Prometheus | Deployment + PVC | Amazon Managed Prometheus |
| Grafana | Deployment, admin/admin, port-forward only | Amazon Managed Grafana |
| Loki | Deployment | CloudWatch Logs |
| cluster-autoscaler | helm release | Karpenter |
| Kaniko | build Job | CodeBuild |
| Trivy / Syft | scan + sbom Jobs | ECR enhanced scanning (Inspector) |

So: **an AWS-native control plane with a self-managed data plane.** That is a
coherent architecture, not an accident of drift.

---

## 2. Three constraints that dominate the decision

Any migration argument has to survive these. Most don't.

### 2.1 This platform is a measurement instrument

Its entire purpose is measuring contestant latency to microsecond resolution
with kernel-stamped timestamps. That imposes constraints a normal web stack
doesn't have:

- The **eBPF capture cannot move.** It runs as a privileged pod pinned by
  `NodeName` into the contestant's network namespace. There is no managed
  service equivalent, and there never will be.
- **Every network hop added to the measurement path is variance you no longer
  control.** Today Kafka, Postgres and Redis are one intra-VPC hop from their
  clients, on nodes you own. MSK and RDS put an ENI, a different AZ and an
  AWS-managed scheduler between them.
- We already know the measurements are sensitive at this scale: today's 25k run
  showed **77 ms p99 spikes from an allocator resize inside the contestant**.
  A stack that introduces its own tail latency makes findings like that harder
  to attribute, not easier.

### 2.2 The cluster is ephemeral by design

`production/contest.sh` creates the cluster and `99-teardown.sh` destroys it.
Lifetime is hours, not months. That inverts the usual managed-service calculus:

- **Provisioning time becomes bring-up time.** RDS is ~10 minutes, MSK ~15–30.
  Today's entire terraform apply was ~20 minutes; adding MSK could double it.
- **Managed services resist teardown.** We already hit this with one S3 bucket:
  `prevent_destroy` left it orphaned and broke the *next* apply
  (`BucketAlreadyExists`), which needed an import step in `01-cluster.sh` to fix.
  Every managed service with retention semantics adds another instance of that
  problem.
- **Cost inverts.** A 20Gi PVC on a node you are already paying for is nearly
  free. MSK + RDS + ElastiCache have hourly floors whether or not a contest is
  running.

### 2.3 Local k3s runs the same tree

`overlays/local-k3s` and `overlays/eks-contest` share one base. That is what
makes local development meaningful — and the divergence between them is
precisely what caused **five of the seven defects** in the last bring-up
(`docs/remaining-work.md`).

Moving Kafka to MSK or Postgres to RDS means the two environments no longer run
the same topology. You would be *deliberately re-creating* the exact class of
bug the last two sessions were spent eliminating. This is the strongest argument
against, and it applies to every stateful component.

---

## 3. Component by component

### Do not move

**Kafka.** RF=1 single broker is a *decision*, not a limitation — benchmark data
is re-runnable, so replication buys nothing and costs write bandwidth
(`patch-kafka-single-broker.yaml`). It is on the measurement path: the capture
publishes `orders.acked` through it at hundreds of thousands of records/s. MSK
would add hops, cost, and a second topology, in exchange for durability the
design explicitly does not want.

**TimescaleDB.** Holds per-(session, wave) HDR histograms. Purely derived data
that is regenerated by re-running a session. No AWS equivalent offers hypertables
without re-engineering the ingester's rollup.

**Redis.** Live-SSE tile state, seconds of lifetime. ElastiCache for a cache
whose contents are worthless after the run is pure overhead.

**Prometheus / Loki.** Contest-lifetime observability. AMP/AMG are worth
considering only if you want dashboards public without a port-forward — which is
an *access* problem, better solved by the Ingress work that auth already needs.

**Kaniko → CodeBuild.** Kaniko runs inside the cluster where the network policy,
node pool and IRSA story already exist. CodeBuild would need its own VPC config
and a way back into ECR, to solve nothing currently broken.

### Move — one component

**MinIO → S3.** This is the clear win and the only unambiguous one:

- It holds exactly three things: contestant artifacts (the submitted zips),
  `submissions/<id>/trivy-report.json`, and `submissions/<id>/sbom.json`.
- **All three should outlive the cluster.** Today they die with a 50Gi PVC on
  teardown. Contest artifacts and their security reports are precisely the things
  you would want after a dispute.
- The plumbing already exists — S3 is in use for terraform state and the results
  bucket, IRSA is wired, and `submission-api` already talks the S3 API (MinIO is
  S3-compatible, so this is an endpoint and credential change, not a rewrite).
- It *removes* a StatefulSet and a PVC rather than adding a service, so bring-up
  gets shorter, not longer.
- It does not touch the measurement path at all.

The one thing to get right: the local overlay should keep MinIO, so
`overlays/local-k3s` stays self-contained and offline-capable. That means the
endpoint must be overlay-level config, which it already is (`spawner-secret`,
`submission-api-secret`).

### Consider, for a specific reason

**cluster-autoscaler → Karpenter.** Not for tidiness — for **scale-up latency**.
Today's concurrent-submission failure had the autoscaler fire at 13:37:06 and
deliver nodes at 13:37:43, while the scheduler had already placed the algo pod on
a full node at 13:37:25. Karpenter provisions in tens of seconds rather than
minutes and bin-packs deliberately, which would narrow that window. It does not
*fix* the slot-footprint race (finding 11) — only reserving the pair does — but
it reduces how often the race is entered. Already parked in the design docs.

**Postgres → RDS.** Only if contest results must survive teardown. Today they
don't: this session's scores and telemetry died with the cluster. But there is a
cheaper answer already in the tree — `results-export-job.yaml`, currently
`suspend: true`, which `pg_dump`s both databases to the results bucket. Turning
that on costs nothing and solves the actual requirement. RDS solves it by making
the database permanent, which is a much larger change for the same outcome.

**ECR enhanced scanning instead of Trivy.** Marginal. Trivy already works, and
its output is currently **consumed by nothing** — no gate, no leaderboard
surface. Fix the consumption question first; the scanner choice is downstream of
it and may not matter.

---

## 4. Recommendation

**Do one thing: move MinIO to S3.** It is the only component where "AWS-native"
and "actually better" coincide — artifacts and scan reports outlive the cluster,
one StatefulSet and one PVC disappear, IRSA already exists, and the measurement
path is untouched.

**Turn on the results-export job** in the same pass. Together those two changes
mean nothing of value dies at teardown, which is the real requirement hiding
behind most of the "should this be RDS" instinct.

**Revisit Karpenter** when the slot-footprint race is fixed, not before —
otherwise it narrows a window whose existence you have not yet removed.

**Leave the rest.** Kafka, Timescale and Redis are ephemeral, derived, or on the
measurement path. Moving them costs bring-up time, money, local/EKS parity, and
measurement fidelity, and buys durability the design explicitly does not want.

The genuine gap in this stack is not that it self-hosts a data plane. It is
**auth, ingress and multi-tenancy** (`docs/remaining-work.md`) — there is no
Ingress, no TLS, no public origin, and the frontend has auth compiled out. That
is where the AWS-native answer is unambiguous: ALB + ACM + Route53, and Cognito
or the existing Google OAuth behind them. Any effort spent making the data plane
managed is effort not spent on the thing that actually blocks a real contest.

---

## 5. What can never move

For completeness, so nobody re-opens these:

- **eBPF capture** — privileged, pinned into the contestant's netns by `NodeName`.
- **The sandbox isolation model** — taints, dedicated nodes, cpuset pinning, and
  the 1-slot-per-node property.
- **bot-fleet workers** — need dedicated physical cores with no neighbour noise;
  that is why they are 1-pod-per-node on Graviton.

These are the platform. Everything else is plumbing around them.

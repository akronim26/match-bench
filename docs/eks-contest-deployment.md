# EKS contest deployment — full rewrite design

Status: DRAFT for review, 2026-08-02. Supersedes `infra/terraform/` as-is (written
~1.5 months ago, predates: jumbo-MTU capture (no net-tune), measured capture sizing,
validator KEDA scaling, the 4-partition status topic, distributed ingester, B4).
Decision: rewrite, not patch.

Decided inputs (2026-08-02): **contest tier** (not the 2M/s bench tier), **ephemeral
lifecycle** (apply → use → destroy, any duration; nothing in-cluster is precious),
**in-cluster KRaft Kafka**, **all on-demand — no spot anywhere (decided 2026-08-02)**.

## 1. What "contest tier" means, concretely (REVISED 2026-08-02)

**Run group = 4 scenarios**: one pass-1 `correctness` (max-rate, single connection,
~40–50k orders/s) + pass-2 `ramp` with the full HFT profile — **generation reaching
~500k orders/s at peak** — plus `spike` and `constant`. The contestant is battle-tested
at the ramp tier; `SpikeRecoveryNS` stays a scored metric.

**The load model is therefore ramp-dominated.** Worst case is not "4 modest runs": it
is up to 4 sessions in flight whose peaks can coincide. Aggregate loadgen worst case
≈ 1–2M orders/s — bench-tier magnitude — so loadgen, Kafka and telemetry are sized
ELASTICALLY with the peak measured on this cluster (runbook step 6), not assumed.

**Collision this creates with the band ceiling — decision needed.** The platform
admits 4 concurrent SESSIONS total (24 partitions / width 6 = 4 exclusive bands). A
4-scenario run group that executes its scenarios concurrently consumes ALL 4 bands —
one contestant's group monopolizes the platform. Options:
  (a) groups run scenarios SEQUENTIALLY (1 band per contestant at a time; 4
      contestants' groups in flight; a full group takes ~4× scenario duration);
  (b) grow the band space (48 partitions / width 6 = 8 bands) — touches producers,
      validator, ingester replica math, and is exactly the §7 Kafka topology exercise;
  (c) hybrid: sequential within group, 4 groups concurrent (= (a), the cheap answer).
**DECIDED 2026-08-02: (a/c) — sequential within group, continue-on-fail.**
Implemented in submission-api: StartBenchmark inserts every child run upfront but
publishes only the first scenario (entered as `queued`); the benchmark-status consumer
claims and dispatches the next `requested` run on each terminal session (completed OR
failed — a failed correctness gate still lets the scale scenarios produce metrics;
correctness scores 0 via pass-1-only aggregation). Claim is `FOR UPDATE SKIP LOCKED`
+ status-guarded → redelivery-safe, dispatch-at-most-once. (b) 8 bands remains the
lever if groups/hour proves too low in rehearsal.

## 2. Node pools

| pool | count | type | vCPU/RAM | label | taint | why this size |
|---|---|---|---|---|---|---|
| general | 2 | m6i.2xlarge | 8/32GB | `role=general` | none | DBs + ingester + **validator burst (4 pods × 2 CPU = 8 CPU)** + controllers + observability. 2× m6i.xlarge cannot absorb the validator burst alongside Timescale. The ~40GB request-to-capacity RAM gap is NOT waste: it is OS page cache under Timescale (write-heavy at ramp-peak ingest), Postgres and Prometheus — same design as the Kafka nodes. The c6i.2xlarge alternative saves ~$0.09/h but puts limits-burst (~24Gi) against ~29Gi allocatable, evicting DB page cache exactly when validator burst and ingest peak coincide. |
| kafka | **1** | m6i.xlarge | 4/16GB | `pool=kafka` | `kafka=true:NoSchedule` | **Revised 2026-08-03 to 1 node / 1 broker.** The design argued for 2 brokers from the ramp-dominated load model, but the eks-contest overlay pins the statefulset to a single broker (decided during the first bring-up, after the base 3-broker shape left `kafka-2` Pending on a 2-node pool) — so the second node was pure cost. RF=1 throughout (re-runnable benchmark data; replication buys nothing and costs write bandwidth). Node count and broker count MUST move together: raising `kafka_desired_size` also requires `overlays/eks-contest/patch-kafka-single-broker.yaml` (replicas, quorum voters, internal-topic RFs). **M3 decides** whether one broker holds at the ~500k orders/s ramp target; `kafka_max_size=2` keeps the headroom. |
| sandbox | 1–4 | c6i.2xlarge | 8/16GB | `pool=sandbox` | `sandbox=true:NoSchedule` | **x86 stays**: the BPF capture object is built and verifier-proven on x86_64/AL2023, and contestant submissions are compiled for this arch. **One slot per node**: requests algo 4 CPU/8Gi + capture 2 CPU/2Gi = 6 CPU/10Gi of ~7.5/14.5 allocatable; limits algo 4/8Gi + capture 4/4Gi = the whole node under burst, BY DESIGN — the slack IS the capture's burst room, and per-node isolation makes cross-contestant capture starvation topologically impossible. `sandbox_desired` is the contest-day dial. |
| botworker | 2 + measured | **c7g.xlarge (Graviton, arm64)** | 4/8GB | `pool=botworker` + `arch=arm64` | `botworker=true:NoSchedule` | **ARM by decision**: more physical cores per dollar → better orders/s per $ for the loadgen — pure userspace Rust, the one pool with no BPF dependency. **ONE worker pod per node** (decided 2026-08-02): requests 3 CPU of ~3.6 allocatable make a second worker unschedulable, plus required podAntiAffinity as the explicit guarantee — dedicated cores, no neighbor noise, clean per-pod measurement. Baseline 2 nodes/2 pods always on; KEDA scales PODS on workload.assignments lag, cluster-autoscaler on the managed group (NOT Karpenter — one flag, sufficient) follows with NODES. `botworker_max` is a MEASUREMENT OUTPUT: ceil(worst-case aggregate ÷ M1 per-pod TPS) + 1 — not guessed here. Requires the multi-arch pipeline (§5b). All on-demand — no spot (decided). |

Baseline (1 sandbox, 2 botworkers): 36 x86 vCPU + 16 arm vCPU. Contest-day
(4 sandbox, botworkers scaled to measured need): up to **60 x86 + 80 arm vCPU →
quota ask: ONE Standard on-demand quota (L-1216C47A) ≥ 128** — the Standard bucket
(A,C,D,H,I,M,R,T,Z) covers Graviton c7g too; the family LETTER picks the quota, not
the processor. (Corrected 2026-08-02: an earlier revision asked the "G and VT" quota,
which is GPU graphics instances — irrelevant, and its 0-default/human-review is a
new-account trust setting, not a billing-tier limit.) 56 x86 + 64 arm = 120, ask 128.
Rough on-demand, us-east-1: baseline ≈ $2.2/h; full contest shape ≈ $4–6/h depending
on how many Graviton nodes the measured per-node TPS demands. Ephemeral posture makes
the daily rate mostly irrelevant.

AMI: AL2023 (kernel 6.1) — the BPF verifier bounds in `capture_len` are proven against
it (live-load-tested 2026-08-01). AMI changes re-run the verifier check (runbook step 4).

## 3. Per-pod resources (measured where possible)

| pod | requests | limits | QoS | pool | basis |
|---|---|---|---|---|---|
| contestant (algo) | 4 CPU / **8Gi** | 4 / **8Gi** | Guaranteed | sandbox | `ALGO_CPU=4`, `ALGO_MEMORY=8Gi` (was 2Gi, sized for the 4-vCPU minimal tier — REVISED 2026-08-02: leaving RAM idle on the sandbox node is pointless; 8Gi lets the book grow deep at 500k/s ramp without memory being the artificial ceiling). Integer CPU = cpuset-friendly if `enable_sandbox_cpuset` is ever turned on. |
| eBPF capture | 2 CPU / **2Gi** | 4 / **4Gi** | Burstable | sandbox | Request 2 CPU is the measured CFS floor that ended ringbuf drops. Memory RAISED (2026-08-02) from 1Gi/2Gi: the 4Gi limit means the 256MB ring can grow to 512MB–1GB as a pure config response if M2 shows drops at peak — headroom bought now so the fix later is one constant, not a resize. Update the memory-pairing Go test with these values. |
| bot-fleet-worker | **3 CPU / 6Gi** | 4 / 7Gi | Burstable | botworker | REVISED for 1-pod-per-node (was 1 CPU req when packing multiple per x86 node): ~3 dedicated Graviton cores per worker. KEDA on workload.assignments lag. |
| **correctness-validator** | **2 CPU / 2Gi** | **2 / 2Gi** | **Guaranteed** | general | **CHANGED from 250m/1CPU**: one full-replay validation per pod (`VALIDATOR_CONCURRENCY=1`); a 1-CPU limit would CFS-throttle replay — the exact failure class the capture had. KEDA 1–4 on status-topic lag. 4×2 CPU burst is why general = m6i.2xlarge. |
| kafka broker | 1 CPU / 2Gi | 4 / 8Gi | Burstable | kafka | Unchanged; gp3 PVC (size in tfvars, 100Gi contest default vs bench's 500). |
| telemetry-ingester | 1 CPU / 512Mi | 2 / 1Gi | Burstable | general | 2 replicas (co-partitioned; scale with ORDERS_PARTITIONS if ever needed). |
| telemetry-rollup | 250m / 256Mi | 1 / 512Mi | Burstable | general | |
| timescaledb | 500m / 1Gi | 2 / 2Gi | Burstable | general | gp3 PVC. |
| postgres | 250m / 512Mi | 1 / 1Gi | Burstable | general | gp3 PVC. |
| redis | 100m / 256Mi | 500m / 512Mi | Burstable | general | |
| bot-fleet-controller | 100m / 128Mi | 500m / 256Mi | Burstable | general | replicas=1 REQUIRED (in-memory lease allocators; promote to Postgres before ever scaling). |
| score-computer | 100m / 128Mi | 1 / 512Mi | Burstable | general | |
| submission-api / auth-api / leaderboard-api / frontend | 100m / 128Mi | 500m / 256Mi | Burstable | general | Stateless HTTP. |
| build-worker (spawner) | 250m / 256Mi | 1 / 1Gi | Burstable | general | Kaniko jobs it spawns carry their own resources. |
| gro-disable DaemonSet | 10m / 32Mi | 100m / 64Mi | Burstable | sandbox (all nodes) | REQUIRED regardless of MTU — GRO coalesces to 64KB. Runbook asserts pod count == sandbox node count. |

Security posture carried forward: contestant pods `Drop ALL` caps +
`AllowPrivilegeEscalation:false` + `RuntimeDefault` seccomp (kernel-bypass impossible —
audited); **no privileged initContainers anywhere** (net-tune is deleted); gVisor off
(`RUNTIME_CLASS=""`), NetworkPolicy enforcement is therefore a hard gate — the policies
in `k8s/*/network-policy.yaml` must apply cleanly on the VPC CNI (runbook step 5).

## 4. Environment-variable matrix (contest values)

Only vars whose value is a *decision*; plumbing (brokers, URLs, groups) comes from
overlays/secrets unchanged. Full inventory audited 2026-08-02.

| service | var | contest value | why |
|---|---|---|---|
| sandbox-orchestrator | ALGO_CPU / ALGO_MEMORY | 4 / 8Gi | §3; per-tier knob, overlay-owned |
| | CAPTURE_ENABLED | true | |
| ebpf-latency | CAPTURE_CLAMP_MTU | unset (default 9001) | jumbo regime; 1500 is the rollback lever |
| | ORDERS_PARTITIONS | 24 | must match topic + producers |
| | ORDER_BAND | per-session (orchestrator-injected) | co-partitioning |
| bot-fleet | BOT_PARTITION_BAND_WIDTH | 6 | 24/6 = 4 bands = admission ceiling |
| | BOT_MAX_INFLIGHT_PER_TASK | 64 | B4-proven throttle |
| | MAX_CONCURRENT_WORKLOADS | 2 | memory invariant (worker.rs comment) |
| correctness-validator | VALIDATOR_CONCURRENCY | 1 | one validation per pod |
| | POD_NAME | downward API | per-pod band group (replicas>1 correctness) |
| | VALIDATOR_ORDER_BAND_WIDTH | 6 | must equal producer band width |
| | CROSS_FLOW_WINDOW_US | **OPEN — B6, must be calibrated in the 9001 regime** | old-regime numbers invalid |
| | SETTLE_DELAY_MS / VALIDATION_TIMEOUT_MS | 10000 / per-scenario | |
| submission-api | SEED_SCENARIOS | correctness,ramp,spike,constant | 4-scenario run group (decided 2026-08-02); SpikeRecoveryNS stays scored |
| | RESEED_SCENARIOS | true on first boot, false after | |
| | AUTH_REQUIRED | true | contest = real users |
| score-computer / leaderboard | (defaults) | | |
| kafka topics | benchmark.status.updated partitions | 4 | = bands; init job comment ties them |

## 5. Terraform layout (the rewrite)

```
infra/terraform-v2/
  backend.tf          S3 state + DynamoDB lock (FIRST; current local tfstate is a hazard)
  main.tf             VPC + EKS (AL2023), 4 managed node groups per §2
  variables.tf        every §2/§3 knob; sandbox_desired is THE contest-day dial
  tfvars/
    contest.tfvars    §2 as written (sandbox 1 baseline)
    contest-day.tfvars  sandbox_desired=4
  addons.tf           KEDA, EBS CSI (gp3 default SC), metrics-server
  ecr.tf              per-service repos, immutable tags (exists; carry over)
  irsa.tf             spawner + export-job roles
  export.tf           S3 results bucket + lifecycle; the destroy-precondition target
  precheck.sh         quota >= vCPU total, AMI kernel version, region — fails before apply
```

### 5b. Multi-arch build pipeline (new requirement, Graviton botworkers)

`bot-fleet` (and only it — nothing else schedules on arm) must publish
linux/arm64 alongside linux/amd64:
- `docker buildx build --platform linux/amd64,linux/arm64` in CI, manifest-list
  pushed to ECR — node pulls the right arch automatically, overlays stay arch-blind.
- The bot-fleet image is Rust + librdkafka (Debian, libclang at build time per the
  known builder-image fix) — cross-compiles cleanly under buildx/QEMU; CI budget is
  the only cost.
- The worker deployment gains `nodeSelector: {pool: botworker}` it already has plus
  `kubernetes.io/arch: arm64` — nothing else changes; the Rust pacing/telemetry code
  is arch-independent.
- eBPF is explicitly NOT on these nodes (capture lives on x86 sandbox nodes), so no
  BPF arm build exists or is needed.
- Runbook gains one gate: `bot_worker_fix_roundtrip` example run on an arm node
  before the first real session (catches any arch-specific rdkafka/timing surprise).

Manifests: kustomize overlays `k8s/overlays/{local-k3s,eks-contest}` — local-only
patches (CoreDNS workaround, local registry, storage class) become structurally
unshippable; EKS overlay pins ECR digests, per-tier ALGO_CPU, storage classes.
GitOps (Argo) is deliberately OUT of scope for an ephemeral cluster — `kustomize build
| kubectl apply` from the runbook is sufficient and one less standing component.

## 6. Runbook (apply → contest → destroy, zero hand-steps as the acceptance bar)

1. `precheck.sh && terraform apply -var-file=tfvars/contest.tfvars`
2. CI (or `push-ecr.sh`) — images to ECR by digest; overlay references digests, not tags.
3. `kubectl apply -k k8s/overlays/eks-contest` — includes topic-init Job (4-partition
   status topic), gro-disable, secrets from SSM.
4. Verifier gate: capture Job on a sandbox node loads XDP+tc programs (kernel 6.1 check).
5. NetworkPolicy smoke: denied cross-namespace probe actually DENIED (hard gate, no gVisor).
6. **The FULL b1–b5 suite on EKS** (decided 2026-08-02 — not just b2): b1
   concurrency/leases on real multi-node scheduling, b2 two-pass grading (+ FINAL
   counters gaps=0/drops=0/throttled=0 at MTU 9001 — the jumbo regime's first live
   proof), b3 all three protocols over the VPC CNI, b4 stalled-peer with real
   network buffers, b5 KEDA + cluster-autoscaler against real node provisioning.
   Mechanism: the b-scripts stay unforked — lib-local.sh grows HARNESS_ENV=eks
   (ECR image lookups instead of docker inspect; contestant/stall-sink refs from
   ECR via push-contestants.sh). Then the sizing measurements (§6b M1–M5).
### 6b. Measurement campaign — the numbers that finish the sizing

This design deliberately leaves four capacities as measurement outputs, not guesses.
Order matters: M1 gates node count; M2/M3 share runs; M5 is the go/no-go.

| # | measures | method | output / gate |
|---|---|---|---|
| M1 | max TPS of ONE worker pod (arm, jumbo, ~3 dedicated cores) | drain-sink on a sandbox node, single worker, max-rate (existing `run-sentps` pattern) | per-pod ceiling → `botworker_max = ceil(worst-case aggregate ÷ M1) + 1` |
| M2 | capture ceiling, live | reference engine, offered rate stepped until FINAL counters degrade; per-thread CPU split (`iicpc-ebpf-late` vs `rdk:*`) decides which half to optimize if short | confirm the ~1M records/s the criterion benches predict; gaps=0/drops=0/throttled=0 at peak |
| M3 | telemetry path at ramp peak | same runs as M2 with telemetry on; flushed/s vs consumed/s, ingester group lag | ingester replica count (toward its 24-partition ceiling if lagging) |
| M4 | validator throughput | wall-time of one full-replay (1.8M orders) at 2 CPU + one invariants-mode ramp session | sessions/hour ≥ group arrival rate; confirms KEDA max 4 |
| M5 | full rehearsal | 4 concurrent run groups, contest-day shape, export job at the end | the go/no-go for contest day |

7. Contest-day: `terraform apply -var-file=tfvars/contest-day.tfvars` (sandbox 1→4).
8. Export: k8s Job dumps Postgres + Timescale + leaderboard snapshots to the S3 bucket;
   `terraform destroy` is gated on the export job's completion marker.

## 7. Open items folded in (not blockers for the tf skeleton)

- **W calibration (B6)** in the new regime — env value ships as "measured on this
  cluster", not inherited.
- Run-group scenario reduction (remaining-work §2) — SEED_SCENARIOS above assumes it.
- `P99AtPeakNS` tiebreak decision — scoring config, not infra.
- Karpenter — deliberately deferred; managed groups with min/max are sufficient at
  contest scale and one less moving part in an ephemeral cluster.
- MSK, gVisor, cpuset — decided off/off/off above; revisit only with cause.

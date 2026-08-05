# EKS bring-up 2026-08-02/03 — what happened, what it cost, what it proved

Record of the first real EKS bring-up of the rewritten platform (terraform-v2,
kustomize overlays). Written at teardown. Companion to
`docs/eks-contest-deployment.md` (design) and `docs/eks-bringup-handoff.md`
(the plan this deviated from).

**Goal as narrowed mid-session:** validate that the terraform + k8s configs are
correct on EKS — success criteria 1–3 only (build pipeline, suite green, jumbo
regime). Measurements M1–M5 were explicitly dropped.

**Outcome: partial. Criterion 1 proven, criterion 2/3 NOT.** Nine real
platform/config defects were found and fixed; the b2 gate never went green.
Cluster destroyed rather than burn more hours — every finding below is fixed
in the tree and re-testable on the next bring-up.

---

## 1. What was proven to work on EKS

- `terraform-v2` applies clean: VPC, EKS, 4 node groups (incl. Graviton arm64),
  ECR, IRSA, S3 backend + DynamoDB lock. 129 resources, no manual edits.
- `push-images.sh`: 13 amd64 images + the bot-fleet **multi-arch manifest list**
  (amd64+arm64) to ECR, immutable tags, overlay tag-stamping.
- `kubectl apply -k overlays/eks-contest` brings the platform up; all pods
  Running after the fixes below.
- **The build pipeline works end to end — first time ever, anywhere**: zip
  upload → validation → MinIO → Kafka → spawner → Kaniko build → SBOM scan →
  ECR push → `ready`. Both contestant submissions completed it.
- **A session runs end to end**: slot allocated, contestant pod scheduled and
  Ready, capture Job created and attached (tc egress; XDP falls back to skb
  mode on this AMI), bots connected and sent orders, session reached
  `completed`, `orders.sent` produced to Kafka.
- Cluster-autoscaler provisioned Graviton nodes on demand (accidental live test
  when the worker deployment briefly asked for 5 replicas).

## 2. What was NOT proven

- **`orders.acked` stayed 0 for every run** → no metrics rows (the ingester
  correctly writes none without responses) → no graphs. Root cause identified
  (§3.9) but the fix is UNVERIFIED against a live pod.
- **ProtocolAll (multi-port) submissions never became ready.** The
  single-port contestant (`smoke-rest-echo`) ran; the dual-listener
  (`reference-clob-all`, FIX 9898 + HTTP/WS 8080) sat in `deploying` until the
  deploy deadline, every time.
- b2's 11 assertions never executed; b1/b3/b4/b5 never ran.
- Jumbo-frame regime NOT validated — and in fact the capture still logged
  `clamped capture interface MTU 9001 -> 1500` (§4).

## 3. Defects found and fixed (all committed)

1. **No application Secrets existed.** Every deployment references Secrets that
   only `up-dev.sh` created; nothing in the manifest tree makes them, so 20
   pods hit `CreateContainerConfigError`. → `deploy-local/create-secrets.sh`
   (idempotent, env-neutral). auth-api needed all three `google-*` keys, not
   just client-id.
2. **Kafka shape.** Base carries the bench 3-broker statefulset; locally
   `up.sh` sed-patches it to 1. EKS had 2 kafka nodes → `kafka-2` Pending
   forever, and the KRaft voter list was wrong for the actual replica count.
   → `overlays/eks-contest/patch-kafka-single-broker.yaml`. Recovery required
   wiping the PVCs: a voter-set change is rejected against existing state.
3. **Topics vanished with those PVCs** and nothing recreated them (the
   topic-init Job was already `Complete`). Auto-create is off by design, so
   every publish/consume silently went nowhere — including the build request
   that made b2's first run hang. → re-run the Job after any broker wipe.
4. **Platform replicas 2 → 1** and **auth-api removed from the base** (auth is
   off platform-wide). Local `up.sh` had been scaling these down invisibly.
5. **bot-fleet-worker `replicas: 5` hardcoded** against KEDA's ownership (a
   known audit finding) → 2.
6. **Spawner pushed to ECR with Harbor basic-auth** (`anon/anon`) → 401.
   `REGISTRY_PROVIDER=ecr` activates the IRSA token path (`ecr_aws.go`), and
   `HARBOR_PROJECT` had to become `iicpc` to match the IAM push policy.
   Also needed the SA's IRSA role annotation.
7. **Slot-creation HTTP timeout 15s → 180s** (`ORCHESTRATOR_HTTP_TIMEOUT`).
   Slot creation blocks until the pod is observable, which on a real registry
   includes the FIRST image pull. Local pre-imports every image into
   containerd, so this path had never been exercised; on contest day EVERY
   submission is a cold pull. The failure surfaced as
   `get pod: ... context canceled` — the caller walking away, not a k8s fault.
8. **`DEPLOY_DEADLINE` 60s → 300s** for the same reason: scheduling (possibly
   waiting on a node that is still joining) + cold pull. Symptom was `slot did
   not become ready within 1m0s` while the container had already started.
9. **`SANDBOX_NODE_POOL` was never set** — `slot.go` treats empty as "no
   nodeSelector, no toleration", so contestant pods scheduled onto GENERAL
   nodes: beside Postgres/Kafka/validator, a second slot could not schedule at
   all, and the capture never co-located. This was the true root cause behind
   several earlier symptoms. Never exercised locally: one untainted k3s node
   makes pinning a no-op.
10. **NetworkPolicies had never run anywhere.** `deploy-local`'s `applynp()`
    skips every netpol file, and the previous EKS cluster predates
    `enableNetworkPolicy=true` (which terraform-v2 sets). Three distinct
    defects, each invisible until enforcement was real:
    - `ipBlock: 0.0.0.0/0` does **not** cover cluster-internal destinations
      under the AWS VPC CNI agent — the orchestrator's egress to the API
      server ClusterIP was silently dropped despite a catch-all rule. Fixed by
      listing cluster CIDRs explicitly. (Note: `spawner-isolation` already had
      an explicit `172.20.0.1/32` rule — someone had hit this before.)
    - The orchestrator's **own readiness dial** to a slot's extra ports was not
      permitted by the sandbox ingress rule (benchmark-only), so multi-port
      submissions could never become ready.
    - **Contestants could not answer the load generators**: the sandbox egress
      catch-all excludes private ranges, which is exactly where bot pods live.
      Bots stalled with `inflight` pinned and "abandoning stalled peer"; the
      capture's tc-egress hook saw no responses; `orders.acked` stayed 0.
      Fix committed, UNVERIFIED live.

## 4. Open items for the next bring-up

- **`orders.acked` = 0** — verify the egress fix produces acked events; if not,
  check capture netns targeting (`EBPF_ALGO_POD_UID` / container-id
  resolution) before assuming policy.
- **ProtocolAll slots never ready** — with a LIVE algo pod, dial 8080 from the
  orchestrator's netns. Engine binds both ports (verified in the zip source),
  so this is platform-side.
- ~~**Capture still clamps MTU 9001 → 1500**~~ — **RESOLVED 2026-08-03; this
  diagnosis was wrong.** Nothing in the tree sets `CAPTURE_CLAMP_MTU`. The
  capture image travels as the `CAPTURE_IMAGE` **env var**, which kustomize's
  `images:` transformer cannot rewrite, so EKS ran the stale public
  `ghcr.io/agrawalx/ebpf-latency:demo` — whose compiled-in default was still
  1500 (and `CAPTURE_CAP=1536`). `SPAWNER_IMAGE` had the same defect. Fixed in
  `overlays/eks-contest` via env patches + a `replacements:` block that derives
  both tags from the stamped image fields, plus a guard in `push-images.sh`.
  Full write-up in `docs/eks-bringup-handoff.md` §7c. Re-test `orders.acked`
  and the Kaniko 401 against correctly-tagged images before digging further.
- **`terraform apply` does not reconcile node-group `desired_size`** (module
  defers to autoscalers) — scaling must go through the EKS API or module
  config. Bit us when adding the second sandbox node.
- **b2 concurrency needs 2 sandbox nodes** (one slot per node by design);
  baseline ships 1.
- b1's cross-contestant identity question (auth off ⇒ both submissions share
  `DEFAULT_CONTESTANT_ID`) is still unresolved and blocks b1 on EKS.

## 5. Process lessons (cost real time here)

- **Never `grep -c` the output of `kubectl apply`.** It hid a rejected spawner
  patch and, later, four rejected NetworkPolicies — each hiding the next
  failure for a full debug cycle.
- **Do not machine-edit YAML with line-oriented scripts.** Two attempts
  corrupted netpols (an `except` list reattached to the wrong `ipBlock`;
  `ipBlock` entries spliced inside a `namespaceSelector`). Hand-edit, then
  verify against the API server.
- **Verify against live objects.** One diagnosis was drawn from dialing a pod
  that had already been deleted — the result was meaningless.
- Error messages point at the wrong layer: `context canceled` and
  `dial ... i/o timeout` both named the API server while the real causes were
  a caller timeout and a NetworkPolicy.
- The pattern behind nearly every finding: **local bring-up scripts reshape the
  manifests** (secrets, replica counts, broker count, image pre-import, netpols
  skipped). Anything they paper over is untested until EKS. Closing that gap —
  making local run the same tree through the same overlay mechanism — is the
  highest-value follow-up.

## 6. Cost

~5 hours of cluster time at roughly \$2/h baseline (7–8 nodes; briefly more
when the autoscaler added Graviton nodes), i.e. on the order of \$10–15, plus
NAT/ECR data transfer. Teardown followed immediately.

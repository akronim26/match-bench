# `production/` — clone → deploy → verify → run

One command brings up the platform on EKS, proves it grades correctly, and tears
it down again:

```bash
./production/contest.sh                      # provision -> deploy -> verify -> smoke
./production/contest.sh --from 03            # resume after a failure
./production/contest.sh --only 05            # just re-run verification
CONFIRM=yes ./production/contest.sh --teardown
```

Baseline cost is roughly **$2/hour** from the moment phase 01 completes.

---

## Why this exists

`e2e/` was meant to be this path. It stopped being it: it targets
`infra/terraform` (v1, last touched 2026-06-14, commit message *"probably
last"*), it never references `overlays/`, and it configures the cluster
**imperatively** — `kubectl set env`, `sed`-mutated manifests piped to apply,
inline heredocs.

That is not a style complaint. In the 2026-08-03 bring-up, **five of seven
defects were "the harness injects it, the overlay never created it"**: a stale
capture image that made the platform grade nothing while looking perfectly
healthy, a missing `auth-api` Service that CrashLooped the frontend, Kafka that
would not tolerate its own node's taint, an IRSA annotation nobody applied, and
a NetworkPolicy egress gap misdiagnosed for months as a ProtocolAll bug.

The overlay path is now the tested one — a contestant ran end to end, reference
engine **0.9921**, acker correctly disqualified at **0.0417**.

## Two rules

### 1. Scripts orchestrate; they never configure

No `kubectl set env`, `patch`, `scale`, `set image`, `annotate`; no `sed`-mutated
manifests; no heredoc manifests. If a deployment needs a value, it is declared
in `k8s/` or `overlays/` and arrives through `kubectl apply -k`.

**Secrets are the one exception** — they must not be in git. See
`secrets/schema.md`.

### 2. Self-contained by construction

`production/` must be able to outlive `e2e/`, `deploy-bench/` and
`infra/terraform` (v1), so it calls **no script outside itself**:

| kind | examples | what production/ does |
|---|---|---|
| declarative assets | `infra/terraform-v2/`, `k8s/`, `overlays/eks-contest`, tfvars | **uses them** — duplicating them would be worse than the disease |
| scripts | `push-images.sh`, `create-secrets.sh`, `e2e/*.sh`, `up-dev.sh` | **read, then reimplemented here** |

Both rules are enforced by `./production/check-declarative.sh`, which is
verified to fail on a violation rather than merely to pass. A rule enforced only
by discipline decays.

## Phases

| phase | does | why it is where it is |
|---|---|---|
| `00-preflight` | creds, quota, tooling, buildx/binfmt, assets, stale forwards | fails before spending money, not mid-nodegroup |
| `01-cluster` | `terraform apply` + kubeconfig + node/addon assertions | `-var enable_spawner_irsa=false` is **required** here — see below |
| `02-images` | build + push 14 images, stamp the overlay, **guard the stamp** | ECR repos are immutable; the guard is what catches env-carried images |
| `03-platform` | namespaces → secrets → `apply -k` → topics → rollouts | namespaces **must** precede secrets |
| `04-irsa` | second `terraform apply` + spawner restart | the SA only exists after phase 03 |
| `05-verify` | five gates, non-zero exit on any failure | "all pods Running" is not the property we need |
| `06-smoke` | one reference contestant, graded, asserted | the only phase that proves the platform *works* |
| `99-teardown` | destroy + orphan sweep + prove nothing bills | two steps exist purely to stop silent billing |

### The three ordering constraints

Each exists because the obvious order fails:

1. **Images before namespaces** — ECR repos come from terraform.
2. **Namespaces before secrets** — nothing else in the tree creates the six
   namespaces, and a namespaced Secret cannot be created into a missing
   namespace. The published guide used to say "secrets before the overlay",
   which only ever worked because the overlay had already been applied once —
   which is precisely how 20 pods reached `CreateContainerConfigError`.
3. **Terraform applied twice** — `kubernetes_annotations.spawner_sa_irsa`
   *patches* an existing ServiceAccount and has no `depends_on`, but
   `build-spawner` is created by the platform manifests. So phase 01 overrides
   `enable_spawner_irsa=false` and phase 04 runs without the override. The
   spawner must then be **restarted**: web-identity env is injected at pod
   creation, so a running pod never picks it up.

## What the gates actually check

A cluster can be entirely healthy by every ordinary measure and still grade
nothing. Phase 05 checks the properties that distinguish the two:

- **Image identity, read off the LIVE Deployments.** `CAPTURE_IMAGE` and
  `SPAWNER_IMAGE` travel as env vars, which kustomize's `images:` transformer
  cannot rewrite. This is the check that would have caught the stale `:demo`
  capture.
- **Placement.** `kafka-0` on the kafka node, the sandbox node clear for the
  contestant, `gro-disable` on every sandbox node, workers within botworker
  node count.
- **IRSA.** Without it every contestant build fails at `CreateRepository`.
- **Declared config.** `READY_DEADLINE`, `VALIDATION_TIMEOUT_MS`,
  `DEFAULT_CONTESTANT_ID` and `SANDBOX_NODE_POOL` — all of which existed only as
  Go defaults until 2026-08-03 and were injected by every harness.

Phase 06 additionally dials the contestant from inside the orchestrator's netns,
asserts `algo` and `capture` share a node, and asserts a **known-good** engine
scores ≥ 0.95 — so a low score means the platform is broken, not the engine.

## Scope

**In:** provision, deploy, verify, one graded reference submission, teardown.

**Out, deliberately:**

- **Auth.** Not a config flip: the frontend has auth *compiled out*
  (`authDisabled()` returns a literal `true`), there is no `/auth/callback`
  route, no call site sends a token, and there is no Ingress or TLS anywhere in
  the tree. Needs a frontend rebuild plus new infra.
- **Multi-tenancy.** `sha256` is globally `UNIQUE`, so a second contestant
  uploading an identical zip silently receives the first's submission and is
  then 404'd out of it. `ListRunGroups`/`GetRunGroup` have no ownership gate.
- **Managed secrets.** Development credentials by decision — see
  `secrets/schema.md`. One seam (`resolve_secrets()`) to replace.

## Known couplings to resolve at retirement

`production/` is free of *script* dependencies, but two asset dependencies point
at directories that may eventually be retired. Neither blocks anything today;
both need a decision before deleting those trees:

- `02-images.sh` builds `iicpc/drain-sink` and `iicpc/stall-sink` from
  **`deploy-local/drain-sink/`** and **`deploy-local/stall-sink/`**. These are
  measurement fixtures (Dockerfile + Go source), not scripts, so the guard does
  not flag them — but they would need to move here if `deploy-local/` goes.
- `fixtures/reference-clob-fix.zip` is copied from `deploy-local/`. Note the
  equivalent fixtures in `e2e/` are **stale** against their own sources
  (`contestant-matching-engine.zip` is 19 KB against a 52 KB `src/main.rs`) and
  nothing in the repo rebuilds them — which is why this path does not use them.

## Related

- `docs/eks-cluster-guide.md` — the prose companion; these scripts implement it
- `docs/remaining-work.md` — the full bug list, including the ones still open
  that change how results should be read

# IICPC — local deploy (clone-and-go)

Bring up the whole benchmarking platform on a single-node **k3s**, then submit your
matching engine as a zip **from the browser** and watch it get benchmarked.

Platform images are pulled from the public registry `ghcr.io/agrawalx/*:demo`, and
contestant images are built **inside the cluster** (kaniko → in-cluster registry).
So the only things you need locally are **k3s and kubectl** — no local Docker.

## Prerequisites

- A running **k3s** (or any single-node Kubernetes) and a `kubectl` context pointing at it.
  ```bash
  curl -sfL https://get.k3s.io | sh -        # installs k3s + kubectl
  sudo k3s kubectl get nodes                 # should be Ready
  # use it without sudo:
  mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config && sudo chown $(id -u):$(id -g) ~/.kube/config
  ```
- `python3` (used by the helper scripts).
- Recommended **≥ 8 CPU cores** — the default seed drives 25,000 orders/s constant
  (spike/ramp peak 50,000). On a smaller box, lower the rates (see *Tuning the load* below).

## Quickstart

```bash
deploy-local/up.sh          # create everything, pull images, seed scenarios (~3–5 min first run)
```

**One-time registry trust (root).** Contestant submissions are built in-cluster and
pushed to an in-cluster registry over plain HTTP; the k3s node must be told to trust it.
The first `up.sh` run *generates* `deploy-local/registries.yaml` for your node's IP and
warns if it isn't installed. Install it once and restart k3s, then re-run `up.sh`:

```bash
sudo cp deploy-local/registries.yaml /etc/rancher/k3s/registries.yaml
sudo systemctl restart k3s
deploy-local/up.sh          # re-run after k3s is back
```

(Platform comes up without this, but *submitting* an engine would fail to pull until it's done.)

```bash
deploy-local/forward.sh     # port-forward the UI + dashboards (leave running)
```

Then open **http://localhost:8080**, go to **Submit**, upload your zip, and click
through to the run — throughput, latency (P50/P99), round-trip and correctness appear live.

| URL | What |
|-----|------|
| http://localhost:8080 | Frontend — submit, leaderboard, run detail |
| http://localhost:3000 | Grafana (admin / admin) — throughput + latency dashboards |
| http://localhost:9090 | Prometheus |

Auth is **off**: every visitor is the contestant `echo-contestant`, so you can submit
and re-run without logging in.

## Your submission zip

Zip the project so `benchmark.yaml` is at the **root** and your code is under `src/`:

```
your-engine.zip
├── benchmark.yaml
└── src/...                # + go.mod / Cargo.toml / CMakeLists.txt as needed
```

`benchmark.yaml`:

```yaml
protocol: FIX            # FIX | REST | WS
language: cpp            # cpp | rust | go
port: 9898               # MUST be 9898 or 8080 (the eBPF capture only filters these)
team_name: my-team
build:
  type: cmake            # cmake (cpp) | cargo (rust) | go
  target: matching_engine   # cmake: the add_executable() name; go: ignored
```

Your engine must listen on `port` and answer every order with a response that echoes the
order id, so the eBPF capture can pair request↔response and measure service time. A worked
example (a C++ price-time-priority book behind a FIX front-end) is in
`deploy-local/cpp-matching-engine.zip`.

## Tuning the load

Scenarios (`constant`, `spike`, `ramp`) are **rebuilt from env on every `submission-api`
boot** while `RESEED_SCENARIOS=true`. Edit the values in `up.sh` step 8, or live:

```bash
kubectl -n platform set env deployment/submission-api \
  CONSTANT_TOTAL_RPS=10000 SPIKE_PEAK_RPS=10000 RAMP_PEAK_RPS=10000   # then it restarts
```

### The knobs (env → `submission-api`)

| Env | Default (`up.sh`) | Meaning |
|-----|-------------------|---------|
| `SEED_SCENARIOS`     | `constant` | CSV allowlist of which scenarios to seed (`constant,spike,ramp`) |
| `CONSTANT_TOTAL_RPS` | `25000` | steady-state order rate for `constant` |
| `SPIKE_PEAK_RPS`     | `50000` | burst peak for `spike` |
| `RAMP_PEAK_RPS`      | `50000` | top-of-ramp rate for `ramp` (9 waves) |
| `CONSTANT_DURATION_S` / `SPIKE_DURATION_S` / `RAMP_DURATION_S` | `60` / `60` / `180` | scenario wall-time (seconds) |
| `SPIKE_PREWINDOW_S` / `SPIKE_BURST_S` | `25` / `10` | baseline lead-in, then burst window inside `spike` |

### How a total RPS becomes tasks

Each scenario's RPS budget is split across a **fixed traffic mix** — 60 % HFT + 25 %
retail + 15 % institutional — at fixed per-bot rates (HFT 1000, retail 5, institutional
300 rps). One *task* = one bot. The mix and per-bot rates are **compile-time constants**
in `services/submission-api/internal/scenarios/builder.go`, not env. Only the totals and
durations are configurable. Task count therefore falls out of the total:

```
tasks(R) = R·0.60/1000  +  R·0.25/5  +  R·0.15/300
         =    hft        +   retail    +   institutional
```

Retail bots (5 rps each) dominate the *count*. Worked example, `CONSTANT_TOTAL_RPS=25000`:
`15 hft + 1250 retail + 12 inst = 1277 tasks`.

### The sharding limit (why task count matters)

The bot-fleet-controller shards tasks across worker pods:
`worker_count = ceil(total_tasks / MAX_TASKS_PER_WORKER)` (default `MAX_TASKS_PER_WORKER=1000`,
controller env). **`bot-fleet` replicas must be ≥ `worker_count`**, and the
`workload.assignments` topic must have ≥ `worker_count` partitions — otherwise a shard is
never assigned and *that share of the load is silently dropped* (a run looks fine but
delivers half the orders).

`up.sh` scales `bot-fleet` to **1**, so the safe ceiling is **≤ 1000 tasks**, i.e.
`CONSTANT_TOTAL_RPS ≤ ~19000` for the default mix. Above that either:

- scale out: `kubectl -n benchmark scale deploy/bot-fleet --replicas=N` (N ≥ `worker_count`,
  and repartition `workload.assignments` to ≥ N), or
- keep one worker but use a **pure-HFT override** (below) so the task count stays small.

### Pure-HFT override (high rate, few tasks)

For a clean high-throughput run on one worker, replace the mix with N HFT bots at 1000 rps
(N tasks = N·1000 rps). This writes the scenario row directly, so first turn off reseed or
the next `submission-api` restart rebuilds the mix:

```bash
kubectl -n platform set env deployment/submission-api RESEED_SCENARIOS=false

# 25 HFT tasks @ 1000 rps = 25000 rps, 25 tasks (≤1000 → 1 worker)
kubectl -n data exec postgres-0 -c postgres -- psql -U iicpc -d iicpc -c "
UPDATE scenarios SET task_specs = (
  SELECT jsonb_agg(jsonb_build_object(
    'task_id', g, 'profile','hft', 'target_rps',1000,
    'start_offset_ns',0, 'duration_ns', duration_ns,
    'market_pct',10, 'cancel_pct',30, 'replace_pct',10))
  FROM generate_series(0, 24) g)
WHERE name='constant';"
```

Bump `generate_series(0, N-1)` for N tasks / N·1000 rps.

## Teardown

```bash
for ns in benchmark sandbox build platform observability data; do kubectl delete ns $ns; done
```

## Notes / gotchas

- **Single-node artifact:** intra-node pod traffic is veth-switched, so the eBPF capture
  logs some `truncated_captures` (GSO super-segments > the 1536 B cap). It's ~1% sampled
  loss and disappears on a real multi-node cluster; safe to ignore locally.
- **Contestant throughput:** a deep order book / unbounded engine state shows up as a
  throughput ceiling — the bot's `iicpc_bot_inflight` metric and 1s `bot send snapshot`
  pod logs make it visible. That's the engine, not the platform.

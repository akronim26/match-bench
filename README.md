<div align="center">
  <img src="frontend/public/logo.png" alt="match-bench logo" width="100%" />

  <h3>Fair, repeatable benchmarking for high-frequency trading algorithms.</h3>
  <p><em>A benchmark is only useful if nobody can game the measurement.</em></p>
</div>

---

**match-bench** lets participants upload trading algorithms, runs each submission in an isolated environment, sends every participant the same deterministic market workload, measures latency outside the participant's code, validates correctness, and publishes scores on a live leaderboard.

## Table Of Contents

- [Overview](#overview)
- [Benchmark Snapshot](#benchmark-snapshot)
- [Key Features](#key-features)
- [How It Works](#how-it-works)
- [Screenshots](#screenshots)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Deployment (AWS EKS)](#deployment-aws-eks)
- [Architecture Document](#architecture-document)
- [Demo Video](#demo-video)
- [Design Notes](#design-notes)

## Overview

match-bench is a multi-service benchmark platform for evaluating untrusted trading algorithms. A submitted algorithm is built into a container, deployed into a locked-down Kubernetes pod, driven by deterministic benchmark traffic, measured at the Linux network boundary, checked against a correctness model, and scored.

## Benchmark Snapshot

The figures below are **measured on EKS** (or the local k3s HDR harness where noted), not projected. Methodology, the raw data files, and the bottleneck hunt behind each number are in the [architecture doc's benchmarking section](docs/architecture/ARCHITECTURE.md#benchmarking-bottleneck-hunt--scope-for-improvement).

| Measurement | Result |
|---|---:|
| Order generation, telemetry **off** (drain), per botworker node | **~600–790k orders/sec** (peak ~748k on one `c6i.xlarge`) |
| Order generation, telemetry **on** (single worker → 1 broker) | **~445k orders/sec**, lossless |
| Measurement pipeline (eBPF capture → Kafka → ingester) | lossless **≥ ~144k samples/sec** (own ceiling not yet reached) |
| Single echo-contestant pod, cross-node | ~150k delivered/sec (caps before the pipeline) |
| Kernel-stamped service-time p99 (healthy) | **~98 µs** |
| Platform self-benchmark tier (target) | **~2M orders/sec** — 3 botworker nodes, 2-broker Kafka, 96 partitions, 8 ingesters |

The load generator is not the bottleneck at contest scale: a single node sustains hundreds of thousands of orders/sec, and the data plane scales out across Kafka partitions (24 in prod, 96 in the bench tier). The 2M/s tier is a documented, *partially-validated* target — the drain path (generation + telemetry + ingester) is ready to validate; eBPF-capture and echo-contestant capacity at 2M/s are still unverified. Revalidate on the exact node type, kernel, networking mode, and Kafka settings used for the event.

## Key Features

- **Untrusted-code sandboxing**: participant algorithms run in isolated Kubernetes pods with hardened security settings.
- **Deterministic workloads**: every participant receives the same logical test stream for a given seed and workload mix.
- **Kernel-side latency measurement**: request and response timing is captured outside the participant's process using Linux eBPF hooks.
- **Correctness validation**: outputs are replayed through a reference model before a score is accepted.
- **Latency histograms**: telemetry is aggregated into HDR histograms for percentile-based analysis.
- **Live leaderboard**: APIs and frontend expose scores, run details, and live updates.
- **Cloud-ready deployment**: Terraform and Kubernetes manifests support AWS EKS deployment.
- **Local development stack**: Docker Compose brings up the core data, Kafka, platform, and observability dependencies.

## How It Works

For each submission, match-bench follows this flow:

1. The participant uploads an algorithm.
2. The build system creates a runnable container image.
3. The sandbox orchestrator starts the algorithm in an isolated pod.
4. The bot fleet sends deterministic market traffic to the algorithm.
5. The latency service records when requests enter and responses leave the pod.
6. Telemetry services aggregate latency and throughput metrics.
7. The correctness validator checks whether the algorithm behaved correctly.
8. The score computer produces the final leaderboard score.

The important detail is where measurement happens. match-bench does not ask the submitted algorithm how fast it was. It observes network traffic from outside the algorithm and computes latency from those observations.

## Screenshots

A completed run in the contestant UI — scored kernel-stamped service time vs the full round trip, the HDR latency histogram, the live throughput timeline, and per-scenario (constant / spike / ramp) verdicts:

![Run detail page](docs/graph1.png)

The latency-by-percentile decomposition: the flat lower curve is the scored algo service time (`t7 − t3`); the rising tail is the full round trip (`r9 − t0`). The gap between them is non-algo overhead — coordinated omission + network + kernel queueing:

![Latency by percentile distribution](docs/graph2.png)

## Architecture

The full, **code-derived** architecture lives in **[docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md)** — a standalone deep reference with a section per microservice, the Kafka topology and partitioning model, the horizontal-scaling design, the benchmarking numbers, the bottleneck hunt, and the consolidated open items. It was written by reading the source directly, with every non-obvious claim cited to a `path:line`.

It contains fresh, code-derived diagrams: a system-context map, the run-lifecycle sequence, the Kafka topic graph, and the node-pool isolation model. [`design.md`](./design.md) remains the detailed engineering-rationale companion.

## Tech Stack

| Area | Technology |
|---|---|
| APIs and orchestration | Go |
| Load generation and telemetry | Rust |
| Frontend | Next.js |
| Event bus | Kafka |
| Metadata storage | PostgreSQL |
| Time-series metrics | TimescaleDB |
| Hot snapshots | Redis |
| Artifact storage | MinIO / S3 |
| Runtime isolation | Kubernetes |
| Kernel measurement | eBPF, XDP, tc |
| Observability | Prometheus, Grafana, Loki |
| Cloud infrastructure | Terraform, AWS EKS, ECR, IRSA, KEDA |

## Repository Layout

```text
frontend/                  Web application
services/                  Backend, benchmark, telemetry, and scoring services
libs/                      Shared Go and Rust libraries
schemas/                   Shared event and Kafka topic schemas
k8s/                       Kubernetes manifests
infra/                     AWS Terraform and deployment Makefile
ops/                       Prometheus, Grafana, Kafka, and operational config
bootstrap/                 Secret templates and bootstrap notes
docs/                      Local run and cloud deployment guides
design.md                  Detailed engineering design
```

## Getting Started

There are two local paths, both current:

**1. Dependency stack + services from source (fast dev loop).** Bring up Postgres, TimescaleDB, Redis, Kafka, and MinIO (optionally the platform APIs and observability too) with Docker Compose, then run individual services from source. Full steps and the local endpoint table are in **[deploy-local/README.md](./deploy-local/README.md)**.

```bash
cp .env.example .env
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d   # core deps + Kafka topic init
```

**2. Full platform on local k3s (no-cost end-to-end demo).** [`deploy-local/up.sh`](./deploy-local/up.sh) deploys the *entire* stack — data tier, all services, eBPF capture, telemetry, validator, and frontend — onto a single local k3s node (1-broker Kafka, all replicas 1, gVisor off, tiny scenarios). This mirrors the EKS end-to-end run without the cloud cost and is the demo path.

```bash
deploy-local/up.sh        # builds images into the in-cluster registry, then applies the platform
```

## Testing

Test commands are maintained separately:

- [Testing command reference](./docs/testing_commands.md)

The test guide covers Go tests, Rust tests, frontend checks, integration tests, and Kubernetes smoke checks.

## Deployment (AWS EKS)

The **recommended end-to-end path is the [`e2e/`](./e2e/) suite** — clone → Terraform → deploy → run a real benchmark with every service on (load generator, eBPF capture, telemetry, validator, scoring, leaderboard). It is the most current and complete guide:

- **[e2e/README.md](./e2e/README.md)** — topology, contestants, scenarios, the required capture-fidelity setup, validated ceilings, and the full run order.
- **[e2e/cluster.md](./e2e/cluster.md)** — Terraform apply/destroy, kubeconfig, and re-scaling (you run Terraform).

```bash
cd infra/terraform && AWS_PROFILE=iicpc terraform apply -var-file="$(git rev-parse --show-toplevel)/e2e/e2e.tfvars"
AWS_PROFILE=iicpc aws eks update-kubeconfig --name iicpc-prod --region us-east-1
e2e/01-images.sh && e2e/02-bootstrap.sh && e2e/04-scenarios.sh   # then run via the UI, or e2e/run.sh <scenario>
```

For deeper or alternative needs:

- **[docs/aws-deployment.md](./docs/aws-deployment.md)** — the detailed, manual EKS reference (node groups, network policies, IRSA, KEDA, ALB controller, secrets, dependency-correct deploy order, gVisor).
- **Scaling tiers:** [`bench/`](./bench/) drives the ~2M orders/s platform self-benchmark (multi-broker Kafka, 96 partitions, 8 ingesters); [`deploy-bench/`](./deploy-bench/) runs the load-generator capacity sweep (drain sink, 1→2 nodes).
- **Supporting:** [infra/README.md](./infra/README.md) and the [secret bootstrap guide](./bootstrap/README.md).

> **Free-plan caveat:** new AWS Free-plan accounts cap nodes at 2 vCPU, which cannot run the platform (the data tier alone needs ~5 vCPU and the exclusive-core sandbox needs ≥4 vCPU). Use a paid account for EKS, or the local k3s path above for a no-cost demo. See the [architecture doc's deployment section](docs/architecture/ARCHITECTURE.md#deployment-isolation--observability) for details.

## Architecture Document

- **[Platform Architecture](docs/architecture/ARCHITECTURE.md)** — canonical, code-derived, standalone
- [DeepWiki](https://deepwiki.com/akronim26/match-bench) — community-generated; mostly correct but ~2–3 days behind on the latest benchmarking and bug-fix work
- [Google Doc](https://docs.google.com/document/d/13tPoT2Bfk82HeOVpX94-9zFqoKTqFExBVFwyRXRliBk/edit?usp=sharing)


## Design Notes

The detailed system design is documented in [design.md](./design.md). It explains the measurement model, deterministic workload generation, sandboxing strategy, telemetry pipeline, correctness validation, and scoring model.

## Development Team

- [@agrawalx](https://github.com/agrawalx)
- [@akronim26](https://github.com/akronim26)

# Secret contract

The 13 Secrets every deployment references. Derived from the `secretKeyRef`
entries across `k8s/` — this is the authoritative list, and a missing key is a
`CreateContainerConfigError`, not a runtime error, so the pod never starts.

Secrets are the ONE thing `production/` creates imperatively. That is correct:
they must not be in git. Everything else is declared in `k8s/` + `overlays/`.

## Status: development credentials

**These are development values and the platform is not credential-hardened.**
That is a deliberate, recorded decision, not an oversight: authentication is out
of scope for the current `production/` path (see `production/README.md`), and a
half-built secret story is worse than an obviously-dev one.

Replacing them is a single seam — `resolve_secrets()` in `load.sh`. Everything
below it reads variables and does not care where they came from.

## The secrets

| # | Secret | Namespace | Keys | Notes |
|---|---|---|---|---|
| 1 | `postgres-secret` | data | `password` | **rotation hazard** — see below |
| 2 | `timescaledb-secret` | data | `password` | same hazard |
| 3 | `minio-secret` | data | `access-key`, `secret-key` | |
| 4 | `kafka-secret` | data | `cluster-id` | **create-once** — see below |
| 5 | `submission-api-secret` | platform | `database-url`, `minio-access-key`, `minio-secret-key` | |
| 6 | `auth-api-secret` | platform | `google-client-id`, `google-client-secret`, `google-allowed-redirect-uris` | **create-once**; required even with auth OFF |
| 7 | `leaderboard-api-secret` | platform | `database-url`, `timescale-url`, `kafka-brokers`, `redis-addr` | |
| 8 | `spawner-secret` | build | `database-url`, `minio-access-key`, `minio-secret-key`, `harbor-staging-endpoint`, `harbor-production-endpoint`, `harbor-user`, `harbor-password` | endpoints are the ECR registry on EKS |
| 9 | `bot-fleet-controller-secret` | benchmark | `database-url` | |
| 10 | `correctness-validator-secret` | benchmark | `kafka-brokers`, `database-url` | |
| 11 | `score-computer-secret` | benchmark | `database-url`, `timescale-url`, `kafka-brokers`, `redis-addr` | |
| 12 | `telemetry-ingester-secrets` | benchmark | `KAFKA_BROKERS`, `TIMESCALE_URL`, `REDIS_URL` | **UPPER-CASE keys**, unlike every other secret |
| 13 | `grafana-admin` | observability | `admin-user`, `admin-password` | |

## Three behaviours that are load-bearing

**`kafka-secret` is create-once.** `cluster-id` is the KRaft cluster identity.
Regenerating it against an initialised log dir bricks the broker — it refuses to
start with a cluster-id mismatch and the PVC must be wiped, which loses every
topic (auto-create is off, so publishes then silently go nowhere until
`kafka-topic-init` is re-run).

**`auth-api-secret` is create-once, and must exist even with auth OFF.**
`submission-api` reads `google-client-id` out of it unconditionally
(`k8s/platform/submission-api/deployment.yaml`), so without the Secret the pod
never starts — the app's own `mustEnv` is not what fails, kubelet is. The
create-once guard is what stops a re-run clobbering real Google credentials once
they exist.

**`postgres-secret` / `timescaledb-secret` rotation is not idempotent.** The
StatefulSet only consumes `POSTGRES_PASSWORD` when it *initialises* PGDATA. Once
the volume exists, rewriting the Secret changes what every client sends while
the database still expects the old password — every DSN breaks at once and
nothing reports why. `load.sh` therefore treats an existing password as
authoritative rather than overwriting it.

## Upgrading to managed secrets

Replace `resolve_secrets()` in `load.sh`. The intended target, already named in
`bootstrap/README.md`, is **External Secrets Operator + AWS Secrets Manager**
(or Sealed Secrets). Everything downstream of `resolve_secrets()` — the 13
`apply_secret` calls, the create-once guards — stays as-is.

One thing to clean up when that happens:
`bootstrap/secrets-live/platform-auth-api.yaml` currently holds a real Google
OAuth client id and secret in plaintext on disk. It is correctly gitignored and
never committed, but it should be rotated and removed rather than migrated.

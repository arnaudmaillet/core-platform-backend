# CLAUDE.md

Guidance for working in `core-platform-backend`. Keep this file short and factual — it is
loaded into every session. Deep detail lives in `docs/` (linked below).

## What this is

A single Rust workspace (`crates/`) for a hyperscale, event-driven social backend:
~17 services as hexagonal (DDD) crates, each shipped as one or more per-binary
container images. Sync contracts are versioned gRPC (`*-api` crates); async
contracts are Kafka topics governed by the `event-topology` registry.

> **Infra & GitOps live in a separate repo:**
> [`arnaudmaillet/core-platform-infra`](https://github.com/arnaudmaillet/core-platform-infra)
> (Terraform/Terragrunt + EKS + Karpenter + ArgoCD + the Kustomize overlays; staging
> syncs from *its* `develop`, prod from *its* `main`). This repo builds immutable
> `:<git-sha>` images; the fleet CI pins them into the infra repo. Split from the
> former `core-platform` monorepo on 2026-10-04 (history preserved in both).

## Repo layout

| Path | What |
|---|---|
| `crates/foundation`, `crates/platform`, `crates/storage` | shared libs (error, health, resilience, cqrs, transport, telemetry, auth-context, Postgres/Scylla/Redis adapters, `service-runtime`, `test-support`) |
| `crates/services/<svc>` | per-service hexagonal crate (`domain → application(ports) → infrastructure(adapters)`) |
| `crates/apps/<svc>-server` (and `-worker`) | thin deployable binaries (`serve::<XService>(addr)`) |
| `crates/contracts/<svc>-api`, `crates/contracts/proto` | gRPC contract crates + protos |
| `crates/contracts/event-topology` | **authoritative Kafka producer/consumer registry** (golden-tested; generates `docs/domain/EVENT_CATALOG.md`) |
| `deploy/` | `Dockerfile` (one image per binary) + `docker-bake.hcl` (CI packaging) |
| `local-dev/` | docker-compose fleet for local frontend testing |
| `docs/` | ADRs, architecture (C4), domain (event catalog, context map), i18n, templates |

## Common commands

```bash
# Build / test / lint the workspace
cargo build --workspace
cargo clippy --workspace --all-targets
cargo test  --workspace                 # unit tests; integration tests are feature-gated:
cargo test -p <svc> --features integration-<svc>   # needs Docker (Scylla/Redis/Kafka/PG containers)

# Contracts
( cd crates/contracts/proto && buf lint )          # buf breaking runs in CI on PRs;
                                                   # merges to develop/main publish to
                                                   # buf.build/core-platform/contracts (needs BUF_TOKEN)

# Build a service image (one image per binary)
docker build -f deploy/Dockerfile --build-arg BIN=<svc>-server -t <repo>/<svc>-server:<tag> .
# (CI instead compiles all binaries once per arch and bakes runtime images: deploy/docker-bake.hcl)

# i18n drift gate (MUST pass — see i18n rule below)
bash tools/i18n/i18n-drift.sh check
bash tools/i18n/i18n-drift.sh stamp <file>.fr.md   # re-stamp after editing an EN source

```

## Service & gRPC port registry

`chat` 50051 · `profile` 50052 · `social-graph` 50053 · `geo-discovery` 50054 ·
`notification` 50055 · `post` 50056 · `comment` 50057 · `engagement` 50058 ·
`account` 50059 · `auth` 50060 · `moderation` 50061 · `search` 50062 · `media` 50063 ·
`counter-server` 50064 · `counter-worker` 50065 · `realtime-gateway` 50066 (gRPC) + **8443** (public WSS) ·
`realtime-dispatcher` 50067 · `audit-server` 50068 · `audit-worker` 50069 · `timeline` **50070** ·
`media-worker` 50071 (health/reflection only — video transcode consumer, no domain RPC)

One port per service. (`timeline` was 50060, moved to 50070 to clear a collision
with `auth` — PR #522.) Every client-facing server also runs the **client edge**
listener on **9443** (`GRPC_EDGE_ADDR`): the ALB-facing, token-authenticated,
allow-listed listener (see the edge rule below). Each service owns an error-code namespace, e.g. `TML-`
(timeline), `SCH-` (search), `MED-` (media), `CTR-` (counter), `AUD-` (audit),
`RTM-` (realtime), `SGR-`, `PST-`, etc.

## Conventions (do these)

- **Service shape:** hexagonal — keep domain pure, put I/O behind `application/port`
  traits, implement in `infrastructure`. New binaries are thin: read addr from
  `<SVC>_GRPC_ADDR`, call `service_runtime::serve::<XService>(addr)`.
- **Kafka consumers MUST use `run_consumer`** (manual commit after success/DLQ,
  backoff+jitter, domain idempotency). Never hand-roll a consume loop.
- **Async contracts go through the `event-topology` registry** — adding a
  producer/consumer edge means editing the registry (a contract test fails on a
  "phantom edge"); then regenerate the catalog (`tools/event-catalog/sync.sh`).
  The registry is also what provisions the brokers: the `topic-provisioner`
  PreSync Job creates every stream topic + `.dlq` (MSK runs with topic
  auto-creation off) — a registry edit is the whole workflow.
- **Service tiers** are an explicit runtime contract (pod label `tier:`): TIER-0 =
  fail-closed (`auth`, `moderation`, `audit`); TIER-1 = fail-open
  (`counter`, `media`, `search`, `realtime`). Respect the posture when adding code.
- **Client edge (public gRPC):** the mesh port trusts the network; the **edge
  listener** (`:9443`, behind the ALB) trusts only the `auth` edge token. A
  service exposes an RPC publicly by listing it in `Service::EDGE_POLICY`
  (`service.rs`; unlisted = `UNIMPLEMENTED` on the edge) **and** binding the
  request's actor field in the handler: `edge::require_account` (account-id
  field must equal the token `sub`) or `edge::require_profile` (profile-id field
  must be in the token's `pids`). Never read the actor from the request alone on
  an edge-exposed RPC. Only `auth.Login`/`Refresh`/`StartGuestSession` are `public`;
  staff/admin RPCs stay off the edge until a permission catalogue exists. **Guest
  tokens** (`kind = guest`, `sub = guest:<id>`) are refused on every `authenticated`
  rule and only reach `permission(…, "read:public")` rules (members carry `read:public`
  too). Reads that depend on who
  is reading (drafts, private profiles, blocks) take the reader from `edge::viewer`
  (`Internal` = mesh, unfiltered; `Anonymous`; `Member` + `pids`), never from a
  request field.

## Cross-repo contract (with `core-platform-infra`)

- **`develop` is protected** — branch off it, open a PR. Since the split no bot
  commits land here: the fleet CI (`fleet-images-deploy.yml`) builds every binary
  on a push to `develop`, then pins the `:<git-sha>` tags into the infra repo's
  `k8s/overlays/staging` (deploy key `PLATFORM_PIN_DEPLOY_KEY`). ArgoCD rolls out
  from there.
- **Adding a binary / service** (order matters): infra repo first — ECR repo in
  `live/global/artifacts/ecr` **applied**, manifests, overlays' `images:` entry,
  NetworkPolicy; then here — crate, `crates/apps/<bin>`, `FLEET_BINS`, port.
  A push before the ECR repo exists fails the fleet build.
- **Coupled change** (new env var, secret, Kafka consumer needing a scaler):
  infra side first with a tolerant default, then the code; cross-link the PRs
  (`arnaudmaillet/core-platform-backend#N` ↔ `arnaudmaillet/core-platform-infra#M`).
- **Kafka topics** need no infra PR: the infra repo's `topic-provisioner` PreSync
  Job runs this repo's registry (pinned image).
- **Image tags** are immutable `:<git-sha>`; never introduce a floating tag in
  either repo. GitOps/IaC rules (apply order, EKS version, Kustomize CRD refs,
  state keys) are in the infra repo's `CLAUDE.md`.

## i18n rule

English is canonical; French is a co-located `*.fr.md` whose YAML frontmatter
records the SHA-256 of the EN source it was translated from. **If you edit an EN
doc that has a `.fr.md`, update the FR too and re-stamp** (`i18n-drift.sh stamp`),
or CI fails. Contracts (error codes, env vars, topic names, identifiers) stay in
English inside FR files. See `docs/i18n/`.

## Key references

- Docs entry point & taxonomy: `docs/README.md`
- Infra, GitOps, runbooks, NetworkPolicy call graph: `core-platform-infra` → `docs/README.md`
- Event plane (who produces/consumes what): `docs/domain/EVENT_CATALOG.md`
- Domain context map / ubiquitous language: `docs/domain/`
- Architecture decisions: `docs/adr/`

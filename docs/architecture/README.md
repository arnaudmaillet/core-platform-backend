# Architecture (C4) — regenerated

This directory holds the **corrected C4 model**, expressed as a Structurizr workspace
([`workspace.dsl`](./workspace.dsl)).

> **Derived artifact.** This model is *generated from* the domain documentation — it is not a source
> of truth. The truth is the code plus [`docs/domain/CONTEXT_MAP.md`](../domain/CONTEXT_MAP.md) and
> the per-service Domain Cards (`crates/services/<svc>/docs/DOMAIN.md`). If the diagram disagrees
> with those, the diagram is stale — regenerate it, don't trust it.

It **supersedes** the pre-fleet model (since removed), which described an architecture that never
shipped — phantom services, wrong stack, 7 missing services.

## What's modelled

- **System Context** — the platform and its external dependencies (Keycloak, S3/MinIO, CloudFront, APNs/FCM, KMS/witness).
- **Containers** — the 17 services (realtime split into its gateway + dispatcher), the shared datastore technologies (ScyllaDB / PostgreSQL / Redis / OpenSearch), and the Kafka event backbone.
- **Dynamic** — the "post published → fan-out" flow as a worked example.
- Service shape encodes role (service / worker / edge); colour encodes subdomain class (**Core** red, **Supporting** blue), both taken from `CONTEXT_MAP.md`. Async edges (through Kafka) are dashed; sync gRPC edges are solid.

## Modelling choices

- **One container per service.** Dual-binary services (audit, counter, search, notification) are noted in their description; `realtime` is split into `realtime-gateway` and `realtime-dispatcher` because the public WebSocket edge is architecturally distinct.
- **Datastores are one container per technology.** Per-service isolation (keyspace / database / namespace) lives in each Domain Card, not here.
- **Async routes through Kafka.** Producer→Kafka and Kafka→consumer edges carry the topic names; the producer→consumer *semantics* live in [`EVENT_CATALOG.md`](../domain/EVENT_CATALOG.md).
- Synchronous client read/write API traffic enters via the platform ingress (see [`docs/infrastructure`](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md)) and is not enumerated, to keep the container view readable.

## Rendering

Render with the [Structurizr CLI](https://structurizr.com/help/cli) or by importing `workspace.dsl`
into [Structurizr Lite](https://structurizr.com/lite). Run from the repo root:

```bash
docker run --rm -it -p 8080:8080 \
  -v "$PWD/docs/architecture:/usr/local/structurizr" \
  structurizr/lite:2025.05.28
```

Then open <http://localhost:8080/>. Lite re-parses `workspace.dsl` on each request, so edits show
up on refresh — no restart needed. On Apple Silicon, add `--platform linux/amd64` if the image tag
you pull is amd64-only. Lite writes a derived `workspace.json` (gitignored) next to the DSL.

## Keeping it true

When `CONTEXT_MAP.md` or a Domain Card changes a relationship, store, or subdomain class, update
`workspace.dsl` in the same change. The model is small and hand-maintained on purpose; a future
generator could emit it from the Domain Cards + the event-topology registry guard.

> **DSL syntax gotcha.** Structurizr DSL is line-based: **one statement per line**, and blocks
> (`element "Tag" { … }`, relationships) span multiple lines. There is no `;` separator. Packing
> two relationships onto one line with `;`, or collapsing a style block onto a single line, makes
> the parser fail with `Too many tokens`. Keep each statement on its own line.

> 🇫🇷 Miroir français : [`README.fr.md`](./README.fr.md).

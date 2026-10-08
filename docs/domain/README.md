# Domain & Functional Documentation

> **Purpose.** This directory holds the **cross-context** functional documentation for the
> platform — the facts that *no single service can keep true on its own*. Per-service domain
> docs live **with their crate**, at `crates/services/<svc>/docs/DOMAIN.md`; this directory only
> holds what spans contexts and indexes the rest.

## The split rule

> **Document each fact where it can become wrong.** If exactly one bounded context can make a
> statement false, it lives in that crate's `DOMAIN.md`. If two or more contexts must agree for
> it to be true, it lives here.

| Lives **with the crate** (`crates/services/<svc>/docs/DOMAIN.md`) | Lives **here** (`docs/domain/`) |
|---|---|
| Bounded context purpose, non-goals | The **context map** (relationships between contexts) |
| Aggregates, invariants, state machines | Cross-context **ubiquitous language** |
| Data ownership (what this context is SoR for) | The semantic **event catalog** |
| Per-context glossary, workflows | This index |

## Ground truth

The source of truth for *what exists* is **the code**, never a diagram:
- the crates — `crates/services/*`, `crates/apps/*`;
- their `*.v1` proto contracts;
- the **event-topology registry guard** (machine-checked, therefore trustworthy).

All documentation here is *derived from* those. The legacy pre-fleet C4 model has been **removed**; a
corrected C4 model has been *regenerated from* the Domain Cards + `CONTEXT_MAP.md` at
[`docs/architecture/`](../architecture/README.md).

## Contents

| File | What it holds | Status |
|---|---|---|
| [`CONTEXT_MAP.md`](./CONTEXT_MAP.md) | DDD context map across all 17 contexts, with relationship patterns (ACL / Conformist / Published Language / OHS / Customer-Supplier / Separate Ways) | ✅ populated |
| [`UBIQUITOUS_LANGUAGE.md`](./UBIQUITOUS_LANGUAGE.md) | Terms shared across more than one context — shared (one meaning) vs overloaded (per-context); per-context terms stay in each `DOMAIN.md` | ✅ populated |
| [`EVENT_CATALOG.md`](./EVENT_CATALOG.md) | Semantic meaning of every domain event (authored from the Domain Cards; topic/producer/consumer columns to be reconciled with the topology guard) | ✅ populated |

## Service domain docs (per-crate `DOMAIN.md`)

The 17 services. Each link will resolve once that service's `docs/DOMAIN.md` lands (see the
rollout in the strategy). Tier drives depth: TIER-0/1 get the full DEEP sections; TIER-2 keeps
CORE plus collapsed one-line DEEP sections.

| Service | Bounded context | `DOMAIN.md` | Status |
|---|---|---|---|
| `account` | Account / Identity SoR | [`crates/services/account/docs/DOMAIN.md`](../../crates/services/account/docs/DOMAIN.md) | ✅ |
| `audit` | Tamper-evident compliance evidence | [`crates/services/audit/docs/DOMAIN.md`](../../crates/services/audit/docs/DOMAIN.md) | ✅ |
| `auth` | Authentication / session / IdP broker | [`crates/services/auth/docs/DOMAIN.md`](../../crates/services/auth/docs/DOMAIN.md) | ✅ |
| `chat` | Conversations & messaging | [`crates/services/chat/docs/DOMAIN.md`](../../crates/services/chat/docs/DOMAIN.md) | ✅ |
| `comment` | Comment threads | [`crates/services/comment/docs/DOMAIN.md`](../../crates/services/comment/docs/DOMAIN.md) | ✅ |
| `counter` | Counter / analytics SoReference | [`crates/services/counter/docs/DOMAIN.md`](../../crates/services/counter/docs/DOMAIN.md) | ✅ |
| `engagement` | Likes (points) & interaction counters | [`crates/services/engagement/docs/DOMAIN.md`](../../crates/services/engagement/docs/DOMAIN.md) | ✅ |
| `geo-discovery` | Geo-spatial discovery | [`crates/services/geo-discovery/docs/DOMAIN.md`](../../crates/services/geo-discovery/docs/DOMAIN.md) | ✅ |
| `media` | Media control plane | [`crates/services/media/docs/DOMAIN.md`](../../crates/services/media/docs/DOMAIN.md) | ✅ |
| `moderation` | Trust, safety & integrity | [`crates/services/moderation/docs/DOMAIN.md`](../../crates/services/moderation/docs/DOMAIN.md) | ✅ |
| `notification` | Notification activity feed | [`crates/services/notification/docs/DOMAIN.md`](../../crates/services/notification/docs/DOMAIN.md) | ✅ |
| `post` | Content / posts | [`crates/services/post/docs/DOMAIN.md`](../../crates/services/post/docs/DOMAIN.md) | ✅ |
| `profile` | Public personas | [`crates/services/profile/docs/DOMAIN.md`](../../crates/services/profile/docs/DOMAIN.md) | ✅ |
| `realtime` | Live delivery / connection plane | [`crates/services/realtime/docs/DOMAIN.md`](../../crates/services/realtime/docs/DOMAIN.md) | ✅ |
| `search` | Discovery read-model | [`crates/services/search/docs/DOMAIN.md`](../../crates/services/search/docs/DOMAIN.md) | ✅ |
| `social-graph` | Follower / following relations | [`crates/services/social-graph/docs/DOMAIN.md`](../../crates/services/social-graph/docs/DOMAIN.md) | ✅ |
| `timeline` | Timeline fan-out | [`crates/services/timeline/docs/DOMAIN.md`](../../crates/services/timeline/docs/DOMAIN.md) | ✅ |

## Shared infrastructure library contracts (`foundation/` + `platform/`)

The 14 cross-cutting libraries every service composes onto. These are **not** bounded contexts — they own
no business data and emit no domain events; they own a **technical capability** (a mechanism, a contract, a
boot sequence). Their `DOMAIN.md` therefore follows the **library variant**
([`docs/templates/DOMAIN.lib.template.md`](../templates/DOMAIN.lib.template.md)): same 10-section skeleton
and CORE/DEEP tiering, but the "data ownership" section becomes a *dependency-direction / purity* boundary
and the "domain events" section collapses to emitted signals (`tracing`/metrics/none).

**`foundation/`** — pure leaves and near-root contracts (no IO unless stated):

| Crate | Shared capability | `DOMAIN.md` | Status |
|---|---|---|---|
| `error` | Distributed-error contract (trait · severity · wire shape) | [`crates/foundation/error/docs/DOMAIN.md`](../../crates/foundation/error/docs/DOMAIN.md) | ✅ |
| `health` | Liveness/readiness probe contract (graph-leaf) | [`crates/foundation/health/docs/DOMAIN.md`](../../crates/foundation/health/docs/DOMAIN.md) | ✅ |
| `infra-config` | Externalized config & fail-closed hot-reload | [`crates/foundation/infra-config/docs/DOMAIN.md`](../../crates/foundation/infra-config/docs/DOMAIN.md) | ✅ |
| `resilience` | Egress fault tolerance (circuit breaker · retry · timeout) | [`crates/foundation/resilience/docs/DOMAIN.md`](../../crates/foundation/resilience/docs/DOMAIN.md) | ✅ |
| `traffic` | Ingress rate limiting (pure GCRA mechanism) | [`crates/foundation/traffic/docs/DOMAIN.md`](../../crates/foundation/traffic/docs/DOMAIN.md) | ✅ |
| `validate-core` | Zero-dependency validation abstraction (Separated Interface) | [`crates/foundation/validate-core/docs/DOMAIN.md`](../../crates/foundation/validate-core/docs/DOMAIN.md) | ✅ |

**`platform/`** — the application-dispatch, transport, security, and runtime layers:

| Crate | Shared capability | `DOMAIN.md` | Status |
|---|---|---|---|
| `auth-context` | Inbound JWT verification + task-local identity | [`crates/platform/auth-context/docs/DOMAIN.md`](../../crates/platform/auth-context/docs/DOMAIN.md) | ✅ |
| `cqrs` | In-process Command/Query bus + middleware pipeline | [`crates/platform/cqrs/docs/DOMAIN.md`](../../crates/platform/cqrs/docs/DOMAIN.md) | ✅ |
| `service-runtime` | Unified fleet bootstrap (the `Service` trait + `serve`) | [`crates/platform/service-runtime/docs/DOMAIN.md`](../../crates/platform/service-runtime/docs/DOMAIN.md) | ✅ |
| `telemetry` | One-call observability bootstrap (logs · traces · metrics) | [`crates/platform/telemetry/docs/DOMAIN.md`](../../crates/platform/telemetry/docs/DOMAIN.md) | ✅ |
| `test-support` | Integration-test scaffolding (dev-only) | [`crates/platform/test-support/docs/DOMAIN.md`](../../crates/platform/test-support/docs/DOMAIN.md) | ✅ |
| `traffic-redis` | Redis-lease distributed backend for `traffic` | [`crates/platform/traffic-redis/docs/DOMAIN.md`](../../crates/platform/traffic-redis/docs/DOMAIN.md) | ✅ |
| `transport` | gRPC + Kafka with trace propagation + `run_consumer` | [`crates/platform/transport/docs/DOMAIN.md`](../../crates/platform/transport/docs/DOMAIN.md) | ✅ |
| `validation` | CQRS input-validation middleware + `VAL-xxxx` codes | [`crates/platform/validation/docs/DOMAIN.md`](../../crates/platform/validation/docs/DOMAIN.md) | ✅ |

## Authoring

- Service template: [`docs/templates/DOMAIN.template.md`](../templates/DOMAIN.template.md) — copy to
  `crates/services/<svc>/docs/DOMAIN.md` and fill it.
- Shared-library template: [`docs/templates/DOMAIN.lib.template.md`](../templates/DOMAIN.lib.template.md) —
  for `crates/foundation/*` and `crates/platform/*`; same skeleton, library-oriented sections.
- Decisions: capture rationale as immutable ADRs under [`docs/adr/`](../adr/README.md) and link
  them from `DOMAIN.md §9` — never inline the *why*.
- i18n: English is canonical; a `DOMAIN.fr.md` mirror follows the
  [translation standard](../i18n/TRANSLATION.md). The drift gate (`tools/i18n/i18n-drift.sh`)
  covers **any** `<name>.<lang>.md`, so `DOMAIN.fr.md`, `CONTEXT_MAP.fr.md`, etc. are checked the
  same way as READMEs.

> 🇫🇷 French mirror: [`README.fr.md`](./README.fr.md).

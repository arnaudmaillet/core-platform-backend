# `search` — Find profiles, posts, and hashtags across the network in tens of milliseconds, without ever holding the truth

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — high-visibility discovery surface, but **derived and fail-open**: not in any synchronous write path; an outage degrades discovery, it never blocks a write/publish/login |
> | **Deployable** | `crates/apps/search-server` (library crate: `crates/services/search`) |
> | **Datastores** | OpenSearch cluster — indices `profiles`, `posts`, `hashtags` (each behind read/write aliases). **No** Postgres / Scylla / Redis of its own |
> | **Async** | publishes **nothing** · consumes `post.v1.events`, `profile.v1.events`, `moderation.v1.events` (+ a derived hashtag stream) (Kafka) |
> | **Upstream callers** | gateway / BFF (the `Search` / `Suggest` query path) |
> | **Downstream deps** | OpenSearch, Kafka. Source-of-record stores belong to `post` / `profile` — search calls **no** service on the query path |
> | **SLO** | `<TODO>` avail · `Search` p99 `< <TODO ~150> ms` · ingestion lag `< <TODO ~5> s` |

---

## 🎯 Overview & Service Role

`search` is the platform's **discovery read-model**: a derived, typo-tolerant inverted index over profiles, posts, and hashtags. It owns the *matching, typo-tolerance, and ranking*; it owns **nothing authoritative**. Every byte in its index is a disposable copy that must be reconstructable from the source services at any moment — it is a System-of-*Reference*, never a System-of-Record.

The hard problem it solves is **serving full-text discovery at the network's read-volume without coupling discovery to the write path or to any source service**. A naive design queries the content services on every search and indexes synchronously on every write; that couples search latency to N services and taxes every publish. The resolving pattern is the cleanest **CQRS split** in the fleet: the command side is **100% asynchronous Kafka consumers** (there is no write RPC), and the query side is a **stateless gRPC read** that touches only the engine.

**Core objectives:** (1) indexing is **async and off the write path** — publishing a post never waits on search; (2) the query path is **self-contained** — no inter-service call, results are references the caller hydrates; (3) the index is **fully reconstructable** from source-of-record + events (reindex is a first-class operation); (4) posture is **fail-open** — a search outage degrades to empty/partial results, never an upstream block.

| Concern | Path | Latency contract | Notes |
|---|---|---|---|
| **Ingestion** | async Kafka consumers (`run_consumer`) | none (off the write path) | event → searchable in seconds; lag is an SLO, not a consistency requirement |
| **Query** | synchronous gRPC, engine-only | bounded p99 | returns ranked references + minimal display projection; no fan-out |
| **Reindex** | offline / blue-green via aliases | n/a | rebuild from source-of-record; atomic alias swap, zero downtime |

---

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS where it fits, **OpenSearch** as the one canonical engine, Kafka for ingestion. There is no second datastore — search holds no truth to persist.

```
 post-service       ── post.v1.events ──┐
 profile-service    ── profile.v1.events ──┤
                                           ├─► [run_consumer · one loop per topic] ─► Projector ─► SearchIndex ─► OpenSearch
 moderation-service ── moderation.v1.events ──┤      (manual commit, retry, DLQ)     (pure xform)   (port)        (aliases:
 (derived hashtags) ── from post events ──┘                                                                       posts-write/read)
                                                                                                                      ▲
   gateway / BFF ──► search.v1.SearchService/Search ──────────── stateless query (engine-only, no fan-out) ─────────┘
                          returns (entity_type, id, score, snippet) + minimal display projection
```

**Out-of-order correctness is pushed into the engine.** Every index document carries a monotonic `doc_version` derived from the source entity's own version/`updated_at`; writes use OpenSearch **`version_type=external`**, so a stale, replayed, or reordered event can never clobber a newer document — no locks, no read-modify-write in the consumer. This is the search analogue of `moderation`'s monotonic per-subject enforcement version.

> **Invariants** (and where enforced):
> - **Search holds no source of truth.** Results are references `(entity_type, id, score, snippet)` + a minimal indexed display projection; the caller hydrates live/authoritative fields. Litmus: every indexed field must be rebuildable by replaying events or scanning the SoR — domain + projector.
> - **External versioning is the idempotency mechanism.** Out-of-order / redelivered events are resolved by the engine's version guard, not by consumer-side state — infrastructure boundary.
> - **Moderation visibility is a first-class input.** A `moderation` `EnforcementApplied` (RemoveContent/VisibilityLimit) flips `searchable=false` (document retained, because moderation is reversible); `EnforcementReversed` flips it back — application layer.
> - **Personal block/mute is NOT indexed.** A shared inverted index cannot bake per-viewer exclusions; block/mute is a per-query filter applied at the edge — boundary contract.
> - **GDPR erasure is a deep purge.** Actor purge runs `delete_by_query` on `author_id` across every index with no retained tombstone (the index is a copy of indexable PII) — infrastructure boundary.

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-5xx / non-`UNAVAILABLE`) | `<TODO 99.9%>` | 30d rolling | `<metric>` |
| Query latency p99 (`Search`) | `< <TODO 150> ms` | 1h | `<metric>` |
| Ingestion lag (event → searchable) | `< <TODO 5> s` | live | `<consumer-group> lag` |
| Reindex throughput | `<TODO docs/s>` | per job | reindex job metric |

**Error budget:** `<TODO>`. **On burn:** `<freeze rollout | page>`. Note: because search is fail-open, the *availability* objective covers query-path degradation, not data correctness — correctness is covered by ingestion lag + the golden-query relevance suite.

---

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

**Downstream — what `search` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| OpenSearch | the index (match/rank/store) | queries fail, ingestion stalls | **Soft** — query returns empty/partial (fail-open); ingestion resumes from last committed offset |
| Kafka | ingestion stream | indexing stops advancing | **Soft** — index goes stale, lag grows; no data lost (manual commit) |

**Upstream — who depends on `search` (your blast radius if YOU fail):**

| Caller | Uses | User-visible impact if `search` is down |
|---|---|---|
| gateway / BFF | `Search` / `Suggest` | discovery/search box degrades to empty or stale results; **no** write, publish, or login is affected |

> **Critical path?** **No** — derived, async, fail-open. Search is never in the synchronous path of a write, publish, or auth flow.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### gRPC — `search.v1.SearchService` *(Phase 1)*

The synchronous surface is deliberately **read-only**: `Search` (federated, filterable by entity type, cursor-paginated), `Suggest`/autocomplete (prefix), and `MultiSearch` (fan-out + merge). **There is no write/index RPC** — ingestion is Kafka-only — and search **publishes no events** (it is a terminal read-model).

> **Wire contract:** results are references — `(entity_type, id, score, highlight/snippet)` plus the minimal indexed display fields (handle, display name, thumbnail key, `author_id`, `created_at`). Callers MUST hydrate volatile/authoritative fields (live counts, signed media URLs, follow-state, current bio) from `post`/`profile`. Search returns no authoritative entity.

### Rust ports (hexagonal contract) *(Phase 3)*

```rust
#[async_trait] pub trait SearchIndex { /* upsert(doc, version) · delete(id) · set_visibility(id, bool) · delete_by_query(author_id) · query(SearchQuery) */ }
#[async_trait] pub trait IndexAdmin { /* create-index · alias · reindex — the Phase-7 ops surface */ }
```

### Error contract

Every fault implements `error::AppError` with a stable `SCH-XXXX` code, mapped to gRPC `Status` / HTTP by the shared `error` crate:

| Range | Class |
|---|---|
| `SCH-1xxx` | query / parse |
| `SCH-2xxx` | index / upsert (incl. `SCH-2002` stale-version skip) |
| `SCH-3xxx` | projection / transform |
| `SCH-4xxx` | engine availability (fail-open core; retryable) |
| `SCH-5xxx` | reindex / alias / migration |
| `SCH-8xxx` | inbound event decode / source mapping |
| `SCH-9xxx` | cross-cutting (domain/parse, event consumption) |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

> Kafka topics are an API. A schema change in a consumed topic breaks indexing exactly like a proto change.

**Publishes:** none. Search is a terminal read-model.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `post.v1.events` | `search-post-indexer` | index/update/delete posts (content hydrated via `GetPost`) | DLQ `post.v1.events.dlq` |
| `profile.v1.events` | `search-profile-indexer` | index/update/delete profiles (content hydrated via `GetProfileById`); owner masking → **owner** visibility flag | DLQ `profile.v1.events.dlq` |
| `moderation.v1.events` | `search-moderation-indexer` | flip the **moderation** visibility flag on hide; restore on reversal | DLQ `moderation.v1.events.dlq` |
| `<hashtag stream>` | `search-post-indexer` | maintain the hashtag index (derived from post events) | DLQ `<...>.dlq` |

> **Dual visibility authorities:** a document is searchable only when **both** flags permit it — `searchable = moderation_searchable AND owner_searchable`. The two are independent fields, each with its own version guard, written by different streams (`moderation.v1.events` vs a profile owner-masking event). Neither authority can override the other: a profile owner restoring their own visibility cannot lift a platform moderation hide, and vice-versa.

> **Runtime contract (mandatory):** all consumers run under `run_consumer` — manual commit after a terminal outcome, bounded retry with backoff + jitter, DLQ on exhaustion/poison, rebuild-from-last-committed-offset on broker error. **Idempotency:** the engine's external-version guard (`version_type=external`); deletes are naturally idempotent; a stale-version write (`SCH-2002`) and an unknown event type are folded into `Ok` so the offset still commits. One `run_consumer` loop per source topic (logic branches on topic).

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| OpenSearch unavailable (query) | `Search` errors / latency | **fail-open** — return empty/partial, never 5xx the page | check cluster health; circuit-breaker keeps the query path responsive |
| OpenSearch unavailable (ingest) | ingestion lag rises | consumer retries within budget, then DLQ; offset not committed → no loss | restore cluster; consumer resumes from last committed offset |
| Out-of-order / replayed event | — | external versioning rejects the stale write (`SCH-2002`, folded to `Ok`) | none (by design) |
| Mapping/analyzer change needed | — | blue-green reindex into a new physical index + atomic alias swap | run the reindex job (Phase 7) |
| Index/alias missing | `SCH-4003 IndexNotFound` | hard fault (deployment/migration gap), not retried | apply index-mapping migration before rollout |

**Backpressure & limits:** query page-size caps + cursor pagination; a query-path circuit breaker / hard timeout so a slow engine sheds load rather than queueing; ingestion is naturally rate-limited by consumer throughput.

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
search = { path = "crates/services/search" }
```

Library-only. Will implement [`service_runtime::Service`](../../platform/service-runtime/README.md) as `search::service::SearchService` (Phase 5) — `build` wires the OpenSearch adapter **and spawns the ingestion consumers** (one supervised `run_consumer` loop per source topic), `register` adds the gRPC services, `health_probes` pings the engine. Telemetry, config + hot-reload, ingress rate-limiting, health, and graceful shutdown are owned by the runtime.

### Bootstrap (`crates/apps/search-server`)

```rust
use search::service::SearchService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("SEARCH_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50062".to_owned()).parse()?;
    service_runtime::serve::<SearchService>(addr).await
}
```

> **Build status:** complete through Phase 7 (8 phases: scaffold → proto → domain → application+ports → OpenSearch adapter+decode → server+consumers → live IT → hardening). The live integration suite is gated behind `integration-search`. Post, **profile**, and moderation ingestion are all wired (post + profile content is hydrated via `GetPost` / `GetProfileById`).
>
> **Viewer-aware results.** For any caller but the mesh, a page of hits is filtered with social-graph's `CheckAccess` on its authors (one bulk call, split to the RPC's caps): a **post** hit stays only if its author is `VISIBLE` to the reader (not a private author they don't follow, no block either way, not hidden); a **profile** hit stays unless `HIDDEN` (a private profile stays findable by its header); hashtags always stay. Suggestions follow the profile rule; post suggestions are dropped for clients (they name no author). If the check fails, search stays up (fail-open) but shows nothing unchecked: profile and post hits are dropped, hashtags kept, `degraded = true`. A page can come back short; the cursor is unchanged. `exclude_author_ids` remains an extra, caller-supplied exclusion (personal exclusions are never indexed).

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `search`-specific variables *(filled per phase)*

| Variable | Required | Default | Description |
|---|---|---|---|
| `SEARCH_GRPC_ADDR` | No | `0.0.0.0:50062` | gRPC listen address |
| `SEARCH_OPENSEARCH_URL` | No | `http://localhost:9200` | OpenSearch endpoint |
| `SEARCH_INDEX_PREFIX` | No | `search` | index/alias namespace (`<prefix>-profiles`, `-posts`, `-hashtags`) |
| `SEARCH_OPENSEARCH_USER` / `SEARCH_OPENSEARCH_PASSWORD` | No | — | optional basic auth (set both) |
| `SEARCH_QUERY_TIMEOUT_MS` | No | `800` | hard per-request engine timeout; on elapse the query fails **open** (degraded) |
| `SEARCH_POST_GRPC_ENDPOINT` | No | `http://localhost:50056` | `post` endpoint for the ingestion content hydrator |
| `SEARCH_HYDRATE_RPC_TIMEOUT_MS` | No | `5000` | per-request deadline on hydration RPCs — a hung call would otherwise stall the consumer's partition |
| `SEARCH_HYDRATE_CONNECT_TIMEOUT_MS` | No | `2000` | connect deadline when dialing the `post` / `profile` channels |
| `SEARCH_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Yes** (prod) | `http://localhost:50053` | `social-graph` endpoint for the query-path audience filter (`CheckAccess`); unreachable → client results degrade to hashtags only |
| `SEARCH_AUDIENCE_RPC_TIMEOUT_MS` / `SEARCH_AUDIENCE_CONNECT_TIMEOUT_MS` | No | `500` / `500` | deadlines on that call (it runs inside the user's request) |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BROKERS` | **Yes** | — | ingestion stream brokers |

> Engine semantics are canonical on **OpenSearch** (single-node in dev/CI for parity, cluster in prod). A Meilisearch adapter may exist for local velocity but is **not** a relevance-correctness target.

### Compile-time features
- `integration-search` — gates the live, container-backed integration suite (single-node OpenSearch) and the golden-query relevance suite.
- `build.rs` compiles `contracts/proto/search/v1/*.proto` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

- **Index mappings/analyzers are migrations.** They are versioned artifacts (`MAPPING_VERSION`) created at boot by `IndexAdmin::ensure_indices` and applied **before** the new binary serves — the search analogue of SQL/CQL migrations.
- **Reindex is first-class** (`application::reindex::Reindexer`). A mapping/analyzer change is a **blue-green cutover via aliases**: create a fresh physical index, repoint the **write** alias (live writes + backfill land on the new index), backfill from the source-of-record, then repoint the **read** alias last. Zero downtime — and external versioning means a backfilled doc can never clobber a newer live write.
- **Rebuild from truth.** Because Kafka retention is finite, the index is rebuilt by a **dual-source backfill** (`BackfillSource`) that scans the `post`/`profile` SoR, not by "replay from earliest". *(The concrete gRPC-backed `BackfillSource` is a deferred follow-up — it needs the live services and, for profiles, an upstream scan/event capability.)*
- **Rollback:** safe — the binary is stateless; index aliases let you repoint to the previous physical index instantly.
- **Stateful gotchas:** analyzer/tokenizer config is effectively a schema; changing it requires a reindex, never an in-place edit.

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

- **Runtime:** multi-threaded Tokio (ingestion consumers + query handlers). Global tracing/OTel subscriber installed before `serve`; W3C trace-context propagated across the Kafka boundary.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| Ingestion lag (per consumer group) | staleness of the index | sustained `> SLO` ⇒ page |
| `Search` p99 latency | discovery responsiveness | `> SLO` ⇒ investigate engine |
| DLQ produce rate (`*.dlq`) | poison / retry-exhausted ingestion | any sustained rate ⇒ page |
| Golden-query relevance pass-rate | ranking regressions | any failure ⇒ block release |

---

## 🛠️ Local Development &nbsp;·&nbsp; CORE

```bash
cargo build -p search && cargo clippy -p search --all-targets
cargo test  -p search                                  # fast, infra-free unit run
docker compose up -d opensearch kafka                  # repo-root compose (Phase 6)
cargo test  -p search --features integration-search    # live suite (single-node OpenSearch)
```

---

## 🚨 Troubleshooting & Runbook &nbsp;·&nbsp; CORE

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. Search returns empty/partial results.**
Root cause: OpenSearch unavailable or degraded — search fails **open** by design rather than erroring the page. Mitigation: check cluster health; the query path stays responsive via circuit-breaker; results recover when the engine does.

**2. New/edited content isn't searchable.**
Root cause: ingestion lag or a stuck consumer. Mitigation: check consumer-group lag and the `*.dlq` topics; a broker/engine error holds the offset (no loss), so the consumer catches up once the dependency recovers.

**3. Moderated/banned content still appears in search.**
Root cause: the `moderation.v1.events` consumer is lagging or the `searchable` flip was dead-lettered. Mitigation: check the moderation-event consumer lag + DLQ; reprocess the enforcement event.

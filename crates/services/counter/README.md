# `counter` — Count every view, like, share, and follow at firehose scale, serve the totals in sub-milliseconds, and own none of the truth

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — high-visibility engagement surface, but **derived and fail-open**: not in any synchronous write path; an outage degrades counts to stale-but-served, it never blocks a like/follow/publish |
> | **Deployable** | **two** binaries — `crates/apps/counter-server` (read path) **and** `crates/apps/counter-worker` (stream aggregator). Library crate: `crates/services/counter` |
> | **Datastores** | **Redis** (hot live counters · HLL · CMS) · **Postgres** (warm materialized totals + reconciliation ledger) · **ScyllaDB TWCS** (cold historical time-series). Owns no entity |
> | **Async** | publishes `counter.v1.popularity` (coarse ranking signal) · consumes `view.v1.events`, `impression.v1.events`, `click.v1.events`, `engagement.reactions`, social-graph follow events (Kafka) |
> | **Upstream callers** | gateway / BFF, `timeline`, `search` (count hydration + ranking) |
> | **Downstream deps** | Redis, Postgres, Scylla, Kafka. Source-of-record stays in `post` / `profile` / `media` / `engagement` / `social-graph` — counter calls **no** service on the read path |
> | **SLO** | `<TODO>` avail · `BatchGetCounters` p99 `< <TODO ~5> ms` · ingestion lag `< <TODO ~10> s` |

---

## 🎯 Overview & Service Role

`counter` is the platform's **real-time counter aggregator and analytics System-of-Reference**: it absorbs the engagement/view firehose, serves coarse magnitudes back to the fleet at sub-millisecond latency, and owns **no** entity. Every count is a derived materialization of truth that lives in the owning services — reconstructable, at any moment, by replaying their event streams. It is a System-of-*Reference*, never a System-of-Record.

The hard problem it solves is **absorbing millions of concurrent engagements per second without melting a transactional row**. A naive design does `UPDATE … SET count = count + 1` per event — turning a viral post into a single hot ScyllaDB partition or a single locked Postgres row, where throughput collapses to the reciprocal of one lock's latency. The resolving pattern is a **write-amplification funnel**: sharded Kafka ingestion → in-process windowed pre-aggregation that collapses N events into one delta → pipelined Redis hot counters → batched, idempotent write-behind. **No per-event write ever touches a durable store.**

**Core objectives:** (1) ingestion is **async and off the write path** — liking a post never waits on `counter`; (2) the read path is **self-contained and sub-millisecond** — a thin Redis multi-get, no inter-service call, no analytics-table scan inline; (3) counts are **fully reconstructable** from source-of-record + events (reconciliation is first-class); (4) posture is **fail-open** — a counter outage degrades to stale-but-served, never an upstream block.

| Concern | Path | Latency contract | Notes |
|---|---|---|---|
| **Ingestion** | async Kafka consumers (`run_consumer`) in `counter-worker` | none (off the write path) | firehose → windowed delta → Redis in seconds; lag is an SLO, not a consistency requirement |
| **Read** | synchronous gRPC, Redis-only (cache-aside to Postgres on miss) | sub-ms p99 | returns magnitudes for entity references; no fan-out |
| **Ranking signal** | async `counter.v1.popularity` (coarse, slow-loop) | none | `search` / `timeline` consume it; never a synchronous call |

---

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS where it fits, a **three-tier store** (Redis hot / Postgres warm / Scylla cold), Kafka for ingestion. The defining structural choice is **two deployables**: the read server and the stream worker share a domain crate but no process, deployment, or failure domain.

```
 edge/BFF telemetry  ── view.v1.events ──┐
                     ── impression.v1.events ──┤
                     ── click.v1.events ──┤      ┌─────────────── counter-worker ───────────────┐
 engagement-service  ── engagement.reactions ──┤  │ [run_consumer · per topic]                    │
 social-graph        ── follow events ──┘      ├─►│  → windowed pre-aggregation (N events → 1 Δ)   │
                                               │  │  → Redis (HINCRBY / PFADD / CMS, shard re-agg) │
 (sharded keys spread hot entities)            │  │  → idempotent write-behind (window-keyed)      │
                                               │  │  → reconciliation loop → publish popularity    │
                                               │  └────────┬───────────────┬──────────────┬────────┘
                                               │           ▼               ▼              ▼
                                               │      Redis (hot)    Postgres (warm   Scylla TWCS
                                               │                     totals+ledger)   (cold series)
                                               │           ▲                                 
   gateway/BFF/timeline/search ──► counter.v1.CounterService/BatchGetCounters ── Redis-only read ─┘
                                       returns magnitudes for (entity_type, id) · cache-aside on miss
   search / timeline ◄── counter.v1.popularity (coarse, slow-loop ranking signal)
```

**Write-amplification is pushed out before durability.** A viral post's millions of `+1`s are spread across N partitions by a **sharded key** (`entity_id:{0..N}`), folded by each worker into one in-memory delta per **tumbling window**, pipelined into Redis, and only *then* flushed — batched and idempotent on `(entity, metric, window_id)` — into Postgres/Scylla. A worker crash and Kafka redelivery re-applies the *same* window without double-counting.

> **Invariants** (and where enforced):
> - **Counter holds no source of truth.** Reads return magnitudes for an entity *reference*; the caller hydrates the entity from its SoR. Litmus: every count must be rebuildable by replaying events or scanning the SoR — domain + reconciliation.
> - **It answers "how many?", never "who?"/"which?".** Per-actor edge state (who liked, who follows) belongs to `engagement` / `social-graph`. The moment a question needs an identity or a set, it delegates — boundary contract.
> - **No per-event durable write.** Every write to Postgres/Scylla is a window aggregate; the durable flush is idempotent on `(entity, metric, window_id)` — infrastructure boundary.
> - **Exact vs probabilistic by metric class.** Likes/shares/followers/comments are *exact-but-reconcilable* (a window onto a set another SoR owns); views/impressions/unique-viewers/reach are *probabilistic by design* — total via sharded counters (double-count tolerated), uniques via **HyperLogLog**, trending via **Count-Min Sketch** — domain.
> - **Read and ranking are separate delivery mechanisms.** The sub-ms pull (`BatchGetCounters`) and the coarse push (`counter.v1.popularity`) never share a path, so ranking fan-out never taxes the read tier — application.

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-5xx / non-`UNAVAILABLE`) | `<TODO 99.9%>` | 30d rolling | `<metric>` |
| Read latency p99 (`BatchGetCounters`) | `< <TODO 5> ms` | 1h | `<metric>` |
| Ingestion lag (event → counted) | `< <TODO 10> s` | live | `<consumer-group> lag` |
| Counter drift (approximate vs reconciled) | `< <TODO 0.5%>` | per reconciliation cycle | reconciliation metric |

**Error budget:** `<TODO>`. **On burn:** `<freeze rollout | page>`. Note: because `counter` is fail-open, the *availability* objective covers read-path degradation (stale-but-served), not exactness — exactness is covered by ingestion lag + the reconciliation drift SLI.

---

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

**Downstream — what `counter` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | hot counters / HLL / CMS (read + write path) | reads degrade, hot writes stall | **Soft** — reads fall back to last-flushed Postgres total (stale-but-served); the worker buffers/retries within budget |
| Postgres | warm materialized totals + reconciliation ledger | cache-aside misses + flush stall | **Soft** — hot reads still serve from Redis; durable flush retries (no loss, offsets uncommitted) |
| Scylla (TWCS) | cold historical time-series | `GetTimeSeries` degrades | **Soft** — live counters unaffected; only historical analytics is impacted |
| Kafka | ingestion + popularity publish | counting stops advancing | **Soft** — counts go stale, lag grows; no data lost (manual commit) |

**Upstream — who depends on `counter` (your blast radius if YOU fail):**

| Caller | Uses | User-visible impact if `counter` is down |
|---|---|---|
| gateway / BFF | `BatchGetCounters` | engagement counts render stale or absent; **no** write, like, follow, or publish is affected |
| `timeline` / `search` | `counter.v1.popularity` | ranking falls back to its last coarse snapshot; discovery still works |

> **Critical path?** **No** — derived, async, fail-open. `counter` is never in the synchronous path of a write, like, follow, publish, or auth flow.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### gRPC — `counter.v1.CounterService` *(Phase 1)*

The synchronous surface is deliberately **read-only**: `BatchGetCounters` (magnitudes for a batch of entity references + metric mask — the feed-hydration hot path), `GetTrending` (top-K for a scope, served from CMS + a bounded heap), and `GetTimeSeries` (historical buckets — the one RPC allowed to touch the cold Scylla tier, explicitly *not* sub-ms and off the feed path). **There is no write/increment RPC** — ingestion is Kafka-only.

**Hidden like counts (#809).** When a post's author hides like counts, its `LIKE` value is dropped from
`BatchGetCounters` for anyone but the author (one of the caller's profiles, from the token) — guests
included — and a `LIKE` `GetTrending` drops the post (the ranks close up, so its place does not tell
either). Other metrics are untouched; the mesh reads everything. Whose post it is and the author's setting
come from post (`BatchGetLikeVisibility`, cached 60 s per instance); when post cannot answer, every
post's `LIKE` is withheld and the readout is marked `degraded` — never an error. Without
`COUNTER_POST_GRPC_ENDPOINT` nothing is withheld (a warning at boot).

> **Wire contract:** results are magnitudes attached to a reference — `(entity_type, id, metric, value)` plus approximate-vs-exact provenance. Callers MUST hydrate the entity itself (post body, profile, media URL) from its SoR. `counter` returns no authoritative entity and no per-actor membership.

### Rust ports (hexagonal contract) *(Phase 3)*

```rust
#[async_trait] pub trait CounterStore    { /* incr_window · read_batch · pfadd/pfcount · cms_topk — the hot Redis tier */ }
#[async_trait] pub trait CounterLedger   { /* upsert_window(idempotent) · read_total · reconcile — the warm Postgres tier */ }
#[async_trait] pub trait TimeSeriesStore { /* append_bucket · range — the cold Scylla tier */ }
#[async_trait] pub trait SignalPublisher { /* publish coarse popularity — counter.v1.popularity */ }
```

### Error contract

Every fault implements `error::AppError` with a stable `CTR-XXXX` code, mapped to gRPC `Status` / HTTP by the shared `error` crate:

| Range | Class |
|---|---|
| `CTR-1xxx` | read / query |
| `CTR-2xxx` | aggregation / window |
| `CTR-3xxx` | flush / write-behind (retryable) |
| `CTR-4xxx` | store availability (fail-open core; retryable) |
| `CTR-5xxx` | reconciliation / drift |
| `CTR-8xxx` | inbound event decode / source mapping |
| `CTR-9xxx` | cross-cutting (domain/parse, event consumption) |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

> Kafka topics are an API. A schema change in a consumed topic breaks counting exactly like a proto change.

**Publishes:**

| Topic | Key | Purpose |
|---|---|---|
| `counter.v1.popularity` | entity id | coarse, periodic popularity/trending snapshot for ranking; consumed by `search` (`PopularityScore`) and `timeline`. Never a per-event count |

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `view.v1.events` | `counter-view-aggregator` | aggregate views (total via sharded counter, uniques via HLL) | DLQ `view.v1.events.dlq` |
| `impression.v1.events` | `counter-impression-aggregator` | aggregate impressions / reach | DLQ `impression.v1.events.dlq` |
| `click.v1.events` | `counter-click-aggregator` | aggregate clicks / CTR inputs | DLQ `click.v1.events.dlq` |
| `engagement.reactions` | `counter-reaction-aggregator` | aggregate like/share magnitudes (supersedes engagement's raw counters) | DLQ `engagement.reactions.dlq` |
| `<social-graph follow events>` | `counter-follow-aggregator` | aggregate follower / following counts | DLQ `<...>.dlq` |

> **Runtime contract (mandatory):** all consumers run under `run_consumer` — manual commit after a terminal outcome, bounded retry with backoff + jitter, DLQ on exhaustion/poison, rebuild-from-last-committed-offset on broker error. **Idempotency:** the durable flush is keyed by `(entity, metric, window_id)`, so a redelivered event re-applies the same window without double-counting; an unmapped/unknown event (`CTR-8002`) is folded into `Ok` so the offset still commits; approximate metrics tolerate at-least-once double-counting by design.

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis unavailable (read) | `BatchGetCounters` latency / errors | **fail-open** — fall back to last-flushed Postgres total (stale), never 5xx the feed | check Redis health; reads recover when the hot tier does |
| Redis unavailable (ingest) | ingestion lag rises | worker buffers/retries within budget, then DLQ; offset uncommitted → no loss | restore Redis; consumers resume from last committed offset |
| Postgres flush failing | flush retries, lag on durable total | hot counts unaffected; durable total lags | restore Postgres; idempotent re-flush catches up |
| Hot entity (viral) | one entity dominates a partition | sharded key (`entity_id:{0..N}`) + two-stage re-aggregation spreads load | none (by design); raise shard count if needed |
| Reconciliation drift > tolerance | `CTR-5002 DriftThresholdExceeded` | approximate count corrected against replayed SoR truth | investigate the source stream; the reconciliation loop self-heals exact metrics |
| Kafka unavailable | counts stop advancing | consumers idle; no loss (manual commit) | restore brokers; counters catch up |

**Backpressure & limits:** windowed aggregation is the primary shed (N→1); per-request batch-size caps on `BatchGetCounters`; a read-path hard timeout so a slow Redis sheds load rather than queueing; ingestion is naturally rate-limited by consumer throughput.

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
counter = { path = "crates/services/counter" }
```

Library-only. Will implement [`service_runtime::Service`](../../platform/service-runtime/README.md) **twice** (Phase 5): `counter::service::CounterReadService` (the `counter-server` binary — `build` wires the Redis read adapter, `register` adds the gRPC service, `health_probes` pings Redis) and `counter::service::CounterWorkerService` (the `counter-worker` binary — `build` wires all three store adapters + the Kafka publisher and **spawns the supervised aggregation consumers**, no gRPC ingress). Telemetry, config + hot-reload, health, and graceful shutdown are owned by the runtime.

### Bootstrap (`crates/apps/counter-server`)

```rust
use counter::service::CounterReadService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("COUNTER_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50064".to_owned()).parse()?;
    service_runtime::serve::<CounterReadService>(addr).await
}
```

> **Build status:** complete through Phase 7 (all 8 phases: scaffold → proto → domain → application+ports → adapters → server+worker wiring → live IT → hardening). The live integration suite is gated behind `integration-counter` and exercises the real Redis + Postgres + Scylla tiers. **The reconciliation loop is wired:** the worker runs a supervised sweep that pages reconcilable pairs from the ledger and heals exact-counter drift against `social-graph` (the authoritative SoR) via `GetRelationStatus`. *Follower/following* are reconciled today; *like/share/comment* reconciliation stays deferred — `engagement` exposes weighted reaction scores (not a reaction count), and its share/comment counters are the approximate ones this service supersedes. Remaining follow-ups: a standalone popularity cadence, the concrete shard-fan-out producer, and a graceful-shutdown drain hook.
>
> **Authorization (deployment requirement):** `counter` self-authorizes nothing. The read RPCs are caller-facing aggregate magnitudes; gate access at the gateway / `auth-context` before exposure. Counts carry no per-actor identity, so they leak no membership.

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `counter`-specific variables *(filled per phase)*

| Variable | Required | Default | Description |
|---|---|---|---|
| `COUNTER_GRPC_ADDR` | No | `0.0.0.0:50064` | read-server gRPC listen address |
| `COUNTER_WORKER_GRPC_ADDR` | No | `0.0.0.0:50065` | worker health/reflection listen address (no domain RPC) |
| `COUNTER_AGGREGATION_WINDOW_MS` | No | `5000` | tumbling pre-aggregation window (the N→1 collapse) |
| `COUNTER_FLUSH_INTERVAL_MS` | No | `=window` | how often the worker drains closed windows and flushes |
| `COUNTER_SHARD_COUNT` | No | `16` | hot-entity key shards (`entity_id:{0..N}`) |
| `COUNTER_READ_TIMEOUT_MS` | No | `50` | hard per-request hot-read timeout; on elapse the read fails **open** (stale ledger total) |
| `COUNTER_POPULARITY_INTERVAL_S` | No | `60` | slow-loop cadence for the popularity signal (reserved; currently coupled to flush) |
| `COUNTER_RECONCILE_INTERVAL_S` | No | `3600` | reconciliation sweep cadence (follower/following drift correction) |
| `COUNTER_DRIFT_TOLERANCE` | No | `5` | absolute drift tolerated before reconciliation corrects an exact counter |
| `COUNTER_SOCIAL_GRAPH_GRPC_ENDPOINT` | No | `http://localhost:50053` | `social-graph` endpoint — authoritative follower/following counts for reconciliation |
| `COUNTER_SOCIAL_GRAPH_RPC_TIMEOUT_MS` | No | `5000` | per-request deadline on `social-graph` RPCs — a hung call would otherwise stall the reconcile loop |
| `COUNTER_SOCIAL_GRAPH_CONNECT_TIMEOUT_MS` | No | `2000` | connect deadline when dialing the `social-graph` channel |
| `COUNTER_POST_GRPC_ENDPOINT` | No | unset | `post`'s mesh address (e.g. `http://post:50056`), read server only: hidden like counts are withheld (#809). Unset → withheld from nobody |
| `COUNTER_POST_RPC_TIMEOUT_MS` · `COUNTER_POST_CONNECT_TIMEOUT_MS` | No | `500` · `1000` | deadlines of that call |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `REDIS_URL` | **Yes** | — | hot counter tier |
| `DATABASE_URL` | **Yes** | — | warm ledger (Postgres) |
| `SCYLLA_NODES` | **Yes** | — | cold time-series (Scylla) |
| `KAFKA_BROKERS` | **Yes** | — | ingestion + popularity publish |

### Compile-time features
- `integration-counter` — gates the live, container-backed integration suite (real Redis + Postgres + Scylla — the full hot/warm/cold tiering).
- `build.rs` (Phase 1, in `counter-api`) compiles `contracts/proto/counter/v1/*.proto` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

- **Two deployables, scaled independently.** `counter-server` scales with fleet read QPS; `counter-worker` scales with ingest firehose volume. They are released together (same image/tag) but rolled and autoscaled separately.
- **Schema migrations** (Postgres ledger tables + Scylla TWCS time-series tables) are owned by `crates/apps/migrator`, applied before the new binary serves.
- **Rebuild from truth.** Because Kafka retention is finite, exact counts are repaired by the **reconciliation loop** scanning/replaying the owning SoR (`engagement` reactions, `social-graph` follows), not by "replay from earliest". Approximate counts (views) are accepted as approximate.
- **Rollback:** safe — both binaries are stateless over their stores; the worker resumes from last committed offsets, the server is pure read.
- **Stateful gotchas:** changing `COUNTER_AGGREGATION_WINDOW_MS` or `COUNTER_SHARD_COUNT` mid-flight affects in-flight windows — drain or accept a transient lag blip; durable idempotency keys make it safe, not seamless.

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

- **Runtime:** multi-threaded Tokio. `counter-worker` runs the aggregation consumers + flush scheduler + reconciliation loop; `counter-server` runs the read handlers. Global tracing/OTel subscriber installed before serve; W3C trace-context propagated across the Kafka boundary.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| Ingestion lag (per consumer group) | staleness of live counts | sustained `> SLO` ⇒ page |
| `BatchGetCounters` p99 latency | feed-hydration responsiveness | `> SLO` ⇒ investigate Redis |
| Durable-flush failure / lag | warm-tier divergence, replay risk | sustained ⇒ page |
| Reconciliation drift | approximate-vs-truth divergence | `> SLO` ⇒ investigate source stream |
| DLQ produce rate (`*.dlq`) | poison / retry-exhausted ingestion | any sustained rate ⇒ page |

---

## 🛠️ Local Development &nbsp;·&nbsp; CORE

```bash
cargo build -p counter && cargo clippy -p counter --all-targets
cargo test  -p counter                                    # fast, infra-free unit run
docker compose up -d redis postgres scylla kafka          # repo-root compose (Phase 6)
cargo test  -p counter --features integration-counter     # live suite (hot/warm/cold tiers)
```

---

## 🚨 Troubleshooting & Runbook &nbsp;·&nbsp; CORE

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. Counts render stale or zero.**
Root cause: Redis degraded — reads fail **open** to the last-flushed Postgres total rather than erroring the feed. Mitigation: check Redis health; live counts recover when the hot tier does; the durable total bounds how stale the fallback can be.

**2. New engagement isn't reflected in counts.**
Root cause: ingestion lag or a stuck consumer. Mitigation: check consumer-group lag and the `*.dlq` topics; a broker/store error holds the offset (no loss), so the worker catches up once the dependency recovers.

**3. A count looks wrong / has drifted.**
Root cause: at-least-once double-count on an approximate metric, or a missed exact event. Mitigation: approximate metrics are accepted within tolerance; exact metrics self-heal on the next reconciliation cycle — check the reconciliation drift metric and, if `CTR-5002` fired, the source stream.

**4. One viral entity is hot-spotting a partition.**
Root cause: shard count too low for the entity's rate. Mitigation: raise `COUNTER_SHARD_COUNT`; the two-stage sharded-counter design spreads the entity across more partitions/workers and re-aggregates in Redis.

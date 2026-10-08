# `engagement` — Weighted reaction scoring & high-volume interaction counters

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — real-time interaction backbone; degradable to durable ledger |
> | **Deployable** | `crates/apps/engagement-server` (library crate: `crates/services/engagement`) |
> | **Datastores** | Redis (authoritative hot path) · ScyllaDB keyspace `engagement` (durable ledger) |
> | **Async** | publishes `engagement.reactions` (+ `engagement.score_updated`) · consumes `comment.created` / `comment.deleted` |
> | **Upstream callers** | `<TODO: gateway>` |
> | **Downstream deps** | Redis, ScyllaDB, Kafka |
> | **SLO** | reaction swap p99 **< 5 ms** (zero Scylla on hot path) · snapshot read p99 ~0.3 ms |

---

## 🎯 Overview & Service Role

`engagement` is the real-time interaction backbone. For every published post it owns three data
categories: **weighted reactions** (emoji/icon/gif, one active per `(post_id, profile_id)`),
**high-volume counters** (views/shares, Redis-incremented and write-behind-flushed to Scylla), and
**comment counts** (reactively ingested from `comment.*`).

The hard problem it solves is **millions of concurrent reactions without Paxos**: a naive
read-modify-write or ScyllaDB LWT would collapse under rapid-toggle storms. It resolves this with a
**Redis-primary Lua atomic swap** — Redis's single-threaded execution serializes all swaps for a
`(post, profile)` pair, with zero Scylla reads on the hot path — and a **Kafka write-behind** path that
persists the durable ledger asynchronously.

**Core objectives:** sub-ms reaction swap with no hot-path Scylla; no rapid-toggle races; Kafka
write-behind for multi-region streaming and crash durability. **Redis is authoritative; Scylla counters
are approximate analytics.**

---

## 📐 Architecture & Concepts

```
WRITE PATH (hot, <5ms): gRPC ─► Upsert/RemoveReaction, RecordView/Share
                           ─► RedisScoreStore (Lua EVAL / INCR, one round-trip)
                           ─► KafkaProducer(engagement.reactions, key post_id:profile_id)

WRITE-BEHIND (async): ReactionWriteBehindWorker  (consumes engagement.reactions → Scylla post_reactions, idempotent; each write stamped with its event's time, so the latest event wins whatever order it lands in)
                      CounterFlushWorker (every 5s) (DirtyPostTracker → Redis GETSET 0 → Scylla counters)
                      CommentEventConsumer (consumes comment.created/deleted → Redis INCR/DECR + Scylla counter)

READ PATH: GetPostEngagement ─► RedisScoreStore::get_snapshot (4 parallel GETs, ~0.3ms p99)
```

**Redis key layout:** `engagement:r:{post}:{profile}` (HASH, per-profile reaction = swap source);
`engagement:scores:{post}` (HASH, authoritative weighted scores); `engagement:views/shares/comments:{post}`
(counters). **ScyllaDB:** `engagement.post_reactions` (durable ledger, PK `((post_id), profile_id)`),
`engagement.reactions_by_profile` (its mirror by profile, PK `((profile_id), post_id)`, written and
deleted with it in one LOGGED BATCH), `engagement.post_interaction_counters` (approximate counter table).

**A profile's reactions (#653).** `ListReactionsByProfile(profile_id, limit, page_token)` is **mesh
only** (never on the edge): the GDPR data export reads which posts a profile reacted to, with which
reaction and when, paged by post id (the token is the last post id). It reads the ledger, so it needs
the write-behind path (Kafka + Scylla); without it the RPC answers `ENG-5003` (`UNAVAILABLE`).
Reactions from before `reactions_by_profile` existed are indexed by an opt-in, idempotent backfill at
startup (`ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE=true`: a paged scan of `post_reactions`).

> **Invariants** (and where enforced): one active reaction per `(post_id, profile_id)` — enforced
> atomically by the Lua swap; concurrent swaps for the same pair are serialized by Redis's
> single-threaded Lua context; ledger UPSERT is idempotent (safe re-delivery).

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Reaction swap p99 (hot path) | **< 5 ms** | 1h | `engagement_reaction_upsert_duration_ms` |
| `GetPostEngagement` p99 | ~0.3 ms (target < 5 ms) | 1h | snapshot read histogram |
| Counter flush lag | `< <TODO>` posts | live | `engagement_counter_flush_lag_posts` |
| Write-behind consumer lag | `< <TODO>` | live | `engagement_write_behind_consumer_lag` |
| Durability (reactions) | ledger eventually consistent | — | Kafka at-least-once → idempotent UPSERT |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | authoritative hot path | reaction/view/share commands fail | **Hard** — `503 Unavailable` (backpressure to callers) |
| ScyllaDB | durable ledger + counters | write-behind backs off | **Soft** — Redis stays consistent; ledger catches up |
| Kafka | write-behind + comment ingest | persistence + comment counts lag | **Soft** — hot path unaffected |
| `post` (gRPC `BatchGetLikeVisibility`, #809) | whose post it is and whether its author hides like counts | likes withheld from non-authors | **Fail closed** for likes only (views/shares/comments unaffected); 60 s cache |

**Upstream (blast radius):**

| Caller | Uses | Impact if `engagement` is down |
|---|---|---|
| `<TODO: gateway>` | reaction/view/share + `GetPostEngagement` | no reactions, no engagement counts on posts |
| `geo-discovery` | consumes `engagement.score_updated` | map virality scores go stale |

> **Critical path?** **Yes** for the reaction write/read path (Redis-backed); persistence is async.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `engagement.v1.EngagementService`

```protobuf
service EngagementService {
  rpc UpsertReaction    (UpsertReactionRequest)    returns (CommandResponse);
  rpc RemoveReaction    (RemoveReactionRequest)    returns (CommandResponse);
  rpc RecordView        (RecordViewRequest)        returns (CommandResponse);
  rpc RecordShare       (RecordShareRequest)       returns (CommandResponse);
  rpc GetPostEngagement (GetPostEngagementRequest) returns (PostEngagementView);
  rpc BatchGetLikes     (BatchGetLikesRequest)     returns (BatchGetLikesResponse); // likes (#665)
  rpc ListReactionsByProfile (ListReactionsByProfileRequest) returns (ListReactionsByProfileResponse); // mesh only
}
```

**Hidden like counts (#809).** When a post's author hides like counts (profile interaction settings),
`GetPostEngagement` returns no `reaction_scores` and a zero `total_weighted_score` to anyone but the
author (one of the caller's profiles, from the token) — guests included; views, shares and comments stay.
The mesh reads everything. Whose post it is and the author's setting come from post
(`BatchGetLikeVisibility`, cached 60 s per instance); when post cannot answer, likes are withheld.
Without `ENGAGEMENT_POST_GRPC_ENDPOINT` nothing is withheld (a warning at boot).

**Likes are points (#665).** A like is a point staked in the wallet; engagement turns the wallet's
`StakeCommitted` (`wallet.v1.events`, group `engagement-stakes`) into each post's and comment's like
count. Each event carries the account's **total** on the target, applied with one Lua script (the
account's total and the target's sum under the target's hash tag, `engagement:{post:<id>}:…`): a total
no larger than the one held changes nothing, so redeliveries, the wallet outbox's at-least-once and
out-of-order events are absorbed with no marker. The durable copy is Scylla `likes_by_target` /
`likes_by_account` (migration 0005), written with the stake's time as the write timestamp.
`GetPostEngagement` adds `like_count`, `my_likes` (a member: its account's own) and `likes_hidden`;
`BatchGetLikes` (edge `public_read`, ≤ 100 targets) gives the same for posts and comments. Hidden like
counts (#809) apply to posts: `count` 0 and `hidden`, the reader's own likes still shown. The heart
reactions (`UpsertReaction`) go in the next part.

### Rust ports (hexagonal contract)

```rust
pub trait ScoreStore: Send + Sync + 'static {
    async fn atomic_upsert_reaction(&self, post, profile, kind, weight) -> Result<Option<(ReactionKind, i64)>, EngagementError>;
    async fn atomic_remove_reaction(&self, post, profile) -> Result<Option<(ReactionKind, i64)>, EngagementError>;
    async fn incr_view(&self, post) -> Result<(), EngagementError>;
    async fn incr_share(&self, post) -> Result<(), EngagementError>;
    async fn get_snapshot(&self, post) -> Result<PostEngagementSnapshot, EngagementError>;
}
pub trait ReactionLedger: Send + Sync + 'static { /* upsert/remove/scan_for_recovery/apply_interaction_delta (write-behind only) */ }
```

### Error contract (`ENG-xxxx`)

| Range | Category |
|---|---|
| `ENG-1xxx` | reaction state (not found, wrong author) |
| `ENG-2xxx` | reaction kind / weight validation |
| `ENG-3xxx` | Kafka / event publish |
| `ENG-5xxx` | worker / Lua script |
| `ENG-9xxx` | id parsing / domain violation |

---

## 📨 Events & Async Contract

**Publishes:**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `engagement.reactions` | every reaction/view/share | `post_id:profile_id` | own `ReactionWriteBehindWorker`; `notification` (reactions) |
| `engagement.score_updated` | virality recompute | `post_id` | `geo-discovery` (map score sync) |

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `comment.created` / `comment.deleted` | `engagement-comment-consumer` | INCR/DECR comment counter (Redis + Scylla) | DLQ `{topic}.dlq` |
| `wallet.v1.events` | `engagement-stakes` | `stake_committed` → likes (#665): the account's total on a post or comment, idempotent and order-proof (Redis Lua + Scylla) | DLQ `{topic}.dlq` |

> **Runtime contract (mandatory):** the comment consumer and write-behind worker run under
> `run_consumer` — manual commit after success, bounded retry with backoff + jitter, DLQ on
> exhaustion/poison. The ledger UPSERT is idempotent, so re-delivery is safe.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis unavailable | reaction/view/share fail | **Hard** — `503`; backpressure to callers | check Redis; hot path requires it |
| ScyllaDB unavailable | write-behind backs off | **Soft** — Redis consistent; ledger catches up | check Scylla compaction/disk I/O |
| Worker crash | partitions reassigned | at-least-once replay (`run_consumer`); idempotent UPSERT | none — self-healing |
| Redis restart **without AOF** | scores + reaction state lost | counters lose current window; reactions need cold-start recovery | enable AOF; rebuild from Scylla ledger |
| Rapid reaction toggle | — | Lua serializes; no race | none |

**Backpressure & limits.** The hot path is one Redis round-trip per op. `CounterFlushWorker` (default
5 s) bounds counter write amplification. ScyllaDB counters are approximate by design — never treat them
as authoritative.

---

## 📦 Integration & Usage

```toml
[dependencies]
engagement = { path = "crates/services/engagement" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`engagement::service::EngagementService` — `build` wires the Redis score store, reaction-weights config,
the Kafka publisher, and the write-behind workers; `register` adds the gRPC + reflection services;
`health_probes` checks Redis (the always-on hot path). Built with fred's `i-scripts` feature for Lua.

### Bootstrap (`crates/apps/engagement-server`)

```rust
use std::net::SocketAddr;
use engagement::service::EngagementService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("ENGAGEMENT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50058".to_owned())
        .parse()?;
    service_runtime::serve::<EngagementService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### Reaction weight matrix

| Variable | Default | Description |
|---|---|---|
| `ENGAGEMENT_REACTION_WEIGHT_HEART` | `1` | ❤️ score weight |
| `ENGAGEMENT_REACTION_WEIGHT_FIRE` | `2` | 🔥 score weight |
| `ENGAGEMENT_REACTION_WEIGHT_ROCKET` | `5` | 🚀 score weight |
| `ENGAGEMENT_REACTION_WEIGHT_CLAP` | `1` | 👏 score weight |
| `ENGAGEMENT_REACTION_WEIGHT_SAD` | `1` | 😢 score weight |
| `ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE` | unset | `true`: index every existing reaction by profile at startup (once; idempotent) — #653 |
| `ENGAGEMENT_POST_GRPC_ENDPOINT` | unset | post's mesh address (e.g. `http://post:50056`): hidden like counts are withheld (#809). Unset → withheld from nobody |
| `ENGAGEMENT_POST_RPC_TIMEOUT_MS` · `ENGAGEMENT_POST_CONNECT_TIMEOUT_MS` | `500` · `1000` | deadlines of that call |

### Service + inherited infrastructure

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENGAGEMENT_COUNTER_FLUSH_INTERVAL_SECS` | No | `5` | View/share flush cadence. |
| `REDIS_URL` | **Yes** | — | Redis connection (AOF recommended). |
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB ledger. |
| `KAFKA_BROKERS` | **Yes** | `localhost:9092` | Kafka brokers. |
| `ENGAGEMENT_GRPC_ADDR` | No | `0.0.0.0:50058` | gRPC bind address. |

> Full `SCYLLA_*` / `REDIS_*` / `KAFKA_*` tuning lives in the shared storage/transport crates.

### Compile-time features
- `fred` with `i-scripts` (Lua atomic swap). `build.rs` compiles `proto/engagement/v1/*.proto`.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `0001_create_keyspace.cql` → `0002_create_post_reactions_table.cql` →
  `0003_create_post_interaction_counters_table.cql` → `0004_create_reactions_by_profile_table.cql` →
  `0005_create_likes_tables.cql` against
  `engagement`, applied **before** first start. (0002's table comment held a `;`; the integration
  suites' runner split on it until it became quote-aware like `apps/migrator` — prod never was affected.
  It is a comma now — same schema.)
- **Redis durability:** enable AOF (`appendonly yes`, `appendfsync everysec`) — without it, a restart
  loses the current flush window and requires cold-start recovery from the Scylla ledger.
- **Kafka:** pre-create `engagement.reactions` with ≥ 12 partitions.
- **Rollout/Rollback:** `<TODO>`; the gRPC tier is stateless, but workers are at-least-once consumers —
  safe to roll.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread (required — `tokio::join!` on the read path).

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `engagement_reaction_upsert_duration_ms` | hot-path latency | p99 > 10 ms ⇒ Redis spike |
| `engagement_counter_flush_lag_posts` | flush worker health | > 10 000 ⇒ behind |
| `engagement_write_behind_consumer_lag` | ledger persistence | > 50 000 ⇒ Kafka consumer lag |
| `engagement_redis_errors_total` | hot-path availability | any spike ⇒ Redis connectivity |
| `engagement_scylla_errors_total` | ledger durability | any spike ⇒ Scylla connectivity |

---

## 🛠️ Local Development

```bash
cargo build -p engagement && cargo clippy -p engagement -- -D warnings
cargo test  -p engagement
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/engagement/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. Reaction scores drift after a Redis restart.**
Root cause: Redis was flushed/restarted without AOF; the `engagement:scores:*` and `engagement:r:*`
hashes are lost. Mitigation: enable AOF to prevent recurrence; run cold-start recovery (scan
`engagement.post_reactions`, group by `(post_id, kind)`, sum weights, `HSET` rebuild) before restarting
the gRPC server.

**2. Write-behind consumer lag grows continuously.**
Root cause: Scylla writes slower than the produce rate, or too few consumer members. Mitigation: check
`engagement_write_behind_consumer_lag`; scale `ReactionWriteBehindWorker` instances; verify
`post_reactions` compaction isn't saturating disk I/O.

**3. `ENG-5001 ScriptReturnInvalid` in logs.**
Root cause: the Lua swap returned an unexpected type — usually a Redis version mismatch (null-return
behavior differs 6.x vs 7.x) or a key of the wrong type. Mitigation: verify Redis ≥ 7.0; check
`TYPE engagement:r:{post}:{profile}` is `hash`; delete a corrupt key and let the next upsert recreate
it (the Kafka outbox still replays to Scylla).

# `engagement` — Likes & high-volume interaction counters

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — real-time interaction backbone; degradable to durable ledger |
> | **Deployable** | `crates/apps/engagement-server` (library crate: `crates/services/engagement`) |
> | **Datastores** | Redis (authoritative hot path) · ScyllaDB keyspace `engagement` (durable copies) |
> | **Async** | publishes nothing · consumes `wallet.v1.events` (likes), `account.v1.events` (erasure) and `comment.created` / `comment.deleted` |
> | **Upstream callers** | `<TODO: gateway>` |
> | **Downstream deps** | Redis, ScyllaDB, Kafka, `post` (hidden like counts) |
> | **SLO** | snapshot read p99 ~0.3 ms (zero Scylla on the read path) |

---

## 🎯 Overview & Service Role

`engagement` is the real-time interaction backbone. For every post it owns three data categories:
**likes** (#665: a like is a point staked in the wallet; engagement keeps each account's total per post
and comment, and their sum), **high-volume counters** (views/shares, Redis-incremented and flushed to
Scylla), and **comment counts** (reactively ingested from `comment.*`).

The hard problem it solves is **counting at hot-post scale without Paxos**: likes arrive from the
wallet's outbox at least once and in any order, and views arrive in bursts. Likes are made idempotent
by carrying the account's **total**, applied by one Lua script that only moves forward; counters are
plain Redis `INCR`s, flushed to Scylla by a background worker.

**Core objectives:** sub-ms reads with no Scylla on the hot path; likes exact under redelivery and
reordering; durable copies for recovery and the GDPR export. **Redis is authoritative; Scylla counters
are approximate analytics.**

The weighted reactions (heart, fire, rocket, clap, sad: `UpsertReaction` / `RemoveReaction`,
`engagement.reactions`) were removed with #665: a like is a point, and there is no unlike.

---

## 📐 Architecture & Concepts

```
LIKES (async):  wallet.v1.events StakeCommitted ─► StakeConsumer (group engagement-stakes)
                  ─► RedisLikeStore (one Lua script: the account's total + the target's sum)
                  ─► ScyllaLikeLedger (likes_by_target + likes_by_account, write timestamp = stake time)

COUNTERS (hot, <5ms): gRPC ─► RecordView/Share ─► RedisScoreStore (INCR, one round-trip)
                CounterFlushWorker (every 5s) (DirtyPostTracker → Redis GETSET 0 → Scylla counters)
                CommentEventConsumer (comment.created/deleted → Redis INCR/DECR + Scylla counter)

READ PATH: GetPostEngagement / BatchGetLikes ─► Redis (counters + likes, ~0.3ms p99)
           ListLikesByAccount (mesh, GDPR export) ─► Scylla likes_by_account
```

**Redis key layout:** `engagement:{post:<id>}:likes` / `engagement:{post:<id>}:likers` (and
`{comment:<id>}`: the target's sum and each account's total, under the target's hash tag; the likers
expire 30 days after the target's last like, the sum never);
`engagement:views/shares/comments:{post}` (counters). **ScyllaDB:** `engagement.likes_by_target` (who
liked a target, PK `((target_kind, target_id), account_id)`), `engagement.likes_by_account` (what an
account liked, PK `((account_id), target_kind, target_id)`), `engagement.post_interaction_counters`
(approximate counter table). Migration 0006 drops the reactions' `post_reactions` and
`reactions_by_profile`.

**Expired likers.** A likers hash that holds every liker carries `_complete` (set on the target's first
like, or when a rehydration finished). Once it expired, an account missing from a new one is
**unknown**, not zero: the stake consumer then rehydrates the whole hash from `likes_by_target`
(`HSETNX`, so a newer Redis total wins; anonymous likers included) before applying, so the next stake
adds only the difference; an account deleted meanwhile is forgotten again after each page (its
`erased_accounts` mark is written before the eraser's `HDEL`). A read falls back to the reader's row in `likes_by_target` and starts one
background rehydration (`:rehydrating`, `SET NX` for 60 s).

**What an account liked (#653, #665).** `ListLikesByAccount(account_id, limit, page_token)` is **mesh
only** (never on the edge): the GDPR data export reads each post and comment the account liked, its
points, the profile that liked last and when, paged by target (the token is the last target,
`kind:id`). It reads Scylla, so it needs the Kafka path; without it the RPC answers `ENG-5003`
(`UNAVAILABLE`).

**A deleted account's likes (GDPR Art. 17).** On `account.v1.events` `account_deleted` (group
`engagement-account-erasure`), who liked goes and the counts stay: the points are kept, anonymously.
`LikeEraser` first marks the account erased (`engagement.erased_accounts`, 30-day TTL, migration 0007,
with the deletion's own time), then for each target it liked removes its `:likers` entry and, in one
logged batch, swaps its `likes_by_target` row for an **anonymous** one with the same total and deletes
its `likes_by_account` row — so the counts stay rebuildable from `likes_by_target`. The anonymous id is
a UUIDv5 of the account, the target and the deletion's time: a replay (a redelivery, a batch that timed
out but landed) rewrites the same row, and it never collides with an account id (v7). The Scylla writes
are stamped with the erasure's time, so a stake made before it and landing late cannot write the
account back. The stake consumer drops a marked account's stakes, and re-checks after applying one (the
erasure may have listed the targets before it).

> **Invariants** (and where enforced): a target's like count is the sum of its accounts' totals, and an
> account's total only grows — both enforced atomically by the Lua script (a total no larger than the
> one held changes nothing); the Scylla copy keeps the newest total whatever the order events land in
> (write timestamp = stake time).

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| `GetPostEngagement` p99 | ~0.3 ms (target < 5 ms) | 1h | snapshot read histogram |
| Counter flush lag | `< <TODO>` posts | live | `engagement_counter_flush_lag_posts` |
| Stake consumer lag | `< <TODO>` | live | consumer group `engagement-stakes` lag |
| Durability (likes) | Scylla copy eventually consistent | — | Kafka at-least-once → monotone totals |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | authoritative hot path | view/share commands and reads fail; stakes retried | **Hard** — `503 Unavailable` (backpressure to callers) |
| ScyllaDB | durable copies + counters | stakes retried, flush backs off; `ListLikesByAccount` fails | **Soft** — Redis reads unaffected; copies catch up |
| Kafka | likes + comment ingest | like and comment counts lag | **Soft** — reads unaffected |
| `post` (gRPC `BatchGetLikeVisibility`, #809) | whose post it is and whether its author hides like counts | likes withheld from non-authors | **Fail closed** for likes only (views/shares/comments unaffected); 60 s cache |

**Upstream (blast radius):**

| Caller | Uses | Impact if `engagement` is down |
|---|---|---|
| clients (edge) | view/share + `GetPostEngagement` / `BatchGetLikes` | no like or engagement counts on posts |
| `account` | `ListLikesByAccount` (GDPR export) | exports retried next pass |

> **Critical path?** **Yes** for the read path (Redis-backed); likes and persistence are async.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `engagement.v1.EngagementService`

```protobuf
service EngagementService {
  rpc RecordView        (RecordViewRequest)        returns (CommandResponse);
  rpc RecordShare       (RecordShareRequest)       returns (CommandResponse);
  rpc GetPostEngagement (GetPostEngagementRequest) returns (PostEngagementView);
  rpc BatchGetLikes     (BatchGetLikesRequest)     returns (BatchGetLikesResponse); // likes (#665)
  rpc ListLikesByAccount (ListLikesByAccountRequest) returns (ListLikesByAccountResponse); // mesh only
}
```

**Hidden like counts (#809).** When a post's author hides like counts (profile interaction settings),
`GetPostEngagement` returns a zero `like_count` and `likes_hidden` to anyone but the
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
counts (#809) apply to posts: `count` 0 and `hidden`, the reader's own likes still shown.
`PostEngagementView` fields 2 and 3 (`reaction_scores`, `total_weighted_score`) are reserved.

### Rust ports (hexagonal contract)

```rust
pub trait LikeStore: Send + Sync + 'static {       // Redis: the hot copy
    async fn apply_total(&self, target, account, total) -> Result<i64, EngagementError>; // points added
    async fn counts(&self, targets) -> Result<Vec<i64>, EngagementError>;
    async fn mine(&self, account, targets) -> Result<Vec<i64>, EngagementError>;
}
pub trait LikeLedger: Send + Sync + 'static {      // Scylla: the durable copy
    async fn record(&self, target, account, profile_id, total, at_micros) -> Result<(), EngagementError>;
    async fn list_by_account(&self, account, limit, after) -> Result<Vec<AccountLike>, EngagementError>;
}
pub trait ScoreStore: Send + Sync + 'static { /* incr_view/share/comment, decr_comment, get_snapshot */ }
pub trait CounterLedger: Send + Sync + 'static { /* apply_interaction_delta (flush + comment consumer) */ }
```

### Error contract (`ENG-xxxx`)

| Range | Category |
|---|---|
| `ENG-5xxx` | worker / Lua script / ledger unavailable (`ENG-5003`) |
| `ENG-6xxx` | peers (`ENG-6001`: post unavailable, likes withheld) |
| `ENG-9xxx` | id parsing / domain violation / like target (`ENG-9004`) |

`ENG-1001`, `ENG-2001`/`2002`, `ENG-3001` and `ENG-9002` belonged to the weighted reactions; they are
retired, never reused.

---

## 📨 Events & Async Contract

**Publishes:** nothing. Like-driven fan-out (notifications, counters, interests, country ladder)
reads the wallet's `wallet.v1.events` directly.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `comment.created` / `comment.deleted` | `engagement-comment-consumer` | INCR/DECR comment counter (Redis + Scylla) | DLQ `{topic}.dlq` |
| `wallet.v1.events` | `engagement-stakes` | `stake_committed` → likes (#665): the account's total on a post or comment, idempotent and order-proof (Redis Lua + Scylla); a deleted account's stakes are dropped | DLQ `{topic}.dlq` |
| `account.v1.events` | `engagement-account-erasure` | `account_deleted` → forget who liked (the counts stay); other events skipped | DLQ `{topic}.dlq` |

> **Runtime contract (mandatory):** the stake, account and comment consumers run under `run_consumer` — manual
> commit after success, bounded retry with backoff + jitter, DLQ on exhaustion/poison. Totals are
> monotone, so re-delivery is safe.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis unavailable | view/share and reads fail | **Hard** — `503`; backpressure to callers | check Redis; hot path requires it |
| ScyllaDB unavailable | stakes retried, flush backs off | **Soft** — Redis reads unaffected; copies catch up | check Scylla compaction/disk I/O |
| Worker crash | partitions reassigned | at-least-once replay (`run_consumer`); monotone totals | none — self-healing |
| Redis restart **without AOF** | like sums + counters lost | counters lose current window; likes need recovery | enable AOF; rebuild likes from `likes_by_target` |
| Duplicate / late stake | — | Lua ignores a total ≤ the one held | none |

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
`engagement::service::EngagementService` — `build` wires the Redis stores, the Scylla copies and the
workers (stakes, counter flush, comments); `register` adds the gRPC + reflection services;
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

### Likes

| Variable | Default | Description |
|---|---|---|
| `ENGAGEMENT_POST_GRPC_ENDPOINT` | unset | post's mesh address (e.g. `http://post:50056`): hidden like counts are withheld (#809). Unset → withheld from nobody |
| `ENGAGEMENT_POST_RPC_TIMEOUT_MS` · `ENGAGEMENT_POST_CONNECT_TIMEOUT_MS` | `500` · `1000` | deadlines of that call |

### Service + inherited infrastructure

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENGAGEMENT_COUNTER_FLUSH_INTERVAL_SECS` | No | `5` | View/share flush cadence. |
| `REDIS_URL` | **Yes** | — | Redis connection (AOF recommended). |
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB copies. |
| `KAFKA_BROKERS` | **Yes** | `localhost:9092` | Kafka brokers. |
| `ENGAGEMENT_GRPC_ADDR` | No | `0.0.0.0:50058` | gRPC bind address. |

> Full `SCYLLA_*` / `REDIS_*` / `KAFKA_*` tuning lives in the shared storage/transport crates.

### Compile-time features
- `fred` with `i-scripts` (the likes' Lua script). `build.rs` compiles `proto/engagement/v1/*.proto`.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `0001_create_keyspace.cql` → `0002_create_post_reactions_table.cql` →
  `0003_create_post_interaction_counters_table.cql` → `0004_create_reactions_by_profile_table.cql` →
  `0005_create_likes_tables.cql` → `0006_drop_reaction_tables.cql` →
  `0007_create_erased_accounts_table.cql` against
  `engagement`, applied **before** first start. (0002's table comment held a `;`; the integration
  suites' runner split on it until it became quote-aware like `apps/migrator` — prod never was affected.
  It is a comma now — same schema.)
- **Redis durability:** enable AOF (`appendonly yes`, `appendfsync everysec`) — without it, a restart
  loses the current flush window and requires cold-start recovery from the Scylla ledger.
- **Kafka:** topics come from the event-topology registry (the infra repo's `topic-provisioner`);
  `engagement.reactions` is no longer produced nor provisioned. The `ENGAGEMENT_REACTION_WEIGHT_*` and
  `ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE` variables are gone (#665).
- **Rollout/Rollback:** `<TODO>`; the gRPC tier is stateless, but workers are at-least-once consumers —
  safe to roll.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread (required — `tokio::join!` on the read path).

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `engagement_counter_flush_lag_posts` | flush worker health | > 10 000 ⇒ behind |
| `engagement-stakes` consumer lag | likes freshness | > 50 000 ⇒ Kafka consumer lag |
| `engagement_redis_errors_total` | hot-path availability | any spike ⇒ Redis connectivity |
| `engagement_scylla_errors_total` | copies' durability | any spike ⇒ Scylla connectivity |

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

**1. Like counts drift after a Redis restart.**
Root cause: Redis was flushed/restarted without AOF; the `engagement:{post:*}:likes` / `:likers` keys are
lost. Mitigation: enable AOF to prevent recurrence; rebuild from `engagement.likes_by_target` (per
target, `HSET` each account's total into `:likers` and their sum into `:likes`) before serving reads.

**2. Stake consumer lag grows continuously.**
Root cause: Scylla writes slower than the stake rate, or too few consumer members. Mitigation: check the
`engagement-stakes` group lag; scale engagement-server replicas (≤ the topic's partitions); verify
`likes_by_target` compaction isn't saturating disk I/O.

**3. `ENG-5001 ScriptReturnInvalid` in logs.**
Root cause: a Lua script returned an unexpected type — usually a key of the wrong type. Mitigation:
verify Redis ≥ 7.0; check `TYPE engagement:{post:<id>}:likers` is `hash`; delete a corrupt key and
rebuild it from `likes_by_target` (stakes are redelivered as totals, so the next one repairs the
account's entry).

# `timeline` — Hybrid fan-out home feed that keeps celebrities off the write path

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — user-facing "Following" feed; derived, cold-start transparent |
> | **Deployable** | `crates/apps/timeline-server` (library crate: `crates/services/timeline`) |
> | **Datastores** | Redis (materialized feeds + VIP registries) · ScyllaDB keyspace `timeline` (durable cold store) |
> | **Async** | publishes nothing · consumes `post.published` / `post.deleted` / `social-graph.followed` / `.unfollowed` |
> | **Upstream callers** | `<TODO: BFF / mobile>`; calls `social-graph` (gRPC) |
> | **Downstream deps** | Redis, ScyllaDB, Kafka, `social-graph` |
> | **SLO** | hot-read sub-ms (Redis ZSET) · VIP write amplification O(1)/post |

---

## 🎯 Overview & Service Role

`timeline` provides the home feed (the "Following" tab). It aggregates posts from followed accounts
into a ranked, paginated stream using a **hybrid fan-out-on-write / fan-out-on-read** architecture.

The hard problem it solves is the **Celebrity Fan-out Problem**: a VIP author with millions of
followers would, under pure fan-out-on-write, generate millions of feed writes per post. It resolves
this by **tier-routing on the author**: Standard/Premium authors fan out to followers' Redis ZSETs;
VIP authors never fan out — their posts land in a per-author ZSET that is merged in-process at query
time, bounding write amplification to O(1) per post regardless of follower count.

**Core objectives:** sub-ms hot reads (pre-materialized Redis ZSETs, opaque cursors); VIP write
isolation; zero post content stored (only `(post_id, author_id, published_at_ms)` tokens — hydration is
the client/BFF's job); cold-start transparency (Scylla served immediately, Redis warmed async,
`is_cold` flag tells the BFF to show "loading").

---

## 📐 Architecture & Concepts

The gRPC surface is **query-only** — all writes arrive via Kafka workers.

```
Kafka: post.published │ post.deleted │ social-graph.followed/unfollowed
   ▼                    ▼                ▼
PostPublishedWorker  PostDeletedWorker  Follow{Created,Deleted}Worker
 (Std/Prem → fan-out  (VIP → ZREM;       (Created → add to following set,
  to followers' ZSETs;  Std/Prem → Scylla  backfill Std/Prem posts;
  VIP → ZADD vip:{})    purge)             Deleted → prune)
   └─────────────────────┬──────────────────────┘
                         ▼
   Redis: timeline:feed:{profile}  ZSET (per-follower) · timeline:vip:{author} ZSET
          timeline:following:{id}  SET · timeline:tier:{author} · timeline:warm:{profile}
                         ▼ cold-start
   ScyllaDB: timeline.feed_items_by_profile (TWCS) · timeline.posts_by_author (reverse index)
                         ▼
   gRPC TimelineService.GetFollowingFeed ─► BFF / mobile
```

**Fan-out routing** (a hard domain invariant in `AuthorTier::fan_out_mode()`, **not** a config flag):

| Tier | Mode | Write | Read |
|---|---|---|---|
| `Standard` (0) | `Write` | push to every follower ZSET + Scylla INSERT | serve `timeline:feed:{profile}` |
| `Premium` (1) | `Write` | same as Standard | same |
| `Vip` (2) | `Read` | ZADD `timeline:vip:{author}` only | merge at query time (`try_join_all`) |

Author tier is denormalized into every `post.published` event — **no synchronous tier lookup on the
write path**. ZSET members encode `"{post_id}:{author_id}"` so the BFF identifies the author without a
secondary lookup.

> **Invariants:** VIP authors never fan out (write amplification O(1)/post); cold-start returns Scylla
> data with `is_cold=true` and warms Redis async; following-set rebuild on Redis miss paginates
> `SocialGraphService.ListFollowing` and conservatively routes unknown tiers to `Standard`.

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| `GetFollowingFeed` p99 — warm (Redis) | `< <TODO> ms` | 1h | gRPC histogram |
| Cold-start fallback p99 (Scylla) | `< <TODO> ms` | 1h | Scylla read histogram |
| Fan-out ingest lag (`post.published`) | `< <TODO> s` | live | consumer-group lag |
| VIP write amplification | O(1) per post | — | invariant (`fan_out_mode`) |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | hot feed + VIP registries | warm reads fail | **Soft** — cold-start path serves from Scylla |
| ScyllaDB (`timeline`) | durable cold store | cold-start + ingest fail | **Hard** for cold reads; ingest retries |
| Kafka | fan-out ingest | feed stops updating | **Soft** — existing feed served |
| `social-graph` (gRPC) | following-set rebuild | rebuild on Redis miss fails | **Soft** — boots lazily; `TML-3001` retryable |
| `social-graph` (gRPC) | `CheckAccess` for the discovery feed | discovery reads fail | **Hard** for discovery (fail closed, `TML-8002`) |
| `social-graph` (gRPC) | `ListMutedProfiles` (scope posts) for both feeds | mutes are not applied for that request | **Soft** (fail open: a mute is a preference, not a safety rule) |
| `geo-discovery` (gRPC) | NEARBY candidates (`QueryTile`) | NEARBY fails | **Soft** — other rankings unaffected; `TML-8003` |

**Upstream (blast radius):**

| Caller | Uses | Impact if `timeline` is down |
|---|---|---|
| `<TODO: BFF / mobile>` | `GetFollowingFeed` | the Following home feed stops loading |
| iOS client (edge) | `GetDiscoveryFeed` | For You / Trending / Nearby stop loading |

> **Critical path?** Yes for the home-feed surface; it is a derived read-model, so an outage degrades
> the feed but not posting/social actions.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `timeline.v1.TimelineService`

```protobuf
rpc GetFollowingFeed(GetFollowingFeedRequest) returns (GetFollowingFeedResponse);

message GetFollowingFeedRequest  { string profile_id=1; int32 limit=2; string page_token=3; }
message GetFollowingFeedResponse { repeated FeedItem items=1; string next_page_token=2; bool is_cold=3; }
message FeedItem { string post_id=1; string author_id=2; int64 published_at_ms=3; }

rpc GetDiscoveryFeed(GetDiscoveryFeedRequest) returns (GetDiscoveryFeedResponse);   // edge public_read

message GetDiscoveryFeedRequest  { DiscoveryRanking ranking=1; string region=2; optional double lat=3;
                                   optional double lng=4; ContentLevel content_level=5; string page_token=6; int32 limit=7;
                                   string profile_id=8; bool non_personalized=9; }
message GetDiscoveryFeedResponse { repeated FeedItem items=1; string next_page_token=2; string region_applied=3;
                                   ContentLevel content_level_applied=4; bool personalized=5; }

// Interest tags (#662), edge authenticated + require_profile; each returns the tags left, heaviest first.
rpc ListInterests(ListInterestsRequest)   returns (InterestsResponse);   // { string profile_id=1; }
rpc RemoveInterest(RemoveInterestRequest) returns (InterestsResponse);   // { string profile_id=1; string tag=2; }
rpc ResetInterests(ResetInterestsRequest) returns (InterestsResponse);   // { string profile_id=1; }
message InterestsResponse { repeated Interest interests=1; }             // Interest { string tag=1; double weight=2; }
```

> **Wire contract:** the cursor is `base64url("{published_at_ms}:{post_id_hyphenated}")` — opaque to
> clients, decoded server-side only. `limit` is clamped to `TIMELINE_MAX_PAGE_SIZE`. `is_cold=true` means
> the page was served from ScyllaDB while Redis warms asynchronously.

### Discovery feed (`GetDiscoveryFeed`)

A feed that needs no follow graph: For You for guests and members (#673, B3), ranked for a member by its
interest tags unless it opted out (#662).

- **Pool** (Redis, `timeline:{disc}:recent|fresh|hot` + a `timeline:disc:post:<id>` hash per post): posts
  published in the last `TIMELINE_DISCOVERY_WINDOW_SECS` (72 h), capped at `TIMELINE_DISCOVERY_POOL_CAP`. Fed
  by one consumer (`timeline-discovery`) on `post.v1.events` (published with the caption's hashtags /
  deleted), `counter.v1.popularity` (all-time popularity) and `moderation.v1.events` (version-guarded
  restriction, recorded even before the publication is seen). Cache-like: if Redis loses it, it refills
  within one window.
- **Rankings.** `TRENDING` = the hot score `log10(max(popularity, 1)) + (published_s − epoch) / gravity`
  (`TIMELINE_DISCOVERY_HOT_GRAVITY_SECS`, 12.5 h: a post that much newer ranks like one ten times more
  popular), posts with some popularity only; `RECENT` = newest first; `FOR_YOU` = three hot for one *fresh*
  (no popularity yet); `NEARBY` = geo-discovery's posts around `lat`/`lng` (a wide ring above geo's
  virality floor, plus a close ring with no floor), ranked by hot score; a guest only gets its granted
  country (geo-discovery country access, passed as `guest_principal`).
- **Filters.** Deleted, removed and visibility-limited posts are never shown; age-gated ones only at
  `CONTENT_LEVEL_STANDARD`. A **guest or a 13–17 reader (the token's `age` bracket) always gets
  `RESTRICTED`**; anyone else gets what it asks for (the client sends the profile's sensitive-content
  setting, #662), `RESTRICTED` by default. The reader comes from the token
  (`edge::viewer`): authors it may not see (`CheckAccess` ≠ `VISIBLE`) are left out, and the read **fails
  closed** (`TML-8002`, UNAVAILABLE) when social-graph cannot answer. Mesh callers are unfiltered by audience.
- **Mutes (#659).** Posts by authors any of the reader's profiles muted (scope posts) are left out of the
  discovery feed, and posts by authors the feed's profile muted out of the **following feed** (a muted VIP is
  not even read; the cursor moves past skipped posts). One `ListMutedProfiles` call per request, failing open.
- **Interest tags (#662).** The same consumer reads `wallet.v1.events` (#665: a like is a point): a
  profile's *first* batch of likes on a pooled post (`stake_committed` with `first`; later batches, likes
  on comments, and a post taken down or deleted teach nothing) adds
  weight to the post's hashtags in the profile's interests (Redis, `timeline:int:{<profile>}` ZSET, plus
  `:seen` — a post counts once per 30 days — `:muted`, the removed tags, and `:off`, the opt-out). Weights decay with a 30-day
  half-life (stored inflated to a fixed epoch, so a write only increments), the 100 heaviest are kept, and
  an idle store expires after 180 days. A **FOR_YOU** page read with `profile_id` (bound to the token's
  profiles) is re-ranked by the affinity of its posts with the 20 heaviest tags — within the page, so the
  cursor is untouched; the streams' order holds between equals — and says so (`personalized`). An
  unreachable store serves the page unranked. **Opt-out, enforced here:** `ProfileFeedSettingsChanged` on
  `profile.v1.events` with `non_personalized` (profile `FeedSettings`, DSA Art. 38) erases what was learnt
  and sets `:off`, so nothing is learnt and no page is ranked until the holder turns it back on — whatever
  the request says (its `non_personalized` only turns ranking off for one read). A **13–17** profile is born
  opted out (UK Children's Code) and may opt in. A guest is never personalised; the other rankings and the
  following feed never are.
  `ListInterests` / `RemoveInterest` (the tag stays out: later reactions no longer teach it) /
  `ResetInterests` (everything, removed tags included; the opt-out stays) are owner-only. `ProfileDeleted`
  on `profile.v1.events` erases the profile's four keys (GDPR Art. 17).
- **Paging.** The cursor carries one position per stream and is bound to its ranking. A page can be short
  (even empty) with a non-empty token when its candidates were filtered out; a post moving from fresh to hot
  can come back on a later page — clients de-duplicate by `post_id`.
- **Region.** Accepted, not applied yet: v1 ranks one global pool (`region_applied` is empty).

### Rust ports (hexagonal contract)

```rust
pub trait FeedStore: Send + Sync { /* Redis hot ZSET: add/cap/prefix-remove/range */ }
pub trait VipRegistry: Send + Sync { /* per-VIP ZSET (ZADD+cap+TTL) */ }
pub trait TierCache: Send + Sync { /* author tier + warm flag */ }
pub trait FollowingStore: Send + Sync { /* following set (SADD/SREM/SMEMBERS) */ }
pub trait FeedRepository / AuthorPostRepository: Send + Sync { /* ScyllaDB cold layer */ }
pub trait SocialGraphClient: Send + Sync { /* paginated gRPC to social-graph */ }
pub trait DiscoveryPool: Send + Sync { /* discovery indices + per-post meta (Redis) */ }
pub trait InterestStore: Send + Sync { /* a profile's interest tags: reinforce / top / remove / reset (Redis) */ }
pub trait NearbyPosts: Send + Sync { /* geo-discovery QueryTile around a point */ }
```

### Error contract (`TML-xxxx`)

| Code | Variant | HTTP |
|---|---|---|
| TML-1001 | `FeedNotFound` | 404 |
| TML-2001/2002 | `FanOutFailed` / `VipRegistryWriteFailed` | 500 |
| TML-3001/3002 | `SocialGraphClientError` (retryable) / `SocialGraphInvalidId` | 500 |
| TML-4001 | `ColdStartFailed` | 500 |
| TML-5001/5002 | `ScriptReturnInvalid` / `BackfillFailed` | 500 |
| TML-6001 | `InvalidPageToken` | 422 |
| TML-8001 | `LocationRequired` (NEARBY without lat/lng) | 422 |
| TML-8002/8003 | `AccessCheckUnavailable` / `NearbyUnavailable` (retryable) | 503 |
| TML-9001..9004 | invalid ids / domain violation | 422 |

---

## 📨 Events & Async Contract

**Publishes:** none — `timeline` is a pure read-model materializer.

**Consumes:**

| Topic | Consumer group | Worker / action | On poison/exhaustion |
|---|---|---|---|
| `post.published` | `timeline-post-published` | fan-out (Std/Prem) or VIP-register | DLQ `{topic}.dlq` |
| `post.deleted` | `timeline-post-deleted` | VIP ZREM or Scylla purge | DLQ `{topic}.dlq` |
| `social-graph.followed` | `timeline-sg-followed` | backfill recent posts + update following set | DLQ `{topic}.dlq` |
| `social-graph.unfollowed` | `timeline-sg-unfollowed` | prune posts + update following set | DLQ `{topic}.dlq` |
| `post.v1.events` · `counter.v1.popularity` · `moderation.v1.events` · `wallet.v1.events` · `profile.v1.events` | `timeline-discovery` | discovery pool (publish / delete, hot score, restriction); interest tags (first like, `ProfileFeedSettingsChanged` opt-out, `ProfileDeleted` erasure) | DLQ `{topic}.dlq` |

> **Runtime contract (mandatory):** all workers run under `run_consumer` — manual commit after success,
> bounded retry with backoff + jitter, DLQ on exhaustion/poison. All downstream writes are idempotent
> (ZADD idempotent; Scylla upserts via INSERT).

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis unavailable / cold | warm reads fail | **Soft** — cold-start serves Scylla (`is_cold=true`), warms async | check Redis; self-heals |
| ScyllaDB unavailable | cold-start + ingest fail | **Hard** for cold path; ingest retries via `run_consumer` | check Scylla; drain DLQ |
| `social-graph` unreachable at boot | following rebuild fails | lazily-connected channel — timeline still boots; `TML-3001` retryable | check social-graph health |
| Tier cache miss | author tier unknown | conservatively routes to `Standard` (no blocking; corrected on next `post.published`) | none — self-correcting |
| Fan-out ingest lag | feed stale | retries within budget | scale the relevant consumer |

**Backpressure & limits.** `TIMELINE_FEED_CAP` (default 500) and `TIMELINE_VIP_REGISTRY_CAP` (200) bound
ZSET size; `TIMELINE_MAX_VIP_MERGE_SOURCES` (50) caps per-request VIP merges; `TIMELINE_MAX_PAGE_SIZE`
clamps pages.

---

## 📦 Integration & Usage

```toml
[dependencies]
timeline = { path = "crates/services/timeline" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`timeline::service::TimelineService` — `build` maps `TimelineConfig → AppConfig`, constructs the
social-graph gRPC client over a **lazily-connected** channel (timeline boots even if social-graph isn't
reachable yet), assembles cache/persistence adapters + CQRS buses, and spawns the five ingestion
workers; `register` adds the gRPC + reflection services (query-only surface); `health_probes` checks
Scylla/Redis.

### Bootstrap (`crates/apps/timeline-server`)

```rust
use std::net::SocketAddr;
use timeline::service::TimelineService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("TIMELINE_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50070".to_owned())
        .parse()?;
    service_runtime::serve::<TimelineService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### `timeline`-specific variables

| Variable | Default | Description |
|---|---|---|
| `TIMELINE_FEED_CAP` | `500` | Max entries per follower's Redis ZSET. |
| `TIMELINE_VIP_REGISTRY_CAP` | `200` | Max entries per VIP author ZSET. |
| `TIMELINE_BACKFILL_LIMIT` | `100` | Max posts backfilled on follow. |
| `TIMELINE_WARM_TTL_SECS` | `86400` | Warm-flag TTL (24 h). |
| `TIMELINE_TIER_CACHE_TTL_SECS` | `3600` | Author tier cache TTL. |
| `TIMELINE_VIP_REGISTRY_TTL_SECS` | `604800` | VIP ZSET TTL (7 d). |
| `TIMELINE_MAX_PAGE_SIZE` | `50` | Max page size. |
| `TIMELINE_MAX_VIP_MERGE_SOURCES` | `50` | Max VIP ZSETs merged per request. |
| `TIMELINE_SOCIAL_GRAPH_PAGE_SIZE` | `500` | Pagination size for social-graph lists. |
| `TIMELINE_SOCIAL_GRAPH_ENDPOINT` | `http://social-graph:50051` | social-graph gRPC endpoint. |
| `TIMELINE_DISCOVERY_WINDOW_SECS` | `259200` | How long a post stays in the discovery pool (72 h). |
| `TIMELINE_DISCOVERY_POOL_CAP` | `10000` | Max posts in the discovery pool. |
| `TIMELINE_DISCOVERY_HOT_GRAVITY_SECS` | `45000` | Hot-score gravity (12.5 h per 10× popularity). |
| `TIMELINE_GEO_DISCOVERY_ENDPOINT` | `http://localhost:50054` | geo-discovery gRPC endpoint (NEARBY). |
| `TIMELINE_NEARBY_RADIUS_KM` · `TIMELINE_NEARBY_CLOSE_RADIUS_KM` | `25` · `2` | NEARBY wide / close ring radius. |
| `TIMELINE_NEARBY_CANDIDATES` | `300` | NEARBY candidates ranked per request. |
| `TIMELINE_KAFKA_GROUP_*` | `timeline-*` | Consumer group IDs (post-published/deleted, sg-followed/unfollowed, discovery). |

> Standard ScyllaDB / Redis / Kafka connection variables from the shared storage crates apply.
> `TIMELINE_GRPC_ADDR` defaults to `0.0.0.0:50070`.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `0001_create_keyspace.cql` → `0002_create_feed_items_by_profile_table.cql` →
  `0003_create_posts_by_author_table.cql` against `timeline`, applied **before** first start.
- **Stateful gotchas:** `AuthorTier::fan_out_mode()` is a hard invariant, not config — changing tier
  semantics requires a feed rebuild. ZSET member encoding (`{post_id}:{author_id}`) and cursor format are
  read contracts.
- **Rollout/Rollback:** `<TODO>`; the lazily-connected social-graph channel makes boot order tolerant —
  safe to roll.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread (VIP merge uses `try_join_all`). Global tracing/OTel subscriber
  installed before `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `GetFollowingFeed` p99 (warm) | hot-read SLO | > SLO ⇒ page |
| `is_cold` rate | Redis warm-coverage | sustained high ⇒ check warming / Redis evictions |
| fan-out consumer lag | feed freshness | > threshold ⇒ scale consumers |
| `TML-3001` rate | social-graph dependency health | spike ⇒ check social-graph |
| DLQ produce rate (`{topic}.dlq`) | poison / retry-exhausted | any sustained rate ⇒ page |

---

## 🛠️ Local Development

```bash
docker compose up -d scylladb redis kafka     # repo-root compose
for f in crates/services/timeline/migrations/*.cql; do cqlsh -f "$f"; done
cargo build -p timeline && cargo clippy -p timeline --all-targets
cargo test  -p timeline
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. A VIP author's posts don't appear in followers' feeds.**
Root cause: this is by design — VIP posts are *not* fanned out; they live in `timeline:vip:{author}` and
are merged at query time. If they're missing from the merged result, check `TIMELINE_MAX_VIP_MERGE_SOURCES`
(the follower may follow more VIPs than the merge cap) or the VIP ZSET TTL. Mitigation: confirm
`ZCARD timeline:vip:{author}` > 0; raise the merge cap if a user follows many VIPs.

**2. `GetFollowingFeed` keeps returning `is_cold=true`.**
Root cause: the warm flag (`timeline:warm:{profile}`) keeps expiring or the async warm task is failing —
often Redis eviction pressure or a `ScriptReturnInvalid` (TML-5001) in the warm path. Mitigation: check
Redis `maxmemory`/eviction and the warm-task logs; the cold path is still correct (served from Scylla),
just slower.

**3. A new follow's posts don't show up (no backfill).**
Root cause: the `social-graph.followed` event was consumed but backfill failed (`TML-5002`), or the
followee is VIP (no backfill — merged live instead). Mitigation: check `timeline-sg-followed` lag/DLQ;
verify the followee's tier — VIP follows are correct to skip backfill.

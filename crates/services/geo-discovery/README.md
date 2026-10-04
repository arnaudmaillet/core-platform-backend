# `geo-discovery` — Real-time H3 spatial index for the global map feed, served in two Redis round-trips

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — query-only read surface; degradable to ScyllaDB |
> | **Deployable** | `crates/apps/geo-discovery-server` (library crate: `crates/services/geo-discovery`) |
> | **Datastores** | Redis (ZSET index + msgpack pin & card projections) · ScyllaDB keyspace `geo_discovery` |
> | **Async** | publishes nothing · consumes `post.published` / `post.deleted` / `moderation.v1.events` / `engagement.score_updated` / `profile.tier_changed` |
> | **Upstream callers** | `<TODO: BFF / map clients>` |
> | **Downstream deps** | Redis, ScyllaDB, Kafka |
> | **SLO** | tile query p99 **< 50 ms** at continental scale |

---

## 🎯 Overview & Service Role

`geo-discovery` is the geospatial ingestion + query engine behind the global interactive map. Every
published post is encoded into Uber H3 hexagonal cells at three resolutions, scored by real-time
virality, and exposed through a **two-step Radar/Focus read contract** — with no fan-out to `post` or
`profile` at query time.

The hard problem it solves is that **a global map feed with 100 M active posts cannot afford N+1 gRPC
lookups on every viewport pan**. It resolves this by splitting the read path by interaction stage:

- **Radar (`QueryTile`)** — the high-frequency pan/zoom path. Returns lightweight `RadarPin`s
  (`post_id`, exact `lat`/`lng`, `thumbnail_url`) served **exclusively from Redis** (spatial ZSET +
  pin projection). No card hydration, no ScyllaDB, fail-open.
- **Focus (`GetGeoTimeline`)** — the on-tap path. Batches focused `post_id`s into fully-hydrated
  `MapPostCard`s (caption, author metadata, tier), read from Redis with a ScyllaDB fallback — the cold
  read the pan path deliberately avoids.

Card fields are denormalized at ingest so rendering stays local. `thumbnail_url`, `caption`, and
`author_tier` arrive on `post.published`; `author_handle` / `author_avatar_url` are reserved on the
card and backfilled from `profile.v1.events` (a separate join — empty until then). Dynamic relational
state (friend/following) is resolved client-side, preserving a *shared* card cache and avoiding
O(users × posts) cache variants.

**Core objectives:** sub-50 ms P99 tile query (Redis pipeline + MGET); ~9 GB Redis at 100 M posts
(Top-K cap + cold eviction + power-law filtering); zero query-time fan-out; 48 h default TTL (30 d for
premium), enforced via Scylla `USING TTL` and Redis `EX`.

---

## 📐 Architecture & Concepts

The gRPC surface is **query-only** — all writes arrive via Kafka workers.

```
WRITE: post.published          ─► PostIndexerWorker  (H3 encode R5/7/9 → Scylla INSERT ×4 → Redis ZADD+cap ×3 → pin SET always → card SET if score≥θ)
       engagement.score_updated ─► ScoreUpdaterWorker (Scylla UPDATE score → ZADD XX ×3, skip-if-absent)
       profile.tier_changed     ─► TierSyncWorker     (Scylla UPDATE author_tier → Redis DEL card)
       (60s tick)               ─► TilePrunerWorker    (PRUNE_COLD_TILES Lua → DEL cold tile ZSETs)

READ (Radar):  QueryTile      ─► zoom→resolution ─► viewport→grid_disk (≤50 tiles)
                              ─► Phase 1: ZRANGEBYSCORE ×N (1 RTT via fred mux) → post_ids
                              ─► Phase 2: pin GET ×M (1 RTT) → RadarPin   (Redis-only, no Scylla fallback)
READ (Focus):  GetGeoTimeline ─► card MGET ×M (1 RTT) → MapPostCard
                              ─► (miss): Scylla get_card (Fast profile)
```

**Redis taxonomy:** `sg:geo:tile:{h3}:{res}` (ZSET, score=virality, pruned), `sg:geo:pin:{post_id}`
(STRING, msgpack `RadarPin`, `EX ttl` — Radar projection, written for every indexed post),
`sg:geo:card:{post_id}` (STRING, msgpack `MapPostCard`, `EX ttl` — Focus projection, score-gated),
`sg:geo:hot_tiles` (ZSET, last-access epoch per tile).
**ScyllaDB:** `posts_by_tile` (TWCS, PK `(h3_index, resolution)` — composite to avoid hot urban shards),
`map_post_cards` (LCS, PK `post_id` — pure point reads, single mutable score column).

Three atomic Lua scripts drive the hot path: `ZADD_TOPK` (cap per tile, evict lowest on overflow),
`ZADD_XX` (update only if member present — evicted posts never re-inserted), `PRUNE_COLD_TILES` (evict
tiles idle past the cold threshold).

> ⚠️ **Cluster note:** `PRUNE_COLD_TILES` builds tile keys inside Lua and is **not Redis Cluster-safe**
> (cross-slot `DEL`). This service assumes standalone / single-shard Redis.

> **Invariants:** zoom→resolution mapping with virality floors (R5 floor 500 / R7 50 or 5 / R9 0) and
> Top-K caps (200/500/1000) bound per-tile RAM regardless of urban density.

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| `QueryTile` p99 | **< 50 ms** | 1h | `geo_discovery_tile_query_duration_ms` |
| Cache miss ratio | < 0.30 | 5m | `geo_discovery_cache_miss_ratio` |
| `post.published` ingest lag | < 30 s | live | `geo_discovery_post_indexer_lag_seconds` |
| `engagement.score_updated` lag | < 10 s | live | `geo_discovery_score_updater_lag_seconds` |
| Redis spatial RAM (hot tiles) | < 50 000 tiles | live | `geo_discovery_hot_tile_count` |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`. Map data is acceptably stale within the 48 h
retention window, so ingest-lag SLOs are softer than the query-latency SLO.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | ZSET index + card cache | query latency rises | **Soft** — reads fall back to Scylla (not an outage) |
| ScyllaDB (`geo_discovery`) | durable source of truth | ingest retries; cold reads fail | **Hard** for cold path; Redis-served queries unaffected |
| Kafka | ingest (post/score/tier) | map data goes stale | **Soft** — queries still serve cached data |

**Upstream (blast radius):**

| Caller | Uses | Impact if `geo-discovery` is down |
|---|---|---|
| `<TODO: BFF / map clients>` | `QueryTile` (Radar), `GetGeoTimeline` (Focus) | the map feed stops loading |

> **Critical path?** Yes for the map surface specifically; it is a derived read-model, so a full outage
> degrades the map but nothing else.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `geo_discovery.v1.GeoDiscoveryService`

```protobuf
service GeoDiscoveryService {
  rpc QueryTile      (QueryTileRequest)      returns (QueryTileResponse);      // Radar (pan): lean pins
  rpc GetGeoTimeline (GetGeoTimelineRequest) returns (GetGeoTimelineResponse); // Focus (tap): full cards
  rpc GetCountryAccess (GetCountryAccessRequest) returns (GetCountryAccessResponse); // country access from location
}
message QueryTileRequest  { Viewport viewport = 1; int32 zoom_level = 2; string guest_principal = 3; } // zoom ∈ [0,15]; 3 = mesh only
message QueryTileResponse { reserved 1; repeated RadarPin pins = 3; int32 tile_count = 2; } // field 1 was `cards`
message RadarPin { string post_id=1; double lat=2; double lng=3; string thumbnail_url=4; }

message GetGeoTimelineRequest  { repeated string post_ids = 1; }
message GetGeoTimelineResponse { repeated MapPostCard cards = 1; }
message GetCountryAccessRequest  { string current_country = 1; }                 // ISO alpha-2 from the device, or empty
message GetCountryAccessResponse { string current_country = 1; CountryAccessOutcome outcome = 2; } // echoed only when GRANTED
message MapPostCard { string post_id=1; string author_id=2; string author_handle=3;
  string author_avatar_url=4; string thumbnail_url=5; int64 h3_index_r7=6;
  float virality_score=7; int64 published_at_ms=8; AuthorTier author_tier=9; string caption=10; }
```

> **Radar vs Focus.** `QueryTile` is the high-frequency pan path → lean `RadarPin`s (Redis-only,
> fail-open). `GetGeoTimeline` is the on-tap batch path → hydrated `MapPostCard`s (Redis + Scylla
> fallback). `QueryTileResponse` field **1** previously held `repeated MapPostCard cards`; it is now
> **reserved**, with pins on a fresh field number to stay wire/JSON-compatible (`buf WIRE_JSON`).

> **What the map may show.** A post leaves the map when it is **deleted** (`post.deleted`, for good)
> or when moderation **removes or limits** it (`moderation.v1.events`: `remove_content` /
> `visibility_limit` on a post — the map is a discovery surface). The card row is marked
> (`suppressed`, `moderation_version`; writes carry the row's remaining TTL) and its pin and cached
> card leave Redis, so both paths drop it; a **newer** reversal restores it and rebuilds the pin
> and spatial-index entries from the card's stored coordinates. The two consumers are unordered: a
> decision that arrives before its post.published is kept as a **tombstone** (visibility cells only,
> 30-day TTL), and the indexer then stores the card without surfacing it, re-stamping the
> visibility with the card's TTL so it never expires first. **Per reader:** for any caller but the mesh, pins and cards are
> kept only for authors the reader may see (social-graph `CheckAccess`, one bulk call; private authors
> they don't follow, blocks either way and hidden authors are dropped; one's own posts always stay).
> That check fails **closed** (`GEO-6001`, `UNAVAILABLE`).

> **Country access (guest mode, #680 B10).** `GetCountryAccess` (edge `public_read`: members and guests)
> takes the country the device derived from its location — an ISO code, never a coordinate. It is
> **granted** only when the request's network country (GeoIP: the client address the ALB appended to
> `X-Forwarded-For`, looked up in a MaxMind-format database, `GEO_GEOIP_MMDB_PATH`) is that country **or
> a neighbouring one** (a border town, a phone on a neighbouring network); an unknown network grants
> nothing (`UNVERIFIABLE`), as does a missing database. The grant is stored per principal (token `sub`,
> `sg:geo:cc:{sub}`, `GEO_COUNTRY_GRANT_TTL_SECS`); a new grant replaces the previous one (the country
> left behind locks again) and an empty `current_country` clears it. **A guest session's map** —
> `QueryTile`, `GetGeoTimeline`, and timeline's NEARBY feed (mesh `guest_principal`) — shows **only its
> granted country, and nothing without one**. Members are not limited in v1 (no server-side unlocks
> yet). Countries and neighbours come from `data/countries.json`, the iOS app's own Natural Earth
> borders, so the server and the device put a post in the same country (a point within ~2 km of a
> border or coast counts for that country).
>
> **Wire contract:** `AuthorTier` is 0-based **with** an `UNSPECIFIED=0` safe default (= Standard);
> `STANDARD=1, PREMIUM=2, VIP=3`. Badge rendering: `author_tier` → static badge; `is_friend`/`is_following`
> are deliberately **absent** (resolved client-side from the session social graph). `author_handle` /
> `author_avatar_url` are reserved on the card and backfilled from `profile.v1.events` (empty until then).

### Rust ports (hexagonal contract)

```rust
pub trait SpatialIndex: Send + Sync { /* upsert (ZADD+cap), update_score (ZADD XX), query (ZRANGEBYSCORE), touch_hot_tiles */ }
pub trait PinStore:     Send + Sync { /* set, mget (same-length Vec, None=miss), del — Radar pin projection */ }
pub trait CardStore:    Send + Sync { /* set, mget (same-length Vec, None=miss), del — Focus card projection */ }
pub trait TileRepository: Send + Sync { /* insert_tile_entry, upsert_card, update_card_score/tier, get_card, list_tile_post_ids */ }
pub trait GeoIp: Send + Sync { /* IP → country (MaxMind DB file); None = unknown */ }
pub trait CountryGrantStore: Send + Sync { /* get / set / clear the country granted to a principal (Redis) */ }
```

### Error contract (`GEO-xxxx`)

| Code | HTTP | Meaning |
|---|---|---|
| GEO-1001/1002 | 422 | coords outside WGS-84 / invalid H3 index |
| GEO-2001/2002 | 422 | viewport SW≥NE / zoom outside [0,15] |
| GEO-4001 | 500 | Lua returned unexpected value |
| GEO-5001/5002 | 500 | msgpack ser / deser failure |
| GEO-9001..9003 | 422 | malformed UUIDs / domain violation |
| GEO-6001 | 503 | audience check (social-graph `CheckAccess`) unavailable; client reads fail closed (`UNAVAILABLE`, retryable) |
| GEO-9004/9005 | 422 | invalid ISO country code / country access without a client principal (mesh) |

---

## 📨 Events & Async Contract

**Publishes:** none — `geo-discovery` is a pure read-model materializer.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `post.published` | `geo-discovery-post-indexer` | H3 index + card projection | DLQ `{topic}.dlq` |
| `post.deleted` + `moderation.v1.events` | `geo-discovery-visibility` | map suppression: delete → permanent; `remove_content` / `visibility_limit` on a post → hidden; a newer reversal → restored (version-guarded; actor-level and other events skipped) | DLQ `{topic}.dlq` |
| `engagement.score_updated` | `geo-discovery-score-updater` | virality score sync (ZADD XX) | DLQ `{topic}.dlq` |
| `profile.tier_changed` | `geo-discovery-tier-sync` | author tier sync + card invalidation (one event per `post_id`, stateless) | DLQ `{topic}.dlq` |

> **Runtime contract (mandatory):** all three workers run under `run_consumer` — retry in place with
> backoff + jitter (≤5 attempts), dead-letter on exhaustion and commit past it so a partition never
> stalls. At-least-once; all writes idempotent. Scylla is the durable source of truth; Redis ZSETs
> repopulate on replay from `earliest`.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis unavailable | query latency up | **Soft** — Scylla write succeeds; query degrades to full Scylla reads | auto-recovers on reconnect |
| ScyllaDB unavailable | ingest retries | `run_consumer` retries→DLQ; cold reads fail | drain DLQ once Scylla recovers |
| Consumer lag | map data stale | query path unaffected (Redis/Scylla reads) | scale consumer replicas |
| Redis memory pressure | `hot_tile_count` climbs | TilePruner evicts every 60 s; Top-K cap bounds per-tile | lower `GEO_TILE_COLD_THRESHOLD_SECS` |
| Score event storm | — | `ZADD_XX` skips absent members; no ZSET inflation | self-limiting |

**Backpressure & limits.** Top-K cap on every `ZADD`; cold-tile eviction every 60 s; viewport capped at
≤50 H3 tiles per query. Writes use Strict (`LocalQuorum`), reads use Fast (`LocalOne` + speculative).

---

## 📦 Integration & Usage

```toml
[dependencies]
geo-discovery = { path = "crates/services/geo-discovery" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`geo_discovery::service::GeoDiscoveryService` — `build` constructs Scylla/Redis clients, instantiates
`RedisGeoSpatialIndex`/`RedisPinStore`/`RedisCardStore`/`ScyllaTileRepository`, registers `QueryTileHandler` (Radar) + `GetGeoTimelineHandler` (Focus) (query-only
surface; writes arrive via Kafka), and spawns the three workers + `TilePrunerWorker`; `register` adds
the gRPC + reflection services; `health_probes` checks Scylla/Redis.

### Bootstrap (`crates/apps/geo-discovery-server`)

```rust
use std::net::SocketAddr;
use geo_discovery::service::GeoDiscoveryService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("GEO_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50054".to_owned())
        .parse()?;
    service_runtime::serve::<GeoDiscoveryService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_URI` | **Yes** | — | ScyllaDB contact points. |
| `REDIS_URL` | **Yes** | — | Redis connection URL (standalone / single-shard). |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers. |
| `GEO_GRPC_ADDR` | No | `0.0.0.0:50054` | gRPC bind address. |
| `GEO_CARD_CACHE_THRESHOLD` | No | `10.0` | Min virality to cache a card (power-law filter). |
| `GEO_DEFAULT_RETENTION_SECS` | No | `172800` | Default post TTL (48 h). **Must match Scylla `default_time_to_live`.** |
| `GEO_TILE_PRUNER_INTERVAL_SECS` | No | `60` | Cold-tile eviction tick. |
| `GEO_TILE_COLD_THRESHOLD_SECS` | No | `1800` | Inactivity window before a tile ZSET is evicted. |
| `GEO_POST_INDEXER_GROUP_ID` / `GEO_SCORE_UPDATER_GROUP_ID` / `GEO_TIER_SYNC_GROUP_ID` | No | service-specific | Kafka consumer groups. |
| `GEO_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Yes** (prod) | `http://localhost:50053` | social-graph endpoint for the per-reader audience filter (`CheckAccess`); client reads fail closed without it. |
| `GEO_AUDIENCE_RPC_TIMEOUT_MS` / `GEO_AUDIENCE_CONNECT_TIMEOUT_MS` | No | `500` / `500` | deadlines on that call. |
| `GEO_VISIBILITY_GROUP_ID` | No | `geo-discovery-visibility` | Kafka group of the map-suppression consumer. |
| `GEO_GEOIP_MMDB_PATH` | **Yes** (prod) | — | MaxMind-format IP→country database (DB-IP Lite country, CC BY 4.0 — *IP Geolocation by DB-IP*, <https://db-ip.com>; or GeoLite2-Country). Missing → country access grants nothing. |
| `GEO_GEOIP_PRIVATE_NETWORK_COUNTRY` | No | — | What a private/loopback client address resolves to: an ISO code, or `*` = the device's claim. **Local fleet only** — never in a deployed env. |
| `GEO_TRUSTED_PROXY_HOPS` | No | `1` | Proxies appending to `X-Forwarded-For` (the ALB); the client address is that many entries from the right. |
| `GEO_COUNTRY_GRANT_TTL_SECS` | No | `43200` | How long a granted country stays open without being confirmed again (12 h). |

> No compile-time feature flags. `build.rs` compiles `proto/geo_discovery/v1/*.proto`. ScyllaDB profiles:
> Strict (`LocalQuorum`) for mutations, Fast (`LocalOne` + speculative) for reads.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `0001_create_keyspace.cql` → `0002_create_posts_by_tile_table.cql` →
  `0003_create_map_post_cards_table.cql` → `0004_add_author_tier_column.cql` →
  `0005_add_caption_column.cql` against `geo_discovery`, applied **before** first start.
- **Stateful gotchas:** `GEO_DEFAULT_RETENTION_SECS` must equal the Scylla table TTL; the composite
  `(h3_index, resolution)` partition key and zoom→resolution mapping are read contracts.
- **Cold-start:** workers replay from `earliest`; Redis ZSETs repopulate automatically. Safe to roll.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread (required — `tokio::join!` concurrent Scylla+Redis writes). `h3o` is
  pure Rust. Memory floor ~512 MB; `ulimit -n ≥ 4096`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `geo_discovery_tile_query_duration_ms` | query SLO | p99 > 50 ms ⇒ page |
| `geo_discovery_cache_miss_ratio` | Redis offload health | > 0.30 for 5m ⇒ investigate |
| `geo_discovery_hot_tile_count` | spatial RAM | > 50 000 ⇒ tune pruner |
| `geo_discovery_post_indexer_lag_seconds` | ingest freshness | > 30 s ⇒ scale consumers |
| `geo_discovery_card_serialization_errors` | schema/memory | > 0 in 1m ⇒ investigate |

**Redis memory budget (reference):** ~0.7 GB at 10 M posts, ~7 GB at 100 M. Levers: per-resolution
`top_k_cap`, `GEO_CARD_CACHE_THRESHOLD`, `GEO_TILE_COLD_THRESHOLD_SECS`.

---

## 🛠️ Local Development

```bash
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/geo-discovery/migrations/*.cql; do cqlsh 127.0.0.1 9042 -f "$f"; done
cargo build -p geo-discovery && cargo clippy -p geo-discovery -- -D warnings
cargo test  -p geo-discovery
SCYLLA_URI=127.0.0.1:9042 REDIS_URL=redis://127.0.0.1:6379 KAFKA_BROKERS=127.0.0.1:9092 cargo run -p geo-discovery
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. `QueryTile` returns `tile_count > 0` but empty `pins`.**
Root cause (most common): ZSETs empty but Scylla has rows ⇒ the `geo-discovery-post-indexer` is lagging;
or ZSETs populated but pins missing ⇒ pin writes failed (the Radar path is Redis-only with no Scylla
fallback — check `geo_discovery_card_serialization_errors` and indexer logs). Mitigation: check
`kafka-consumer-groups --describe --group geo-discovery-post-indexer` and scale. Verify the client
isn't sending inverted SW/NE coords. (Empty Focus `cards` from `GetGeoTimeline` instead points at
`GEO_CARD_CACHE_THRESHOLD` too high — but Focus falls back to ScyllaDB, so a card should still resolve.)

**2. Redis memory growing without bound.**
Root cause: TilePruner crashed (check for `tile pruner worker started`; `geo_discovery_tile_pruner_evictions`
stuck at 0) or `GEO_TILE_COLD_THRESHOLD_SECS` too high. Mitigation: restart/redeploy; lower the cold
threshold to `900`. Emergency: `redis-cli FLUSHDB` (announce maintenance) — next queries cold-start from
Scylla.

**3. Score updates not reflected on the map.**
Root cause: the post was Top-K evicted (`ZADD_XX` skips absent members — expected; refreshes on TTL), or
`geo-discovery-score-updater` has consumer lag. Mitigation: compare Scylla `map_post_cards.virality_score`
to the Redis `ZSCORE`; if Scylla is stale too, scale the score-updater consumer.

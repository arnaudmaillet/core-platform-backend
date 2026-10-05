# `notification` — Semantic event ingestion, durable activity feed, and real-time push

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-2** — derived/best-effort; feed is durable, pushes are best-effort |
> | **Deployable** | `crates/apps/notification-server` (library crate: `crates/services/notification`) |
> | **Datastores** | ScyllaDB keyspace `notification` (TWCS feed + counters) · Redis (collapse + unread) |
> | **Async** | publishes nothing · consumes `engagement.reactions` / `comment.created` / `post.published` / `social-graph.followed` / `social-graph.follow_requested` |
> | **Upstream callers** | `<TODO: mobile / BFF (stream + feed reads)>` |
> | **Downstream deps** | ScyllaDB, Redis, Kafka |
> | **SLO** | unread-count read sub-ms (Redis) · feed read O(1) paginated · push best-effort |

---

## 🎯 Overview & Service Role

`notification` closes the user feedback loop. It ingests semantic business events from Kafka
(`engagement.reactions`, `comment.created`, `post.published`), persists durable per-profile activity
records to ScyllaDB, and dispatches real-time pushes to active clients via a gRPC server-streaming
channel.

The hard problem it solves is **celebrity fan-out**: a post drawing 10k+ reactions/second would saturate
a single ScyllaDB partition and spam the target. It resolves this with a **layered write-collapse
pipeline** — in-batch HashMap collapse, a Redis cross-batch window for hot subjects, and an hourly
per-subject cap — so a viral subject becomes a single, periodically-flushed activity row.

**Core objectives:** O(1) cursor-paginated activity feed (no `ALLOW FILTERING`); sub-ms unread badge
(Redis L1, Scylla counter L2 fallback); fan-out protection on celebrity partitions. **SRP:** stores
semantic relation IDs only — no localized strings, handles, or content; UI hydration is the client's job.

---

## 📐 Architecture & Concepts

```
Kafka: engagement.reactions │ comment.created │ post.published
   │                          │                 │
ReactionNotificationWorker  CommentNotificationWorker  MentionNotificationWorker
 (L1 in-batch collapse,     (cache comment author,    (cache post author, parse
  L2 Redis hot window,       block-gate + self-guard)  @mentions from caption)
  L3 hourly cap)
   └──────────────┬──────────────────┬──────────────────┘
                  ▼
       CollapseFlushWorker (polls notification:window_schedule ZSET every 30s,
                            drains settled Redis windows → single Scylla row)
                  ▼
   ScyllaDB notification.notifications_by_profile (TWCS 7d windows, 90d TTL,
       PK target_profile_id, CK created_at DESC, notification_id ASC)
                  ▼
   gRPC NotificationService: List / GetUnreadCount / MarkRead / MarkAllRead
                            + StreamNotifications (tokio::broadcast per profile)
```

> **Invariants:** `NotificationView` carries only UUIDs + enum ints (no PII/content). `MarkRead`
> requires both `notification_id` AND `created_at_ms` (the full Scylla clustering key for a point
> UPDATE). `read_horizon_ms` (set by `MarkAllRead`) renders everything `created_at_ms ≤ horizon` as read
> regardless of the per-row `is_read` flag. Idempotency: dedupe claim keys
> (`notification:dedupe:{profile}:…`) prevent a redelivered event from double-incrementing the counter.

---

## 📊 Service Level Objectives (SLO)

> TIER-2: the durable feed and unread counter carry soft objectives; real-time pushes are explicitly
> best-effort (no delivery guarantee — clients reconcile via `ListNotifications`).

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| `GetUnreadCount` p99 (Redis L1) | `< <TODO> ms` | 1h | gRPC histogram |
| `ListNotifications` p99 (paginated) | `< <TODO> ms` | 1h | Scylla read histogram |
| Ingest consumer lag | `< <TODO> s` | live | `kafka_consumer_group_lag{group=~"notification-.*"}` |
| Feed durability | no acked notification lost | — | Scylla write + manual-commit at-least-once |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`notification`) | durable feed + counters | feed writes/reads fail | **Hard** for feed; at-least-once retries |
| Redis | collapse windows + unread L1 + block/author caches | collapse + unread degrade | **Soft** — Scylla counter is durability anchor |
| Kafka | event ingest | new notifications stop | **Soft** — existing feed served; at-least-once on recovery |

**Upstream (blast radius):**

| Caller | Uses | Impact if `notification` is down |
|---|---|---|
| `<TODO: mobile / BFF>` | feed reads + `StreamNotifications` | the bell icon / activity feed stops updating |

> **Critical path?** **No** — derived, async, best-effort. An outage degrades engagement but does not
> block core user actions.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `notification.v1.NotificationService`

```protobuf
service NotificationService {
  rpc ListNotifications   (ListNotificationsRequest)   returns (ListNotificationsResponse);
  rpc GetUnreadCount      (GetUnreadCountRequest)       returns (GetUnreadCountResponse);
  rpc MarkRead            (MarkReadRequest)             returns (CommandResponse);  // needs notification_id + created_at_ms
  rpc MarkAllRead         (MarkAllReadRequest)          returns (CommandResponse);  // sets read_horizon_ms
  rpc StreamNotifications (StreamNotificationsRequest)  returns (stream StreamNotificationsResponse);
  // Push devices and preferences (#654)
  rpc RegisterDevice                (RegisterDeviceRequest)                returns (CommandResponse);
  rpc UnregisterDevice              (UnregisterDeviceRequest)              returns (CommandResponse);
  rpc GetNotificationPreferences    (GetNotificationPreferencesRequest)    returns (NotificationPreferences);
  rpc UpdateNotificationPreferences (UpdateNotificationPreferencesRequest) returns (NotificationPreferences);
  rpc ResolvePushTargets            (ResolvePushTargetsRequest)            returns (ResolvePushTargetsResponse);  // MESH-ONLY
}
```

**Push devices and preferences (#654).** The contract and storage for push; **nothing sends a push
yet** (no APNs credentials). Every RPC but `ResolvePushTargets` is edge `authenticated` and bound to
`profile_id`.
- `RegisterDevice(device_id, token, platform, environment, timezone)` — call it on every launch. A token
  registered for another **account** leaves that account's profiles (a handed-over phone never gets the
  old account's pushes); a device's new token replaces its old one. Tables `push_devices` (by profile)
  and `push_device_tokens` (by token). **Bound to the device (#725):** `device_id` is the id the session
  sent at login (`DeviceContext.device_id`, the token's `did`): on the edge another id is
  `PERMISSION_DENIED`, and a token registered from **another device** for another account is refused
  (`NTF-2004`, `ALREADY_EXISTS`) — a token only moves between accounts on its own phone, so a leaked token
  cannot be pulled into someone else's account. A session without `did` (and the mesh) keeps the old rule.
- Preferences (`notification_preferences`, one JSON document per profile): push and email per category
  (likes, comments, mentions, new followers, follow requests, messages, posts from followed accounts,
  places nearby, wallet), a pause (≤ 8 h), quiet hours read in the holder's IANA zone. Defaults: every
  push on, every email off; **13–17: quiet hours 22:00–07:00** (from the token's `age` on reads, and
  written at a teen's first `RegisterDevice` so the sender applies them). Stored as the teen default,
  they lift once the token says `18+`: at the next edge `RegisterDevice` (the app refreshes its device at
  launch) or preferences call. Quiet hours the holder set themselves stay; an unknown age (the mesh, no
  date of birth) changes nothing. Marketing email is the
  account's `marketing` consent (`account.v1.UpdateConsents`), not a second toggle here.
- `ResolvePushTargets(profile_id, category)` (mesh) is what the push sender will ask: the devices, or
  `allowed = false` when the category is off, a pause runs or it is quiet hours.

### Rust ports (hexagonal contract)

```rust
pub trait NotificationRepository: Send + Sync + 'static { /* insert, list_paginated, mark_read, *_counter */ }
pub trait UnreadCounter:          Send + Sync + 'static { /* incr/decr/reset/get + read_horizon (Redis L1 + Scylla L2) */ }
pub trait BlockCache:             Send + Sync + 'static { /* is_blocked(sender, target) — social-graph gate */ }
pub trait StreamRegistry:         Send + Sync + 'static { /* subscribe/broadcast (broadcast::Receiver per profile) */ }
pub trait DeviceRegistry:         Send + Sync + 'static { /* register/unregister/devices — push devices per profile */ }
pub trait PreferenceStore:        Send + Sync + 'static { /* get/put — notification preferences per profile */ }
```

### Error contract (`NTF-xxxx`)

`NTF-1xxx` lifecycle … `NTF-6001` author-cache miss (reaction notification dropped) … `NTF-9xxx`
identifiers — via the shared `error` crate.

---

## 📨 Events & Async Contract

**Publishes:** none — `notification` is a pure consumer/sink.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `engagement.reactions` | `notification-reaction-consumer` | reaction notifications (collapsed) | DLQ `{topic}.dlq` |
| `social-graph.followed` + `social-graph.follow_requested` | `notification-follow-consumer` | follows → `FOLLOW` to the followee; a request to a private profile → `FOLLOW_REQUEST` to its owner (the app opens the requests inbox); an approved request (`via_request`) → `FOLLOW_ACCEPTED` to the requester, not the owner again (#755). Subject: the other profile (`SUBJECT_KIND_PROFILE`). Block-gated, self-guarded, one notification per event (deterministic id) | DLQ `{topic}.dlq` |
| `comment.created` | `notification-comment-consumer` | comment notifications (block-gated, self-guarded; none for a `quiet` event: a restricted author, #659) | DLQ `{topic}.dlq` |
| `post.published` | `notification-mention-consumer` | parse `@mentions`, cache post author | DLQ `{topic}.dlq` |

> **Runtime contract (mandatory):** all workers run under `run_consumer` — manual commit after success
> (`enable_auto_commit=false`, earliest reset), bounded retry with backoff + jitter, DLQ on
> exhaustion/poison. Scale consumer replicas up to each topic's partition count.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Celebrity fan-out (10k/s) | — | L1 in-batch + L2 Redis 30 s window (heat > 100) + L3 hourly cap (3/subject) | none — designed for it |
| Redis unavailable | block/heat checks skipped | workers proceed; Scylla writes continue; unread accrues inconsistency until recovery | check Redis; Scylla counter reconciles |
| ScyllaDB unavailable | feed writes fail | at-least-once: offset not committed → retry → DLQ; pushes best-effort | check Scylla; drain DLQ |
| Slow stream client | `RecvError::Lagged` | `tokio::broadcast` drops old; stream ends with `Status::DataLoss` | client reconnects + re-polls `ListNotifications` |
| CollapseFlushWorker crash | window not flushed | Redis TTL (window + 10 s grace) expires the key; schedule member stays so next startup re-drains (no-op if empty) | restart worker; at worst one window lost |

**Backpressure & limits.** `NOTIFICATION_MAX_PAGE_SIZE` caps feed pages; `NOTIFICATION_STREAM_BUFFER_SIZE`
bounds per-profile broadcast; the hourly cap and Redis collapse window bound celebrity write volume.

---

## 📦 Integration & Usage

```toml
[dependencies]
notification = { path = "crates/services/notification" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`notification::service::NotificationService` — `build` wires the repository, cache, broadcast registry,
CQRS buses, and the Kafka workers; `register` adds the gRPC + reflection services; `health_probes`
checks Scylla/Redis.

### Bootstrap (`crates/apps/notification-server`)

```rust
use std::net::SocketAddr;
use notification::service::NotificationService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("NOTIFICATION_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50055".to_owned())
        .parse()?;
    service_runtime::serve::<NotificationService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### `notification`-specific variables (key subset)

| Variable | Default | Description |
|---|---|---|
| `NOTIFICATION_HOT_SUBJECT_THRESHOLD` | `100` | Reactions / 5-min window to activate L2 Redis cross-batch collapse. |
| `NOTIFICATION_COLLAPSE_WINDOW_SECS` | `30` | Redis collapse window TTL. |
| `NOTIFICATION_COLLAPSE_FLUSH_INTERVAL_SECS` | `30` | CollapseFlushWorker poll cadence. |
| `NOTIFICATION_MAX_PER_SUBJECT_PER_HOUR` | `3` | Hourly cap per `(target, subject, kind)`. |
| `NOTIFICATION_UNREAD_CAP` | `99` | Max unread badge value (shows "99+"). |
| `NOTIFICATION_DEDUPE_TTL_SECS` | `86400` | Idempotency claim TTL — must exceed worst-case redelivery window. |
| `NOTIFICATION_MAX_PAGE_SIZE` | `50` | Feed page cap. |
| `NOTIFICATION_STREAM_BUFFER_SIZE` | `256` | `tokio::broadcast` capacity per streaming profile. |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_HOSTS` | **Yes** | — | ScyllaDB contact points. |
| `SCYLLA_KEYSPACE` | No | `notification` | Keyspace. |
| `REDIS_URL` | **Yes** | — | Redis connection URL. |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers. |
| `NOTIFICATION_GRPC_ADDR` | No | `0.0.0.0:50055` | gRPC bind address. |

> Full `SCYLLA_*` / `REDIS_*` / `KAFKA_*` tuning lives in the shared storage/transport crates.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `001_keyspace.cql` → `002_notifications_by_profile.cql` →
  `003_notification_unread_counters.cql` against `notification`, applied **before** first boot.
- **Kafka:** topics pre-created — `engagement.reactions` (key `{post}:{profile}`),
  `comment.created`/`comment.deleted` (key `comment_id`), `post.published` (key `post_id`).
- **Rollout/Rollback:** `<TODO>`; workers are at-least-once consumers, gRPC tier stateless — safe to roll.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread. Scylla 5.x+ RF=3; Redis 7.x+.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `notification_suppressed_total{reason="write_error"}` | feed durability | rate > 0.01 ⇒ critical |
| `notification_collapse_window_count` (vs `_written_total`) | flush worker health | flush == 0 while writes > 100 ⇒ warning |
| `kafka_consumer_group_lag{group=~"notification-.*"}` | ingest freshness | > 10 000 ⇒ warning |
| `notification_stream_lagged_total` | slow-client churn | spike ⇒ investigate buffer/clients |
| `notification_unread_cache_miss_total` | Redis L1 health | sustained ⇒ check Redis |

---

## 🛠️ Local Development

```bash
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/notification/migrations/*.cql; do cqlsh -f "$f"; done
cargo build -p notification && cargo clippy -p notification -- -D warnings
cargo test  -p notification
# Smoke: grpcurl -plaintext -d '{"profile_id":"018f..."}' 127.0.0.1:50055 notification.v1.NotificationService/ListNotifications
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. `NTF-6001`: reaction notifications silently dropped for a post.**
Root cause: `ReactionNotificationWorker` reads `notification:pa:{post_id}` (populated by
`MentionNotificationWorker` on `post.published`) before writing; the key is absent if the mention worker
lags or the post predates deployment. Mitigation: check `notification-mention-consumer` lag; replay with
`auto.offset.reset=earliest`; for immediate recovery `SET notification:pa:{post_id} {author} EX 604800`.

**2. Unread badge out of sync after Mark-All-Read.**
Root cause: Redis evicted (no persistence) or `MarkAllRead` reset Redis but failed before the Scylla
counter row. Mitigation: read the durable counter
(`SELECT unread_count FROM notification.notification_unread_counters WHERE target_profile_id = <uuid>`),
then `DEL notification:unread:{profile_id}` — the next `GetUnreadCount` repopulates L1 from Scylla.

**3. CollapseFlushWorker not flushing celebrity windows.**
Root cause: the Tokio task panicked, or `zrangebyscore` is failing on a Redis connection issue.
Mitigation: check logs for the worker panic; `redis-cli ping`; inspect
`ZRANGEBYSCORE notification:window_schedule -inf +inf WITHSCORES LIMIT 0 10`. Windows self-expire
(`collapse_window_secs + 10`), so no double-writes occur if you wait it out.

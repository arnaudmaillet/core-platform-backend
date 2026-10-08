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
> | **Async** | publishes nothing · consumes `wallet.v1.events` / `comment.created` / `post.published` / `social-graph.followed` / `social-graph.follow_requested` / `moderation.v1.events` (appeal outcomes) |
> | **Upstream callers** | `<TODO: mobile / BFF (stream + feed reads)>` |
> | **Downstream deps** | ScyllaDB, Redis, Kafka; APNs + `profile` (push, #654) |
> | **SLO** | unread-count read sub-ms (Redis) · feed read O(1) paginated · push best-effort |

---

## 🎯 Overview & Service Role

`notification` closes the user feedback loop. It ingests semantic business events from Kafka
(`wallet.v1.events`, `comment.created`, `post.published`), persists durable per-profile activity
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
Kafka: wallet.v1.events │ comment.created │ post.published
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
| APNs (#654) | push delivery | pushes not delivered | **Soft** — the feed and badge are unaffected; a push is never retried |
| `profile` (mesh, #654) | the sender's name in a push | alerts name no one (`…_ANON`) | **Soft** |

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

**Push devices and preferences (#654).** Every RPC but `ResolvePushTargets` is edge `authenticated`
and bound to `profile_id`.
- `RegisterDevice(device_id, token, platform, environment, timezone)` — call it on every launch. A token
  registered for another **account** leaves that account's profiles (a handed-over phone never gets the
  old account's pushes); a device's new token replaces its old one. Tables `push_devices` (by profile)
  and `push_device_tokens` (by token). **Bound to the device (#725):** `device_id` is the id the session
  sent at login (`DeviceContext.device_id`, the token's `did`): on the edge another id is
  `PERMISSION_DENIED`, and a client may take a token held by another account only when its session's
  `did` is the device that registered it — any other session, **including one bound to no device**, is
  refused (`NTF-2004`, `ALREADY_EXISTS`). The holder decides, not the caller, so skipping `did` at login
  skips nothing: a token only moves between accounts on its own phone, and a leaked token cannot be
  pulled into someone else's account. The mesh, and holders with no device recorded, keep the old rule.
- Preferences (`notification_preferences`, one JSON document per profile): push and email per category
  (likes, comments, mentions, new followers, follow requests, messages, posts from followed accounts,
  places nearby, wallet), a pause (≤ 8 h), quiet hours read in the holder's IANA zone. Defaults: every
  push on, every email off; **13–17: quiet hours 22:00–07:00** (from the token's `age` on reads, and
  written at a teen's first `RegisterDevice` so the sender applies them). Stored as the teen default,
  they lift once the token says `18+`: at the next edge `RegisterDevice` (the app refreshes its device at
  launch) or preferences call. Quiet hours the holder set themselves stay; an unknown age (the mesh, no
  date of birth) changes nothing. Marketing email is the
  account's `marketing` consent (`account.v1.UpdateConsents`), not a second toggle here.
- `ResolvePushTargets(profile_id, category)` (mesh): the devices, or `allowed = false` when the category
  is off, a pause runs or it is quiet hours — the rule the push sender below applies, for a sender
  outside the service.

**Push delivery (#654).** Every notification written **for the first time** (the unread counter's
idempotency claim: a redelivered event pushes nothing) goes to the recipient's iOS devices over APNs,
unless their preferences hold its category (category off, pause, quiet hours). Fire-and-forget: the
push never delays nor fails the write, and is not retried. Off until the APNs key is configured.
- Categories: reaction → likes, comment / reply → comments, mention → mentions, follow → new
  followers, follow request / accepted → follow requests. **Appeal outcomes are never pushed** (feed
  only, until their recipient is decided).
- **Chat messages** (`chat.message.push`, consumer group `notification-chat-push`, only when push is
  on): chat names the recipients (its inbox rules and per-conversation mutes); each gets it under
  their `messages` preference, pause and quiet hours. Sent **at most once** per message (a Redis claim
  on its id, kept `NOTIFICATION_DEDUPE_TTL_SECS`); a message older than a day is not pushed (a
  backlog). Alert: `title` = the sender's name (else `title-loc-key` `NTF_PUSH_MESSAGE_ANON`), `body`
  = the text (an attachment: `loc-key` `NTF_PUSH_MESSAGE_MEDIA`); no badge (the activity feed is
  untouched); `kind = message`, `subject_kind = conversation`, `subject_id` = the conversation,
  `notification_id` = the message.
- Alert: localized by the app (`loc-key` + `loc-args`): `NTF_PUSH_<KIND>` (args: the sender's name),
  `NTF_PUSH_<KIND>_OTHERS` (args: the name, how many others — a collapsed notification),
  `NTF_PUSH_<KIND>_ANON` (no args: the name is unknown). The name is the sender's display name (else
  handle), read from `profile`'s `GetProfileById` on the mesh and cached 5 min. `badge` = the unread
  count; `thread-id` = the subject; `apns-collapse-id` = the notification id; custom keys
  `notification_id`, `kind`, `subject_kind`, `subject_id` (the app opens the subject from them).
- APNs: HTTP/2 provider API, token auth (ES256 JWT from the team's `.p8` key, re-signed every
  50 min), `apns-expiration` one day. A token APNs calls unregistered (410) or malformed
  (`BadDeviceToken`) is forgotten; any other refusal (e.g. `DeviceTokenNotForTopic`, a configuration
  fault) only logs (`NTF-3002`), so a misconfiguration never wipes the registrations. Android devices
  are skipped until an FCM sender exists.
- **`environment` must match the build:** APNs answers `BadDeviceToken` to a sandbox token sent to
  production (and the reverse), so a device registered with the wrong `environment` is forgotten at
  its first push — "push stopped working" on one device. The `push token gone` log line carries the
  environment; development builds register `SANDBOX`, TestFlight and the App Store `PRODUCTION`.

### Rust ports (hexagonal contract)

```rust
pub trait NotificationRepository: Send + Sync + 'static { /* insert, list_paginated, mark_read, *_counter */ }
pub trait UnreadCounter:          Send + Sync + 'static { /* incr/decr/reset/get + read_horizon (Redis L1 + Scylla L2) */ }
pub trait BlockCache:             Send + Sync + 'static { /* is_blocked(sender, target) — social-graph gate */ }
pub trait StreamRegistry:         Send + Sync + 'static { /* subscribe/broadcast (broadcast::Receiver per profile) */ }
pub trait DeviceRegistry:         Send + Sync + 'static { /* register/unregister/devices — push devices per profile */ }
pub trait PreferenceStore:        Send + Sync + 'static { /* get/put — notification preferences per profile */ }
pub trait PushSender:             Send + Sync + 'static { /* send(device, message) → Delivered | TokenGone (APNs) */ }
pub trait SenderNames:            Send + Sync + 'static { /* display_name(profile) — fail-open (profile, mesh) */ }
pub trait PushNotifier:           Send + Sync + 'static { /* notify(notification) — fire-and-forget hook of every write */ }
```

### Error contract (`NTF-xxxx`)

`NTF-1xxx` lifecycle … `NTF-3002` push not delivered (logged only) … `NTF-6001` author-cache miss (reaction notification dropped) … `NTF-9xxx`
identifiers — via the shared `error` crate.

---

## 📨 Events & Async Contract

**Publishes:** none — `notification` is a pure consumer/sink.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `wallet.v1.events` | `notification-stake-consumer` | like notifications (#665: a like is a point): a liker's **first** batch of likes on a post or comment (`stake_committed` with `first`), the author from the event; later batches and other wallet events skip (collapsed) | DLQ `{topic}.dlq` |
| `social-graph.followed` + `social-graph.follow_requested` | `notification-follow-consumer` | follows → `FOLLOW` to the followee; a request to a private profile → `FOLLOW_REQUEST` to its owner (the app opens the requests inbox); an approved request (`via_request`) → `FOLLOW_ACCEPTED` to the requester, not the owner again (#755). A withdrawn request (`withdrawn_at`: cancelled, declined, cut by a block) **retracts** the owner's `FOLLOW_REQUEST`: deleted, and taken off the badge when still unread and newer than a mark-all-read (a replay does nothing). Subject: the other profile (`SUBJECT_KIND_PROFILE`). Block-gated, self-guarded, one notification per event (deterministic id) | DLQ `{topic}.dlq` |
| `moderation.v1.events` (`appeal_resolved` only) | `notification-appeal-consumer` | an appeal's outcome → `APPEAL_UPHELD` / `APPEAL_OVERTURNED` to **every profile** moderation names (`profile_ids`: the appellant account's profiles, #744). Subject: the appeal (`SUBJECT_KIND_APPEAL`, the app opens it via `ListMyAppeals`). A platform notice: no sender (nil `sender_profile_id`, `sender_count` 0), not block-gated; one notification per (appeal, profile) (deterministic id). Other moderation events are ignored | DLQ `{topic}.dlq` |
| `account.v1.events` (`supervision_started` / `supervision_ended` only) | `notification-supervision-consumer` | family supervision (#670) → `SUPERVISION_STARTED` to both sides; `SUPERVISION_ENDED` to the other side when one ends it (both when an account is erased); `SUPERVISION_CAME_OF_AGE` to both at 18. To every profile account names on the event; sender = the other side's first profile (else the platform), subject = the other side's account (`SUBJECT_KIND_ACCOUNT`: the app opens Settings → Supervision). Not block-gated (a teen is always told); feed only (no push category); one notification per (event, profile) (deterministic id). Other account events are ignored | DLQ `{topic}.dlq` |
| `comment.created` | `notification-comment-consumer` | comment notifications (block-gated, self-guarded; none for a `quiet` event: a restricted author, #659) | DLQ `{topic}.dlq` |
| `post.published` | `notification-mention-consumer` | parse `@mentions`, cache post author | DLQ `{topic}.dlq` |
| `chat.message.push` | `notification-chat-push` | a chat message's push to the recipients chat names (#654), once per message (Redis claim), under each one's `messages` preference; nothing written to the feed. Only when push is on; `latest` reset (a new group never pushes the past) | DLQ `{topic}.dlq` |

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
| APNs unreachable / key refused | pushes not delivered | `NTF-3002` logged per device; feed + badge unaffected; a refused provider token is re-signed | check the APNs key, team id, topic; egress to `api.push.apple.com:443` |
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
| `NOTIFICATION_APNS_KEY_FILE` | — | Path of the APNs `.p8` key (a mounted secret). With the next three, turns push on (#654); any unset: push off. |
| `NOTIFICATION_APNS_KEY_ID` | — | The key's id (JWT `kid`). |
| `NOTIFICATION_APNS_TEAM_ID` | — | The Apple team id (JWT `iss`). |
| `NOTIFICATION_APNS_TOPIC` | — | The app's bundle id (`apns-topic`). |
| `NOTIFICATION_PROFILE_GRPC_ENDPOINT` | — | profile's mesh endpoint, for the sender's name in a push. Unset: alerts name no one. |
| `NOTIFICATION_PROFILE_RPC_TIMEOUT_MS` / `NOTIFICATION_PROFILE_CONNECT_TIMEOUT_MS` | `300` / `1000` | Deadlines of that call. |

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
- **Kafka:** topics pre-created — `wallet.v1.events` (key `{target_kind}:{target_id}`),
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

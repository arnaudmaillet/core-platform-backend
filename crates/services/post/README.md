# `post` — The canonical source of truth for user-created content

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — the content publish path; feeds and discovery derive from its events |
> | **Deployable** | `crates/apps/post-server` (library crate: `crates/services/post`) |
> | **Datastores** | ScyllaDB keyspace `post` (2 tables) |
> | **Async** | publishes `post.v1.events` (unified) + `post.published` / `post.updated` / `post.deleted` (legacy) · consumes `profile.v1.events` (author-tier denormalization) + `moderation.v1.events` (read restriction) |
> | **Upstream callers** | `<TODO: gateway>` |
> | **Downstream deps** | ScyllaDB, Kafka |
> | **SLO** | `<TODO>` avail · `GetPost` p99 `<TODO>` · publish p99 `<TODO>` |

---

## 🎯 Overview & Service Role

`post` is the canonical registry for user-created posts across multiple media formats (Carousel,
MainVideo, TextOnly). It enforces content invariants, manages a `Draft → Published → Deleted`
lifecycle, and emits a Kafka event on every state transition. It is the **fan-out trigger** for the
rest of the platform — timeline, geo-discovery, and notification all build their projections from
`post.*` events.

The hard problem it solves is **being a clean event source**: every published/updated/deleted post must
produce exactly one durable, correctly-keyed event that downstream materializers can trust, while the
write path stays O(1). It resolves this with a two-table wide-column schema (point store + creator
index) and a publish step gated on a successful durable write. It has **no knowledge** of feeds,
timelines, or social graphs.

**Core objectives:** content invariants are non-negotiable (carousel cardinality, video caps, MIME
allowlist); the lifecycle is forward-only (`Draft→Published` irreversible, soft-delete only); every
transition emits its event.

---

## 📐 Architecture & Concepts

Hexagonal / DDD, CQRS buses, ScyllaDB durable store, Kafka events.

```
gRPC PostService ─► CQRS bus ─► Create/Publish/Update/Delete handlers ─► ScyllaPostRepository (dual-write)
                            └─► Get/ListByProfile handlers
                                            │
                  KafkaEventPublisher ◄─────┘  ─► post.published / post.updated / post.deleted
```

**Storage design — two-table wide-column schema:**
- `post.posts` — canonical store, PK `post_id`, O(1) point lookups.
- `post.posts_by_profile` — creator-feed index, PK `profile_id`, CK `created_at DESC, post_id ASC`.

Every write **dual-writes both tables sequentially**. Attachments are stored as validated JSON (a
`text` column) to avoid ScyllaDB UDT migration complexity.

> **Invariants** (and where enforced, in the `Post` aggregate FSM): Carousel 2–10 items, carousel
> videos ≤ 15 s, video items require `thumbnail_url`; MainVideo = single video + thumbnail; TextOnly =
> zero attachments; threading `parent_id`/`root_id` both-present-or-both-absent; `profile_id` on
> Publish/Update/Delete must match the author.

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-`UNAVAILABLE`) | `<TODO>` | 30d | gRPC status metrics |
| `GetPost` p99 (point read) | `< <TODO> ms` | 1h | Scylla read histogram |
| `PublishPost` p99 (durable + event) | `< <TODO> ms` | 1h | handler histogram |
| Event emission completeness | 1 event per committed transition | — | publish success rate |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`post`) | durable store | reads + writes fail | **Hard** — `UNAVAILABLE` |
| Kafka | event emission | downstream projections stall | **Soft** — writes commit; see note |

**Upstream (blast radius — `post.*` events feed most of the read fleet):**

| Caller | Uses | User-visible impact if `post` is down |
|---|---|---|
| `timeline` | `post.published` / `post.deleted` | no new posts enter home feeds |
| `geo-discovery` | `post.published` | new posts don't appear on the map |
| `notification` | `post.published` (mentions) | mention notifications stop |

> **Critical path?** **Yes** for publishing; the write path is user-facing and the event is the
> upstream trigger for the entire read-side fleet.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `post.v1.PostService`

```protobuf
service PostService {
  rpc CreatePost (CreatePostRequest) returns (CreatePostResponse);          // draft; PostId pre-generated at boundary
  rpc PublishPost (PublishPostRequest) returns (CommandResponse);           // Draft→Published; emits post.published
  rpc UpdatePost (UpdatePostRequest) returns (CommandResponse);             // emits post.updated
  rpc DeletePost (DeletePostRequest) returns (CommandResponse);             // soft-delete; emits post.deleted
  rpc RestorePost (RestorePostRequest) returns (CommandResponse);           // #663 within 30 days: published again (re-emits post.published at its original time) or a draft
  rpc ListRecentlyDeleted (ListRecentlyDeletedRequest) returns (ListRecentlyDeletedResponse); // #663 the author's restorable posts, newest deletion first
  rpc GetPost (GetPostRequest) returns (PostView);                          // point lookup; viewer-aware
  rpc ListPostsByProfile (ListPostsByProfileRequest) returns (ListPostsByProfileResponse); // cursor-paginated; viewer-aware
}
// CreatePostRequest / PostView carry an optional GeoPoint location:
message GeoPoint { double lat = 1; double lng = 2; }  // WGS-84; absent → post is not geo-indexed
```

**Reuse permissions (#669).** A post may set its own `allow_remix` / `allow_sound_reuse`; unset, it
follows its author's profile default (projected from `ProfileInteractionSettingsChanged` into
`post.author_reuse_settings`; teens default to off). An original sound belongs to the post that made it
(`post.audio_origins`, first writer wins). `CreatePost` with someone else's sound — whatever the request
calls it — is refused (`PST-1008`, 403) unless that post, or failing an override its author, allows reuse;
one's own sounds and unknown ones (library tracks) are free. Remix has no server surface yet: the flag is
stored for the client.

**Recently deleted (#663).** A delete is a tombstone: the post is indexed in `post.deleted_by_profile`
(rows expire after 30 days) and `ListRecentlyDeleted` shows the author their restorable posts.
`RestorePost` brings one back within 30 days as it was — published (re-announced on `post.published` at its
original publication time, so feeds and search take it back; a post moderation removed or limited is
restored to its author only, never re-announced, so a takedown survives delete → restore) or a draft — and
removes it from the list. geo-discovery suppresses a deleted post for good: a restored post's map pin does
not come back. Both are edge `authenticated`, bound to `profile_id`.

**Viewer-aware reads.** The reader comes from the transport (`edge::viewer`), never from a request
field. A draft, a deleted post, or a post moderation **removed** is visible to its author (any
profile in the token's `pids`) and to mesh callers only; anyone else gets `PST-1001` from `GetPost`
and does not see it in `ListPostsByProfile` (filtered per page, so a page can come back short
while `next_token` stays valid). `PostView.moderation` / `PostSummary.moderation` tell the author
what is in force; `LIMITED` posts stay readable (discovery applies it). An **`AGE_GATED`** post is
not found for a reader not cleared for mature content — an anonymous client, a guest, or a 13–17 holder
(the token's `age`) — and left out of their lists; its author and adults read it.
Then the **author's audience**, from social-graph's mesh-only `CheckAccess`: a private author the
reader does not follow, a block either way, or a hidden author → `PST-1001` / an empty list. The
author and mesh callers skip the check; anyone else (anonymous included) depends on it, and an
outage fails closed with `PST-5001` (`UNAVAILABLE`), never by serving the post.

**Post history window (#664).** An author may show visitors only their recent posts (6 months, 1 month,
3 days). For any client but the author, `ListPostsByProfile` stops at the first older post (the list is
newest first; no next token) and `GetPost` answers `PST-1001` for one; the author and mesh callers see every
post. Nothing is deleted. The window comes from profile's `ProfileTabSettingsChanged`, projected into
`post.author_post_windows` by the author-settings consumer below.

**Location sharing (#657). `PostView.location` is what the author shares with the reader: the
post's own point for the author; for anyone else (the mesh included) the point by default, the
centre of its H3 R5 cell (~87 km², the map's city band) at city level, and nothing in ghost mode.
The setting comes from profile's `ProfileLocationSettingsChanged`, projected into
`post.author_location_settings`, and applies to posts made before it changed. A store error fails
the read rather than show the point.

### Error contract (`PST-xxxx`)

| Code | Variant | HTTP |
|---|---|---|
| PST-1001 | `PostNotFound` | 404 |
| PST-1002/1003 | `PostAlreadyPublished` / `PostAlreadyDeleted` | 409 |
| PST-1004 | `NotDraft` | 422 |
| PST-1005 | `AuthorMismatch` | 403 |
| PST-1006 | `PostNotDeleted` (restore of a post that is not deleted) | 409 |
| PST-1007 | `RestoreWindowExpired` (deleted more than 30 days ago) | 410 |
| PST-1008 | `SoundReuseNotAllowed` (the sound's creator does not allow reuse) | 403 |
| PST-2001..2003 | carousel cardinality / video length | 422 |
| PST-3001..3004 | thumbnail / MIME / CDN URL / dimensions | 422 |
| PST-9001/9002 | invalid post/profile ID | 422 |
| PST-9003 | `AttachmentsCorrupted` (JSON deser) | 500 |
| PST-9004 | `DomainViolation` | 422 |
| PST-5001 | `AccessCheckUnavailable` (social-graph `CheckAccess` did not answer; retryable) | 503 → `UNAVAILABLE` |

---

## 📨 Events & Async Contract

> Kafka topics are an API. Downstream materializers (timeline, geo-discovery, notification) trust the
> `author_tier` and coordinates carried here — schema changes break them like a proto change.

**Publishes:**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `post.v1.events` | every lifecycle event (`PostPublished` / `PostUpdated` / `PostDeleted`) | `post_id` | `search` (post indexing) |
| `post.published` | `PublishPost` success — carries denormalized `author_tier`, plus `caption` / `thumbnail_url` / optional `lat`/`lng` for the geo projection | `post_id` | `timeline`, `geo-discovery`, `notification` |
| `post.updated` | `UpdatePost` success | `post_id` | `<TODO>` |
| `post.deleted` | `DeletePost` success | `post_id` | `timeline`, `geo-discovery` |

> **Two emission styles, by design.** `post.v1.events` is the unified, versioned stream (the fleet convention, like `moderation.v1.events` / `profile.v1.events`): the whole internally-tagged `DomainEvent`, keyed by `post_id`. The legacy per-type topics (`post.published` / `.updated` / `.deleted`, bare payloads) are retained for their existing consumers (`timeline` / `geo-discovery` / `notification`); every event is published to **both**. Migrating those consumers onto `post.v1.events` and retiring the legacy topics is a future cleanup.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `profile.v1.events` | `post-author-tier` | denormalize `ProfileTierChanged` into the `author_tiers` projection (`profile_id → tier`); read on the publish path to stamp `author_tier` onto published posts. Other event types commit as no-ops | DLQ `profile.v1.events.dlq` |
| `profile.v1.events` | `post-author-location` | project `ProfileLocationSettingsChanged` into `author_location_settings` (`profile_id → ghost, city`), which `GetPost` applies to the location it shows anyone but the author, and `ProfileTabSettingsChanged` into `author_post_windows` (`profile_id → window_days`), which both reads apply. Starts from the earliest offset (a teen profile is created ghosted). Other event types commit as no-ops | DLQ `profile.v1.events.dlq` |
| `moderation.v1.events` | `post-moderation` | record `enforcement_applied` / `enforcement_reversed` on a **post** as its moderation restriction (`remove_content` → Removed, `visibility_limit` → Limited, `age_gate` → AgeGated; reversal → None), version-guarded by moderation's per-subject `EnforcementVersion` so redelivery converges. Other entities, actor-level actions and other event types commit as no-ops | DLQ `moderation.v1.events.dlq` |

> **Runtime contract:** the event is published after the durable dual-write. Downstream consumers own
> at-least-once handling under `run_consumer`; all of them treat `post.*` as idempotent by `post_id`.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB unavailable | all RPCs fail | **Hard** — `UNAVAILABLE`; nothing acked | check Scylla cluster |
| Partial dual-write (posts ok, index fails) | post readable by id, missing from creator feed | write returns error; client retries (idempotent by `post_id`) | retry; reconcile index if needed |
| Kafka publish fails after commit | post durable, downstream projections miss it | **Soft** — content exists but feeds/map/notifications lag | re-emit event or rely on downstream backfill |
| `AttachmentsCorrupted` on read | `PST-9003` | bad JSON in `text` column | inspect row; data-quality incident |

**Backpressure & limits.** `ListPostsByProfile` is cursor-paginated. Inserts are idempotent on
`post_id` (last-write-wins), so transient retries are safe.

---

## 📦 Integration & Usage

```toml
[dependencies]
post = { path = "crates/services/post" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`post::service::PostService` — `build` wires the ScyllaDB repository and the durable Kafka event
publisher; `register` adds the gRPC + reflection services; `health_probes` checks Scylla.

### Bootstrap (`crates/apps/post-server`)

```rust
use std::net::SocketAddr;
use post::service::PostService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("POST_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50056".to_owned())
        .parse()?;
    service_runtime::serve::<PostService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB contact points + DC for token-aware routing. |
| `SCYLLA_KEYSPACE` | No | `post` | Keyspace (NTS RF=3, LZ4). |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers for `post.*`. |
| `POST_GRPC_ADDR` | No | `0.0.0.0:50056` | gRPC bind address. |
| `POST_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Yes** (prod) | `http://localhost:50053` | social-graph mesh endpoint for the audience check (`CheckAccess`). Reads by anyone but the author fail closed (`PST-5001`, `UNAVAILABLE`) when it does not answer. |
| `POST_SOCIAL_GRAPH_RPC_TIMEOUT_MS` / `_CONNECT_TIMEOUT_MS` | No | `1000` / `1000` | Deadlines for that call. |

> Full `SCYLLA_*` / `KAFKA_*` tuning lives in the shared storage/transport crates.

### Compile-time features
- `build.rs` compiles `proto/post/v1/*.proto` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `migrations/0001_create_keyspace.cql` → `0002_create_posts_table.cql` →
  `0003_create_posts_by_profile_table.cql` → `0004`–`0007` (audio, author tiers, geo, moderation
  columns; online `ALTER`s) against `post`, applied **before** first start.
- **Rollout/Rollback:** `<TODO>`; stateless service, safe to roll.
- **Schema gotcha:** the creator-index clustering order (`created_at DESC, post_id ASC`) is a read
  contract — don't change it after data exists.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread. Global tracing/OTel subscriber installed before `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `PublishPost` p99 | publish-path latency | > SLO ⇒ page |
| `post.*` publish failure rate | downstream feed/map drift | sustained ⇒ check Kafka |
| Scylla write errors | content durability | any spike ⇒ check cluster |
| `PST-9003 AttachmentsCorrupted` count | data-quality | > 0 ⇒ investigate |

---

## 🛠️ Local Development

```bash
cargo build -p post && cargo clippy -p post --all-targets
cargo test  -p post
docker compose up -d scylla kafka             # repo-root compose
for f in crates/services/post/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. `PST-1004 NotDraft` on `PublishPost`.**
Root cause: the post is already `Published` or `Deleted` — the lifecycle is forward-only. Mitigation:
`GetPost` to confirm status; publishing is irreversible and single-shot by design.

**2. A published post is missing from the creator feed but readable by id.**
Root cause: the dual-write partially failed (`posts` ok, `posts_by_profile` not). Mitigation: re-issue
the write (idempotent on `post_id`); if it persists, reconcile the index from `post.posts`.

**3. A new post never reaches timelines/map.**
Root cause: the post committed but the `post.published` event failed to publish, or a downstream
consumer is lagging. Mitigation: check Kafka health and the downstream consumer groups; re-emit the
event if it was dropped post-commit.

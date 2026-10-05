# `social-graph` — Directional follow/block edges between opaque profiles, block-gated and celebrity-safe

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — feeds, notifications, and block-gating depend on it |
> | **Deployable** | `crates/apps/social-graph-server` (library crate: `crates/services/social-graph`) |
> | **Datastores** | ScyllaDB keyspace `social_graph` (5 tables) · Redis (sets + counters) |
> | **Async** | publishes `social-graph.followed` / `.follow_requested` / `.unfollowed` / `.blocked` / `.author_tier_changed` · consumes `profile.v1.events` (audience projection) |
> | **Upstream callers** | `timeline`, `notification`, `<TODO: gateway>` |
> | **Downstream deps** | ScyllaDB, Redis, Kafka |
> | **SLO** | `<TODO>` avail · `GetRelationStatus` p99 `<TODO>` · write p99 `<TODO>` |

---

## 🎯 Overview & Service Role

`social-graph` is the strict owner of **who follows whom** and **who blocks whom**, over opaque
`ProfileId` (UUIDv7) primitives. It enforces the block-gate, derives mutual-follow (friendship), and
emits the follow/block events that drive timeline fan-out and notifications.

The hard problem it solves is the **celebrity fan-in asymmetry**: outbound follows are bounded
(tens of thousands) but inbound follows are unbounded (millions for a celebrity). Materializing the
full inbound set would exhaust Redis. It resolves this by storing **outbound follows as Redis Sets**
(for O(1) mutual-follow derivation) but **inbound followers as O(1) INCR/DECR counters**.

**Core objectives:** never import `profile` or `account` (profiles are opaque IDs); block always wins
(severs follows both directions, gates future follows); friendship is *derived*, never dual-written.
**Out of scope:** profile metadata, timeline construction, notification delivery.

---

## 📐 Architecture & Concepts

Hexagonal / DDD, CQRS buses, ScyllaDB adjacency tables, Redis sets + counters, Kafka events.

```
gRPC SocialGraphService ─► CQRS bus ─► Command handlers ─► SocialGraphRepository (ScyllaDB, 4 tables)
                                    └─► Query handlers   ─► SocialGraphCache (Redis sets + counters)
                                    └─► EventPublisher   ─► Kafka (social-graph.*)
```

**ScyllaDB schema** (keyspace `social_graph`, NTS RF=3):

| Table | Partition key | Clustering key | Purpose |
|---|---|---|---|
| `followers` | `followee_id` | `followed_at DESC, follower_id ASC` | fan-in: who follows X |
| `following` | `follower_id` | `followed_at DESC, followee_id ASC` | fan-out: who X follows |
| `follow_status` | `follower_id` | `followee_id ASC` | point-lookup + `followed_at` for DELETE |
| `blocks` | `blocker_id` | `blockee_id ASC` | block point-lookup + list |

`follow_status` exists because Scylla DELETE needs the **full clustering key**: it stores `followed_at`
as a regular column so unfollow/sever never read-before-write the adjacency lists. No `blocked_by`
mirror is needed — the gate is two O(1) lookups on the same `blocks` table with swapped args.

**Redis strategy:** `sg:following:v1:{id}` (Set) drives `IsFriend(A,B)` = `SISMEMBER(A,B) AND
SISMEMBER(B,A)` — no `friends` table, so no dual-write desync. `sg:followers_count:v1:{id}` /
`sg:following_count:v1:{id}` (counters) satisfy count reads in O(1) space.

> **Invariants** (and where enforced): no self-follow/self-block (handler pre-check); follow rejected
> if any block exists either direction (`Relation::follow()`); re-follow/re-block rejected; block
> severs existing follows both directions (`Relation::block()` → `SeveredFollows`); unblock does **not**
> restore severed follows (intentional — user must re-follow).

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-`UNAVAILABLE`) | `<TODO>` | 30d | gRPC status metrics |
| `GetRelationStatus` p99 (Redis path) | `< <TODO> ms` | 1h | gRPC histogram |
| Follow/Block write p99 | `< <TODO> ms` | 1h | Scylla write histogram |
| Durability | no acked edge lost | — | Scylla `LocalQuorum` |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`social_graph`) | durable edges | reads + writes fail | **Hard** — `UNAVAILABLE` |
| Redis | sets + counters (status/friend/count reads) | status/count reads degrade | **Soft** — durable edges intact |
| Kafka | event emission | downstream fan-out stalls | **Soft** — edges still committed |

**Upstream (blast radius):**

| Caller | Uses | User-visible impact if down |
|---|---|---|
| `timeline` | consumes `social-graph.followed/unfollowed` + calls `ListFollowing` | new follows don't reach the home feed |
| `notification` | block-gate cache (`is_blocked`) | block suppression weakens |

> **Critical path?** Partially — writes are user-initiated (follow/block); much consumption is async.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `social_graph.v1.SocialGraphService`

```protobuf
service SocialGraphService {
  // Commands
  rpc Follow(FollowRequest) returns (CommandResponse);   // a private target gets a pending request (CommandResponse.requested)
  rpc ListFollowRequests(ListFollowRequestsRequest) returns (ListFollowRequestsResponse);   // private profile owner's inbox
  rpc ApproveFollowRequest(AnswerFollowRequestRequest) returns (CommandResponse);   // owner
  rpc DeclineFollowRequest(AnswerFollowRequestRequest) returns (CommandResponse);   // owner
  rpc CancelFollowRequest(CancelFollowRequestRequest) returns (CommandResponse);   // requester
  rpc Unfollow(UnfollowRequest) returns (CommandResponse);
  rpc RemoveFollower(RemoveFollowerRequest) returns (CommandResponse);   // owner: undoes the follower's follow
  rpc SetListPrivacy(SetListPrivacyRequest) returns (ListPrivacy);   // owner: who sees the follower / following lists
  rpc Block(BlockRequest) returns (CommandResponse);
  rpc Unblock(UnblockRequest) returns (CommandResponse);
  rpc Mute(MuteRequest) returns (CommandResponse);   // actor: posts / stories / messages
  rpc Unmute(UnmuteRequest) returns (CommandResponse);
  rpc Restrict(RestrictRequest) returns (CommandResponse);   // actor: its posts' comments by target seen by target + actor only
  rpc Unrestrict(UnrestrictRequest) returns (CommandResponse);
  // Queries
  rpc GetRelationStatus(GetRelationStatusRequest) returns (RelationStatusView);
  rpc ListFollowers(ListFollowersRequest) returns (ListFollowersResponse);
  rpc ListFollowing(ListFollowingRequest) returns (ListFollowingResponse);
  rpc GetListPrivacy(GetListPrivacyRequest) returns (ListPrivacy);   // owner
  rpc ListBlocks(ListBlocksRequest) returns (ListBlocksResponse);
  rpc ListMutes(ListMutesRequest) returns (ListMutesResponse);   // owner
  rpc ListRestricted(ListRestrictedRequest) returns (ListRestrictedResponse);   // owner
  rpc CheckAccess(CheckAccessRequest) returns (CheckAccessResponse);   // MESH-ONLY
  rpc CheckInteraction(CheckInteractionRequest) returns (CheckInteractionResponse);   // MESH-ONLY: may actor comment / mention / message target (its profile interaction settings, blocks); `held` when the target's temporary limit (#669) covers the actor — a comment / message from a non-follower, or a follower of less than 7 days under `recent_followers`
  rpc ListMutedProfiles(ListMutedProfilesRequest) returns (ListMutedProfilesResponse);   // MESH-ONLY: a reader's mutes for one scope (timeline)
  rpc ListRestrictedAmong(ListRestrictedAmongRequest) returns (ListRestrictedAmongResponse);   // MESH-ONLY: which commenters the post owner restricted (comment)
}
```

**Access check (`CheckAccess`, mesh-only).** The audience rule every viewer-aware read applies
(post, comment, these lists): given the reader's profiles (the token's `pids`; none when
anonymous) and up to 100 target profiles, each target is `VISIBLE`, `HEADER_ONLY` (a private
profile no reader profile follows) or `HIDDEN` (a block either way, or a profile hidden by
moderation / an account suspension / deletion). One's own profile is always visible. Four Scylla
queries per call whatever the sizes (`IN` over `follow_status`, `blocks` both ways and
`profile_audience`). Callers take the reader from their own edge request and fail closed when
this RPC is unavailable.

**Viewer-aware lists.** `ListFollowers` / `ListFollowing` of a profile that is not `VISIBLE` to the
reader come back empty with `hidden = true` (the owner and mesh callers always get them).

**List privacy (#659).** The owner picks who else sees each list (`SetListPrivacy`, both bound to
the owner's `profile_id` on the edge): `EVERYONE` (default), `FOLLOWERS`, `MUTUALS` (followers the
owner follows back) or `ONLY_ME`. A reader outside the audience gets an empty page with `hidden =
true`, on every device: the setting lives in `profile_audience.lists` (owned by social-graph,
unlike the other columns of that projection). A reader with several profiles is judged by its
best-placed one. **`RemoveFollower`** undoes a follower's follow exactly as its own `Unfollow`
would (counts, `social-graph.unfollowed`, timeline pruning); the follower is not notified and, on a
private profile, has to ask again.

**Mutes (#659).** `Mute` records, per scope (posts, stories, messages), what the actor stops seeing of the
target; re-muting replaces the scopes, `Unmute` lifts it, `ListMutes` pages the owner's mutes and
`RelationStatusView.muted` tells the actor how it mutes a target. The target is not told and nothing is
severed. Table `social_graph.mutes` (partitioned by muter). Enforced today for **posts**: timeline leaves a
muted author out of the reader's following and discovery feeds through the mesh-only `ListMutedProfiles`
(≤ 10 reader profiles, ≤ 5 000 mutes each). Stories and messages are stored for the client: there is no
stories surface and no message notification yet.

**Restrictions (#659).** `Restrict` makes the target's comments on the actor's posts visible only to the
target and the actor (comment's read gate asks the mesh-only `ListRestrictedAmong` with a page's
commenters, ≤ 100 per call); `Unrestrict` lifts it, `ListRestricted` pages the owner's list and
`RelationStatusView.restricted` tells the actor. The target is not told and nothing is severed. Table
`social_graph.restrictions` (partitioned by owner).

> **Wire contract:** `RelationStatus` (actor's perspective): `NONE`, `FOLLOWING`, `FOLLOWED_BY`,
> `MUTUAL` (implicit friendship), `BLOCKING`, `BLOCKED_BY`.

### Error contract (`SGR-xxxx`)

| Code | Variant | HTTP |
|---|---|---|
| SGR-1001/1002 | `AlreadyFollowing` / `NotFollowing` | 409 / 422 |
| SGR-1003/1004 | `AlreadyBlocked` / `NotBlocked` | 409 / 422 |
| SGR-2001/2002 | `SelfInteraction` / `BlockGateDenied` | 422 |
| SGR-9001/9002 | `DomainViolation` / `InvalidProfileId` | 422 |
| SDB-* / RDB-* / VAL-* | storage / cache / validation (delegated) | varies |

---

## 📨 Events & Async Contract

**Publishes:**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `social-graph.followed` | `Follow` success, or `ApproveFollowRequest` (`via_request: true`) | `{actor}:{target}` | `timeline` (fan-out), `notification` (new follower / request accepted) |
| `social-graph.follow_requested` | `Follow` of a private profile: a pending request (#755) | `{actor}:{target}` | `notification` (the owner is told). `{actor_id, target_id, requested_at}` |
| `social-graph.unfollowed` | `Unfollow` success | `{actor}:{target}` | `timeline` (pruning) |
| `social-graph.blocked` | `Block` success | `{actor}:{target}` | content filtering, notification suppression |
| `social-graph.author_tier_changed` | a follow/unfollow crosses a follower-count tier boundary | `{profile}` | `profile` (persists tier → re-emits on `profile.v1.events` for `post` to denormalize → `timeline`/`geo-discovery` fan-out routing). `{profile_id, new_tier, follower_count, changed_at_ms}` |

`ProfileUnblocked` is **not** published — no downstream fan-out needs it.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `profile.v1.events` | `social-graph-profile-audience` | project the audience facts into `profile_audience`: `ProfileVisibilityChanged` → `private`; `ProfileHidden` / `ProfileDeleted` → `hidden = true`; `ProfileRestored` → `hidden = false`. Column upserts (idempotent; the topic is keyed by `profile_id`, so a profile's facts arrive in order). Other types = no-op commit | DLQ `profile.v1.events.dlq` |

> **Projection start:** no row = public, not hidden. Profiles made private or hidden before this
> consumer first ran need a one-off replay of `profile.v1.events` (no live environment holds data today).

> **Runtime contract:** events are published via a durable Kafka producer after the edge commit.
> Downstream consumers own at-least-once handling under `run_consumer`.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB unavailable | follow/block + lists fail | **Hard** — `UNAVAILABLE` | check Scylla cluster |
| Redis unavailable | `GetRelationStatus`/counts degrade | **Soft** — derive from Scylla where possible | check Redis; counters resync on next write |
| Kafka unavailable | timeline/notification fan-out stalls | **Soft** — edges committed | check brokers; consumers replay |
| Counter drift after Redis loss | follower/following counts wrong | counters are derived, not source-of-truth | rebuild from `followers`/`following` tables |

**Backpressure & limits.** `ListFollowers/Following/Blocks` are cursor-paginated. Writes use the Scylla
**Strict** profile; status reads use **Fast**.

---

## 📦 Integration & Usage

```toml
[dependencies]
social-graph = { path = "crates/services/social-graph" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`social_graph::service::SocialGraphService` — `build` wires the ScyllaDB repository, Redis cache, and
durable Kafka publisher; `register` adds the gRPC + reflection services; `health_probes` checks
Scylla/Redis.

### Bootstrap (`crates/apps/social-graph-server`)

```rust
use std::net::SocketAddr;
use social_graph::service::SocialGraphService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("SOCIAL_GRAPH_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50053".to_owned())
        .parse()?;
    service_runtime::serve::<SocialGraphService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` / `SCYLLA_LOCAL_DC` | **Yes** | — | ScyllaDB contact points + DC for token-aware routing. |
| `SCYLLA_KEYSPACE` | No | `social_graph` | Keyspace (see migrations). |
| `REDIS_HOSTS` | **Yes** | — | Redis nodes for sets + counters. |
| `KAFKA_BROKERS` | **Yes** | — | Kafka brokers for `social-graph.*`. |
| `SOCIAL_GRAPH_GRPC_ADDR` | No | `0.0.0.0:50053` | gRPC bind address. |

> Full `SCYLLA_*` / `REDIS_*` / `KAFKA_*` tuning lives in the shared storage/transport crates.

### Compile-time features
- `build.rs` compiles `proto/social_graph/v1/*.proto` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `migrations/000{1..6}_*.cql` (keyspace + 5 tables) against `social_graph`, applied
  **before** first start.
- **Rollout/Rollback:** `<TODO>`; stateless service, safe to roll.
- **Counter rebuild:** Redis follower/following counters are derived — if Redis is lost, rebuild them
  by counting the `followers`/`following` adjacency tables (offline job).

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread. Global tracing/OTel subscriber installed before `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `GetRelationStatus` p99 | status read-path latency | > SLO ⇒ page |
| `social-graph.*` publish failures | downstream fan-out drift | sustained ⇒ check Kafka |
| `BlockGateDenied` rate | abuse / harassment signal | unusual spike ⇒ investigate |
| Scylla write errors | edge durability | any spike ⇒ check cluster |

---

## 🛠️ Local Development

```bash
cargo build -p social-graph && cargo clippy -p social-graph --all-targets
cargo test  -p social-graph
docker compose up -d scylla redis kafka       # repo-root compose
for f in crates/services/social-graph/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. `SGR-2002 BlockGateDenied` on a `Follow` between two seemingly unrelated profiles.**
Root cause: a block exists in *either* direction (`blocks(A,B)` or `blocks(B,A)`); the gate is
symmetric by design. Mitigation: check both `blocks` rows; if the block is intended, this is correct —
the follow must stay denied until an `Unblock`.

**2. Follower/following counts look wrong after a Redis incident.**
Root cause: the counters are O(1) Redis derivations, not the source of truth; a Redis flush loses them.
Mitigation: rebuild by counting `followers`/`following` for the affected profiles; counters self-heal
forward on the next follow/unfollow.

**3. A new follow never appears in the user's home feed.**
Root cause: the edge committed and `social-graph.followed` published, but `timeline`'s consumer is
lagging or dead-lettered the event. Mitigation: check timeline's `social-graph.followed` consumer lag
and DLQ; the edge itself is durable in Scylla.

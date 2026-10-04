# `comment` — 1-level threaded comment engine with GIF support

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — user-facing content; drives engagement comment counters |
> | **Deployable** | `crates/apps/comment-server` (library crate: `crates/services/comment`) |
> | **Datastores** | ScyllaDB keyspace `comment` (2 tables) |
> | **Async** | publishes `comment.created` / `comment.deleted` · consumes nothing |
> | **Upstream callers** | `<TODO: gateway>` |
> | **Downstream deps** | ScyllaDB, Kafka |
> | **SLO** | feed read p99 **< 5 ms** · at-least-once `comment.*` delivery |

---

## 🎯 Overview & Service Role

`comment` is the exclusive owner of comment lifecycle state. It enforces **strict 1-level threading**
(TikTok/Instagram style), supports rich GIF attachments, and emits Kafka events that drive the
`engagement` service's atomic comment counters in real time.

The hard problem it solves is **paginated thread reads with zero `ALLOW FILTERING`**: top-level
comments and replies must both be valid clustering-key prefix scans on the same partition. It resolves
this with a **nil-UUID sentinel** for top-level parents, so `WHERE post_id = ? AND parent_id = <nil>`
is a clean prefix scan.

**Core objectives:** sub-5 ms P99 paginated reads (Scylla local quorum, TWCS locality); zero
`ALLOW FILTERING`; at-least-once `comment.created`/`comment.deleted` delivery. **Out of scope:**
likes/reactions on comments (owned by `engagement`), post/profile data.

---

## 📐 Architecture & Concepts

Hexagonal / DDD, CQRS buses, ScyllaDB flat-tree store, Kafka events.

```
gRPC CommentService ─► CommandBus ─► CreateComment ─► Comment::create() ─► repo.insert ─► comment.created
                    │             └─► DeleteComment ─► has_active_replies? ─► Tombstone | Purge ─► comment.deleted
                    └─► QueryBus  ─► GetComment (comments, point read, LCS)
                                  ─► ListTopLevel / ListReplies (comments_by_post)
```

**ScyllaDB wide-column flat-tree:**

| Table | Partition key | Clustering keys | Purpose |
|---|---|---|---|
| `comment.comments` | `comment_id` | — | source-of-truth point reads & mutations (LCS) |
| `comment.comments_by_post` | `post_id` | `parent_id, created_at DESC, comment_id` | feed pagination, no ALLOW FILTERING (TWCS) |

**Nil-UUID sentinel:** top-level comments store `parent_id = 0000…0000` (lexicographically smallest),
making the top-level scan a valid clustering prefix; replies use their actual parent `comment_id`.

**Deletion strategy:** `has_active_replies` ? **Tombstone** (null body+gif, keep row so the thread stays
navigable) : **Purge** (physical DELETE from both tables). Both paths emit `comment.deleted`.

> **Invariants** (enforced at the aggregate boundary): text ≤ 500; must have text OR gif (`EmptyContent`);
> complete GIF metadata (`IncompleteGifMetadata`); 1-level max nesting (`NestingDepthExceeded`); cannot
> reply to a deleted parent (`ParentDeleted`); only author may delete (`AuthorMismatch`); no
> re-delete (`CommentAlreadyDeleted`).

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Feed read p99 (`ListTopLevel`/`ListReplies`) | **< 5 ms** | 1h | Scylla read histogram |
| `CreateComment` p99 | `< <TODO> ms` | 1h | gRPC histogram |
| Event delivery | at-least-once `comment.*` | — | publish success rate |
| Durability | no acked comment lost | — | Scylla `LocalQuorum` |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`.

---

## 🔗 Dependencies & Blast Radius

**Downstream:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (`comment`) | durable store | reads + writes fail | **Hard** — `CMT-…/Storage` |
| Kafka | `comment.*` emission | engagement comment counters lag | **Soft** — comments still persist |

**Upstream (blast radius):**

| Caller | Uses | Impact if `comment` is down |
|---|---|---|
| `engagement` | consumes `comment.created`/`deleted` | comment counts stop updating |
| `notification` | consumes `comment.created` | comment notifications stop |

> **Critical path?** Partially — comment writes/reads are user-facing; counter/notification propagation
> is async and eventually consistent.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `comment.v1.CommentService`

```protobuf
service CommentService {
  rpc CreateComment (CreateCommentRequest) returns (CreateCommentResponse);
  rpc DeleteComment (DeleteCommentRequest) returns (CommandResponse);
  rpc GetComment    (GetCommentRequest)    returns (CommentView);
  rpc ListTopLevel  (ListTopLevelRequest)  returns (ListCommentsResponse);
  rpc ListReplies   (ListRepliesRequest)   returns (ListCommentsResponse);
}
```

> **Wire contract:** a reply's `parent_id` must always be the **top-level** `comment_id` (never another
> reply) — the flat-tree allows exactly one nesting level. Pagination cursors are `created_at DESC`;
> inserts after the cursor are never returned (monotonically stable pages).

**Viewer-aware reads.** `GetComment` / `ListTopLevel` / `ListReplies` take the reader from the
transport (`edge::viewer`). For anyone but a mesh caller, a **read gate** decides with one post
`GetPost` and one social-graph `CheckAccess` per call (post author + every comment author on the page):
- the **post** must be readable: published and not removed by moderation (its author sees it
  regardless), and its author `VISIBLE` to the reader (not a private author they don't follow, not
  blocked either way, not hidden). Otherwise `GetComment` is `CMT-1001` and the lists are empty;
- comments by an author **hidden** to the reader (a block either way, a hidden profile) are dropped;
  a private commenter's comment on a readable post stays. A page can come back short while
  `next_token` stays valid.

The gate fails closed: an outage is `CMT-5001` (`UNAVAILABLE`), never the comments.

**Writes are gated too** (#656): `CreateComment` needs a post the commenter may read (else `CMT-1004`, `NOT_FOUND`) whose author takes their comments — social-graph `CheckInteraction(COMMENT)` over the author's interaction settings (everyone / followers / mutuals / no one) and blocks (else `CMT-1005`, `PERMISSION_DENIED`). Commenting on your own post is always allowed; an outage refuses the write (`CMT-5001`).

### Error contract (`CMT-xxxx`)

| Code | Error | HTTP |
|---|---|---|
| CMT-1001/1002/1003 | not found / already deleted / author mismatch | 404 / 409 / 403 |
| CMT-2001/2002/2003 | nesting depth / parent not found / parent deleted | 422 / 404 / 422 |
| CMT-3001/3002 | empty content / incomplete GIF metadata | 422 |
| CMT-1004 | `PostNotFound` (missing, or not readable by the commenter) | 404 → `NOT_FOUND` |
| CMT-1005 | `CommentsRestricted` (the post's author does not take this profile's comments) | 403 → `PERMISSION_DENIED` |
| CMT-5001 | `AccessCheckUnavailable` (read gate down; retryable) | 503 → `UNAVAILABLE` |
| CMT-4001 | Kafka publish failed | 500 |
| CMT-9001..9004 | invalid ids / domain violation | 422 |

---

## 📨 Events & Async Contract

**Publishes:**

| Topic | Trigger | Key | Payload | Consumers |
|---|---|---|---|---|
| `comment.created` | `CreateComment` success | `comment_id` | `comment_id, post_id, author_id, parent_id, created_at_ms` | `engagement` (incr), `notification` |
| `comment.deleted` | `DeleteComment` (either strategy) | `comment_id` | `comment_id, post_id, author_id, deleted_at_ms` | `engagement` (decr) |

**Consumes:** none.

> **Runtime contract:** events are published after the durable write. The downstream
> `engagement-comment-consumer` and `notification-comment-consumer` own at-least-once handling under
> `run_consumer`; engagement can rebuild its counter from its own Scylla table, so a transient publish
> failure is recoverable.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB insert fails | `CommentError::Storage` to caller | retry with same `comment_id` (INSERT is LWW) | check Scylla; retry is safe |
| Kafka publish fails | `CMT-4001`; engagement counters lag | **Soft** — comment persisted; eventual consistency | check Kafka; engagement rebuilds from its ledger |
| Feed read right after soft-delete shows old content | stale replica at `LocalOne` | expected eventual consistency (sub-ms convergence) | retry / route through `GetComment` |

**Backpressure & limits.** Feed lists are cursor-paginated; both tables are updated in the same handler
(sub-ms window between point store and feed index).

---

## 📦 Integration & Usage

```toml
[dependencies]
comment = { path = "crates/services/comment" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`comment::service::CommentService` — `build` wires the ScyllaDB repository and durable Kafka publisher;
`register` adds the gRPC + reflection services; `health_probes` checks Scylla.

### Bootstrap (`crates/apps/comment-server`)

```rust
use std::net::SocketAddr;
use comment::service::CommentService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("COMMENT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50057".to_owned())
        .parse()?;
    service_runtime::serve::<CommentService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_NODES` | **Yes** | — | ScyllaDB contact points (`host:port`). |
| `SCYLLA_KEYSPACE` | No | `comment` | Keyspace (see migrations). |
| `KAFKA_BOOTSTRAP_SERVERS` | **Yes** | — | Kafka brokers. |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` | No | `PLAINTEXT` | Auth for managed Kafka. |
| `COMMENT_GRPC_ADDR` | No | `0.0.0.0:50057` | gRPC bind address. |
| `COMMENT_POST_GRPC_ENDPOINT` / `COMMENT_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Yes** (prod) | `http://localhost:50056` / `:50053` | Mesh endpoints of the read gate (`GetPost`, `CheckAccess`). Non-mesh reads fail closed (`CMT-5001`) without them. |
| `COMMENT_GATE_RPC_TIMEOUT_MS` / `COMMENT_GATE_CONNECT_TIMEOUT_MS` | No | `1000` / `1000` | Deadlines for those calls. |

> Full `SCYLLA_*` / `KAFKA_*` tuning lives in the shared storage/transport crates.

### Compile-time features
- `build.rs` compiles `proto/comment/v1/*.proto` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `0001_create_keyspace.cql` → `0002_create_comments_table.cql` →
  `0003_create_comments_by_post_table.cql` against `comment`, applied **before** first start.
- **Rollout/Rollback:** `<TODO>`; stateless service, safe to roll.
- **Schema gotcha:** the nil-UUID sentinel and `comments_by_post` clustering order are a read contract —
  do not change after data exists.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread. Key spans: `comment.create`, `comment.delete`, `scylla.*`,
  `kafka.publish`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `comment_event_publish_errors_total` | engagement counters diverge | rate > 0 for 5m ⇒ high |
| `scylla_execution_errors_total{service="comment"}` | store health | rate > 0.1 for 2m ⇒ high |
| `CreateComment` p99 | write latency | > 500 ms ⇒ medium |
| `FAILED_PRECONDITION` rate | possible client abuse | spike > baseline ⇒ low |

---

## 🛠️ Local Development

```bash
cargo build -p comment && cargo clippy -p comment -- -D warnings
cargo test  -p comment
docker compose up -d scylla kafka             # repo-root compose
for f in crates/services/comment/migrations/*.cql; do cqlsh -f "$f"; done
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. Engagement comment counters are stale after a comment is created.**
Root cause: the `comment.created` event published, but `engagement-comment-consumer` is lagging or
stopped. Mitigation: check that consumer group's lag; restart the engagement comment consumer.
Engagement rebuilds its counter from its own `post_interaction_counters` Scylla table on restart — no
manual reconciliation needed.

**2. `CMT-2001 NestingDepthExceeded` for a valid-looking reply.**
Root cause: the request's `parent_id` points at a *reply* (non-nil parent), i.e. a reply-to-reply.
Mitigation: clients must always use the original top-level `comment_id` as `parent_id`; verify the
target's `parent_id` in `comment.comments` is the nil UUID.

**3. `comments_by_post` shows deleted content right after a soft-delete.**
Root cause: read-your-writes isn't guaranteed at `LocalOne`; the Fast profile may hit a stale replica.
Mitigation: expected eventual consistency (converges in ms). For consistency-sensitive reads, retry or
route through `GetComment`.

# `chat` — Hyperscale conversations where a private group can go viral without melting

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — real-time critical path |
> | **Deployable** | `crates/apps/chat-server` (library crate: `crates/services/chat`) |
> | **Datastores** | ScyllaDB keyspace `chat` · Redis Cluster (cache + sharded pub/sub) |
> | **Async** | publishes `chat.conversation.*` / `chat.member.*` / `chat.message.sent` · consumes `chat.conversation.unpublished` |
> | **Upstream callers** | `<TODO: gateway / clients via gRPC>` |
> | **Downstream deps** | ScyllaDB, Redis Cluster, Kafka |
> | **SLO** | `<TODO: 99.9%>` avail · live-tail fan-out p99 `<TODO>` · history read p99 `<TODO>` |

---

## 🎯 Overview & Service Role

`chat` is the unified **Conversation** microservice for the platform: it powers both **Group chats**
(symmetric `N↔N` mesh, bounded, with presence/typing/read-receipts) and **Channels** (asymmetric
`1→N` broadcast, passive reading, unbounded audience) — behind a single domain model and a single
gRPC surface.

The hard problem it solves is the **hybrid I/O profile**: an admin can flip a private Group to
**Public**, after which millions of passive guests may read history and subscribe to new messages
*while the core members keep interacting in real time*. Done naively, those guests cause **write
amplification** and **ScyllaDB hot-partitioning** on the exact partition the members are actively
writing.

The service resolves this with the **Shadowing Pattern**: one logical conversation projects onto two
physically isolated runtime planes.

| | **Member Plane** | **Audience Plane** |
|---|---|---|
| Cardinality | bounded (≤ 500) | unbounded (→ millions) |
| Direction | full-duplex `N↔N` | read-only `1→N` |
| Carries | messages **+** presence + typing + receipts | message **shadow** only |
| Per-recipient writes | receipts only, `O(members)` | **zero** |

**Core objectives:** members never feel the audience; guests never touch the member write/presence
loops; one durable message write fans out to *pods* (hundreds), never *subscribers* (millions).

---

## 📐 Architecture & Concepts

Hexagonal / DDD layout (`domain` → `application` → `infrastructure`), CQRS command/query buses,
ScyllaDB for the durable log, Redis Cluster for cache + real-time routing, Kafka for events.

```
                       ┌─────────────────────── gRPC (tonic) ───────────────────────┐
   member client ─────▶│ StreamConversation (full)        SendMessage / GetHistory… │◀──── guest client
   guest  client ─────▶│ StreamPublic (shadow)            ToggleVisibility / Sub…    │
                       └───────────────┬───────────────────────────┬────────────────┘
                                       │ CQRS                       │ real-time fork
                            ┌──────────▼──────────┐      ┌──────────▼───────────────┐
                            │ Command / Query bus │      │ MessageFanout            │
                            └──────────┬──────────┘      │  ├─ hot-tail cache push  │
                durable write          │                 │  ├─ member channel (all) │
            ┌──────────────────────────▼───┐             │  └─ audience shards(msg) │
            │ ScyllaDB                      │             └──────────┬───────────────┘
            │  messages_by_conversation     │                        │ SPUBLISH
            │   PK (conversation_id, bucket)│             ┌──────────▼───────────────┐
            │  members / subscriptions      │             │ Redis Cluster (sharded   │
            └───────────────────────────────┘            │ pub/sub + cache)         │
                                                          │  {conv:<id>} member slot │
   Kafka ◀── KafkaEventPublisher (chat.*)                │  {aud:<id>:<k>} spread   │
   Kafka ──▶ VisibilityWorker (unpublish → close guests) └──────────┬───────────────┘
                                                                     │ SSUBSCRIBE (refcounted)
                                                          ┌──────────▼───────────────┐
                                                          │ PlaneSubscriber (per pod) │
                                                          │  → ConversationRegistry   │
                                                          │     (member | audience)   │
                                                          │  → local broadcast to     │
                                                          │     gRPC streams          │
                                                          └───────────────────────────┘
```

**Hot-partition isolation.** `messages_by_conversation` uses a **composite `(conversation_id, bucket)`
partition key** (`bucket = floor(created_at_ms / CHAT_MESSAGE_BUCKET_HOURS)`). Members write the
*current* bucket (live tail); guests scroll *older* buckets (cold partitions, often different
replicas) — the write hotspot and the bulk read load are physically segregated by bucket age.
New-message reads for guests never hit Scylla at all; they arrive over the broadcast plane.

> **Invariants** (and where enforced): `StreamConversation` requires roster membership
> (`PERMISSION_DENIED` otherwise) — enforced at the gRPC boundary; `StreamPublic` requires
> `visibility == Public`; an outsider of a **private** conversation gets `NOT_FOUND` from every RPC,
> exactly as for a missing conversation (see below); the audience stream is *structurally*
> incapable of carrying presence/typing/receipts; the 500-member group cap is a domain-layer invariant;
> joining a private conversation requires a pending invitation (`NOT_FOUND` otherwise, see below) —
> domain-layer invariant.

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-`UNAVAILABLE` on writes/streams) | `<TODO: 99.9%>` | 30d rolling | gRPC status metrics |
| Live message → member stream delivery p99 | `< <TODO> ms` | 1h | fan-out span / client RTT |
| `GetHistory` read p99 (Fast profile) | `< <TODO> ms` | 1h | Scylla read p99 by profile |
| `SendMessage` durable-ack p99 (Strict profile) | `< <TODO> ms` | 1h | Scylla write p99 |
| Visibility teardown lag (`chat-visibility-consumer`) | `< <TODO> s` | live | consumer-group lag |
| Durability | no acked message lost | — | Scylla LocalQuorum on member writes |

**Error budget:** `<TODO: 0.1% / 30d ≈ 43m>`. **On burn:** `<TODO: freeze rollout / page>`.

> Real-time fan-out is **best-effort by design** and is *outside* the durability SLO: a `SendMessage`
> is "successful" once durably written, regardless of broadcast outcome (see §Failure Modes).

---

## 🔗 Dependencies & Blast Radius

**Downstream — what `chat` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| ScyllaDB (keyspace `chat`) | durable message/member/subscription log | writes + cold history fail | **Hard** — `UNAVAILABLE` |
| Redis Cluster | hot-tail cache + sharded real-time routing + presence | live fan-out + presence stop; history still served from Scylla | **Soft** — durable path unaffected |
| Kafka | domain + visibility events | events not emitted; guests not torn down on unpublish | **Soft** — `SendMessage` still succeeds |
| `social-graph` (`CheckInteraction`, mesh) | who may message whom (#656) | direct conversations and invitations refused | **Fail closed** — `UNAVAILABLE` (`CHT-5001`, retryable); groups and channels unaffected |

**Upstream — who depends on `chat` (blast radius if `chat` fails):**

| Caller | Uses | User-visible impact if `chat` is down |
|---|---|---|
| `<TODO: gateway / mobile+web clients>` | gRPC `ChatService` | no messaging, presence, or channel reads |
| `<TODO: notification>` | consumes `chat.message.sent` / `chat.member.*` | no chat-driven notifications |

> **Critical path?** **Yes** — `chat` is in the synchronous real-time path for every active
> conversation. A full outage is user-visible immediately.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `chat.v1.ChatService`

```protobuf
service ChatService {
  // Lifecycle / membership
  rpc CreateConversation (CreateConversationRequest) returns (CreateConversationResponse);
  rpc OpenDirectConversation  (OpenDirectConversationRequest)  returns (OpenDirectConversationResponse); // #656
  rpc RespondToMessageRequest (RespondToMessageRequestRequest) returns (CommandResponse);                // #656
  rpc ToggleVisibility   (ToggleVisibilityRequest)   returns (CommandResponse);
  rpc JoinAsMember       (JoinAsMemberRequest)       returns (CommandResponse); // private ⇒ invitation required
  rpc InviteMember       (InviteMemberRequest)       returns (CommandResponse); // owner/admin only
  rpc Subscribe          (SubscribeRequest)          returns (CommandResponse);
  rpc Unsubscribe        (UnsubscribeRequest)        returns (CommandResponse);
  // Messaging
  rpc SendMessage (SendMessageRequest) returns (SendMessageResponse);
  rpc MarkRead    (MarkReadRequest)    returns (CommandResponse);
  // Member-Plane signals
  rpc SendTyping (SendTypingRequest) returns (CommandResponse);
  rpc Heartbeat  (HeartbeatRequest)  returns (CommandResponse);
  // Queries
  rpc GetHistory        (GetHistoryRequest)        returns (GetHistoryResponse);
  rpc ListMembers       (ListMembersRequest)       returns (ListMembersResponse);
  rpc ListSubscriptions (ListSubscriptionsRequest) returns (ListSubscriptionsResponse);
  rpc ListConversationsByMember (ListConversationsByMemberRequest) returns (ListConversationsByMemberResponse); // mesh only
  // Real-time streams
  rpc StreamConversation (StreamConversationRequest) returns (stream StreamConversationResponse); // members
  rpc StreamPublic       (StreamPublicRequest)       returns (stream StreamPublicResponse);       // audience
}
```

**A profile's conversations (#653).** `ListConversationsByMember(member_id, limit, page_token)` is
**mesh only** (never on the edge): the GDPR data export lists the conversations a profile is a member of
(id, role, joined_at, kind; paged by conversation id), then reads each one's history as that member
(`GetHistory`). It reads `chat.conversations_by_member`, the reverse of the roster, written and deleted
with `members_by_conversation` in one LOGGED BATCH. Memberships from before that table existed are
indexed by an opt-in, idempotent backfill at startup (`CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER=true`).

> **Wire / enum contract:** proto enum values are **0-based and equal the domain `tinyint`**
> (`CONVERSATION_KIND_GROUP=0`, `…CHANNEL=1`, `…DIRECT=2`; `VISIBILITY_PRIVATE=0`, `…PUBLIC=1`; `ROLE_OWNER=0…GUEST=4`;
> `CONTENT_TYPE_TEXT=0…SYSTEM=2`). No `UNSPECIFIED` sentinel — the gRPC layer casts directly with no
> off-by-one mapping.

**Boundary invariants:** `StreamConversation` requires roster membership; `StreamPublic` requires
`visibility == Public`; the audience stream is structurally incapable of carrying
presence/typing/receipts. `Heartbeat` and `SendTyping` also require roster membership (a non-member
cannot inject Member-Plane signals).

**Private conversations are unprobeable.** A caller that is not on the roster of a **private**
conversation gets the same answer from every RPC as for a conversation that does not exist —
`NOT_FOUND` with `CHT-1009`, byte-identical on the wire to `CHT-1001` — so no RPC can be used to test
whether a private conversation exists. Members, and outsiders of a **public** conversation (which is
discoverable anyway), keep the precise error:

| Caller | Member-only RPCs¹ | `ToggleVisibility` | `GetHistory` | `Subscribe` | `StreamPublic` |
|---|---|---|---|---|---|
| member | allowed | `PERMISSION_DENIED` unless owner/admin | full history | `FAILED_PRECONDITION` (`CHT-1008`) on a private one | `FAILED_PRECONDITION` (`CHT-1008`) on a private one |
| outsider, **public** | `PERMISSION_DENIED` (`CHT-1007`) | `PERMISSION_DENIED` (`CHT-1007`) | from the public-since watermark | allowed | allowed |
| outsider, **private** | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) | `NOT_FOUND` (`CHT-1009`) |
| any, conversation missing | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) | `NOT_FOUND` (`CHT-1001`) |

¹ `SendMessage`, `MarkRead`, `SendTyping`, `Heartbeat`, `ListMembers`, `StreamConversation`. They
authorize with a single roster read; the conversation is loaded **only when that read misses**, to pick
the answer (`application::access::deny_non_member`), so the member hot path is unchanged.
`Subscribe` and `StreamPublic` check membership only on a private conversation, for a `subscriber_id`
bound to the caller at the edge (`edge::require_profile`: one of the token's `pids`; a guest owns
none), so nobody can ask about someone else's membership. `JoinAsMember` and `InviteMember` follow the
same rule (next paragraph).

**Joining a private conversation.** `JoinAsMember` is open-join on a **public** conversation only.
On a **private** one (group or channel) it requires a pending invitation for the caller, issued by an
owner/admin through `InviteMember` (a plain member gets `PERMISSION_DENIED`). The invitation lives in
`chat.invitations_by_conversation`, expires after **7 days** (table TTL; re-inviting refreshes it) and
is **consumed** by the join. Without one, the join fails `NOT_FOUND` with `CHT-1009`, whose gRPC code and
message are identical to `CHT-1001` (conversation not found): an outsider cannot tell a private
conversation from a missing one. An outsider calling `InviteMember` on a private conversation gets the
same answer. The rule is a domain invariant (`Conversation::admit_joiner` / `Conversation::invite`); the
caller stays bound to its own profile at the edge (`edge::require_profile` on `profile_id` /
`inviter_id`). No event is emitted for an invitation yet, so the invitee learns of it out of band.

**Presence settings (#661).** A member who turned **activity status** off (profile discovery settings)
announces no presence: no online/offline event, no heartbeat. One who turned **read receipts** off
broadcasts no `ReceiptEvent`, and `ListMembers` withholds its `last_read` from the other members (it still
sees its own; its unread state is unchanged). The settings are projected from `profile.v1.events`
(`ProfileDiscoverySettingsChanged`, group `chat-presence-settings`, from the earliest offset) into
`chat.presence_settings`; when they cannot be read, presence and receipts are withheld (fail closed).

**Direct messages and message requests (#656).** A `DIRECT` conversation is between two profiles,
**unique per pair** (`chat.direct_conversations`, claimed with an LWT so two concurrent openers share
it), never public, never joined or invited into, with the group's presence loops. It is opened with
`OpenDirectConversation(profile_id, peer_id)` — `CreateConversation` refuses the kind — and the peer's
message settings decide, through social-graph's mesh-only `CheckInteraction(MESSAGE)`:

| Peer's verdict for the opener | Opening | The opener's messages |
|---|---|---|
| allowed | open (`request = false`) | delivered |
| audience excludes them (followers / mutuals), or a temporary limit holds them (#669) | a **request** | **one**, waiting in the peer's requests; more fail `CHT-1010` |
| messages from no one | refused, `PERMISSION_DENIED` (`CHT-1011`) | refused |
| a block, either way | a request, never answered | **withheld**: shown to the sender only |

The recipient accepts by replying, by opening the conversation themselves, or with
`RespondToMessageRequest(accept = true)`. A decline is **silent**: the requester keeps seeing the request
pending (`CHT-1010` on a second message) and may not ask again for **30 days**. A **blocked sender sees
exactly the same** as a sender whose request is pending or declined — same answers, word for word — and
their messages are withheld: stored with `withheld = true`, returned by `GetHistory` to their sender
only, dropped from every other member's stream, never cached nor fanned to an audience, never delivered
even after an unblock. Once open, a conversation stays open whatever the peer's settings become; a
block withholds again. No presence, typing or read receipts flow either way before a request is
answered (`MarkRead` keeps nothing). Request transitions and the one message are compare-and-set
(`IF request_state = ?`, `IF request_sent = false`), so concurrent taps never send two. `InviteMember`
applies the invitee's settings too: messages from no one ⇒ `CHT-1011`; a block ⇒ answered as invited,
nothing stored; a followers / mutuals audience still allows it (an invitation is the invitee's consent).
`chat.message.sent` carries `withheld` and `request`, so a future push consumer delivers neither.
social-graph unreachable ⇒ `UNAVAILABLE` (`CHT-5001`): fail closed. Without
`CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT`, direct messages are off and invitations unchecked.

### Rust ports (hexagonal contract)

```rust
#[async_trait] pub trait MessageRepository {
    async fn insert(&self, message: &Message) -> Result<(), ChatError>;
    async fn list_history(
        &self, conversation_id: &ConversationId, limit: i32,
        cursor: Option<(i64, Uuid)>,          // (created_at_ms, message_id)
        floor_created_at_ms: Option<i64>,     // audience watermark — pushed into Scylla as `created_at >= ?`
    ) -> Result<(Vec<MessageSummary>, Option<(i64, Uuid)>), ChatError>;
}

#[async_trait] pub trait EventPublisher {            // KafkaEventPublisher in prod
    async fn publish_conversation(&self, event: &DomainEvent) -> Result<(), ChatError>;
    async fn publish_message(&self, event: &MessageEvent) -> Result<(), ChatError>;
}
```

### Error contract

Every fault implements `error::AppError` with a stable code, mapped to gRPC `Status` and HTTP by the
shared `error` crate:

| Range | Class |
|---|---|
| `CHT-1xxx` | lifecycle |
| `CHT-2xxx` | validation |
| `CHT-3xxx` | events |
| `CHT-4xxx` | streaming |
| `CHT-5xxx` | peer services (`CHT-5001`: social-graph's interaction check unavailable — retryable) |
| `CHT-9xxx` | identifiers |

`CHT-1009` (private conversation the caller may not see) is deliberately rendered exactly like
`CHT-1001` on the wire (`NOT_FOUND`, same message); the distinct code exists only in server logs.
Every conversation-scoped RPC returns it to an outsider of a private conversation, so `CHT-1007`
(not a member) and `CHT-1008` (not public) only ever reach members or outsiders of a public
conversation.

Direct messages (#656): `CHT-1010` (a request's one message is spent — also the answer to a declined
or blocked sender, by design), `CHT-1011` (the peer takes messages from no one; never the answer to a
block), `CHT-1012` (no request from someone else to answer).

---

## 📨 Events & Async Contract

> Kafka topics are an API. A schema change here breaks consumers exactly like a proto change.

**Publishes:**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `chat.conversation.created` | new conversation created | `conversation_id` | `<TODO>` |
| `chat.conversation.published` | visibility → Public | `conversation_id` | `<TODO>` |
| `chat.conversation.unpublished` | visibility → Private | `conversation_id` | **`chat` itself** (VisibilityWorker) |
| `chat.member.joined` | member added to roster | `conversation_id` | `<TODO: notification>` |
| `chat.member.left` | member removed from roster | `conversation_id` | `<TODO: notification>` |
| `chat.message.sent` | message durably written | `conversation_id` | `<TODO: notification / timeline>` |

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `chat.conversation.unpublished` | `chat-visibility-consumer` | every pod tears down guest streams cluster-wide when a conversation goes Private | DLQ `chat.conversation.unpublished.dlq` |

> **Runtime contract (mandatory):** the VisibilityWorker runs under `run_consumer` — manual commit
> after a terminal outcome, bounded retry with backoff + jitter, DLQ on exhaustion/poison, and
> rebuild-from-last-committed-offset on broker error. The producer injects `traceparent`/`tracestate`;
> the consumer re-establishes the parent span for end-to-end tracing across the async boundary.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| ScyllaDB unavailable | `SendMessage` / cold `GetHistory` fail | **Hard fail** — `UNAVAILABLE`; nothing is acked, so nothing is lost | check Scylla cluster / DC health |
| Redis unavailable | live messages stop; presence/typing gone | **Soft** — `SendMessage` still succeeds (durable); guests still read history from Scylla | check Redis Cluster; clients re-poll `GetHistory` |
| Redis hot-tail cache cold/evicted | passive-reader latency rises | **Soft & safe** — reads fall back to Scylla (durable source of truth) | verify cache hit ratio / cap; usually self-heals |
| Kafka unavailable | unpublish teardown delayed; downstream events stop | **Soft** — fan-out unaffected; teardown resumes from last committed offset | check brokers; watch `chat-visibility-consumer` lag |
| Slow stream consumer (lag) | client gets `Status::data_loss` | dropped per-plane; **member and audience backpressure are independent** — a slow guest never stalls a member | client reconnects + re-polls `GetHistory`; scale pods / raise buffer |
| Pod crash | presence/shard state stale | expiring sorted sets age out without explicit cleanup; drop guards release subs on disconnect | none — self-healing |

**Backpressure & limits.** Each plane has its own per-conversation `tokio::sync::broadcast` channel
(`CHAT_MEMBER_STREAM_BUFFER_SIZE` / `CHAT_AUDIENCE_STREAM_BUFFER_SIZE`); overflow ⇒ `Lagged` ⇒ drop.
Fan-out cost = **pods, not subscribers**: Redis `SPUBLISH` delivers once per *shard*, each pod re-fans
in-process; pod subscriptions are **reference-counted** (subscribe-on-first / unsubscribe-on-last), so
presence/typing/receipt churn never crosses to audience-only nodes. Page size is capped by
`CHAT_MAX_PAGE_SIZE` to prevent full-partition scans. Consistency is tiered: member writes use the
Scylla **Strict** profile (LocalQuorum); guest history uses **Fast** (LocalOne + speculative
execution); admin/analytics scans use **Analytical** (Quorum). All per-conversation Member-Plane keys
share the `{conv:<id>}` hash tag (single slot, no `CROSSSLOT`); audience channels use spreading
`{aud:<id>:<k>}` tags so a viral conversation is not pinned to one node.

---

## 📦 Integration & Usage

```toml
[dependencies]
chat = { path = "crates/services/chat" }
```

The crate is **library-only**. It plugs into the shared fleet runtime by implementing
[`service_runtime::Service`](../../platform/service-runtime/README.md) as `chat::service::ChatService`
— `build` wires every adapter (and spawns the per-pod plane subscriber, registry reapers, and
VisibilityWorker), `register` adds the gRPC services, and `health_probes` exposes the Scylla/Redis
liveness checks. Telemetry, config + hot-reload, ingress rate-limiting, health, and graceful shutdown
are all owned by the runtime.

### Bootstrap (`crates/apps/chat-server`)

```rust
use std::net::SocketAddr;
use chat::service::ChatService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("CHAT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_owned())
        .parse()?;

    // Owns telemetry, infrastructure.toml load + hot-reload, the inbound-trace and traffic
    // layers, dynamic gRPC health, and SIGTERM/SIGINT-drained shutdown — then builds `ChatService`
    // and serves until shutdown.
    service_runtime::serve::<ChatService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### Chat-specific variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CHAT_MAX_PAGE_SIZE` | No | `50` | Server-enforced cap on `GetHistory`/`ListSubscriptions` page size (prevents full-partition scans). |
| `CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER` | No | unset | `true`: index every existing membership by member at startup (once; idempotent) — #653. |
| `CHAT_HOT_TAIL_CACHE_SIZE` | No | `200` | Messages kept in the per-conversation Redis hot-tail cache (read offload). |
| `CHAT_MESSAGE_BUCKET_HOURS` | No | `24` | Time-bucket width for the Scylla message partition key. **Must be identical cluster-wide** and **never changed after data exists** (writer and reader derive the bucket from it). |
| `CHAT_MEMBER_STREAM_BUFFER_SIZE` | No | `256` | `broadcast` capacity per active Member-Plane stream; overflow ⇒ `Lagged`. |
| `CHAT_AUDIENCE_STREAM_BUFFER_SIZE` | No | `1024` | `broadcast` capacity per active Audience-Plane stream (sized larger for fan-out bursts). |
| `CHAT_AUDIENCE_SHARD_COUNT` | No | `16` | Number of Audience-Plane sharded channels a public conversation spreads across. **Must be uniform across the fleet.** |
| `CHAT_PRESENCE_TTL_SECS` | No | `30` | Presence liveness window (also reused as the audience-shard heartbeat TTL). |
| `CHAT_TYPING_TTL_SECS` | No | `6` | Typing-indicator expiry (short by design). |
| `CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT` | No | unset | social-graph's mesh gRPC (`CheckInteraction`, #656). Unset: direct messages off (`CHT-5001`), invitations unchecked. |
| `CHAT_SOCIAL_GRAPH_RPC_TIMEOUT_MS` / `CHAT_SOCIAL_GRAPH_CONNECT_TIMEOUT_MS` | No | `1000` / `1000` | Its request and connect timeouts. |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SCYLLA_CONTACT_POINTS` | **Yes** | — | ScyllaDB seed nodes (host:port, comma-separated). |
| `SCYLLA_LOCAL_DC` | **Yes** | — | Local datacenter for token/DC-aware routing. |
| `SCYLLA_KEYSPACE` | No | `chat` | Keyspace (see migrations). |
| `REDIS_HOSTS` | **Yes** | — | Redis Cluster nodes (host:port, comma-separated). |
| `KAFKA_BROKERS` | **Yes** | — | Kafka bootstrap brokers. |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` | No | plaintext | Auth for managed Kafka. |

> Full connection/timeout/reconnect tuning (`SCYLLA_*`, `REDIS_*`, `KAFKA_*`) is documented in the
> `scylla-storage`, `redis-storage`, and `transport` shared crates.

### Compile-time features
- `fred` is built with `["partial-tracing", "i-scripts"]`; the shared `redis-storage` transitively
  enables fred's **`subscriber-client`** (required for `SSUBSCRIBE`/`SPUBLISH`).
- `build.rs` compiles the protobuf contract (`proto/chat/v1/*.proto`) and emits a reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** apply `crates/services/chat/migrations/0001…0010.cql` against the `chat` keyspace
  **before** first start / before rolling a new binary.
- **Stateful gotchas:** `CHAT_MESSAGE_BUCKET_HOURS` and `CHAT_AUDIENCE_SHARD_COUNT` must be **uniform
  cluster-wide**, and `CHAT_MESSAGE_BUCKET_HOURS` must **never change after data exists** — divergent
  bucket math silently breaks history pagination.
- **Rollout:** `<TODO: rolling / canary strategy>`. Plane subscriptions and presence are self-healing
  on pod churn, so rolling restarts are safe.
- **Rollback:** `<TODO: confirm migrations are forward-compatible with N-1 binary>`.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime prerequisites:** a multi-threaded **Tokio** runtime (long-lived streaming tasks,
  background reapers, Kafka consumers, per-stream heartbeat tasks). The global tracing/OTel subscriber
  must be installed before `serve` (clients attach lifecycle + W3C trace-context propagation across
  the Kafka boundary).

| Signal | Why it matters | Suggested alert |
|---|---|---|
| Broadcast `Lagged` rate (per plane) | clients can't keep up; stream churn | audience-plane lag spike ⇒ raise `CHAT_AUDIENCE_STREAM_BUFFER_SIZE` / add pods |
| Scylla `messages_by_conversation` read p99 by profile | cold-history read pressure / cache miss | p99 > SLO ⇒ verify hot-tail cache hit rate |
| Hot-tail cache hit ratio | read offload health | sustained drop ⇒ Redis pressure / cap too small |
| Kafka consumer lag (`chat-visibility-consumer`) | delayed Audience-Plane teardown | lag > threshold ⇒ broker/Redis investigation |
| DLQ produce rate (`chat.*.dlq`) | poison / retry-exhausted events | any sustained rate ⇒ page |
| Active member vs audience subscriptions per pod | fan-out skew / hotspotting | imbalance ⇒ rebalance shards |

---

## 🛠️ Local Development

```bash
# Build / format / lint (this crate)
cargo build  -p chat
cargo fmt    -p chat
cargo clippy -p chat --all-targets
cargo test   -p chat

# Whole workspace (CI gate)
cargo build  --workspace
cargo clippy --workspace
```

**Local backing services** (ScyllaDB + Redis Cluster + Kafka):

```bash
docker compose up -d scylla redis kafka      # from the repo-root compose file
for f in crates/services/chat/migrations/*.cql; do cqlsh -f "$f"; done
```

> Disk note: the workspace `target/` is large; `rm -rf target/debug/incremental` safely reclaims space
> (rebuildable cache) if a build fails with `No space left on device`.

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. `StreamPublic` (or `Subscribe`, `GetHistory`, …) returns `NOT_FOUND` for a conversation that exists.**
Root cause: the conversation is `Private` (or was just unpublished) and the caller is not on its
roster: the service answers exactly as for a missing conversation (`CHT-1009` in the server logs,
see *Private conversations are unprobeable*); a member gets `FAILED_PRECONDITION` (`CHT-1008`)
instead. Audience access requires `visibility == Public`. Mitigation: confirm via `GetHistory` as a member, or
`ToggleVisibility{make_public:true}` if intended. After an unpublish, the `VisibilityWorker` closes
guest streams cluster-wide — clients must stop retrying `StreamPublic`.

**2. Guests see no new messages, but members do.**
Root cause: the Audience Plane isn't fanning — usually **no active shard** in the routing registry
(every audience pod's shard heartbeat expired) or `CHAT_AUDIENCE_SHARD_COUNT` mismatch between pods.
Mitigation: verify `chat:{aud:<id>:<k>}` shard activation in Redis and that `CHAT_AUDIENCE_SHARD_COUNT`
is uniform across the fleet; confirm pods can `SPUBLISH`/`SSUBSCRIBE` (Redis 7+ sharded pub/sub). New
joiners still get history from the hot-tail cache, so a fanning gap looks like "history works, live
doesn't."

**3. History pagination misses messages or returns empty mid-scroll.**
Root cause: a `CHAT_MESSAGE_BUCKET_HOURS` value that differs between writers and readers (bucket math
diverges), or a scroll older than `MAX_BUCKET_WALK` (90 buckets) per request. Mitigation: make
`CHAT_MESSAGE_BUCKET_HOURS` identical cluster-wide and never change it after data exists; for deep
history, page with the returned cursor rather than one large request. For audience readers, remember
reads are floored at the public-since watermark by design.

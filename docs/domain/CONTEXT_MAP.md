# Context Map

> Populated from the 17 per-service Domain Cards (`crates/services/<svc>/docs/DOMAIN.md` §1 + §7),
> grounded in the crates, their `*.v1` protos, and the event-topology registry guard. This file is
> **multi-owner** (the architecture guild owns it). Do **not** derive it from the old pre-fleet C4
> (since removed).

## What this document is

A [DDD **context map**](https://martinfowler.com/bliki/BoundedContext.html): the bounded contexts
and the *type of coupling* on every edge between them. The coupling type — not just "A talks to B" —
tells a reviewer what breaks when a neighbour changes.

## Relationship patterns (the vocabulary)

| Pattern | Meaning in this codebase |
|---|---|
| **Open-Host Service / Published Language (OHS/PL)** | A context publishes a stable contract for all comers — our `*.v1` protos and Kafka topics |
| **Anti-Corruption Layer (ACL)** | A consumer translates a foreign schema into its own model at the edge — the `infrastructure/decode.rs` wire→domain mappers |
| **Conformist** | A downstream accepts an upstream's model as-is, no translation |
| **Customer / Supplier** | A synchronous (gRPC) dependency where the downstream's needs shape the upstream |
| **Separate Ways** | Two contexts deliberately *not* integrated (decoupled on purpose) |
| **Shared Kernel** | A shared model both contexts depend on (the foundation crates, e.g. `auth-context`) |

## Subdomain classification

Drives investment. Rolled up from each Domain Card's class line.

| Subdomain class | Contexts | Rationale |
|---|---|---|
| **Core** | `post`, `comment`, `chat`, `social-graph`, `engagement` | The content + social fabric — the platform's competitive substance |
| **Supporting** | `account`, `auth`, `profile`, `audit`, `moderation`*, `media`, `notification`, `realtime`, `search`, `geo-discovery`, `counter`, `timeline` | Necessary capabilities and derived read-models / planes; bespoke but not value-origin |

> \* `moderation` is classed **Core** in its own Domain Card (trust & safety is product-differentiating
> for a UGC network); it is the one judgement call most worth revisiting. All other classifications
> follow "Core = where truth/value originates; Supporting = derived, infrastructural, or compliance".

## The map — asynchronous edges (Kafka, Published Language)

> Direction is **producer → consumer**. Every producer topic is part of that context's OHS/PL; every
> consumer applies an ACL (`decode.rs`) unless noted Conformist.

| Producer | Topic (Published Language) | Consumer | Pattern | What breaks downstream if the producer changes |
|---|---|---|---|---|
| `account` | `account.v1.events` | `audit` | ACL | compliance evidence + GDPR Art. 17 crypto-shred loop |
| `account` | `account.v1.events` | `profile` | ACL | persona provisioning |
| `account` | `account.v1.events` (`account_deleted`) | `engagement` | ACL | erasure of who liked (#665; the counts stay) |
| `auth` | `auth.v1.events` | `audit` | ACL | session-lifecycle evidence |
| `profile` | `profile.v1.events` | `post` | ACL | author snapshots on posts |
| `profile` | `profile.v1.events` | `search` | ACL | profile indexing |
| `profile` | `profile.v1.events` (`tier_changed`) | `geo-discovery` | ACL | author-tier ranking weight |
| `profile` | `profile.v1.events` (`tier_changed`) | `timeline` | ACL | push/pull fan-out decision |
| `profile` | `profile.v1.events` | `social-graph` | ACL | relation validity vs profile existence |
| `post` | `post.v1.events` | `timeline` | ACL | feed fan-out |
| `post` | `post.v1.events` | `search` | ACL | post indexing |
| `post` | `post.v1.events` | `geo-discovery` | ACL | map cards |
| `post` | `post.v1.events` | `counter` | ACL | post magnitudes |
| `post` | `post.v1.events` | `realtime` | ACL (broadcast) | live post broadcast |
| `comment` | `comment.created` / `comment.deleted` | `notification` | ACL | reply notifications |
| `comment` | `comment.created` / `comment.deleted` | `engagement` / `counter` | ACL | comment counts |
| `wallet` | `wallet.v1.events` | `engagement` | ACL | like counts (#665: a like is a point) |
| `wallet` | `wallet.v1.events` | `notification` | ACL | like notifications (a liker's first batch) |
| `wallet` | `wallet.v1.events` | `counter` | ACL | like magnitudes |
| `wallet` | `wallet.v1.events` | `timeline` | ACL | interest tags (a first like) |
| `wallet` | `wallet.v1.events` | `geo-discovery` | ACL | each country's likes (the unlock ladder) |
| `counter` | `counter.v1.popularity` | `search` | ACL | popularity ranking signal |
| `counter` | `counter.v1.popularity` | `realtime` | ACL (broadcast) | live engagement counters |
| `moderation` | `moderation.v1.events` (`decision_recorded`) | `audit` | ACL | DSA-rationale compliance evidence |
| `moderation` | `moderation.v1.events` (Plane B) | `timeline`, `chat`, `account` | ACL | enforcement denormalization |
| `moderation` | `moderation.v1.events` | `post`, `search` | ACL | content visibility/enforcement reflection |
| `moderation` | `moderation.v1.events` (takedown) | `media` | ACL | asset takedown |
| `media` | `media.v1.events` | `post`, `profile`, `search` | ACL | embeds / indexing |
| `notification` | `notification.v1.events` | `realtime` | ACL (targeted) | live notification delivery |
| `chat` | `chat.conversation.unpublished` | `chat` `VisibilityWorker` | internal | audience-plane teardown |
| `social-graph` | relation events (`ProfileFollowed`…) | — | OHS (deferred) | the `social-graph.follows` Kafka producer is **deferred**; consumers read via gRPC today |

## The map — synchronous edges (gRPC + verify)

| Caller | Callee | Pattern | Mechanism | What breaks if the callee changes |
|---|---|---|---|---|
| `media` | `moderation` | Customer/Supplier (sync, fail-closed) | `Screen` RPC | catastrophic-category upload gating |
| `moderation` | `account` | Customer/Supplier | suspension/ban execution | enforcement application |
| `timeline` | `social-graph` | Customer/Supplier | follower-set reads for fan-out | feed fan-out |
| `counter` | `social-graph` | Customer/Supplier | follower-count reconciliation | follower-magnitude drift heal |
| `post` | `media` | Customer/Supplier | `MediaAttachment` references | media in posts |
| `comment` | `post` | Customer/Supplier | `PostId` references | comment validity |
| `auth` | `account` | Customer/Supplier | `SubjectLink` ↔ `AccountId` | session subject resolution |
| `wallet` | `engagement` | Customer/Supplier | `GetLikePositions` (mesh) | stake settlement (#665): how early each position came |
| `realtime` | `auth` | Conformist (verify-only) | `auth-context` ES256 token verify at handshake | new connection authentication |
| **all services** | `auth` | Shared Kernel / OHS | edge token verified in-process via `auth-context` | every authenticated call |

## Notable non-integrations (Separate Ways)

| A | B | Why deliberately decoupled |
|---|---|---|
| `realtime` | `chat` | `chat.message.sent` is **not** consumed by `realtime`; chat runs its own live plane (coexist-first — see [`ADR-0003`](../adr/0003-realtime-is-a-fail-open-system-of-connection.md) / [`ADR-0006`](../adr/0006-chat-shadowing-pattern-member-vs-audience-plane.md)). Consolidation is a future decision. |

## Terminal sinks (consume, never produce of record)

`audit`, `search`, `timeline`, `geo-discovery`, `realtime` publish **nothing of record** — they are
read-models or evidence sinks. See each Domain Card §8.

## Diagram

> The C4 model has been **regenerated from this map** (and the per-service Domain Cards) as a derived
> artifact: [`docs/architecture/workspace.dsl`](../architecture/workspace.dsl). This document remains
> the authoritative source for the relationships; the diagram is generated to match it.

# `notification` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Notifications — the user activity feed + push fan-out |
> | **Subdomain class** | **Supporting** — a derived delivery/feed plane; owns no source content |
> | **System of …** | **Record** for the notification activity feed (derived from upstream facts) |
> | **Aggregate root(s)** | `Notification` (`domain`) |
> | **Tier** | **TIER-2** — best-effort / derived |
> | **Failure posture** | **Fail-open** — a missed notification is re-derivable; nothing blocks |
> | **Upstream contexts** | `comment`, `engagement`, `post`, `social-graph` — via **ACL** over Kafka |
> | **Downstream contexts** | clients (feed read + gRPC broadcast stream); offline push (APNs/FCM); delegated from `realtime` |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `notification` is the authority for **the activity feed**: it answers
**"what should this user be told happened, how many are unread, and how do we deliver it?"**

**The hard problem.** Collapsing a high-fan-in event stream into a per-user feed without write
amplification — a **Redis write-collapse fan-out** (3 layers + an hourly cap) over a TWCS activity
feed, with deterministic UUIDv5 ids and a claim-gated unread counter.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Own the source events → it derives notifications from upstream facts.
- ❌ Be the live socket → `realtime` delivers live; notification owns the durable feed + offline push.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Notification | A single activity-feed entry | `Notification`, `NotificationId`, `NotificationKind` |
| Subject | The thing the notification is about | `SubjectId`, `SubjectKind` |
| Read / created event | Feed lifecycle facts | `NotificationCreatedEvent`, `NotificationReadEvent` |
| Write-collapse | Coalescing many triggers into one feed entry | (Redis fan-out) |
| Unread counter | The claim-gated unread badge count | (Redis `SET NX`) |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Notification` | aggregate root | Feed-entry identity + read state |
| `NotificationKind` / `SubjectKind` | enum | Closed notification/subject vocabularies |
| `SubjectId` | VO | What the notification references |

> **Invariant.** Ids are deterministic UUIDv5 (idempotent on redelivery); the unread counter is
> claim-gated (`SET NX`) so a re-delivered event can't double-increment; unique senders collapse
> via `SADD`. `created_at` is event-time, not ingest-time.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- The per-user notification feed + unread counters — **ScyllaDB** (TWCS activity feed) + **Redis** (write-collapse counters). Derived, but authoritative for the feed view.

**The "do-not-write" list:** notification never mutates the source content; it reacts to events.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Notification ids are deterministic UUIDv5 (idempotent) | domain | `NTF-1xxx` |
| I2 | The unread counter is claim-gated (no double-count on redelivery) | application (Redis `SET NX`) | `NTF-1xxx` |
| I3 | `created_at` is event-time, not ingest-time | domain | `NTF-9xxx` |

---

## 6. Workflows & Orchestration &nbsp;·&nbsp; DEEP

N/A (TIER-2, collapsed) — consumes upstream events (`comment.created`, `wallet.v1.events` (likes, #665), `post.published`, social follows) under `run_consumer`, write-collapses into the per-user feed, increments the claim-gated unread counter, and pushes via the gRPC broadcast stream (live) or APNs/FCM (offline, delegated from `realtime`).

## 7. Context Relationships &nbsp;·&nbsp; DEEP

N/A (TIER-2, collapsed) — **upstream (ACL):** `comment`, `engagement`, `post`, `social-graph` event streams. **downstream (OHS):** clients (feed + broadcast stream); offline push providers.

## 8. Domain Events &nbsp;·&nbsp; DEEP

N/A (TIER-2, collapsed) — publishes **none of record** (its `NotificationCreated`/`Read` events are internal feed state). Consumes upstream facts whose meanings are owned by their contexts.

## 9. Decisions & Rationale &nbsp;·&nbsp; DEEP

N/A (TIER-2, collapsed) — keystone choice: Redis write-collapse fan-out + claim-gated idempotent unread counter (deterministic UUIDv5 ids, event-time `created_at`, `SET NX` unread claim, `SADD` unique-sender collapse) — [`ADR-0012`](../../../../docs/adr/0012-notification-write-collapse-fanout-claim-gated-counter.md) (Accepted).

## 10. Subdomain Classification & Evolution &nbsp;·&nbsp; DEEP

N/A (TIER-2, collapsed) — **Supporting**, low volatility; deferred: richer notification types, preference center, digest batching.

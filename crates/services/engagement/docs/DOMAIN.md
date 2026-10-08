# `engagement` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Engagement — likes (points) and interaction counters |
> | **Subdomain class** | **Core** — direct user interaction with content; likes are product fabric |
> | **System of …** | **Reference** for likes (the wallet's stakes are the record, #665); **Record** for view/share counters until `counter` supersedes them |
> | **Aggregate root(s)** | none — likes are applied totals (`LikeTarget` VO), counters are increments |
> | **Tier** | **TIER-1** |
> | **Failure posture** | **Fail-open-ish** — Redis-primary with Lua atomicity, Kafka-fed |
> | **Upstream contexts** | `wallet` (stakes); end-user clients (views/shares); `comment` (counts); `post` (hidden like counts) |
> | **Downstream contexts** | `account` (GDPR export, `ListLikesByAccount`) — via **Open Host Service** (mesh gRPC) |
> | **Decision log** | [`ADR-0009`](../../../../docs/adr/0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) |

---

## 1. Business Capability & Non-Goals

**Capability.** `engagement` answers **"how many likes does this post or comment have, how many are
mine, and how often was it viewed, shared and commented?"**

**The hard problem.** Turning the wallet's at-least-once, unordered stake events into exact like counts
at hot-post scale — each event carries the account's **total**, applied by a Lua script that only moves
forward — without a database round-trip on the read path.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Decide whether a like is allowed (points, caps, visibility, self-like) → `wallet` owns stakes.
- ❌ Fan likes out (notifications, interests, country ladder) → they read `wallet.v1.events` directly.
- ❌ Own the content liked → `post` / `comment`.
- ❌ Weighted reactions (heart, fire, rocket, clap, sad) — removed with #665: a like is a point.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Like | One point an account staked on a post or comment (no unlike) | `LikeStore`, `LikeLedger` |
| Like target | The post or comment a like lands on | `LikeTarget` |
| Total | An account's points on one target; only grows | `apply_total`, `AccountLike::total` |
| Like count | The sum of the totals on a target | `LikeSummary::count` |
| Hidden likes | The author hides like counts (#809): count withheld from non-authors | `LikeVisibility` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `LikeTarget` | VO | Only a post or a comment, with a bounded id |
| `PostId` | VO | The post a counter belongs to |
| `AccountLike` | read model | What an account liked (GDPR export) |
| `PostEngagementSnapshot` | read model | A post's view/share/comment counters |

**Lifecycle of an account's likes on a target:**

```
(none) --(stake, total n)--> n --(stake, total m > n)--> m      (a total ≤ the one held: no change)
```

> **Legal transitions only.** Totals only grow; the target's count moves by the difference, atomically.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- View/share/comment counters — **Redis** (primary) with an approximate **ScyllaDB** copy.

**It holds a reference copy of:** likes — the wallet's stakes, as totals per (account, target): Redis
(read path) and ScyllaDB `likes_by_target` / `likes_by_account` (recovery, GDPR export).

**The "do-not-write" list:** engagement never decides a like; it does not own the content.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | A target's like count is the sum of its accounts' totals | Lua script (one hash tag per target) | — (atomic) |
| I2 | An account's total on a target only grows; redeliveries and late events change nothing | Lua script; Scylla write timestamp = stake time | — (ignored) |
| I3 | Hidden like counts reach only the author and the mesh; when post cannot say, they are withheld | application | `ENG-6001` (fail closed) |
| I4 | Only posts and comments can be liked | `LikeTarget::parse` | `ENG-9004` |

---

## 6. Workflows & Orchestration

**Like.** The wallet commits a stake and publishes `StakeCommitted` (outbox); engagement's
`StakeConsumer` applies the account's total in Redis, then writes the durable copy.

**Read.** `GetPostEngagement` / `BatchGetLikes` read counters and likes from Redis, withholding hidden
counts per post's answer (`BatchGetLikeVisibility`, cached 60 s).

**Export.** account's GDPR export pages `ListLikesByAccount` (mesh only) into `likes.json`.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| `wallet` | upstream | Conformist | `wallet.v1.events` (`stake_committed`) | like counts break |
| `comment` | upstream | ACL | `comment.created` / `comment.deleted` | comment counts break |
| `post` | upstream | Customer/Supplier | gRPC `BatchGetLikeVisibility` | hidden like counts withheld from everyone but the mesh |
| `account` | downstream | Open Host Service | gRPC `ListLikesByAccount` | the GDPR export fails (retried) |

---

## 8. Domain Events (semantics, not wire)

engagement publishes no events. It consumes:

| Event | Means | Effect here |
|---|---|---|
| `wallet.v1.events` `stake_committed` | an account's points on a target reached a new total | like count and the account's own move |
| `comment.created` / `comment.deleted` | a comment was posted / removed | comment counter ±1 |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Redis-primary Lua-atomic hot path, Kafka-fed durable copies | [`ADR-0009`](../../../../docs/adr/0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) | Accepted (amended by #665) |
| Likes are points staked in the wallet; reactions removed | #665 | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Core — direct content interaction.
- **Volatility:** low — likes follow the wallet's stake contract.
- **Known modeling debt:** erasing an account's likes on `account_deleted`; the likers hash has no TTL
  (a rehydration floor from Scylla is planned).
- **Deferred capabilities:** like settlement (gems earned from likes, #665).

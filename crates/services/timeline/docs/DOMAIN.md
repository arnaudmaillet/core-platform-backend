# `timeline` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Timeline — the home-feed read model |
> | **Subdomain class** | **Supporting** — a derived feed projection; owns no source content |
> | **System of …** | **Reference (SoRef)** — a materialized per-user feed, rebuildable from upstream |
> | **Aggregate root(s)** | `FeedEntry` (projection), addressed by `FeedCursor` |
> | **Tier** | **TIER-1** |
> | **Failure posture** | **Fail-open** — a degraded feed returns fewer/staler entries, never an error |
> | **Upstream contexts** | `post` (content), `social-graph` (follower graph) — via events + gRPC |
> | **Downstream contexts** | clients (feed read); publishes none of record |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `timeline` is the authority for **the home feed**: it answers
**"what should this user see in their feed right now, in order?"**

**The hard problem.** Feed generation at scale without the fan-out-on-write cost of celebrities or
the fan-out-on-read cost of everyone — a **hybrid push/pull** model: materialize feeds for normal
authors (push), pull for high-tier authors at read time, merged via a Lua `ZREVRANGEBYSCORE`.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Own posts → `post` is the SoR; timeline holds feed references.
- ❌ Own the follower graph → reads `social-graph` (gRPC) for fan-out.
- ❌ Rank by popularity magnitudes → that signal comes from `counter` (where wired).

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Feed entry | One materialized item in a user's feed | `FeedEntry` |
| Feed cursor | The pagination position in a feed | `FeedCursor` |
| Fan-out mode | Push (materialize) vs pull (read-time) per author | `FanOutMode` |
| Author tier | The tier that decides push vs pull | `AuthorTier` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `FeedEntry` | projection (aggregate) | A feed item's identity + ordering score |
| `FeedCursor` | VO | Stable pagination position |
| `FanOutMode` | enum | Push vs pull decision per author |
| `AuthorTier` | enum | The tier driving the hybrid decision |

> **Invariant.** Feed entries are ordered by score (Lua `ZREVRANGEBYSCORE` via eval); members are
> encoded compactly; `from_uuid` is infallible. High-tier authors are pulled at read time, not
> materialized, to bound fan-out cost.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth (of *reference*) for:**
- The materialized per-user feed — **Redis** (feed ZSETs) + **ScyllaDB** (durable materialization). Rebuildable from `post` + `social-graph`.

**This context holds derived copies it does NOT own:**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| Post content/refs | `post` | `post.v1.events` / `post.deleted` | eventually consistent |
| Follower graph | `social-graph` | gRPC follower-set reads | read-time |
| Author tier | `profile` (emits) | tier-change consumption | eventually consistent |

**The "do-not-write" list:** timeline never writes posts or the graph — it projects them into feeds.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Hybrid fan-out — normal authors pushed, high-tier pulled at read | domain/application | `TML-1xxx` |
| I2 | Feed ordering by score via Lua `ZREVRANGEBYSCORE` | infrastructure (Lua) | `TML-1xxx` |
| I3 | Reads fail open (degrade, never error) | application | `TML-1xxx` |
| I4 | A deleted post is removed from feeds | application (consumer) | `TML-1xxx` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Fan-out on publish (push).** Consume `PostPublished` (`post.v1.events`) → read the author's follower set from
`social-graph` (gRPC) → for normal-tier authors, materialize the entry into each follower's feed
ZSET. High-tier authors are skipped here (pulled at read).

**Read (hybrid merge).** A feed read merges the materialized push entries with a read-time pull of
the user's high-tier followees, ordered by score via Lua `ZREVRANGEBYSCORE`, paginated by
`FeedCursor`. Fail-open on a degraded backend.

**Teardown.** Consume `post.deleted` → remove the entry from affected feeds.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| `post` | upstream | ACL | `post.v1.events` / `post.deleted` | feed freshness/teardown breaks |
| `social-graph` | upstream | Customer/Supplier (gRPC) | follower-set reads | fan-out breaks |
| `profile` | upstream | ACL | `tier_changed` | push/pull decision goes stale |
| clients | downstream | OHS | feed-read RPC | the home feed breaks |

> **Anti-Corruption Layer:** the `post` event consumer translates post lifecycle into feed mutations.

---

## 8. Domain Events (semantics, not wire)

> Publishes **none of record** — it is a read-model. It consumes `post` (and reads `social-graph`);
> their meanings are owned by those contexts.

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Hybrid push/pull fan-out (materialize normal authors, pull high-tier at read) | [`ADR-0017`](../../../../docs/adr/0017-timeline-hybrid-push-pull-fanout.md) | Accepted |
| Forward-compatibility for author-tier-driven fan-out (shipped #469) | _open — author-tier initiative_ | Scoped |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Supporting — a derived feed projection over `post` + `social-graph`.
- **Volatility:** medium — ranking and the push/pull threshold evolve.
- **Known modeling debt:** fan-out performance tuning (TD-4); author-tier producer side not yet complete.
- **Deferred capabilities:** popularity-weighted ranking from `counter`; personalized/ML ranking.

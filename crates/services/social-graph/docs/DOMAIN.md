# `social-graph` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Social Graph — follower/following and block relations |
> | **Subdomain class** | **Core** — the social graph is the network itself |
> | **System of …** | **Record** for relations (follows, blocks) and derived author tier |
> | **Aggregate root(s)** | `Relation` (with `FollowEdge` / `BlockEdge`) |
> | **Tier** | **TIER-1** |
> | **Failure posture** | **Fail-closed on writes** — a relation change must be atomic + durable |
> | **Upstream contexts** | clients (follow/block); `profile` (identity) |
> | **Downstream contexts** | `timeline` (fan-out, gRPC reads), `counter` (follower counts), `profile` (tier) — via gRPC + events |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `social-graph` is the authority for **relations**: it answers
**"who follows whom, who blocked whom, and what tier is this author?"**

**The hard problem.** Maintaining a high-fan-out relation graph with consistent reverse indexes and
hot-set reads — a 4-table ScyllaDB schema with Redis Set hot-relation caching, logged-batch
atomicity for the dual-write, and tier thresholds derived from follower counts.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Build timelines → `timeline` reads the graph (gRPC) and fans out.
- ❌ Own author tier *presentation* → `social-graph` computes it; `profile` owns + emits it.
- ❌ Own profiles → `profile`.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Relation | A directed edge between two profiles | `Relation`, `RelationKind`, `RelationStatus` |
| Follow / block edge | The two relation kinds | `FollowEdge`, `BlockEdge` |
| Relation context | The surrounding metadata of a relation | `RelationContext` |
| Author tier | The tier derived from follower count | `AuthorTier`, `TierThresholds`, `AuthorTierChanged` |
| Severed follows | Follows removed when a block is applied | `SeveredFollows` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Relation` | aggregate root | Edge consistency across forward + reverse indexes |
| `FollowEdge` / `BlockEdge` | VO | The two directed relation kinds |
| `RelationStatus` / `RelationKind` | enum | Closed relation vocabularies |
| `AuthorTier` / `TierThresholds` | VO | Tier derivation from follower count |
| `SeveredFollows` | VO | The follows a block tears down |

**Relation transitions:**

```
(none) --(follow)--> following --(unfollow)--> (none)
(none) --(follow a private profile)--> requested --(owner approves)--> following
                                        requested --(owner declines / requester cancels / block)--> (none)
(none) --(block)--> blocked (severs existing follows both ways)
```

> **Legal transitions only.** A block severs existing follows (`SeveredFollows`); forward and reverse
> indexes are written atomically (logged batch); a follower-count crossing a threshold changes `AuthorTier`.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Relations (follows, blocks) and their reverse indexes — **ScyllaDB** (4-table) + **Redis** (hot-relation Sets). No other service writes relations.

**This context holds copies it does NOT own:**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| Profile existence | `profile` | `profile.v1.events` | eventually consistent |

**The "do-not-write" list:** social-graph never builds feeds and never writes profile presentation
(it computes tier; `profile` owns + emits it).

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Forward + reverse relation indexes are written atomically | application (logged batch) | `SGR-1xxx` |
| I2 | A block severs existing follows both directions | domain | `SGR-1xxx` |
| I3 | Author tier is derived from follower count crossing `TierThresholds` | domain | — |
| I4 | Hot-relation reads are served from Redis Sets, rebuildable from Scylla | infrastructure | `SGR-1xxx` |
| I5 | Following a private profile creates a pending request, never a follow: no access, not in the lists, until the owner approves (one logged batch: request rows out, follow rows in); a block drops requests both ways | domain + application | `SGR-1005` / `SGR-1006` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Follow / unfollow / block.** Mutate the `Relation` aggregate → atomic logged-batch write of
forward + reverse Scylla rows + Redis hot-set update. A block emits `SeveredFollows`.

**Tier computation.** A follower-count change crossing a `TierThresholds` boundary produces
`AuthorTierChanged`, feeding the profile→tier flow (author-tier initiative; producer side scoped).

**Reads.** `timeline` reads the follower set via gRPC for fan-out; `counter` reconciles follower
counts via gRPC (the `social-graph.follows` Kafka stream is a deferred producer).

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| `profile` | upstream | ACL | `profile.v1.events` | relation validity vs unknown profiles |
| `timeline` | downstream | Customer/Supplier (gRPC) | follower-set reads for fan-out | feed fan-out breaks |
| `counter` | downstream | Customer/Supplier (gRPC) | follower-count reconciliation | follower magnitudes drift |
| `profile` | downstream | Published Language | tier change flow | author-tier emission breaks |

> **Anti-Corruption Layer:** the `profile` event consumer keeps relation validity aligned with
> profile existence.

---

## 8. Domain Events (semantics, not wire)

| Event | Means | Emitted when | Who reacts |
|---|---|---|---|
| `ProfileFollowed` / `ProfileUnfollowed` | a follow edge was created/removed | follow/unfollow commits | timeline/counter (consumers; follows-stream wiring deferred) |
| `ProfileBlocked` / `ProfileUnblocked` | a block edge changed (severs follows) | block/unblock commits | feeds |
| `AuthorTierChanged` | the author's tier changed | follower count crosses a threshold | `profile` (owns + re-emits) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| 4-table ScyllaDB schema + Redis hot Sets + logged-batch atomicity for dual-write | [`ADR-0016`](../../../../docs/adr/0016-social-graph-four-table-scylla-logged-batch.md) | Accepted |
| Author tier: social-graph computes → profile owns + emits | _open — author-tier initiative_ | Scoped |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Core — the relation graph is the social network.
- **Volatility:** low-to-medium — relation kinds are stable; tier policy may tune.
- **Known modeling debt:** orphan reconciliation (TD-6); the `social-graph.follows` Kafka producer is deferred (counter consumes via gRPC for now).
- **Deferred capabilities:** NebulaGraph-style recommendation traversals; mutual/second-degree queries.

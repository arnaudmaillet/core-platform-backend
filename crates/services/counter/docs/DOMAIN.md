# `counter` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Counter / Analytics — magnitudes ("how many?") |
> | **Subdomain class** | **Supporting** — a derived measurement plane; valuable but not the product's value origin |
> | **System of …** | **Reference (SoRef)** for counts/cardinalities/trends — exact-but-reconcilable, never edge state ("who?") |
> | **Aggregate root(s)** | `Metric` + `WindowAggregator` (`domain`) |
> | **Tier** | **TIER-1** |
> | **Failure posture** | **Fail-open** — a read degrades to a stale/approximate magnitude, never an error on the hot path |
> | **Upstream contexts** | view/impression/click producers, `wallet` (stakes: likes, #665) — via **ACL** over Kafka |
> | **Downstream contexts** | `search` (PopularityScore), `realtime` (broadcast) — via **Published Language** (`counter.v1.popularity`) |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `counter` is the authority for **magnitudes**: it answers
**"how many views / likes / followers / trending rank does this entity have?"** — never *who* did
what (that is edge state, owned elsewhere).

**The hard problem.** Counting a firehose at write-volume without losing accuracy or melting the
hot path: pure-Kafka ingest with windowed N→1 pre-aggregation, two-stage sharded counters, a
3-tier store (Redis hot / Postgres warm-SoRef + reconciliation / Scylla TWCS cold), HLL for unique
views and CMS for trending, exact-but-reconcilable for likes/follows.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Hold edge state (who liked/followed whom) → `engagement` / `social-graph`.
- ❌ Be a System of Record → it is a reconcilable System of *Reference*.
- ❌ Serve raw event analytics → it serves aggregated magnitudes.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Metric | A counted quantity with a kind + aggregation | `Metric`, `MetricKind`, `Aggregation` |
| Window | The idempotency linchpin — a bounded aggregation interval | `WindowId`, `WindowKey`, `WindowSize`, `WindowAggregator` |
| Observation / delta | A single signal / the folded change to apply | `Observation`, `WindowDelta` |
| Cardinality | Approximate unique count (HLL) | `Cardinality` |
| Popularity score | The published engagement magnitude + weights | `PopularityScore`, `PopularityWeights` |
| Trending | CMS-backed ranked items within a scope | `TrendingItem`, `TrendingScope`, `TrendingQuery` |
| Time-series bucket | A cold-tier TWCS time bucket | `TimeSeriesBucket`, `TimeGranularity` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Metric` | aggregate root | Orthogonal `MetricKind` (exact/approx) × `Aggregation` (sum/cardinality) |
| `WindowAggregator` | aggregate root | N→1 fold; `WindowId` makes flushes idempotent |
| `CounterValue` / `CountSnapshot` / `Cardinality` | VO | A magnitude / point-in-time snapshot / HLL estimate |
| `PopularityScore` / `PopularityWeights` | VO | The published popularity signal |
| `EntityRef` / `EntityId` / `EntityKind` | VO | What is being counted |

> **`WindowId` is the linchpin.** It gates all-tier side effects (the delta ledger), so a replayed
> flush cannot double-count.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth (of *reference*) for:**
- Magnitudes — **Redis** (hot) + **Postgres** (warm SoRef + reconciliation ledger) + **ScyllaDB** (cold TWCS time-series). Reconcilable against the owning edge SoRs.

**This context holds derived copies it does NOT own:**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| Follower/following truth | `social-graph` | reconciliation source (gRPC) | reconciled periodically (drift → `CTR-5002`) |
| Likes (stakes) | `wallet` | `wallet.v1.events` | eventually consistent |

**The "do-not-write" list:** counter never owns *who* — it derives magnitudes and reconciles to the
edge SoRs; it supersedes engagement's *raw* view/share counts only.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | A flush is idempotent per `WindowId` (no double-count on replay) | domain + Pg ledger CTE | `CTR-2xxx` |
| I2 | Magnitudes never claim edge identity ("who") | domain | — |
| I3 | Hot reads fail open (hard timeout → stale/approx) | application | `CTR-4xxx` |
| I4 | Reference values reconcile to the owning SoR; drift alarms | application | `CTR-5002` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Ingest → aggregate → flush.** Signals stream in (`view.v1.events`, `impression.v1.events`,
`click.v1.events`, engagement) → `WindowAggregator` folds N→1 → `DeltaFlusher` writes idempotently
(gated by the `WindowId` ledger) across the 3 tiers → `PopularityPublisher` emits
`counter.v1.popularity`.

**Read (fail-open).** Hot reads hit Redis with a hard `tokio::timeout`; on miss/timeout return a
stale/approximate value rather than erroring.

**Reconcile.** A `Reconciler` periodically heals magnitudes against the owning SoR
(`set_total`/overwrite); divergence raises `CTR-5002`.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| view/impression/click producers | upstream | ACL | `*.v1.events` | counts stop advancing |
| `wallet` | upstream | ACL | `wallet.v1.events` (`stake_committed`) | like magnitudes break |
| `social-graph` | reconcile source | Customer/Supplier | gRPC follower/following | follower-count reconciliation breaks |
| `search` | downstream | Published Language | `counter.v1.popularity` | search's PopularityScore goes stale |
| `realtime` | downstream | Published Language | `counter.v1.popularity` (broadcast) | live counters stop |

> **Anti-Corruption Layer:** the pure `decode` layer maps each upstream signal wire shape into
> `Observation`.

---

## 8. Domain Events (semantics, not wire)

| Event | Means | Emitted when | Who reacts |
|---|---|---|---|
| `counter.v1.popularity` | an entity's popularity magnitude changed | a window flush updates a popularity score | `search` (ranking), `realtime` (live broadcast) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Magnitudes ("how many") are a separate reconcilable SoRef, distinct from edge state ("who"); supersedes engagement's raw counts | [`ADR-0008`](../../../../docs/adr/0008-counter-magnitudes-are-a-reconcilable-soref.md) | Accepted |
| `WindowId`-gated idempotent flush across a 3-tier store | [`ADR-0008`](../../../../docs/adr/0008-counter-magnitudes-are-a-reconcilable-soref.md) | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Supporting — a measurement/reference plane derived from the edge SoRs.
- **Volatility:** medium — new metric kinds and producers are additive.
- **Known modeling debt:** like/share/comment reconcile awaits a like-count RPC (engagement).
- **Deferred capabilities:** upstream view/impression/click producers; the `social-graph.follows` stream; shard-fan-out producer.

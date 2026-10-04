# `moderation` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Trust, Safety & Integrity — the integrity *decision of record* |
> | **Subdomain class** | **Core** — for a UGC network, integrity enforcement is product-differentiating and bespoke; safety *is* part of the product's value |
> | **System of …** | **Record** for "what action was taken against which entity, under which policy version, with what evidence" |
> | **Aggregate root(s)** | `Case`, `Decision`, `EnforcementAction`, `Appeal`, `PenaltyLedger` (`domain::aggregate`) |
> | **Tier** | **TIER-0** |
> | **Failure posture** | **Per-category split** — CSAM/NCII/TVEC `Screen` fails **closed**; everything else is async/fail-open |
> | **Upstream contexts** | `post`, `comment`, `chat`, `media` (content + Screen); classifier services (signals); users (reports) — via **ACL** over Kafka/gRPC |
> | **Downstream contexts** | `timeline`, `chat`, `account` (Plane-B enforcement denorm); `audit` (`decision_recorded` evidence); `account` (gRPC suspension execution) — via **Open-Host Service / Published Language** |
> | **Decision log** | [`ADR-0002`](../../../../docs/adr/0002-moderation-decision-enforcement-sor-with-fail-closed-screen.md) |

---

## 1. Business Capability & Non-Goals

**Capability.** `moderation` is the authority for **integrity decisions and enforcement**: it
answers **"is this actor restricted / this content actioned, by what authority, and can we prove it
to a regulator?"**

**The hard problem.** Moderating at the network's *write volume* without becoming a global latency
bottleneck. A naive design calls a moderation RPC on every post/message/upload, taxing every write
and coupling content availability to an integrity outage. The resolving pattern is a **three-plane
split** that decouples the heavy classification/review path from the hot decision path.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Classify content with ML inline → classifiers are upstream signal *producers*; `Screen` is inference-free hash/blocklist lookup only.
- ❌ Store content → it references content by `SubjectRef`, never holds the bytes (`media`/`post` own those).
- ❌ Be the review UI → the ops console is a separate caller of the case/appeal API.
- ❌ Serve enforcement on the hot read path via RPC → the fleet reads **Plane B** (events + Redis projection), not `GetEnforcementState`.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Subject | The normalized target of moderation: entity type + id + actor + surface | `SubjectRef` |
| Case | A unit of review opened on a subject when signals cross a threshold | `Case` |
| Decision | An append-only, auditable ruling (the legal evidence ledger entry) | `Decision`, `DecisionAuthor` |
| Enforcement action | The applied consequence, versioned monotonically per subject | `EnforcementAction`, `EnforcementVersion` |
| Strike / penalty ledger | The graduated-enforcement history that escalates consequences | `Strike`, `PenaltyLedger`, `PenaltyPolicy` |
| Appeal | A subject's challenge to a decision; resolution is a *new* decision | `Appeal`, `AppealStatus` |
| Signal / report | A classifier verdict / a user abuse report feeding Plane A | `Signal`, `Report` |
| Screen | The narrow, synchronous, fail-closed pre-publish gate (Plane C) | (RPC `Screen`) |
| Policy category / version | The policy under which a decision was made; pinned for auditability | `PolicyCategory`, `PolicyVersion` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Case` | aggregate root | A subject under review; keyed by deterministic UUIDv5 of subject identity (idempotent open) |
| `Decision` | aggregate root | Append-only ruling; a reversal is a *new* decision, never a mutation |
| `EnforcementAction` | aggregate root | Carries a per-subject monotonic `EnforcementVersion` — a reversal can never race ahead of a re-application |
| `Appeal` | aggregate root | Challenge lifecycle; resolution emits a new `Decision` |
| `PenaltyLedger` | aggregate root | Graduated escalation state per actor |
| `SubjectRef` / `EntityType` / `ActorId` | VO | The normalized integrity target — never vendor or content-internal fields |
| `PolicyCategory` / `PolicyVersion` / `Confidence` | VO | The decision's policy basis and certainty |

**Lifecycle (case → enforcement):**

```
signals/report --(threshold)--> Case opened --(review/decide)--> Decision recorded --> EnforcementAction applied (v+1)
                                                     ▲                                          │
                                                 Appeal filed ◄───────────── (reversal = new Decision) ──┘
```

> **Legal transitions only.** Decisions are never retro-mutated (append-only ledger); a reversal is
> a new `Decision`. The `EnforcementVersion` and the `Screen` hash scheme must never change after
> data exists.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Cases, decisions (WORM), appeals, penalty ledger, policy versions, and each reporter's record of their reports (`reports`) — **Postgres** db `moderation`. The decision ledger is append-only.
- Signal / evidence history — **ScyllaDB** keyspace `moderation` (TWCS).
- The enforcement projection + Screen hash corpus — **Redis** (derived/rebuildable).

**This context holds copies it does NOT own (read-model / denormalization):**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| Content existence / metadata to build a `SubjectRef` | `post`, `comment`, `chat`, `media` | `post.v1.events`, `comment.*`, chat content (Plane A) | post-hoc (optimistic publish) |
| Classifier verdicts | classifier services | `moderation.signals` | best-effort |

**The "do-not-write" list:** moderation never mutates content (it references it); account
suspension is *executed* by `account` over gRPC — moderation records the decision and requests
execution, it does not own account state.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Decisions are append-only — a reversal is a new decision | domain + Postgres | `MOD-2xxx` |
| I2 | `Screen` is deterministic and inference-free (hash/blocklist only; ML never inline) | infrastructure boundary | — |
| I3 | Per-category fail policy — CSAM/NCII/TVEC fail **closed**; spam/borderline fail **open** | application | `MOD-7002`/`MOD-7003` |
| I4 | `EnforcementAction` version is monotonic per subject (reversal can't race re-application) | domain | concurrency `MOD-9xxx` |
| I5 | Cases are idempotent — keyed by deterministic UUIDv5 of subject identity | domain (consumer) | dedup folds into `Ok` |
| I6 | Mutating ops RPCs are reviewer-privileged — not self-authorized | deployment edge (auth-context) | `PERMISSION_DENIED` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Plane A — async ingestion (off the write path, fail-open).**
1. Content `*.created` / reports / classifier signals arrive on Kafka.
2. `moderation-ingestion-consumer` runs cheap deterministic checks (blocklist, known-bad hash, actor history).
3. Fan-out to async classifiers; open a `Case` when signals cross a threshold.
- **Idempotency:** Cases keyed by UUIDv5 of subject; intentional skips (block-gated, self-target, dedup) fold into `Ok`.

**Decision → enforcement (graduated).** A `Decision` is recorded under a pinned `PolicyVersion`; the penalty engine escalates via the `PenaltyLedger`; an `EnforcementAction` is applied at version *v+1* and projected to Redis + emitted on `moderation.v1.events`.

**Plane B — enforcement state (hot read, O(1)).** The fleet reads `mod:enf:{actor:<id>}` from the Redis projection rebuilt by consumers — never a per-item RPC.

**Plane C — Screen gate (sync, fail-closed).** `media`/`post` call `Screen` pre-publish for catastrophic categories. A hard timeout (`MODERATION_SCREEN_TIMEOUT_MS`, default 200ms) + circuit breaker bound it; on elapse/outage the gate returns `MOD-7002` and the caller blocks the upload.

**Appeal.** A subject files an appeal; resolution records a new `Decision` (possibly a reversal) and a corresponding enforcement reversal.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| `post`/`comment`/`chat`/`media` | upstream | ACL | content `*.events` → `SubjectRef` (Plane A) | a content schema change breaks subject construction |
| classifier services | upstream | ACL | `moderation.signals` | lost ML signals → engine degrades to deterministic rules |
| `media`/`post` | downstream | OHS (sync) | `Screen` RPC | a `Screen` contract change breaks the publish gate |
| `timeline`/`chat`/`account` | downstream | Published Language | `moderation.v1.events` (Plane B) | enforcement denorm breaks |
| `audit` | downstream | Published Language | `moderation.v1.events` · `decision_recorded` | the compliance evidence trail breaks |
| `account` | downstream | Customer/Supplier (gRPC) | suspension/ban execution | lifecycle actions can't apply (decision still recorded, retried) |

> **Anti-Corruption Layer:** the Plane-A ingestion consumers translate each content/signal wire
> shape into the normalized `SubjectRef` / `Signal` domain types — vendor and content-internal
> fields never leak into the model.

---

## 8. Domain Events (semantics, not wire)

> Meaning only; wire schema is owned by the proto/README. Roll-up in `docs/domain/EVENT_CATALOG.md`.

| Event (on `moderation.v1.events`) | Means | Emitted when | Who reacts |
|---|---|---|---|
| `decision_recorded` | An authoritative integrity ruling was made — carries *who decided* and *why* (DSA statement-of-reasons) | a decision is recorded (auto-screen, human review, appeal reversal) | `audit` — seals the rationale into a crypto-shreddable envelope |
| `enforcement_applied` / `enforcement_reversed` | A consequence was applied / lifted against an actor (versioned) | enforcement action commits | `timeline`, `chat`, `account` — Plane-B denorm |
| `case_opened` / `case_resolved` | A review unit was opened / closed | ingestion threshold / reviewer action | Plane-B consumers |
| `appeal_resolved` | An appeal was decided | appeal resolution | Plane-B consumers |

> All events are keyed by `actor_id` for per-actor ordering. `decision_recorded` is the dedicated
> compliance-evidence variant; offender-centric consumers ignore it, `audit` consumes only it +
> `enforcement_applied`.

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Moderation is the decision/enforcement SoR with a narrow fail-closed Screen gate (three-plane split) | [`ADR-0002`](../../../../docs/adr/0002-moderation-decision-enforcement-sor-with-fail-closed-screen.md) | Accepted |
| `decision_recorded` as the audit-facing evidence event (vs. mapping offender-centric events) | _see ADR-0001 context_ | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Core — trust & safety is product-differentiating for a UGC platform; the three-plane design and graduated enforcement are bespoke.
- **Volatility:** medium — policy categories, the penalty engine, and classifier integration evolve with product and regulation; the *ledger* and version discipline are stable.
- **Known modeling debt:** gateway authorization for mutating ops RPCs is a deployment-time `<TODO>` (the service does not self-authorize).
- **Deferred capabilities:** richer DSA transparency reporting; `chat` content ingestion depth; cross-surface actor reputation.

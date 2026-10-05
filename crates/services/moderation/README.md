# `moderation` — Decide, enforce, and prove integrity actions without taxing every write in the network

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — legal/compliance system of record; the `Screen` gate is in the synchronous publish path for catastrophic-harm categories |
> | **Deployable** | `crates/apps/moderation-server` (library crate: `crates/services/moderation`) |
> | **Datastores** | Postgres db `moderation` (decision/case SoR) · ScyllaDB keyspace `moderation` (signal/evidence history) · Redis Cluster (enforcement projection + Screen hash corpus) |
> | **Async** | publishes `moderation.v1.events` · consumes `post.v1.events`, `comment.*`, chat content, `moderation.reports`, `moderation.signals` (Kafka) |
> | **Upstream callers** | `media`, `post` (Screen gate); internal ops console (case/queue/appeal API) |
> | **Downstream deps** | `account` (gRPC — suspension execution), classifier services (signals), Postgres, ScyllaDB, Redis, Kafka |
> | **SLO** | `<TODO>` avail · `Screen` p99 `< <TODO> ms` · async review lag `< <TODO> s` |

---

## 🎯 Overview & Service Role

`moderation` is the **trust, safety & compliance** microservice: it owns the *integrity decision of record* — what action was taken against which entity, under which policy version, with what evidence — and is the authoritative, auditable source of that fact for the rest of the fleet and for regulators.

The hard problem it solves is **doing moderation at the network's write-volume without becoming a global latency bottleneck**. A naive design calls a moderation RPC on every post, message, and upload; that taxes every write and couples content availability to an integrity outage. The resolving pattern is a **three-plane split**: the heavy classification/review path is decoupled from the hot decision path.

**Core objectives:** (1) content is reviewed **post-hoc and asynchronously** by default — zero added latency on the user's write path; (2) the hot *read* path reads **denormalized enforcement state**, never a per-item moderation RPC; (3) only a narrow, **fail-closed** synchronous `Screen` gate guards catastrophic-harm categories; (4) every enforcement is an **immutable, auditable** record sufficient for DSA / NCMEC / law-enforcement obligations.

| Plane | Path | Latency contract | Used for |
|---|---|---|---|
| **A — Ingestion** | async Kafka consumers | none (off the write path) | ~99% of content: optimistic publish, post-hoc review |
| **B — Enforcement state** | events + Redis projection | local / O(1) | "is this actor restricted / content hidden" on the hot read path |
| **C — Screen gate** | synchronous gRPC | bounded, hard-timeout | CSAM / NCII / TVEC only — deterministic hash lookup, fail-closed |

---

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS where it fits, a **three-store split**, Kafka for ingestion and enforcement events.

```
                         user reports          classifier signals
                              │                        │
  post/comment/chat/media     ▼                        ▼
  *.created  ─────────►  moderation.reports     moderation.signals
       │                      │                        │
       ▼                      ▼                        ▼
  ┌──────────────── Plane A: ingestion consumers (run_consumer) ───────────────┐
  │  cheap deterministic checks (blocklist · known-bad hash · actor history)    │
  │  → fan-out to async classifiers → open Case on threshold                    │
  └───────────────┬───────────────────────────────────────────────┬───────────┘
                  ▼                                                 ▼
        graduated-enforcement engine                       Scylla: signal /
        (PenaltyLedger · policy version)                   evidence history (TWCS)
                  │
                  ▼
        Postgres SoR: cases · decisions(WORM) · appeals · penalty_ledger · policy_versions
                  │
   ┌──────────────┼───────────────────────────────────────────────┐
   ▼              ▼                                                 ▼
 moderation.v1.events            Redis projection                Plane C: Screen
 (Plane B denorm:            mod:enf:{actor:<id>}  ◄── hot-read   (hash corpus / bloom,
  timeline/chat/account)     synchronous O(1) checks              fail-closed gate)
```

**Enforcement consistency.** Each `EnforcementAction` carries a **monotonic version per subject**, so a reversal can never race ahead of a re-application (the same generation discipline `auth` uses for sessions). Enforcement events are **keyed by `actor_id`** for per-actor ordering.

> **Invariants** (and where enforced):
> - **Decisions are append-only** (`decisions` is the legal evidence ledger; a reversal is a *new* decision, never a mutation) — domain + Postgres.
> - **Screen is deterministic and inference-free** — only hash/blocklist lookups; ML never runs inline (infrastructure boundary).
> - **Per-category fail policy** — CSAM/NCII/TVEC fail **closed** (block on uncertainty or gate outage); spam/borderline fail **open** (stays up, reviewed async) — application layer.
> - **Domain idempotency** — Cases keyed by deterministic UUIDv5 of subject identity; redelivery is real (consumer standard).

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| `Screen` availability (Plane C) | `<TODO 99.95%>` | 30d rolling | `<metric>` |
| `Screen` latency p99 | `< <TODO> ms` (hash-lookup only) | 1h | `<metric>` |
| Ingestion review lag (Plane A) | `< <TODO> s` | live | `moderation-ingestion-consumer` lag |
| Enforcement propagation (decision → Plane B visible) | `< <TODO> s` | 1h | `<metric>` |
| Decision-ledger durability | no acked decision lost | — | Postgres synchronous commit |

**Error budget:** `<TODO>`. **On burn:** `<TODO: freeze rollout | page>`. **Special rule:** a sustained `Screen`/`HashCorpus` outage is a **fail-closed** event — uploads in catastrophic categories are blocked, not degraded; page immediately.

---

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

**Downstream — what `moderation` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Postgres | decision/case SoR | writes to the ledger fail | **Hard** — `UNAVAILABLE` for case/decision/appeal RPCs |
| ScyllaDB | signal/evidence history | history writes fail | **Soft** — decisions still recorded; history backfilled |
| Redis | enforcement projection + Screen corpus | Plane B/C reads fail | **Hard for Plane C** (fail-closed), **Soft for Plane B** (consumers re-denormalize) |
| Kafka | ingestion + enforcement events | no ingestion / no denorm | **Soft** — backlog drains on recovery; content already live |
| `account` (gRPC) | suspension/ban execution | lifecycle actions can't apply | **Soft** — decision recorded, enforcement retried |
| classifier services | ML signals | no ML signals | **Soft** — engine runs on deterministic rules only |

**Upstream — who depends on `moderation` (blast radius if it fails):**

| Caller | Uses | User-visible impact if `moderation` is down |
|---|---|---|
| `media`, `post` | `Screen` (Plane C) | catastrophic-category uploads **blocked** (fail-closed); normal content unaffected |
| `timeline`, `chat`, `account` | `moderation.v1.events` (Plane B) | enforcement propagation delayed; already-applied state unchanged |
| ops console | case/queue/appeal API | reviewers cannot triage/decide; backlog grows |

> **Critical path?** **Partially** — only the Plane C `Screen` gate is synchronous (and only for `media`/`post`, only for catastrophic categories). Everything else is async/derived.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### gRPC — `moderation.v1.ModerationService` *(contract lands in Phase 1)*

```protobuf
// Plane C — the narrow, fail-closed pre-publish gate (media/post only).
rpc Screen (ScreenRequest) returns (ScreenResponse);

// Client reports (edge member_or_guest — DSA Art. 16, guests included).
rpc SubmitReport  (..) returns (..);
rpc ListMyReports (..) returns (..);   // the caller's reports + outcome (DSA Art. 16(5))

// Ops console — case / queue / appeal lifecycle.
rpc OpenCase   (..) returns (..);   rpc AssignCase (..) returns (..);
rpc DecideCase (..) returns (..);   rpc ListQueue  (..) returns (..);
rpc FileAppeal (..) returns (..);   rpc ResolveAppeal (..) returns (..);   rpc ListMyAppeals (..) returns (..);

// Compliance / back-office.
rpc GetStatementOfReasons (..) returns (..);   // DSA SoR; edge: the sanctioned account only
rpc GetEnforcementState   (..) returns (..);   // edge: own state only; DISCOURAGED on hot path
```

> **Wire / contract rule:** the surface exposes only normalized integrity types — `SubjectRef` (entity_type + entity_id + actor_id + surface), policy category, action type, case/appeal ids — never classifier-vendor or content-internal fields.
>
> **Hot-path rule:** the fleet reads enforcement via **Plane B** (events + Redis projection), **not** `GetEnforcementState`. The RPC exists for back-office/cold reads and for the account's own settings screen.
>
> **The sanctioned account (DSA Art. 17, 20).** On the edge (`authenticated`), `GetEnforcementState` binds `actor_id` to
> the token (`require_account`); each `EnforcementView` carries its `decision_id`, the key for
> `GetStatementOfReasons` and `FileAppeal`. Both answer `NOT_FOUND` for a decision about another account — on the edge
> for the statement, everywhere for an appeal (only the sanctioned account appeals). Mesh reads of a statement stay
> unfiltered (back-office).
>
> **Following an appeal (DSA Art. 20).** A decision is appealable for **183 days** (Art. 20(1): six months at least):
> the statement carries `appealable_until` (unset for a non-appealable category), and `FileAppeal` past it answers
> `MOD-5003` (`FAILED_PRECONDITION`). There is **one appeal per decision and appellant** (unique `(decision_id,
> actor_id)`): filing again — or concurrently — returns the stored appeal, whatever its state. The appellant follows
> it (Art. 20(4)–(5)): their statement carries `appeal` (their appeal of that decision), and `ListMyAppeals` (edge
> `authenticated`, the appellant is the token, newest first, paged like `ListMyReports`) returns each appeal's
> `status` and, once resolved, `resolved_at` and the reviewer's reasons (`outcome`, from `ResolveAppeal.rationale`,
> required). The outcome is also **notified** (#744): `appeal_resolved` names the appellant account's profiles
> (`profile_ids`, from profile's `ListProfilesByAccount`, deleted ones left out), and `notification` tells each of
> them. They are read before anything is written, so an unreachable profile directory makes `ResolveAppeal` answer
> `MOD-8004` (`UNAVAILABLE`, retry) with nothing resolved.
>
> **A reporter's appeal (DSA Art. 20(1)).** The notifier is a complainant too: a **member** who reported the content
> before the decision may appeal it (`FileAppeal` with their own account as `actor_id`; checked against `reports`).
> Their report leads to the decision: `ReportView.decision_id`, the latest decision on its subject since the report,
> once its case is decided (coarse: still not which sanction). Their appeal is its own (`by_reporter`), beside the
> sanctioned account's, with the same window, deduplication and reads. It never reverses anything: upheld, the decision
> stands; **overturned, the case goes back to review** (`triaged`, its report `UNDER_REVIEW` again) for a new
> decision, the earlier one and its enforcement standing until then. Guests' appeals wait for the guest-mode question
> (#681).
>
> **Client reports (`SubmitReport`).** Anyone may report (DSA Art. 16), so the RPC is open to every client
> session, member **or guest** (edge `member_or_guest`). The **reporter** is the verified token (a member's account,
> or `guest:<id>`), never a request field; a mesh call has no reporter and is refused (`MOD-6007`). The client sends
> only the content (`POST` / `COMMENT` / `PROFILE` + id): the **account** a decision would penalise is resolved
> server-side (post / comment → author profile → owning account, over the mesh), so a reporter can never point a
> sanction at someone else. Quota per reporter: `MODERATION_REPORTS_PER_HOUR` / `_PER_DAY` (20 / 100) in Redis,
> **fail-open** if Redis is down (reporting is a right; the queue absorbs it). The report then feeds the subject's
> case like a pipeline report; a guest's is a weaker signal (`guest_report`, 0.3 vs 0.5). `OpenCase` stays
> mesh-only (it is the reviewer console's). **Flood bounds** (many reporters, e.g. guest sessions): a case keeps at
> most `MAX_SIGNALS_PER_SOURCE` (25) signals per source — further reports are still recorded for their reporters
> but add nothing to the case — and a subject is sent to classification at most once per
> `MODERATION_CLASSIFICATION_DEBOUNCE_SECS` (600, Redis `SET NX`, fail-open).
>
> **The reporter's view (`ListMyReports`, DSA Art. 16(5)).** Each report is also recorded for its reporter (`reports`
> table, one row per reporter × subject, keyed by the id `SubmitReport` returns). `ListMyReports` (edge
> `member_or_guest`, reporter = the token, keyset-paged newest first) joins each report to its case: open / triaged ⇒
> `UNDER_REVIEW`, actioned / appealed ⇒ `ACTION_TAKEN`, dismissed (incl. overturned on appeal) ⇒ `NO_VIOLATION`.
> Coarse on purpose: a reporter never learns which sanction hit the reported account. Member and guest ids are
> kept apart (`reporter_kind`).
>
> **Authorization (deployment requirement):** the mutating ops RPCs (`DecideCase`, `AssignCase`, `OpenCase`, `ResolveAppeal`) are **privileged** — they ban/suspend/remove. The service does not self-authorize the caller; mutating RPCs **must** be restricted to authenticated reviewer principals at the edge (gateway authz / `auth-context` permission gate, e.g. `moderation:decide`) before exposure. `Screen`, `SubmitReport`, `ListMyReports`, `FileAppeal`, `ListMyAppeals`, `GetStatementOfReasons` and `GetEnforcementState` are caller-facing; the rest are reviewer-only.

### Rust ports (hexagonal contract) *(Phase 3)*

`SignalSource` · `Case/Decision/Penalty/Appeal Repository` · `EnforcementProjection` (Redis) · `HashCorpus` (Screen) · `ClassifierGateway` · `AccountDirectory` (gRPC) · `EventPublisher` — AFIT, no `async_trait`, in-memory fakes for the application tier.

### Error contract

Every fault implements `error::AppError` with a stable `MOD-XXXX` code (see [`src/error.rs`](src/error.rs)):

| Range | Class |
|---|---|
| `MOD-1xxx` | case lifecycle |
| `MOD-2xxx` | decision ledger (append-only) |
| `MOD-3xxx` | enforcement action |
| `MOD-4xxx` | penalty / strikes / policy |
| `MOD-5xxx` | appeal |
| `MOD-6xxx` | report intake |
| `MOD-7xxx` | Screen gate / hash corpus (Plane C) |
| `MOD-8xxx` | external integrity deps (classifiers / account directory) |
| `MOD-9xxx` | cross-cutting (domain/parse · concurrency · event publish) |

`DB-*` / `SDB-*` / `RDS-*` / `VAL-*` are delegated from the storage and validation crates.

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

> Kafka topics are an API. A schema change here breaks consumers exactly like a proto change.

**Publishes:**

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `moderation.v1.events` | enforcement applied/reversed, case opened/resolved, appeal resolved | `actor_id` | `timeline`, `chat`, `account` (Plane B denorm) |
| `moderation.v1.events` · `decision_recorded` | a decision is recorded (automated screen, human review, appeal reversal) | `actor_id` | `audit` (compliance evidence) |

> The **`decision_recorded`** event is the dedicated compliance-evidence record the `audit` plane consumes — unlike the offender-centric Plane-B events above, it carries *who decided* (the authority) and *why* (the rationale / DSA statement-of-reasons), sourced from the immutable `Decision` ledger. The rationale is sealed into a crypto-shreddable envelope by `audit` at ingest; by convention it is policy-referential, not content-quoting. Other consumers ignore this variant.

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `post.v1.events` / `comment.*` / chat content | `moderation-ingestion-consumer` | Plane A: build subject, screen cheaply, open cases | DLQ `<topic>.dlq` |
| `moderation.reports` | `moderation-report-consumer` | user abuse reports (dedup → case) | DLQ `moderation.reports.dlq` |
| `moderation.signals` | `moderation-signal-consumer` | classifier verdicts → graduated engine | DLQ `moderation.signals.dlq` |

> **Runtime contract (mandatory):** all consumers run under `run_consumer` — manual commit after a terminal outcome, bounded retry with backoff + jitter, DLQ on exhaustion/poison, rebuild-from-last-committed-offset on broker error. **Idempotency:** Cases keyed by deterministic UUIDv5 of subject identity; intentional skips (block-gated, self-target, dedup) fold into `Ok` so they commit rather than flood the DLQ.

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis / hash corpus down | `Screen` returns `MOD-7002/7003` | **Fail-closed** for CSAM/NCII/TVEC → callers block the upload | Page; restore corpus; uploads resume |
| Postgres down | case/decision RPCs `UNAVAILABLE` | Ledger writes fail; **no silent decisions** | Page; failover; ingestion backlog drains after |
| Kafka lag (ingestion) | review delayed | content already live (optimistic); cases open late | Check broker / classifier; lag self-drains |
| `account` gRPC down | suspensions not applied | decision recorded; enforcement retried | Check `account`; retries converge |
| classifier down | fewer signals | engine runs on **deterministic rules only** | Soft; investigate classifier |

**Backpressure & limits:** the `Screen` gate has a **hard timeout + circuit breaker** (Phase 7) so a moderation outage can't wedge `media`/`post`; ingestion sheds via consumer lag, not by dropping; queue depth is the load signal for human-review capacity.

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
moderation = { path = "crates/services/moderation" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as `moderation::service::ModerationService` *(Phase 5)* — `build` wires adapters and self-spawns the ingestion/report/signal consumers, `register` adds the gRPC services, `health_probes` exposes liveness over Postgres + Scylla + Redis. Telemetry, config + hot-reload, ingress rate-limiting, health, and graceful shutdown are owned by the runtime.

### Bootstrap (`crates/apps/moderation-server`) *(Phase 5 shape)*

```rust
use moderation::service::ModerationService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("MODERATION_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50061".to_owned()).parse()?;
    service_runtime::serve::<ModerationService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `moderation`-specific variables *(grow per phase)*

| Variable | Required | Default | Description |
|---|---|---|---|
| `MODERATION_GRPC_ADDR` | No | `0.0.0.0:50061` | gRPC bind address |
| `MODERATION_ACCOUNT_GRPC_ENDPOINT` | No | `http://localhost:50059` | `account` service endpoint (suspension execution) |
| `MODERATION_POST_GRPC_ENDPOINT` · `MODERATION_COMMENT_GRPC_ENDPOINT` · `MODERATION_PROFILE_GRPC_ENDPOINT` | **Yes** (prod) | `:50056` · `:50057` · `:50052` on localhost | report subject resolution (whose content a client report targets) |
| `MODERATION_CONTENT_RPC_TIMEOUT_MS` · `MODERATION_CONTENT_CONNECT_TIMEOUT_MS` | No | `1000` · `1000` | deadlines on those calls |
| `MODERATION_REPORTS_PER_HOUR` · `MODERATION_REPORTS_PER_DAY` | No | `20` · `100` | client report quota per reporter (member or guest) |
| `MODERATION_CLASSIFICATION_DEBOUNCE_SECS` | No | `600` | at most one classification request per subject per window |
| `MODERATION_ACCOUNT_RPC_TIMEOUT_MS` | No | `2000` | per-request deadline on `account` RPCs (tonic has no default timeout) |
| `MODERATION_ACCOUNT_CONNECT_TIMEOUT_MS` | No | `2000` | connect deadline when dialing the `account` channel |
| `MODERATION_SCREEN_TIMEOUT_MS` | No | `200` | hard timeout for the Plane C gate; on elapse the gate returns `MOD-7002` and the caller fails closed for catastrophic categories |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_*` | **Yes** | — | decision/case SoR connection |
| `SCYLLA_*` | **Yes** | — | signal/evidence history connection |
| `REDIS_HOSTS` | **Yes** | — | enforcement projection + Screen corpus |
| `KAFKA_BROKERS` | **Yes** | — | ingestion + enforcement events |

> Full connection/timeout/reconnect tuning lives in the shared storage/transport crates.

### Compile-time features
- `integration-moderation` *(Phase 6)* — gates the container-backed integration suite.
- `build.rs` compiles `moderation.v1` and emits the reflection descriptor set *(Phase 1)*.

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

- **Migrations:** `crates/services/moderation/migrations/*.{sql,cql}` (Postgres SoR + Scylla history) *(Phase 4)*. Apply **before** rolling the new binary, via the `migrator` init container.
- **Rollout:** rolling; risky policy-engine changes gate behind a pinned **policy version** (a decision records the version it was made under, so rollouts are auditable and reversible).
- **Rollback:** binary rollback is safe; the decision ledger is append-only and forward-compatible. **Never** retro-mutate a decision — record a reversal.
- **Stateful gotchas:** the **Screen hash scheme** and the **subject-version** monotonicity must never change after data exists.

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

- **Runtime:** multi-threaded Tokio (ingestion/report/signal consumers + gRPC). Global tracing/OTel subscriber installed before `serve`; W3C trace-context propagated across the Kafka boundary.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `Screen` p99 + error rate | catastrophic-harm gate health | breach ⇒ page (fail-closed blocks uploads) |
| ingestion consumer lag | post-hoc review latency | sustained growth ⇒ investigate |
| enforcement propagation lag | Plane B denorm freshness | breach ⇒ investigate |
| DLQ produce rate (`*.dlq`) | poison / retry-exhausted | any sustained rate ⇒ page |
| decision write errors | ledger durability | any ⇒ page |

---

## 🛠️ Local Development &nbsp;·&nbsp; CORE

```bash
cargo build  -p moderation && cargo clippy -p moderation --all-targets
cargo test   -p moderation
# Phase 6+: live integration suite (boots Postgres + Scylla + Redis + Kafka)
# cargo test -p moderation --features integration-moderation
```

> **Build status:** complete through Phase 7 — proto contract, domain, application + ports, infrastructure adapters (Postgres/Scylla/Redis/Kafka), runtime wiring + self-spawned ingestion consumers, a live container-backed integration suite, and hardening (the `Screen` hard timeout). `MOD-XXXX` errors, unit tests, and the `integration-moderation` suite are all green. Org metadata (owner, on-call, SLO numbers) and gateway authorization are deployment-time `<TODO>`s.

---

## 🚨 Troubleshooting & Runbook &nbsp;·&nbsp; CORE

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. `media`/`post` uploads failing with `MOD-7002`/`MOD-7003`.**
Root cause: the `Screen` gate or hash corpus (Redis) is unavailable; for catastrophic-harm categories the caller's policy is **fail-closed**, so uploads are blocked by design. Mitigation: restore the Redis corpus; verify `MODERATION_SCREEN_TIMEOUT_MS`; uploads resume automatically once the gate is healthy.

**2. Content that should be actioned is still visible.**
Root cause: Plane B denormalization lag (enforcement event not yet consumed by `timeline`/`chat`) **or** the ingestion backlog hasn't reached the subject yet (Plane A is post-hoc). Mitigation: check `moderation.v1.events` consumer lag downstream and ingestion consumer lag; confirm the `EnforcementAction` exists in Postgres and its Redis projection key `mod:enf:{actor:<id>}`.

**3. `*.dlq` produce rate climbing.**
Root cause: poison signals/reports or an exhausted dependency. Mitigation: inspect `x-dlq-*` headers; if a dependency outage, fix and replay from DLQ; if genuinely poison, leave parked and triage.

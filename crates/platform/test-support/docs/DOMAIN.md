# `test-support` — Domain & Functional Contract

> The integration-test backbone: containers, migrations, and the anti-flake await. It answers *"what is identical across every service's live suite, so each suite carries only its own scenarios?"*

> **Domain Card**
>
> | | |
> |---|---|
> | **Shared capability** | Backend-agnostic integration-test scaffolding: container orchestration, migration runners, and the `await_until` synchronisation primitive |
> | **Layer** | `platform` — **dev-only**; never linked into a service binary |
> | **Subdomain class** | **Supporting** — test infrastructure; leverage is consistency + the anti-flake discipline |
> | **Primary abstraction(s)** | `containers::*` + `migrate::*` + `await_until` (`test_support`) |
> | **Footprint** | dev-only — a `[dev-dependency]`; boots Docker containers; requires a running Docker daemon |
> | **Failure posture** | N/A — test scaffolding; correctness = no flakes, not runtime resilience |
> | **Depends on** | `testcontainers(-modules)`, `rdkafka`, `tokio`, `scylla(-storage)`, `sqlx`, `tracing` |
> | **Consumed by** | every service's live suite (`tests/<svc>_it/`), as a `[dev-dependency]` |
> | **Decision log** | none — rationale in [`README §Architecture`](../README.md) |

---

## 1. Technical Capability & Non-Goals &nbsp;·&nbsp; CORE

**Capability.** `test-support` is the fleet's authority for **live-test infrastructure**: it answers
**"how does every service boot its real backends (Scylla/Redis/Kafka/Postgres/MinIO), apply migrations once,
and synchronise assertions without sleeping?"** — so each service's suite carries only its composition-root
graph and its scenarios.

**The hard problem.** Live integration suites are flaky and slow when each re-invents container boot, port
assignment, migration application, and (worst) fixed `sleep`s racing slow containers. `test-support` extracts
the five pillars from the `chat` gold-standard suite so the *identical* parts are written once and the anti-flake
discipline (`await_until`, never `sleep`) is enforced by the shared primitive.

**Non-goals — what this crate deliberately does NOT do:**
- ❌ Be linked into a service binary → it is dev-only; that would drag test scaffolding into production.
- ❌ Own test logic / scenarios → those live in each service's `tests/<svc>_it/`.
- ❌ Provide isolation by teardown → isolation is by namespacing (fresh UUID keys), a per-harness discipline.

---

## 2. Ubiquitous Language &nbsp;·&nbsp; CORE

| Term | Meaning in this crate | Code symbol |
|---|---|---|
| Ready entry point | Boot the container (once) + apply migrations (once), return the endpoint | `scylla_ready`, `postgres_ready` |
| Migration runner | Idempotent DDL apply with single-node adaptation | `scylla_apply`, `postgres_apply` |
| The await primitive | Poll observable state against a deadline — never `sleep` | `await_until` |
| Namespacing | Fresh UUID keys per scenario for parallel isolation | (discipline in each harness) |

---

## 3. Public Model & Contract Surface &nbsp;·&nbsp; CORE

| Element | Kind | Contract / invariant boundary it guards |
|---|---|---|
| `containers::*_ready` | lazy boot+migrate | `OnceCell`-backed; one container set per test binary; OS-mapped ports |
| `containers::ensure_topics` | Kafka helper | Explicit topic pre-creation (no auto-create races) |
| `migrate::*_apply` | idempotent runner | ScyllaDB `SimpleStrategy RF=1` / raw-SQL Postgres, applied once |
| `await_until(label, deadline, probe)` | sync primitive | THE anti-flake rule — poll, never fixed-sleep |

---

## 4. Ownership & Architectural Boundaries &nbsp;·&nbsp; CORE

**This crate owns:**
- The parts identical across every service's live suite: container orchestration, migration runners, and the
  synchronisation primitive.

**This crate deliberately does NOT own / must NOT link:**

| Concern | Lives in | Why the edge points that way |
|---|---|---|
| Scenarios + the composition-root graph | each service's `tests/<svc>_it/` | Service-specific; not identical |
| The namespacing discipline | each harness | The crate provides shared infra; isolation is a per-scenario rule |
| Any production code path | — | It is dev-only; never under `[dependencies]` |

**The "do-not-depend-on" list:** it must never appear under a service's `[dependencies]` — only
`[dev-dependencies]`. Linking it into a binary drags `testcontainers`/`rdkafka` into production.

---

## 5. Invariants & Contract Rules &nbsp;·&nbsp; CORE

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Dev-only — never linked into a service binary | dependency placement (`[dev-dependencies]`) | test scaffolding leaks into prod |
| I2 | One container set per test binary; backends boot lazily once | `OnceCell` | port conflicts / wasted boots |
| I3 | Every endpoint uses the OS-assigned mapped host port | `containers` | port collisions across parallel suites |
| I4 | Migrations applied exactly once (single-node adaptation) | `OnceCell` + runners | replication errors on one node |
| I5 | Zero fixed sleeps — synchronisation is `await_until` only | the primitive (discipline) | flaky CI |

---

## 6. Control Flow & Lifecycle &nbsp;·&nbsp; DEEP

**Per test binary.** A scenario calls e.g. `containers::scylla_ready("chat", "migrations")`: the backend boots
lazily through a `OnceCell` (shared by every scenario in the binary), migrations apply once (single-node
adaptation), and the OS-mapped endpoint is returned. The harness then drives the service's `App::build` against
that endpoint, runs a scenario (minting fresh UUID keys for isolation), and asserts with
`await_until(label, deadline, probe)` — polling observable state until true or the deadline, never sleeping.

---

## 7. Crate Coupling (dependency-graph slice) &nbsp;·&nbsp; DEEP

| Neighbour crate | Direction | Pattern | Mechanism | What breaks if it changes |
|---|---|---|---|---|
| `scylla-storage` / `sqlx` | upstream | Conformist | migration runners | live-test schema setup |
| `testcontainers(-modules)` | upstream | Conformist | container boot + mapped ports | the whole orchestration |
| every service's live suite | downstream | Published Contract | `*_ready` + `await_until` | every integration suite |

> **Stability seam:** `await_until` and the `*_ready` entry points are the shared contract every suite builds
> on; the dev-only placement is itself an enforced architectural rule.

---

## 8. Emitted Signals & Side-Effects &nbsp;·&nbsp; DEEP

N/A as a production signal — it emits only test-time `tracing`. Side effects (test-time): starts Docker
containers (labelled `core-platform.test-support.pid=<PID>`, removed when the test process exits or is
interrupted), applies migrations, creates Kafka topics. None of this reaches a deployed binary.

---

## 9. Decisions & Rationale &nbsp;·&nbsp; DEEP

| Decision | Where recorded | Status |
|---|---|---|
| Extract the five identical pillars from the `chat` gold standard | [`README §Architecture`](../README.md) | Accepted |
| `await_until` as the single synchronisation primitive (anti-flake) | [`README §Architecture`](../README.md) | Accepted |
| Isolation by namespacing, not teardown (parallel suites on shared containers) | [`README §Architecture`](../README.md) | Accepted |
| Dev-only — never a production dependency | [`README §Architecture`](../README.md) | Accepted |

---

## 10. Classification & Evolution &nbsp;·&nbsp; DEEP

- **Classification:** Supporting — test infrastructure; leverage is uniformity + the enforced anti-flake rule.
- **Stability:** stable contract — the five pillars are settled.
- **Volatility:** low — new backends are added as new `testcontainers-modules` features + a `*_ready` entry.
- **Deferred capabilities:** none; new backend modules are additive.

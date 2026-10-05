# `test-support` — Shared integration-test scaffolding: containers, migrations, and the anti-flake await

> **Crate Card**
>
> | | |
> |---|---|
> | **Role** | `platform` — **dev-only** test backbone (never linked into a service binary) |
> | **Package** | `test-support` (dir: `crates/platform/test-support`) |
> | **Consumed by** | every service's live integration suite (`tests/<svc>_it/`), as a `[dev-dependency]` |
> | **Depends on** | `testcontainers(-modules)`, `rdkafka`, `tokio`, `scylla(-storage)`, `sqlx`, `tracing`, `libc` + `signal-hook` (container reaper) |
> | **Stability** | stable contract |
> | **Feature flags** | none |
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |

---

## 🎯 Overview & role

`test-support` is the backend-agnostic backbone every service's live test suite is built on. It owns
the parts that are *identical* across services — container orchestration, migration runners, and the
synchronization primitive — so each service's `tests/<svc>_it/` carries only what is irreducibly
service-specific (its composition-root graph and its scenarios).

**Architectural boundary** — **dev-only**: it is a `[dev-dependency]` and must **never** be linked into
a service binary. It provides infrastructure, not test logic; the namespacing discipline that gives
parallel isolation lives in each service's harness, not here.

---

## 📐 Architecture & key decisions

The six pillars (extracted from the `chat` gold-standard suite):

- **One container set per test binary** — each backend boots lazily through a `tokio::sync::OnceCell`
  and is shared by every scenario in the binary; Kafka/Postgres boot only when a scenario first asks.
  The statics are never dropped, so a reaper removes every container labelled with the process's PID
  (`core-platform.test-support.pid`) at normal exit (`atexit`) and on SIGINT/SIGTERM/SIGQUIT — the set
  lives exactly as long as the test binary.
- **Zero port conflicts** — every endpoint is resolved from the **OS-assigned mapped host port**;
  nothing is statically bound, so suites run concurrently.
- **Migrations applied exactly once** — behind a `OnceCell`, with the single-node replication
  adaptation (ScyllaDB `SimpleStrategy RF=1`) or a raw-SQL runner (Postgres).
- **Zero fixed sleeps** — `await_until` is the *single* synchronization primitive: assertions poll
  observable state against a deadline, never `sleep` a fixed amount. This is the anti-flake rule.
- **Isolation by namespacing, not teardown** — scenarios mint fresh UUID keys so the suite runs in
  parallel against the shared containers (the discipline lives in each harness; this crate provides the
  infra).
- **Registry hiccups don't fail a suite** — before each boot, `ensure_image` checks the local image
  store and, if the image is missing, `docker pull`s it with 4 attempts and linear backoff (a cut
  stream or timeout from Docker Hub used to fail the whole CI job). A cached image costs one local
  `docker image inspect`; a permanent error (unknown tag, denied) is not retried and surfaces through
  testcontainers exactly as before.

---

## 🔌 Public API & contract

```rust
// containers.rs — lazy, OnceCell-backed, OS-mapped-port endpoints
pub async fn scylla_contact_point() -> String;
pub async fn scylla_ready(keyspace: &str, migrations_dir: &str) -> String;   // boot + migrate once
pub async fn redis_endpoint() -> String;
pub async fn kafka_brokers() -> String;
pub async fn ensure_topics(brokers: &str, topics: &[&str]);
pub async fn postgres_ready(migrations_dir: &str) -> String;                 // boot + migrate once
pub async fn ensure_image(descriptor: &str);                                  // pull-if-missing, retried (boot calls it)
pub const OWNER_LABEL: &str = "core-platform.test-support.pid";              // on every container; value = PID

// migrate.rs — idempotent runners (single-node adaptation)
pub async fn scylla_apply(contact_point: &str, keyspace: &str, migrations_dir: &str);
pub async fn postgres_apply(url: &str, migrations_dir: &str);

// wait.rs — THE synchronization primitive
pub async fn await_until<F, Fut>(label: &str, deadline: Duration, probe: F)   // re-exported at crate root
where F: FnMut() -> Fut, Fut: Future<Output = bool>;
```

> **Contract notes:** `scylla_ready`/`postgres_ready` are the canonical entry points — they boot the
> container (once) *and* apply migrations (once) before returning the endpoint. Never add a fixed
> `sleep` in a harness; express the wait as an `await_until` probe over observable state.

---

## 📦 Integration

```toml
[dev-dependencies]                       # dev-only — NEVER a normal dependency
test-support = { workspace = true }
```

```rust
use test_support::{containers, await_until};
use std::time::Duration;

let contact = containers::scylla_ready("chat", "migrations").await;   // boots + migrates once
// ... drive the service's App::build against `contact`, run a scenario, then assert without sleeping:
await_until("message visible to guest", Duration::from_secs(5), || async {
    guest_history(&client).await.len() == 1
}).await;
```

---

## ⚙️ Configuration & feature flags

None — no environment variables and no cargo features. Endpoints are discovered from the booted
containers (OS-mapped ports); the only runtime prerequisites are a **running Docker daemon** and the
**`docker` CLI on `PATH`** (the exit-time reaper and the image pre-pull shell out to it — see Gotcha 5).

---

## 🧪 Testing

```bash
cargo clippy -p test-support --all-targets
# Exercised transitively by each service's suite, e.g.:
cargo test -p chat --features integration        # boots containers via test-support
```

This crate is scaffolding — its own surface is covered through the service suites that consume it.

---

## 🚨 Gotchas / FAQ

> The sharp edges. One entry per real trap.

**1. A build pulled `testcontainers` into a service binary.**
`test-support` is **dev-only** — it must appear under `[dev-dependencies]`, never `[dependencies]`.
Linking it into a service binary drags `testcontainers`/`rdkafka` test scaffolding into production.

**2. Flaky test that passes locally, fails in CI.**
Almost always a fixed `sleep` racing a slow container. Replace it with `await_until(label, deadline,
probe)` polling the actual observable state — that's the entire anti-flake contract.

**3. ScyllaDB migration fails with a replication error on a single node.**
The runners adapt DDL to single-node `SimpleStrategy RF=1`; if you bypass `scylla_apply`/`scylla_ready`
and run raw `NetworkTopologyStrategy` DDL, it won't satisfy RF on one node. Go through the runner.

**4. Two scenarios interfere with each other's data.**
Isolation is by **namespacing, not teardown** — each scenario must mint fresh UUID keys/topics. The
containers are shared across the binary by design; don't rely on a clean slate between scenarios.

**5. Test containers left running / Scylla boots fail with `StartupTimeout`.**
Each run removes its own containers at exit or on Ctrl-C; only a SIGKILL, an abort, or a missing `docker`
CLI (a `test-support:` warning on stderr) leaves them behind. Sweep every leftover from any run with:

```bash
docker rm -f -v $(docker ps -aq --filter label=core-platform.test-support.pid)
```

Scylla runs with `--reactor-backend epoll` (≈1k of the host's 65536 `fs.aio-max-nr` events instead of
≈51k with linux-aio), so several Scylla suites — or worktrees — can run side by side. A container log
saying `Could not setup Async I/O … aio-max-nr` means unlabelled containers from an older checkout are
holding the budget: remove the stale `scylladb/scylla` containers by hand.

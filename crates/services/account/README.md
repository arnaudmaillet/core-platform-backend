# `account` — Private identity lifecycle: the platform's system of record for who a person *is*

> **Service Card**
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — identity is on the auth critical path |
> | **Deployable** | `crates/apps/account-server` (library crate: `crates/services/account`) |
> | **Datastores** | PostgreSQL / CockroachDB-compatible (db `account`) |
> | **Async** | publishes `account.v1.events` (AccountCreated/Activated/Suspended/Deleted/…) · consumes nothing |
> | **Upstream callers** | `<TODO: auth gateway>`, `profile` (via events) |
> | **Downstream deps** | PostgreSQL/CockroachDB |
> | **SLO** | `<TODO: 99.95%>` avail · status-read p99 `<TODO>` · write p99 `<TODO>` |

---

## 🎯 Overview & Service Role

`account` manages the complete **private lifecycle of a physical person** on the platform: identity
verification, credentials, KYC compliance, GDPR rights, and role-based access control. It is the
authoritative system of record for account existence and status — the gateway's auth middleware
resolves every request against it.

The hard problem it solves is **correctness under concurrency and compliance**: account state is a
strict state machine (lifecycle + KYC), every mutation must be serializable against concurrent
writers, and PII handling is legally constrained (GDPR Art. 17 / Art. 20). It resolves this with an
**optimistic-locking aggregate** (compare-and-swap on a version counter) over CockroachDB, and a
domain layer that rejects illegal status transitions outright.

**Core objectives:** never lose a write to a concurrent update; never permit an illegal lifecycle
transition; never store a secret in plaintext. Financial state is explicitly **out of scope** —
owned by the dedicated `ledger` service (SRP at hyperscale).

---

## 📐 Architecture & Concepts

Clean Architecture / DDD (`domain` → `application` → `infrastructure`): the `domain` layer is free of
I/O, `application` holds pure CQRS handlers (17 commands, 5 queries), all I/O lives in
`infrastructure` (Postgres adapter + tonic gRPC).

```
gRPC (tonic) ─► AccountServiceHandler ─► Command/Query bus ─► AccountRepository (port)
                                                                      │
                                                          PostgreSQL / CockroachDB
                                                          (optimistic lock: version CAS)
                  AccountCreated/… ─► account.v1.events (Kafka) ─► profile, …
```

**Optimistic locking.** Every write is `UPDATE accounts SET …, version = version + 1 WHERE id = $1
AND version = $n`. Zero rows affected ⇒ `ConcurrentModification` (retryable, mapped to `ABORTED`).
`AccountId` (UUIDv7) implements `ShardKey`; all writes route through `run_on_shard(&account_id, …)`
for topology-agnostic transaction routing.

> **Invariants** (and where enforced): lifecycle transitions
> (`PendingVerification→Active→Suspended→Active`, `Active→Deactivated→Active` — the holder signs
> back in, `→Deleted`; a suspended account cannot deactivate) and KYC transitions
> (`NotStarted→Submitted→InReview→Approved|Rejected`) are enforced in the `Account` aggregate —
> illegal transitions return `FAILED_PRECONDITION`. Uniqueness on `(identity_id, email)` makes
> `CreateAccount` idempotent.
>
> **Phone-only accounts** (guest-mode sign-up by SMS): an account has an email **or** a phone number
> (or both). The email is optional (`NULL` for a phone-only account; `AccountView.email` is then
> empty, and `account.created` carries no `email`), a phone number belongs to one account at most
> (`ACC-1004`), and `VerifyPhone` activates a `PendingVerification` account the way `VerifyEmail` does.

---

## 📊 Service Level Objectives (SLO)

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-`UNAVAILABLE`) | `<TODO: 99.95%>` | 30d rolling | gRPC status metrics |
| `GetAccountStatus` p99 (auth hot path) | `< <TODO> ms` | 1h | gRPC histogram |
| Write p99 (CAS commit) | `< <TODO> ms` | 1h | Postgres exec histogram |
| Durability | no acked write lost | — | CockroachDB serializable commit |

**Error budget:** `<TODO>`. **On burn:** `<TODO>`. `GetAccountStatus` is the tightest SLI — the auth
gateway calls it on the request path, so its latency is multiplied across the whole fleet.

---

## 🔗 Dependencies & Blast Radius

**Downstream — what `account` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| PostgreSQL / CockroachDB | system of record | all reads + writes fail | **Hard** — `UNAVAILABLE` |
| Kafka | event emission (`account.v1.events`) | downstream projections stall | **Soft** — writes still commit |

**Upstream — who depends on `account` (blast radius if `account` fails):**

| Caller | Uses | User-visible impact if `account` is down |
|---|---|---|
| `<TODO: auth gateway>` | `GetAccountStatus` | **logins/authz fail platform-wide** |
| `profile` | consumes `account.v1.events` | profile masking on suspend/deactivate/delete stops |

> **Critical path?** **Yes** — `GetAccountStatus` is in the synchronous auth path; an account outage
> degrades every authenticated request across the fleet.

---

## 🔌 Public Interfaces & API Contract

### gRPC — `account.v1.AccountService`

```protobuf
service AccountService {
  // Commands (all return CommandResponse { success, account_id })
  rpc CreateAccount (CreateAccountRequest) returns (CommandResponse);
  rpc VerifyEmail (VerifyEmailRequest) returns (CommandResponse);   // mesh only: auth, after a proof (verified id_token or code)
  rpc VerifyPhone (VerifyPhoneRequest) returns (CommandResponse);   // mesh only: auth, after an SMS code
  rpc ChangePassword (ChangePasswordRequest) returns (CommandResponse);
  rpc EnrollMfa (EnrollMfaRequest) returns (CommandResponse);
  rpc RevokeMfa (RevokeMfaRequest) returns (CommandResponse);
  rpc UpdateKycStatus (UpdateKycStatusRequest) returns (CommandResponse);
  rpc SuspendAccount (SuspendAccountRequest) returns (CommandResponse);
  rpc ReactivateAccount (ReactivateAccountRequest) returns (CommandResponse);
  rpc DeactivateAccount (DeactivateAccountRequest) returns (CommandResponse);
  rpc ResumeDeactivatedAccount (ResumeDeactivatedAccountRequest) returns (CommandResponse); // auth, on Login
  rpc RecordLogin (RecordLoginRequest) returns (CommandResponse);
  rpc RecordFailedLogin (RecordFailedLoginRequest) returns (CommandResponse);
  rpc RequestGdprDeletion (RequestGdprDeletionRequest) returns (CommandResponse);
  rpc CancelGdprDeletion (CancelGdprDeletionRequest) returns (CommandResponse);  // within the grace period; signing in cancels too
  rpc AnonymizeAccount (AnonymizeAccountRequest) returns (CommandResponse);
  rpc RequestDataExport (RequestDataExportRequest) returns (CommandResponse);
  rpc AssignRole (AssignRoleRequest) returns (CommandResponse);
  rpc RevokeRole (RevokeRoleRequest) returns (CommandResponse);
  // Queries
  rpc GetAccountById (GetAccountByIdRequest) returns (AccountView);
  rpc GetAccountByIdentityId (GetAccountByIdentityIdRequest) returns (AccountView);
  rpc GetAccountByEmail      (GetAccountByEmailRequest)      returns (AccountView);   // mesh only (auth sign-up): never on the edge
  rpc GetAccountByPhone      (GetAccountByPhoneRequest)      returns (AccountView);   // mesh only (auth phone sign-up)
  rpc GetAccountStatus (GetAccountStatusRequest) returns (AccountStatusView); // auth hot path
  rpc SetDateOfBirth (SetDateOfBirthRequest) returns (AccountView);          // once, when none is on file; min age 13 (16 in AU)
  rpc GetGdprRecord (GetGdprRecordRequest) returns (GdprRecordView);          // the holder's own on the edge
  rpc UpdateConsents (UpdateConsentsRequest) returns (GdprRecordView);         // GDPR Art. 7 consents + history
  rpc ListAccountsByStatus (ListAccountsByStatusRequest) returns (ListAccountsByStatusResponse);
}
```

> **Wire / enum contract:** enums are **1-based** (no `UNSPECIFIED` zero). `AccountStatus`
> `PENDING_VERIFICATION=1…DELETED=5`; `KycStatus` `NOT_STARTED=1…REJECTED=5`; `AccountRole`
> `USER=1…SUPER_ADMIN=6`. **Handler defaults** for fields absent in proto:
> `RecordFailedLogin.max_attempts=5`, `lockout_duration_secs=900`,
> `RequestGdprDeletion.retention_days=30`, `EnrollMfa.recovery_code_hashes=[]` (server-generated).

**Security at the boundary:** passwords stored as Argon2id only (plaintext never accepted); TOTP seeds
AES-256-GCM encrypted (`EncryptedBytes`); recovery codes Bcrypt-hashed; secret fields suppress
`Display`/`Debug` and carry `#[serde(skip)]`.

### Rust ports (hexagonal contract)

```rust
pub trait AccountRepository: Send + Sync + 'static { /* save (CAS), find_by_id, find_by_identity_id, … */ }
```

### Error contract

| Range / variant | gRPC status |
|---|---|
| `AccountNotFound`, `RoleNotAssigned` | `NOT_FOUND` |
| `IdentityAlreadyRegistered`, `EmailAlreadyRegistered`, `MfaAlreadyEnrolled`, `RoleAlreadyAssigned`, `GdprDeletionAlreadyRequested`, `EmailAlreadyVerified` | `ALREADY_EXISTS` |
| `ConcurrentModification` | `ABORTED` (**retryable**) |
| `AccountNotActive`, `InvalidStatusTransition`, `InvalidKycTransition`, `MfaNotEnrolled`, `AccountAlreadyAnonymized` | `FAILED_PRECONDITION` |
| `Validation`, `InvalidAccountRole/KycStatus/AccountStatus` | `INVALID_ARGUMENT` |
| `Storage` | `UNAVAILABLE` |

Stable codes are `ACC-1xxx` (lifecycle) … `ACC-9xxx` (identifiers), via the shared `error` crate.

---

## 📨 Events & Async Contract

> Kafka topics are an API. A schema change here breaks consumers exactly like a proto change.

**Publishes:**

| Topic | Carries (event kinds) | Key | Consumers |
|---|---|---|---|
| `account.v1.events` | `AccountCreated`, `AccountActivated`, `AccountSuspended`, `AccountDeactivated`, `AccountDeleted`, `EmailChanged`, `EmailVerified`, `PhoneChanged`, `PasswordChanged`, `KycStatusChanged`, `MfaEnrolled`, `MfaRevoked`, `GdprDeletionRequested`, `GdprDataExportRequested`, `GdprDeletionCancelled`, `ConsentsUpdated`, `DateOfBirthSet` | `account_id` | `profile` (suspend/deactivate/delete → mask; activate → restore) |

**Consumes:** none — `account` is a pure event producer.

> **Runtime contract:** events are published best-effort after the durable commit; a Kafka failure does
> not fail the command. Consumers (e.g. `profile`) own at-least-once handling under `run_consumer` and
> dead-letter to `account.v1.events.dlq`.

---

## 🌩️ Failure Modes & Degradation

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Postgres/CockroachDB unavailable | all RPCs fail | **Hard fail** — `UNAVAILABLE`; nothing acked, nothing lost | check DB cluster / ranges |
| Write contention on hot account | `ConcurrentModification` (`ABORTED`) | CAS rejects the stale writer; client retries | none — correct behavior; investigate retry storms |
| Kafka unavailable | downstream projections stale | **Soft** — commits succeed, events buffered/dropped | check brokers; downstream replays |

**Backpressure & limits.** `ListAccountsByStatus` is paginated. Failed-login lockout (`max_attempts`
default 5, `lockout_duration_secs` default 900) throttles credential-stuffing at the domain layer.

---

## 📦 Integration & Usage

```toml
[dependencies]
account = { path = "crates/services/account" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) as
`account::service::AccountService` — `build` constructs the PostgreSQL pool via `PgPoolBuilder` and
wires the CQRS buses, `register` adds the gRPC + reflection services, `health_probes` checks Postgres
(the `Arc`-backed pool is shared with the probe).

### Bootstrap (`crates/apps/account-server`)

```rust
use std::net::SocketAddr;
use account::service::AccountService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("ACCOUNT_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50059".to_owned())
        .parse()?;
    service_runtime::serve::<AccountService>(addr).await
}
```

---

## ⚙️ Configuration & Runtime Environment

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_*` (URL/pool/timeouts) | **Yes** | — | CockroachDB-compatible connection; see the `postgres-storage` crate. |
| `KAFKA_BROKERS` | **Yes** | — | Kafka bootstrap brokers for `account.v1.events`. |
| `ACCOUNT_GRPC_ADDR` | No | `0.0.0.0:50059` | gRPC bind address. |
| `ACCOUNT_REQUIRE_STEP_UP` | No | `false` | `DeactivateAccount` and `RequestGdprDeletion` on the edge need a credential proof under 5 min old (the token's `auth_time`, from `auth.v1.Login` / `VerifyCredentials`); otherwise `PERMISSION_DENIED` `step_up_required…`. Turn on once clients step up. |
| `ACCOUNT_GDPR_JANITOR_INTERVAL_SECS` | No | `3600` | How often account-server anonymizes the accounts whose erasure grace period (30 days) has ended; `0` turns the janitor off. Safe on every replica (optimistic CAS). |

> Full connection/timeout/pool tuning lives in the shared `postgres-storage` and `transport` crates.

### Compile-time features
- `build.rs` compiles `proto/account/v1/*.proto` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback

- **Migrations:** `crates/services/account/migrations/*.sql` (ANSI semantics, CockroachDB-compatible,
  UUIDv7 PKs for range-friendly clustering). Apply **before** rolling a new binary.
- **Rollout:** `<TODO: rolling / canary>`. Stateless service; safe to roll.
- **Rollback:** `<TODO: confirm migrations forward-compatible with N-1 binary>`.
- **Compliance gotcha:** `AnonymizeAccount` is irreversible (PII overwrite) — never run it as part of a
  rollback/replay.

---

## 📈 Telemetry, Performance & Metrics

- **Runtime:** Tokio multi-thread. Global tracing/OTel subscriber installed before `serve`.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `GetAccountStatus` p99 | auth-path latency, fleet-amplified | p99 > SLO ⇒ page |
| `ConcurrentModification` rate | write contention / retry storms | sustained spike ⇒ investigate hot accounts |
| `account.v1.events` publish failures | downstream projection drift | sustained rate ⇒ check Kafka |
| Postgres exec errors | DB health | any spike ⇒ check cluster |

---

## 🛠️ Local Development

```bash
cargo build -p account && cargo clippy -p account --all-targets
cargo test  -p account
docker compose up -d postgres                 # repo-root compose
for f in crates/services/account/migrations/*.sql; do psql -f "$f"; done
```

---

## 🚨 Troubleshooting & Runbook

> Format: **symptom → root cause → mitigation.**

**1. `ABORTED: ConcurrentModification` on every write to one account.**
Root cause: two writers racing the version CAS, or a client that retries without re-reading the
current `version`. Mitigation: clients must re-read the aggregate and retry with the fresh version;
a persistent storm points at a buggy retry loop, not the DB.

**2. `FAILED_PRECONDITION: InvalidStatusTransition`.**
Root cause: the requested lifecycle/KYC transition is illegal from the current state (e.g. reactivating
a `Deleted` account). Mitigation: query the current `status`/`kyc_status` via `GetAccountById`; the
state machine in §Architecture defines the legal edges.

**3. Profile not masked after a suspension/deletion.**
Root cause: the event published, but `profile`'s `account.v1.events` consumer is lagging or
dead-lettered the record. Mitigation: check the consumer group lag and `account.v1.events.dlq`; the
account write itself is durable regardless.

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
  rpc ChangeEmail (ChangeEmailRequest) returns (CommandResponse);   // mesh only: auth, after a code to the new address (#651)
  rpc ChangePhone (ChangePhoneRequest) returns (CommandResponse);   // mesh only: idem, by SMS — both set + verify at once; ACC-1003/1004 if taken
  rpc ChangePassword (ChangePasswordRequest) returns (CommandResponse);
  rpc EnrollMfa (EnrollMfaRequest) returns (CommandResponse);
  rpc RevokeMfa (RevokeMfaRequest) returns (CommandResponse);
  rpc GetMfaSecret (GetMfaSecretRequest) returns (MfaSecretView);
  rpc ConsumeRecoveryCode (ConsumeRecoveryCodeRequest) returns (CommandResponse);
  rpc ReplaceRecoveryCodes (ReplaceRecoveryCodesRequest) returns (CommandResponse);
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
  rpc FindProfilesByContacts (FindProfilesByContactsRequest) returns (FindProfilesByContactsResponse); // #661, the caller's own account on the edge
  // Family supervision (#670), the caller's own account on the edge
  rpc CreateSupervisionInvite (CreateSupervisionInviteRequest) returns (SupervisionInviteView);
  rpc AcceptSupervisionInvite (AcceptSupervisionInviteRequest) returns (SupervisionView);
  rpc ListSupervisions (ListSupervisionsRequest) returns (ListSupervisionsResponse);
  rpc EndSupervision (EndSupervisionRequest) returns (ListSupervisionsResponse);
  rpc SetSupervisionLimits (SetSupervisionLimitsRequest) returns (SupervisionLimitsView);
  rpc GetSupervisionLimits (GetSupervisionLimitsRequest) returns (SupervisionLimitsView);
  rpc ReportScreenTime (ReportScreenTimeRequest) returns (ScreenTimeView);
  rpc GetSupervisionOverview (GetSupervisionOverviewRequest) returns (SupervisionOverview);
  rpc ListSupervisedConnections (ListSupervisedConnectionsRequest) returns (ListSupervisedConnectionsResponse);
  rpc ListSupervisedReports (ListSupervisedReportsRequest) returns (ListSupervisedReportsResponse);
}
```

> **Wire / enum contract:** enums are **1-based** (no `UNSPECIFIED` zero). `AccountStatus`
> `PENDING_VERIFICATION=1…DELETED=5`; `KycStatus` `NOT_STARTED=1…REJECTED=5`; `AccountRole`
> `USER=1…SUPER_ADMIN=6`. **Handler defaults** for fields absent in proto:
> `RecordFailedLogin.max_attempts=5`, `lockout_duration_secs=900`,
> `RequestGdprDeletion.retention_days=30`.

**Security at the boundary:** passwords stored as Argon2id only (plaintext never accepted); secret fields
suppress `Display`/`Debug` and carry `#[serde(skip)]`.

**Finding one's contacts (#661).** `FindProfilesByContacts(account_id, email_sha256[], phone_sha256[])`
takes SHA-256 hashes of an address book's contacts — lower-cased, trimmed email addresses and E.164
phone numbers — at most 1000 per call, and **keeps none of them**. A hash matches an **active**
account's **verified** email / phone: the hashes are generated columns (`account_contact_sha256`,
migration 0007) indexed for verified contacts only, so they follow every write. The matched accounts'
profiles are then read over the mesh (profile, as the mesh: status and discovery settings) and kept when
active and findable through that channel (`by_email` / `by_phone`), never the caller's own, never one
the caller's profiles block or are blocked by (social-graph `CheckAccess`, a missing answer counts as
hidden). Each result names the hash it came from, so the app shows the contact. Profile or
social-graph unreachable ⇒ `ACC-7006` (`UNAVAILABLE`, retryable). Hashes of phone numbers hide nothing
(a numbering plan hashes in minutes), so each account may look up **5000 hashes per UTC day**
(`contact_lookup_quota`, migration 0008): every submitted hash counts, matched or not, reserved
atomically before anything is matched; past it ⇒ `ACC-7007` (`RESOURCE_EXHAUSTED`, `retry-after-secs` =
until the next UTC midnight). Stored phone numbers are E.164 already (validated on every write), so the
phone hash column needs no normalization.

**Family supervision (#670), part 1: pairing.** A parent (a **known adult**: 18+ by date of birth) pairs
with a teen (13–17). Either side calls `CreateSupervisionInvite(account_id, role)` (its own side) and
shares the code (10 characters of Crockford base32, ~50 bits; shown or as a QR code; **single use, 24
hours**; typing ignores case, spaces and dashes, and reads O/I/L as 0/1/1); the other side calls
`AcceptSupervisionInvite(account_id, code)` and takes the other side. Ages are read from the date of
birth at both ends (an unknown age fits neither side: `ACC-3002`; a teen who turned 18 meanwhile voids
their invite: `ACC-3001`); one's own code is `ACC-3006`; a teen has **two supervisors at most**
(`ACC-3003`); a parent may supervise several teens. After those checks the invite is **claimed** for
the acceptor with a compare-and-set (`claimed_by`, migration 0010): two accounts racing on one code
never both pair (the other gets `ACC-3001`), the same acceptor's retry completes, and a wrong acceptor
(failing a check) never spends it. An unknown or expired code counts against the account: **10 per
hour**, then `ACC-3007` (`RESOURCE_EXHAUSTED`) — enumeration is closed for something as sensitive as
supervising a minor. `ListSupervisions` shows **both sides** the link
(the other side's account and active profiles, since when) — the teen always sees who supervises them.
`EndSupervision(account_id, other_account_id)` ends it from either side (`ACC-3005` when there is none).
A supervision ends by itself when the teen turns 18 (the sweep, `ACCOUNT_SUPERVISION_SWEEP_INTERVAL_SECS`,
which also drops expired codes) and when either account is erased (the GDPR janitor ends them first; a
failure leaves the account for its next pass). Storage (migration 0009): `supervision_invites` on the
shard of the code; `supervisions` on the **teen's** shard (an advisory lock keeps "two at most" atomic;
coming of age joins the teen's `accounts` row there), with `supervisions_by_supervisor` on the
supervisor's shard — written teen-first, idempotent, a stale index entry dropped on read. Every start
and end is published (`SupervisionStarted` / `SupervisionEnded { ended_by: by_teen | by_supervisor |
came_of_age | account_deleted }`, `account_id` = the teen) with both sides' profile ids, so each side can
be told. Edge: the caller's account.

**Supervision limits (#670 part 2).** One **shared** set per teen (`supervision_limits`, migration
0011, on the teen's shard): either supervisor sets it with `SetSupervisionLimits(account_id,
teen_account_id, limits)` and the last change applies (`set_by`, `set_at` kept). Each limit is a
**floor** the teen may only exceed in strictness: `private_account`; the loosest audience for
`messages` / `comments` (`followers` < `mutuals` < `no_one`); `hidden_from_search` (handle search,
suggestions, contact matching — a shared QR code or link still works); `daily_minutes` (15–1440,
else `ACC-9001`). Not their supervisor ⇒ `ACC-3005`. The teen and each supervisor read them with
`GetSupervisionLimits`. Every change is published (`SupervisionLimitsSet`, with the teen's profile
ids): profile tightens the teen's settings to the floors and refuses loosening them (part 2b). When
the teen's **last** supervision ends (by either side, at 18, on erasure) the limits are lifted
(`SupervisionLimitsCleared`): the settings keep their values, unlocked. **Screen time:** the app
reports its use with `ReportScreenTime(account_id, minutes ≤ 15, timezone)` and learns today's total
across devices and whether the limit is reached (it then shows the pause screen; the server does not
cut requests). Only a teen with a daily limit is counted (`screen_time`, per account and local day,
kept 4 weeks).

**The supervisor's view (#670 part 3).** Each supervisor and the teen themselves see **the same
view** (`teen_account_id` empty: the caller's own; anyone else ⇒ `ACC-3005`):
`GetSupervisionOverview` — the limits, the last 7 days with time counted (most recent first, the
teen's local days) and the teen's active profiles; `ListSupervisedConnections(profile_id, kind)` —
one page of a teen's profile's following, followers or blocked profiles, read from social-graph as
the mesh (whatever the lists' privacy; a profile that is not the teen's ⇒ `ACC-3005`; a supervisor
the teen blocked is never listed);
`ListSupervisedReports` — one page of the reports the teen made, newest first: **who or what, when
and the decision** (`under_review` / `action_taken` / `no_violation`), never the teen's own words
(moderation's mesh-only `ListReportsByReporter` does not return them). A teen must be able to report
unseen: a `self_harm`, `csam` or `ncii` report, or one about a supervisor's content, is never listed
(moderation leaves them out, paging included; the teen still sees them in their own `ListMyReports`). Pages: 50 by default, 100 at
most. social-graph or moderation unreachable ⇒ `ACC-3008` (`UNAVAILABLE`, retryable).

**GDPR data export (#653, Art. 15/20).** `RequestDataExport` marks the export pending; the **export
pass** (`ExportDueData`) then builds, per pending account, a ZIP of JSON files — the holder's own
account record (contact details, consents, sign-in settings; **no** password hash, MFA material or
internal fields), the other services' files (`ExportSources`: profiles, posts, comments, likes, recent
searches, the social graph, conversations, media links), a `README.txt` — stores it privately
(`exports/<account>/<id>.zip`, `S3ExportStore`: static keys, `ACCOUNT_EXPORT_*`) and records its **object
key** on the GDPR record — never a signed link, which is a bearer credential: `GetGdprRecord` signs the
link on read for what is left of the 7 days (`data_export_url` / `data_export_expires_at`, shown to the
holder and to auth; none while a newer request is pending, once expired, or without the store). `GdprDataExportCompleted` (without the link — it
is a credential) lets auth email it. A failing source leaves the export pending (never a partial
archive) and the pass retries it (`ACC-7005`); the save is version-checked, so a request made while an
export was being built is built anew. Pending accounts come from a partial index (migration 0005). The
sources are the other services' **mesh-only** RPCs (`MeshExportPeers`, every page, each message
transcoded to JSON through the service's own descriptor set): per profile its profile, posts, comments
(`ListCommentsByAuthor`), recent searches (search's
`ListRecentSearches`, over the mesh, #816), social graph, and conversations
(`ListConversationsByMember`, then: a `DIRECT` conversation in full through `GetHistory`; a group or
channel through chat's mesh-only `GetFormerMemberHistory`, the holder's own messages with the others' as
`{"from": "another member"}` placeholders — direct is the conversation's kind, never its roster size;
groups the holder left (#656, `left_at_ms`) are included, up to the departure, without their roster); for the account its
likes (engagement's `ListLikesByAccount`, #665: a like is a point, the account's) and its media
(`ListAssetsByOwner`, links valid 7 days). The server runs the pass every
`ACCOUNT_EXPORT_INTERVAL_SECS` when the store is configured (`ACCOUNT_EXPORT_BUCKET`, core-platform-infra#28).

**MFA is auth's (#649); account only keeps it.** Every MFA RPC is **mesh only**: the holder enrolls and
disables two-step sign-in through auth, after a step-up. auth encrypts the TOTP seed with its own key
(AES-256-GCM; account stores the ciphertext and never reads it), hashes each backup code, and checks
codes. `EnrollMfa` takes the ciphertext and at least 6 distinct code hashes (`ACC-9001` otherwise,
`ACC-5001` when MFA is already on). `GetMfaSecret` hands auth the ciphertext and the number of codes left.
`ConsumeRecoveryCode` spends one code by its hash, once: the save is version-checked, so of two
concurrent spends one fails, and a spent or unknown code is `ACC-5003`. `ReplaceRecoveryCodes` takes a
regenerated set. `AccountView.mfa_enrolled` / `mfa_recovery_codes_remaining` show the holder where they
stand.

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
| `AccountNotActive`, `InvalidStatusTransition`, `InvalidKycTransition`, `MfaNotEnrolled`, `RecoveryCodeInvalid`, `AccountAlreadyAnonymized` | `FAILED_PRECONDITION` |
| `Validation`, `InvalidAccountRole/KycStatus/AccountStatus` | `INVALID_ARGUMENT` |
| `Storage` | `UNAVAILABLE` |

Stable codes are `ACC-1xxx` (lifecycle) … `ACC-9xxx` (identifiers), via the shared `error` crate.

---

## 📨 Events & Async Contract

> Kafka topics are an API. A schema change here breaks consumers exactly like a proto change.

**Publishes:**

| Topic | Carries (event kinds) | Key | Consumers |
|---|---|---|---|
| `account.v1.events` | `AccountCreated`, `AccountActivated`, `AccountSuspended`, `AccountDeactivated`, `AccountDeleted`, `EmailChanged`, `EmailVerified`, `PhoneChanged`, `PasswordChanged`, `KycStatusChanged`, `MfaEnrolled`, `MfaRevoked`, `GdprDeletionRequested`, `GdprDataExportRequested`, `GdprDeletionCancelled`, `GdprDataExportCompleted`, `ConsentsUpdated`, `DateOfBirthSet`, `SupervisionStarted`, `SupervisionEnded`, `SupervisionLimitsSet`, `SupervisionLimitsCleared` (#670; `account_id` = the teen) | `account_id` | `profile` (suspend/deactivate/delete → mask; activate → restore) |

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
| `ACCOUNT_SUPERVISION_SWEEP_INTERVAL_SECS` | No | `3600` | How often supervisions whose teen turned 18 end and expired supervision invites go (#670); `0` turns it off. Idempotent, safe on every replica. |
| `ACCOUNT_EXPORT_BUCKET` · `ACCOUNT_EXPORT_S3_ENDPOINT` · `ACCOUNT_EXPORT_S3_PUBLIC_ENDPOINT` · `ACCOUNT_EXPORT_S3_REGION` | No | unset · `https://s3.amazonaws.com` · = endpoint · `us-east-1` | The GDPR export store (#653). Unset bucket: exports stay pending. |
| `ACCOUNT_EXPORT_S3_ACCESS_KEY` · `ACCOUNT_EXPORT_S3_SECRET_KEY` | No | unset | Static keys for that bucket (a 7-day presign needs non-session credentials). |
| `ACCOUNT_EXPORT_INTERVAL_SECS` | No | `300` | How often the export pass runs; `0` turns it off. |
| `ACCOUNT_SEARCH_GRPC_ENDPOINT` | No | `http://localhost:50062` | search's mesh address: the profiles' recent searches in the export (#816). |
| `ACCOUNT_MODERATION_GRPC_ENDPOINT` | No | `http://localhost:50061` | moderation's mesh address: a supervised teen's reports (#670). Unreachable: `ListSupervisedReports` answers `ACC-3008`. |
| `ACCOUNT_{PROFILE,POST,COMMENT,ENGAGEMENT,SOCIAL_GRAPH,CHAT,MEDIA}_GRPC_ENDPOINT` | No | `http://localhost:<port>` | The export's mesh sources. An unreachable one leaves the export pending. |

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

# `audit` — Record who did what, to whom, when, and under what authority — once, immutably, forever — and prove it was never altered

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — legal/regulatory-critical evidence plane. **Split posture:** *fail-open at producers* (audit liveness never browns out the business mesh) but *fail-closed on durability and on the synchronous break-glass lane* (the most dangerous actions are denied if they cannot be provably recorded first) |
> | **Deployable** | **two** binaries — `crates/apps/audit-server` (read/record plane, holds the gRPC reads + the sync `RecordPrivileged` RPC) **and** `crates/apps/audit-worker` (ingest/verify plane: the `run_consumer` lane + the verify/anchor/retention/shred loops). Library crate: `crates/services/audit` |
> | **Listeners** | internal **gRPC** only — `:50068` (server: reads + `RecordPrivileged`) · `:50069` (worker: health/reflection). **No public/client listener** — clients never talk to audit |
> | **Datastores** | append-only **Postgres** (canonical hash-chained ledger, `UPDATE`/`DELETE` revoked) + **S3/MinIO Object Lock** WORM archive (long-term, compliance mode) + **KMS/HSM** signer & per-subject DEK vault (a separate trust domain) |
> | **Async** | **consumes** `audit.v1.events` (the fleet-wide compliance event topic) + decision streams from `moderation` / `auth` / `account` (Kafka). **Publishes** nothing of record (audit is a terminal sink) |
> | **Upstream callers** | fleet services emitting compliance events (async) + a narrow set of privileged-action callers (sync `RecordPrivileged`); DPO / internal-audit / regulator tooling (read/export) |
> | **Downstream deps** | Postgres, Object-Lock store, KMS/HSM, an external anchor/witness (RFC 3161 timestamp and/or a cross-account WORM bucket), Kafka. Identity↔pseudonym mapping stays in `account` — audit never holds it |
> | **SLO** | `<TODO>` ingest durability (zero-loss for in-scope events) · `<TODO>` `RecordPrivileged` p99 · `<TODO>` query p99 · integrity-verification cadence `<TODO>` |

---

## 🎯 Overview & Service Role

`audit` is the platform's **tamper-evident compliance evidence plane**: the append-only, hash-chained System of Record that answers *"who did what, to whom, when, under what authority, and with what outcome"* for every security-, privacy- and regulatory-relevant event in the fleet.

It is emphatically **not** "logging, but serious". Application telemetry (traces, metrics, debug logs) and a compliance trail are two different substances that merely look alike on a screen, and conflating them is a category error a hyperscale system punishes. **Telemetry** is best-effort, mutable, sampled, retention-cycled, and read by every engineer — correct for observability, fatal for evidence. An **audit trail** must be zero-loss for in-scope events, append-only, tamper-evident, provably *complete*, retained for years, access-controlled (and its own reads audited), and erasable at the *field* level without destroying the record. The moment a real PII identifier lands in a Loki index you have created an uncontrolled copy of personal data with no field-level erasure primitive — and a GDPR Art. 17 request now contradicts the Art. 5(2) accountability duty. A dashboard also cannot answer the only question a regulator or a SOC2 auditor actually asks: *can you prove this record is complete and unaltered, and that you'd detect it if it weren't?* That is what this service exists to provide.

**Core objectives:** (1) a **zero-loss, append-only ledger** of compliance events; (2) **tamper-evidence even against a hostile operator** — a compromised DBA or rogue admin cannot rewrite or truncate history undetectably; (3) reconcile the **GDPR erasure ⇄ audit-retention paradox** via crypto-shredding, so PII can be irreversibly destroyed while the record and its proof survive; (4) **never become a bottleneck or SPOF** — producers are decoupled behind Kafka and never block on audit; (5) a **narrow, access-controlled** read/export surface for DPO / internal audit / regulators, whose every use is itself an audit event.

| Concern | Path | Posture | Notes |
|---|---|---|---|
| **Bulk ingestion** | async Kafka (`audit.v1.events`, `run_consumer`) | fail-open at producer / zero-loss via the log | ~99% of traffic; a write spike becomes consumer *lag*, never producer backpressure |
| **Privileged actions** | sync gRPC `RecordPrivileged` on `:50068` | **fail-closed** | break-glass / legal-hold / consent changes — *denied* unless provably recorded first |
| **Read / export** | sync gRPC `Query` / `Export` / `VerifyIntegrity` | access-controlled, itself audited | DPO / internal audit / regulator; need-to-know + separation of duties |

---

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), Kafka for bulk ingestion, an append-only Postgres ledger as the canonical store, an Object-Lock WORM archive for long-term immutability, and KMS/HSM for signing + per-subject key custody. The defining structural choice is **two deployables** — a read/record server and an ingest/verify worker — that share a domain crate but no process, deployment, or failure domain.

```
    fleet services (moderation · auth · account · privileged actions everywhere)
        │ async emit (fire-and-forget)            │ sync, must-record-first
        ▼                                         ▼
   ┌──────────┐                          ┌──────────────────┐
   │  Kafka   │  audit.v1.events         │  audit-server    │  RecordPrivileged
   │ (buffer) │  + decision streams      │  :50068 (gRPC)   │  (FAIL-CLOSED)
   └────┬─────┘                          │  Query/Export/   │  + Query/Export reads
        │ run_consumer                   │  VerifyIntegrity │  (access-controlled,
        ▼                                └────────┬─────────┘   itself audited)
   ┌─────────────────────────────────────────────┴───────────┐
   │  audit-worker  — dedupe → CHAIN → persist → archive       │
   │  + verifier / checkpoint-anchor / retention / shred loops │
   └───┬─────────────────────┬──────────────────────┬─────────┘
       ▼                     ▼                       ▼
  ┌──────────┐        ┌──────────────┐        ┌──────────────┐
  │ Postgres │ hash-  │  Object Lock │ WORM   │  KMS / HSM   │ sign checkpoints
  │  ledger  │ chain  │ (S3/MinIO)   │ archive│  + DEK vault │ + per-subject DEK
  │ INSERT-  │        │ compliance   │        │ (separate    │ (crypto-shred)
  │  only    │        │  mode        │        │  trust dom.) │
  └──────────┘        └──────────────┘        └──────┬───────┘
                                                     ▼
                                       external anchor / witness
                                  (RFC 3161 TSA · cross-account WORM)
```

**Immutability is three layers of defense in depth.** (A) **Hash chain** — every record carries `H(prev_hash ‖ canonical(payload) ‖ sequence_no)` in a per-partition append-only chain; any mutation, reorder, or deletion breaks every downstream hash, and the monotonic per-partition `sequence_no` makes truncation/gap detection trivial. (B) **Infrastructure WORM** — the Postgres ledger has `UPDATE`/`DELETE` revoked at the role level (the service can only `INSERT`), and the long-term archive uses Object Lock in *compliance mode*, where not even the root account can delete before retention expiry. (C) **Signed, externally-anchored checkpoints** — the worker periodically signs a Merkle root over all partition heads with a key held in a *separate* KMS/HSM principal, then anchors it to an independent witness; a standalone verifier recomputes the chain and compares. To forge history undetectably an attacker would need simultaneous control of four separate trust domains.

> **Invariants** (and where enforced):
> - **PII never enters the chain in cleartext.** A subject is referenced by an opaque pseudonym; unavoidable PII goes in a per-subject **crypto-shreddable envelope** (the chain hashes the *ciphertext*) — domain + infrastructure.
> - **Erasure is key-destruction, not record-deletion.** A GDPR Art. 17 request destroys the per-subject DEK; the PII becomes permanently undecryptable while the record, its sequence, its hash, and the non-PII compliance metadata remain intact and verifiable — application.
> - **Lawful retention overrides erasure.** A subject under an active legal hold (GDPR Art. 17(3)) is not shredded; field-level selective shred keeps the pseudonymous decision record — domain.
> - **The most dangerous actions fail closed.** `RecordPrivileged` denies the action unless durability is confirmed first — application.
> - **Reads are evidence too.** Every query/export is itself recorded; access is need-to-know with separation of duties — application.
> - **Audit is a terminal sink.** It records decisions other services make; it makes none, and publishes nothing of record — domain.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### Internal gRPC — `audit.v1` *(Phase 1)*

There is **no client-facing interface.** The internal surface is a narrow `audit.v1` gRPC service on `:50068`: the **synchronous, fail-closed** `RecordPrivileged` (used only for the must-record-before-permitted class), and the access-controlled `Query` / `Export` / `VerifyIntegrity` reads for DPO / internal audit / regulators. The worker (`:50069`) serves health/reflection only. *(Proto lands in Phase 1 — no code yet.)*

### Rust ports (hexagonal contract) *(Phase 3)*

```rust
#[async_trait] pub trait LedgerStore      { /* append-only per-partition hash-chained insert + read */ }
#[async_trait] pub trait WormArchive      { /* Object-Lock (compliance-mode) long-term write/read */ }
#[async_trait] pub trait KeyVault         { /* sign checkpoints · mint/destroy per-subject DEK (crypto-shred) */ }
#[async_trait] pub trait CheckpointAnchor { /* publish/verify the signed Merkle root against the external witness */ }
#[async_trait] pub trait EventSource      { /* the upstream Kafka compliance + decision streams */ }
```

### Error contract

Every fault implements `error::AppError` with a stable `AUD-XXXX` code, mapped to gRPC `Status` / HTTP by the shared `error` crate:

| Range | Class |
|---|---|
| `AUD-1xxx` | event intake / contract validation |
| `AUD-2xxx` | **ledger integrity** (hash chain, sequence gaps, checkpoint/witness divergence — alarm, never retry) |
| `AUD-3xxx` | audit-read authorization (the privileged read surface; itself audited) |
| `AUD-4xxx` | storage-plane availability (the durability core; retryable; the sync lane fails closed on `AUD-4004`) |
| `AUD-5xxx` | crypto-shred / key lifecycle (the GDPR erasure pattern) |
| `AUD-6xxx` | retention / legal hold |
| `AUD-8xxx` | async ingestion (`run_consumer`) surface |
| `AUD-9xxx` | cross-cutting (domain/parse) |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

> Kafka topics are an API. A schema change in a consumed topic breaks the audit trail exactly like a proto change.

**Publishes:** none of record. Audit is a terminal evidence sink. (An integrity-alarm signal on tamper/gap/witness-divergence is operational, not a System-of-Record stream.)

**Consumes** *(Phase 4)*:

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `audit.v1.events` | `audit-ingest` | the fleet-wide compliance event firehose → dedupe → chain → persist → archive | DLQ `audit.v1.events.dlq` |
| `moderation.v1.events` ✅ wired | `audit-moderation` | `decision_recorded` (the authority + the DSA rationale — sealed into a crypto-shreddable envelope at ingest) and `enforcement_applied`; other variants are a benign skip | DLQ `moderation.v1.events.dlq` |
| `auth.v1.events` ✅ wired | `audit-auth` | `session_issued` / `session_revoked` (the authentication lifecycle — structured metadata, no PII, no sealing); other variants are a benign skip | DLQ `auth.v1.events.dlq` |
| `account.v1.events` ✅ wired | `audit-account` | the full account surface — PII-bearing `account_created` / `email_changed` / `email_verified` / `phone_changed` (sealed into a crypto-shreddable envelope), security (`password_changed`, `mfa_*` → Authentication), identity lifecycle (`activated`/`deactivated`/`suspended`/`deleted`, `kyc_status_changed` → **Identity**), authorization (`role_*` → Authorization), the GDPR events (`gdpr_deletion_requested` / `gdpr_deletion_cancelled` → DataErasure, `gdpr_data_export_requested` → DataExport) and `consents_updated` → **Consent** (Art. 7(1) evidence) — and `deleted` (the erasure itself, after the 30-day grace period) **also crypto-shreds the subject** (Art. 17, closing the loop) | DLQ `account.v1.events.dlq` |

> **Runtime contract (mandatory):** all consumers run under `run_consumer` — manual commit only after the event is durably persisted *and* chained, bounded retry with backoff + jitter, DLQ on poison/exhaustion. **No committed offset ever advances past an un-persisted event → zero loss.** **Idempotency:** events carry a deterministic UUIDv5 id; a redelivery is deduped (`AUD-1004`, folded into `Ok`), so each logical event appears in the chain exactly once. An event with nothing recordable (`AUD-8002`) is a harmless skip folded into `Ok`. Per-partition chains keep the write path parallel (no global serialization); a periodic global Merkle root stitches the partition heads.

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Postgres ledger down | ingest stalls | **fail-open at producer** — events buffer in Kafka; offsets uncommitted → no loss (`AUD-4001`) | restore Postgres; consumer drains the backlog |
| Object-Lock archive down | archival lags | ledger still canonical; archive catches up (`AUD-4002`) | restore archive; reconcile |
| KMS/HSM / DEK vault down | can't sign checkpoints / shred | chaining continues; checkpoint + shred deferred (`AUD-4003`) | restore vault; resume anchor/shred loops |
| External witness down | checkpoints unwitnessed | chain intact; anchoring deferred (`AUD-2005`) | restore witness; re-anchor |
| **`RecordPrivileged` can't confirm durability** | privileged action blocked | **fail-closed** — action denied (`AUD-4004`); caller retries/aborts | confirm storage health; the deny is correct |
| Hash mismatch / sequence gap | verifier raises | **alarm, do not retry** (`AUD-2001`/`AUD-2002`) — tampering/truncation signal | **treat as a security incident**; isolate, investigate |
| Checkpoint ≠ witness | verifier raises | **alarm** (`AUD-2004`) — operator-level tamper signal | **security incident**; cross-domain forensics |
| Write spike (50×) | consumer lag rises | Kafka absorbs it; audit drains at its own pace; no loss | scale `audit-worker`; watch lag |
| Erasure vs legal hold | shred refused | lawful retention wins (`AUD-5002`) | confirm hold; selective field-shred if applicable |

**Backpressure & limits:** the durable Kafka log is the buffer (producers never block); per-partition parallel chaining; hard timeouts on ledger/archive/vault/witness calls; the sync lane's durable-commit deadline (`AUD-4004`).

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
audit = { path = "crates/services/audit" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md) **twice** (Phase 5): `audit::AuditServerService` (the `audit-server` binary — the gRPC reads + the synchronous fail-closed `RecordPrivileged` RPC, plus backend health probes) and `audit::AuditWorkerService` (the `audit-worker` binary — the supervised `run_consumer` ingestion lane + the checkpoint-anchor loop, no domain RPC; the crypto-shred and retention-expiry loops await their input sources — see *Deferred*). Telemetry, config + hot-reload, health, and graceful shutdown are owned by the runtime.

> **Build status:** **complete through Phase 7.** All eight phases are landed: the `AUD-XXXX` namespace + two binaries (P0), the `audit.v1` contract (P1), the pure tamper-evidence domain — hash chain, crypto-shred, retention/holds, Merkle checkpoint (P2), the application layer + six ports + the shared fail-open/fail-closed commit (P3), the infrastructure adapters — append-only Postgres ledger, Object-Lock WORM archive, key vault, anchor, `run_consumer` ingest + the proto/JSON mapping (P4), the two-binary split with the gRPC `AuditService` (P5), and the live suite + migrations (P6). **78 unit tests** plus a live `integration-audit` suite (real Postgres + MinIO Object Lock) cover the domain, the codec/decode mappings, all six handlers, and end-to-end over the real adapters: the append→chain→archive→verify roundtrip, **in-place tamper detection** (a rogue `UPDATE` caught as `AUD-2001`), the **crypto-shred** that erases PII while the chain still verifies, the checkpoint round-trip, and idempotent replay. Hardening + failure sims (P7): the synchronous lane is wrapped in a hard durable-commit deadline, exercised by a sim proving **a break-glass action is denied (`AUD-4004`) when audit cannot confirm durability** — and nothing is recorded. Security-reviewed: no token, PII, payload or ciphertext is ever logged (subjects are pseudonyms; the opaque PII envelope is never logged), and there is no fallible panic in a hot path. **KMS/witness hardening (issues #482/#483):** KEK custody and checkpoint signing move to KMS (a hand-rolled SigV4 KMS client, pinned to the AWS SigV4 test-suite vectors), and the signed Merkle root is anchored to an independent WORM witness — closing the operator-level threat. **116 unit tests** + a **13-scenario** live suite (real Postgres + MinIO), including crypto-shred against the KMS-backed cipher and an **adversarial double-tamper** (ledger row + Postgres pointer) that `verify_global` still catches via the externally-anchored signed root.
>
> **Closed (was deferred):**
> - **KEK custody → KMS** *(issue #482)* — the per-subject DEKs are wrapped/unwrapped by KMS under a principal the ledger DB role cannot assume (`KmsSubjectCipher` behind the `SubjectCipher` port). No raw KEK ever lives in audit's env or memory; `subject_keys` is unchanged (the stored blob is the KMS ciphertext), and crypto-shred stays "delete the wrapped-DEK row". The env-KEK `AesGcmSubjectCipher` remains the local/dev fallback, selected by config.
> - **Signed checkpoints + external witness** *(issue #483)* — the Merkle root is signed in KMS's trust domain and anchored to an independent WORM witness (`WitnessCheckpointAnchor`); `verify_global` validates the signature and reconciles the live heads against the **externally-anchored** root, so an operator who rewrites *both* a ledger row **and** the Postgres `checkpoint_anchors` pointer is still caught (`AUD-2004`). The Postgres pointer survives only as a convenience index. The unsigned `PgCheckpointAnchor` remains the local/dev fallback. Covered by an adversarial live-IT (double-tamper of ledger + pointer, real MinIO witness).
>
> **Deferred (explicit, not gaps):**
> - **KMS/witness *provisioning*** remains an IAM / org-structure commitment — the code is in place behind the ports, but the integrity story is only as strong as the separation between the ledger principal, the KMS signing/encryption principal, and the cross-account WORM witness. Choosing an RFC 3161 TSA vs a second-account Object-Lock bucket, and the key-rotation policy, are ops decisions; local/dev + CI run on the env-KEK + HMAC fallbacks (or LocalStack KMS).
> - **Producer adoption** — **`moderation`, `auth` and `account` are all wired.** moderation's `decision_recorded` + `enforcement_applied` (rationale sealed), auth's `session_issued` + `session_revoked` (no PII), and account's `account_created` / `email_changed` (PII sealed) + the two GDPR events are consumed and chained. An account `deleted` (the erasure once its grace period ends; a deletion *request* is only recorded, since it can still be cancelled) **crypto-shreds the subject**, so all their sealed PII across feeds becomes unreadable while the chain still verifies — the Art. 17 erasure loop, closed end to end.
> - **The crypto-shred consumer** (needs an erasure-request source) and **the retention-expiry sweep** (needs resolved retention policies) — the handlers exist and are tested; only the worker loops that drive them await their input sources.
> - **Read-self-auditing** — recording each authorized query/export as its own `DATA_ACCESS` ledger event. Caller authentication + per-RPC authorization (`AUD-3001`/`AUD-3002`/`AUD-3004`/`AUD-3005`) are **wired**: every RPC verifies the ES256 edge token via `auth-context` and requires its `audit:*` permission, deny-all when unconfigured (`AUDIT_JWKS_URL` below). Until the `DATA_ACCESS` events land, the authorized principal + RPC are traced as the interim access trail.
> - **Forward pagination** beyond a single capped page; **blockchain anchoring** (overkill — RFC 3161 + cross-account WORM suffices); **real-time SIEM streaming**; **automated DSA transparency-report generation**; **cross-region ledger replication**.

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `audit`-specific variables *(filled per phase)*

| Variable | Required | Default | Description |
|---|---|---|---|
| `AUDIT_SERVER_GRPC_ADDR` | No | `0.0.0.0:50068` | server: reads + `RecordPrivileged` gRPC address |
| `AUDIT_WORKER_GRPC_ADDR` | No | `0.0.0.0:50069` | worker: health/reflection address (no domain RPC) |
| `AUDIT_KEK_BASE64` | No (dev/fallback) | dev key | base64 of the 32-byte env KEK wrapping per-subject DEKs — the **local/dev fallback** `AesGcmSubjectCipher`. Superseded in production by `AUDIT_KMS_*` (KMS holds custody); a fixed dev key is derived if unset |
| `AUDIT_KMS_ENDPOINT` | **Yes (prod)** | — | KMS endpoint URL. **Presence switches on** KMS KEK custody (#482) + KMS checkpoint signing (#483). AWS KMS or LocalStack; absent ⇒ env-KEK + HMAC fallbacks |
| `AUDIT_KMS_REGION` / `AUDIT_KMS_ACCESS_KEY` / `AUDIT_KMS_SECRET_KEY` | No | `us-east-1` / `AWS_*` | KMS SigV4 region + credentials (fall back to the standard `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) |
| `AUDIT_KMS_DEK_KEY_ID` / `AUDIT_KMS_SIGNING_KEY_ID` | No | `alias/audit-dek` / `alias/audit-checkpoint` | the symmetric DEK-wrapping key (#482) and the asymmetric checkpoint-signing key (#483); the latter under a principal the ledger DB role cannot assume |
| `AUDIT_KMS_SIGNING_ALGORITHM` | No | `ECDSA_SHA_256` | KMS `SigningAlgorithm` for the checkpoint signature |
| `AUDIT_WITNESS_ENDPOINT` | **Yes (prod)** | — | cross-account WORM-bucket (S3 Object Lock) endpoint. **Presence switches on** the external-witness anchor (#483); absent ⇒ Postgres-only `PgCheckpointAnchor` (dev). `AUDIT_WITNESS_{REGION,BUCKET,ACCESS_KEY,SECRET_KEY}` configure it |
| `AUDIT_CHECKPOINT_SIGNING_KEY_BASE64` | No (dev) | dev key | base64 of the 32-byte HMAC checkpoint-signing key used **only** when KMS is unset (dev/CI signed-checkpoint path) |
| `AUDIT_JWKS_URL` | **Yes (prod)** | — | JWKS endpoint verifying the ES256 edge tokens on the privileged surface. **Presence switches on** the `auth-context` caller gate; absent ⇒ **deny-all** (`AUD-3004`) — the surface never opens by omission. Every RPC requires its `audit:*` permission: `audit:record` / `audit:read` / `audit:export` / `audit:verify` |
| `AUDIT_TOKEN_ISSUER` / `AUDIT_TOKEN_AUDIENCE` | No (recommended) | — | expected `iss` / `aud` claims of the edge token (set to the values `auth` mints) |
| `AUDIT_JWKS_TIMEOUT_MS` | No | `10000` | per-request JWKS fetch timeout |
| `<ledger / archive config>` | **Yes** | — | append-only Postgres ledger + Object-Lock WORM archive |
| `<KAFKA_BROKERS>` | **Yes** *(worker)* | — | upstream compliance + decision ingestion |

### Compile-time features
- `integration-audit` *(Phase 6)* — gates the live, container-backed integration suite (real Postgres + MinIO Object Lock + Kafka): the append→chain→archive→verify path, tamper-detection, and the crypto-shred lifecycle.

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

- **Two deployables, scaled independently.** `audit-server` scales with read/record QPS; `audit-worker` scales with ingest throughput. Released together (same image/tag), rolled and scaled separately.
- **Storage is append-only + WORM.** Migrations are *additive only* — the ledger role holds `INSERT` but not `UPDATE`/`DELETE`; the archive is Object-Lock compliance-mode. A migration that tries to mutate history must be impossible by construction.
- **Key custody is a separate trust domain.** The signing key and per-subject DEKs live in KMS/HSM under a principal distinct from the database role — provisioned out of band, never co-located with the ledger.
- **Rollback:** safe for the binaries (stateless over Postgres/Kafka; the worker resumes from last committed offsets). The *data* is by design irreversible — that is the point.

---

## 🛠️ Local Development &nbsp;·&nbsp; CORE

```bash
cargo build -p audit && cargo clippy -p audit --all-targets
cargo test  -p audit                                    # fast, infra-free unit run
docker compose up -d postgres minio kafka               # repo-root compose (Phase 6)
cargo test  -p audit --features integration-audit       # live suite (Phase 6)
```

---

## 🚨 Troubleshooting & Runbook &nbsp;·&nbsp; OPS

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. A privileged/break-glass action is being denied.**
Root cause: the synchronous lane could not confirm durable+chained commit within the deadline (`AUD-4004`) — by design it fails *closed*. Mitigation: check ledger/KMS health; the deny is correct — the action must not proceed unrecorded. Resolve the storage fault, then retry.

**2. The verifier raised a hash mismatch or sequence gap.**
Root cause (critical): a record was altered or the tail truncated (`AUD-2001`/`AUD-2002`) — the storage operator is assumed potentially hostile. Mitigation: **treat as a security incident.** Do not "repair" the chain; isolate, snapshot, and reconcile against the externally-anchored checkpoints to bound the tampering window.

**3. A signed checkpoint disagrees with the external witness.**
Root cause (critical): operator-level tampering past the database role (`AUD-2004`). Mitigation: **security incident**; cross-trust-domain forensics — the witness is independent precisely so this is detectable.

**4. A GDPR erasure request isn't taking effect.**
Root cause: the subject is under an active legal hold; lawful retention (Art. 17(3)) overrides erasure (`AUD-5002`). Mitigation: confirm the hold; apply field-level selective shred (destroy the contact-PII envelope, retain the pseudonymous decision record) if lawful.

**5. Consumer lag climbing during a traffic spike.**
Root cause: ingest throughput exceeds worker capacity — expected; Kafka is absorbing it with zero loss. Mitigation: scale `audit-worker`; lag drains. Producers are unaffected (they never block on audit).

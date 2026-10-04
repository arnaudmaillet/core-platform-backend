# `account` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Account / Identity — the user account system of record |
> | **Subdomain class** | **Supporting** — identity lifecycle is necessary infrastructure, not a user-facing differentiator; bespoke because it carries the platform's PII + GDPR obligations |
> | **System of …** | **Record** for account existence, credentials metadata, KYC, roles, and GDPR state |
> | **Aggregate root(s)** | `Account` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Failure posture** | **Fail-closed** — account writes are authoritative; a lifecycle write must durably commit |
> | **Upstream contexts** | `auth` (federated IdP subject link), internal ops/registration flows |
> | **Downstream contexts** | `audit` (compliance), `profile` (persona over the account) — via **Open-Host Service / Published Language** (`account.v1.events`) |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `account` is the authority for **the user account**: it answers
**"does this account exist, what is its lifecycle state, role, and KYC status, and what is the
lawful state of its personal data?"**

**The hard problem.** Owning PII as the system of record while honouring GDPR — erasure, export,
rectification — without other services holding shadow copies of identity that erasure can't reach.
Account is the *one* place a real identity lives; everyone else holds derived, non-authoritative
references.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Authenticate / issue sessions or tokens → owned by `auth`.
- ❌ Hold the public persona (handle, bio, avatar) → owned by `profile`.
- ❌ Verify tokens → that's the `auth-context` library.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Account | The authoritative user account record | `Account`, `AccountId` |
| Identity id | The stable cross-system subject identifier | `IdentityId` |
| Credential metadata | Password hash, MFA enrolment, recovery codes — *metadata*, not auth flow | `PasswordHash`, `MfaState`, `RecoveryCodeHash` |
| KYC status | Know-your-customer verification state | `KycStatus`, `KycStatusChanged` |
| Role | Authorization role assigned to the account | `AccountRole`, `RoleAssigned`/`RoleRevoked` |
| GDPR record | The lawful-data-handling state (consents, export/deletion requests) | `GdprRecord`, `ConsentPurpose`, `ConsentsUpdated`, `GdprDeletionRequested`, `GdprDataExportRequested` |
| Encrypted bytes | At-rest-encrypted PII fields | `EncryptedBytes` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Account` | aggregate root | Lifecycle state machine + uniqueness of email/identity |
| `EmailAddress` / `PhoneNumber` / `CountryCode` | VO | Validity enforced at construction |
| `PasswordHash` / `MfaState` / `RecoveryCodeHash` | VO | Credential metadata integrity |
| `GdprRecord` | VO | Erasure/export request state |
| `AccountStatus` / `AccountRole` / `KycStatus` | enum | Closed lifecycle / authorization / verification vocabularies |

**Lifecycle:**

```
created --> activated --(suspend)--> suspended --(reactivate)--> activated
   │            │                                                   │
   │            └--(deactivate)--> deactivated --(holder signs in: auth Login)--> activated
   └── gdpr_deletion_requested ──> (active: deactivated) ──30 days, janitor──> deleted (anonymized)
                                   └──(holder signs in / CancelGdprDeletion)──> deletion withdrawn
```

> **Legal transitions only.** Email/phone changes are events, not silent mutations; a GDPR deletion
> is terminal and triggers downstream crypto-shred in `audit`.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Account records, credentials metadata, roles, KYC, and GDPR state — **Postgres** db `account`. No other service writes these.

**This context holds copies it does NOT own:**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| IdP subject link | `auth` / federated IdP | `auth` linking flow | at link time |

**The "do-not-write" list:** account never issues tokens, never writes profile presentation data,
and never stores another service's projection.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Email/identity uniqueness | domain + Postgres unique constraint | `ACC-1xxx` |
| I2 | PII at rest is encrypted (`EncryptedBytes`) | infrastructure | — |
| I3 | A lifecycle change emits an event (no silent state change) | domain (after-save publish) | — |
| I4 | GDPR deletion is terminal and propagates erasure downstream | application | `ACC-1xxx` |
| I5 | Roles are explicit grants/revocations, audited | domain | `ACC-1xxx` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Registration / lifecycle.** A create/activate/suspend/deactivate command mutates the `Account`
aggregate, persists to Postgres, and publishes the corresponding `account.v1.events` variant
**after** the save (the EventPublisher port → Kafka).

**GDPR erasure (Art. 17).** `RequestGdprDeletion` schedules the erasure 30 days out
(`deletion_scheduled_at`) and deactivates an active account (profiles hidden, sessions end). During
the grace period, signing back in or `CancelGdprDeletion` withdraws it (`gdpr_deletion_cancelled`).
Once due, the **janitor** in account-server (`ACCOUNT_GDPR_JANITOR_INTERVAL_SECS`) anonymizes the
account — PII cleared, email replaced by a unique tombstone, status `deleted` — and emits
`account_deleted`; `audit` consumes that and **crypto-shreds the subject's per-subject DEK**,
rendering all that subject's sealed PII across the fleet permanently unreadable while the chain
still verifies — closing the erasure loop end to end.

**GDPR export (Art. 15/20).** `gdpr_data_export_requested` → emitted for downstream fulfilment.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| `auth` | upstream | Customer/Supplier | IdP `SubjectLink` ↔ `AccountId` | session subject resolution breaks |
| `audit` | downstream | Published Language | `account.v1.events` (PII sealed; GDPR pair) | compliance evidence + Art. 17 loop break |
| `profile` | downstream | Published Language | `account.v1.events` | persona provisioning breaks |

> **Anti-Corruption Layer:** consumers (`audit`, `profile`) translate `account.v1.events` into their
> own models; account exposes a stable published language and owns no consumer's schema.

---

## 8. Domain Events (semantics, not wire)

| Event (`account.v1.events`) | Means | Emitted when | Who reacts |
|---|---|---|---|
| `account_created` / `email_changed` / `email_verified` / `phone_changed` | PII-bearing lifecycle facts | the corresponding command commits | `audit` (PII sealed), `profile` |
| `password_changed` / `mfa_enrolled` / `mfa_revoked` | security facts (no PII) | credential change | `audit` (Authentication) |
| `activated`/`deactivated`/`suspended`/`deleted`, `kyc_status_changed` | identity lifecycle | lifecycle transition | `audit` (Identity), `profile` |
| `role_assigned` / `role_revoked` | authorization change | role grant/revoke | `audit` (Authorization) |
| `gdpr_deletion_requested` / `gdpr_deletion_cancelled` / `gdpr_data_export_requested` | a lawful data right was invoked / withdrawn | user/DPO request | `audit` (evidence; the crypto-shred follows `deleted`) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Account is the identity SoR; built its outbound event lane (was a phantom producer) so `audit`/`profile` can consume | [`ADR-0004`](../../../../docs/adr/0004-account-is-the-single-identity-sor.md) | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Supporting — invest for correctness, PII safety, and GDPR compliance; not a differentiator.
- **Volatility:** low — driven by regulatory and IdP-integration change, not feature churn.
- **Known modeling debt:** none material recorded.
- **Deferred capabilities:** richer KYC workflows; export fulfilment pipeline downstream of the export event.

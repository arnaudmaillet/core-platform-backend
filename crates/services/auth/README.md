# `auth` — Authentication boundary: issue, track, and revoke sessions without a DB hit per request

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — every authenticated request depends on tokens this service issues |
> | **Deployable** | `crates/apps/auth-server` (library crate: `crates/services/auth`) |
> | **Datastores** | PostgreSQL/CockroachDB (db `auth`) · Redis Cluster (sessions/blacklist) |
> | **Async** | publishes `auth.v1.events` (SessionIssued/SessionRevoked/SubjectLinked) · consumes nothing |
> | **Upstream callers** | gateway / edge, end-user clients (login & refresh) |
> | **Downstream deps** | Keycloak (IdP), `account` (gRPC, identity SoR), PostgreSQL, Redis Cluster |
> | **SLO** | `<TODO: 99.95%>` avail · login p99 `<TODO>` · refresh p99 `<TODO>` |

> **✅ Status — all phases (0–7) complete.** Contract, domain, application, infrastructure,
> server wiring, a live container-backed integration suite, and ops hardening (ES256 signing-key
> **ring rotation** + **JWKS** publication, SLO/failure-mode/runbook docs) are all in place and
> green. Remaining `<TODO>`s are deployment-specific values (team, on-call, concrete SLO numbers).
> See [`project_auth_service_blueprint`] for the full design and phased plan.

---

## 🎯 Overview & Service Role

`auth` is the platform's **issuance / session / IdP-broker** boundary. It owns the
*authentication act and its lifecycle* — login brokering, session tracking, refresh-token
rotation, revocation, and edge-token minting — plus the one piece of identity data that is an
authentication concern: the IdP-subject ↔ `account_id` linkage.

The hard problem it solves is **authenticating hyperscale traffic without a datastore read per
call**. A naive design checks a session table on every request and melts under load. `auth`
resolves this with a **split-token model**: short-lived, locally-verifiable **edge tokens**
(verified in pure CPU by the `auth-context` library in every downstream service) plus long-lived,
server-side, single-use **refresh tokens** with mandatory rotation and reuse-detection. Instant
global logout rides a per-session **generation** counter in Redis Cluster, so revocation is
milliseconds — never a write amplified across every reader.

**Core objectives:** (1) no datastore read on the request hot path; (2) refresh-token reuse =
compromise ⇒ revoke the whole session generation; (3) **100% IdP-agnostic** — the domain and
application layers never name Keycloak; migrating to Cognito/Okta/custom is a new infrastructure
adapter and zero domain change.

### What this service does **not** own
| Concern | Owner |
|---|---|
| Who a person *is* (identity record, KYC, GDPR, RBAC roles) | `account` service (identity SoR) |
| Credentials (passwords, MFA, recovery) | Keycloak (IdP) — federated model |
| Inbound token *verification* on the hot path | `auth-context` platform library |

---

### Guest sessions (guest mode)

`StartGuestSession` (edge **public**) gives an app installation an anonymous, **read-only** session
before sign-up. No account is created or looked up: the session's `account_id` is a fresh **guest
id**, and the row is `kind = 'guest'`. The edge token carries `sub = "guest:<guest_id>"`,
`kind = "guest"`, `perms = ["read:public"]`, no `pids`, and the installation's `did`; `Refresh`
rotates it like a member's (no directory or profile lookup). `device.device_id` is **required**; the
device, attestation flag, locale and hints are recorded in `guest_principals` (the welcome gift is
once per device at sign-up). The client edge refuses a guest token on every `authenticated` method
(`PERMISSION_DENIED`), so guests only reach `permission(…, "read:public")` routes; realtime refuses
guest handshakes. **Members carry `read:public` too** (added at every mint). Guest session issuance
is not published to the outbox (the audit plane records accounts). App Attest verification and
per-IP / per-device limits on `StartGuestSession` are the abuse-control slice (B5); until then the
RPC is **off by default** (`AUTH_GUEST_SESSIONS_ENABLED`; the local fleet turns it on).

### Credentials and step-up

The password lives at the IdP only. `ChangePassword` (edge **authenticated**, members) proves the
current password with a password grant under the subject's login name, sets the new one through the
Keycloak Admin API (`reset-password`, a confidential service-account client with `view-users` +
`manage-users`), and optionally signs every **other** session out (`password_changed` revocations;
the caller's stays). New passwords: 8–128 characters and not the current one (`AUT-VAL-024/025/026`),
then the realm policy (`AUT-5006`, its rule in the message). Without the admin client configured the
credential RPCs answer `UNAVAILABLE` (`AUT-5005`).

**Step-up.** A token minted right after a credential proof — `Login`, or `VerifyCredentials`
(re-prove the password; an MFA code is refused with `AUT-5007` until enrolment exists) — carries
`auth_time`; a refreshed one does not. Destructive RPCs elsewhere call
`transport::grpc::edge::require_recent_auth` (`auth_time` ≤ 5 min old, mesh exempt) and answer
`PERMISSION_DENIED` `step_up_required…` otherwise; `account` gates `DeactivateAccount` /
`RequestGdprDeletion` behind `ACCOUNT_REQUIRE_STEP_UP`. `VerifyCredentials` re-mints the caller's
access token (same session, same refresh token).

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS command/query buses,
PostgreSQL for the durable session ledger, Redis Cluster for the hot-path generation map /
blacklist, Kafka for events. The IdP sits behind an `IdentityProviderPort` (Port/Adapter), so no
Keycloak type leaks above `infrastructure`.

```
            ┌──────────────────────── auth-service ───────────────────────┐
 client ──► │ Login/Refresh/Logout ─► CQRS bus ─► ports:                  │
            │   IdentityProviderPort ─┐   SessionRepo/RefreshRepo (PG)     │
            │   AccountDirectoryPort ─┤   SessionCachePort (Redis gen/blk) │
            │   TokenMinterPort ──────┘   SubjectLinkRepo (PG)             │
            └───────┬───────────────────────────┬─────────────────────────┘
        broker login│                            │mints edge token (ES256; PASETO fast-follow)
                    ▼                            ▼
              Keycloak (IdP)            downstream services verify LOCALLY via auth-context
                    │                            │  (pure-CPU sig check + optional O(1)
              resolves identity ──► account      │   Redis `gen` check for instant logout)
```

**Split-token hot path.** 99% of API calls = signature verify only, no I/O. Revocation is a
`generation` bump written through to `auth:sess:{account}:gen` in Redis; an edge token carrying a
stale `gen` is rejected. Only `/refresh` (low QPS) touches PostgreSQL.

> **Invariants** (and where enforced): edge-token TTL ⊆ session TTL ⊆ absolute cap; refresh
> rotation is mandatory + single-use, and reuse revokes the whole generation (enforced in the
> `Session` aggregate, Phase 2); `SubjectLink (iss,sub)→account_id` is immutable (Phase 2);
> session issuance is gated on `account` status (application layer, Phase 3).

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-5xx / non-`UNAVAILABLE`) | `<TODO 99.95%>` | 30d rolling | `<grpc_server_handled_total by code>` |
| `Login` latency p99 | `< <TODO> ms` | 1h | `<rpc latency by method>` (dominated by the IdP round-trip) |
| `Refresh` latency p99 | `< <TODO> ms` | 1h | `<rpc latency>` (one Postgres rotation) |
| `Introspect` latency p99 | `< <TODO> ms` | 1h | `<rpc latency>` (CPU verify + ≤1 Redis read) |
| Durability | no acked session/refresh write lost | — | Postgres `LocalQuorum`/fsync |

**Error budget:** `<0.05% / 30d ≈ 21m>`. **On burn:** freeze rollout, page on-call.

> **Note — the edge is not on auth's critical path.** Downstream services verify edge tokens
> *locally* via `auth-context`; only `Login` / `Refresh` / `Logout` hit this service. An auth
> outage stops *new* logins and refreshes but does **not** break in-flight authenticated traffic
> (existing edge tokens keep verifying until they expire).

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

**Downstream — what `auth` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Keycloak (IdP) | credential verification on `Login` | `Login` fails (`UNAVAILABLE`) | **Hard** for new logins; refresh/introspect unaffected |
| `account` (gRPC) | resolve account + gate active on `Login` (a self-deactivated account is resumed: `ResumeDeactivatedAccount`, `reactivated = true`) | `Login` fails | **Hard** for new logins |
| PostgreSQL | session + refresh-token + link ledger | `Refresh`/`Logout` writes fail | **Hard** for refresh/revocation |
| Redis Cluster | generation map + blacklist (hot path) | revocation checks degrade | **Soft** — generation rebuilds from Postgres; a missed blacklist entry expires with the token |
| Kafka | `auth.v1.events` emission | events not emitted | **Soft** — best-effort; falls back to the log publisher |

**Upstream — blast radius if `auth` fails:**

| Caller | Uses | Impact if `auth` is down |
|---|---|---|
| gateway / edge | `Login` / `Refresh` / `Logout` | users cannot sign in, refresh, or sign out; **already-authenticated requests keep working** until tokens expire |
| ops / device-management UI | `ListSessions` / `Introspect` | session listing + server-side introspection unavailable |

## ⚙️ Configuration

| Env var | Purpose | Default |
|---|---|---|
| `AUTH_GRPC_ADDR` | gRPC listen address | `0.0.0.0:50060` |
| `AUTH_SIGNING_PRIVATE_PEM` / `AUTH_SIGNING_PUBLIC_PEM` | **Required.** ES256 edge-token key pair (PEM) | — |
| `AUTH_SIGNING_KID` · `AUTH_TOKEN_ISSUER` · `AUTH_TOKEN_AUDIENCE` | Edge-token `kid` / `iss` / `aud` | `auth-es256-1` · `https://auth.core-platform` · `core-platform` |
| `AUTH_ACCESS_TTL_SECS` · `AUTH_SESSION_TTL_SECS` · `AUTH_ABSOLUTE_TTL_SECS` · `AUTH_REFRESH_TTL_SECS` | Token / session lifetimes | `600` · `1800` · `28800` · `604800` |
| `AUTH_KEYCLOAK_TOKEN_ENDPOINT` · `AUTH_KEYCLOAK_CLIENT_ID` · `AUTH_KEYCLOAK_CLIENT_SECRET` · `AUTH_KEYCLOAK_SCOPE` | IdP broker | — · — · — · `openid` |
| `AUTH_KEYCLOAK_ADMIN_URL` · `AUTH_KEYCLOAK_ADMIN_CLIENT_ID` · `AUTH_KEYCLOAK_ADMIN_CLIENT_SECRET` | Credential management (`ChangePassword`, `VerifyCredentials`): the realm's admin base (`…/admin/realms/<realm>`) and a confidential service-account client with `realm-management` `view-users` + `manage-users`. Unset → those RPCs answer `UNAVAILABLE` (`AUT-5005`). | — |
| `AUTH_ACCOUNT_GRPC_ENDPOINT` | `account` service endpoint | `http://localhost:50059` |
| `AUTH_ACCOUNT_RPC_TIMEOUT_MS` · `AUTH_ACCOUNT_CONNECT_TIMEOUT_MS` | Per-request / connect deadlines on the `account` channel (login hot path — fail fast, never hang) | `2000` · `2000` |
| `AUTH_IDP_HTTP_TIMEOUT_MS` · `AUTH_IDP_CONNECT_TIMEOUT_MS` | Request / connect deadlines on Keycloak HTTP calls (token exchange) | `5000` · `2000` |
| `AUTH_GUEST_SESSIONS_ENABLED` | `StartGuestSession` kill switch. **Off by default**: it writes a session per call with no credential, so keep it off wherever the abuse controls (per-IP / per-device limits, App Attest) are not in front of it. Off → `AUT-1005` (`PERMISSION_DENIED`). | `false` |
| Postgres / Redis / Kafka | via the shared storage crates' own `from_env()` | — |

## 🧪 Local Development

```bash
cargo test -p auth                              # fast, hermetic: unit + cross-crate edge-verify
cargo test -p auth --features integration-auth  # live: boots Postgres + Redis containers
```

The default run needs no Docker. It covers the domain/application/handler units, the ES256
mint↔verify round-trip, and **`tests/edge_token_verify.rs`** — the cross-crate proof that a token
minted here is accepted by the same `auth-context` decoder every downstream service runs.

The `integration-auth` suite (`tests/auth_it/`) boots real **PostgreSQL** + **Redis** via the shared
`test-support` harness and drives the production composition root through the gRPC handler. Auth's
*external* deps (the IdP and the `account` service) are stubbed at their ports. Scenarios:
lifecycle (login → introspect → logout), refresh rotation + reuse-detection → generation revoke,
global logout, and durable-write round-trips. **Keycloak is not containerized** — the OIDC adapter
is unit-tested directly, and the live suite focuses on the session/token machinery over auth's own
stores.

## 🔥 Failure Modes &nbsp;·&nbsp; OPS

| Symptom | Likely root cause | Mitigation |
|---|---|---|
| All `Login` → `UNAVAILABLE` | Keycloak or `account` unreachable | check IdP / `account` health; refresh + introspect keep working |
| `Refresh` → `UNAUTHENTICATED` spike | refresh-token **reuse** (token theft) or a global logout | expected on reuse — the session generation is revoked; investigate the source IP/device |
| Edge tokens accepted after logout | Redis blacklist/generation miss | tokens still die at TTL (≤ `AUTH_ACCESS_TTL_SECS`); verify Redis health and the generation key |
| `Introspect` returns `active:false` for a fresh token | clock skew, or a generation bump (global logout) | check NTP; confirm the account's current generation in Redis |
| Downstream services reject our tokens | JWKS not published / `kid` rotated out | ensure the active **and** retiring public keys are in the published JWKS (see Deployment) |
| `ConcurrentModification` (AUT-8001) | optimistic-lock contention on a session row | retryable — the caller (or gateway) retries; persistent ⇒ investigate duplicate inflight ops |

## 🚀 Deployment &nbsp;·&nbsp; OPS

- **Throttling / lockout is *not* this service's job.** Credential brute-force protection lives in
  Keycloak (federated model); ingress rate-limiting is the shared runtime's `[traffic]` layer. Auth
  adds no redundant throttle.
- **Signing-key rotation (zero-downtime).** Edge tokens are ES256, verified by a **key ring**:
  1. Generate a new P-256 keypair; set it as `AUTH_SIGNING_PRIVATE_PEM` / `AUTH_SIGNING_PUBLIC_PEM`
     with a fresh `AUTH_SIGNING_KID`.
  2. Move the *previous* public key to `AUTH_SIGNING_RETIRING_PUBLIC_PEM` / `AUTH_SIGNING_RETIRING_KID`
     so tokens minted under it keep verifying and stay in the JWKS.
  3. Roll out. New tokens are signed with the new `kid`; old tokens validate against the retiring key.
  4. After one full `AUTH_ABSOLUTE_TTL_SECS` window (no token can predate it), drop the retiring key.
- **JWKS publication.** `Es256TokenMinter::jwks_json()` produces the JWKS for every ring key; publish
  it at the service's well-known JWKS URL so `auth-context` (in every downstream service) fetches and
  caches it. The private key never leaves this service — only public material is published.

## 🛠️ Troubleshooting

- **`required env var AUTH_SIGNING_PRIVATE_PEM is not set` at boot** — the ES256 signing key pair is
  mandatory; provide both PEMs (see Configuration).
- **Tokens verify locally but `Introspect` says inactive** — `Introspect` additionally applies the
  live generation + blacklist checks; a token can be cryptographically valid yet revoked.
- **Run one scenario:** `cargo test -p auth --features integration-auth <name> -- --nocapture`.

---

## 📋 Error Codes

Canonical `AUT-XXXX` namespace — see [`src/error.rs`](src/error.rs) for the authoritative catalogue
(1xxx session · 2xxx refresh/rotation · 3xxx subject linkage · 4xxx token minting · 5xxx IdP broker ·
6xxx account directory · 9xxx domain/parse). Storage (`DB-*`) and validation (`VAL-*`) codes are
delegated transparently.

[`project_auth_service_blueprint`]: ../../../docs/ <!-- TODO: link the design doc when published -->

# `auth` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Authentication — session issuance, refresh, and IdP brokerage |
> | **Subdomain class** | **Supporting** — necessary security capability; partly leans on a federated IdP (Keycloak) but the edge-token / session-generation model is bespoke |
> | **System of …** | **Record** for sessions and refresh tokens (not for the user account — that's `account`) |
> | **Aggregate root(s)** | `Session`, `RefreshToken` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Failure posture** | **Fail-closed** — no valid session/token, no access |
> | **Upstream contexts** | end-user login flows; federated IdP (Keycloak); `account` (subject ↔ account link) |
> | **Downstream contexts** | every service via the `auth-context` verify library; `realtime` (edge-token verify at handshake); `audit` (`auth.v1.events`) |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `auth` is the authority for **authentication state**: it answers
**"is this caller who they claim to be right now, and is this session still valid?"** — issuing
short-lived ES256 edge tokens and managing the refresh lifecycle.

**The hard problem.** Federating an external IdP while issuing the platform's own fast,
stateless-to-verify edge tokens — and being able to **revoke** instantly despite statelessness. The
resolving mechanism is a monotonic per-subject `Generation` that invalidates token families.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Own the user account / PII → `account` is the SoR.
- ❌ Verify tokens in-process for every service → that's the `auth-context` library (a verify, not a call).
- ❌ Authorize business actions → it authenticates; services authorize via permissions/`auth-context`.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Session | An authenticated session of record | `Session`, `SessionId`, `SessionStatus` |
| Refresh token | The long-lived credential that mints access tokens | `RefreshToken`, `RefreshTokenId`, `RefreshTokenHash` |
| Access token claims | The ES256 edge-token payload | `AccessTokenClaims` |
| Generation | Monotonic per-subject counter; bumping it revokes a token family | `Generation` |
| IdP subject | The federated identity-provider subject id | `IdpSubject`, `SubjectLink` |
| Device fingerprint | Per-device binding for a session | `DeviceFingerprint` |
| Permission | An authorization grant carried in claims | `Permission` |
| Revocation reason | Why a session/token was revoked | `RevocationReason` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Session` | aggregate root | Session validity + revocation via `Generation` |
| `RefreshToken` | aggregate root | Token rotation; a used refresh token cannot be reused |
| `AccessTokenClaims` | VO | The signed edge-token contract |
| `Generation` | VO | Monotonic per-subject; the instant-revocation lever |
| `SubjectLink` | VO | IdP subject ↔ `AccountId` binding |

**Session lifecycle:**

```
issued --(refresh: rotate)--> issued' --(revoke / generation-bump / expiry)--> revoked
```

> **Legal transitions only.** A refresh token rotates (old one invalidated); a `Generation` bump
> revokes the whole family; expired/revoked sessions never re-activate.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Sessions and refresh tokens — **Postgres** (durable) + **Redis** (hot session/revocation state). No other service writes these.

**This context holds copies it does NOT own:**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| Account ↔ IdP subject link | `account` / IdP | linking flow | at link time |

**The "do-not-write" list:** auth never mutates account state and never stores profile/business data.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | A refresh token is single-use (rotation invalidates the prior) | domain | `AUT-7xxx` |
| I2 | A `Generation` bump revokes all tokens of that subject family | domain | (revocation) |
| I3 | Access tokens are short-lived and ES256-signed | domain + infrastructure | verify fails downstream |
| I4 | Revocation is fail-closed and immediate | application | — |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Issuance.** Login (federated IdP verified) → create `Session` + `RefreshToken`, mint an ES256
access token, persist, and emit `session_issued` on `auth.v1.events`.

**Refresh (rotation).** Present refresh token → validate + rotate (old invalidated) → mint a new
access token. Reuse of a rotated token is a security signal.

**Revocation.** Explicit logout, security event, or `Generation` bump → mark revoked, emit
`session_revoked`. Verification downstream (via `auth-context`) fails closed thereafter.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| federated IdP (Keycloak) | upstream | Conformist | OIDC/credentials | login breaks |
| `account` | peer | Customer/Supplier | `SubjectLink` ↔ `AccountId` | subject resolution breaks |
| all services | downstream | Open-Host Service (Published Language) | ES256 edge token verified by `auth-context` | every authenticated call breaks |
| `realtime` | downstream | Conformist (verify-only) | edge-token verify at WS handshake | new connections can't authenticate |
| `audit` | downstream | Published Language | `auth.v1.events` | session-lifecycle evidence breaks |

> **Anti-Corruption Layer:** the federated-IdP adapter translates external OIDC claims into the
> internal `Session` / `AccessTokenClaims` model.

---

## 8. Domain Events (semantics, not wire)

| Event (`auth.v1.events`) | Means | Emitted when | Who reacts |
|---|---|---|---|
| `session_issued` | An authenticated session was established | login / token issuance | `audit` (Authentication) |
| `session_revoked` | A session was invalidated | logout / revoke / generation bump | `audit` (Authentication) |
| `subject_linked` | An IdP subject was bound to an account | account-link flow | (internal) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Federated Keycloak credentials + bespoke ES256 edge tokens; distinct from `account` (SoR) and `auth-context` (verify lib) | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepted |
| Instant revocation via monotonic per-subject `Generation` despite stateless tokens | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Supporting — security-critical, federates a generic IdP, bespoke only at the edge-token/session layer.
- **Volatility:** low-to-medium — driven by security posture and IdP changes.
- **Known modeling debt:** none material recorded.
- **Deferred capabilities:** richer device/session management surfaces. Password, two-step-code and passkey step-up (`VerifyCredentials` → `auth_time`), two-step sign-in (#649) and passkeys (#808: registration, and sign-in as a first factor, a second step or a step-up) are live.

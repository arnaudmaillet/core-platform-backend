# `auth` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Authentication — credential checks, session issuance, refresh, and revocation |
> | **Subdomain class** | **Supporting** — necessary security capability; leans on standard credentials (Apple / Google id_tokens, one-time codes, WebAuthn passkeys) checked in-house; the edge-token / session-generation model is bespoke |
> | **System of …** | **Record** for sessions and refresh tokens (not for the user account — that's `account`) |
> | **Aggregate root(s)** | `Session`, `RefreshToken` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Failure posture** | **Fail-closed** — no valid session/token, no access |
> | **Upstream contexts** | end-user sign-in flows; Apple / Google (published id_token keys); `account` (subject ↔ account link) |
> | **Downstream contexts** | every service via the `auth-context` verify library; `realtime` (edge-token verify at handshake); `audit` (`auth.v1.events`) |
> | **Decision log** | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) · [`ADR-0018`](../../../../docs/adr/0018-passwordless-identity-passkeys-for-staff.md) |

---

## 1. Business Capability & Non-Goals

**Capability.** `auth` is the authority for **authentication state**: it answers
**"is this caller who they claim to be right now, and is this session still valid?"** — issuing
short-lived ES256 edge tokens and managing the refresh lifecycle.

**The hard problem.** Issuing the platform's own fast, stateless-to-verify edge tokens — and being
able to **revoke** instantly despite statelessness. The resolving mechanism is a monotonic
per-subject `Generation` that invalidates token families.

**Passwordless (#881, ADR-0018).** Nothing signs in with a password and there is no external IdP:
auth checks every credential itself — a native Apple / Google id_token (against the provider's
published keys, with a server-issued nonce), a one-time code sent to an email address or phone, or a
passkey. Staff grants exist only in sessions opened with a staff-enrolled passkey (#880). `Login`
never creates an account or an identity link: an identity with no account is `AUT-6004` and goes to
`SignUp`.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Own the user account / PII → `account` is the SoR.
- ❌ Verify tokens in-process for every service → that's the `auth-context` library (a verify, not a call).
- ❌ Authorize business actions → it authenticates; services authorize via permissions/`auth-context`.
- ❌ Store or check passwords → there are none (#881); no password reset or change flow exists.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Session | An authenticated session of record | `Session`, `SessionId`, `SessionStatus` |
| Refresh token | The long-lived credential that mints access tokens | `RefreshToken`, `RefreshTokenId`, `RefreshTokenHash` |
| Access token claims | The ES256 edge-token payload | `AccessTokenClaims` |
| Generation | Monotonic per-subject counter; bumping it revokes a token family | `Generation` |
| Identity subject | A sign-in identity `(issuer, subject)`: an Apple / Google subject, or an email address / phone number under `urn:core-platform:email` / `urn:core-platform:phone` (historical type name) | `IdpSubject`, `SubjectLink` |
| Device fingerprint | Per-device binding for a session | `DeviceFingerprint` |
| Permission | An authorization grant carried in claims | `Permission` |
| Revocation reason | Why a session/token was revoked | `RevocationReason` |
| Credential | What a sign-in presents: an id_token, a one-time code or a passkey assertion — never a password | `AuthnGrant` |
| Staff passkey | A passkey enrolled with a staff enrollment grant; the only way to a session carrying staff grants | `StaffGrant`, `StaffEnrollmentGrants` (port) |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Session` | aggregate root | Session validity + revocation via `Generation` |
| `RefreshToken` | aggregate root | Token rotation; a used refresh token cannot be reused |
| `AccessTokenClaims` | VO | The signed edge-token contract |
| `Generation` | VO | Monotonic per-subject; the instant-revocation lever |
| `SubjectLink` | VO | identity subject ↔ `AccountId` binding (made at `SignUp`, never at `Login`) |

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
| Account ↔ identity subject link | `account` (the account) / auth (the link) | `SignUp`, `ChangeContact` | at link time |

**The "do-not-write" list:** auth never mutates account state and never stores profile/business data.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | A refresh token is single-use (rotation invalidates the prior) | domain | `AUT-7xxx` |
| I2 | A `Generation` bump revokes all tokens of that subject family | domain | (revocation) |
| I3 | Access tokens are short-lived and ES256-signed | domain + infrastructure | verify fails downstream |
| I4 | Revocation is fail-closed and immediate | application | — |
| I5 | A staff grant is minted only in a session opened with a staff-enrolled passkey that is still the account's (#880) | application (`Permission::for_session`, re-read at every mint) | member grants only |
| I6 | A passkey is staff-enrolled only with a single-use, unexpired grant bound to its account; grants come from an admin in a staff passkey session (never for itself) or the one-off bootstrap while no staff passkey exists | application + Postgres (conditional update) | `AUT-5032` / `AUT-5033` / `AUT-5034` |
| I7 | A staff session steps up with its passkey only | application | `AUT-5031` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Issuance.** Login (credential checked by auth: id_token, code or passkey; the identity must already
have an account) → create `Session` + `RefreshToken`, mint an ES256
access token, persist, and emit `session_issued` on `auth.v1.events`.

**Refresh (rotation).** Present refresh token → validate + rotate (old invalidated) → mint a new
access token. Reuse of a rotated token is a security signal.

**Revocation.** Explicit logout, security event, or `Generation` bump → mark revoked, emit
`session_revoked`. Verification downstream (via `auth-context`) fails closed thereafter.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| Apple / Google | upstream | Conformist | published JWKS; native id_tokens | their sign-in breaks (codes, passkeys unaffected) |
| `account` | peer | Customer/Supplier | `SubjectLink` ↔ `AccountId` | subject resolution breaks |
| all services | downstream | Open-Host Service (Published Language) | ES256 edge token verified by `auth-context` | every authenticated call breaks |
| `realtime` | downstream | Conformist (verify-only) | edge-token verify at WS handshake | new connections can't authenticate |
| `audit` | downstream | Published Language | `auth.v1.events` | session-lifecycle evidence breaks |

> **Anti-Corruption Layer:** the `FederatedTokenVerifier` adapter translates a provider's id_token
> claims into an internal identity subject; nothing provider-specific reaches the `Session` /
> `AccessTokenClaims` model.

---

## 8. Domain Events (semantics, not wire)

| Event (`auth.v1.events`) | Means | Emitted when | Who reacts |
|---|---|---|---|
| `session_issued` | An authenticated session was established | login / token issuance | `audit` (Authentication) |
| `session_revoked` | A session was invalidated | logout / revoke / generation bump | `audit` (Authentication) |
| `subject_linked` | A sign-in identity was bound to an account | `SignUp`; `ChangeContact` re-keying a code identity | (internal) |
| `staff_enrollment_granted` | A staff passkey enrollment grant was issued (by an admin, or the bootstrap) | `IssueStaffPasskeyEnrollment` / `auth-server bootstrap-staff` | `audit` (PrivilegedAction) |
| `staff_passkey_enrolled` | A passkey became a staff passkey with a grant | `FinishPasskeyRegistration` with a grant | `audit` (PrivilegedAction) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Federated-IdP credentials + bespoke ES256 edge tokens; distinct from `account` (SoR) and `auth-context` (verify lib) | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepted — federated-IdP part superseded by ADR-0018 |
| Passwordless: auth checks every credential itself (Apple / Google, codes, passkeys); staff on staff passkeys only | [`ADR-0018`](../../../../docs/adr/0018-passwordless-identity-passkeys-for-staff.md) | Accepted |
| Instant revocation via monotonic per-subject `Generation` despite stateless tokens | [`ADR-0005`](../../../../docs/adr/0005-auth-federated-idp-with-platform-edge-tokens.md) | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Supporting — security-critical; standard credentials checked in-house, bespoke at the edge-token/session layer.
- **Volatility:** low-to-medium — driven by security posture and sign-in provider changes.
- **Known modeling debt:** none material recorded.
- **Deferred capabilities:** richer device/session management surfaces. Two-step-code and passkey step-up (`VerifyCredentials` → `auth_time`; a staff session with its passkey only), two-step sign-in (#649), passkeys (#808: registration, and sign-in as a first factor, a second step or a step-up) and staff passkeys (#880) are live. Passwords and the external IdP were retired (#881).

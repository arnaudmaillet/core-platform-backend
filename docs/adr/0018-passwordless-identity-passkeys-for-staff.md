# ADR-0018: Passwordless identity: Keycloak retired, passkeys for staff

- **Status:** Accepted
- **Date:** 2026-10-11
- **Context(s) affected:** auth; account; auth-context (verify lib); local fleet; infra (Keycloak deployment)
- **Deciders:** arnaudmaillet (architecture)

## Context and problem

[ADR-0005](./0005-auth-federated-idp-with-platform-edge-tokens.md) had `auth` federate an external
IdP (Keycloak) for credentials. Since then, every member sign-in method moved into `auth` itself:
native Sign in with Apple / Google (id_tokens verified against the provider's keys, server-issued
nonces), one-time codes by email or SMS, and passkeys (#808). The **password path was the only
remaining use of Keycloak** — and with it came a password grant, a reset/change flow, an admin
client, an account lockout and a second user store to erase under GDPR. Passwords are also the
credential most exposed to phishing and reuse, and that risk is worst for **staff**, whose sessions
carry the platform's privileged grants. No account exists and production has never launched, so
there is nothing to migrate.

## Decision

We sign nobody in with a password, and `auth` checks every credential itself (epic #879, #881).
**Members** sign in with Apple / Google (native id_token), a one-time code to their email or phone,
or a passkey; an identity with no account is `AUT-6004` and goes to `SignUp` (`Login` never creates
an account or an identity link). **Staff** sign in with a **staff-enrolled passkey** only (#880):
staff grants exist only in a session opened with a passkey enrolled under a single-use grant from an
admin, or from the one-off `bootstrap-staff` command while no staff passkey exists. Keycloak is
removed from the code (adapters, ports, `AUTH_KEYCLOAK_*`), the password and authorization-code
grants and `ChangePassword` are removed from `auth.v1` / `account.v1` (fields reserved), and Keycloak
leaves the local fleet and the infrastructure (core-platform-infra#68; iOS drops its password screens,
core-platform-ios#840).

## Consequences

- **Positive:** no password to phish, reuse, stuff or leak; staff privileges require a
  phishing-resistant, device-bound credential; one fewer TIER-0 dependency to run, patch and back up;
  GDPR erasure has no external user store to reach; `auth`'s brute-force protection covers every
  credential it accepts.
- **Negative / accepted trade-off:** no password reset flows — credential recovery is a one-time code
  to the account's email or phone (or Apple / Google); a staff member who lost every staff passkey
  needs a new grant from an admin. `account` loses its password storage and its failed-login lockout
  (`RecordFailedLogin`, `locked_until`). Error codes `AUT-5002`, `AUT-5004`…`AUT-5007` are retired and
  never reused.
- **Supersedes:** the federated-IdP part of ADR-0005 (credentials checked by Keycloak). ADR-0005's
  edge-token model — ES256 tokens, generation-based revocation, `pids`, `did`, guest tokens — stands.
- **Closes:** passwords as a phishing target for staff; the second user store and its erasure path.

## Alternatives rejected

| Option | Why rejected |
|---|---|
| Keep Keycloak for passwords only | A whole IdP to run and secure for the one credential we want to drop |
| Keep passwords, checked by `auth` (own hashes) | Still phishable and reusable; brings back reset flows and lockout |
| Staff on the member methods plus a second factor | Codes and TOTP are phishable; a passkey is bound to the origin and the device |
| Keycloak (or another IdP) for staff SSO | Unneeded until a workforce IdP exists; passkeys give the same phishing resistance in-house |

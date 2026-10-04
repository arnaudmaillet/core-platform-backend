# ADR-0005: Auth federates an IdP and issues platform ES256 edge tokens with generation-based revocation

- **Status:** Accepted
- **Date:** 2026-06-26
- **Context(s) affected:** auth; account; auth-context (verify lib); realtime; all services
- **Deciders:** arnaudmaillet (architecture)

## Context and problem

Authentication is three concerns that are easy to conflate: the **account** (who exists — a SoR),
**verification** (does this token check out — a per-service concern), and **issuance** (mint a
credential, manage the session). We also need fast, stateless-to-verify tokens at the edge *and*
the ability to revoke instantly — two goals that normally fight (stateless tokens can't be
recalled).

## Decision

`auth` is a distinct context that **federates an external IdP (Keycloak) for credentials** and
**issues the platform's own short-lived ES256 edge tokens**. It is separate from `account` (the
identity SoR) and from `auth-context` (the in-process verify library every service uses — a verify,
not a call). Instant revocation despite stateless verification is achieved with a **monotonic
per-subject `Generation`**: bumping it invalidates a whole token family; refresh tokens are
single-use (rotation invalidates the prior).

## Consequences

- **Positive:** verification is cheap and decentralized (`auth-context`, no call to auth per
  request); credentials reuse a hardened IdP; revocation is immediate via generation bump.
- **Negative / accepted trade-off:** the generation counter must be checked at verify time
  (a small lookup) for revocation to be timely; token lifetime tuning trades revocation latency
  against verification cost.
- **Closes:** the conflation of account/verify/issuance; the stateless-vs-revocable tension.

## Amendment 2026-09-15 — the `pids` claim and the client edge

The client-facing surface is keyed by **profile** id while `sub` is the **account** id, and
one account owns N profiles. Rather than a per-request lookup (a hop the decision above
rules out) or an acting-profile switch flow, `auth` mints the profiles the account owns
into a `pids` claim at every login and refresh (read from `profile` via
`ListProfilesByAccount`; **fail-safe** — an outage mints an empty claim, never a failed
login). Services bind a request's profile-keyed actor with `require_profile` (must be in
`pids`) and an account-keyed one with `require_account` (must equal `sub`), all in-process
(`transport::grpc::edge`). Verification itself moved from two services (realtime, audit)
to every client-facing server's **edge listener** (`:9443`), via the shared
`auth-context::spawn_edge_decoder` and the `platform_edge` extractor (which reads `perms`;
the default OIDC extractor silently dropped it). Still open from the consequences above:
the **generation lookup** at verify time — revocation remains bounded by the 10-minute
access TTL.

## Amendment 2026-10-04 — the `did` claim

The realtime gateway keys a socket by device and rejects a handshake whose token has no
device claim, but `auth` never minted one, so no real token could open a socket. `auth`
now mints `did`, the `DeviceContext.device_id` the session was bound to at login, on every
login and refresh (`auth_context::edge::EDGE_DEVICE_CLAIM`, realtime's default
`REALTIME_DEVICE_CLAIM`). It comes from the session, not the refresh request, so it stays
stable for the session's life. It is omitted when the client sent no device id, and such a
token still fails the realtime handshake.

## Alternatives rejected

| Option | Why rejected |
|---|---|
| Opaque tokens verified by calling auth per request | Reintroduces a synchronous hop on every authenticated call |
| Long-lived tokens, no generation | No instant revocation; a leaked token stays valid until expiry |
| Build our own credential store instead of federating | Re-solves a generic, security-sensitive problem an IdP already solves |

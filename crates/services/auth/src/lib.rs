//! `auth` — the platform's issuance / session boundary.
//!
//! This service owns the **authentication act and its lifecycle** (sessions,
//! refresh-token rotation, revocation, edge-token minting, and the IdP-subject ↔
//! `account_id` linkage). It is deliberately distinct from its two neighbours:
//!
//! * [`account`](../account) — the identity **System of Record** (who a person
//!   *is*). `auth` *reads* it to gate session issuance; it never stores a second
//!   copy of the user.
//! * [`auth-context`](../../platform/auth-context) — the inbound **verification**
//!   library every service uses to validate an edge token on the hot path. `auth`
//!   *mints* the tokens `auth-context` verifies.
//!
//! Nothing signs in with a password and there is no external IdP (#881):
//! auth checks every credential itself — Apple / Google id_tokens against the
//! provider's keys, one-time codes, passkeys (staff: staff passkeys only).
//!
//! ## Module roadmap (built phase by phase)
//! Phase 0 (now): [`error`] — the canonical `AUT-XXXX` namespace.
//! Phase 2: `domain` · Phase 3: `application` + ports · Phase 4: `infrastructure`
//! · Phase 5: `app` (composition root) + `service` (runtime wiring).

pub mod app;
pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod service;

pub use error::AuthError;

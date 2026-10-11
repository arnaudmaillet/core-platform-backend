//! Edge-token revocation at verification time (#884).
//!
//! An edge token is verified locally (signature + `exp`), so a sign-out, a
//! suspension or a revoked role in `auth` used to leave it working until it
//! expired. Where a [`Revocations`] is attached — the client edge layer and the
//! [`StaffGate`](crate::grpc::edge::StaffGate) — a verified token is also checked
//! against `auth`'s revocation state: its `gen` must not be below the account's
//! current generation, and its session must not be blacklisted.
//!
//! The lookup is behind [`RevocationCheck`] (the Redis one lives in
//! `service-runtime`). When it cannot answer, the posture decides
//! ([`RevocationFailure`]): `Closed` refuses the call (`UNAVAILABLE`), `Open`
//! lets it through with a warning — the token still dies at `exp`.

use std::sync::Arc;

use auth_context::{edge, OidcClaims};
use futures::future::BoxFuture;
use tonic::Status;

/// The revocation state could not be read (store unreachable, timeout).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevocationUnavailable;

/// A revocation lookup in flight: `true` when the token is revoked.
pub type RevocationLookup<'a> = BoxFuture<'a, Result<bool, RevocationUnavailable>>;

/// Whether a token is revoked.
pub trait RevocationCheck: Send + Sync + 'static {
    /// `owner` is the id the generation is kept under (the account, or a
    /// guest's id), `session` the token's `sid`, `generation` its `gen`.
    fn is_revoked<'a>(&'a self, owner: &'a str, session: &'a str, generation: i64) -> RevocationLookup<'a>;
}

/// What happens when the revocation state cannot be read: the service tier's
/// posture (TIER-0 closed, the rest open).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevocationFailure {
    /// Refuse the call (`UNAVAILABLE`).
    Closed,
    /// Let it through with a warning; the token still dies at `exp`.
    Open,
}

/// Why a verified token is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevocationRefusal {
    /// Revoked, or not a token `auth` minted (no `sid` / `gen`).
    Revoked,
    /// The state could not be read and the posture is [`RevocationFailure::Closed`].
    Unavailable,
}

impl RevocationRefusal {
    /// The metric label (`infra_edge_rejected{reason}`).
    pub fn reason(self) -> &'static str {
        match self {
            Self::Revoked => "revoked_token",
            Self::Unavailable => "revocation_unavailable",
        }
    }

    pub fn status(self) -> Status {
        match self {
            Self::Revoked => Status::unauthenticated("token revoked"),
            Self::Unavailable => Status::unavailable("the token cannot be checked right now; retry"),
        }
    }
}

/// A [`RevocationCheck`] with its failure posture.
#[derive(Clone)]
pub struct Revocations {
    check: Arc<dyn RevocationCheck>,
    on_failure: RevocationFailure,
}

impl Revocations {
    pub fn new(check: Arc<dyn RevocationCheck>, on_failure: RevocationFailure) -> Self {
        Self { check, on_failure }
    }

    /// The same check under another posture.
    pub fn with_failure(&self, on_failure: RevocationFailure) -> Self {
        Self { check: Arc::clone(&self.check), on_failure }
    }

    pub fn on_failure(&self) -> RevocationFailure {
        self.on_failure
    }

    /// `Ok` when the verified token may be used.
    pub async fn admit(&self, claims: &OidcClaims) -> Result<(), RevocationRefusal> {
        // `auth` mints `sid` and `gen` on every token: one without them cannot
        // be checked, so it is not one of ours.
        let (Some(session), Some(generation)) = (edge::session_id(claims), edge::generation(claims)) else {
            return Err(RevocationRefusal::Revoked);
        };
        match self.check.is_revoked(edge::generation_owner(claims), session, generation).await {
            Ok(false) => Ok(()),
            Ok(true) => Err(RevocationRefusal::Revoked),
            Err(RevocationUnavailable) => match self.on_failure {
                RevocationFailure::Closed => Err(RevocationRefusal::Unavailable),
                RevocationFailure::Open => {
                    tracing::warn!("edge: revocation state unreadable, token admitted until exp (fail open)");
                    Ok(())
                }
            },
        }
    }
}

impl std::fmt::Debug for Revocations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Revocations").field("on_failure", &self.on_failure).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Revoked when `gen` is below 2 or the session is `s-dead`; `None` = down.
    struct Fixed(Option<()>);

    impl RevocationCheck for Fixed {
        fn is_revoked<'a>(&'a self, owner: &'a str, session: &'a str, generation: i64) -> RevocationLookup<'a> {
            Box::pin(async move {
                assert!(!owner.starts_with("guest:"), "the guest prefix is stripped");
                self.0.ok_or(RevocationUnavailable)?;
                Ok(generation < 2 || session == "s-dead")
            })
        }
    }

    fn claims(extra: serde_json::Value) -> OidcClaims {
        let mut base = json!({ "sub": "acct-1", "exp": 4_102_444_800_i64 });
        base.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        serde_json::from_value(base).unwrap()
    }

    fn up(on_failure: RevocationFailure) -> Revocations {
        Revocations::new(Arc::new(Fixed(Some(()))), on_failure)
    }

    #[tokio::test]
    async fn a_current_token_passes_a_revoked_one_does_not() {
        let r = up(RevocationFailure::Closed);
        assert_eq!(r.admit(&claims(json!({ "sid": "s-1", "gen": 2 }))).await, Ok(()));
        assert_eq!(r.admit(&claims(json!({ "sid": "s-1", "gen": 1 }))).await, Err(RevocationRefusal::Revoked));
        assert_eq!(r.admit(&claims(json!({ "sid": "s-dead", "gen": 5 }))).await, Err(RevocationRefusal::Revoked));
    }

    #[tokio::test]
    async fn a_token_without_sid_or_gen_is_refused() {
        let r = up(RevocationFailure::Open);
        assert_eq!(r.admit(&claims(json!({ "gen": 2 }))).await, Err(RevocationRefusal::Revoked));
        assert_eq!(r.admit(&claims(json!({ "sid": "s-1" }))).await, Err(RevocationRefusal::Revoked));
    }

    #[tokio::test]
    async fn a_guest_token_is_checked_under_its_guest_id() {
        let mut guest = claims(json!({ "sid": "s-1", "gen": 3 }));
        guest.sub = "guest:g-1".into();
        assert_eq!(up(RevocationFailure::Closed).admit(&guest).await, Ok(()));
    }

    #[tokio::test]
    async fn an_unreadable_state_follows_the_posture() {
        let down = Revocations::new(Arc::new(Fixed(None)), RevocationFailure::Closed);
        let token = claims(json!({ "sid": "s-1", "gen": 2 }));
        assert_eq!(down.admit(&token).await, Err(RevocationRefusal::Unavailable));
        assert_eq!(down.with_failure(RevocationFailure::Open).admit(&token).await, Ok(()));
    }
}

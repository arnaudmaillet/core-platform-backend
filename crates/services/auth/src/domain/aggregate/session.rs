use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::event::{DomainEvent, SessionIssued, SessionRevoked};
use crate::domain::value_object::{
    AccessTokenClaims, AccountId, DeviceFingerprint, Generation, IdpSubject, Permission,
    RevocationReason, SessionId, SessionStatus, ProfileId};
use crate::error::AuthError;

/// Parameters to establish a new [`Session`].
#[derive(Debug, Clone)]
pub struct SessionIssueParams {
    pub account_id: AccountId,
    pub subject: IdpSubject,
    /// The account's current generation at issue time.
    pub generation: Generation,
    pub device: DeviceFingerprint,
    pub issued_at: DateTime<Utc>,
    /// Sliding expiry; must be after `issued_at` and at/under `absolute_expiry`.
    pub expires_at: DateTime<Utc>,
    /// Hard cap the session can never be extended past.
    pub absolute_expiry: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// The **Session** aggregate root — the authentication act and its lifecycle.
///
/// A session is the authority for minting edge access tokens. It is *not* the
/// user (that's the `account` service) and *not* the credential (that's the IdP).
///
/// # Invariants (enforced here)
/// 1. A token can be minted only while `status == Active` and the session has not
///    passed its expiry (`ensure_mintable`).
/// 2. A minted access token's lifetime is clamped to the session's horizon
///    (`min(expires_at, absolute_expiry)`), so token TTL ⊆ session TTL ⊆ cap
///    (`mint_access_token`).
/// 3. A session can never be extended beyond its `absolute_expiry` (`extend`).
/// 4. A session is valid for a request only if its `generation` still matches the
///    account's current generation (`is_valid_under`) — a global sign-out bump
///    invalidates it without touching this row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    id: SessionId,
    account_id: AccountId,
    subject: IdpSubject,
    generation: Generation,
    status: SessionStatus,
    device: DeviceFingerprint,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    absolute_expiry: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
    version: i64,

    #[serde(skip)]
    pending_events: Vec<DomainEvent>,
}

impl Session {
    // ─── Construction ────────────────────────────────────────────────────────

    /// Establishes a new `Active` session. Emits [`SessionIssued`].
    ///
    /// Rejects a non-positive lifetime or an `expires_at` beyond the absolute cap.
    pub fn issue(params: SessionIssueParams) -> Result<Self, AuthError> {
        if params.expires_at <= params.issued_at {
            return Err(AuthError::DomainViolation {
                field: "session.expires_at".into(),
                message: "session expiry must be after its issue time".into(),
            });
        }
        if params.absolute_expiry < params.expires_at {
            return Err(AuthError::DomainViolation {
                field: "session.absolute_expiry".into(),
                message: "absolute expiry must be at or after the sliding expiry".into(),
            });
        }

        let id = SessionId::new();
        let event = DomainEvent::SessionIssued(SessionIssued {
            session_id: id,
            account_id: params.account_id,
            subject: params.subject.clone(),
            generation: params.generation,
            issued_at: params.issued_at,
            expires_at: params.expires_at,
            absolute_expiry: params.absolute_expiry,
            occurred_at: params.issued_at,
            correlation_id: params.correlation_id,
        });

        Ok(Self {
            id,
            account_id: params.account_id,
            subject: params.subject,
            generation: params.generation,
            status: SessionStatus::Active,
            device: params.device,
            issued_at: params.issued_at,
            expires_at: params.expires_at,
            absolute_expiry: params.absolute_expiry,
            revoked_at: None,
            version: 0,
            pending_events: vec![event],
        })
    }

    /// Reconstructs a session from storage (no events emitted).
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id: SessionId,
        account_id: AccountId,
        subject: IdpSubject,
        generation: Generation,
        status: SessionStatus,
        device: DeviceFingerprint,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        absolute_expiry: DateTime<Utc>,
        revoked_at: Option<DateTime<Utc>>,
        version: i64,
    ) -> Self {
        Self {
            id,
            account_id,
            subject,
            generation,
            status,
            device,
            issued_at,
            expires_at,
            absolute_expiry,
            revoked_at,
            version,
            pending_events: Vec::new(),
        }
    }

    // ─── Queries ─────────────────────────────────────────────────────────────

    pub fn id(&self) -> SessionId {
        self.id
    }

    pub fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub fn subject(&self) -> &IdpSubject {
        &self.subject
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn status(&self) -> SessionStatus {
        self.status
    }

    pub fn device(&self) -> &DeviceFingerprint {
        &self.device
    }

    pub fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    pub fn absolute_expiry(&self) -> DateTime<Utc> {
        self.absolute_expiry
    }

    pub fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.revoked_at
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    /// Whether the session has passed its sliding or absolute expiry at `now`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at || now >= self.absolute_expiry
    }

    /// Invariant 4: a session backs a request only if it is `Active`, unexpired,
    /// and still on the account's current generation.
    pub fn is_valid_under(&self, current_generation: Generation, now: DateTime<Utc>) -> bool {
        self.status == SessionStatus::Active
            && !self.is_expired(now)
            && self.generation == current_generation
    }

    // ─── Commands ────────────────────────────────────────────────────────────

    /// Invariants 1 + 2: mints an access token clamped to the session horizon.
    ///
    /// `requested_ttl` is the desired edge-token lifetime; the returned claims'
    /// `expires_at` is `min(now + requested_ttl, expires_at, absolute_expiry)`.
    /// Fails if the session cannot currently mint (revoked / expired).
    pub fn mint_access_token(
        &self,
        now: DateTime<Utc>,
        requested_ttl: Duration,
        permissions: Vec<Permission>,
        profile_ids: Vec<ProfileId>,
    ) -> Result<AccessTokenClaims, AuthError> {
        self.ensure_mintable(now)?;

        let horizon = self.expires_at.min(self.absolute_expiry);
        let expires_at = (now + requested_ttl).min(horizon);
        if expires_at <= now {
            // Horizon already reached within this instant — treat as expired.
            return Err(AuthError::SessionExpired);
        }

        Ok(AccessTokenClaims::new(
            self.account_id,
            self.id,
            self.generation,
            permissions,
            profile_ids,
            self.device.device_id().map(str::to_owned),
            now,
            expires_at,
        ))
    }

    /// Invariant 3: slides the sliding expiry forward by `ttl`, never past the
    /// absolute cap. Requires a mintable (active, unexpired) session.
    pub fn extend(&mut self, now: DateTime<Utc>, ttl: Duration) -> Result<(), AuthError> {
        self.ensure_mintable(now)?;
        let proposed = (now + ttl).min(self.absolute_expiry);
        if proposed > self.expires_at {
            self.expires_at = proposed;
            self.touch();
        }
        Ok(())
    }

    /// Revokes the session (single-session logout, reuse-detection, or admin).
    /// Emits [`SessionRevoked`]. Illegal from a terminal state.
    pub fn revoke(
        &mut self,
        now: DateTime<Utc>,
        reason: RevocationReason,
        correlation_id: Uuid,
    ) -> Result<(), AuthError> {
        self.transition_to(SessionStatus::Revoked)?;
        self.revoked_at = Some(now);
        self.touch();
        self.pending_events.push(DomainEvent::SessionRevoked(SessionRevoked {
            session_id: self.id,
            account_id: self.account_id,
            generation: self.generation,
            reason,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Marks an `Active` session that has passed its expiry as `Expired`
    /// (housekeeping). No event — expiry is the absence of validity, not a fact
    /// the rest of the platform must react to.
    pub fn mark_expired(&mut self, now: DateTime<Utc>) -> Result<(), AuthError> {
        if !self.is_expired(now) {
            return Err(AuthError::DomainViolation {
                field: "session".into(),
                message: "session has not yet reached its expiry".into(),
            });
        }
        self.transition_to(SessionStatus::Expired)?;
        self.touch();
        Ok(())
    }

    /// Drains accumulated events for the unit-of-work to publish.
    pub fn drain_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    // ─── Internals ───────────────────────────────────────────────────────────

    fn ensure_mintable(&self, now: DateTime<Utc>) -> Result<(), AuthError> {
        match self.status {
            SessionStatus::Revoked => Err(AuthError::SessionRevoked),
            SessionStatus::Expired => Err(AuthError::SessionExpired),
            SessionStatus::Active if self.is_expired(now) => Err(AuthError::SessionExpired),
            SessionStatus::Active => Ok(()),
        }
    }

    fn transition_to(&mut self, next: SessionStatus) -> Result<(), AuthError> {
        if !self.status.can_transition_to(next) {
            return Err(AuthError::InvalidSessionTransition {
                from: self.status.to_string(),
                to: next.to_string(),
            });
        }
        self.status = next;
        Ok(())
    }

    fn touch(&mut self) {
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-25T12:00:00Z").unwrap().with_timezone(&Utc)
    }

    fn params() -> SessionIssueParams {
        let now = t0();
        SessionIssueParams {
            account_id: AccountId::from_uuid(Uuid::now_v7()),
            subject: IdpSubject::new("iss", "sub").unwrap(),
            generation: Generation::INITIAL,
            device: DeviceFingerprint::default(),
            issued_at: now,
            expires_at: now + Duration::minutes(30),
            absolute_expiry: now + Duration::hours(8),
            correlation_id: Uuid::now_v7(),
        }
    }

    fn active_session() -> Session {
        Session::issue(params()).unwrap()
    }

    #[test]
    fn issue_emits_event_and_starts_active() {
        let mut s = active_session();
        assert_eq!(s.status(), SessionStatus::Active);
        assert_eq!(s.version(), 0);
        let events = s.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "auth.session_issued");
        assert!(s.drain_events().is_empty(), "events drain once");
    }

    #[test]
    fn issue_rejects_non_positive_lifetime() {
        let mut p = params();
        p.expires_at = p.issued_at;
        assert!(matches!(Session::issue(p).unwrap_err(), AuthError::DomainViolation { .. }));
    }

    #[test]
    fn issue_rejects_expiry_beyond_absolute_cap() {
        let mut p = params();
        p.absolute_expiry = p.expires_at - Duration::seconds(1);
        assert!(matches!(Session::issue(p).unwrap_err(), AuthError::DomainViolation { .. }));
    }

    #[test]
    fn mint_clamps_to_sliding_expiry() {
        let s = active_session();
        let now = t0();
        // Ask for 2h, but sliding expiry is 30m away ⇒ clamp to 30m.
        let claims = s.mint_access_token(now, Duration::hours(2), vec![], vec![]).unwrap();
        assert_eq!(claims.expires_at, now + Duration::minutes(30));
        assert_eq!(claims.generation, s.generation());
        assert_eq!(claims.session_id, s.id());
    }

    #[test]
    fn mint_shorter_ttl_is_not_extended() {
        let s = active_session();
        let now = t0();
        let claims = s.mint_access_token(now, Duration::minutes(5), vec![], vec![]).unwrap();
        assert_eq!(claims.expires_at, now + Duration::minutes(5));
        assert_eq!(claims.expires_in_secs(now), 300);
    }

    #[test]
    fn mint_carries_permissions() {
        let s = active_session();
        let perms = vec![Permission::new("posts:write"), Permission::new("ROLE_ADMIN")];
        let claims = s.mint_access_token(t0(), Duration::minutes(5), perms.clone(), vec![]).unwrap();
        assert_eq!(claims.permissions, perms);
    }

    #[test]
    fn mint_carries_the_bound_device_id() {
        let mut p = params();
        p.device = DeviceFingerprint::new(None, None, Some("ios-install-1".into()));
        let s = Session::issue(p).unwrap();
        let claims = s.mint_access_token(t0(), Duration::minutes(5), vec![], vec![]).unwrap();
        assert_eq!(claims.device_id.as_deref(), Some("ios-install-1"));
    }

    #[test]
    fn mint_without_a_device_id_carries_none() {
        let claims =
            active_session().mint_access_token(t0(), Duration::minutes(5), vec![], vec![]).unwrap();
        assert_eq!(claims.device_id, None);
    }

    #[test]
    fn mint_fails_when_expired() {
        let s = active_session();
        let later = t0() + Duration::minutes(31);
        assert!(matches!(
            s.mint_access_token(later, Duration::minutes(5), vec![], vec![]).unwrap_err(),
            AuthError::SessionExpired
        ));
    }

    #[test]
    fn mint_fails_when_revoked() {
        let mut s = active_session();
        s.revoke(t0(), RevocationReason::Logout, Uuid::now_v7()).unwrap();
        assert!(matches!(
            s.mint_access_token(t0(), Duration::minutes(5), vec![], vec![]).unwrap_err(),
            AuthError::SessionRevoked
        ));
    }

    #[test]
    fn extend_never_exceeds_absolute_cap() {
        let mut s = active_session();
        let now = t0() + Duration::minutes(10);
        // Slide by 20h, but cap is 8h from issue ⇒ clamp to absolute_expiry.
        s.extend(now, Duration::hours(20)).unwrap();
        assert_eq!(s.expires_at(), s.absolute_expiry());
    }

    #[test]
    fn extend_slides_forward_within_cap() {
        let mut s = active_session();
        let before = s.expires_at();
        let now = t0() + Duration::minutes(10);
        s.extend(now, Duration::minutes(30)).unwrap();
        assert_eq!(s.expires_at(), now + Duration::minutes(30));
        assert!(s.expires_at() > before);
        assert_eq!(s.version(), 1, "extension bumps version");
    }

    #[test]
    fn extend_fails_on_revoked() {
        let mut s = active_session();
        s.revoke(t0(), RevocationReason::Logout, Uuid::now_v7()).unwrap();
        assert!(matches!(
            s.extend(t0(), Duration::minutes(30)).unwrap_err(),
            AuthError::SessionRevoked
        ));
    }

    #[test]
    fn revoke_emits_event_and_is_terminal() {
        let mut s = active_session();
        s.drain_events();
        s.revoke(t0(), RevocationReason::RefreshReuse, Uuid::now_v7()).unwrap();
        assert_eq!(s.status(), SessionStatus::Revoked);
        let events = s.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "auth.session_revoked");
    }

    #[test]
    fn double_revoke_is_illegal_transition() {
        let mut s = active_session();
        s.revoke(t0(), RevocationReason::Logout, Uuid::now_v7()).unwrap();
        assert!(matches!(
            s.revoke(t0(), RevocationReason::Logout, Uuid::now_v7()).unwrap_err(),
            AuthError::InvalidSessionTransition { .. }
        ));
    }

    #[test]
    fn mark_expired_requires_passing_expiry() {
        let mut s = active_session();
        // Not yet expired.
        assert!(matches!(
            s.mark_expired(t0()).unwrap_err(),
            AuthError::DomainViolation { .. }
        ));
        // After expiry it succeeds.
        s.mark_expired(t0() + Duration::hours(9)).unwrap();
        assert_eq!(s.status(), SessionStatus::Expired);
    }

    #[test]
    fn is_valid_under_checks_generation_status_and_time() {
        let s = active_session();
        let now = t0();
        let g = s.generation();
        // happy path
        assert!(s.is_valid_under(g, now));
        // stale generation ⇒ invalid (global logout happened)
        assert!(!s.is_valid_under(g.next(), now));
        // expired ⇒ invalid
        assert!(!s.is_valid_under(g, now + Duration::hours(9)));
    }

    #[test]
    fn is_valid_under_false_when_revoked() {
        let mut s = active_session();
        let g = s.generation();
        s.revoke(t0(), RevocationReason::Logout, Uuid::now_v7()).unwrap();
        assert!(!s.is_valid_under(g, t0()));
    }
}

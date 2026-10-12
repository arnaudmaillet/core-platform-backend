//! What Login and SignUp share once the account is known: issuing a member
//! session with its tokens, and ending the guest session the device was using.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::application::policy::SessionPolicy;
use crate::application::port::{
    profile_ids_or_empty, EventPublisher, GuestRegistry, ProfileDirectory, RefreshTokenRepository,
    SessionCache, SessionRepository, TokenMinter,
};
use crate::domain::aggregate::{RefreshToken, RefreshTokenIssueParams, Session, SessionIssueParams};
use crate::domain::value_object::{
    AccountId, AgeBracket, DeviceFingerprint, IdpSubject, Permission, RevocationReason, SessionId,
    SessionKind, SessionMethod, SessionSignIn, SessionStatus,
};
use crate::domain::event::{DomainEvent, GuestUpgraded};
use crate::error::AuthError;

/// A freshly issued member session (the plaintext refresh token appears once).
#[derive(Debug, Clone)]
pub struct MemberSession {
    pub session_id:        SessionId,
    pub access_token:      String,
    pub refresh_token:     String,
    pub access_expires_in: i64,
}

/// How a member session was opened, as far as minting is concerned (#880).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOrigin {
    pub sign_in:       SessionSignIn,
    /// Opened with a staff-enrolled passkey: the account's staff grants ride.
    pub staff_passkey: bool,
}

impl SessionOrigin {
    /// Opened with a credential other than a passkey: member grants only.
    pub fn by(method: SessionMethod) -> Self {
        Self { sign_in: SessionSignIn::by(method), staff_passkey: false }
    }

    /// Opened with the identity `subject` proved (not a passkey).
    pub fn of_subject(subject: &crate::domain::value_object::IdpSubject) -> Self {
        Self { sign_in: SessionSignIn::from_parts(SessionMethod::of_subject(subject), None), staff_passkey: false }
    }

    /// Opened with this passkey.
    pub fn passkey(passkey: &super::passkeys::VerifiedPasskey) -> Self {
        Self { sign_in: SessionSignIn::by_passkey(passkey.credential_id.clone()), staff_passkey: passkey.staff_enrolled }
    }
}

/// Issues member sessions and retires guest sessions.
#[derive(Clone)]
pub struct MemberSessions {
    pub profiles:       Arc<dyn ProfileDirectory>,
    pub sessions:       Arc<dyn SessionRepository>,
    pub refresh_tokens: Arc<dyn RefreshTokenRepository>,
    pub cache:          Arc<dyn SessionCache>,
    pub minter:         Arc<dyn TokenMinter>,
    pub publisher:      Arc<dyn EventPublisher>,
    pub policy:         SessionPolicy,
}

impl MemberSessions {
    /// A session of kind `Member` under the account's current generation, its
    /// refresh token, and an edge token carrying `read:public`, the account's
    /// profiles (`pids`) and a fresh `auth_time` (a credential was just proven).
    /// `grants` are `account`'s; the staff ones ride only when `origin` is a
    /// staff-enrolled passkey (#880).
    #[allow(clippy::too_many_arguments)]
    pub async fn issue(
        &self,
        account_id: AccountId,
        subject: IdpSubject,
        device: DeviceFingerprint,
        grants: Vec<Permission>,
        origin: SessionOrigin,
        age_bracket: Option<AgeBracket>,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) -> Result<MemberSession, AuthError> {
        let generation = self.cache.current_generation(&account_id).await?;
        let mut session = Session::issue(SessionIssueParams {
            kind: SessionKind::Member,
            account_id,
            subject,
            generation,
            device,
            sign_in: origin.sign_in,
            issued_at: now,
            expires_at: now + self.policy.session_ttl,
            absolute_expiry: now + self.policy.absolute_ttl,
            correlation_id,
        })?;
        self.sessions.save(&session).await?;
        for event in &session.drain_events() {
            self.publisher.publish(event).await?;
        }

        let generated = self.minter.generate_refresh()?;
        let refresh = RefreshToken::issue(RefreshTokenIssueParams {
            session_id: session.id(),
            account_id,
            token_hash: generated.hash,
            issued_at: now,
            expires_at: now + self.policy.refresh_ttl,
        })?;
        self.refresh_tokens.save(&refresh).await?;

        // The profiles the account owns ride in the token (`pids`) so client-facing
        // services can bind profile-keyed actors to the caller. Fail-safe: an
        // outage mints a token with no profile grants, never a failed login.
        let profile_ids = profile_ids_or_empty(&self.profiles, &account_id).await;
        // Every member may read public content (the edge's `read:public` routes);
        // staff grants only in a staff passkey session (#880).
        let permissions = Permission::for_session(grants, origin.staff_passkey);
        let mut claims = session.mint_access_token(now, self.policy.access_ttl, permissions, profile_ids)?;
        // The credential was proved just now: the token counts as a recent
        // authentication for step-up-gated RPCs (until it is refreshed).
        claims.auth_time = Some(now);
        claims.age_bracket = age_bracket;
        let access_token = self.minter.mint_access(&claims).await?;

        Ok(MemberSession {
            session_id: session.id(),
            access_token,
            refresh_token: generated.plaintext,
            access_expires_in: claims.expires_in_secs(now),
        })
    }

    /// Ends the guest session `guest_refresh_token` belongs to — its device just
    /// signed up or signed in as `account_id` — and records the upgrade.
    ///
    /// Best effort: the member session is what the caller asked for, so an
    /// unknown, expired or non-guest token, or a storage hiccup, is logged and
    /// ignored (the guest session then simply expires).
    pub async fn retire_guest(
        &self,
        guests: &dyn GuestRegistry,
        guest_refresh_token: &str,
        account_id: AccountId,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) {
        if let Err(e) = self.try_retire_guest(guests, guest_refresh_token, account_id, now, correlation_id).await {
            tracing::warn!(error = %e, "guest session not retired (it will expire)");
        }
    }

    async fn try_retire_guest(
        &self,
        guests: &dyn GuestRegistry,
        guest_refresh_token: &str,
        account_id: AccountId,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) -> Result<(), AuthError> {
        let hash = self.minter.hash_refresh(guest_refresh_token)?;
        let Some(token) = self.refresh_tokens.find_by_hash(&hash).await? else {
            return Ok(());
        };
        let Some(mut session) = self.sessions.find_by_id(&token.session_id()).await? else {
            return Ok(());
        };
        if session.kind() != SessionKind::Guest {
            return Ok(()); // never end a member session through this path
        }
        let guest_id = session.account_id();
        if session.status() == SessionStatus::Active {
            session.revoke(now, RevocationReason::GuestUpgraded, correlation_id)?;
            self.sessions.save(&session).await?;
            self.cache.blacklist_session(&session.id(), self.policy.access_ttl).await?;
            self.refresh_tokens.revoke_all_for_session(&session.id()).await?;
            // A guest session's events are not published (the audit plane
            // records accounts; a guest is none).
            let _ = session.drain_events();
        }
        // The first upgrade only: the wallet credits the guest's welcome
        // gift once per device from it (#827).
        if let Some(guest) = guests.mark_upgraded(&guest_id, &account_id, now).await? {
            let event = DomainEvent::GuestUpgraded(GuestUpgraded {
                account_id,
                device_key_hash: guest.device_key_hash(),
                guest_since: guest.first_seen_at,
                occurred_at: now,
                correlation_id,
            });
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}

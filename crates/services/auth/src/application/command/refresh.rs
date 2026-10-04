use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use crate::application::command::IssuedSession;
use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    profile_ids_or_empty, AccountActivation, AccountDirectory, EventPublisher, ProfileDirectory,
    RefreshTokenRepository, SessionCache, SessionRepository, TokenMinter,
};
use crate::domain::value_object::{
    AccountId, DeviceFingerprint, Permission, RevocationReason, SessionId, SessionKind, SessionStatus,
};
use crate::error::AuthError;

/// Rotate a refresh token and mint a fresh edge token.
#[derive(Debug, Clone)]
pub struct RefreshCommand {
    pub refresh_token: String,
    /// Optional device context, re-validated against the session's bound device.
    pub device: DeviceFingerprint,
}

impl Validate for RefreshCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.refresh_token.trim().is_empty() {
            return Err(vec![FieldViolation::new(
                "refresh_token",
                "AUT-VAL-010",
                "refresh_token must not be empty",
            )]);
        }
        Ok(())
    }
}

/// Orchestrates refresh-token rotation with reuse-detection. A re-presented
/// (already-rotated) token triggers a full session-generation revocation.
pub struct RefreshHandler {
    directory: Arc<dyn AccountDirectory>,
    profiles: Arc<dyn ProfileDirectory>,
    sessions: Arc<dyn SessionRepository>,
    refresh_tokens: Arc<dyn RefreshTokenRepository>,
    cache: Arc<dyn SessionCache>,
    minter: Arc<dyn TokenMinter>,
    publisher: Arc<dyn EventPublisher>,
    policy: SessionPolicy,
}

impl RefreshHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        directory: Arc<dyn AccountDirectory>,
        profiles: Arc<dyn ProfileDirectory>,
        sessions: Arc<dyn SessionRepository>,
        refresh_tokens: Arc<dyn RefreshTokenRepository>,
        cache: Arc<dyn SessionCache>,
        minter: Arc<dyn TokenMinter>,
        publisher: Arc<dyn EventPublisher>,
        policy: SessionPolicy,
    ) -> Self {
        Self { directory, profiles, sessions, refresh_tokens, cache, minter, publisher, policy }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<RefreshCommand>,
        now: DateTime<Utc>,
    ) -> Result<IssuedSession, AuthError> {
        ensure_valid(&envelope.payload)?;
        let cmd = envelope.payload;

        // 1. Resolve the presented token by its hash.
        let hash = self.minter.hash_refresh(&cmd.refresh_token)?;
        let mut token = self
            .refresh_tokens
            .find_by_hash(&hash)
            .await?
            .ok_or(AuthError::RefreshTokenNotFound)?;

        let session_id = token.session_id();
        let account_id = token.account_id();

        // 2. Rotate. Keep the successor plaintext to return; only its hash is
        //    persisted. A reuse signal escalates to revoking the whole generation.
        let generated = self.minter.generate_refresh()?;
        let new_plaintext = generated.plaintext;
        let successor = match token.rotate(now, generated.hash, now + self.policy.refresh_ttl) {
            Ok(successor) => successor,
            Err(AuthError::RefreshTokenReuseDetected) => {
                self.revoke_generation(now, account_id, session_id, envelope.correlation_id).await?;
                return Err(AuthError::RefreshTokenReuseDetected);
            }
            Err(other) => return Err(other),
        };

        // 3. Load the session and confirm it is still valid under the account's
        //    current generation (catches a global sign-out that bumped it).
        let mut session = self
            .sessions
            .find_by_id(&session_id)
            .await?
            .ok_or(AuthError::SessionNotFound { id: session_id.as_str() })?;

        let current_generation = self.cache.current_generation(&account_id).await?;
        if !session.is_valid_under(current_generation, now) {
            return Err(AuthError::SessionRevoked);
        }

        // 4. Best-effort device re-binding (fail-open when ids are absent).
        if !session.device().same_device_as(&cmd.device) {
            return Err(AuthError::DomainViolation {
                field: "device".into(),
                message: "refresh device does not match the session's bound device".into(),
            });
        }

        // 5. Re-read authoritative permissions; a deactivated account cannot
        //    refresh. A guest has no account: it keeps `read:public` only.
        //    The age bracket is re-read too: a birthday shows up at the next mint.
        let (permissions, age_bracket) = match session.kind() {
            SessionKind::Guest => (vec![Permission::read_public()], None),
            SessionKind::Member => {
                let snapshot = self.directory.lookup(&account_id).await?;
                match snapshot.activation {
                    AccountActivation::Active => {
                        (Permission::with_read_public(snapshot.permissions), snapshot.age_bracket)
                    }
                    // Only a fresh login resumes a deactivated account.
                    AccountActivation::Deactivated => {
                        return Err(AuthError::AccountNotActive { current: "deactivated".into() });
                    }
                    AccountActivation::Inactive { reason } => {
                        return Err(AuthError::AccountNotActive { current: reason });
                    }
                }
            }
        };

        // 6. Persist the rotation, slide the window, mint the new pair.
        self.refresh_tokens.save(&token).await?; // original, now Rotated
        self.refresh_tokens.save(&successor).await?; // successor, Active
        session.extend(now, self.policy.session_ttl)?;
        self.sessions.save(&session).await?;

        // Re-read the owned profiles too: a profile created since the last mint
        // becomes actionable at the next refresh (fail-safe, see login).
        let profile_ids = match session.kind() {
            SessionKind::Guest => Vec::new(),
            SessionKind::Member => profile_ids_or_empty(&self.profiles, &account_id).await,
        };
        let mut claims =
            session.mint_access_token(now, self.policy.access_ttl, permissions, profile_ids)?;
        claims.age_bracket = age_bracket;
        let access_token = self.minter.mint_access(&claims).await?;

        Ok(IssuedSession {
            account_id,
            session_id,
            access_token,
            refresh_token: new_plaintext,
            access_expires_in: claims.expires_in_secs(now),
            first_link: false,
            reactivated: false,
        })
    }

    /// Reuse-detected: bump the account generation (instant global kill), revoke
    /// the session and all its refresh tokens, and emit the revocation.
    async fn revoke_generation(
        &self,
        now: DateTime<Utc>,
        account_id: AccountId,
        session_id: SessionId,
        correlation_id: uuid::Uuid,
    ) -> Result<(), AuthError> {
        self.cache.bump_generation(&account_id).await?;
        self.refresh_tokens.revoke_all_for_session(&session_id).await?;
        if let Some(mut session) = self.sessions.find_by_id(&session_id).await?
            && session.status() == SessionStatus::Active {
                session.revoke(now, RevocationReason::RefreshReuse, correlation_id)?;
                self.sessions.save(&session).await?;
                self.cache.blacklist_session(&session_id, self.policy.access_ttl).await?;
                for event in &session.drain_events() {
                    self.publisher.publish(event).await?;
                }
            }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{LoginCommand, LoginHandler};
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::Generation;
    use chrono::Duration;
    use uuid::Uuid;

    async fn login(fx: &Fixture) -> crate::application::command::IssuedSession {
        let env = Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: AuthnGrant::Password { username: "u".into(), password: "p".into() },
                device: DeviceFingerprint::default(),
            },
        );
        login_handler(fx).handle(env, t0()).await.unwrap()
    }

    fn login_handler(fx: &Fixture) -> LoginHandler {
        fx.login_handler()
    }

    fn refresh_env(token: &str) -> Envelope<RefreshCommand> {
        Envelope::new(
            Uuid::now_v7(),
            RefreshCommand { refresh_token: token.to_owned(), device: DeviceFingerprint::default() },
        )
    }

    #[tokio::test]
    async fn refresh_rotates_to_a_new_pair() {
        let fx = Fixture::new();
        let first = login(&fx).await;

        let now = t0() + Duration::minutes(1);
        let second = fx.refresh_handler().handle(refresh_env(&first.refresh_token), now).await.unwrap();

        assert_eq!(second.account_id, first.account_id);
        assert_eq!(second.session_id, first.session_id);
        assert_ne!(second.refresh_token, first.refresh_token, "refresh token rotates");
        assert!(!second.access_token.is_empty());
        assert!(!second.first_link);
    }

    #[tokio::test]
    async fn refresh_then_reuse_old_token_detects_and_revokes_generation() {
        let fx = Fixture::new();
        let first = login(&fx).await;
        let account = first.account_id;

        // Legitimate rotation.
        let _second = fx
            .refresh_handler()
            .handle(refresh_env(&first.refresh_token), t0() + Duration::minutes(1))
            .await
            .unwrap();

        // Re-presenting the original (now rotated) token = theft signal.
        let err = fx
            .refresh_handler()
            .handle(refresh_env(&first.refresh_token), t0() + Duration::minutes(2))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::RefreshTokenReuseDetected));

        // Generation bumped (global kill) and the session revoked.
        assert_eq!(fx.cache.current_generation(&account).await.unwrap(), Generation::INITIAL.next());
        assert!(fx.sessions.list_active_by_account(&account).await.unwrap().is_empty());
        assert!(fx.publisher.event_types().contains(&"auth.session_revoked"));
    }

    /// Deactivating ends every device's sessions at its next refresh; only a
    /// fresh login (the holder's credential again) resumes the account.
    #[tokio::test]
    async fn refresh_on_a_deactivated_account_is_refused_and_does_not_resume_it() {
        use crate::application::port::AccountActivation;
        let fx = Fixture::new();
        let first = login(&fx).await;
        fx.directory.set_activation(&first.account_id, AccountActivation::Deactivated);

        let err = fx
            .refresh_handler()
            .handle(refresh_env(&first.refresh_token), t0() + Duration::minutes(1))
            .await
            .unwrap_err();

        assert!(matches!(err, AuthError::AccountNotActive { .. }));
        assert!(fx.directory.resumed().is_empty());
    }

    /// The age bracket is re-read at every mint: a 13–15 holder who turns 16
    /// carries the new bracket from the next refresh.
    #[tokio::test]
    async fn the_age_bracket_rides_in_the_token_and_follows_birthdays() {
        use crate::domain::value_object::AgeBracket;
        let fx = Fixture::new();
        let first = login(&fx).await;
        assert_eq!(fx.minter.verify_access(&first.access_token).await.unwrap().age_bracket, None);

        fx.directory.set_age_bracket(&first.account_id, Some(AgeBracket::Teen16To17));
        let next = fx
            .refresh_handler()
            .handle(refresh_env(&first.refresh_token), t0() + Duration::minutes(1))
            .await
            .unwrap();
        let claims = fx.minter.verify_access(&next.access_token).await.unwrap();
        assert_eq!(claims.age_bracket, Some(AgeBracket::Teen16To17));
    }

    #[tokio::test]
    async fn refresh_unknown_token_is_not_found() {
        let fx = Fixture::new();
        let err = fx.refresh_handler().handle(refresh_env("nope"), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::RefreshTokenNotFound));
    }

    #[tokio::test]
    async fn refresh_after_global_logout_is_rejected() {
        let fx = Fixture::new();
        let issued = login(&fx).await;

        fx.logout_all_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    crate::application::command::LogoutAllSessionsCommand {
                        account_id: issued.account_id.as_str(),
                    },
                ),
                t0() + Duration::minutes(1),
            )
            .await
            .unwrap();

        let err = fx
            .refresh_handler()
            .handle(refresh_env(&issued.refresh_token), t0() + Duration::minutes(2))
            .await
            .unwrap_err();
        // The global logout invalidated the session's refresh tokens, so the
        // presented handle no longer resolves.
        assert!(matches!(err, AuthError::RefreshTokenNotFound));
    }
}

//! `account` changes that end an account's sessions (#884): a suspension, a
//! self-service deactivation, a revoked role. auth signs the account out
//! everywhere, as `LogoutAllSessions` does: a new token generation (every token
//! minted before carries a stale one), then each active session revoked and
//! blacklisted with its refresh tokens. Without it, a suspended account kept its
//! sessions until the next refresh, and a revoked role's permissions stayed in
//! the token until the next mint.
//!
//! Events arrive late and may be redelivered. A suspension or a deactivation is
//! skipped only when `account` reports the account active again (reactivated by
//! an admin, or resumed by its holder signing back in, since the event was
//! published): it keeps the sessions it has now. When `account` cannot be asked,
//! the account is signed out anyway — fail closed: a needless sign-out costs a
//! sign-in, a missed suspension leaves access. A revoked role is always acted on:
//! at worst the holder signs in again.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::application::command::{LogoutAllSessionsHandler, LogoutAllSessionsOutcome};
use crate::application::port::AccountDirectory;
use crate::domain::value_object::{AccountId, RevocationReason};
use crate::error::AuthError;

/// The `account.v1.events` change auth reacts to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountChange {
    Suspended,
    Deactivated,
    RoleRevoked,
}

impl AccountChange {
    fn reason(self) -> RevocationReason {
        match self {
            Self::Suspended => RevocationReason::AccountSuspended,
            Self::Deactivated => RevocationReason::AccountDeactivated,
            Self::RoleRevoked => RevocationReason::RoleRevoked,
        }
    }
}

pub struct AccountRevocation {
    sign_out:  Arc<LogoutAllSessionsHandler>,
    directory: Arc<dyn AccountDirectory>,
}

impl AccountRevocation {
    pub fn new(sign_out: Arc<LogoutAllSessionsHandler>, directory: Arc<dyn AccountDirectory>) -> Self {
        Self { sign_out, directory }
    }

    /// `None` when the change is stale (the account is active again). Errors are
    /// the session stores'; the retryable ones leave the event for a retry.
    pub async fn apply(
        &self,
        account_id: &AccountId,
        change: AccountChange,
        correlation_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<LogoutAllSessionsOutcome>, AuthError> {
        if matches!(change, AccountChange::Suspended | AccountChange::Deactivated) {
            match self.directory.lookup(account_id).await {
                Ok(snapshot) if snapshot.activation.is_active() => {
                    tracing::info!(account.id = %account_id.as_str(), ?change, "stale account change: active again, sessions kept");
                    return Ok(None);
                }
                // Still inactive, or gone (`not_found`): sign it out.
                Ok(_) | Err(AuthError::AccountNotActive { .. }) => {}
                Err(error) => {
                    tracing::warn!(account.id = %account_id.as_str(), ?change, %error, "account unreachable: signing out anyway");
                }
            }
        }
        let outcome = self.sign_out.sign_out_everywhere(account_id, change.reason(), correlation_id, now).await?;
        tracing::info!(
            account.id = %account_id.as_str(),
            ?change,
            sessions = outcome.sessions_revoked,
            "account signed out everywhere"
        );
        Ok(Some(outcome))
    }
}

#[cfg(test)]
mod tests {
    use cqrs::Envelope;

    use super::*;
    use crate::application::command::{IssuedSession, LoginCommand, RefreshCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::{AccountActivation, SessionCache, SessionRepository};
    use crate::domain::value_object::DeviceFingerprint;

    async fn login(fx: &Fixture) -> IssuedSession {
        let env = Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: crate::application::fakes::member_grant(),
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            },
        );
        fx.login_handler().handle(env, t0()).await.unwrap().issued().unwrap()
    }

    fn revocation(fx: &Fixture) -> AccountRevocation {
        AccountRevocation::new(Arc::new(fx.logout_all_handler()), fx.directory.clone())
    }

    async fn refresh(fx: &Fixture, session: &IssuedSession) -> Result<IssuedSession, AuthError> {
        let cmd = RefreshCommand { refresh_token: session.refresh_token.clone(), device: DeviceFingerprint::default() };
        fx.refresh_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await
    }

    #[tokio::test]
    async fn a_suspension_signs_the_account_out_everywhere() {
        let fx = Fixture::new();
        let a = login(&fx).await;
        let _b = login(&fx).await;
        let before = fx.cache.current_generation(&a.account_id).await.unwrap();
        fx.directory.set_activation(&a.account_id, AccountActivation::Inactive { reason: "suspended".into() });

        let out = revocation(&fx).apply(&a.account_id, AccountChange::Suspended, Uuid::now_v7(), t0()).await.unwrap();

        assert_eq!(out.unwrap().sessions_revoked, 2);
        assert_ne!(fx.cache.current_generation(&a.account_id).await.unwrap(), before, "tokens minted before are cut");
        assert!(fx.sessions.list_active_by_account(&a.account_id).await.unwrap().is_empty());
        assert!(fx.cache.is_blacklisted(&a.session_id).await.unwrap());
        assert!(refresh(&fx, &a).await.is_err(), "the refresh token died with the session");
    }

    #[tokio::test]
    async fn a_stale_suspension_or_deactivation_keeps_the_sessions_of_an_account_active_again() {
        let fx = Fixture::new();
        let a = login(&fx).await;
        let before = fx.cache.current_generation(&a.account_id).await.unwrap();

        for change in [AccountChange::Suspended, AccountChange::Deactivated] {
            let out = revocation(&fx).apply(&a.account_id, change, Uuid::now_v7(), t0()).await.unwrap();
            assert!(out.is_none(), "{change:?}: the account is active again");
        }
        assert_eq!(fx.cache.current_generation(&a.account_id).await.unwrap(), before);
        assert!(refresh(&fx, &a).await.is_ok());
    }

    /// A revoked role is acted on without asking `account`: the permissions it
    /// granted are in every token minted before.
    #[tokio::test]
    async fn a_revoked_role_cuts_the_tokens_of_an_active_account() {
        let fx = Fixture::new();
        let a = login(&fx).await;
        let lookups = fx.directory.lookups().len();

        let out = revocation(&fx).apply(&a.account_id, AccountChange::RoleRevoked, Uuid::now_v7(), t0()).await.unwrap();

        assert_eq!(out.unwrap().sessions_revoked, 1);
        assert_eq!(fx.directory.lookups().len(), lookups, "no lookup");
        assert!(refresh(&fx, &a).await.is_err(), "the refresh token died with the session");
        let reasons = fx.publisher.event_types();
        assert!(reasons.contains(&"auth.session_revoked"));
    }

    /// `account` unreachable: the suspension is applied anyway (fail closed).
    #[tokio::test]
    async fn a_suspension_signs_out_even_when_account_cannot_be_asked() {
        let fx = Fixture::new();
        let a = login(&fx).await;
        fx.directory.lookup_down(true);

        let out = revocation(&fx).apply(&a.account_id, AccountChange::Suspended, Uuid::now_v7(), t0()).await.unwrap();

        assert_eq!(out.unwrap().sessions_revoked, 1);
        assert!(fx.sessions.list_active_by_account(&a.account_id).await.unwrap().is_empty());
    }

    /// A deactivated account is signed out; a redelivery finds nothing left.
    #[tokio::test]
    async fn a_deactivation_signs_out_and_a_redelivery_is_harmless() {
        let fx = Fixture::new();
        let a = login(&fx).await;
        fx.directory.set_activation(&a.account_id, AccountActivation::Deactivated);
        let revocation = revocation(&fx);

        let first = revocation.apply(&a.account_id, AccountChange::Deactivated, Uuid::now_v7(), t0()).await.unwrap();
        let again = revocation.apply(&a.account_id, AccountChange::Deactivated, Uuid::now_v7(), t0()).await.unwrap();

        assert_eq!(first.unwrap().sessions_revoked, 1);
        assert_eq!(again.unwrap().sessions_revoked, 0);
    }
}

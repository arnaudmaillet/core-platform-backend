//! GDPR erasure (Art. 17). `account` deletes an account at the end of its grace
//! period and publishes `account_deleted`; auth then deletes what it holds about
//! it: sessions and refresh tokens (device, IP), identity links (for an email or
//! phone code identity the subject *is* the address), and the guest it was
//! before signing up with that guest's sessions. Its access tokens are cut first
//! (a new token generation), so nothing minted before keeps working.
//!
//! The fleet's IdP (Keycloak) holds what auth does not: the user's email,
//! username and password hash. For every link to it, the IdP user is deleted
//! **before** the local rows — the link is the only record of the IdP user id —
//! and an IdP failure aborts the erasure untouched, so the redelivery finds the
//! links again. Apple / Google (verified natively) and the code identities have
//! no IdP user.

use std::sync::Arc;

use crate::application::command::GUEST_ISSUER;
use crate::application::port::{AccountEraser, CredentialAdmin, ErasedAccount, SessionCache, SubjectLinkRepository};
use crate::domain::value_object::{AccountId, SignInMethod};
use crate::error::AuthError;

pub struct AccountErasure {
    eraser:      Arc<dyn AccountEraser>,
    cache:       Arc<dyn SessionCache>,
    links:       Arc<dyn SubjectLinkRepository>,
    credentials: Arc<dyn CredentialAdmin>,
}

impl AccountErasure {
    pub fn new(
        eraser: Arc<dyn AccountEraser>,
        cache: Arc<dyn SessionCache>,
        links: Arc<dyn SubjectLinkRepository>,
        credentials: Arc<dyn CredentialAdmin>,
    ) -> Self {
        Self { eraser, cache, links, credentials }
    }

    /// Idempotent: a replayed `account_deleted` erases nothing more. Errors
    /// (IdP, storage) are retryable and leave the local rows for the retry.
    pub async fn erase(&self, account_id: &AccountId) -> Result<ErasedAccount, AuthError> {
        self.cache.bump_generation(account_id).await?;
        for link in self.links.find_by_account(account_id).await? {
            let subject = link.subject();
            let idp_managed = subject.issuer() != GUEST_ISSUER
                && SignInMethod::from_issuer(subject.issuer()) == SignInMethod::Password;
            if idp_managed {
                self.credentials.delete_user(subject).await?;
            }
        }
        let erased = self.eraser.erase(account_id).await?;
        tracing::info!(
            account.id = %account_id.as_str(),
            sessions = erased.sessions,
            links = erased.links,
            guests = erased.guests,
            "account erased from auth"
        );
        Ok(erased)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::fakes::Fixture;
    use crate::application::port::SubjectLinkRepository;
    use crate::domain::aggregate::SubjectLink;
    use crate::domain::value_object::IdpSubject;
    use error::AppError;

    #[derive(Default)]
    struct RecordingEraser(Mutex<Vec<AccountId>>);

    #[async_trait]
    impl AccountEraser for RecordingEraser {
        async fn erase(&self, account_id: &AccountId) -> Result<ErasedAccount, AuthError> {
            self.0.lock().unwrap().push(*account_id);
            Ok(ErasedAccount { sessions: 2, links: 1, guests: 1 })
        }
    }

    fn erasure(fx: &Fixture, eraser: &Arc<RecordingEraser>) -> AccountErasure {
        AccountErasure::new(Arc::clone(eraser) as _, fx.cache.clone(), fx.links.clone(), fx.credentials.clone())
    }

    async fn link(fx: &Fixture, issuer: &str, subject: &str, account: AccountId) -> IdpSubject {
        let subject = IdpSubject::new(issuer, subject).unwrap();
        let link = SubjectLink::establish(subject.clone(), account, chrono::Utc::now(), uuid::Uuid::now_v7());
        fx.links.save(&link).await.unwrap();
        subject
    }

    #[tokio::test]
    async fn the_idp_user_goes_first_then_the_rows_and_an_idp_failure_keeps_the_rows() {
        let fx = Fixture::new();
        let eraser = Arc::new(RecordingEraser::default());
        let account = AccountId::from_uuid(uuid::Uuid::now_v7());
        let keycloak = link(&fx, "https://sso.example/realms/core", "kc-user-1", account).await;
        link(&fx, crate::domain::value_object::APPLE_ISSUER, "001.apple", account).await;
        link(&fx, crate::domain::value_object::EMAIL_CODE_ISSUER, "ada@example.com", account).await;

        // The IdP is down: nothing is deleted locally, the error is retryable.
        fx.credentials.idp_down(true);
        let err = erasure(&fx, &eraser).erase(&account).await.unwrap_err();
        assert!(err.is_retryable());
        assert!(eraser.0.lock().unwrap().is_empty(), "the links stay for the retry");

        // The retry: only the Keycloak user is deleted at the IdP, then the rows.
        fx.credentials.idp_down(false);
        erasure(&fx, &eraser).erase(&account).await.unwrap();
        assert_eq!(fx.credentials.deleted_users(), vec![keycloak]);
        assert_eq!(*eraser.0.lock().unwrap(), vec![account]);
    }

    #[tokio::test]
    async fn erasure_cuts_the_tokens_then_deletes_the_data() {
        let fx = Fixture::new();
        let eraser = Arc::new(RecordingEraser::default());
        let erasure = erasure(&fx, &eraser);
        let account = AccountId::from_uuid(uuid::Uuid::now_v7());
        let before = fx.cache.current_generation(&account).await.unwrap();

        let erased = erasure.erase(&account).await.unwrap();
        assert_eq!(erased, ErasedAccount { sessions: 2, links: 1, guests: 1 });
        assert_eq!(*eraser.0.lock().unwrap(), vec![account]);
        assert_ne!(fx.cache.current_generation(&account).await.unwrap(), before, "tokens minted before are cut");
    }
}

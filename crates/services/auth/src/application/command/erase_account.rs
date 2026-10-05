//! GDPR erasure (Art. 17). `account` deletes an account at the end of its grace
//! period and publishes `account_deleted`; auth then deletes what it holds about
//! it: sessions and refresh tokens (device, IP), identity links (for an email or
//! phone code identity the subject *is* the address), and the guest it was
//! before signing up with that guest's sessions. Its access tokens are cut first
//! (a new token generation), so nothing minted before keeps working.

use std::sync::Arc;

use crate::application::port::{AccountEraser, ErasedAccount, SessionCache};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

pub struct AccountErasure {
    eraser: Arc<dyn AccountEraser>,
    cache:  Arc<dyn SessionCache>,
}

impl AccountErasure {
    pub fn new(eraser: Arc<dyn AccountEraser>, cache: Arc<dyn SessionCache>) -> Self {
        Self { eraser, cache }
    }

    /// Idempotent: a replayed `account_deleted` erases nothing more.
    pub async fn erase(&self, account_id: &AccountId) -> Result<ErasedAccount, AuthError> {
        self.cache.bump_generation(account_id).await?;
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

    #[derive(Default)]
    struct RecordingEraser(Mutex<Vec<AccountId>>);

    #[async_trait]
    impl AccountEraser for RecordingEraser {
        async fn erase(&self, account_id: &AccountId) -> Result<ErasedAccount, AuthError> {
            self.0.lock().unwrap().push(*account_id);
            Ok(ErasedAccount { sessions: 2, links: 1, guests: 1 })
        }
    }

    #[tokio::test]
    async fn erasure_cuts_the_tokens_then_deletes_the_data() {
        let fx = Fixture::new();
        let eraser = Arc::new(RecordingEraser::default());
        let erasure = AccountErasure::new(Arc::clone(&eraser) as _, fx.cache.clone());
        let account = AccountId::from_uuid(uuid::Uuid::now_v7());
        let before = fx.cache.current_generation(&account).await.unwrap();

        let erased = erasure.erase(&account).await.unwrap();
        assert_eq!(erased, ErasedAccount { sessions: 2, links: 1, guests: 1 });
        assert_eq!(*eraser.0.lock().unwrap(), vec![account]);
        assert_ne!(fx.cache.current_generation(&account).await.unwrap(), before, "tokens minted before are cut");
    }
}

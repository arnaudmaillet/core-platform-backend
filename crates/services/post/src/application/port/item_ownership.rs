use async_trait::async_trait;

use crate::error::PostError;

/// What an account owns in the gem catalogue (#914: card styles), read from
/// the wallet. Failures are `WalletUnavailable`: applying a style fails closed.
#[async_trait]
pub trait ItemOwnership: Send + Sync + 'static {
    async fn owns(&self, account_id: &str, item_id: &str) -> Result<bool, PostError>;
}

use async_trait::async_trait;

use crate::domain::value_object::AccountId;
use crate::error::ProfileError;

/// media's private documents (#777): a verification request may only name
/// the requester's own, ready, `PRIVATE_DOCUMENT` assets.
#[async_trait]
pub trait PrivateDocuments: Send + Sync + 'static {
    /// `Ok` when `asset_id` is a READY private document owned by `account`;
    /// [`ProfileError::VerificationDocumentInvalid`] otherwise (missing,
    /// another account's, another kind, not ready);
    /// [`ProfileError::MediaUnavailable`] when media cannot answer.
    async fn check_owned(&self, asset_id: &str, account: &AccountId) -> Result<(), ProfileError>;
}

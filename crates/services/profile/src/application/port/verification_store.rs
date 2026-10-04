use async_trait::async_trait;

use crate::domain::entity::VerificationRequest;
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

/// Verification requests (#668): the latest per profile, and the queue of
/// pending ones for staff review.
#[async_trait]
pub trait VerificationStore: Send + Sync + 'static {
    async fn get(&self, profile: &ProfileId) -> Result<Option<VerificationRequest>, ProfileError>;

    /// Stores the request; a pending one joins the queue, a decided one leaves it.
    async fn put(&self, profile: &ProfileId, request: &VerificationRequest) -> Result<(), ProfileError>;

    /// The pending requests, oldest first. The page token is opaque.
    async fn pending(&self, limit: i32, page_token: Option<&str>) -> Result<(Vec<ProfileId>, Option<String>), ProfileError>;
}

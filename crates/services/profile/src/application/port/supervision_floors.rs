use async_trait::async_trait;

use crate::domain::value_object::{AccountId, SupervisionFloor};
use crate::error::ProfileError;

/// Each supervised account's floors (#670 part 2).
#[async_trait]
pub trait SupervisionFloors: Send + Sync + 'static {
    /// The account's floors; `None`: not supervised (or no limits).
    async fn get(&self, account: &AccountId) -> Result<Option<SupervisionFloor>, ProfileError>;
    async fn put(&self, account: &AccountId, floor: &SupervisionFloor) -> Result<(), ProfileError>;
    async fn clear(&self, account: &AccountId) -> Result<(), ProfileError>;
}

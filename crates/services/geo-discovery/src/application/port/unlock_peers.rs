use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// What the wallet answered to a gem spend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GemSpend {
    pub spent: bool,
    /// Gems left.
    pub gems:  i64,
}

/// The account's gems, in the wallet (#665): read, and spent for a country
/// (`SpendGems`, once per `key`). Failures are
/// `UnlockDependencyUnavailable` (an unlock fails closed).
#[async_trait]
pub trait GemWallet: Send + Sync + 'static {
    async fn gems(&self, account: Uuid) -> Result<i64, GeoDiscoveryError>;
    /// `end_user`: the spender's edge token, forwarded so the wallet checks
    /// the spender itself (#852).
    async fn spend_for_country(
        &self,
        account: Uuid,
        country: CountryCode,
        amount: i64,
        key: &str,
        end_user: Option<&str>,
    ) -> Result<GemSpend, GeoDiscoveryError>;
}

/// The account's country of residence (account), the first choice for the
/// home country.
#[async_trait]
pub trait ResidenceDirectory: Send + Sync + 'static {
    async fn residence(&self, account: Uuid) -> Result<Option<CountryCode>, GeoDiscoveryError>;
}

//! The unlocks' peers over the mesh (#665): the wallet (`GetWallet`,
//! `SpendGems`) and account (`GetAccountById`'s country of residence). Any
//! failure is `UnlockDependencyUnavailable`: an unlock fails closed.

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;
use uuid::Uuid;

use account_api::account_service_client::AccountServiceClient;
use wallet_api::wallet_service_client::WalletServiceClient;

use crate::application::port::{GemSpend, GemWallet, ResidenceDirectory};
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

fn unavailable(service: &'static str) -> impl Fn(tonic::Status) -> GeoDiscoveryError {
    move |status| GeoDiscoveryError::UnlockDependencyUnavailable { service, reason: status.to_string() }
}

pub struct GrpcGemWallet {
    wallet: WalletServiceClient<Channel>,
}

impl GrpcGemWallet {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { wallet: WalletServiceClient::new(channel) }
    }
}

#[async_trait]
impl GemWallet for GrpcGemWallet {
    async fn gems(&self, account: Uuid) -> Result<i64, GeoDiscoveryError> {
        let wallet = self
            .wallet
            .clone()
            .get_wallet(wallet_api::GetWalletRequest { account_id: account.to_string() })
            .await
            .map_err(unavailable("wallet"))?
            .into_inner();
        Ok(wallet.gems)
    }

    async fn spend_for_country(
        &self,
        account: Uuid,
        country: CountryCode,
        amount: i64,
        key: &str,
    ) -> Result<GemSpend, GeoDiscoveryError> {
        let answer = self
            .wallet
            .clone()
            .spend_gems(wallet_api::SpendGemsRequest {
                account_id:      account.to_string(),
                amount,
                kind:            wallet_api::TransactionKind::CountryUnlock as i32,
                ref_id:          country.to_string(),
                idempotency_key: key.to_owned(),
            })
            .await
            .map_err(unavailable("wallet"))?
            .into_inner();
        Ok(GemSpend { spent: answer.outcome == wallet_api::SpendGemsOutcome::Spent as i32, gems: answer.gems })
    }
}

pub struct GrpcResidenceDirectory {
    account: AccountServiceClient<Channel>,
}

impl GrpcResidenceDirectory {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { account: AccountServiceClient::new(channel) }
    }
}

#[async_trait]
impl ResidenceDirectory for GrpcResidenceDirectory {
    async fn residence(&self, account: Uuid) -> Result<Option<CountryCode>, GeoDiscoveryError> {
        match self
            .account
            .clone()
            .get_account_by_id(account_api::GetAccountByIdRequest { account_id: account.to_string() })
            .await
        {
            // An empty or unreadable residence tells nothing.
            Ok(view) => Ok(CountryCode::try_from(view.into_inner().country_of_residence.trim()).ok()),
            Err(status) if status.code() == Code::NotFound => Ok(None),
            Err(status) => Err(unavailable("account")(status)),
        }
    }
}

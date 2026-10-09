//! The unlocks' peers over the mesh (#665): the wallet (`GetWallet`,
//! `SpendGems`) and account (`GetAccountById`'s country of residence). Any
//! failure is `UnlockDependencyUnavailable`: an unlock fails closed.

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Code;
use uuid::Uuid;

use account_api::account_service_client::AccountServiceClient;
use tonic::service::interceptor::InterceptedService;
use transport::grpc::mesh::MeshTokenInterceptor;
use wallet_api::wallet_service_client::WalletServiceClient;

use crate::application::port::{GemSpend, GemWallet, ResidenceDirectory};
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

fn unavailable(service: &'static str) -> impl Fn(tonic::Status) -> GeoDiscoveryError {
    move |status| GeoDiscoveryError::UnlockDependencyUnavailable { service, reason: status.to_string() }
}

pub struct GrpcGemWallet {
    /// Every call carries this pod's mesh token (#852).
    wallet: WalletServiceClient<InterceptedService<Channel, MeshTokenInterceptor>>,
}

impl GrpcGemWallet {
    /// `channel` must carry request and connect timeouts; the mesh token
    /// comes from `MESH_TOKEN_FILE`.
    pub fn new(channel: Channel) -> Self {
        Self { wallet: WalletServiceClient::with_interceptor(channel, MeshTokenInterceptor::from_env()) }
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
        end_user: Option<&str>,
    ) -> Result<GemSpend, GeoDiscoveryError> {
        let mut request = tonic::Request::new(wallet_api::SpendGemsRequest {
            account_id:      account.to_string(),
            amount,
            kind:            wallet_api::TransactionKind::CountryUnlock as i32,
            ref_id:          country.to_string(),
            idempotency_key: key.to_owned(),
        });
        // The spender's own token: the wallet checks who spends (#852).
        if let Some(token) = end_user
            && let Ok(value) = format!("Bearer {token}").parse()
        {
            request.metadata_mut().insert("authorization", value);
        }
        let answer = self
            .wallet
            .clone()
            .spend_gems(request)
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

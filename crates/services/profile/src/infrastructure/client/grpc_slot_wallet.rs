//! [`SlotWallet`] over the wallet's mesh-only `SpendGems` (#916). Any failure
//! is `WalletUnavailable`: a slot purchase fails closed.

use async_trait::async_trait;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use transport::grpc::mesh::MeshTokenInterceptor;
use wallet_api::wallet_service_client::WalletServiceClient;

use crate::application::port::{SlotSpend, SlotWallet};
use crate::domain::value_object::AccountId;
use crate::error::ProfileError;

pub struct GrpcSlotWallet {
    /// Every call carries this pod's mesh token (#852).
    wallet: WalletServiceClient<InterceptedService<Channel, MeshTokenInterceptor>>,
}

impl GrpcSlotWallet {
    /// `channel` must carry request and connect timeouts; the mesh token
    /// comes from `MESH_TOKEN_FILE`.
    pub fn new(channel: Channel) -> Self {
        Self { wallet: WalletServiceClient::with_interceptor(channel, MeshTokenInterceptor::from_env()) }
    }
}

#[async_trait]
impl SlotWallet for GrpcSlotWallet {
    async fn spend_for_slot(
        &self,
        account: &AccountId,
        slot: i32,
        amount: i64,
        key: &str,
        end_user: Option<&str>,
    ) -> Result<SlotSpend, ProfileError> {
        let mut request = tonic::Request::new(wallet_api::SpendGemsRequest {
            account_id:      account.as_uuid().to_string(),
            amount,
            kind:            wallet_api::TransactionKind::ProfileSlot as i32,
            ref_id:          slot.to_string(),
            idempotency_key: key.to_owned(),
        });
        // The buyer's own token: the wallet checks who spends (#852).
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
            .map_err(|status| ProfileError::WalletUnavailable { reason: status.to_string() })?
            .into_inner();
        Ok(SlotSpend { spent: answer.outcome == wallet_api::SpendGemsOutcome::Spent as i32, gems: answer.gems })
    }
}

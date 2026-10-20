//! The wallet over the mesh (#914): `ListOwnedItems`, to check an author owns
//! the card style they apply.

use async_trait::async_trait;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use transport::grpc::mesh::MeshTokenInterceptor;
use wallet_api::wallet_service_client::WalletServiceClient;

use crate::application::port::ItemOwnership;
use crate::error::PostError;

pub struct GrpcWallet {
    /// Every call carries post's mesh token (#852).
    wallet: WalletServiceClient<InterceptedService<Channel, MeshTokenInterceptor>>,
}

impl GrpcWallet {
    /// `channel` must carry request and connect timeouts.
    pub fn new(channel: Channel) -> Self {
        Self { wallet: WalletServiceClient::with_interceptor(channel, MeshTokenInterceptor::from_env()) }
    }
}

#[async_trait]
impl ItemOwnership for GrpcWallet {
    async fn owns(&self, account_id: &str, item_id: &str) -> Result<bool, PostError> {
        let owned = self
            .wallet
            .clone()
            .list_owned_items(wallet_api::ListOwnedItemsRequest { account_id: account_id.to_owned() })
            .await
            .map_err(|status| PostError::WalletUnavailable { reason: status.to_string() })?
            .into_inner();
        Ok(owned.items.iter().any(|item| item.item_id == item_id))
    }
}

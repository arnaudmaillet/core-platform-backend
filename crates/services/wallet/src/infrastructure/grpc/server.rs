use tonic::{Request, Response, Status};

use super::handler::{proto, WalletServiceHandler};
use proto::wallet_service_server::WalletService;

/// Encoded protobuf descriptor set for gRPC server reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = wallet_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl WalletService for WalletServiceHandler {
    async fn get_wallet(&self, request: Request<proto::GetWalletRequest>) -> Result<Response<proto::Wallet>, Status> {
        self.get_wallet(request).await
    }

    async fn claim_reward(
        &self,
        request: Request<proto::ClaimRewardRequest>,
    ) -> Result<Response<proto::ClaimRewardResponse>, Status> {
        self.claim_reward(request).await
    }

    async fn list_wallet_transactions(
        &self,
        request: Request<proto::ListWalletTransactionsRequest>,
    ) -> Result<Response<proto::ListWalletTransactionsResponse>, Status> {
        self.list_wallet_transactions(request).await
    }
}

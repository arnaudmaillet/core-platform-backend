//! `wallet-server` — the deployable wallet binary (#665).

use std::net::SocketAddr;

use wallet::service::WalletService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = std::env::var("WALLET_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50072".to_owned())
        .parse()?;

    service_runtime::serve::<WalletService>(addr).await
}

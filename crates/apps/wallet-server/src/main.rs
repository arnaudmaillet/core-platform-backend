//! `wallet-server` — the deployable wallet binary (#665).
//!
//! `wallet-server calibration-report [YYYY-MM-DD]` (#893) prints a day's
//! shadow-mode calibration report as JSON instead of serving (yesterday, UTC,
//! by default): run it inside a wallet pod.

use std::net::SocketAddr;

use wallet::service::WalletService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    if let Some(command) = args.next() {
        anyhow::ensure!(
            command == "calibration-report",
            "unknown command {command:?} (only `calibration-report [YYYY-MM-DD]`)"
        );
        println!("{}", wallet::report::calibration_report(args.next().as_deref()).await?);
        return Ok(());
    }

    let addr: SocketAddr = std::env::var("WALLET_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50072".to_owned())
        .parse()?;

    service_runtime::serve::<WalletService>(addr).await
}

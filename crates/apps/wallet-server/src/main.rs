//! `wallet-server` — the deployable wallet binary (#665).
//!
//! `wallet-server calibration-report [YYYY-MM-DD]` (#893) prints a day's
//! shadow-mode calibration report as JSON instead of serving (yesterday, UTC,
//! by default): run it inside a wallet pod.
//!
//! `wallet-server grant-test-gems <account_id> <amount> <key> <reason…>`
//! (#967) credits test gems outside production only (`WALLET_ENVIRONMENT`
//! non-production and `WALLET_TEST_GRANTS=on`), until minting goes live.

use std::net::SocketAddr;

use wallet::service::WalletService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    if let Some(command) = args.next() {
        match command.as_str() {
            "calibration-report" => println!("{}", wallet::report::calibration_report(args.next().as_deref()).await?),
            "grant-test-gems" => println!("{}", wallet::test_grants::grant_test_gems(&args.collect::<Vec<_>>()).await?),
            _ => anyhow::bail!(
                "unknown command {command:?} (`calibration-report [YYYY-MM-DD]` or `grant-test-gems <account_id> <amount> <key> <reason…>`)"
            ),
        }
        return Ok(());
    }

    let addr: SocketAddr = std::env::var("WALLET_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50072".to_owned())
        .parse()?;

    service_runtime::serve::<WalletService>(addr).await
}

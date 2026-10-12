//! `wallet-server calibration-report [YYYY-MM-DD]` (#893): a day's shadow-mode
//! calibration report, as JSON on stdout — run inside a wallet pod by an
//! operator (the default day is yesterday, UTC). Reads every shard; writes
//! nothing.

use std::sync::Arc;

use chrono::{NaiveDate, TimeDelta, Utc};
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};

use crate::application::Wallets;
use crate::config::WalletConfig;
use crate::infrastructure::persistence::PgWalletStore;

/// The report for `day` (yesterday when `None`), pretty JSON.
pub async fn calibration_report(day: Option<&str>) -> anyhow::Result<String> {
    let day = match day {
        Some(day) => NaiveDate::parse_from_str(day, "%Y-%m-%d")
            .map_err(|e| anyhow::anyhow!("day {day:?} is not YYYY-MM-DD: {e}"))?,
        None => (Utc::now() - TimeDelta::days(1)).date_naive(),
    };
    let pool = PgPoolBuilder::build(PostgresConfig::from_env())
        .await
        .map_err(|e| anyhow::anyhow!("wallet postgres pool: {e}"))?;
    let store = Arc::new(PgWalletStore::new(TransactionManager::new(pool)));
    let wallets = Wallets::new(store, WalletConfig::from_env());
    let report = wallets.calibration_report(day).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(serde_json::to_string_pretty(&report)?)
}

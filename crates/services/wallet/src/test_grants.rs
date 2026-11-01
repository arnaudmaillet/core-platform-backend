//! `wallet-server grant-test-gems <account_id> <amount> <key> <reason…>`
//! (#967): credits gems to a test account, until minting goes live — run
//! inside a wallet pod by an operator. Only where `WALLET_ENVIRONMENT` is a
//! non-production one and `WALLET_TEST_GRANTS=on`; refused anywhere else.
//! Once per key; written as its own kind (`test_grant`).

use std::sync::Arc;

use chrono::Utc;
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};

use crate::application::Wallets;
use crate::config::WalletConfig;
use crate::infrastructure::persistence::PgWalletStore;

/// What the grant did, for the operator's terminal.
pub async fn grant_test_gems(args: &[String]) -> anyhow::Result<String> {
    let usage = || anyhow::anyhow!("usage: wallet-server grant-test-gems <account_id> <amount> <idempotency_key> <reason…>");
    let [account, amount, key, reason @ ..] = args else { return Err(usage()) };
    if reason.is_empty() {
        return Err(usage());
    }
    let amount: i64 = amount.parse().map_err(|_| anyhow::anyhow!("amount {amount:?} is not a number"))?;
    let config = WalletConfig::from_env();
    // Refuse before touching the database.
    if let Err(reason) = &config.test_grants {
        anyhow::bail!("test grants are off here: {reason}");
    }
    let pool = PgPoolBuilder::build(PostgresConfig::from_env())
        .await
        .map_err(|e| anyhow::anyhow!("wallet postgres pool: {e}"))?;
    let wallets = Wallets::new(Arc::new(PgWalletStore::new(TransactionManager::new(pool))), config);
    let (credited, gems) = wallets
        .grant_test_gems(account, key, amount, &reason.join(" "), Utc::now())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(match credited {
        true => format!("credited {amount} test gems to {account}: {gems} gems now"),
        false => format!("key {key} was already used: nothing more credited ({gems} gems now)"),
    })
}

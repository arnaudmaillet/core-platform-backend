use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{error, info};

use crate::application::command::AnonymizeDueAccounts;

/// Accounts anonymized per pass at most; a backlog drains over passes.
const BATCH: i64 = 200;

/// Runs the GDPR janitor every `interval` for the life of the process: each
/// pass anonymizes the accounts whose erasure grace period has ended. A failed
/// pass is logged and retried at the next tick (the work stays listed).
pub async fn run_gdpr_janitor(janitor: Arc<AnonymizeDueAccounts>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match janitor.run(Utc::now(), BATCH).await {
            Ok(pass) if pass.anonymized + pass.skipped > 0 => info!(
                anonymized = pass.anonymized,
                skipped = pass.skipped,
                "gdpr janitor pass"
            ),
            Ok(_) => {}
            Err(e) => error!(error = %e, "gdpr janitor pass failed; retrying next tick"),
        }
    }
}

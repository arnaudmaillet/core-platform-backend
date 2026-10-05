use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{error, info};

use crate::application::command::ExportDueData;

/// Exports built per pass at most (each gathers one account's data from every
/// service); a backlog drains over passes.
const BATCH: i64 = 20;

/// Runs the GDPR export pass every `interval` for the life of the process
/// (#653). A failed pass is logged and retried at the next tick (the work
/// stays listed).
pub async fn run_export_pass(pass: Arc<ExportDueData>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match pass.run(Utc::now(), BATCH).await {
            Ok(done) if done.delivered + done.retried > 0 => {
                info!(delivered = done.delivered, retried = done.retried, "gdpr export pass")
            }
            Ok(_) => {}
            Err(e) => error!(error = %e, "gdpr export pass failed; retrying next tick"),
        }
    }
}

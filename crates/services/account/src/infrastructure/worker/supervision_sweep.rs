use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{error, info};

use crate::application::command::Supervisions;

/// Family supervision's periodic pass (#670): supervisions whose teen turned
/// 18 end (announced), expired invites go. Idempotent; safe on every replica.
pub async fn run_supervision_sweep(supervisions: Arc<Supervisions>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match supervisions.sweep(Utc::now()).await {
            Ok(0) => {}
            Ok(ended) => info!(ended, "supervision sweep: teens came of age"),
            Err(e) => error!(error = %e, "supervision sweep failed; retrying next tick"),
        }
    }
}

//! Push delivery adapters (#654): APNs, and sender names read from profile.

pub mod apns_sender;
pub mod grpc_sender_names;

pub use apns_sender::{ApnsConfig, ApnsSender};
pub use grpc_sender_names::GrpcSenderNames;

use std::sync::Arc;
use std::time::Duration;

use crate::app::PushBackends;
use crate::application::port::{NoSenderNames, SenderNames};

/// Push delivery from the environment (#654):
/// - `NOTIFICATION_APNS_KEY_FILE` (the `.p8` key), `NOTIFICATION_APNS_KEY_ID`,
///   `NOTIFICATION_APNS_TEAM_ID`, `NOTIFICATION_APNS_TOPIC` (the bundle id):
///   all four set turns push on; otherwise push is off (logged).
/// - `NOTIFICATION_PROFILE_GRPC_ENDPOINT`: profile's mesh endpoint, for the
///   sender's name in the alert (deadlines `NOTIFICATION_PROFILE_RPC_TIMEOUT_MS`
///   / `NOTIFICATION_PROFILE_CONNECT_TIMEOUT_MS`, 300 ms / 1 s). Unset: alerts
///   name no one.
pub fn from_env() -> PushBackends {
    let Some(config) = apns_config_from_env() else {
        return PushBackends::default();
    };
    let sender = match ApnsSender::new(config) {
        Ok(sender) => sender,
        Err(error) => {
            tracing::error!(%error, "push off: the APNs key cannot be used");
            return PushBackends::default();
        }
    };
    tracing::info!("push on: APNs");
    PushBackends { sender: Some(Arc::new(sender)), names: sender_names_from_env() }
}

fn apns_config_from_env() -> Option<ApnsConfig> {
    let var = |key: &str| std::env::var(key).ok().filter(|v| !v.trim().is_empty());
    let (file, key_id, team_id, topic) = (
        var("NOTIFICATION_APNS_KEY_FILE"),
        var("NOTIFICATION_APNS_KEY_ID"),
        var("NOTIFICATION_APNS_TEAM_ID"),
        var("NOTIFICATION_APNS_TOPIC"),
    );
    let (Some(file), Some(key_id), Some(team_id), Some(topic)) = (file, key_id, team_id, topic) else {
        tracing::warn!("push off: NOTIFICATION_APNS_KEY_FILE / _KEY_ID / _TEAM_ID / _TOPIC not all set");
        return None;
    };
    match std::fs::read(&file) {
        Ok(key_pem) => Some(ApnsConfig { key_pem, key_id, team_id, topic }),
        Err(error) => {
            tracing::error!(%error, file, "push off: the APNs key file cannot be read");
            None
        }
    }
}

fn sender_names_from_env() -> Arc<dyn SenderNames> {
    let Some(endpoint) = std::env::var("NOTIFICATION_PROFILE_GRPC_ENDPOINT").ok().filter(|v| !v.trim().is_empty())
    else {
        tracing::warn!("NOTIFICATION_PROFILE_GRPC_ENDPOINT unset: push alerts name no one");
        return Arc::new(NoSenderNames);
    };
    let ms = |key: &str, default: u64| {
        Duration::from_millis(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
    };
    match tonic::transport::Channel::from_shared(endpoint) {
        Ok(endpoint) => {
            let channel = endpoint
                .timeout(ms("NOTIFICATION_PROFILE_RPC_TIMEOUT_MS", 300))
                .connect_timeout(ms("NOTIFICATION_PROFILE_CONNECT_TIMEOUT_MS", 1_000))
                .connect_lazy();
            Arc::new(GrpcSenderNames::new(channel))
        }
        Err(error) => {
            tracing::error!(%error, "invalid NOTIFICATION_PROFILE_GRPC_ENDPOINT: push alerts name no one");
            Arc::new(NoSenderNames)
        }
    }
}

//! `profile.v1.events` → engagement (#829): `ProfileTabSettingsChanged` says
//! whether the profile shows its Likes and Saved tabs (#872) to others;
//! `ProfileDeleted` (#873) drops the profile's flags, its Likes tab and its
//! saves, as of the deletion. Other
//! profile events are skipped. Last writer wins (per-profile order from the
//! topic key), so a redelivery is harmless.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::{ProfileTabs, SavedPosts, TabFlags};
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "profile.v1.events";

/// profile's event wire, internally tagged on `type`; only what engagement
/// acts on is read.
#[derive(Debug, Deserialize)]
pub struct ProfileEvent {
    #[serde(rename = "type")]
    kind:       String,
    #[serde(default)]
    profile_id: String,
    #[serde(default)]
    show_likes: Option<bool>,
    #[serde(default)]
    show_saved: Option<bool>,
    #[serde(default)]
    occurred_at_ms: i64,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Tabs(String, TabFlags),
    /// The profile and the deletion's time (µs).
    Deleted(String, i64),
    Poison(String),
}

fn outcome(event: &ProfileEvent) -> Outcome {
    if !matches!(event.kind.as_str(), "ProfileTabSettingsChanged" | "ProfileDeleted") {
        return Outcome::Skip;
    }
    if event.profile_id.is_empty() || event.profile_id.len() > 64 {
        return Outcome::Poison(format!("{} with profile_id {:?}", event.kind, event.profile_id));
    }
    if event.kind == "ProfileDeleted" {
        if event.occurred_at_ms <= 0 {
            return Outcome::Poison(format!("ProfileDeleted without occurred_at_ms for {}", event.profile_id));
        }
        return Outcome::Deleted(event.profile_id.clone(), event.occurred_at_ms.saturating_mul(1_000));
    }
    // Absent on an older event: the default.
    let defaults = TabFlags::default();
    let flags = TabFlags {
        likes: event.show_likes.unwrap_or(defaults.likes),
        saved: event.show_saved.unwrap_or(defaults.saved),
    };
    Outcome::Tabs(event.profile_id.clone(), flags)
}

pub struct ProfileConsumer {
    kafka_config: KafkaClientConfig,
    tabs:         Arc<dyn ProfileTabs>,
    saves:        Arc<dyn SavedPosts>,
    group_id:     String,
}

impl ProfileConsumer {
    pub fn new(
        kafka_config: KafkaClientConfig,
        tabs: Arc<dyn ProfileTabs>,
        saves: Arc<dyn SavedPosts>,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, tabs, saves, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — profile consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("profile consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "profile consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;
        let handle = KafkaConsumerBuilder::new(config).subscribe(TOPIC).build().map_err(|e| e.to_string())?;
        tracing::info!(topic = TOPIC, group = %self.group_id, "profile consumer started");
        let policy = RetryPolicy::default();
        run_consumer::<ProfileEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move {
                match outcome(event) {
                    Outcome::Skip => ProcessOutcome::Done,
                    Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                    Outcome::Tabs(profile, flags) => ProcessOutcome::from_result(worker.tabs.set_tabs(&profile, flags).await),
                    Outcome::Deleted(profile, at_micros) => {
                        let forgotten = match worker.tabs.forget(&profile, at_micros).await {
                            Ok(()) => worker.saves.forget_profile(&profile, at_micros).await,
                            Err(e) => Err(e),
                        };
                        ProcessOutcome::from_result(forgotten)
                    }
                }
            })
        })
        .await
        .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tab_flags_are_read_from_the_tab_settings_and_others_skip() {
        let hidden: ProfileEvent = serde_json::from_str(
            r#"{"type":"ProfileTabSettingsChanged","profile_id":"p1","post_window":"all","show_likes":false,
                "show_saved":false,"show_reposts":true,"show_places":true,"occurred_at_ms":1}"#,
        )
        .unwrap();
        assert_eq!(outcome(&hidden), Outcome::Tabs("p1".into(), TabFlags { likes: false, saved: false }));
        let saved: ProfileEvent =
            serde_json::from_str(r#"{"type":"ProfileTabSettingsChanged","profile_id":"p1","show_saved":true}"#).unwrap();
        assert_eq!(outcome(&saved), Outcome::Tabs("p1".into(), TabFlags { likes: true, saved: true }));
        let older: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileTabSettingsChanged","profile_id":"p1"}"#).unwrap();
        assert_eq!(outcome(&older), Outcome::Tabs("p1".into(), TabFlags::default()));
        let other: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileUpdated","profile_id":"p1"}"#).unwrap();
        assert_eq!(outcome(&other), Outcome::Skip);
        let bad: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileTabSettingsChanged","profile_id":""}"#).unwrap();
        assert!(matches!(outcome(&bad), Outcome::Poison(_)));
    }

    #[test]
    fn a_deleted_profile_is_forgotten_as_of_its_deletion() {
        let deleted: ProfileEvent =
            serde_json::from_str(r#"{"type":"ProfileDeleted","profile_id":"p1","occurred_at_ms":1700000000123}"#).unwrap();
        assert_eq!(outcome(&deleted), Outcome::Deleted("p1".into(), 1_700_000_000_123_000));
        let untimed: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileDeleted","profile_id":"p1"}"#).unwrap();
        assert!(matches!(outcome(&untimed), Outcome::Poison(_)));
    }
}

//! `profile.v1.events` → engagement (#829): `ProfileTabSettingsChanged` says
//! whether the profile shows its Likes tab to others. Other profile events
//! are skipped. Last writer wins (per-profile order from the topic key), so a
//! redelivery is harmless.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::ProfileTabs;
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
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    ShowLikes(String, bool),
    Poison(String),
}

fn outcome(event: &ProfileEvent) -> Outcome {
    if event.kind != "ProfileTabSettingsChanged" {
        return Outcome::Skip;
    }
    if event.profile_id.is_empty() || event.profile_id.len() > 64 {
        return Outcome::Poison(format!("ProfileTabSettingsChanged with profile_id {:?}", event.profile_id));
    }
    // Absent on an older event: shown (the default).
    Outcome::ShowLikes(event.profile_id.clone(), event.show_likes.unwrap_or(true))
}

pub struct ProfileConsumer {
    kafka_config: KafkaClientConfig,
    tabs:         Arc<dyn ProfileTabs>,
    group_id:     String,
}

impl ProfileConsumer {
    pub fn new(kafka_config: KafkaClientConfig, tabs: Arc<dyn ProfileTabs>, group_id: impl Into<String>) -> Self {
        Self { kafka_config, tabs, group_id: group_id.into() }
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
                    Outcome::ShowLikes(profile, shown) => {
                        ProcessOutcome::from_result(worker.tabs.set_shows_likes(&profile, shown).await)
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
    fn the_likes_flag_is_read_from_the_tab_settings_and_others_skip() {
        let hidden: ProfileEvent = serde_json::from_str(
            r#"{"type":"ProfileTabSettingsChanged","profile_id":"p1","post_window":"all","show_likes":false,
                "show_saved":false,"show_reposts":true,"show_places":true,"occurred_at_ms":1}"#,
        )
        .unwrap();
        assert_eq!(outcome(&hidden), Outcome::ShowLikes("p1".into(), false));
        let older: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileTabSettingsChanged","profile_id":"p1"}"#).unwrap();
        assert_eq!(outcome(&older), Outcome::ShowLikes("p1".into(), true));
        let other: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileUpdated","profile_id":"p1"}"#).unwrap();
        assert_eq!(outcome(&other), Outcome::Skip);
        let bad: ProfileEvent = serde_json::from_str(r#"{"type":"ProfileTabSettingsChanged","profile_id":""}"#).unwrap();
        assert!(matches!(outcome(&bad), Outcome::Poison(_)));
    }
}

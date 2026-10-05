//! Projects the authors' location sharing (#657) from `profile.v1.events`
//! (`ProfileLocationSettingsChanged`) into `geo_discovery.location_settings`,
//! which every map query applies. Other profile events are skipped.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;
use uuid::Uuid;

use crate::application::port::LocationSettingsStore;
use crate::domain::value_object::{LocationAudience, LocationSharing};
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC_PROFILE_EVENTS: &str = "profile.v1.events";

/// Lenient read of profile's `ProfileEventWire` (tagged `type`, PascalCase).
#[derive(Debug, Deserialize)]
pub struct ProfileEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    profile_id: String,
    #[serde(default)]
    ghost:      Option<bool>,
    /// `precise` | `city`.
    #[serde(default)]
    precision:  Option<String>,
    /// `everyone` | `followers` | `mutuals` (#657); absent from older events.
    #[serde(default)]
    audience:   Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Record(Uuid, LocationSharing),
    Poison(String),
}

fn outcome(event: &ProfileEvent) -> Outcome {
    if event.event_type != "ProfileLocationSettingsChanged" {
        return Outcome::Skip;
    }
    let Ok(author) = Uuid::parse_str(&event.profile_id) else {
        return Outcome::Poison(format!("bad profile_id {:?}", event.profile_id));
    };
    let city = match event.precision.as_deref() {
        Some("city") => true,
        Some("precise") => false,
        other => return Outcome::Poison(format!("precision {other:?}")),
    };
    let Some(ghost) = event.ghost else {
        return Outcome::Poison("no ghost flag".into());
    };
    let audience = match event.audience.as_deref() {
        None | Some("everyone") => LocationAudience::Everyone,
        Some("followers") => LocationAudience::Followers,
        Some("mutuals") => LocationAudience::Mutuals,
        Some(other) => return Outcome::Poison(format!("audience {other:?}")),
    };
    Outcome::Record(author, LocationSharing { ghost, city, audience })
}

pub struct LocationSettingsWorker {
    kafka_config: KafkaClientConfig,
    store:        Arc<dyn LocationSettingsStore>,
    group_id:     String,
}

impl LocationSettingsWorker {
    pub fn new(
        kafka_config: KafkaClientConfig,
        store: Arc<dyn LocationSettingsStore>,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, store, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — location settings consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("location settings consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "location settings consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe_many([TOPIC_PROFILE_EVENTS])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "location settings consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<ProfileEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { worker.process(event).await })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &ProfileEvent) -> ProcessOutcome {
        match outcome(event) {
            Outcome::Skip => ProcessOutcome::Done,
            Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
            // An upsert: redelivery is harmless; per-profile ordering comes
            // from the topic's profile_id key.
            Outcome::Record(author, sharing) => {
                ProcessOutcome::from_result(self.store.set(author, sharing).await)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use profile::infrastructure::publisher::wire::ProfileEventWire;

    use super::*;

    /// Serialized with profile's own wire type, read back as the worker does.
    fn wire(event: ProfileEventWire) -> ProfileEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn location_settings_are_read_from_profiles_own_wire() {
        let id = Uuid::now_v7();
        let event = wire(ProfileEventWire::ProfileLocationSettingsChanged {
            profile_id: id.to_string(),
            ghost: true,
            precision: "city".into(),
            audience: "mutuals".into(),
            on_new_posts: true,
            occurred_at_ms: 1,
        });
        assert_eq!(
            outcome(&event),
            Outcome::Record(id, LocationSharing { ghost: true, city: true, audience: LocationAudience::Mutuals })
        );

        let other = wire(ProfileEventWire::ProfileUpdated { profile_id: id.to_string(), occurred_at_ms: 1 });
        assert_eq!(outcome(&other), Outcome::Skip);
    }
}

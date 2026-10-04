//! Projects the members' presence settings (#661) from `profile.v1.events`
//! (`ProfileDiscoverySettingsChanged`) into `chat.presence_settings`. Other
//! profile events are skipped.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::{PresenceSettings, PresenceSettingsStore};
use crate::domain::value_object::ProfileId;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "profile.v1.events";

/// Lenient read of profile's `ProfileEventWire` (tagged `type`, PascalCase).
#[derive(Debug, Deserialize)]
pub struct ProfileEvent {
    #[serde(rename = "type")]
    event_type:      String,
    #[serde(default)]
    profile_id:      String,
    #[serde(default)]
    activity_status: Option<bool>,
    #[serde(default)]
    read_receipts:   Option<bool>,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Record(ProfileId, PresenceSettings),
    Poison(String),
}

fn outcome(event: &ProfileEvent) -> Outcome {
    if event.event_type != "ProfileDiscoverySettingsChanged" {
        return Outcome::Skip;
    }
    let Ok(profile) = ProfileId::try_from(event.profile_id.as_str()) else {
        return Outcome::Poison(format!("bad profile_id {:?}", event.profile_id));
    };
    match (event.activity_status, event.read_receipts) {
        (Some(activity_status), Some(read_receipts)) => {
            Outcome::Record(profile, PresenceSettings { activity_status, read_receipts })
        }
        _ => Outcome::Poison("missing presence flags".into()),
    }
}

/// Every pod reads the stream in its own stable group; the projection is a
/// shared table, so one consumer per group is enough. Starts from the earliest
/// offset: settings made before the group first ran must not be skipped.
pub struct PresenceSettingsWorker {
    kafka_config: KafkaClientConfig,
    store:        Arc<dyn PresenceSettingsStore>,
    group_id:     String,
}

impl PresenceSettingsWorker {
    pub fn new(
        kafka_config: KafkaClientConfig,
        store: Arc<dyn PresenceSettingsStore>,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, store, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — presence settings consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("presence settings consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "presence settings consumer error — restarting after 5 s");
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
            .subscribe(TOPIC)
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(topic = TOPIC, group = %self.group_id, "presence settings consumer started");

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
            Outcome::Record(profile, settings) => {
                ProcessOutcome::from_result(self.store.set(&profile, settings).await)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use profile::infrastructure::publisher::wire::ProfileEventWire;
    use uuid::Uuid;

    use super::*;

    /// Serialized with profile's own wire type, read back as the worker does.
    fn wire(event: ProfileEventWire) -> ProfileEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn presence_flags_are_read_from_profiles_own_wire() {
        let id = Uuid::now_v7();
        let event = wire(ProfileEventWire::ProfileDiscoverySettingsChanged {
            profile_id: id.to_string(),
            activity_status: false,
            read_receipts: true,
            by_phone: true,
            by_email: true,
            by_handle_search: true,
            by_qr: true,
            in_suggestions: true,
            occurred_at_ms: 1,
        });
        assert_eq!(
            outcome(&event),
            Outcome::Record(
                ProfileId::from_uuid(id),
                PresenceSettings { activity_status: false, read_receipts: true },
            ),
        );
        let other = wire(ProfileEventWire::ProfileUpdated { profile_id: id.to_string(), occurred_at_ms: 1 });
        assert_eq!(outcome(&other), Outcome::Skip);
    }
}

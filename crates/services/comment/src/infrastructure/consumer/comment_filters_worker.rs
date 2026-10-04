//! Projects the post owners' comment filters (#660) from `profile.v1.events`
//! (`ProfileCommentFiltersChanged`) into `comment.comment_filters`, which the
//! comment reads apply. Other profile events are skipped.

use std::sync::Arc;

use error::AppError;
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::config::ProducerConfig;
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::application::port::CommentFilterStore;
use crate::domain::comment_filter::CommentFilter;
use crate::domain::value_object::ProfileId;

const TOPIC: &str = "profile.v1.events";

/// Lenient read of profile's `ProfileEventWire` (tagged `type`, PascalCase).
#[derive(Debug, Deserialize)]
pub struct ProfileEvent {
    #[serde(rename = "type")]
    event_type:       String,
    #[serde(default)]
    profile_id:       String,
    #[serde(default)]
    hidden_words:     Option<Vec<String>>,
    #[serde(default)]
    filter_offensive: Option<bool>,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Record(ProfileId, CommentFilter),
    Poison(String),
}

fn outcome(event: &ProfileEvent) -> Outcome {
    if event.event_type != "ProfileCommentFiltersChanged" {
        return Outcome::Skip;
    }
    let Ok(owner) = ProfileId::try_from(event.profile_id.as_str()) else {
        return Outcome::Poison(format!("bad profile_id {:?}", event.profile_id));
    };
    match (&event.hidden_words, event.filter_offensive) {
        (Some(words), Some(filter_offensive)) => {
            Outcome::Record(owner, CommentFilter { hidden_words: words.clone(), filter_offensive })
        }
        _ => Outcome::Poison("missing comment filter fields".into()),
    }
}

/// One stable group; the projection is a shared table. Starts from the
/// earliest offset: filters set before the group first ran must not be lost.
pub struct CommentFiltersWorker {
    kafka_config: KafkaClientConfig,
    store:        Arc<dyn CommentFilterStore>,
    group_id:     String,
}

impl CommentFiltersWorker {
    pub fn new(
        kafka_config: KafkaClientConfig,
        store: Arc<dyn CommentFilterStore>,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, store, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match KafkaProducerBuilder::new(ProducerConfig::new(self.kafka_config.clone())).build() {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — comment filters consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("comment filters consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "comment filters consumer error — restarting after 5 s");
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
        tracing::info!(topic = TOPIC, group = %self.group_id, "comment filters consumer started");

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
            Outcome::Record(owner, filter) => match self.store.set(&owner, &filter).await {
                Ok(()) => ProcessOutcome::Done,
                Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
                Err(e) => ProcessOutcome::Reject(e.to_string()),
            },
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
    fn filters_are_read_from_profiles_own_wire() {
        let id = Uuid::now_v7();
        let event = wire(ProfileEventWire::ProfileCommentFiltersChanged {
            profile_id: id.to_string(),
            hidden_words: vec!["spoiler".into()],
            filter_offensive: false,
            occurred_at_ms: 1,
        });
        assert_eq!(
            outcome(&event),
            Outcome::Record(
                ProfileId::try_from(id.to_string().as_str()).unwrap(),
                CommentFilter { hidden_words: vec!["spoiler".into()], filter_offensive: false },
            ),
        );
        let other = wire(ProfileEventWire::ProfileUpdated { profile_id: id.to_string(), occurred_at_ms: 1 });
        assert_eq!(outcome(&other), Outcome::Skip);
    }
}

use async_trait::async_trait;
use transport::{
    error::TransportError,
    kafka::{envelope::EventEnvelope, producer::handle::KafkaProducerHandle},
};

use crate::application::port::EventPublisher;
use crate::domain::event::{DomainEvent, PostDeletedEvent};
use crate::error::PostError;

// The legacy per-type topic still read by timeline and geo-discovery for
// deletions (bare payload). `post.published` and `post.updated` are retired
// (#487): every consumer reads `post.v1.events`.
const TOPIC_DELETED: &str = "post.deleted";

// The unified, versioned stream (the fleet convention, like `moderation.v1.events`
// / `profile.v1.events`): the whole internally-tagged `DomainEvent`, keyed by
// `post_id`. Consumed by search, timeline, realtime, notification and
// geo-discovery.
const TOPIC_V1: &str = "post.v1.events";

fn transport_err(e: TransportError) -> PostError {
    PostError::DomainViolation {
        field:   "kafka".to_owned(),
        message: e.to_string(),
    }
}

pub struct KafkaEventPublisher {
    producer: KafkaProducerHandle,
}

impl KafkaEventPublisher {
    pub fn new(producer: KafkaProducerHandle) -> Self {
        Self { producer }
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), PostError> {
        // 1. The legacy deletion topic (timeline, geo-discovery).
        if let DomainEvent::PostDeleted(e) = event {
            publish_deleted(&self.producer, e).await?;
        }
        // 2. The unified `post.v1.events` stream, every event.
        publish_v1(&self.producer, event).await
    }
}

/// Publishes the whole internally-tagged event to `post.v1.events`, keyed by
/// `post_id`. `DomainEvent` is `#[serde(tag = "type")]`, so the wire payload is
/// `{"type":"PostPublished", ...}` — the shape `search` deserializes.
async fn publish_v1(
    producer: &KafkaProducerHandle,
    event:    &DomainEvent,
) -> Result<(), PostError> {
    let (post_id, profile_id, event_type) = match event {
        DomainEvent::PostPublished(e) => (&e.post_id, &e.profile_id, "PostPublished"),
        DomainEvent::PostUpdated(e)   => (&e.post_id, &e.profile_id, "PostUpdated"),
        DomainEvent::PostDeleted(e)   => (&e.post_id, &e.profile_id, "PostDeleted"),
    };
    let envelope = EventEnvelope::new(TOPIC_V1, post_id.clone(), event.clone())
        .with_header("event_type", event_type)
        .with_header("post_id",    post_id.clone())
        .with_header("profile_id", profile_id.clone());

    producer.publish(envelope).await.map_err(transport_err)
}

async fn publish_deleted(
    producer: &KafkaProducerHandle,
    event:    &PostDeletedEvent,
) -> Result<(), PostError> {
    let key      = event.post_id.clone();
    let envelope = EventEnvelope::new(TOPIC_DELETED, key, event.clone())
        .with_header("event_type",  "PostDeleted")
        .with_header("post_id",     event.post_id.clone())
        .with_header("profile_id",  event.profile_id.clone());

    producer.publish(envelope).await.map_err(transport_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::event::PostPublishedEvent;

    /// Locks the `post.v1.events` wire shape the `search` decoder depends on:
    /// internally tagged on `type`, with the fields flattened alongside it.
    #[test]
    fn post_v1_payload_is_internally_tagged() {
        let event = DomainEvent::PostPublished(PostPublishedEvent {
            post_id:         "post-1".to_owned(),
            profile_id:      "prof-9".to_owned(),
            kind:            "text".to_owned(),
            published_at_ms: 1_700_000_000_000,
            author_tier:     0,
            audio_id:        None,
            audio_kind:      None,
            caption:         "hello world".to_owned(),
            thumbnail_url:   Some("https://cdn/t.jpg".to_owned()),
            lat:             Some(48.8566),
            lng:             Some(2.3522),
        });
        let value = serde_json::to_value(&event).expect("serialize");
        assert_eq!(value["type"], "PostPublished");
        assert_eq!(value["post_id"], "post-1");
        assert_eq!(value["profile_id"], "prof-9");
        assert_eq!(value["published_at_ms"], 1_700_000_000_000_i64);
    }

    /// Locks the geo-discovery denormalization carried on `post.v1.events`'
    /// PostPublished (geo-discovery and notification read it there, #487):
    /// caption, cover thumbnail, and optional location. Absent location must be
    /// omitted from the wire payload (geo-discovery skips indexing in that case).
    #[test]
    fn post_published_carries_geo_denormalization() {
        let with_location = serde_json::to_value(&PostPublishedEvent {
            post_id:         "post-1".to_owned(),
            profile_id:      "prof-9".to_owned(),
            kind:            "text".to_owned(),
            published_at_ms: 1_700_000_000_000,
            author_tier:     0,
            audio_id:        None,
            audio_kind:      None,
            caption:         "at the beach".to_owned(),
            thumbnail_url:   Some("https://cdn/t.jpg".to_owned()),
            lat:             Some(48.8566),
            lng:             Some(2.3522),
        })
        .expect("serialize");
        assert_eq!(with_location["caption"], "at the beach");
        assert_eq!(with_location["thumbnail_url"], "https://cdn/t.jpg");
        assert_eq!(with_location["lat"], 48.8566);
        assert_eq!(with_location["lng"], 2.3522);

        let no_location = serde_json::to_value(&PostPublishedEvent {
            post_id:         "post-2".to_owned(),
            profile_id:      "prof-9".to_owned(),
            kind:            "text".to_owned(),
            published_at_ms: 1_700_000_000_000,
            author_tier:     0,
            audio_id:        None,
            audio_kind:      None,
            caption:         String::new(),
            thumbnail_url:   None,
            lat:             None,
            lng:             None,
        })
        .expect("serialize");
        assert!(no_location.get("lat").is_none(), "absent lat must be omitted");
        assert!(no_location.get("lng").is_none(), "absent lng must be omitted");
        assert!(no_location.get("thumbnail_url").is_none(), "absent thumbnail must be omitted");
        assert_eq!(no_location["caption"], "");
    }
}

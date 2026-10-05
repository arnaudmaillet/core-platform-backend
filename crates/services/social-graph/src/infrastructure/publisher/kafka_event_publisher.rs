use async_trait::async_trait;
use serde::Serialize;
use transport::{
    error::TransportError,
    kafka::{envelope::EventEnvelope, producer::handle::KafkaProducerHandle},
};

use crate::application::port::EventPublisher;
use crate::domain::event::{
    AuthorTierChanged, DomainEvent, FollowRequested, ProfileBlocked, ProfileFollowed, ProfileUnfollowed,
};
use crate::error::SocialGraphError;

const TOPIC_FOLLOWED:   &str = "social-graph.followed";
const TOPIC_UNFOLLOWED: &str = "social-graph.unfollowed";
const TOPIC_BLOCKED:    &str = "social-graph.blocked";
/// A follow request to a private profile (#755), for notification.
const TOPIC_FOLLOW_REQUESTED: &str = "social-graph.follow_requested";
/// The author-tier signal `profile` consumes (then persists + re-emits on
/// `profile.v1.events` for `post` to denormalize). Keyed by profile id.
const TOPIC_AUTHOR_TIER_CHANGED: &str = "social-graph.author_tier_changed";

fn transport_err(e: TransportError) -> SocialGraphError {
    SocialGraphError::DomainViolation {
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
    async fn publish(&self, event: &DomainEvent) -> Result<(), SocialGraphError> {
        match event {
            DomainEvent::ProfileFollowed(e) => publish_followed(&self.producer, e).await,
            DomainEvent::FollowRequested(e) => publish_follow_requested(&self.producer, e).await,
            DomainEvent::ProfileUnfollowed(e) => publish_unfollowed(&self.producer, e).await,
            DomainEvent::ProfileBlocked(e) => publish_blocked(&self.producer, e).await,
            // ProfileUnblocked is not published downstream per the interface contract.
            DomainEvent::ProfileUnblocked(_) => Ok(()),
            DomainEvent::AuthorTierChanged(e) => {
                publish_author_tier_changed(&self.producer, e).await
            }
        }
    }
}

/// Wire payload for `social-graph.author_tier_changed`. `new_tier` is the shared
/// `u8` taxonomy (0=Standard, 1=Premium, 2=Vip).
#[derive(Serialize)]
struct AuthorTierChangedWire {
    profile_id:    String,
    new_tier:      u8,
    follower_count: i64,
    changed_at_ms: i64,
}

async fn publish_author_tier_changed(
    producer: &KafkaProducerHandle,
    event:    &AuthorTierChanged,
) -> Result<(), SocialGraphError> {
    let key  = event.profile_id.as_str();
    let wire = AuthorTierChangedWire {
        profile_id:     event.profile_id.as_str(),
        new_tier:       event.new_tier.as_u8(),
        follower_count: event.follower_count,
        changed_at_ms:  event.changed_at.timestamp_millis(),
    };
    let envelope = EventEnvelope::new(TOPIC_AUTHOR_TIER_CHANGED, key, wire)
        .with_header("event_type", "AuthorTierChanged")
        .with_header("profile_id", event.profile_id.as_str())
        .with_header("new_tier",   event.new_tier.as_u8().to_string());

    producer.publish(envelope).await.map_err(transport_err)
}

async fn publish_followed(
    producer: &KafkaProducerHandle,
    event:    &ProfileFollowed,
) -> Result<(), SocialGraphError> {
    let key      = format!("{}:{}", event.actor_id, event.target_id);
    let envelope = EventEnvelope::new(TOPIC_FOLLOWED, key, event.clone())
        .with_header("event_type", "ProfileFollowed")
        .with_header("actor_id",   event.actor_id.as_str())
        .with_header("target_id",  event.target_id.as_str());

    producer.publish(envelope).await.map_err(transport_err)
}

async fn publish_follow_requested(
    producer: &KafkaProducerHandle,
    event:    &FollowRequested,
) -> Result<(), SocialGraphError> {
    let key      = format!("{}:{}", event.actor_id, event.target_id);
    let envelope = EventEnvelope::new(TOPIC_FOLLOW_REQUESTED, key, event.clone())
        .with_header("event_type", "FollowRequested")
        .with_header("actor_id",   event.actor_id.as_str())
        .with_header("target_id",  event.target_id.as_str());

    producer.publish(envelope).await.map_err(transport_err)
}

async fn publish_unfollowed(
    producer: &KafkaProducerHandle,
    event:    &ProfileUnfollowed,
) -> Result<(), SocialGraphError> {
    let key      = format!("{}:{}", event.actor_id, event.target_id);
    let envelope = EventEnvelope::new(TOPIC_UNFOLLOWED, key, event.clone())
        .with_header("event_type", "ProfileUnfollowed")
        .with_header("actor_id",   event.actor_id.as_str())
        .with_header("target_id",  event.target_id.as_str());

    producer.publish(envelope).await.map_err(transport_err)
}

async fn publish_blocked(
    producer: &KafkaProducerHandle,
    event:    &ProfileBlocked,
) -> Result<(), SocialGraphError> {
    let key      = format!("{}:{}", event.actor_id, event.target_id);
    let envelope = EventEnvelope::new(TOPIC_BLOCKED, key, event.clone())
        .with_header("event_type", "ProfileBlocked")
        .with_header("actor_id",   event.actor_id.as_str())
        .with_header("target_id",  event.target_id.as_str());

    producer.publish(envelope).await.map_err(transport_err)
}

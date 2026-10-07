//! Event-publication adapters for the `auth.v1.events` topic.
//!
//! All three domain events share one topic, keyed by `account_id` so a person's
//! auth events keep per-account order on one partition; the `event_type` header
//! carries the dotted routing key.

pub mod kafka_event_publisher;
pub mod outbox_relay;
pub mod pg_outbox_publisher;
pub mod log_event_publisher;

pub use kafka_event_publisher::KafkaEventPublisher;
pub use log_event_publisher::LogEventPublisher;

/// The single Kafka topic every auth domain event is published to.
pub const TOPIC_AUTH_EVENTS: &str = "auth.v1.events";

use crate::domain::event::DomainEvent;
use crate::domain::value_object::AccountId;

/// The partition key (account id) for an event — keeps per-account ordering.
pub(crate) fn event_key(event: &DomainEvent) -> AccountId {
    match event {
        DomainEvent::SessionIssued(e) => e.account_id,
        DomainEvent::SessionRevoked(e) => e.account_id,
        DomainEvent::SubjectLinked(e) => e.account_id,
        DomainEvent::AppAuthorized(e) => e.account_id,
        DomainEvent::AppAuthorizationRevoked(e) => e.account_id,
    }
}

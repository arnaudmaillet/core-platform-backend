//! Event-publication adapters for the `account.v1.events` topic.
//!
//! Every account domain event shares one topic, keyed by `account_id` so a
//! person's account events keep per-account order on one partition; the
//! `event_type` header carries the dotted routing key. The repository drains the
//! aggregate's events after a successful durable write and publishes them here.

pub mod kafka_event_publisher;
pub mod log_event_publisher;

pub use kafka_event_publisher::KafkaEventPublisher;
pub use log_event_publisher::LogEventPublisher;

use crate::domain::event::DomainEvent;
use crate::domain::value_object::AccountId;

/// The single Kafka topic every account domain event is published to.
pub const TOPIC_ACCOUNT_EVENTS: &str = "account.v1.events";

/// The partition key (account id) for an event — keeps per-account ordering.
pub(crate) fn event_key(event: &DomainEvent) -> AccountId {
    match event {
        DomainEvent::AccountCreated(e) => e.account_id,
        DomainEvent::EmailVerified(e) => e.account_id,
        DomainEvent::PasswordChanged(e) => e.account_id,
        DomainEvent::EmailChanged(e) => e.account_id,
        DomainEvent::PhoneChanged(e) => e.account_id,
        DomainEvent::MfaEnrolled(e) => e.account_id,
        DomainEvent::MfaRevoked(e) => e.account_id,
        DomainEvent::RoleAssigned(e) => e.account_id,
        DomainEvent::RoleRevoked(e) => e.account_id,
        DomainEvent::AccountSuspended(e) => e.account_id,
        DomainEvent::AccountActivated(e) => e.account_id,
        DomainEvent::AccountDeactivated(e) => e.account_id,
        DomainEvent::AccountDeleted(e) => e.account_id,
        DomainEvent::KycStatusChanged(e) => e.account_id,
        DomainEvent::GdprDeletionRequested(e) => e.account_id,
        DomainEvent::GdprDataExportRequested(e) => e.account_id,
        DomainEvent::GdprDataExportCompleted(e) => e.account_id,
        DomainEvent::ConsentsUpdated(e) => e.account_id,
        DomainEvent::GdprDeletionCancelled(e) => e.account_id,
        DomainEvent::DateOfBirthSet(e) => e.account_id,
        DomainEvent::SupervisionStarted(e) => e.account_id,
        DomainEvent::SupervisionEnded(e) => e.account_id,
    }
}

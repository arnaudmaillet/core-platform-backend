//! Inbound Kafka integrations (on the shared at-least-once `run_consumer`).

pub mod account_event_consumer;

pub use account_event_consumer::{run_account_event_consumer, ACCOUNT_EVENTS_GROUP, ACCOUNT_EVENTS_TOPIC};

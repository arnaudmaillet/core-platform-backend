//! [`AccessLog`] on the audit plane's generic ingest lane, `audit.v1.events`
//! (#837): one `data_access` record per staff link to a private document, in
//! the audit-owned JSON shape (`audit::infrastructure::decode::AuditEventWire`).

use async_trait::async_trait;
use serde_json::json;
use transport::kafka::envelope::EventEnvelope;
use transport::kafka::producer::handle::KafkaProducerHandle;
use uuid::Uuid;

use crate::application::port::{AccessLog, DocumentView};
use crate::error::MediaError;

/// The audit plane's generic ingest lane.
pub const TOPIC_AUDIT_EVENTS: &str = "audit.v1.events";

/// The audit record of `view` (its own id: every link is its own access).
pub fn audit_record(view: &DocumentView, event_id: Uuid) -> serde_json::Value {
    let mut attributes = serde_json::Map::new();
    if let Some(expires_at) = view.expires_at {
        attributes.insert("link_expires_at_ms".into(), json!(expires_at.timestamp_millis().to_string()));
    }
    json!({
        "event_id": event_id.to_string(),
        "category": "data_access",
        "subject_pseudonym": view.owner.as_str(),
        "actor": { "actor_type": "admin", "pseudonym": view.viewer, "session_ref": "" },
        "action": "media.private_document.viewed",
        "resource": { "type": "private_document", "id": view.asset_id.as_str() },
        "outcome": "permitted",
        "lawful_basis": "legitimate_interests",
        "source_service": "media",
        "correlation_id": "",
        "occurred_at_ms": view.at.timestamp_millis(),
        "attributes": attributes,
    })
}

pub struct KafkaAccessLog {
    producer: KafkaProducerHandle,
}

impl KafkaAccessLog {
    pub fn new(producer: KafkaProducerHandle) -> Self {
        Self { producer }
    }
}

#[async_trait]
impl AccessLog for KafkaAccessLog {
    async fn document_viewed(&self, view: &DocumentView) -> Result<(), MediaError> {
        let key = view.asset_id.as_str();
        let envelope = EventEnvelope::new(TOPIC_AUDIT_EVENTS, key.clone(), audit_record(view, Uuid::now_v7()))
            .with_header("event_type", "media.private_document.viewed".to_owned())
            .with_header("asset_id", key);
        self.producer
            .publish(envelope)
            .await
            .map_err(|e| MediaError::AccessRecordFailed { reason: e.to_string() })
    }
}

/// No broker (local dev): the view is logged only.
pub struct LogAccessLog;

#[async_trait]
impl AccessLog for LogAccessLog {
    async fn document_viewed(&self, view: &DocumentView) -> Result<(), MediaError> {
        tracing::info!(
            asset_id = %view.asset_id,
            viewer = %view.viewer,
            "private document viewed (log access log; no Kafka configured)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;
    use crate::domain::value_object::{AssetId, OwnerId};

    /// The record decodes through audit's own `audit.v1.events` mapping: a
    /// `data_access` by an admin, on the document's owner.
    #[test]
    fn audit_reads_the_record() {
        let now = Utc::now();
        let view = DocumentView {
            asset_id:   AssetId::new(),
            owner:      OwnerId::from_uuid(Uuid::now_v7()),
            viewer:     "staff-1".into(),
            at:         now,
            expires_at: Some(now + Duration::minutes(5)),
        };
        let wire: audit::infrastructure::decode::AuditEventWire =
            serde_json::from_value(audit_record(&view, Uuid::now_v7())).unwrap();
        let event = audit::infrastructure::decode::map_audit_event(wire).expect("audit maps it");
        assert_eq!(event.category(), audit::domain::EventCategory::DataAccess);
        assert_eq!(event.action(), "media.private_document.viewed");
    }
}

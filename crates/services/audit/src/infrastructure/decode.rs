//! The async ingest lane's decode layer: an audit-owned JSON wire schema for the
//! `audit.v1.events` topic, plus the pure `map_audit_event` distillation into a
//! domain [`AuditEvent`].
//!
//! Like `counter` / `realtime` / `search`, audit must not depend on the producing
//! services' crates (a sideways services→services edge the tiering forbids), so it
//! owns its read schema: minimal, lenient structs that match the published JSON
//! (extra fields ignored, so an additive upstream change never breaks the
//! consumer). The fleet's Kafka is JSON (`run_consumer` deserializes via serde),
//! so this lane is JSON even though the synchronous `RecordPrivileged` gRPC lane
//! carries the same logical shape as `audit.v1` protobuf (see [`super::codec`]).
//!
//! Producers: `media` records each staff view of a private document here
//! (`data_access`, #837; its wire test decodes through [`map_audit_event`]).
//! `moderation` / `auth` / `account` feed audit through their own topics.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde::Deserialize;

use crate::domain::{
    Actor, ActorPseudonym, ActorType, AuditEvent, EventCategory, EventId, LawfulBasis,
    NewAuditEvent, Outcome, PiiEnvelope, ResourceRef, SubjectKeyRef, SubjectPseudonym, TenantId,
};
use crate::error::AuditError;

pub const TOPIC_AUDIT_EVENTS: &str = "audit.v1.events";

/// The audit-owned JSON shape of a compliance event on `audit.v1.events`. Lenient:
/// unknown fields are ignored, optional fields default.
#[derive(Debug, Clone, Deserialize)]
pub struct AuditEventWire {
    pub event_id: String,
    pub category: String,
    #[serde(default)]
    pub subject_pseudonym: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub actor: ActorWire,
    pub action: String,
    #[serde(default)]
    pub resource: ResourceWire,
    pub outcome: String,
    #[serde(default)]
    pub lawful_basis: Option<String>,
    #[serde(default)]
    pub source_service: String,
    #[serde(default)]
    pub correlation_id: String,
    pub occurred_at_ms: i64,
    #[serde(default)]
    pub pii: Option<PiiWire>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActorWire {
    #[serde(default)]
    pub actor_type: String,
    pub pseudonym: String,
    #[serde(default)]
    pub session_ref: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResourceWire {
    #[serde(default, rename = "type")]
    pub resource_type: String,
    #[serde(default)]
    pub id: String,
}

/// PII envelope on the JSON lane: ciphertext + nonce are base64 (binary can't ride
/// raw in JSON). Most async events carry none.
#[derive(Debug, Clone, Deserialize)]
pub struct PiiWire {
    pub subject_key_ref: String,
    pub ciphertext_b64: String,
    #[serde(default)]
    pub nonce_b64: String,
    #[serde(default)]
    pub algorithm: String,
}

/// Distill a decoded wire event into a domain [`AuditEvent`]. `run_consumer` owns
/// deserialization (poison bytes dead-letter before reaching here); this is the
/// pure, total mapping. A bad category / outcome / base64 is a poison-class fault
/// (`AUD-1003` / `AUD-1001`) so it dead-letters rather than looping.
pub fn map_audit_event(wire: AuditEventWire) -> Result<AuditEvent, AuditError> {
    let category = category_from_str(&wire.category)?;
    let actor = Actor::new(
        actor_type_from_str(&wire.actor.actor_type),
        ActorPseudonym::new(wire.actor.pseudonym)?,
        wire.actor.session_ref,
    );
    let pii = wire.pii.map(decode_pii).transpose()?;

    AuditEvent::try_new(NewAuditEvent {
        event_id: EventId::new(wire.event_id)?,
        category,
        subject: opt(wire.subject_pseudonym).map(SubjectPseudonym::new).transpose()?,
        tenant: opt(wire.tenant_id).map(TenantId::new).transpose()?,
        actor,
        action: wire.action,
        resource: ResourceRef::new(wire.resource.resource_type, wire.resource.id),
        outcome: outcome_from_str(&wire.outcome)?,
        lawful_basis: wire
            .lawful_basis
            .as_deref()
            .map(lawful_basis_from_str)
            .unwrap_or(LawfulBasis::Unspecified),
        source_service: wire.source_service,
        correlation_id: wire.correlation_id,
        occurred_at: chrono::DateTime::from_timestamp_millis(wire.occurred_at_ms)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap()),
        pii,
        attributes: wire.attributes,
    })
}

fn decode_pii(wire: PiiWire) -> Result<PiiEnvelope, AuditError> {
    let ciphertext = b64(&wire.ciphertext_b64)?;
    let nonce = if wire.nonce_b64.is_empty() {
        Vec::new()
    } else {
        b64(&wire.nonce_b64)?
    };
    Ok(PiiEnvelope::sealed(
        SubjectKeyRef::new(wire.subject_key_ref)?,
        ciphertext,
        nonce,
        wire.algorithm,
    ))
}

fn b64(s: &str) -> Result<Vec<u8>, AuditError> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| AuditError::MalformedAuditEvent {
            reason: format!("invalid base64 PII envelope: {e}"),
        })
}

fn opt(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

fn category_from_str(s: &str) -> Result<EventCategory, AuditError> {
    Ok(match s {
        "authentication" => EventCategory::Authentication,
        "authorization" => EventCategory::Authorization,
        "moderation" => EventCategory::Moderation,
        "consent" => EventCategory::Consent,
        "data_access" => EventCategory::DataAccess,
        "data_export" => EventCategory::DataExport,
        "data_erasure" => EventCategory::DataErasure,
        "privileged_action" => EventCategory::PrivilegedAction,
        "retention" => EventCategory::Retention,
        other => {
            return Err(AuditError::UnknownEventCategory {
                category: other.to_owned(),
            });
        }
    })
}

fn actor_type_from_str(s: &str) -> ActorType {
    match s {
        "user" => ActorType::User,
        "admin" => ActorType::Admin,
        "service" => ActorType::Service,
        _ => ActorType::System,
    }
}

fn outcome_from_str(s: &str) -> Result<Outcome, AuditError> {
    Ok(match s {
        "permitted" => Outcome::Permitted,
        "denied" => Outcome::Denied,
        "executed" => Outcome::Executed,
        "failed" => Outcome::Failed,
        other => {
            return Err(AuditError::MalformedAuditEvent {
                reason: format!("unknown outcome '{other}'"),
            });
        }
    })
}

fn lawful_basis_from_str(s: &str) -> LawfulBasis {
    match s {
        "consent" => LawfulBasis::Consent,
        "contract" => LawfulBasis::Contract,
        "legal_obligation" => LawfulBasis::LegalObligation,
        "vital_interests" => LawfulBasis::VitalInterests,
        "public_task" => LawfulBasis::PublicTask,
        "legitimate_interests" => LawfulBasis::LegitimateInterests,
        _ => LawfulBasis::Unspecified,
    }
}

#[cfg(test)]
mod tests {
    use error::AppError;

    use super::*;

    fn wire_json(category: &str, outcome: &str) -> AuditEventWire {
        serde_json::from_value(serde_json::json!({
            "event_id": "evt-1",
            "category": category,
            "subject_pseudonym": "7f3a",
            "tenant_id": "tenant-7",
            "actor": { "actor_type": "admin", "pseudonym": "adm-1", "session_ref": "s-1" },
            "action": "account.suspend",
            "resource": { "type": "account", "id": "acc-9" },
            "outcome": outcome,
            "lawful_basis": "legal_obligation",
            "source_service": "moderation",
            "correlation_id": "trace-1",
            "occurred_at_ms": 1_750_000_000_000i64,
            "extra_ignored_field": true
        }))
        .unwrap()
    }

    #[test]
    fn maps_a_well_formed_event() {
        let event = map_audit_event(wire_json("moderation", "executed")).unwrap();
        assert_eq!(event.event_id().as_str(), "evt-1");
        assert_eq!(event.category(), EventCategory::Moderation);
        assert_eq!(event.subject().unwrap().as_str(), "7f3a");
        assert_eq!(event.action(), "account.suspend");
    }

    #[test]
    fn unknown_category_is_poison() {
        let err = map_audit_event(wire_json("nonsense", "executed")).unwrap_err();
        assert_eq!(err.error_code(), "AUD-1003");
        assert!(!err.is_retryable()); // dead-letter, not retry
    }

    #[test]
    fn unknown_outcome_is_poison() {
        let err = map_audit_event(wire_json("moderation", "exploded")).unwrap_err();
        assert_eq!(err.error_code(), "AUD-1001");
    }

    #[test]
    fn pii_envelope_is_base64_decoded() {
        let ciphertext_b64 =
            base64::engine::general_purpose::STANDARD.encode(b"secret-bytes");
        let mut wire = wire_json("data_access", "permitted");
        wire.pii = Some(PiiWire {
            subject_key_ref: "dek:7f3a".to_owned(),
            ciphertext_b64,
            nonce_b64: String::new(),
            algorithm: "AES-256-GCM".to_owned(),
        });
        let event = map_audit_event(wire).unwrap();
        assert_eq!(event.pii().unwrap().ciphertext(), b"secret-bytes");
    }

    #[test]
    fn invalid_base64_is_poison() {
        let mut wire = wire_json("data_access", "permitted");
        wire.pii = Some(PiiWire {
            subject_key_ref: "dek:7f3a".to_owned(),
            ciphertext_b64: "!!!not-base64!!!".to_owned(),
            nonce_b64: String::new(),
            algorithm: "AES-256-GCM".to_owned(),
        });
        let err = map_audit_event(wire).unwrap_err();
        assert_eq!(err.error_code(), "AUD-1001");
    }
}

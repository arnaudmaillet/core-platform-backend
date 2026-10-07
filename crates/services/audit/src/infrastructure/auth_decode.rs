//! Audit-side decode of the auth feed (`auth.v1.events`) into domain
//! [`AuditEvent`]s. Like the moderation feed, audit owns this read schema (it must
//! not depend on the `auth` crate), so the wire shapes are lenient structs
//! hand-matched to auth's published JSON.
//!
//! Scope: `session_issued` and `session_revoked` — the authentication lifecycle —
//! and `app_authorized` / `app_authorization_revoked` — consent given to a
//! third-party app and its withdrawal (#667, GDPR Art. 7). Every other auth event
//! (e.g. `subject_linked`) is a benign skip.
//!
//! Unlike moderation, auth events carry **no free-text PII** — only structured
//! metadata over pseudonymous ids (the account id is the subject pseudonym; audit
//! never resolves it). So there is no rationale to seal: this feed needs no cipher,
//! and the mapping is a total, pure function.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::domain::{
    Actor, ActorPseudonym, ActorType, AuditEvent, EventCategory, EventId, LawfulBasis,
    NewAuditEvent, Outcome, ResourceRef, SubjectPseudonym,
};
use crate::error::AuditError;

pub const TOPIC_AUTH_EVENTS: &str = "auth.v1.events";

/// Namespace for deterministic UUIDv5 audit-event ids from auth coordinates.
/// (`b"audit_auth_evt_v5"`.)
const NS_AUDIT_AUTH: Uuid = Uuid::from_u128(0x6175_6469_745f_6175_7468_5f65_7674_5f35);

const SOURCE: &str = "auth";

// ── Wire schema (hand-matched to auth's published JSON) ────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthEventWire {
    SessionIssued(SessionIssuedWire),
    SessionRevoked(SessionRevokedWire),
    AppAuthorized(AppAuthorizedWire),
    AppAuthorizationRevoked(AppAuthorizationRevokedWire),
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppAuthorizedWire {
    pub account_id: String,
    pub app_id: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppAuthorizationRevokedWire {
    pub account_id: String,
    pub app_id: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionIssuedWire {
    pub session_id: String,
    pub account_id: String,
    #[serde(default)]
    pub generation: i64,
    pub expires_at: DateTime<Utc>,
    pub absolute_expiry: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionRevokedWire {
    pub session_id: String,
    pub account_id: String,
    #[serde(default)]
    pub generation: i64,
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

// ── Mapping ────────────────────────────────────────────────────────────────────

/// A successful login → an `Authentication` record. The actor is the authenticated
/// user themselves; the session id is the resource and the actor's session ref.
pub fn map_session_issued(wire: &SessionIssuedWire) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("session_id".to_owned(), wire.session_id.clone());
    attributes.insert("generation".to_owned(), wire.generation.to_string());
    attributes.insert("expires_at".to_owned(), wire.expires_at.to_rfc3339());
    attributes.insert("absolute_expiry".to_owned(), wire.absolute_expiry.to_rfc3339());

    AuditEvent::try_new(NewAuditEvent {
        event_id: EventId::new(derive_id(&["auth.session_issued", &wire.session_id]))?,
        category: EventCategory::Authentication,
        subject: Some(SubjectPseudonym::new(wire.account_id.clone())?),
        tenant: None,
        actor: Actor::new(
            ActorType::User,
            ActorPseudonym::new(wire.account_id.clone())?,
            wire.session_id.clone(),
        ),
        action: "auth.session_issued".to_owned(),
        resource: ResourceRef::new("session", wire.session_id.clone()),
        outcome: Outcome::Executed,
        lawful_basis: LawfulBasis::Unspecified,
        source_service: SOURCE.to_owned(),
        correlation_id: wire.correlation_id.clone(),
        occurred_at: wire.occurred_at,
        pii: None,
        attributes,
    })
}

/// A session revocation → an `Authentication` record. The acting authority is
/// derived from the reason: an administrative revocation is an admin action, a
/// (global) logout or a password change is the user's, a refresh-reuse revocation is the system's.
pub fn map_session_revoked(wire: &SessionRevokedWire) -> Result<AuditEvent, AuditError> {
    let (actor_type, actor_pseudonym) = match wire.reason.as_str() {
        "administrative" => (ActorType::Admin, SOURCE),
        "logout" | "global_logout" | "password_changed" | "mfa_changed" => (ActorType::User, wire.account_id.as_str()),
        // refresh_reuse and anything else: an automated security revocation.
        _ => (ActorType::System, SOURCE),
    };

    let mut attributes = BTreeMap::new();
    attributes.insert("session_id".to_owned(), wire.session_id.clone());
    attributes.insert("generation".to_owned(), wire.generation.to_string());
    attributes.insert("reason".to_owned(), wire.reason.clone());

    AuditEvent::try_new(NewAuditEvent {
        event_id: EventId::new(derive_id(&["auth.session_revoked", &wire.session_id]))?,
        category: EventCategory::Authentication,
        subject: Some(SubjectPseudonym::new(wire.account_id.clone())?),
        tenant: None,
        actor: Actor::new(
            actor_type,
            ActorPseudonym::new(actor_pseudonym.to_owned())?,
            wire.session_id.clone(),
        ),
        action: "auth.session_revoked".to_owned(),
        resource: ResourceRef::new("session", wire.session_id.clone()),
        outcome: Outcome::Executed,
        lawful_basis: LawfulBasis::Unspecified,
        source_service: SOURCE.to_owned(),
        correlation_id: wire.correlation_id.clone(),
        occurred_at: wire.occurred_at,
        pii: None,
        attributes,
    })
}

/// Consent given to a third-party app (#667) → a `Consent` record: the holder
/// is the actor, the app the resource, the scopes the consent's content. The
/// id is derived from (account, app, instant): a retried grant is one record.
pub fn map_app_authorized(wire: &AppAuthorizedWire) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("scopes".to_owned(), wire.scopes.join(" "));
    app_consent(
        "auth.app_authorized",
        &wire.account_id,
        &wire.app_id,
        wire.occurred_at,
        &wire.correlation_id,
        attributes,
    )
}

/// Its withdrawal (GDPR Art. 7(3)) → a `Consent` record, the holder's act.
pub fn map_app_authorization_revoked(wire: &AppAuthorizationRevokedWire) -> Result<AuditEvent, AuditError> {
    app_consent(
        "auth.app_authorization_revoked",
        &wire.account_id,
        &wire.app_id,
        wire.occurred_at,
        &wire.correlation_id,
        BTreeMap::new(),
    )
}

fn app_consent(
    action: &str,
    account_id: &str,
    app_id: &str,
    occurred_at: DateTime<Utc>,
    correlation_id: &str,
    attributes: BTreeMap<String, String>,
) -> Result<AuditEvent, AuditError> {
    AuditEvent::try_new(NewAuditEvent {
        event_id: EventId::new(derive_id(&[action, account_id, app_id, &occurred_at.to_rfc3339()]))?,
        category: EventCategory::Consent,
        subject: Some(SubjectPseudonym::new(account_id.to_owned())?),
        tenant: None,
        actor: Actor::new(ActorType::User, ActorPseudonym::new(account_id.to_owned())?, String::new()),
        action: action.to_owned(),
        resource: ResourceRef::new("third_party_app", app_id.to_owned()),
        outcome: Outcome::Executed,
        lawful_basis: LawfulBasis::Consent,
        source_service: SOURCE.to_owned(),
        correlation_id: correlation_id.to_owned(),
        occurred_at,
        pii: None,
        attributes,
    })
}

fn derive_id(parts: &[&str]) -> String {
    Uuid::new_v5(&NS_AUDIT_AUTH, parts.join(":").as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issued() -> SessionIssuedWire {
        serde_json::from_value(serde_json::json!({
            "type": "session_issued",
            "session_id": "11111111-1111-1111-1111-111111111111",
            "account_id": "22222222-2222-2222-2222-222222222222",
            "subject": { "provider": "keycloak", "subject": "kc-abc" },
            "generation": 3,
            "issued_at": "2026-06-26T12:00:00Z",
            "expires_at": "2026-06-26T13:00:00Z",
            "absolute_expiry": "2026-06-27T12:00:00Z",
            "occurred_at": "2026-06-26T12:00:00Z",
            "correlation_id": "33333333-3333-3333-3333-333333333333"
        }))
        .map(|w| match w {
            AuthEventWire::SessionIssued(s) => s,
            _ => panic!("expected SessionIssued"),
        })
        .unwrap()
    }

    fn revoked(reason: &str) -> SessionRevokedWire {
        serde_json::from_value(serde_json::json!({
            "type": "session_revoked",
            "session_id": "11111111-1111-1111-1111-111111111111",
            "account_id": "22222222-2222-2222-2222-222222222222",
            "generation": 3,
            "reason": reason,
            "occurred_at": "2026-06-26T12:00:00Z",
            "correlation_id": "33333333-3333-3333-3333-333333333333"
        }))
        .map(|w| match w {
            AuthEventWire::SessionRevoked(s) => s,
            _ => panic!("expected SessionRevoked"),
        })
        .unwrap()
    }

    #[test]
    fn session_issued_maps_to_an_authentication_record_with_no_pii() {
        let event = map_session_issued(&issued()).unwrap();
        assert_eq!(event.category(), EventCategory::Authentication);
        assert_eq!(event.action(), "auth.session_issued");
        assert_eq!(event.actor().actor_type, ActorType::User);
        assert_eq!(event.subject().unwrap().as_str(), "22222222-2222-2222-2222-222222222222");
        assert!(!event.has_pii());
        assert_eq!(event.attributes().get("generation").unwrap(), "3");
    }

    #[test]
    fn revocation_actor_is_derived_from_the_reason() {
        assert_eq!(map_session_revoked(&revoked("logout")).unwrap().actor().actor_type, ActorType::User);
        assert_eq!(map_session_revoked(&revoked("administrative")).unwrap().actor().actor_type, ActorType::Admin);
        assert_eq!(map_session_revoked(&revoked("refresh_reuse")).unwrap().actor().actor_type, ActorType::System);
        // Signing the other devices out on a password change is the holder's act.
        assert_eq!(map_session_revoked(&revoked("password_changed")).unwrap().actor().actor_type, ActorType::User);
        // Two-step sign-in turned on / codes regenerated by the holder (#649).
        assert_eq!(map_session_revoked(&revoked("mfa_changed")).unwrap().actor().actor_type, ActorType::User);
    }

    #[test]
    fn revocation_carries_the_reason_in_attributes() {
        let event = map_session_revoked(&revoked("global_logout")).unwrap();
        assert_eq!(event.action(), "auth.session_revoked");
        assert_eq!(event.attributes().get("reason").unwrap(), "global_logout");
    }

    #[test]
    fn event_ids_are_deterministic_and_distinct_per_kind() {
        let i1 = map_session_issued(&issued()).unwrap();
        let i2 = map_session_issued(&issued()).unwrap();
        assert_eq!(i1.event_id(), i2.event_id());
        // Same session, different lifecycle event → different id.
        assert_ne!(i1.event_id(), map_session_revoked(&revoked("logout")).unwrap().event_id());
    }

    #[test]
    fn subject_linked_decodes_to_other() {
        let wire: AuthEventWire =
            serde_json::from_value(serde_json::json!({ "type": "subject_linked", "account_id": "x" })).unwrap();
        assert!(matches!(wire, AuthEventWire::Other));
    }

    /// #667: consent to a third-party app and its withdrawal are `Consent`
    /// records under the `consent` basis; a retried grant maps to one id.
    #[test]
    fn app_consent_and_its_withdrawal_are_consent_records() {
        let granted: AuthEventWire = serde_json::from_value(serde_json::json!({
            "type": "app_authorized",
            "account_id": "22222222-2222-2222-2222-222222222222",
            "app_id": "partner.app",
            "scopes": ["email", "profile:read"],
            "occurred_at": "2026-10-08T12:00:00Z",
            "correlation_id": "33333333-3333-3333-3333-333333333333"
        }))
        .unwrap();
        let AuthEventWire::AppAuthorized(granted) = granted else { panic!("expected AppAuthorized") };
        let record = map_app_authorized(&granted).unwrap();
        assert_eq!((record.category(), record.action()), (EventCategory::Consent, "auth.app_authorized"));
        assert_eq!(record.attributes().get("scopes").unwrap(), "email profile:read");
        assert_eq!(record.event_id(), map_app_authorized(&granted).unwrap().event_id(), "a retry is one record");

        let revoked: AuthEventWire = serde_json::from_value(serde_json::json!({
            "type": "app_authorization_revoked",
            "account_id": "22222222-2222-2222-2222-222222222222",
            "app_id": "partner.app",
            "occurred_at": "2026-10-08T13:00:00Z"
        }))
        .unwrap();
        let AuthEventWire::AppAuthorizationRevoked(revoked) = revoked else { panic!("expected AppAuthorizationRevoked") };
        let withdrawal = map_app_authorization_revoked(&revoked).unwrap();
        assert_eq!(withdrawal.action(), "auth.app_authorization_revoked");
        assert_eq!(withdrawal.actor().actor_type, ActorType::User);
        assert_ne!(withdrawal.event_id(), record.event_id());
    }
}

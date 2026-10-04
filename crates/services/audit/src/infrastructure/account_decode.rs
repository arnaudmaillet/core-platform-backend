//! Audit-side decode of the account feed (`account.v1.events`) into domain
//! [`AuditEvent`]s. Audit owns this read schema (no dependency on the `account`
//! crate); the wire shapes are lenient structs hand-matched to account's published
//! JSON.
//!
//! The full account event surface maps here — the PII/GDPR core (`account_created`,
//! `email_changed`, `email_verified`, `phone_changed` carry PII sealed into a
//! crypto-shreddable envelope; `gdpr_deletion_requested` → `DataErasure` + drives
//! the crypto-shred; `gdpr_data_export_requested` → `DataExport`) plus the
//! lifecycle (`activated`/`deactivated`/`suspended`/`deleted` → `Authorization`),
//! security (`password_changed`, `mfa_enrolled`/`revoked` → `Authentication`) and
//! authorization (`role_assigned`/`revoked`, `kyc_status_changed`) events. Anything
//! still outside this set is a benign skip.
//!
//! The PII maps take an already-sealed [`PiiEnvelope`] (the consumer seals via the
//! audit cipher) so this layer stays pure.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::domain::{
    Actor, ActorPseudonym, ActorType, AuditEvent, EventCategory, EventId, LawfulBasis,
    NewAuditEvent, Outcome, PiiEnvelope, ResourceRef, SubjectPseudonym,
};
use crate::error::AuditError;

pub const TOPIC_ACCOUNT_EVENTS: &str = "account.v1.events";

/// Namespace for deterministic UUIDv5 audit-event ids. (`b"audit_acct_evt_v5"`.)
const NS_AUDIT_ACCOUNT: Uuid = Uuid::from_u128(0x6175_6469_745f_6163_6374_5f65_7674_5f35);

const SOURCE: &str = "account";

// ── Wire schema (hand-matched to account's published JSON) ─────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AccountEventWire {
    AccountCreated(AccountCreatedWire),
    EmailChanged(EmailChangedWire),
    EmailVerified(EmailVerifiedWire),
    PhoneChanged(PhoneChangedWire),
    PasswordChanged(BareAccountEventWire),
    MfaEnrolled(MfaEnrolledWire),
    MfaRevoked(BareAccountEventWire),
    RoleAssigned(RoleWire),
    RoleRevoked(RoleWire),
    AccountSuspended(AccountSuspendedWire),
    AccountActivated(BareAccountEventWire),
    AccountDeactivated(BareAccountEventWire),
    AccountDeleted(AccountDeletedWire),
    KycStatusChanged(KycStatusChangedWire),
    GdprDeletionRequested(GdprDeletionRequestedWire),
    GdprDataExportRequested(GdprDataExportRequestedWire),
    GdprDeletionCancelled(GdprDeletionCancelledWire),
    ConsentsUpdated(ConsentsUpdatedWire),
    #[serde(other)]
    Other,
}

/// The fields shared by events that carry no payload beyond the account + time.
#[derive(Debug, Clone, Deserialize)]
pub struct BareAccountEventWire {
    pub account_id: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountCreatedWire {
    pub account_id: String,
    /// Absent for a phone-only account.
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub country_of_residence: Option<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailChangedWire {
    pub account_id: String,
    /// Absent when the account had no email (a phone-only account adding one).
    #[serde(default)]
    pub old_email: Option<String>,
    pub new_email: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailVerifiedWire {
    pub account_id: String,
    pub email: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PhoneChangedWire {
    pub account_id: String,
    #[serde(default)]
    pub new_phone: Option<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MfaEnrolledWire {
    pub account_id: String,
    #[serde(default)]
    pub recovery_codes_count: u64,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RoleWire {
    pub account_id: String,
    pub role: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountSuspendedWire {
    pub account_id: String,
    #[serde(default)]
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountDeletedWire {
    pub account_id: String,
    #[serde(default)]
    pub deleted_by: Option<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KycStatusChangedWire {
    pub account_id: String,
    #[serde(default)]
    pub old_status: String,
    #[serde(default)]
    pub new_status: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GdprDeletionRequestedWire {
    pub account_id: String,
    #[serde(default)]
    pub retention_days: u32,
    pub scheduled_deletion_at: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GdprDeletionCancelledWire {
    pub account_id: String,
    pub was_scheduled_at: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsentChangeWire {
    pub purpose: String,
    pub granted: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsentsUpdatedWire {
    pub account_id: String,
    #[serde(default)]
    pub changes: Vec<ConsentChangeWire>,
    #[serde(default)]
    pub policy_version: Option<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GdprDataExportRequestedWire {
    pub account_id: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub correlation_id: String,
}

impl AccountCreatedWire {
    pub fn pii_plaintext(&self) -> String {
        format!(
            "email={};country={}",
            self.email.as_deref().unwrap_or(""),
            self.country_of_residence.as_deref().unwrap_or("")
        )
    }
}

impl EmailChangedWire {
    pub fn pii_plaintext(&self) -> String {
        format!("old_email={};new_email={}", self.old_email.as_deref().unwrap_or(""), self.new_email)
    }
}

impl EmailVerifiedWire {
    pub fn pii_plaintext(&self) -> String {
        format!("email={}", self.email)
    }
}

impl PhoneChangedWire {
    /// The PII plaintext to seal, when a phone number is present (a removal has none).
    pub fn pii_plaintext(&self) -> Option<String> {
        self.new_phone.as_ref().map(|p| format!("phone={p}"))
    }
}

// ── Shared builder ─────────────────────────────────────────────────────────────

/// The common shape of an account-sourced audit event. `occurred_at` is folded
/// into the id so recurring events (a re-suspend, a role change, a password change)
/// get distinct, replay-stable ids.
#[allow(clippy::too_many_arguments)]
fn account_event(
    action: &str,
    category: EventCategory,
    actor_type: ActorType,
    actor_pseudonym: &str,
    account_id: &str,
    occurred_at: DateTime<Utc>,
    correlation_id: &str,
    lawful_basis: LawfulBasis,
    pii: Option<PiiEnvelope>,
    attributes: BTreeMap<String, String>,
) -> Result<AuditEvent, AuditError> {
    AuditEvent::try_new(NewAuditEvent {
        event_id: EventId::new(derive_id(&[action, account_id, &occurred_at.to_rfc3339()]))?,
        category,
        subject: Some(SubjectPseudonym::new(account_id.to_owned())?),
        tenant: None,
        actor: Actor::new(actor_type, ActorPseudonym::new(actor_pseudonym.to_owned())?, String::new()),
        action: action.to_owned(),
        resource: ResourceRef::new("account", account_id.to_owned()),
        outcome: Outcome::Executed,
        lawful_basis,
        source_service: SOURCE.to_owned(),
        correlation_id: correlation_id.to_owned(),
        occurred_at,
        pii,
        attributes,
    })
}

// ── PII / GDPR core ────────────────────────────────────────────────────────────

pub fn map_account_created(
    wire: &AccountCreatedWire,
    sealed_pii: PiiEnvelope,
) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    if !wire.role.is_empty() {
        attributes.insert("role".to_owned(), wire.role.clone());
    }
    if !wire.status.is_empty() {
        attributes.insert("status".to_owned(), wire.status.clone());
    }
    account_event(
        "account.created",
        EventCategory::Identity,
        ActorType::System,
        SOURCE,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Contract,
        Some(sealed_pii),
        attributes,
    )
}

pub fn map_email_changed(
    wire: &EmailChangedWire,
    sealed_pii: PiiEnvelope,
) -> Result<AuditEvent, AuditError> {
    account_event(
        "account.email_changed",
        EventCategory::Authentication,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Contract,
        Some(sealed_pii),
        BTreeMap::new(),
    )
}

pub fn map_email_verified(
    wire: &EmailVerifiedWire,
    sealed_pii: PiiEnvelope,
) -> Result<AuditEvent, AuditError> {
    account_event(
        "account.email_verified",
        EventCategory::Authentication,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Contract,
        Some(sealed_pii),
        BTreeMap::new(),
    )
}

/// A phone change. `sealed_pii` is `Some` when a number was set, `None` when it was
/// removed.
pub fn map_phone_changed(
    wire: &PhoneChangedWire,
    sealed_pii: Option<PiiEnvelope>,
) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    if wire.new_phone.is_none() {
        attributes.insert("phone_removed".to_owned(), "true".to_owned());
    }
    account_event(
        "account.phone_changed",
        EventCategory::Authentication,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        if sealed_pii.is_some() { LawfulBasis::Contract } else { LawfulBasis::Unspecified },
        sealed_pii,
        attributes,
    )
}

pub fn map_gdpr_deletion_requested(
    wire: &GdprDeletionRequestedWire,
) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("retention_days".to_owned(), wire.retention_days.to_string());
    attributes.insert("scheduled_deletion_at".to_owned(), wire.scheduled_deletion_at.to_rfc3339());
    account_event(
        "account.gdpr_deletion_requested",
        EventCategory::DataErasure,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::LegalObligation,
        None,
        attributes,
    )
}

/// A withdrawn erasure request: evidence that nothing was erased.
pub fn map_gdpr_deletion_cancelled(
    wire: &GdprDeletionCancelledWire,
) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("was_scheduled_at".to_owned(), wire.was_scheduled_at.to_rfc3339());
    account_event(
        "account.gdpr_deletion_cancelled",
        EventCategory::DataErasure,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::LegalObligation,
        None,
        attributes,
    )
}

/// Consents given or withdrawn — the tamper-evident copy of the evidence
/// GDPR Art. 7(1) asks the controller to keep (account holds the history).
/// `changes` reads e.g. `analytics=withdrawn,marketing=granted`.
pub fn map_consents_updated(wire: &ConsentsUpdatedWire) -> Result<AuditEvent, AuditError> {
    let mut changes: Vec<String> = wire
        .changes
        .iter()
        .map(|c| format!("{}={}", c.purpose, if c.granted { "granted" } else { "withdrawn" }))
        .collect();
    changes.sort();
    let mut attributes = BTreeMap::new();
    attributes.insert("changes".to_owned(), changes.join(","));
    if let Some(version) = &wire.policy_version {
        attributes.insert("policy_version".to_owned(), version.clone());
    }
    account_event(
        "account.consents_updated",
        EventCategory::Consent,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Consent,
        None,
        attributes,
    )
}

pub fn map_gdpr_data_export_requested(
    wire: &GdprDataExportRequestedWire,
) -> Result<AuditEvent, AuditError> {
    account_event(
        "account.gdpr_data_export_requested",
        EventCategory::DataExport,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::LegalObligation,
        None,
        BTreeMap::new(),
    )
}

// ── Security (Authentication) ──────────────────────────────────────────────────

pub fn map_password_changed(wire: &BareAccountEventWire) -> Result<AuditEvent, AuditError> {
    bare(wire, "account.password_changed", EventCategory::Authentication, ActorType::User)
}

pub fn map_mfa_revoked(wire: &BareAccountEventWire) -> Result<AuditEvent, AuditError> {
    bare(wire, "account.mfa_revoked", EventCategory::Authentication, ActorType::User)
}

pub fn map_mfa_enrolled(wire: &MfaEnrolledWire) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("recovery_codes_count".to_owned(), wire.recovery_codes_count.to_string());
    account_event(
        "account.mfa_enrolled",
        EventCategory::Authentication,
        ActorType::User,
        &wire.account_id,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Unspecified,
        None,
        attributes,
    )
}

// ── Authorization (lifecycle + roles + kyc) ────────────────────────────────────

pub fn map_account_activated(wire: &BareAccountEventWire) -> Result<AuditEvent, AuditError> {
    bare(wire, "account.activated", EventCategory::Identity, ActorType::System)
}

pub fn map_account_deactivated(wire: &BareAccountEventWire) -> Result<AuditEvent, AuditError> {
    bare(wire, "account.deactivated", EventCategory::Identity, ActorType::System)
}

pub fn map_account_suspended(wire: &AccountSuspendedWire) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    if !wire.reason.is_empty() {
        attributes.insert("reason".to_owned(), wire.reason.clone());
    }
    account_event(
        "account.suspended",
        EventCategory::Identity,
        ActorType::System,
        SOURCE,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Unspecified,
        None,
        attributes,
    )
}

pub fn map_account_deleted(wire: &AccountDeletedWire) -> Result<AuditEvent, AuditError> {
    // An admin-initiated delete names its actor; a system/self delete does not.
    let (actor_type, actor) = match &wire.deleted_by {
        Some(admin) => (ActorType::Admin, admin.as_str()),
        None => (ActorType::System, SOURCE),
    };
    account_event(
        "account.deleted",
        EventCategory::Identity,
        actor_type,
        actor,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Unspecified,
        None,
        BTreeMap::new(),
    )
}

pub fn map_role_assigned(wire: &RoleWire) -> Result<AuditEvent, AuditError> {
    role_event(wire, "account.role_assigned")
}

pub fn map_role_revoked(wire: &RoleWire) -> Result<AuditEvent, AuditError> {
    role_event(wire, "account.role_revoked")
}

pub fn map_kyc_status_changed(wire: &KycStatusChangedWire) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("old_status".to_owned(), wire.old_status.clone());
    attributes.insert("new_status".to_owned(), wire.new_status.clone());
    account_event(
        "account.kyc_status_changed",
        EventCategory::Identity,
        ActorType::System,
        SOURCE,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Unspecified,
        None,
        attributes,
    )
}

// ── helpers ────────────────────────────────────────────────────────────────────

fn bare(
    wire: &BareAccountEventWire,
    action: &str,
    category: EventCategory,
    actor_type: ActorType,
) -> Result<AuditEvent, AuditError> {
    let actor = if actor_type == ActorType::User { wire.account_id.as_str() } else { SOURCE };
    account_event(
        action,
        category,
        actor_type,
        actor,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Unspecified,
        None,
        BTreeMap::new(),
    )
}

fn role_event(wire: &RoleWire, action: &str) -> Result<AuditEvent, AuditError> {
    let mut attributes = BTreeMap::new();
    attributes.insert("role".to_owned(), wire.role.clone());
    account_event(
        action,
        EventCategory::Authorization,
        ActorType::System,
        SOURCE,
        &wire.account_id,
        wire.occurred_at,
        &wire.correlation_id,
        LawfulBasis::Unspecified,
        None,
        attributes,
    )
}

fn derive_id(parts: &[&str]) -> String {
    Uuid::new_v5(&NS_AUDIT_ACCOUNT, parts.join(":").as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::SubjectKeyRef;

    fn sealed() -> PiiEnvelope {
        PiiEnvelope::sealed(SubjectKeyRef::new("dek:acc").unwrap(), b"ct".to_vec(), b"n".to_vec(), "AES-256-GCM")
    }

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-27T12:00:00Z").unwrap().with_timezone(&Utc)
    }

    fn bare_wire() -> BareAccountEventWire {
        BareAccountEventWire { account_id: "acc-1".to_owned(), occurred_at: ts(), correlation_id: String::new() }
    }

    #[test]
    fn account_created_seals_pii_out_of_attributes() {
        let wire = AccountCreatedWire {
            account_id: "acc-1".to_owned(),
            email: Some("user@example.com".to_owned()),
            role: "user".to_owned(),
            status: "active".to_owned(),
            country_of_residence: Some("FR".to_owned()),
            occurred_at: ts(),
            correlation_id: String::new(),
        };
        let e = map_account_created(&wire, sealed()).unwrap();
        assert_eq!(e.category(), EventCategory::Identity);
        assert!(e.has_pii());
        assert!(!e.attributes().values().any(|v| v.contains("user@example.com")));
    }

    #[test]
    fn security_events_map_to_authentication() {
        assert_eq!(map_password_changed(&bare_wire()).unwrap().category(), EventCategory::Authentication);
        assert_eq!(map_mfa_revoked(&bare_wire()).unwrap().category(), EventCategory::Authentication);
        let mfa = MfaEnrolledWire { account_id: "acc-1".into(), recovery_codes_count: 8, occurred_at: ts(), correlation_id: String::new() };
        let e = map_mfa_enrolled(&mfa).unwrap();
        assert_eq!(e.category(), EventCategory::Authentication);
        assert_eq!(e.attributes().get("recovery_codes_count").unwrap(), "8");
    }

    #[test]
    fn lifecycle_maps_to_identity_and_roles_to_authorization() {
        // Account lifecycle/identity-state → Identity.
        assert_eq!(map_account_activated(&bare_wire()).unwrap().category(), EventCategory::Identity);
        let susp = AccountSuspendedWire { account_id: "acc-1".into(), reason: "fraud".into(), occurred_at: ts(), correlation_id: String::new() };
        let e = map_account_suspended(&susp).unwrap();
        assert_eq!(e.category(), EventCategory::Identity);
        assert_eq!(e.attributes().get("reason").unwrap(), "fraud");
        // Role grants are genuine authorization.
        let role = RoleWire { account_id: "acc-1".into(), role: "support_agent".into(), occurred_at: ts(), correlation_id: String::new() };
        let r = map_role_assigned(&role).unwrap();
        assert_eq!(r.category(), EventCategory::Authorization);
        assert_eq!(r.attributes().get("role").unwrap(), "support_agent");
    }

    #[test]
    fn account_deleted_attributes_the_admin_when_present() {
        let by_admin = AccountDeletedWire { account_id: "acc-1".into(), deleted_by: Some("admin-9".into()), occurred_at: ts(), correlation_id: String::new() };
        let e = map_account_deleted(&by_admin).unwrap();
        assert_eq!(e.actor().actor_type, ActorType::Admin);
        assert_eq!(e.actor().pseudonym.as_str(), "admin-9");
        let by_system = AccountDeletedWire { account_id: "acc-1".into(), deleted_by: None, occurred_at: ts(), correlation_id: String::new() };
        assert_eq!(map_account_deleted(&by_system).unwrap().actor().actor_type, ActorType::System);
    }

    #[test]
    fn phone_change_seals_when_set_and_flags_removal() {
        let set = PhoneChangedWire { account_id: "acc-1".into(), new_phone: Some("+12025551234".into()), occurred_at: ts(), correlation_id: String::new() };
        assert_eq!(set.pii_plaintext().as_deref(), Some("phone=+12025551234"));
        assert!(map_phone_changed(&set, Some(sealed())).unwrap().has_pii());
        let removed = PhoneChangedWire { account_id: "acc-1".into(), new_phone: None, occurred_at: ts(), correlation_id: String::new() };
        let e = map_phone_changed(&removed, None).unwrap();
        assert!(!e.has_pii());
        assert_eq!(e.attributes().get("phone_removed").unwrap(), "true");
    }

    #[test]
    fn gdpr_and_kyc_categories() {
        let del = GdprDeletionRequestedWire { account_id: "acc-1".into(), retention_days: 30, scheduled_deletion_at: ts(), occurred_at: ts(), correlation_id: String::new() };
        assert_eq!(map_gdpr_deletion_requested(&del).unwrap().category(), EventCategory::DataErasure);
        let kyc = KycStatusChangedWire { account_id: "acc-1".into(), old_status: "submitted".into(), new_status: "approved".into(), occurred_at: ts(), correlation_id: String::new() };
        let e = map_kyc_status_changed(&kyc).unwrap();
        assert_eq!(e.attributes().get("new_status").unwrap(), "approved");
    }

    #[test]
    fn newly_unmapped_events_would_be_other() {
        // Sanity: an unknown future type still decodes to Other (not poison).
        let wire: AccountEventWire = serde_json::from_value(serde_json::json!({ "type": "account_future_thing", "account_id": "x" })).unwrap();
        assert!(matches!(wire, AccountEventWire::Other));
    }

    #[test]
    fn recurring_events_get_distinct_ids_per_occurrence() {
        let mut a = bare_wire();
        let mut b = bare_wire();
        b.occurred_at = ts() + chrono::Duration::seconds(1);
        a.occurred_at = ts();
        assert_ne!(
            map_password_changed(&a).unwrap().event_id(),
            map_password_changed(&b).unwrap().event_id()
        );
    }

    #[test]
    fn a_cancelled_deletion_is_erasure_evidence_and_consents_are_consent_evidence() {
        let cancelled = map_gdpr_deletion_cancelled(&GdprDeletionCancelledWire {
            account_id: "acc-1".into(),
            was_scheduled_at: ts(),
            occurred_at: ts(),
            correlation_id: String::new(),
        })
        .unwrap();
        assert_eq!(cancelled.action(), "account.gdpr_deletion_cancelled");
        assert_eq!(cancelled.category(), EventCategory::DataErasure);

        let consents = map_consents_updated(&ConsentsUpdatedWire {
            account_id: "acc-1".into(),
            changes: vec![
                ConsentChangeWire { purpose: "marketing".into(), granted: true },
                ConsentChangeWire { purpose: "analytics".into(), granted: false },
            ],
            policy_version: Some("PP-1".into()),
            occurred_at: ts(),
            correlation_id: String::new(),
        })
        .unwrap();
        assert_eq!(consents.category(), EventCategory::Consent);
        assert_eq!(consents.attributes().get("changes").unwrap(), "analytics=withdrawn,marketing=granted");
        assert_eq!(consents.attributes().get("policy_version").unwrap(), "PP-1");
    }

    /// The wire tags account publishes (`#[serde(tag = "type")]`, snake_case).
    #[test]
    fn decodes_the_new_account_event_types() {
        let cancelled: AccountEventWire = serde_json::from_value(serde_json::json!({
            "type": "gdpr_deletion_cancelled", "account_id": "acc-1",
            "was_scheduled_at": "2026-11-03T12:00:00Z", "occurred_at": "2026-10-04T12:00:00Z",
            "correlation_id": "c"
        }))
        .unwrap();
        assert!(matches!(cancelled, AccountEventWire::GdprDeletionCancelled(_)));
        let consents: AccountEventWire = serde_json::from_value(serde_json::json!({
            "type": "consents_updated", "account_id": "acc-1",
            "changes": [{ "purpose": "marketing", "granted": true }],
            "policy_version": null, "occurred_at": "2026-10-04T12:00:00Z", "correlation_id": "c"
        }))
        .unwrap();
        assert!(matches!(consents, AccountEventWire::ConsentsUpdated(c) if c.changes.len() == 1));
    }

    #[test]
    fn a_phone_only_account_has_no_email_on_the_wire() {
        let wire: AccountCreatedWire = serde_json::from_str(
            r#"{"account_id":"acc-2","role":"user","status":"pending_verification","country_of_residence":"FR","occurred_at":"2026-06-27T12:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(wire.email, None);
        assert_eq!(wire.pii_plaintext(), "email=;country=FR");
        let changed: EmailChangedWire = serde_json::from_str(
            r#"{"account_id":"acc-2","new_email":"a@b.co","occurred_at":"2026-06-27T12:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(changed.pii_plaintext(), "old_email=;new_email=a@b.co");
    }
}

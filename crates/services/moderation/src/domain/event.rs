//! Domain events the moderation context publishes to the `moderation.v1.events`
//! Kafka topic.
//!
//! Events are serde structs (JSON on the wire), matching the fleet convention —
//! they are deliberately **not** proto messages (the proto contract is the
//! synchronous RPC surface only). Every event carries `actor_id`, which the
//! infrastructure publisher (Phase 4) uses as the partition key so all events for
//! one actor stay ordered — a reversal can never be delivered ahead of the
//! application it reverses. This is the Plane B denormalization feed: `timeline`,
//! `chat`, and `account` consume it to apply enforcement on the hot read path.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::aggregate::DecisionAuthor;
use crate::domain::value_object::{
    ActionType, ActorId, AppealId, CaseId, DecisionId, EnforcementId, EnforcementVersion,
    PolicyCategory, PolicyVersion, SubjectRef,
};

/// A review case was opened for a subject.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseOpened {
    pub case_id: CaseId,
    pub subject: SubjectRef,
    pub actor_id: ActorId,
    pub category: PolicyCategory,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// A case was actioned or dismissed (a decision was recorded).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseResolved {
    pub case_id: CaseId,
    pub decision_id: DecisionId,
    pub actor_id: ActorId,
    pub action: ActionType,
    pub category: PolicyCategory,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// An enforcement action was applied — the Plane B signal that downstream
/// services denormalize to flip visibility / restrict the actor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnforcementApplied {
    pub enforcement_id: EnforcementId,
    pub subject: SubjectRef,
    pub actor_id: ActorId,
    pub action: ActionType,
    pub version: EnforcementVersion,
    pub applied_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// An enforcement action was reversed (appeal overturn / re-review). Carries the
/// version so a consumer can reject a stale reversal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnforcementReversed {
    pub enforcement_id: EnforcementId,
    pub subject: SubjectRef,
    pub actor_id: ActorId,
    pub version: EnforcementVersion,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// An appeal was resolved (upheld or overturned).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppealResolved {
    pub appeal_id: AppealId,
    pub decision_id: DecisionId,
    /// The appellant (the sanctioned account, or the reporter).
    pub actor_id: ActorId,
    /// Filed by a reporter (DSA Art. 20(1) notifier), not the sanctioned
    /// account: no enforcement follows from it. Absent from older events.
    #[serde(default)]
    pub by_reporter: bool,
    pub overturned: bool,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// The **compliance evidence** record of a decision — the dedicated event the
/// `audit` plane consumes (the existing offender-centric events above are the
/// Plane-B denormalization feed and deliberately omit the authority + reason).
///
/// Unlike its siblings it carries *who decided* (`author` — a human reviewer or an
/// automated rule) and *why* (`rationale`, the DSA statement-of-reasons) alongside
/// the affected `subject`, so audit can answer "who did what, to whom, under what
/// authority, with what stated reason". The `rationale` is sealed into a
/// crypto-shreddable envelope by `audit` at ingest (it may reference content) — by
/// convention it should be policy-referential, not content-quoting. Emitted at
/// every site that records a `Decision` (automated screen, human review, appeal
/// reversal). Partitioned by the affected actor, like its siblings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRecorded {
    pub decision_id: DecisionId,
    pub subject: SubjectRef,
    /// Who (or what) made the decision — the authority.
    pub author: DecisionAuthor,
    pub action: ActionType,
    pub category: PolicyCategory,
    pub policy_version: PolicyVersion,
    /// The DSA statement-of-reasons. Sealed into a crypto-shreddable PII envelope
    /// downstream in `audit`.
    pub rationale: String,
    /// Set when this decision reverses an earlier one (an appeal overturn).
    pub reverses: Option<DecisionId>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// Sealed sum type of every domain event moderation publishes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DomainEvent {
    CaseOpened(CaseOpened),
    CaseResolved(CaseResolved),
    DecisionRecorded(DecisionRecorded),
    EnforcementApplied(EnforcementApplied),
    EnforcementReversed(EnforcementReversed),
    AppealResolved(AppealResolved),
}

impl DomainEvent {
    /// Dotted routing key used as the Kafka message type header.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::CaseOpened(_) => "moderation.case_opened",
            Self::CaseResolved(_) => "moderation.case_resolved",
            Self::DecisionRecorded(_) => "moderation.decision_recorded",
            Self::EnforcementApplied(_) => "moderation.enforcement_applied",
            Self::EnforcementReversed(_) => "moderation.enforcement_reversed",
            Self::AppealResolved(_) => "moderation.appeal_resolved",
        }
    }

    /// The actor this event concerns — the Kafka partition key, guaranteeing
    /// per-actor ordering across all moderation events.
    pub fn actor_id(&self) -> ActorId {
        match self {
            Self::CaseOpened(e) => e.actor_id,
            Self::CaseResolved(e) => e.actor_id,
            Self::DecisionRecorded(e) => e.subject.actor_id(),
            Self::EnforcementApplied(e) => e.actor_id,
            Self::EnforcementReversed(e) => e.actor_id,
            Self::AppealResolved(e) => e.actor_id,
        }
    }
}

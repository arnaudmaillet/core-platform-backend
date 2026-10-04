use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::{ActorId, PolicyCategory, ReportId, ReporterKind, SubjectRef};
use crate::error::ModerationError;

/// The **Report** aggregate — a user-submitted abuse report. It is an *input* to
/// the ingestion pipeline (it feeds a [`Case`](crate::domain::aggregate::Case)),
/// not a decision, so it emits no events. Its id is **deterministic**
/// ([`ReportId::for_report`]) so a reporter submitting the same report twice
/// collapses to one record (dedup).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    id: ReportId,
    reporter_id: ActorId,
    reporter_kind: ReporterKind,
    subject: SubjectRef,
    category: PolicyCategory,
    reason: String,
    reported_at: DateTime<Utc>,
}

impl Report {
    /// Files a report. Rejects a self-report — a reporter may not report their own
    /// content (the actor behind the subject is the reporter).
    pub fn file(
        reporter_id: ActorId,
        reporter_kind: ReporterKind,
        subject: SubjectRef,
        category: PolicyCategory,
        reason: impl Into<String>,
        reported_at: DateTime<Utc>,
    ) -> Result<Self, ModerationError> {
        if reporter_id == subject.actor_id() {
            return Err(ModerationError::SelfReportRejected);
        }
        let id = ReportId::for_report(reporter_id, &subject);
        Ok(Self {
            id,
            reporter_id,
            reporter_kind,
            subject,
            category,
            reason: reason.into(),
            reported_at,
        })
    }

    /// Reconstructs from storage.
    pub fn reconstitute(
        id: ReportId,
        reporter_id: ActorId,
        reporter_kind: ReporterKind,
        subject: SubjectRef,
        category: PolicyCategory,
        reason: String,
        reported_at: DateTime<Utc>,
    ) -> Self {
        Self { id, reporter_id, reporter_kind, subject, category, reason, reported_at }
    }

    pub fn id(&self) -> ReportId {
        self.id
    }

    pub fn reporter_id(&self) -> ActorId {
        self.reporter_id
    }

    pub fn reporter_kind(&self) -> ReporterKind {
        self.reporter_kind
    }

    pub fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    pub fn category(&self) -> PolicyCategory {
        self.category
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn reported_at(&self) -> DateTime<Utc> {
        self.reported_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::value_object::EntityType;
    use uuid::Uuid;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-25T12:00:00Z").unwrap().with_timezone(&Utc)
    }

    fn subject_by(author: u128) -> SubjectRef {
        SubjectRef::new(EntityType::Comment, "c1", ActorId::from_uuid(Uuid::from_u128(author)), "thread").unwrap()
    }

    #[test]
    fn rejects_self_report() {
        let me = ActorId::from_uuid(Uuid::from_u128(5));
        let my_content = subject_by(5);
        assert!(matches!(
            Report::file(me, ReporterKind::Member, my_content, PolicyCategory::Spam, "x", t0()).unwrap_err(),
            ModerationError::SelfReportRejected
        ));
    }

    #[test]
    fn dedups_same_reporter_and_subject() {
        let reporter = ActorId::from_uuid(Uuid::from_u128(9));
        let target = subject_by(1);
        let r1 = Report::file(reporter, ReporterKind::Member, target.clone(), PolicyCategory::Hate, "a", t0()).unwrap();
        let r2 = Report::file(reporter, ReporterKind::Member, target, PolicyCategory::Hate, "different reason", t0()).unwrap();
        assert_eq!(r1.id(), r2.id(), "id is content-addressed for dedup");
    }
}

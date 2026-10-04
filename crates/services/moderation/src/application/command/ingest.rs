//! Plane A ingestion — the async, post-hoc path the Phase 4 Kafka consumers drive.
//! Both handlers open-or-load the deterministic case for a subject and append an
//! evidence signal, so redelivery is idempotent (the same content event upserts
//! the same case).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::port::{CaseRepository, ClassifierGateway, EventPublisher, ReportRepository};
use crate::domain::aggregate::{Case, CaseOpenParams, Report};
use crate::domain::value_object::{
    ActorId, CaseId, Confidence, PolicyCategory, ReporterKind, Signal, SubjectRef,
};
use crate::error::ModerationError;

/// Shared helper: load the subject's case or open a fresh one.
async fn open_or_load(
    cases: &Arc<dyn CaseRepository>,
    subject: &SubjectRef,
    category: PolicyCategory,
    now: DateTime<Utc>,
    correlation_id: uuid::Uuid,
) -> Result<Case, ModerationError> {
    let id = CaseId::for_subject(subject);
    match cases.find_by_id(&id).await? {
        Some(case) => Ok(case),
        None => Ok(Case::open(CaseOpenParams {
            subject: subject.clone(),
            category,
            queue: "default".into(),
            priority: "normal".into(),
            opened_at: now,
            correlation_id,
        })),
    }
}

// ─── Ingest a user report ─────────────────────────────────────────────────────

/// Where a report came from: the intake pipeline (`moderation.reports`), a
/// member in the app, or a guest (DSA Art. 16: anyone may report).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOrigin {
    Pipeline,
    Member,
    Guest,
}

impl ReportOrigin {
    /// The evidence signal a report of this origin adds to its case.
    pub fn signal(self) -> (&'static str, f64) {
        match self {
            Self::Pipeline | Self::Member => ("report", 0.5),
            Self::Guest => ("guest_report", 0.3),
        }
    }

    /// Who the reporter is: pipeline reports carry an account id, like a
    /// member's.
    pub fn reporter_kind(self) -> ReporterKind {
        match self {
            Self::Pipeline | Self::Member => ReporterKind::Member,
            Self::Guest => ReporterKind::Guest,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestReportCommand {
    pub reporter_id: ActorId,
    pub subject: SubjectRef,
    pub category: PolicyCategory,
    pub reason: String,
}

pub struct IngestReportHandler {
    reports: Arc<dyn ReportRepository>,
    cases: Arc<dyn CaseRepository>,
    publisher: Arc<dyn EventPublisher>,
    classifiers: Arc<dyn ClassifierGateway>,
}

impl IngestReportHandler {
    pub fn new(
        reports: Arc<dyn ReportRepository>,
        cases: Arc<dyn CaseRepository>,
        publisher: Arc<dyn EventPublisher>,
        classifiers: Arc<dyn ClassifierGateway>,
    ) -> Self {
        Self { reports, cases, publisher, classifiers }
    }

    /// Returns the (deterministic) id of the case the report fed into.
    pub async fn handle(
        &self,
        envelope: Envelope<IngestReportCommand>,
        now: DateTime<Utc>,
    ) -> Result<CaseId, ModerationError> {
        self.handle_as(envelope, now, ReportOrigin::Pipeline).await
    }

    /// As [`handle`](Self::handle), weighting the report by where it came from:
    /// a guest's report is a weaker signal than a member's.
    pub async fn handle_as(
        &self,
        envelope: Envelope<IngestReportCommand>,
        now: DateTime<Utc>,
        origin: ReportOrigin,
    ) -> Result<CaseId, ModerationError> {
        let cmd = envelope.payload;

        // The Report aggregate enforces the self-report invariant and dedup id.
        // It is recorded first, for the reporter (ListMyReports): idempotent, and
        // until the case is saved it simply reads as under review.
        let report = Report::file(
            cmd.reporter_id,
            origin.reporter_kind(),
            cmd.subject.clone(),
            cmd.category,
            cmd.reason,
            now,
        )?;
        self.reports.record(&report).await?;

        let mut case = open_or_load(&self.cases, &cmd.subject, cmd.category, now, envelope.correlation_id).await?;
        let (source, confidence) = origin.signal();
        let signal = Signal::new(source, cmd.category, Confidence::clamped(confidence), now)?;

        // A report on an already-resolved case is a no-op here (it would reopen via
        // a separate re-review path); fold it into success so the consumer commits.
        if case.add_signal(signal).is_ok() {
            self.cases.save(&case).await?;
            for event in &case.drain_events() {
                self.publisher.publish(event).await?;
            }
        }

        // Fan out to async classifiers (fire-and-forget).
        self.classifiers.request_classification(&cmd.subject).await?;
        Ok(case.id())
    }
}

// ─── Ingest a classifier signal ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IngestSignalCommand {
    pub subject: SubjectRef,
    pub source: String,
    pub category: PolicyCategory,
    pub confidence: Confidence,
}

pub struct IngestSignalHandler {
    cases: Arc<dyn CaseRepository>,
    publisher: Arc<dyn EventPublisher>,
}

impl IngestSignalHandler {
    pub fn new(cases: Arc<dyn CaseRepository>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { cases, publisher }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<IngestSignalCommand>,
        now: DateTime<Utc>,
    ) -> Result<CaseId, ModerationError> {
        let cmd = envelope.payload;
        let mut case = open_or_load(&self.cases, &cmd.subject, cmd.category, now, envelope.correlation_id).await?;
        let signal = Signal::new(cmd.source, cmd.category, cmd.confidence, now)?;
        if case.add_signal(signal).is_ok() {
            self.cases.save(&case).await?;
            for event in &case.drain_events() {
                self.publisher.publish(event).await?;
            }
        }
        Ok(case.id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::EntityType;
    use uuid::Uuid;

    fn subject() -> SubjectRef {
        SubjectRef::new(EntityType::Comment, "c1", ActorId::from_uuid(Uuid::from_u128(1)), "thread").unwrap()
    }

    fn report_env(reporter: u128) -> Envelope<IngestReportCommand> {
        Envelope::new(
            Uuid::now_v7(),
            IngestReportCommand {
                reporter_id: ActorId::from_uuid(Uuid::from_u128(reporter)),
                subject: subject(),
                category: PolicyCategory::Harassment,
                reason: "abusive".into(),
            },
        )
    }

    #[tokio::test]
    async fn first_report_opens_a_case_and_requests_classification() {
        let fx = Fixture::new();
        let id = fx.ingest_report_handler().handle(report_env(2), t0()).await.unwrap();
        assert_eq!(id, CaseId::for_subject(&subject()));
        assert_eq!(fx.cases.count(), 1);
        assert_eq!(fx.publisher.event_types(), vec!["moderation.case_opened"]);
        assert_eq!(fx.classifiers.request_count(), 1);
    }

    #[tokio::test]
    async fn redelivered_subject_reuses_the_same_case() {
        let fx = Fixture::new();
        fx.ingest_report_handler().handle(report_env(2), t0()).await.unwrap();
        fx.ingest_report_handler().handle(report_env(3), t0()).await.unwrap();
        // Same deterministic case, two signals, only one CaseOpened.
        assert_eq!(fx.cases.count(), 1);
        let opened = fx.publisher.event_types().iter().filter(|t| **t == "moderation.case_opened").count();
        assert_eq!(opened, 1);
        let case = fx.cases.find_by_id(&CaseId::for_subject(&subject())).await.unwrap().unwrap();
        assert_eq!(case.signals().len(), 2);
    }

    #[tokio::test]
    async fn self_report_is_rejected() {
        let fx = Fixture::new();
        // reporter == subject actor (id 1)
        let err = fx.ingest_report_handler().handle(report_env(1), t0()).await.unwrap_err();
        assert!(matches!(err, ModerationError::SelfReportRejected));
        assert_eq!(fx.cases.count(), 0);
    }

    #[tokio::test]
    async fn signal_ingestion_appends_to_the_case() {
        let fx = Fixture::new();
        let env = Envelope::new(
            Uuid::now_v7(),
            IngestSignalCommand {
                subject: subject(),
                source: "classifier:text-v2".into(),
                category: PolicyCategory::Hate,
                confidence: Confidence::new(0.92).unwrap(),
            },
        );
        fx.ingest_signal_handler().handle(env, t0()).await.unwrap();
        let case = fx.cases.find_by_id(&CaseId::for_subject(&subject())).await.unwrap().unwrap();
        assert_eq!(case.signals().len(), 1);
        assert_eq!(case.signals()[0].source(), "classifier:text-v2");
    }
}

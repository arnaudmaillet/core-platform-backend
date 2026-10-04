//! Client reports (DSA Art. 16: any individual may report illegal content,
//! guests included). The reporter comes from the client's token, never from the
//! request; the account behind the reported content is resolved server-side.
//! The report then follows the pipeline's ingestion path into the subject's case.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::command::{IngestReportCommand, IngestReportHandler, ReportOrigin};
use crate::application::port::{ReportRateLimiter, SubjectResolver};
use crate::domain::value_object::{ActorId, EntityType, PolicyCategory, ReportId, SubjectRef};
use crate::error::ModerationError;

/// Who files a client report, from the verified edge token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reporter {
    /// A signed-in account.
    Member(ActorId),
    /// A guest installation (its guest id).
    Guest(ActorId),
}

impl Reporter {
    fn id(self) -> ActorId {
        match self {
            Self::Member(id) | Self::Guest(id) => id,
        }
    }

    fn origin(self) -> ReportOrigin {
        match self {
            Self::Member(_) => ReportOrigin::Member,
            Self::Guest(_) => ReportOrigin::Guest,
        }
    }

    /// The rate-limit key: members and guests never share a bucket.
    fn quota_key(self) -> String {
        match self {
            Self::Member(id) => format!("member:{}", id.as_str()),
            Self::Guest(id) => format!("guest:{}", id.as_str()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmitReportCommand {
    pub reporter: Reporter,
    pub entity_type: EntityType,
    pub entity_id: String,
    /// Where in the app the report was made (e.g. "post_menu").
    pub surface: String,
    pub category: PolicyCategory,
    pub reason: String,
}

pub struct SubmitReportHandler {
    resolver: Arc<dyn SubjectResolver>,
    limiter: Arc<dyn ReportRateLimiter>,
    ingest: Arc<IngestReportHandler>,
}

impl SubmitReportHandler {
    pub fn new(
        resolver: Arc<dyn SubjectResolver>,
        limiter: Arc<dyn ReportRateLimiter>,
        ingest: Arc<IngestReportHandler>,
    ) -> Self {
        Self { resolver, limiter, ingest }
    }

    /// Returns the report's deterministic id (reporter × subject).
    pub async fn handle(
        &self,
        envelope: Envelope<SubmitReportCommand>,
        now: DateTime<Utc>,
    ) -> Result<ReportId, ModerationError> {
        let correlation_id = envelope.correlation_id;
        let cmd = envelope.payload;

        if !matches!(cmd.entity_type, EntityType::Post | EntityType::Comment | EntityType::Profile) {
            return Err(ModerationError::UnsupportedReportTarget {
                entity_type: cmd.entity_type.as_str().to_owned(),
            });
        }
        if cmd.entity_id.trim().is_empty() {
            return Err(ModerationError::InvalidSubjectRef("entity_id must not be empty".into()));
        }
        if !self.limiter.admit(&cmd.reporter.quota_key()).await? {
            return Err(ModerationError::ReportRateLimited);
        }

        let actor = self
            .resolver
            .responsible_account(cmd.entity_type, &cmd.entity_id)
            .await?
            .ok_or_else(|| ModerationError::ReportedContentNotFound {
                entity_type: cmd.entity_type.as_str().to_owned(),
                id: cmd.entity_id.clone(),
            })?;
        let subject = SubjectRef::new(cmd.entity_type, cmd.entity_id, actor, cmd.surface)?;
        let report_id = ReportId::for_report(cmd.reporter.id(), &subject);

        self.ingest
            .handle_as(
                Envelope::new(
                    correlation_id,
                    IngestReportCommand {
                        reporter_id: cmd.reporter.id(),
                        subject,
                        category: cmd.category,
                        reason: cmd.reason,
                    },
                ),
                now,
                cmd.reporter.origin(),
            )
            .await?;
        Ok(report_id)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::CaseRepository;
    use crate::domain::value_object::CaseId;

    fn handler(fx: &Fixture) -> SubmitReportHandler {
        SubmitReportHandler::new(
            Arc::clone(&fx.subjects) as _,
            Arc::clone(&fx.report_quota) as _,
            Arc::new(IngestReportHandler::new(
                Arc::clone(&fx.cases) as _,
                Arc::clone(&fx.publisher) as _,
                Arc::clone(&fx.classifiers) as _,
            )),
        )
    }

    fn cmd(reporter: Reporter, entity_type: EntityType, entity_id: &str) -> Envelope<SubmitReportCommand> {
        Envelope::new(
            Uuid::now_v7(),
            SubmitReportCommand {
                reporter,
                entity_type,
                entity_id: entity_id.into(),
                surface: "post_menu".into(),
                category: PolicyCategory::Spam,
                reason: "spam".into(),
            },
        )
    }

    fn id(n: u128) -> ActorId {
        ActorId::from_uuid(Uuid::from_u128(n))
    }

    async fn case_signal_sources(fx: &Fixture, entity_type: EntityType, entity_id: &str, owner: ActorId) -> Vec<String> {
        let subject = SubjectRef::new(entity_type, entity_id, owner, "post_menu").unwrap();
        let case = fx.cases.find_by_id(&CaseId::for_subject(&subject)).await.unwrap().expect("case opened");
        case.signals().iter().map(|s| s.source().to_owned()).collect()
    }

    #[tokio::test]
    async fn a_guest_report_resolves_the_owner_server_side_and_feeds_the_case_as_a_weaker_signal() {
        let fx = Fixture::new();
        let owner = id(7);
        fx.subjects.own("post-1", owner);

        let report_id = handler(&fx).handle(cmd(Reporter::Guest(id(99)), EntityType::Post, "post-1"), t0()).await.unwrap();
        let subject = SubjectRef::new(EntityType::Post, "post-1", owner, "post_menu").unwrap();
        assert_eq!(report_id, ReportId::for_report(id(99), &subject), "deterministic per reporter × subject");
        assert_eq!(case_signal_sources(&fx, EntityType::Post, "post-1", owner).await, vec!["guest_report"]);

        handler(&fx).handle(cmd(Reporter::Member(id(98)), EntityType::Post, "post-1"), t0()).await.unwrap();
        assert_eq!(
            case_signal_sources(&fx, EntityType::Post, "post-1", owner).await,
            vec!["guest_report", "report"],
            "the same case, the subject's account taken from the resolver, never the client"
        );
    }

    #[tokio::test]
    async fn the_quota_unknown_content_unsupported_targets_and_self_reports_are_refused() {
        let fx = Fixture::new();
        fx.subjects.own("post-1", id(7));
        let h = handler(&fx);

        assert!(matches!(
            h.handle(cmd(Reporter::Guest(id(99)), EntityType::Post, "missing"), t0()).await,
            Err(ModerationError::ReportedContentNotFound { .. })
        ));
        assert!(matches!(
            h.handle(cmd(Reporter::Guest(id(99)), EntityType::ChatMessage, "m-1"), t0()).await,
            Err(ModerationError::UnsupportedReportTarget { .. })
        ));
        assert!(matches!(
            h.handle(cmd(Reporter::Member(id(7)), EntityType::Post, "post-1"), t0()).await,
            Err(ModerationError::SelfReportRejected)
        ));

        // Quota: 20 per reporter in the fixture; the counter also counted the
        // refused calls above for guest 99, so use a fresh reporter.
        for _ in 0..20 {
            h.handle(cmd(Reporter::Guest(id(50)), EntityType::Post, "post-1"), t0()).await.unwrap();
        }
        assert!(matches!(
            h.handle(cmd(Reporter::Guest(id(50)), EntityType::Post, "post-1"), t0()).await,
            Err(ModerationError::ReportRateLimited)
        ));
        // Another reporter, and a member with the same uuid, have their own buckets.
        h.handle(cmd(Reporter::Member(id(50)), EntityType::Post, "post-1"), t0()).await.unwrap();
    }
}

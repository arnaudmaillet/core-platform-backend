use std::sync::Arc;

use chrono::DateTime;
use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::command::Reporter;
use crate::application::port::{FiledReport, ReportCursor, ReportRepository};
use crate::domain::value_object::{ReportId, ReportStatus};
use crate::error::ModerationError;

/// Default and maximum page sizes for [`ListMyReportsQuery`].
pub const DEFAULT_PAGE_SIZE: usize = 20;
pub const MAX_PAGE_SIZE: usize = 50;

/// The caller's own reports, newest first (DSA Art. 16(5)). The reporter comes
/// from the verified token, never from the request.
#[derive(Debug, Clone)]
pub struct ListMyReportsQuery {
    pub reporter: Reporter,
    /// Opaque cursor from the previous page; `None` for the first.
    pub page_token: Option<String>,
    /// `0` ⇒ [`DEFAULT_PAGE_SIZE`]; capped at [`MAX_PAGE_SIZE`].
    pub page_size: usize,
}

impl Query for ListMyReportsQuery {
    type Response = MyReportsPage;
}

/// One of the reporter's reports and what became of it.
#[derive(Debug, Clone)]
pub struct MyReport {
    pub filed: FiledReport,
    pub status: ReportStatus,
}

#[derive(Debug, Clone)]
pub struct MyReportsPage {
    pub reports: Vec<MyReport>,
    /// `None` on the last page.
    pub next_page_token: Option<String>,
}

pub struct ListMyReportsHandler {
    reports: Arc<dyn ReportRepository>,
}

impl ListMyReportsHandler {
    pub fn new(reports: Arc<dyn ReportRepository>) -> Self {
        Self { reports }
    }
}

impl QueryHandler<ListMyReportsQuery> for ListMyReportsHandler {
    type Error = ModerationError;

    async fn handle(&self, envelope: Envelope<ListMyReportsQuery>) -> Result<MyReportsPage, Self::Error> {
        let query = envelope.payload;
        let after = query.page_token.as_deref().map(decode_cursor).transpose()?;
        let limit = match query.page_size {
            0 => DEFAULT_PAGE_SIZE,
            n => n.min(MAX_PAGE_SIZE),
        };

        // One extra row tells whether another page follows.
        let mut filed = self
            .reports
            .list_for_reporter(query.reporter.kind(), &query.reporter.id(), after, limit + 1)
            .await?;
        let next_page_token = if filed.len() > limit {
            filed.truncate(limit);
            filed.last().map(|last| {
                encode_cursor(ReportCursor {
                    reported_at: last.report.reported_at(),
                    id: last.report.id(),
                })
            })
        } else {
            None
        };

        let reports = filed
            .into_iter()
            .map(|filed| MyReport { status: ReportStatus::of_case(filed.case_status), filed })
            .collect();
        Ok(MyReportsPage { reports, next_page_token })
    }
}

/// `"{reported_at_micros}_{report_id}"` — microseconds, the precision Postgres
/// stores, so the keyset bound is exact.
fn encode_cursor(cursor: ReportCursor) -> String {
    format!("{}_{}", cursor.reported_at.timestamp_micros(), cursor.id.as_str())
}

fn decode_cursor(token: &str) -> Result<ReportCursor, ModerationError> {
    let invalid = || ModerationError::InvalidIdentifier(format!("invalid page_token: '{token}'"));
    let (micros, id) = token.split_once('_').ok_or_else(invalid)?;
    let reported_at =
        DateTime::from_timestamp_micros(micros.parse().map_err(|_| invalid())?).ok_or_else(invalid)?;
    let id = Uuid::parse_str(id).map_err(|_| invalid())?;
    Ok(ReportCursor { reported_at, id: ReportId::from_uuid(id) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{SubmitReportCommand, SubmitReportHandler};
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::{ActorId, EntityType, PolicyCategory};
    use chrono::Duration;

    fn reporter() -> ActorId {
        ActorId::from_uuid(Uuid::from_u128(7))
    }

    async fn report(fx: &Fixture, handler: &SubmitReportHandler, who: Reporter, post: &str, minutes: i64) {
        fx.subjects.own(post, ActorId::from_uuid(Uuid::from_u128(99)));
        handler
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    SubmitReportCommand {
                        reporter: who,
                        entity_type: EntityType::Post,
                        entity_id: post.into(),
                        surface: "post_menu".into(),
                        category: PolicyCategory::Spam,
                        reason: format!("about {post}"),
                    },
                ),
                t0() + Duration::minutes(minutes),
            )
            .await
            .unwrap();
    }

    fn query(reporter: Reporter, page_token: Option<String>, page_size: usize) -> Envelope<ListMyReportsQuery> {
        Envelope::new(Uuid::now_v7(), ListMyReportsQuery { reporter, page_token, page_size })
    }

    #[tokio::test]
    async fn pages_a_reporters_own_reports_newest_first() {
        let fx = Fixture::new();
        let submit = fx.submit_report_handler();
        let me = Reporter::Member(reporter());
        for (i, post) in ["p1", "p2", "p3"].iter().enumerate() {
            report(&fx, &submit, me, post, i as i64).await;
        }
        // Someone else's report, and a guest sharing the same id, stay out.
        report(&fx, &submit, Reporter::Member(ActorId::from_uuid(Uuid::from_u128(8))), "p4", 9).await;
        report(&fx, &submit, Reporter::Guest(reporter()), "p5", 10).await;

        let handler = fx.list_my_reports_handler();
        let first = handler.handle(query(me, None, 2)).await.unwrap();
        let ids: Vec<_> = first.reports.iter().map(|r| r.filed.report.subject().entity_id().to_owned()).collect();
        assert_eq!(ids, vec!["p3", "p2"]);
        assert!(first.reports.iter().all(|r| r.status == ReportStatus::UnderReview));

        let second = handler.handle(query(me, first.next_page_token, 2)).await.unwrap();
        let ids: Vec<_> = second.reports.iter().map(|r| r.filed.report.subject().entity_id().to_owned()).collect();
        assert_eq!(ids, vec!["p1"]);
        assert!(second.next_page_token.is_none());
    }

    #[tokio::test]
    async fn a_garbled_page_token_is_refused() {
        let fx = Fixture::new();
        let err = fx
            .list_my_reports_handler()
            .handle(query(Reporter::Member(reporter()), Some("nope".into()), 0))
            .await
            .unwrap_err();
        assert!(matches!(err, ModerationError::InvalidIdentifier(_)));
    }
}

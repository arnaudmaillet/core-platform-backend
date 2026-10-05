use std::sync::Arc;

use chrono::DateTime;
use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::{AppealCursor, AppealRepository};
use crate::domain::aggregate::Appeal;
use crate::domain::value_object::{ActorId, AppealId};
use crate::error::ModerationError;

/// Default and maximum page sizes for [`ListMyAppealsQuery`].
pub const DEFAULT_PAGE_SIZE: usize = 20;
pub const MAX_PAGE_SIZE: usize = 50;

/// The caller's own appeals, newest first, each with its status and, once
/// resolved, the reviewer's reasons (DSA Art. 20(4)–(5)). The appellant comes
/// from the verified token, never from the request.
#[derive(Debug, Clone)]
pub struct ListMyAppealsQuery {
    pub appellant: ActorId,
    /// Opaque cursor from the previous page; `None` for the first.
    pub page_token: Option<String>,
    /// `0` ⇒ [`DEFAULT_PAGE_SIZE`]; capped at [`MAX_PAGE_SIZE`].
    pub page_size: usize,
}

impl Query for ListMyAppealsQuery {
    type Response = MyAppealsPage;
}

#[derive(Debug, Clone)]
pub struct MyAppealsPage {
    pub appeals: Vec<Appeal>,
    /// `None` on the last page.
    pub next_page_token: Option<String>,
}

pub struct ListMyAppealsHandler {
    appeals: Arc<dyn AppealRepository>,
}

impl ListMyAppealsHandler {
    pub fn new(appeals: Arc<dyn AppealRepository>) -> Self {
        Self { appeals }
    }
}

impl QueryHandler<ListMyAppealsQuery> for ListMyAppealsHandler {
    type Error = ModerationError;

    async fn handle(&self, envelope: Envelope<ListMyAppealsQuery>) -> Result<MyAppealsPage, Self::Error> {
        let query = envelope.payload;
        let after = query.page_token.as_deref().map(decode_cursor).transpose()?;
        let limit = match query.page_size {
            0 => DEFAULT_PAGE_SIZE,
            n => n.min(MAX_PAGE_SIZE),
        };

        // One extra row tells whether another page follows.
        let mut appeals = self.appeals.list_for_appellant(&query.appellant, after, limit + 1).await?;
        let next_page_token = if appeals.len() > limit {
            appeals.truncate(limit);
            appeals.last().map(|last| encode_cursor(AppealCursor { filed_at: last.filed_at(), id: last.id() }))
        } else {
            None
        };
        Ok(MyAppealsPage { appeals, next_page_token })
    }
}

/// `"{filed_at_micros}_{appeal_id}"` — microseconds, the precision Postgres
/// stores, so the keyset bound is exact.
fn encode_cursor(cursor: AppealCursor) -> String {
    format!("{}_{}", cursor.filed_at.timestamp_micros(), cursor.id.as_str())
}

fn decode_cursor(token: &str) -> Result<AppealCursor, ModerationError> {
    let invalid = || ModerationError::InvalidIdentifier(format!("invalid page_token: '{token}'"));
    let (micros, id) = token.split_once('_').ok_or_else(invalid)?;
    let filed_at = DateTime::from_timestamp_micros(micros.parse().map_err(|_| invalid())?).ok_or_else(invalid)?;
    let id = Uuid::parse_str(id).map_err(|_| invalid())?;
    Ok(AppealCursor { filed_at, id: AppealId::from_uuid(id) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::DecisionId;
    use chrono::Duration;

    fn me() -> ActorId {
        ActorId::from_uuid(Uuid::from_u128(7))
    }

    async fn appeal(fx: &Fixture, who: ActorId, minutes: i64) -> Appeal {
        let appeal = Appeal::file(DecisionId::new(), who, "unfair", t0() + Duration::minutes(minutes)).unwrap();
        fx.appeals.file(&appeal).await.unwrap()
    }

    fn query(page_token: Option<String>, page_size: usize) -> Envelope<ListMyAppealsQuery> {
        Envelope::new(Uuid::now_v7(), ListMyAppealsQuery { appellant: me(), page_token, page_size })
    }

    #[tokio::test]
    async fn the_callers_appeals_newest_first_in_pages() {
        let fx = Fixture::new();
        let handler = ListMyAppealsHandler::new(fx.appeals.clone());
        let (oldest, middle, newest) = (appeal(&fx, me(), 1).await, appeal(&fx, me(), 2).await, appeal(&fx, me(), 3).await);
        appeal(&fx, ActorId::from_uuid(Uuid::from_u128(8)), 4).await; // someone else's

        let first = handler.handle(query(None, 2)).await.unwrap();
        let ids: Vec<_> = first.appeals.iter().map(Appeal::id).collect();
        assert_eq!(ids, vec![newest.id(), middle.id()]);
        let second = handler.handle(query(first.next_page_token, 2)).await.unwrap();
        assert_eq!(second.appeals.iter().map(Appeal::id).collect::<Vec<_>>(), vec![oldest.id()]);
        assert!(second.next_page_token.is_none());
    }

    #[tokio::test]
    async fn a_forged_page_token_is_refused() {
        let fx = Fixture::new();
        let handler = ListMyAppealsHandler::new(fx.appeals.clone());
        assert!(handler.handle(query(Some("nope".into()), 2)).await.is_err());
    }
}

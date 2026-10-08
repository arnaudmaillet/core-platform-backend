//! An account's position on posts and comments (#665): its points, the
//! target's count just before its first like, and the count now — what the
//! wallet's stake settlement scores. Mesh only.

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::likes::{positions, LikePosition};
use crate::application::port::{LikeLedger, LikeStore};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// Targets one call may read.
pub const MAX_TARGETS: usize = 100;

pub struct GetLikePositionsQuery {
    pub account_id: String,
    pub targets:    Vec<LikeTarget>,
}

impl Query for GetLikePositionsQuery {
    type Response = Vec<LikePosition>;
}

pub struct GetLikePositionsHandler {
    pub like_store:  Arc<dyn LikeStore>,
    /// The durable copy, for a target whose likers expired from Redis.
    pub like_ledger: Option<Arc<dyn LikeLedger>>,
}

impl QueryHandler<GetLikePositionsQuery> for GetLikePositionsHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<GetLikePositionsQuery>) -> Result<Vec<LikePosition>, EngagementError> {
        let query = &envelope.payload;
        if query.targets.len() > MAX_TARGETS {
            return Err(EngagementError::DomainViolation {
                field:   "targets".into(),
                message: format!("at most {MAX_TARGETS} per call"),
            });
        }
        if uuid::Uuid::parse_str(&query.account_id).is_err() {
            return Err(EngagementError::DomainViolation { field: "account_id".into(), message: query.account_id.clone() });
        }
        positions(self.like_store.as_ref(), self.like_ledger.as_deref(), &query.account_id, &query.targets).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::Likes;
    use crate::application::port::Position;

    const ME: &str = "0199c3a0-0000-7000-8000-000000000001";
    const YOU: &str = "0199c3a0-0000-7000-8000-000000000002";

    async fn stake(likes: &Likes, target: &LikeTarget, account: &str, total: i64) {
        let applied = crate::application::likes::apply_total(likes, likes, target, account, total).await.unwrap();
        likes.record(target, account, "p", Position { total, arrival: applied.arrival }, 1).await.unwrap();
    }

    fn handler(likes: &Arc<Likes>, ledger: bool) -> GetLikePositionsHandler {
        GetLikePositionsHandler {
            like_store:  likes.clone(),
            like_ledger: ledger.then(|| likes.clone() as Arc<dyn LikeLedger>),
        }
    }

    async fn ask(handler: &GetLikePositionsHandler, targets: &[LikeTarget]) -> Vec<LikePosition> {
        let query = GetLikePositionsQuery { account_id: ME.into(), targets: targets.to_vec() };
        handler.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.unwrap()
    }

    #[tokio::test]
    async fn the_position_tells_how_early_the_account_came() {
        let likes = Arc::new(Likes::default());
        let (early, late, never) = (LikeTarget::Post("early".into()), LikeTarget::Post("late".into()), LikeTarget::Post("never".into()));
        stake(&likes, &early, ME, 5).await;
        stake(&likes, &early, YOU, 40).await;
        stake(&likes, &late, YOU, 40).await;
        stake(&likes, &late, ME, 5).await;
        // A later stake by the same account keeps its arrival.
        stake(&likes, &late, ME, 9).await;

        let got = ask(&handler(&likes, true), &[early.clone(), late.clone(), never]).await;
        assert_eq!(got[0], LikePosition { total: 5, arrival: Some(0), count: 45 });
        assert_eq!(got[1], LikePosition { total: 9, arrival: Some(40), count: 49 });
        assert_eq!(got[2], LikePosition { total: 0, arrival: None, count: 0 });

        // Expired from Redis: the durable copy answers.
        likes.expire(&late);
        let got = ask(&handler(&likes, true), std::slice::from_ref(&late)).await;
        assert_eq!(got[0], LikePosition { total: 9, arrival: Some(40), count: 49 });
        let got = ask(&handler(&likes, false), std::slice::from_ref(&late)).await;
        assert_eq!(got[0], LikePosition { total: 0, arrival: None, count: 49 }, "no durable copy on this instance");
    }

    #[tokio::test]
    async fn too_many_targets_or_a_bad_account_are_refused() {
        let likes = Arc::new(Likes::default());
        let h = handler(&likes, true);
        let many = vec![LikeTarget::Post("p".into()); MAX_TARGETS + 1];
        let query = GetLikePositionsQuery { account_id: ME.into(), targets: many };
        assert!(h.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.is_err());
        let query = GetLikePositionsQuery { account_id: "nope".into(), targets: vec![] };
        assert!(h.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.is_err());
    }
}

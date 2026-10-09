//! An account's likes when it is deleted (#665, GDPR Art. 17): who liked
//! goes, the counts stay — the points are kept, anonymously.
//!
//! The account is first marked erased (the stake consumer then drops its late
//! stakes), then each target it liked forgets it: its Redis entry goes, and in
//! Scylla its row becomes an anonymous one with the same total (so the counts
//! stay rebuildable from the durable copy) while its own list entry goes, both
//! at once. Scylla writes are stamped with the erasure's time, so a stake made
//! before it (always earlier) cannot write the account back. Re-running is
//! harmless: a forgotten target has left the list, and the anonymous row's id
//! depends only on the erasure ([`anonymous_liker`]), so a replay rewrites it
//! rather than adding a second one.

use std::sync::Arc;

use uuid::Uuid;

use crate::application::port::{ForgottenLike, LikeLedger, LikeStore};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// Targets forgotten per round.
const PAGE: i32 = 500;

/// The namespace of anonymous liker ids (UUIDv5; real account ids are v7, so
/// the two never collide).
const ANONYMOUS_LIKERS: Uuid = Uuid::from_u128(0x6c1b_9f3e_2d4a_4e8b_9a07_51c3_e2f6_0d18);

/// The anonymous liker that takes `account`'s place on `target`: the same for
/// every replay of one erasure (keyed by the deletion's time), and not
/// derivable from the account id alone.
pub fn anonymous_liker(account: &str, target: &LikeTarget, erased_at_micros: i64) -> Uuid {
    Uuid::new_v5(&ANONYMOUS_LIKERS, format!("{account}|{target}|{erased_at_micros}").as_bytes())
}

pub struct LikeEraser {
    pub store:  Arc<dyn LikeStore>,
    pub ledger: Arc<dyn LikeLedger>,
}

impl LikeEraser {
    /// Erases `account`, deleted at `erased_at_micros` (the deletion's own
    /// time, the same on every replay), with Scylla writes stamped
    /// `at_micros` (now: later than any of its stakes); returns the targets
    /// it was forgotten on.
    pub async fn erase(&self, account: &str, erased_at_micros: i64, at_micros: i64) -> Result<usize, EngagementError> {
        self.ledger.mark_erased(account, erased_at_micros).await?;
        let (mut forgotten, mut after) = (0, None);
        loop {
            let page = self.ledger.list_by_account(account, PAGE, after.as_ref()).await?;
            let likes: Vec<_> = page
                .into_iter()
                .map(|like| ForgottenLike {
                    anonymous_id: anonymous_liker(account, &like.target, erased_at_micros),
                    target:       like.target,
                    total:        like.total,
                    profile_ids:  like.profile_ids,
                })
                .collect();
            if likes.is_empty() {
                break;
            }
            let targets: Vec<_> = likes.iter().map(|l| l.target.clone()).collect();
            self.store.forget(account, &targets).await?;
            self.ledger.forget(account, &likes, at_micros).await?;
            forgotten += likes.len();
            if likes.len() < PAGE as usize {
                break;
            }
            after = targets.last().cloned();
        }
        self.ledger.forget_account(account, at_micros).await?;
        Ok(forgotten)
    }
}

#[cfg(test)]
mod tests {
    use crate::application::fakes::Likes;
    use crate::application::port::Position;
    use super::*;
    use crate::domain::value_object::LikeTarget;

    #[tokio::test]
    async fn who_liked_goes_the_counts_stay() {
        let likes = Arc::new(Likes::default());
        let targets: Vec<_> = (0..1_201).map(|i| LikeTarget::Post(format!("p{i:04}"))).collect();
        for t in &targets {
            likes.apply_total(t, "gone", 3).await.unwrap().unwrap();
            likes.record(t, "gone", "p", Position { total: 3, arrival: None }, 1).await.unwrap();
            likes.apply_total(t, "stays", 2).await.unwrap().unwrap();
            likes.record(t, "stays", "p", Position { total: 2, arrival: None }, 1).await.unwrap();
        }
        let eraser = LikeEraser { store: likes.clone(), ledger: likes.clone() };

        assert_eq!(eraser.erase("gone", 7, 10).await.unwrap(), 1_201, "every page");
        assert_eq!(likes.erased_at("gone").await.unwrap(), Some(7));
        assert_eq!(likes.mine("gone", &targets).await.unwrap(), vec![Some(0); 1_201]);
        assert_eq!(likes.counts(&targets).await.unwrap(), vec![5; 1_201], "the points are kept");
        assert_eq!(likes.mine("stays", &targets[..1]).await.unwrap(), vec![Some(2)]);
        assert!(likes.list_by_account("gone", 10, None).await.unwrap().is_empty());
        assert_eq!(likes.list_by_account("stays", 10, None).await.unwrap().len(), 10);
        // The durable copy still sums to the count, without the account.
        let by_target = likes.by_target.lock().unwrap().clone();
        assert!(by_target.values().all(|l| !l.contains_key("gone") && l.values().sum::<i64>() == 5));

        // A redelivered deletion finds nothing left.
        assert_eq!(eraser.erase("gone", 7, 11).await.unwrap(), 0);

        // A replay of a target's forget (a batch that timed out but landed)
        // rewrites the same anonymous row.
        let t = &targets[0];
        let again = ForgottenLike { target: t.clone(), total: 3, anonymous_id: anonymous_liker("gone", t, 7), profile_ids: Vec::new() };
        LikeLedger::forget(likes.as_ref(), "gone", &[again], 12).await.unwrap();
        assert_eq!(likes.by_target.lock().unwrap()[t].values().sum::<i64>(), 5, "no second anonymous row");
        assert_ne!(anonymous_liker("gone", t, 7), anonymous_liker("gone", t, 8), "keyed by the erasure");
    }
}

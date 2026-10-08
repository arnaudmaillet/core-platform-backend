//! A target's likers, whole again after they expired from Redis (#665): the
//! count never expires, but each account's total does after a while without a
//! like, and comes back from the durable copy (Scylla `likes_by_target`).
//! A stake rehydrates its target before applying (an account's total must be
//! known for the next one to add only the difference); a read falls back to
//! the durable copy for the reader and starts one rehydration in the
//! background. A rehydration racing an account's erasure forgets it again:
//! the account is marked erased before the eraser forgets it in Redis, so a
//! total loaded after that is followed by a check that sees the mark.

use std::sync::Arc;

use crate::application::port::{LikeLedger, LikeStore};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// Likers loaded per round.
const PAGE: i32 = 1_000;

/// Loads every liker of `target` from the durable copy; returns how many.
pub async fn rehydrate(store: &dyn LikeStore, ledger: &dyn LikeLedger, target: &LikeTarget) -> Result<usize, EngagementError> {
    let (mut loaded, mut after) = (0, None::<String>);
    loop {
        let page = ledger.likers_of(target, PAGE, after.as_deref()).await?;
        let last = page.len() < PAGE as usize;
        store.rehydrate(target, &page, last).await?;
        let accounts: Vec<String> = page.iter().map(|(account, _)| account.clone()).collect();
        for erased in ledger.erased_among(&accounts).await? {
            store.forget(&erased, std::slice::from_ref(target)).await?;
        }
        loaded += page.len();
        if last {
            return Ok(loaded);
        }
        after = page.last().map(|(account, _)| account.clone());
    }
}

/// Applies `account`'s `total` on `target`, rehydrating the target first if
/// its likers expired; returns the likes added.
pub async fn apply_total(
    store: &dyn LikeStore,
    ledger: &dyn LikeLedger,
    target: &LikeTarget,
    account: &str,
    total: i64,
) -> Result<i64, EngagementError> {
    if let Some(added) = store.apply_total(target, account, total).await? {
        return Ok(added);
    }
    let loaded = rehydrate(store, ledger, target).await?;
    tracing::info!(target = %target, loaded, "likers rehydrated");
    store.apply_total(target, account, total).await?.ok_or(EngagementError::ScriptReturnInvalid)
}

/// `account`'s likes on each target: from Redis, or — where its likers
/// expired — from the durable copy (0 without one), starting their
/// rehydration in the background.
pub async fn mine(
    store: &Arc<dyn LikeStore>,
    ledger: Option<&Arc<dyn LikeLedger>>,
    account: &str,
    targets: &[LikeTarget],
) -> Result<Vec<i64>, EngagementError> {
    let held = store.mine(account, targets).await?;
    let mut out = Vec::with_capacity(targets.len());
    for (target, held) in targets.iter().zip(held) {
        let mine = match (held, ledger) {
            (Some(mine), _) => mine,
            (None, None) => 0,
            (None, Some(ledger)) => {
                if store.claim_rehydration(target).await? {
                    let (store, ledger, target) = (Arc::clone(store), Arc::clone(ledger), target.clone());
                    tokio::spawn(async move {
                        if let Err(error) = rehydrate(store.as_ref(), ledger.as_ref(), &target).await {
                            tracing::warn!(%error, target = %target, "likers rehydration failed");
                        }
                    });
                }
                ledger.total_of(target, account).await?.unwrap_or(0)
            }
        };
        out.push(mine);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::Likes;

    async fn stake(likes: &Likes, target: &LikeTarget, account: &str, total: i64) -> i64 {
        let added = apply_total(likes, likes, target, account, total).await.unwrap();
        likes.record(target, account, "p", total, 1).await.unwrap();
        added
    }

    #[tokio::test]
    async fn after_the_likers_expire_a_stake_adds_only_the_difference() {
        let likes = Likes::default();
        let post = LikeTarget::Post("p1".into());
        assert_eq!(stake(&likes, &post, "a", 10).await, 10);
        assert_eq!(stake(&likes, &post, "b", 4).await, 4);

        likes.expire(&post);
        assert_eq!(likes.mine("a", std::slice::from_ref(&post)).await.unwrap(), vec![None], "unknown, not 0");
        assert_eq!(likes.apply_total(&post, "a", 15).await.unwrap(), None, "the store asks for the floor");

        // The stake rehydrates the target first: 5 more, not 15.
        assert_eq!(stake(&likes, &post, "a", 15).await, 5);
        assert_eq!(likes.counts(std::slice::from_ref(&post)).await.unwrap(), vec![19]);
        assert_eq!(likes.mine("b", std::slice::from_ref(&post)).await.unwrap(), vec![Some(4)], "every liker is back");
        assert_eq!(likes.mine("never", std::slice::from_ref(&post)).await.unwrap(), vec![Some(0)], "and whole again");
        // A redelivery of the old total changes nothing.
        assert_eq!(stake(&likes, &post, "b", 4).await, 0);
    }

    /// An erasure racing a rehydration: the account's row was read before
    /// the eraser forgot it; the rehydration forgets it again.
    #[tokio::test]
    async fn a_rehydration_never_brings_a_deleted_account_back() {
        let likes = Likes::default();
        let post = LikeTarget::Post("p1".into());
        stake(&likes, &post, "gone", 6).await;
        stake(&likes, &post, "stays", 2).await;
        likes.expire(&post);
        // Marked erased, its durable row not yet swapped (the race).
        likes.mark_erased("gone", 1).await.unwrap();

        rehydrate(&likes, &likes, &post).await.unwrap();
        let one = std::slice::from_ref(&post);
        assert_eq!(likes.mine("gone", one).await.unwrap(), vec![Some(0)]);
        assert_eq!(likes.mine("stays", one).await.unwrap(), vec![Some(2)]);
        assert_eq!(likes.counts(one).await.unwrap(), vec![8], "the counts stay");
    }

    #[tokio::test]
    async fn a_reader_whose_likes_expired_reads_them_from_the_durable_copy() {
        let likes = Arc::new(Likes::default());
        let post = LikeTarget::Post("p1".into());
        stake(&likes, &post, "a", 7).await;
        likes.expire(&post);

        let store: Arc<dyn LikeStore> = likes.clone();
        let ledger: Arc<dyn LikeLedger> = likes.clone();
        let targets = [post.clone()];
        assert_eq!(mine(&store, Some(&ledger), "a", &targets).await.unwrap(), vec![7]);
        assert_eq!(mine(&store, Some(&ledger), "never", &targets).await.unwrap(), vec![0]);
        // Without a durable copy on this instance: 0, as before.
        likes.expire(&post);
        assert_eq!(mine(&store, None, "a", &targets).await.unwrap(), vec![0]);
    }
}

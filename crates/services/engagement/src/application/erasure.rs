//! An account's likes when it is deleted (#665, GDPR Art. 17): who liked
//! goes, the counts stay — the points are kept, anonymously.
//!
//! The account is first marked erased (the stake consumer then drops its late
//! stakes), then each target it liked forgets it, in Redis and in Scylla, and
//! last its own list. Scylla deletes are stamped with the erasure's time, so a
//! stake made before it (always earlier) cannot write the account back.
//! Re-running is harmless: the list is deleted last, so a failed pass is
//! picked up where it stopped.

use std::sync::Arc;

use crate::application::port::{LikeLedger, LikeStore};
use crate::error::EngagementError;

/// Targets forgotten per round.
const PAGE: i32 = 500;

pub struct LikeEraser {
    pub store:  Arc<dyn LikeStore>,
    pub ledger: Arc<dyn LikeLedger>,
}

impl LikeEraser {
    /// Erases `account`'s likes as of `at_micros`; returns the targets it
    /// was forgotten on.
    pub async fn erase(&self, account: &str, at_micros: i64) -> Result<usize, EngagementError> {
        self.ledger.mark_erased(account).await?;
        let (mut forgotten, mut after) = (0, None);
        loop {
            let page = self.ledger.list_by_account(account, PAGE, after.as_ref()).await?;
            let targets: Vec<_> = page.into_iter().map(|like| like.target).collect();
            if targets.is_empty() {
                break;
            }
            self.store.forget(account, &targets).await?;
            self.ledger.forget(account, &targets, at_micros).await?;
            forgotten += targets.len();
            if targets.len() < PAGE as usize {
                break;
            }
            after = targets.last().cloned();
        }
        self.ledger.forget_account(account, at_micros).await?;
        Ok(forgotten)
    }
}

#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::Utc;

    use super::*;
    use crate::application::port::AccountLike;
    use crate::domain::value_object::LikeTarget;

    /// Redis and Scylla in one: per target, each account's total and the sum.
    #[derive(Default)]
    pub struct Likes {
        pub likers: Mutex<HashMap<LikeTarget, HashMap<String, i64>>>,
        pub counts: Mutex<HashMap<LikeTarget, i64>>,
        pub rows:   Mutex<BTreeMap<(String, String, String), i64>>,
        pub erased: Mutex<HashSet<String>>,
    }

    fn key(t: &LikeTarget) -> (String, String) {
        (t.kind().to_owned(), t.id().to_owned())
    }

    #[async_trait]
    impl LikeStore for Likes {
        async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<i64, EngagementError> {
            let mut likers = self.likers.lock().unwrap();
            let held = likers.entry(target.clone()).or_default().entry(account.to_owned()).or_insert(0);
            let added = (total - *held).max(0);
            *held = (*held).max(total);
            *self.counts.lock().unwrap().entry(target.clone()).or_insert(0) += added;
            Ok(added)
        }
        async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
            let counts = self.counts.lock().unwrap();
            Ok(targets.iter().map(|t| counts.get(t).copied().unwrap_or(0)).collect())
        }
        async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
            let likers = self.likers.lock().unwrap();
            Ok(targets.iter().map(|t| likers.get(t).and_then(|l| l.get(account)).copied().unwrap_or(0)).collect())
        }
        async fn forget(&self, account: &str, targets: &[LikeTarget]) -> Result<(), EngagementError> {
            let mut likers = self.likers.lock().unwrap();
            for t in targets {
                if let Some(l) = likers.get_mut(t) {
                    l.remove(account);
                }
            }
            Ok(())
        }
    }

    #[async_trait]
    impl LikeLedger for Likes {
        async fn record(&self, target: &LikeTarget, account: &str, _: &str, total: i64, _: i64) -> Result<(), EngagementError> {
            let (kind, id) = key(target);
            self.rows.lock().unwrap().insert((account.to_owned(), kind, id), total);
            Ok(())
        }
        async fn list_by_account(
            &self,
            account: &str,
            limit: i32,
            after: Option<&LikeTarget>,
        ) -> Result<Vec<AccountLike>, EngagementError> {
            let after = after.map(key);
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|((a, kind, id), _)| a == account && after.as_ref().is_none_or(|k| (kind, id) > (&k.0, &k.1)))
                .take(limit as usize)
                .map(|((_, kind, id), total)| AccountLike {
                    target:     LikeTarget::parse(kind, id).unwrap(),
                    total:      *total,
                    profile_id: "p".into(),
                    liked_at:   Utc::now(),
                })
                .collect())
        }
        async fn mark_erased(&self, account: &str) -> Result<(), EngagementError> {
            self.erased.lock().unwrap().insert(account.to_owned());
            Ok(())
        }
        async fn is_erased(&self, account: &str) -> Result<bool, EngagementError> {
            Ok(self.erased.lock().unwrap().contains(account))
        }
        /// The per-target copy is not modeled: the account's list stands for
        /// both, and goes last.
        async fn forget(&self, _: &str, _: &[LikeTarget], _: i64) -> Result<(), EngagementError> {
            Ok(())
        }
        async fn forget_account(&self, account: &str, _: i64) -> Result<(), EngagementError> {
            self.rows.lock().unwrap().retain(|(a, _, _), _| a != account);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::Likes;
    use super::*;
    use crate::domain::value_object::LikeTarget;

    #[tokio::test]
    async fn who_liked_goes_the_counts_stay() {
        let likes = Arc::new(Likes::default());
        let targets: Vec<_> = (0..1_201).map(|i| LikeTarget::Post(format!("p{i:04}"))).collect();
        for t in &targets {
            likes.apply_total(t, "gone", 3).await.unwrap();
            likes.record(t, "gone", "p", 3, 1).await.unwrap();
            likes.apply_total(t, "stays", 2).await.unwrap();
            likes.record(t, "stays", "p", 2, 1).await.unwrap();
        }
        let eraser = LikeEraser { store: likes.clone(), ledger: likes.clone() };

        assert_eq!(eraser.erase("gone", 10).await.unwrap(), 1_201, "every page");
        assert!(likes.is_erased("gone").await.unwrap());
        assert_eq!(likes.mine("gone", &targets).await.unwrap(), vec![0; 1_201]);
        assert_eq!(likes.counts(&targets).await.unwrap(), vec![5; 1_201], "the points are kept");
        assert_eq!(likes.mine("stays", &targets[..1]).await.unwrap(), vec![2]);
        assert!(likes.list_by_account("gone", 10, None).await.unwrap().is_empty());
        assert_eq!(likes.list_by_account("stays", 10, None).await.unwrap().len(), 10);

        // A redelivered deletion finds nothing left.
        assert_eq!(eraser.erase("gone", 11).await.unwrap(), 0);
    }
}

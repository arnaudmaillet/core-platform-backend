//! In-memory likes for the application's tests.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;

use crate::application::port::{ForgottenLike, LikeLedger, LikeStore};
use crate::error::EngagementError;
use crate::application::port::AccountLike;
use crate::domain::value_object::LikeTarget;

/// Redis and Scylla in one: per target, each account's total and the sum
/// (the likers expire as Redis's do: [`Likes::expire`]); the durable copy by
/// account and by target.
#[derive(Default)]
pub struct Likes {
    pub likers:    Mutex<HashMap<LikeTarget, HashMap<String, i64>>>,
    /// Targets whose likers are whole (Redis's `_complete`).
    pub complete:  Mutex<HashSet<LikeTarget>>,
    pub counts:    Mutex<HashMap<LikeTarget, i64>>,
    pub rows:      Mutex<BTreeMap<(String, String, String), i64>>,
    pub by_target: Mutex<HashMap<LikeTarget, HashMap<String, i64>>>,
    pub erased:    Mutex<HashMap<String, i64>>,
}

impl Likes {
    /// The target's likers expire from Redis (its count stays).
    pub fn expire(&self, target: &LikeTarget) {
        self.likers.lock().unwrap().remove(target);
        self.complete.lock().unwrap().remove(target);
    }

    /// As the scripts' `known`.
    fn known(&self, target: &LikeTarget, account: &str) -> Option<i64> {
        let held = self.likers.lock().unwrap().get(target).and_then(|l| l.get(account)).copied();
        let count = self.counts.lock().unwrap().get(target).copied().unwrap_or(0);
        held.or_else(|| (self.complete.lock().unwrap().contains(target) || count == 0).then_some(0))
    }
}

fn key(t: &LikeTarget) -> (String, String) {
    (t.kind().to_owned(), t.id().to_owned())
}

#[async_trait]
impl LikeStore for Likes {
    async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<Option<i64>, EngagementError> {
        let Some(old) = self.known(target, account) else { return Ok(None) };
        let fresh = !self.likers.lock().unwrap().contains_key(target);
        if fresh && self.counts.lock().unwrap().get(target).copied().unwrap_or(0) == 0 {
            self.complete.lock().unwrap().insert(target.clone());
        }
        let added = (total - old).max(0);
        if added > 0 {
            self.likers.lock().unwrap().entry(target.clone()).or_default().insert(account.to_owned(), total);
            *self.counts.lock().unwrap().entry(target.clone()).or_insert(0) += added;
        }
        Ok(Some(added))
    }
    async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError> {
        let counts = self.counts.lock().unwrap();
        Ok(targets.iter().map(|t| counts.get(t).copied().unwrap_or(0)).collect())
    }
    async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<Option<i64>>, EngagementError> {
        Ok(targets.iter().map(|t| self.known(t, account)).collect())
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
    async fn rehydrate(&self, target: &LikeTarget, likers: &[(String, i64)], complete: bool) -> Result<(), EngagementError> {
        let mut held = self.likers.lock().unwrap();
        let held = held.entry(target.clone()).or_default();
        for (account, total) in likers {
            held.entry(account.clone()).or_insert(*total);
        }
        if complete {
            self.complete.lock().unwrap().insert(target.clone());
        }
        Ok(())
    }
    async fn claim_rehydration(&self, _: &LikeTarget) -> Result<bool, EngagementError> {
        Ok(true)
    }
}

#[async_trait]
impl LikeLedger for Likes {
    async fn erased_among(&self, accounts: &[String]) -> Result<Vec<String>, EngagementError> {
        let erased = self.erased.lock().unwrap();
        Ok(accounts.iter().filter(|a| erased.contains_key(a.as_str())).cloned().collect())
    }
    async fn total_of(&self, target: &LikeTarget, account: &str) -> Result<Option<i64>, EngagementError> {
        Ok(self.by_target.lock().unwrap().get(target).and_then(|l| l.get(account)).copied())
    }
    async fn likers_of(&self, target: &LikeTarget, limit: i32, after: Option<&str>) -> Result<Vec<(String, i64)>, EngagementError> {
        let mut likers: Vec<_> =
            self.by_target.lock().unwrap().get(target).map(|l| l.clone().into_iter().collect()).unwrap_or_default();
        likers.sort();
        Ok(likers.into_iter().filter(|(a, _)| after.is_none_or(|after| a.as_str() > after)).take(limit as usize).collect())
    }
    async fn record(&self, target: &LikeTarget, account: &str, _: &str, total: i64, _: i64) -> Result<(), EngagementError> {
        let (kind, id) = key(target);
        self.rows.lock().unwrap().insert((account.to_owned(), kind, id), total);
        self.by_target.lock().unwrap().entry(target.clone()).or_default().insert(account.to_owned(), total);
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
    async fn mark_erased(&self, account: &str, erased_at: i64) -> Result<(), EngagementError> {
        self.erased.lock().unwrap().insert(account.to_owned(), erased_at);
        Ok(())
    }
    async fn erased_at(&self, account: &str) -> Result<Option<i64>, EngagementError> {
        Ok(self.erased.lock().unwrap().get(account).copied())
    }
    async fn forget(&self, account: &str, likes: &[ForgottenLike], _: i64) -> Result<(), EngagementError> {
        let mut by_target = self.by_target.lock().unwrap();
        let mut rows = self.rows.lock().unwrap();
        for like in likes {
            let likers = by_target.entry(like.target.clone()).or_default();
            likers.remove(account);
            likers.insert(like.anonymous_id.to_string(), like.total);
            let (kind, id) = key(&like.target);
            rows.remove(&(account.to_owned(), kind, id));
        }
        Ok(())
    }
    async fn forget_account(&self, account: &str, _: i64) -> Result<(), EngagementError> {
        self.rows.lock().unwrap().retain(|(a, _, _), _| a != account);
        Ok(())
    }
}


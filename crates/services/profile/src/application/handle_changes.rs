//! Paid handle changes (#911): an account's first handle change is free,
//! later ones cost gems (a gem sink with no effect on ranking or visibility,
//! charter V5.3 §35). A caller who may not spend gems (under 18, or age
//! unknown) changes for free, once per 30 days.
//!
//! The gems are spent only once the new handle is claimed, keyed by the
//! account's change number (`handle-change-<n>`): a retry pays once, and a
//! taken or invalid handle is never charged. The change count grows `n - 1 →
//! n` once the change is saved. Mesh callers (no end user) change for free,
//! uncounted.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};

use crate::application::port::{GemPurchase, GemWallet, ProfileRepository};
use crate::domain::value_object::{AccountId, ProfileId};
use crate::error::ProfileError;

/// An account's handle changes so far, and the last one's time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleChangeRecord {
    pub changes: i32,
    pub last_at: DateTime<Utc>,
}

/// Where each account's handle changes are counted (no row: none yet).
#[async_trait]
pub trait HandleChangeStore: Send + Sync + 'static {
    async fn get(&self, account: &AccountId) -> Result<Option<HandleChangeRecord>, ProfileError>;

    /// `from` → `from + 1` changes at `at`, if the account holds `from` (LWT).
    async fn record(&self, account: &AccountId, from: i32, at: DateTime<Utc>) -> Result<(), ProfileError>;

    /// The account was deleted.
    async fn erase(&self, account: &AccountId) -> Result<(), ProfileError>;
}

/// The price policy (server policy, echoed by `GetHandleChangePrice`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleChangePolicy {
    /// A paid change's price in gems.
    pub price:    i64,
    /// Between two free changes of a caller who may not spend gems.
    pub cooldown: TimeDelta,
}

impl Default for HandleChangePolicy {
    fn default() -> Self {
        Self { price: 100, cooldown: TimeDelta::days(30) }
    }
}

/// Who changes a handle: whether they may spend gems (an adult, from the
/// edge token), and that token, forwarded to the wallet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandlePayer {
    pub adult: bool,
    pub token: Option<String>,
}

/// The next change of an account, for a payer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleQuote {
    /// In gems; 0 = free.
    pub price:          i64,
    /// The payer may not spend gems: free, once per cooldown.
    pub restricted:     bool,
    /// `restricted` only: when the next free change opens, if not yet.
    pub next_change_at: Option<DateTime<Utc>>,
    /// The account's changes so far.
    pub changes:        i32,
}

pub struct HandleChanges {
    pub store:  Arc<dyn HandleChangeStore>,
    /// `None` in unit tests: `quote_for_profile` needs it.
    pub repo:   Option<Arc<dyn ProfileRepository>>,
    /// `None`: a paid change cannot be charged (`WalletUnavailable`).
    pub wallet: Option<Arc<dyn GemWallet>>,
    pub policy: HandleChangePolicy,
}

impl HandleChanges {
    /// The quote of `profile`'s account (`GetHandleChangePrice`).
    pub async fn quote_for_profile(
        &self,
        profile: &ProfileId,
        payer: &HandlePayer,
        now: DateTime<Utc>,
    ) -> Result<HandleQuote, ProfileError> {
        let repo = self.repo.as_ref().ok_or_else(|| ProfileError::ProfileNotFound { id: profile.to_string() })?;
        let found = repo.find_by_id(profile).await?.ok_or_else(|| ProfileError::ProfileNotFound { id: profile.to_string() })?;
        self.quote(&found.account_id(), payer, now).await
    }

    pub async fn quote(&self, account: &AccountId, payer: &HandlePayer, now: DateTime<Utc>) -> Result<HandleQuote, ProfileError> {
        let record = self.store.get(account).await?;
        let changes = record.map_or(0, |r| r.changes);
        Ok(if payer.adult {
            HandleQuote { price: if changes == 0 { 0 } else { self.policy.price }, restricted: false, next_change_at: None, changes }
        } else {
            let next = record.map(|r| r.last_at + self.policy.cooldown).filter(|next| *next > now);
            HandleQuote { price: 0, restricted: true, next_change_at: next, changes }
        })
    }

    /// Spends a quoted price for `profile`'s change: `false` when the wallet
    /// holds too few gems.
    pub async fn charge(
        &self,
        account: &AccountId,
        profile: ProfileId,
        quote: &HandleQuote,
        payer: &HandlePayer,
    ) -> Result<bool, ProfileError> {
        let wallet = self.wallet.as_ref().ok_or_else(|| ProfileError::WalletUnavailable { reason: "not configured".into() })?;
        let key = format!("handle-change-{}", quote.changes + 1);
        let spend = wallet
            .spend(account, GemPurchase::HandleChange { profile }, quote.price, &key, payer.token.as_deref())
            .await?;
        Ok(spend.spent)
    }

    /// Counts a saved change.
    pub async fn record(&self, account: &AccountId, quote: &HandleQuote, now: DateTime<Utc>) -> Result<(), ProfileError> {
        self.store.record(account, quote.changes, now).await
    }
}

#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    pub struct MemHandleChanges(pub Mutex<HashMap<AccountId, HandleChangeRecord>>);

    #[async_trait]
    impl HandleChangeStore for MemHandleChanges {
        async fn get(&self, account: &AccountId) -> Result<Option<HandleChangeRecord>, ProfileError> {
            Ok(self.0.lock().unwrap().get(account).copied())
        }
        async fn record(&self, account: &AccountId, from: i32, at: DateTime<Utc>) -> Result<(), ProfileError> {
            let mut map = self.0.lock().unwrap();
            if map.get(account).map_or(0, |r| r.changes) == from {
                map.insert(*account, HandleChangeRecord { changes: from + 1, last_at: at });
            }
            Ok(())
        }
        async fn erase(&self, account: &AccountId) -> Result<(), ProfileError> {
            self.0.lock().unwrap().remove(account);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    fn changes() -> (HandleChanges, Arc<MemHandleChanges>) {
        let store = Arc::new(MemHandleChanges::default());
        (HandleChanges { store: Arc::clone(&store) as _, repo: None, wallet: None, policy: HandleChangePolicy::default() }, store)
    }

    #[tokio::test]
    async fn the_first_change_is_free_then_adults_pay_and_minors_wait() {
        let (c, store) = changes();
        let me = AccountId::from_uuid(uuid::Uuid::now_v7());
        let adult = HandlePayer { adult: true, token: None };
        let minor = HandlePayer::default();
        let now = Utc::now();
        assert_eq!(c.quote(&me, &adult, now).await.unwrap().price, 0, "the first change");
        c.record(&me, &c.quote(&me, &adult, now).await.unwrap(), now).await.unwrap();
        let paid = c.quote(&me, &adult, now).await.unwrap();
        assert_eq!((paid.price, paid.changes, paid.restricted), (100, 1, false));

        let waiting = c.quote(&me, &minor, now + TimeDelta::days(29)).await.unwrap();
        assert_eq!((waiting.price, waiting.restricted, waiting.next_change_at), (0, true, Some(now + TimeDelta::days(30))));
        let open = c.quote(&me, &minor, now + TimeDelta::days(30)).await.unwrap();
        assert_eq!(open.next_change_at, None, "the cooldown is over");

        // A raced record counts once.
        c.record(&me, &paid, now).await.unwrap();
        c.record(&me, &paid, now).await.unwrap();
        assert_eq!(store.get(&me).await.unwrap().unwrap().changes, 2);
        assert!(matches!(c.charge(&me, ProfileId::new(), &paid, &adult).await, Err(ProfileError::WalletUnavailable { .. })));
    }
}

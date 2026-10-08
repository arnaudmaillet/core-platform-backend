//! The wallet's use cases: read it, claim the hourly reward, page the
//! history, erase it with its account.

use std::sync::Arc;

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use uuid::Uuid;

use crate::application::port::{ClaimOutcome, GemSpend, PackOutcome, SpendOutcome, TransactionCursor, WalletStore};
use crate::config::WalletConfig;
use crate::domain::{AccountId, ClaimState, Currency, IdempotencyKey, Operation, Transaction, TransactionKind, Wallet};
use crate::error::WalletError;

/// Default and maximum history page sizes.
pub const DEFAULT_PAGE: usize = 50;
pub const MAX_PAGE: usize = 100;

/// A wallet and its claim surface, at a moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletView {
    pub wallet: Wallet,
    pub claim:  ClaimState,
}

/// A claim's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimReply {
    pub outcome: ClaimOutcome,
    pub awarded: i32,
    pub view:    WalletView,
}

/// A stake pack purchase's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackReply {
    pub outcome: PackOutcome,
    pub view:    WalletView,
}

/// Longest `ref_id` a spend may name.
const MAX_REF_LEN: usize = 64;

/// One page of history; `next_page_token` is `None` on the last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPage {
    pub transactions:    Vec<Transaction>,
    pub next_page_token: Option<String>,
}

pub struct Wallets {
    store:  Arc<dyn WalletStore>,
    config: WalletConfig,
}

impl Wallets {
    pub fn new(store: Arc<dyn WalletStore>, config: WalletConfig) -> Self {
        Self { store, config }
    }

    /// The economy's terms (echoed to the app).
    pub fn config(&self) -> &WalletConfig {
        &self.config
    }

    /// The account's wallet (opened on first use).
    pub async fn get(&self, account: &str, now: DateTime<Utc>) -> Result<WalletView, WalletError> {
        let account = AccountId::parse(account)?;
        let now = ledger_time(now);
        let wallet = self.store.open(&account, self.config.starter_gems, now).await?;
        Ok(self.view(wallet, now))
    }

    /// Claims the hourly reward; `TooEarly` and `DailyCapReached` are
    /// ordinary answers, not errors.
    pub async fn claim(&self, account: &str, key: &str, now: DateTime<Utc>) -> Result<ClaimReply, WalletError> {
        let account = AccountId::parse(account)?;
        let key = IdempotencyKey::for_operation(Operation::Claim, key)?;
        let now = ledger_time(now);
        let result = self.store.claim(&account, &key, &self.config.claim, self.config.starter_gems, now).await?;
        Ok(ClaimReply { outcome: result.outcome, awarded: result.awarded, view: self.view(result.wallet, now) })
    }

    /// Buys the ×100 stake pack with gems. `restricted`: the caller may not
    /// spend gems (under 18, or age unknown) — `WAL-3001`.
    pub async fn buy_stake_pack(
        &self,
        account: &str,
        key: &str,
        restricted: bool,
        now: DateTime<Utc>,
    ) -> Result<PackReply, WalletError> {
        let account = AccountId::parse(account)?;
        let key = IdempotencyKey::for_operation(Operation::StakePack, key)?;
        if restricted {
            return Err(WalletError::GemSpendingRestricted);
        }
        let now = ledger_time(now);
        let (outcome, wallet) =
            self.store.buy_stake_pack(&account, &key, &self.config.stake_pack, self.config.starter_gems, now).await?;
        Ok(PackReply { outcome, view: self.view(wallet, now) })
    }

    /// Another service spends gems (a country unlock); it checked the
    /// spender may. Returns the outcome and the gems left.
    pub async fn spend_gems(
        &self,
        account: &str,
        key: &str,
        amount: i64,
        kind: TransactionKind,
        ref_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(SpendOutcome, i64), WalletError> {
        let account = AccountId::parse(account)?;
        let key = IdempotencyKey::for_operation(Operation::SpendGems, key)?;
        if amount <= 0 {
            return Err(WalletError::InvalidSpend { reason: "the amount must be positive".into() });
        }
        if kind != TransactionKind::CountryUnlock {
            return Err(WalletError::InvalidSpend { reason: format!("{} is not a gem spend", kind.as_str()) });
        }
        if ref_id.len() > MAX_REF_LEN {
            return Err(WalletError::InvalidSpend { reason: "ref_id is longer than 64 characters".into() });
        }
        let spend = GemSpend { amount, kind, ref_id: Some(ref_id.to_owned()).filter(|r| !r.is_empty()) };
        let (outcome, wallet) =
            self.store.spend_gems(&account, &key, &spend, self.config.starter_gems, ledger_time(now)).await?;
        Ok((outcome, wallet.gems))
    }

    /// One page of the account's history, newest first.
    pub async fn history(
        &self,
        account: &str,
        currency: Option<Currency>,
        page_size: usize,
        page_token: &str,
    ) -> Result<HistoryPage, WalletError> {
        let account = AccountId::parse(account)?;
        let after = (!page_token.is_empty()).then(|| decode_cursor(page_token)).transpose()?;
        let limit = match page_size {
            0 => DEFAULT_PAGE,
            n => n.min(MAX_PAGE),
        };
        // One extra row tells whether another page follows.
        let mut transactions = self.store.history(&account, currency, after, limit + 1).await?;
        let next_page_token = if transactions.len() > limit {
            transactions.truncate(limit);
            transactions.last().map(|t| encode_cursor(TransactionCursor { created_at: t.created_at, id: t.id }))
        } else {
            None
        };
        Ok(HistoryPage { transactions, next_page_token })
    }

    /// The account was deleted: its wallet and history go.
    pub async fn erase(&self, account: &AccountId) -> Result<bool, WalletError> {
        self.store.erase(account).await
    }

    fn view(&self, wallet: Wallet, now: DateTime<Utc>) -> WalletView {
        let claim = wallet.claim_state(&self.config.claim, now);
        WalletView { wallet, claim }
    }
}

/// Microseconds, the precision Postgres keeps: a stored time reads back equal
/// (the history's cursor relies on it).
fn ledger_time(now: DateTime<Utc>) -> DateTime<Utc> {
    now.duration_trunc(TimeDelta::microseconds(1)).unwrap_or(now)
}

/// `"{created_at_micros}_{id}"`.
fn encode_cursor(cursor: TransactionCursor) -> String {
    format!("{}_{}", cursor.created_at.timestamp_micros(), cursor.id)
}

fn decode_cursor(token: &str) -> Result<TransactionCursor, WalletError> {
    let invalid = || WalletError::InvalidPageToken { value: token.to_owned() };
    let (micros, id) = token.split_once('_').ok_or_else(invalid)?;
    let created_at = DateTime::from_timestamp_micros(micros.parse().map_err(|_| invalid())?).ok_or_else(invalid)?;
    Ok(TransactionCursor { created_at, id: Uuid::parse_str(id).map_err(|_| invalid())? })
}

#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::ClaimResult;
    use crate::domain::{ClaimDecision, ClaimPolicy, PackDecision, StakePackPolicy, TransactionKind};

    type Movement = (Currency, i64, i64, TransactionKind, Option<String>);

    /// The ledger in memory, with the Postgres adapter's semantics.
    #[derive(Default)]
    pub struct MemoryStore {
        wallets: Mutex<HashMap<AccountId, Wallet>>,
        /// (transaction, idempotency key)
        ledger:  Mutex<Vec<(Transaction, String)>>,
    }

    impl MemoryStore {
        fn open_locked(&self, account: &AccountId, starter_gems: i64, now: DateTime<Utc>) -> Wallet {
            let mut wallets = self.wallets.lock().unwrap();
            if let Some(wallet) = wallets.get(account) {
                return wallet.clone();
            }
            let wallet = Wallet::opened(*account, starter_gems);
            if starter_gems > 0 {
                let gift = (Currency::Gems, starter_gems, starter_gems, TransactionKind::StarterGift, None);
                self.record(account, gift, "sys:starter-gems", now);
            }
            wallets.insert(*account, wallet.clone());
            wallet
        }

        /// (currency, delta, balance after, kind, ref)
        fn record(&self, account: &AccountId, movement: Movement, key: &str, now: DateTime<Utc>) {
            let (currency, delta, balance_after, kind, ref_id) = movement;
            let t = Transaction { id: Uuid::now_v7(), account: *account, currency, delta, balance_after, kind, ref_id, created_at: now };
            self.ledger.lock().unwrap().push((t, key.to_owned()));
        }

        fn replayed(&self, account: &AccountId, key: &IdempotencyKey) -> Option<i64> {
            self.ledger.lock().unwrap().iter().find(|(t, k)| t.account == *account && k == key.as_str()).map(|(t, _)| t.delta)
        }
    }

    #[async_trait]
    impl WalletStore for MemoryStore {
        async fn open(&self, account: &AccountId, starter_gems: i64, now: DateTime<Utc>) -> Result<Wallet, WalletError> {
            Ok(self.open_locked(account, starter_gems, now))
        }

        async fn claim(
            &self,
            account: &AccountId,
            key: &IdempotencyKey,
            policy: &ClaimPolicy,
            starter_gems: i64,
            now: DateTime<Utc>,
        ) -> Result<ClaimResult, WalletError> {
            let mut wallet = self.open_locked(account, starter_gems, now);
            if let Some(delta) = self.replayed(account, key) {
                return Ok(ClaimResult { outcome: ClaimOutcome::Claimed, awarded: delta as i32, wallet });
            }
            let (outcome, awarded) = match wallet.decide_claim(policy, now) {
                ClaimDecision::Claim { awarded, streak } => {
                    wallet.apply_claim(awarded, streak, now);
                    let movement = (Currency::Points, i64::from(awarded), wallet.points, TransactionKind::Claim, None);
                    self.record(account, movement, key.as_str(), now);
                    self.wallets.lock().unwrap().insert(*account, wallet.clone());
                    (ClaimOutcome::Claimed, awarded)
                }
                ClaimDecision::TooEarly { .. } => (ClaimOutcome::TooEarly, 0),
                ClaimDecision::DailyCapReached { .. } => (ClaimOutcome::DailyCapReached, 0),
            };
            Ok(ClaimResult { outcome, awarded, wallet })
        }

        async fn buy_stake_pack(
            &self,
            account: &AccountId,
            key: &IdempotencyKey,
            policy: &StakePackPolicy,
            starter_gems: i64,
            now: DateTime<Utc>,
        ) -> Result<(PackOutcome, Wallet), WalletError> {
            let mut wallet = self.open_locked(account, starter_gems, now);
            if self.replayed(account, key).is_some() {
                return Ok((PackOutcome::Bought, wallet));
            }
            let outcome = match wallet.decide_stake_pack(policy) {
                PackDecision::Buy => {
                    wallet.apply_stake_pack(policy);
                    let movement = (Currency::Gems, -policy.price_gems, wallet.gems, TransactionKind::StakePack, None);
                    self.record(account, movement, key.as_str(), now);
                    self.wallets.lock().unwrap().insert(*account, wallet.clone());
                    PackOutcome::Bought
                }
                PackDecision::StillActive => PackOutcome::StillActive,
                PackDecision::InsufficientGems => PackOutcome::InsufficientGems,
            };
            Ok((outcome, wallet))
        }

        async fn spend_gems(
            &self,
            account: &AccountId,
            key: &IdempotencyKey,
            spend: &GemSpend,
            starter_gems: i64,
            now: DateTime<Utc>,
        ) -> Result<(SpendOutcome, Wallet), WalletError> {
            let mut wallet = self.open_locked(account, starter_gems, now);
            if self.replayed(account, key).is_some() {
                return Ok((SpendOutcome::Spent, wallet));
            }
            if !wallet.can_spend_gems(spend.amount) {
                return Ok((SpendOutcome::InsufficientGems, wallet));
            }
            wallet.spend_gems(spend.amount);
            let movement = (Currency::Gems, -spend.amount, wallet.gems, spend.kind, spend.ref_id.clone());
            self.record(account, movement, key.as_str(), now);
            self.wallets.lock().unwrap().insert(*account, wallet.clone());
            Ok((SpendOutcome::Spent, wallet))
        }

        async fn history(
            &self,
            account: &AccountId,
            currency: Option<Currency>,
            after: Option<TransactionCursor>,
            limit: usize,
        ) -> Result<Vec<Transaction>, WalletError> {
            let mut rows: Vec<Transaction> = self
                .ledger
                .lock()
                .unwrap()
                .iter()
                .map(|(t, _)| t.clone())
                .filter(|t| t.account == *account && currency.is_none_or(|c| c == t.currency))
                .filter(|t| after.is_none_or(|a| (t.created_at, t.id) < (a.created_at, a.id)))
                .collect();
            rows.sort_by_key(|t| std::cmp::Reverse((t.created_at, t.id)));
            rows.truncate(limit);
            Ok(rows)
        }

        async fn erase(&self, account: &AccountId) -> Result<bool, WalletError> {
            self.ledger.lock().unwrap().retain(|(t, _)| t.account != *account);
            Ok(self.wallets.lock().unwrap().remove(account).is_some())
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::fakes::MemoryStore;
    use super::*;
    use crate::domain::TransactionKind;

    fn at(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, day, hour, 0, 0).unwrap()
    }

    fn wallets() -> Wallets {
        Wallets::new(Arc::new(MemoryStore::default()), WalletConfig::default())
    }

    fn key() -> String {
        Uuid::now_v7().to_string()
    }

    #[tokio::test]
    async fn a_wallet_opens_with_the_starter_gems_once() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        let first = w.get(&account, at(8, 10)).await.unwrap();
        assert_eq!((first.wallet.points, first.wallet.gems), (0, 100));
        assert!(first.claim.available);
        w.get(&account, at(8, 11)).await.unwrap();
        let history = w.history(&account, None, 0, "").await.unwrap();
        assert_eq!(history.transactions.len(), 1, "the gift is given once");
        assert_eq!(history.transactions[0].kind, TransactionKind::StarterGift);
    }

    #[tokio::test]
    async fn a_claim_credits_once_per_key_and_the_hour_holds() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        let k = key();
        let claimed = w.claim(&account, &k, at(8, 10)).await.unwrap();
        assert_eq!((claimed.outcome, claimed.awarded, claimed.view.wallet.points), (ClaimOutcome::Claimed, 25, 25));
        let replay = w.claim(&account, &k, at(8, 10)).await.unwrap();
        assert_eq!((replay.outcome, replay.awarded, replay.view.wallet.points), (ClaimOutcome::Claimed, 25, 25), "credited once");
        let early = w.claim(&account, &key(), at(8, 10)).await.unwrap();
        assert_eq!((early.outcome, early.awarded), (ClaimOutcome::TooEarly, 0));
        assert_eq!(early.view.claim.next_claim_at, Some(at(8, 11)));
        assert!(w.claim(&account, "bad", at(8, 12)).await.is_err(), "WAL-9002");
    }

    #[tokio::test]
    async fn the_history_pages_newest_first_and_filters_by_currency() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        for hour in 0..5 {
            w.claim(&account, &key(), at(8, hour)).await.unwrap();
        }
        let first = w.history(&account, None, 4, "").await.unwrap();
        assert_eq!(first.transactions.len(), 4);
        assert!(first.transactions.windows(2).all(|p| p[0].created_at >= p[1].created_at));
        let rest = w.history(&account, None, 4, first.next_page_token.as_deref().unwrap()).await.unwrap();
        assert_eq!(rest.transactions.len(), 2, "1 claim + the starter gift");
        assert!(rest.next_page_token.is_none());
        let gems = w.history(&account, Some(Currency::Gems), 0, "").await.unwrap();
        assert_eq!(gems.transactions.len(), 1);
        assert!(w.history(&account, None, 0, "nope").await.is_err(), "WAL-9003");
    }

    #[tokio::test]
    async fn a_pack_is_bought_once_per_key_never_stacks_and_minors_are_refused() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        assert!(matches!(
            w.buy_stake_pack(&account, &key(), true, at(8, 10)).await,
            Err(WalletError::GemSpendingRestricted)
        ));
        let k = key();
        let bought = w.buy_stake_pack(&account, &k, false, at(8, 10)).await.unwrap();
        assert_eq!((bought.outcome, bought.view.wallet.gems, bought.view.wallet.stake_shots), (PackOutcome::Bought, 50, 3));
        let replay = w.buy_stake_pack(&account, &k, false, at(8, 10)).await.unwrap();
        assert_eq!((replay.outcome, replay.view.wallet.gems), (PackOutcome::Bought, 50), "charged once");
        let again = w.buy_stake_pack(&account, &key(), false, at(8, 10)).await.unwrap();
        assert_eq!((again.outcome, again.view.wallet.gems), (PackOutcome::StillActive, 50));
    }

    /// A claim's key cannot pass as a paid pack: keys are scoped per operation.
    #[tokio::test]
    async fn a_key_used_for_a_claim_does_not_buy_a_pack_for_free() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        let k = key();
        w.claim(&account, &k, at(8, 10)).await.unwrap();
        let pack = w.buy_stake_pack(&account, &k, false, at(8, 10)).await.unwrap();
        assert_eq!((pack.outcome, pack.view.wallet.gems), (PackOutcome::Bought, 50), "actually charged");
    }

    #[tokio::test]
    async fn another_service_spends_gems_once_per_key_within_the_balance() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        let (spent, left) = w.spend_gems(&account, "country-FR", 30, TransactionKind::CountryUnlock, "FR", at(8, 10)).await.unwrap();
        assert_eq!((spent, left), (SpendOutcome::Spent, 70));
        let replay = w.spend_gems(&account, "country-FR", 30, TransactionKind::CountryUnlock, "FR", at(8, 10)).await.unwrap();
        assert_eq!(replay, (SpendOutcome::Spent, 70), "charged once");
        let short = w.spend_gems(&account, "country-US", 80, TransactionKind::CountryUnlock, "US", at(8, 10)).await.unwrap();
        assert_eq!(short, (SpendOutcome::InsufficientGems, 70));
        assert!(w.spend_gems(&account, "country-JP", 0, TransactionKind::CountryUnlock, "JP", at(8, 10)).await.is_err());
        assert!(w.spend_gems(&account, "country-JP", 5, TransactionKind::Claim, "JP", at(8, 10)).await.is_err());
        let history = w.history(&account, Some(Currency::Gems), 0, "").await.unwrap();
        assert_eq!(history.transactions[0].ref_id.as_deref(), Some("FR"));
    }

    #[tokio::test]
    async fn erasing_drops_the_wallet_and_its_history() {
        let w = wallets();
        let account = Uuid::now_v7().to_string();
        w.claim(&account, &key(), at(8, 10)).await.unwrap();
        assert!(w.erase(&AccountId::parse(&account).unwrap()).await.unwrap());
        assert!(!w.erase(&AccountId::parse(&account).unwrap()).await.unwrap());
    }
}

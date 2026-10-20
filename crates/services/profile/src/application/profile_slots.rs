//! Profile slots (#916): an account holds one profile free; more are bought
//! with gems, at most [`SlotPolicy::max_slots`], each slot at twice the
//! previous one's price. A slot is bought once and kept: deleting a profile
//! frees its slot, creating one later reuses it, and the price depends on the
//! slots owned, never on the profiles alive. An account that held more
//! profiles before slots existed keeps them (its slots are its profiles,
//! recorded the first time they are counted).
//!
//! A purchase is keyed by the slot it buys (`profile-slot-<n>`): the wallet
//! charges slot `n` at most once, whatever the retries or the races, and the
//! slot count grows `n - 1 → n` with an LWT.

use std::sync::Arc;

use async_trait::async_trait;

use crate::application::port::{ProfileRepository, ProfileSlotStore, SlotWallet};
use crate::domain::value_object::AccountId;
use crate::error::ProfileError;

/// Slot prices and the cap (server policy, echoed to the app).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotPolicy {
    /// Profiles an account may hold at most (bought slots included).
    pub max_slots:  i32,
    /// The 2nd slot's price in gems; each later one costs twice the previous.
    pub base_price: i64,
}

impl Default for SlotPolicy {
    /// 100 / 200 / 400 / 800 gems for the 2nd to the 5th.
    fn default() -> Self {
        Self { max_slots: 5, base_price: 100 }
    }
}

impl SlotPolicy {
    /// What the next slot costs for an account owning `owned`; `None` at the
    /// cap.
    pub fn next_price(&self, owned: i32) -> Option<i64> {
        (owned >= 1 && owned < self.max_slots).then(|| self.base_price << (owned - 1).min(62))
    }
}

/// An account's slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotState {
    pub slots:      i32,
    /// Profiles alive.
    pub used:       i32,
    pub max_slots:  i32,
    /// `None` at the cap.
    pub next_price: Option<i64>,
}

/// What became of a slot purchase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOutcome {
    Bought,
    /// The account holds the most slots already: nothing charged.
    MaxReached,
    InsufficientGems,
    /// The price moved since the app showed it: nothing charged.
    PriceChanged,
}

/// A slot purchase's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotReply {
    pub outcome: SlotOutcome,
    pub state:   SlotState,
    /// Gems left, when the wallet was asked.
    pub gems:    Option<i64>,
    /// The slot's price (the next one's when refused).
    pub price:   i64,
}

/// How many profiles an account holds alive.
#[async_trait]
pub trait AccountProfiles: Send + Sync + 'static {
    async fn count(&self, account: &AccountId) -> Result<i32, ProfileError>;
}

/// [`AccountProfiles`] from the repository's account index (deleted
/// profiles leave it).
pub struct IndexedProfiles(pub Arc<dyn ProfileRepository>);

/// Profiles counted per page.
const PAGE: i32 = 100;

#[async_trait]
impl AccountProfiles for IndexedProfiles {
    async fn count(&self, account: &AccountId) -> Result<i32, ProfileError> {
        let (mut used, mut token) = (0usize, None::<String>);
        loop {
            let (page, next) = self.0.list_by_account(account, PAGE, token.as_deref()).await?;
            used += page.len();
            match next {
                Some(next) if !next.is_empty() => token = Some(next),
                _ => break,
            }
        }
        Ok(i32::try_from(used).unwrap_or(i32::MAX))
    }
}

pub struct ProfileSlots {
    pub store:    Arc<dyn ProfileSlotStore>,
    pub profiles: Arc<dyn AccountProfiles>,
    /// `None`: slots cannot be bought (`WalletUnavailable`); creating within
    /// the slots owned still works.
    pub wallet: Option<Arc<dyn SlotWallet>>,
    pub policy: SlotPolicy,
}

impl ProfileSlots {
    /// The account's slots: its row, else one — or, for an account holding
    /// more profiles than that, its profiles, recorded for good.
    pub async fn state(&self, account: &AccountId) -> Result<SlotState, ProfileError> {
        let used = self.profiles.count(account).await?;
        let slots = match self.store.get(account).await? {
            Some(slots) => slots,
            None if used > 1 => self.store.init(account, used).await?,
            None => 1,
        };
        Ok(SlotState { slots, used, max_slots: self.policy.max_slots, next_price: self.policy.next_price(slots) })
    }

    /// `NoFreeProfileSlot` when every slot of the account is in use.
    pub async fn check_create(&self, account: &AccountId) -> Result<(), ProfileError> {
        let state = self.state(account).await?;
        if state.used >= state.slots { Err(ProfileError::NoFreeProfileSlot) } else { Ok(()) }
    }

    /// Buys the account's next slot at `expected_price`. `adult`: the buyer
    /// may spend gems (the edge token), else `GemSpendingRestricted`;
    /// `token`: forwarded to the wallet.
    pub async fn buy(
        &self,
        account: &AccountId,
        expected_price: i64,
        adult: bool,
        token: Option<&str>,
    ) -> Result<SlotReply, ProfileError> {
        if !adult {
            return Err(ProfileError::GemSpendingRestricted);
        }
        let state = self.state(account).await?;
        let Some(price) = state.next_price else {
            return Ok(SlotReply { outcome: SlotOutcome::MaxReached, state, gems: None, price: 0 });
        };
        if price != expected_price {
            return Ok(SlotReply { outcome: SlotOutcome::PriceChanged, state, gems: None, price });
        }
        let wallet = self.wallet.as_ref().ok_or_else(|| ProfileError::WalletUnavailable { reason: "not configured".into() })?;
        let slot = state.slots + 1;
        let spend = wallet.spend_for_slot(account, slot, price, &format!("profile-slot-{slot}"), token).await?;
        if !spend.spent {
            return Ok(SlotReply { outcome: SlotOutcome::InsufficientGems, state, gems: Some(spend.gems), price });
        }
        // Applied or not (a raced purchase of the same slot, charged once by
        // the wallet's key), the account holds at least `slot` now.
        let slots = self.store.grow(account, state.slots, slot).await?;
        let state = SlotState { slots, next_price: self.policy.next_price(slots), ..state };
        Ok(SlotReply { outcome: SlotOutcome::Bought, state, gems: Some(spend.gems), price })
    }
}

#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::SlotSpend;

    #[derive(Default)]
    pub struct MemSlots(pub Mutex<HashMap<AccountId, i32>>);

    #[async_trait]
    impl ProfileSlotStore for MemSlots {
        async fn get(&self, account: &AccountId) -> Result<Option<i32>, ProfileError> {
            Ok(self.0.lock().unwrap().get(account).copied())
        }
        async fn init(&self, account: &AccountId, slots: i32) -> Result<i32, ProfileError> {
            Ok(*self.0.lock().unwrap().entry(*account).or_insert(slots))
        }
        async fn grow(&self, account: &AccountId, from: i32, to: i32) -> Result<i32, ProfileError> {
            let mut map = self.0.lock().unwrap();
            let slots = map.entry(*account).or_insert(1);
            if *slots == from {
                *slots = to;
            }
            Ok(*slots)
        }
        async fn erase(&self, account: &AccountId) -> Result<(), ProfileError> {
            self.0.lock().unwrap().remove(account);
            Ok(())
        }
    }

    /// Gems per account (100 by default), spends keyed once.
    #[derive(Default)]
    pub struct MemSlotWallet {
        pub gems:  Mutex<HashMap<AccountId, i64>>,
        pub spent: Mutex<Vec<(AccountId, String, i64)>>,
    }

    #[async_trait]
    impl SlotWallet for MemSlotWallet {
        async fn spend_for_slot(
            &self,
            account: &AccountId,
            _: i32,
            amount: i64,
            key: &str,
            _: Option<&str>,
        ) -> Result<SlotSpend, ProfileError> {
            let mut gems = self.gems.lock().unwrap();
            let balance = gems.entry(*account).or_insert(100);
            let mut spent = self.spent.lock().unwrap();
            if spent.iter().any(|(a, k, _)| a == account && k == key) {
                return Ok(SlotSpend { spent: true, gems: *balance });
            }
            if *balance < amount {
                return Ok(SlotSpend { spent: false, gems: *balance });
            }
            *balance -= amount;
            spent.push((*account, key.to_owned(), amount));
            Ok(SlotSpend { spent: true, gems: *balance })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    /// Profiles alive per account.
    #[derive(Default)]
    struct MemProfiles(std::sync::Mutex<std::collections::HashMap<AccountId, i32>>);

    #[async_trait]
    impl AccountProfiles for MemProfiles {
        async fn count(&self, account: &AccountId) -> Result<i32, ProfileError> {
            Ok(*self.0.lock().unwrap().get(account).unwrap_or(&0))
        }
    }

    struct World {
        slots:    ProfileSlots,
        profiles: Arc<MemProfiles>,
        store:    Arc<MemSlots>,
        wallet:   Arc<MemSlotWallet>,
    }

    fn world() -> World {
        let (profiles, store, wallet) =
            (Arc::new(MemProfiles::default()), Arc::new(MemSlots::default()), Arc::new(MemSlotWallet::default()));
        let slots = ProfileSlots {
            store:    Arc::clone(&store) as _,
            profiles: Arc::clone(&profiles) as _,
            wallet:   Some(Arc::clone(&wallet) as _),
            policy:   SlotPolicy::default(),
        };
        World { slots, profiles, store, wallet }
    }

    fn account() -> AccountId {
        AccountId::from_uuid(uuid::Uuid::now_v7())
    }

    #[tokio::test]
    async fn one_slot_is_free_and_a_bought_one_is_kept_and_charged_once() {
        let w = world();
        let me = account();
        assert!(w.slots.check_create(&me).await.is_ok(), "the first profile");
        w.profiles.0.lock().unwrap().insert(me, 1);
        assert!(matches!(w.slots.check_create(&me).await, Err(ProfileError::NoFreeProfileSlot)));

        let moved = w.slots.buy(&me, 50, true, None).await.unwrap();
        assert_eq!((moved.outcome, moved.price), (SlotOutcome::PriceChanged, 100));
        let bought = w.slots.buy(&me, 100, true, None).await.unwrap();
        assert_eq!((bought.outcome, bought.state.slots, bought.state.next_price, bought.gems), (SlotOutcome::Bought, 2, Some(200), Some(0)));
        assert!(w.slots.check_create(&me).await.is_ok(), "the bought slot");

        // A crash after the spend: the retry pays the same slot once.
        *w.store.0.lock().unwrap().get_mut(&me).unwrap() = 1;
        let retry = w.slots.buy(&me, 100, true, None).await.unwrap();
        assert_eq!((retry.outcome, retry.state.slots), (SlotOutcome::Bought, 2));
        assert_eq!(w.wallet.spent.lock().unwrap().len(), 1, "charged once");

        let short = w.slots.buy(&me, 200, true, None).await.unwrap();
        assert_eq!((short.outcome, short.gems), (SlotOutcome::InsufficientGems, Some(0)));
        assert_eq!(w.store.get(&me).await.unwrap(), Some(2), "nothing granted unpaid");
    }

    #[tokio::test]
    async fn the_cap_minors_and_older_accounts() {
        let w = world();
        let me = account();
        w.store.0.lock().unwrap().insert(me, 5);
        let capped = w.slots.buy(&me, 1_600, true, None).await.unwrap();
        assert_eq!(capped.outcome, SlotOutcome::MaxReached);
        assert!(matches!(w.slots.buy(&me, 100, false, None).await, Err(ProfileError::GemSpendingRestricted)));

        // Three profiles from before slots: grandfathered, recorded for good.
        let older = account();
        w.profiles.0.lock().unwrap().insert(older, 3);
        let state = w.slots.state(&older).await.unwrap();
        assert_eq!((state.slots, state.used, state.next_price), (3, 3, Some(400)));
        w.profiles.0.lock().unwrap().insert(older, 2);
        assert_eq!(w.slots.state(&older).await.unwrap().slots, 3, "kept after a delete");

        let unconfigured = ProfileSlots { wallet: None, ..world().slots };
        let lone = account();
        assert!(matches!(unconfigured.buy(&lone, 100, true, None).await, Err(ProfileError::WalletUnavailable { .. })));
    }

    #[test]
    fn slots_double_in_price_up_to_the_cap() {
        let policy = SlotPolicy::default();
        let prices: Vec<_> = (1..=5).map(|owned| policy.next_price(owned)).collect();
        assert_eq!(prices, [Some(100), Some(200), Some(400), Some(800), None]);
        assert_eq!(policy.next_price(7), None, "grandfathered past the cap");
    }
}

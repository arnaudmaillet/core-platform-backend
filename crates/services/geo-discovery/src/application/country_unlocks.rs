//! A member's countries on the map (#665): the free home country, and those
//! unlocked with gems at the price their rank sets.
//!
//! The home country is the account's country of residence, recorded once
//! for good when first seen (never the network's: a VPN must not pick a free
//! country). An unlock checks the caller may spend gems (an adult: the edge
//! token) and the price the app showed, records that **agreed price as
//! pending**, asks the wallet for the gems (keyed per country: charged at
//! most once, whatever the retries), then records the unlock. A retry after a
//! failure in between finds the pending price: it pays that price (the
//! wallet replays the key), never another one, and is never refused for a
//! price that moved meanwhile. A pending price is kept 24 h.
//!
//! A **24 h visa** (#910) is a temporary unlock, keyed per purchase by the
//! client: the agreed price and the visa's new end (24 h past the running
//! visa's end, or past now) are recorded under the key before the gems are
//! spent, so a retry pays once and grants the same end.

use std::sync::Arc;

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use uuid::Uuid;

use crate::application::country_standings::CountryStandings;
use crate::application::port::{AccountCountries, CountrySpend, CountryUnlockStore, GemWallet, ResidenceDirectory, VisaPurchase};
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// What became of an unlock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnlockOutcome {
    Unlocked,
    /// The home country, or unlocked before: nothing charged.
    AlreadyUnlocked,
    InsufficientGems,
    /// The price moved since the app showed it: nothing charged.
    PriceChanged,
}

/// How long a visa runs (#910).
pub const VISA_HOURS: i64 = 24;

/// What became of a visa purchase (#910).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisaOutcome {
    Granted,
    /// The home country, or unlocked for good: nothing charged.
    AlreadyUnlocked,
    InsufficientGems,
    /// The price moved since the app showed it: nothing charged.
    PriceChanged,
}

/// A visa purchase's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisaReply {
    pub outcome:   VisaOutcome,
    pub countries: AccountCountries,
    pub gems:      i64,
    /// The visa's price (the agreed one for a retried purchase).
    pub price:     i64,
    /// `Granted`: when the visa ends.
    pub until:     Option<DateTime<Utc>>,
}

/// Who asks for an unlock: whether they may spend gems (an adult, from the
/// edge token), and that token, forwarded to the wallet so it checks the
/// spender itself (#852).
#[derive(Debug, Clone, Default)]
pub struct Spender {
    pub adult: bool,
    pub token: Option<String>,
}

/// An unlock's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlockReply {
    pub outcome:   UnlockOutcome,
    pub countries: AccountCountries,
    pub gems:      i64,
    /// The country's price (the agreed one for a pending purchase).
    pub price:     i64,
}

pub struct CountryUnlocking {
    pub store:     Arc<dyn CountryUnlockStore>,
    pub standings: Arc<CountryStandings>,
    pub wallet:    Arc<dyn GemWallet>,
    pub residence: Arc<dyn ResidenceDirectory>,
    pub atlas:     &'static CountryAtlas,
    /// The member map filter (`GEO_COUNTRY_UNLOCKS_ENABLED`).
    pub filtering: bool,
}

impl CountryUnlocking {
    /// The member's countries, the home country recorded from the account's
    /// residence the first time it is known.
    pub async fn countries(&self, account: Uuid) -> Result<AccountCountries, GeoDiscoveryError> {
        let mut countries = self.store.get(account).await?;
        if countries.home.is_none()
            && let Some(country) = self.residence.residence(account).await?.filter(|c| self.atlas.knows(*c))
        {
            countries.home = Some(self.store.set_home_once(account, country).await?);
        }
        Ok(countries)
    }

    /// The member's countries and gems (the shop's one read).
    pub async fn view(&self, account: Uuid) -> Result<(AccountCountries, i64), GeoDiscoveryError> {
        let countries = self.countries(account).await?;
        let gems = self.wallet.gems(account).await?;
        Ok((countries, gems))
    }

    /// Unlocks `country` at `expected_price` for `spender` — who may spend
    /// gems (from the edge token) — else `GEO-3001`.
    pub async fn unlock(
        &self,
        account: Uuid,
        country: &str,
        expected_price: i64,
        spender: &Spender,
        now: DateTime<Utc>,
    ) -> Result<UnlockReply, GeoDiscoveryError> {
        if !spender.adult {
            return Err(GeoDiscoveryError::GemSpendingRestricted);
        }
        let country = CountryCode::try_from(country.trim())?;
        if !self.atlas.knows(country) {
            return Err(GeoDiscoveryError::InvalidCountryCode(country.to_string()));
        }
        let mut countries = self.countries(account).await?;
        if countries.has(country) {
            let gems = self.wallet.gems(account).await?;
            return Ok(UnlockReply { outcome: UnlockOutcome::AlreadyUnlocked, countries, gems, price: 0 });
        }
        // A purchase left half-way: its agreed price, no new price check.
        let price = match countries.pending.iter().find(|(c, _)| *c == country) {
            Some((_, agreed)) => *agreed,
            None => {
                let price = self.price(country, now).await?;
                if price != expected_price {
                    let gems = self.wallet.gems(account).await?;
                    return Ok(UnlockReply { outcome: UnlockOutcome::PriceChanged, countries, gems, price });
                }
                self.store.mark_pending(account, country, price).await?;
                price
            }
        };
        let spend = self
            .wallet
            .spend_for_country(account, CountrySpend::Unlock, country, price, &format!("country-{country}"), spender.token.as_deref())
            .await?;
        countries.pending.retain(|(c, _)| *c != country);
        if !spend.spent {
            self.store.clear_pending(account, country).await?;
            return Ok(UnlockReply { outcome: UnlockOutcome::InsufficientGems, countries, gems: spend.gems, price });
        }
        self.store.add(account, country, price, now).await?;
        countries.unlocked.push(country);
        // Unlocked for good: a running visa to it ends (the store dropped it).
        countries.visas.retain(|(c, _)| *c != country);
        Ok(UnlockReply { outcome: UnlockOutcome::Unlocked, countries, gems: spend.gems, price })
    }

    /// Buys a 24 h visa to `country` at `expected_price` under `key` (#910),
    /// for `spender` — who may spend gems — else `GEO-3001`.
    pub async fn buy_visa(
        &self,
        account: Uuid,
        country: &str,
        expected_price: i64,
        key: &str,
        spender: &Spender,
        now: DateTime<Utc>,
    ) -> Result<VisaReply, GeoDiscoveryError> {
        if !spender.adult {
            return Err(GeoDiscoveryError::GemSpendingRestricted);
        }
        let key_ok = (8..=40).contains(&key.len()) && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if !key_ok {
            return Err(GeoDiscoveryError::InvalidIdempotencyKey);
        }
        let country = CountryCode::try_from(country.trim())?;
        if !self.atlas.knows(country) {
            return Err(GeoDiscoveryError::InvalidCountryCode(country.to_string()));
        }
        let mut countries = self.countries(account).await?;
        let reply = |outcome, countries: AccountCountries, gems, price, until| VisaReply { outcome, countries, gems, price, until };
        let purchase = match self.store.visa_purchase(account, key).await? {
            // A retry: what the key agreed, whatever the request says now.
            Some(purchase) if purchase.settled => {
                let gems = self.wallet.gems(account).await?;
                return Ok(reply(VisaOutcome::Granted, countries, gems, purchase.price, Some(purchase.until)));
            }
            Some(purchase) => purchase,
            None => {
                if countries.has(country) {
                    let gems = self.wallet.gems(account).await?;
                    return Ok(reply(VisaOutcome::AlreadyUnlocked, countries, gems, 0, None));
                }
                let price = self.visa_price(country, now).await?;
                if price != expected_price {
                    let gems = self.wallet.gems(account).await?;
                    return Ok(reply(VisaOutcome::PriceChanged, countries, gems, price, None));
                }
                let from = countries.visa_until(country, now).unwrap_or(now);
                // Milliseconds, what the store keeps: the end read back is the end granted.
                let until = from + TimeDelta::hours(VISA_HOURS);
                let until = until.duration_trunc(TimeDelta::milliseconds(1)).unwrap_or(until);
                let purchase = VisaPurchase { country, price, until, settled: false };
                self.store.record_visa_purchase(account, key, purchase).await?;
                purchase
            }
        };
        let spend_key = format!("visa-{}-{key}", purchase.country);
        let spend = self
            .wallet
            .spend_for_country(account, CountrySpend::Visa, purchase.country, purchase.price, &spend_key, spender.token.as_deref())
            .await?;
        if !spend.spent {
            self.store.clear_visa_purchase(account, key).await?;
            return Ok(reply(VisaOutcome::InsufficientGems, countries, spend.gems, purchase.price, None));
        }
        self.store.grant_visa(account, purchase.country, purchase.until).await?;
        self.store.record_visa_purchase(account, key, VisaPurchase { settled: true, ..purchase }).await?;
        countries.visas.retain(|(c, until)| *c != purchase.country || *until > purchase.until);
        if countries.visas.iter().all(|(c, _)| *c != purchase.country) {
            countries.visas.push((purchase.country, purchase.until));
        }
        Ok(reply(VisaOutcome::Granted, countries, spend.gems, purchase.price, Some(purchase.until)))
    }

    /// A 24 h visa's price now, from the country's rank (#910).
    pub async fn visa_price(&self, country: CountryCode, now: DateTime<Utc>) -> Result<i64, GeoDiscoveryError> {
        let ladder = self.standings.ladder(now).await?;
        let rank = ladder.standings.iter().find(|s| s.country == country).map_or(u32::MAX, |s| s.rank);
        Ok(self.standings.pricing.visa_price(rank))
    }

    /// A country's price now, from its rank.
    async fn price(&self, country: CountryCode, now: DateTime<Utc>) -> Result<i64, GeoDiscoveryError> {
        let ladder = self.standings.ladder(now).await?;
        let rank = ladder.standings.iter().find(|s| s.country == country).map_or(u32::MAX, |s| s.rank);
        Ok(self.standings.pricing.price(rank, false))
    }
}

#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::GemSpend;

    /// `fail_next_add`: the next `add` fails (a crash after the spend);
    /// `.2`: visa purchases by (account, key); `.3`: the next `grant_visa`
    /// fails.
    #[derive(Default)]
    pub struct MemUnlocks(
        pub Mutex<HashMap<Uuid, AccountCountries>>,
        pub Mutex<bool>,
        pub Mutex<HashMap<(Uuid, String), VisaPurchase>>,
        pub Mutex<bool>,
    );

    #[async_trait]
    impl CountryUnlockStore for MemUnlocks {
        async fn get(&self, account: Uuid) -> Result<AccountCountries, GeoDiscoveryError> {
            Ok(self.0.lock().unwrap().get(&account).cloned().unwrap_or_default())
        }
        async fn set_home_once(&self, account: Uuid, country: CountryCode) -> Result<CountryCode, GeoDiscoveryError> {
            let mut map = self.0.lock().unwrap();
            let entry = map.entry(account).or_default();
            Ok(*entry.home.get_or_insert(country))
        }
        async fn mark_pending(&self, account: Uuid, country: CountryCode, price: i64) -> Result<(), GeoDiscoveryError> {
            let mut map = self.0.lock().unwrap();
            let entry = map.entry(account).or_default();
            entry.pending.retain(|(c, _)| *c != country);
            entry.pending.push((country, price));
            Ok(())
        }
        async fn clear_pending(&self, account: Uuid, country: CountryCode) -> Result<(), GeoDiscoveryError> {
            if let Some(entry) = self.0.lock().unwrap().get_mut(&account) {
                entry.pending.retain(|(c, _)| *c != country);
            }
            Ok(())
        }
        async fn add(&self, account: Uuid, country: CountryCode, _: i64, _: DateTime<Utc>) -> Result<(), GeoDiscoveryError> {
            if std::mem::take(&mut *self.1.lock().unwrap()) {
                return Err(GeoDiscoveryError::DomainViolation { field: "store".into(), message: "down".into() });
            }
            let mut map = self.0.lock().unwrap();
            let entry = map.entry(account).or_default();
            entry.pending.retain(|(c, _)| *c != country);
            entry.visas.retain(|(c, _)| *c != country);
            if !entry.unlocked.contains(&country) {
                entry.unlocked.push(country);
            }
            Ok(())
        }
        async fn visa_purchase(&self, account: Uuid, key: &str) -> Result<Option<VisaPurchase>, GeoDiscoveryError> {
            Ok(self.2.lock().unwrap().get(&(account, key.to_owned())).copied())
        }
        async fn record_visa_purchase(&self, account: Uuid, key: &str, purchase: VisaPurchase) -> Result<(), GeoDiscoveryError> {
            self.2.lock().unwrap().insert((account, key.to_owned()), purchase);
            Ok(())
        }
        async fn clear_visa_purchase(&self, account: Uuid, key: &str) -> Result<(), GeoDiscoveryError> {
            self.2.lock().unwrap().remove(&(account, key.to_owned()));
            Ok(())
        }
        async fn grant_visa(&self, account: Uuid, country: CountryCode, until: DateTime<Utc>) -> Result<(), GeoDiscoveryError> {
            if std::mem::take(&mut *self.3.lock().unwrap()) {
                return Err(GeoDiscoveryError::DomainViolation { field: "store".into(), message: "down".into() });
            }
            let mut map = self.0.lock().unwrap();
            let visas = &mut map.entry(account).or_default().visas;
            match visas.iter_mut().find(|(c, _)| *c == country) {
                Some((_, end)) => *end = (*end).max(until),
                None => visas.push((country, until)),
            }
            Ok(())
        }
    }

    /// A wallet: gems per account, spends keyed once.
    #[derive(Default)]
    pub struct MemWallet {
        pub gems:  Mutex<HashMap<Uuid, i64>>,
        pub spent: Mutex<Vec<(Uuid, String, i64)>>,
    }

    #[async_trait]
    impl GemWallet for MemWallet {
        async fn gems(&self, account: Uuid) -> Result<i64, GeoDiscoveryError> {
            Ok(*self.gems.lock().unwrap().get(&account).unwrap_or(&100))
        }
        async fn spend_for_country(
            &self,
            account: Uuid,
            _: CountrySpend,
            _: CountryCode,
            amount: i64,
            key: &str,
            _: Option<&str>,
        ) -> Result<GemSpend, GeoDiscoveryError> {
            let mut gems = self.gems.lock().unwrap();
            let balance = gems.entry(account).or_insert(100);
            let mut spent = self.spent.lock().unwrap();
            if spent.iter().any(|(a, k, _)| *a == account && k == key) {
                return Ok(GemSpend { spent: true, gems: *balance });
            }
            if *balance < amount {
                return Ok(GemSpend { spent: false, gems: *balance });
            }
            *balance -= amount;
            spent.push((account, key.to_owned(), amount));
            Ok(GemSpend { spent: true, gems: *balance })
        }
    }

    /// Residences by account.
    #[derive(Default)]
    pub struct MemResidence(pub Mutex<HashMap<Uuid, CountryCode>>);

    #[async_trait]
    impl ResidenceDirectory for MemResidence {
        async fn residence(&self, account: Uuid) -> Result<Option<CountryCode>, GeoDiscoveryError> {
            Ok(self.0.lock().unwrap().get(&account).copied())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::fakes::*;
    use super::*;

    fn adult() -> Spender {
        Spender { adult: true, token: Some("edge-token".into()) }
    }
    use crate::application::country_standings::tests::MemActivity;
    use crate::application::port::CountryActivityStore;
    use crate::domain::country_standing::UnlockPricing;

    fn cc(code: &str) -> CountryCode {
        CountryCode::try_from(code).unwrap()
    }

    struct World {
        unlocking: CountryUnlocking,
        store:     Arc<MemUnlocks>,
        wallet:    Arc<MemWallet>,
        residence: Arc<MemResidence>,
        activity:  Arc<MemActivity>,
    }

    fn world() -> World {
        let (store, wallet, residence, activity) = (
            Arc::new(MemUnlocks::default()),
            Arc::new(MemWallet::default()),
            Arc::new(MemResidence::default()),
            Arc::new(MemActivity::default()),
        );
        let standings = Arc::new(CountryStandings::new(
            Arc::clone(&activity) as _,
            CountryAtlas::embedded(),
            UnlockPricing::default(),
            30,
            Duration::ZERO,
        ));
        let unlocking = CountryUnlocking {
            store: Arc::clone(&store) as _,
            standings,
            wallet: Arc::clone(&wallet) as _,
            residence: Arc::clone(&residence) as _,
            atlas: CountryAtlas::embedded(),
            filtering: true,
        };
        World { unlocking, store, wallet, residence, activity }
    }

    #[tokio::test]
    async fn the_home_country_is_the_residence_only_and_stays() {
        let w = world();
        let me = Uuid::now_v7();
        assert_eq!(w.unlocking.countries(me).await.unwrap().home, None, "no residence, no free country");
        w.residence.0.lock().unwrap().insert(me, cc("FR"));
        assert_eq!(w.unlocking.countries(me).await.unwrap().home, Some(cc("FR")));
        // Recorded for good: a later residence does not move it.
        w.residence.0.lock().unwrap().insert(me, cc("IT"));
        assert_eq!(w.unlocking.countries(me).await.unwrap().home, Some(cc("FR")));
    }

    #[tokio::test]
    async fn an_unlock_is_charged_once_at_the_shown_price() {
        let w = world();
        let me = Uuid::now_v7();
        w.residence.0.lock().unwrap().insert(me, cc("FR"));
        let now = Utc::now();
        // A quiet country ranks past 30: 15 gems.
        let refused = w.unlocking.unlock(me, "IS", 50, &adult(), now).await.unwrap();
        assert_eq!((refused.outcome, refused.price), (UnlockOutcome::PriceChanged, 15));
        assert!(refused.countries.pending.is_empty(), "nothing pending on a refused price");
        let unlocked = w.unlocking.unlock(me, "IS", 15, &adult(), now).await.unwrap();
        assert_eq!((unlocked.outcome, unlocked.gems), (UnlockOutcome::Unlocked, 85));
        assert!(unlocked.countries.has(cc("IS")) && unlocked.countries.has(cc("FR")));
        let again = w.unlocking.unlock(me, "IS", 15, &adult(), now).await.unwrap();
        assert_eq!((again.outcome, again.gems), (UnlockOutcome::AlreadyUnlocked, 85));
        let home = w.unlocking.unlock(me, "FR", 0, &adult(), now).await.unwrap();
        assert_eq!(home.outcome, UnlockOutcome::AlreadyUnlocked, "home is free");
        assert_eq!(w.wallet.spent.lock().unwrap().len(), 1);
    }

    /// Review of #856: the gems are taken, then recording the unlock fails.
    /// The retry pays the agreed price once (the wallet replays the key) and
    /// is not refused for a price that moved meanwhile.
    #[tokio::test]
    async fn a_failure_after_the_spend_is_settled_at_the_agreed_price() {
        let w = world();
        let me = Uuid::now_v7();
        *w.store.1.lock().unwrap() = true;
        assert!(w.unlocking.unlock(me, "JP", 15, &adult(), Utc::now()).await.is_err());
        assert_eq!(w.store.get(me).await.unwrap().pending, vec![(cc("JP"), 15)]);
        // Japan climbs to the top tier meanwhile.
        w.activity.add(cc("JP"), Utc::now().date_naive(), 99, 1, "jp").await.unwrap();
        let settled = w.unlocking.unlock(me, "JP", 50, &adult(), Utc::now()).await.unwrap();
        assert_eq!((settled.outcome, settled.price, settled.gems), (UnlockOutcome::Unlocked, 15, 85));
        assert_eq!(w.wallet.spent.lock().unwrap().len(), 1, "charged once");
        assert!(w.store.get(me).await.unwrap().pending.is_empty());
    }

    #[tokio::test]
    async fn a_busy_country_costs_more_and_gems_must_suffice() {
        let w = world();
        let me = Uuid::now_v7();
        w.activity.add(cc("JP"), Utc::now().date_naive(), 10, 1, "jp").await.unwrap();
        w.wallet.gems.lock().unwrap().insert(me, 40);
        let short = w.unlocking.unlock(me, "JP", 50, &adult(), Utc::now()).await.unwrap();
        assert_eq!((short.outcome, short.price, short.gems), (UnlockOutcome::InsufficientGems, 50, 40));
        assert!(!short.countries.has(cc("JP")));
        assert!(w.store.get(me).await.unwrap().pending.is_empty(), "an unpaid purchase is dropped");
    }

    /// #910: a visa costs 20 % of the unlock (a quiet country: 3), runs 24 h,
    /// and a second one extends it from its end; a retry under a key charges
    /// once and grants the same end; home and unlocked countries need none.
    #[tokio::test]
    async fn a_visa_runs_a_day_and_another_extends_it_once_per_key() {
        let w = world();
        let me = Uuid::now_v7();
        w.residence.0.lock().unwrap().insert(me, cc("FR"));
        let now = Utc::now().duration_trunc(TimeDelta::milliseconds(1)).unwrap();
        let day = TimeDelta::hours(VISA_HOURS);

        let moved = w.unlocking.buy_visa(me, "IS", 15, "visa-key-1", &adult(), now).await.unwrap();
        assert_eq!((moved.outcome, moved.price), (VisaOutcome::PriceChanged, 3));
        let first = w.unlocking.buy_visa(me, "IS", 3, "visa-key-1", &adult(), now).await.unwrap();
        assert_eq!((first.outcome, first.gems, first.until), (VisaOutcome::Granted, 97, Some(now + day)));
        assert_eq!(first.countries.on_map(now), vec![cc("FR"), cc("IS")]);
        let retry = w.unlocking.buy_visa(me, "IS", 3, "visa-key-1", &adult(), now + TimeDelta::hours(1)).await.unwrap();
        assert_eq!((retry.outcome, retry.gems, retry.until), (VisaOutcome::Granted, 97, Some(now + day)), "charged once");

        let later = now + TimeDelta::hours(5);
        let second = w.unlocking.buy_visa(me, "IS", 3, "visa-key-2", &adult(), later).await.unwrap();
        assert_eq!((second.outcome, second.gems, second.until), (VisaOutcome::Granted, 94, Some(now + day + day)), "from its end");
        let countries = w.unlocking.countries(me).await.unwrap();
        assert_eq!(countries.visa_until(cc("IS"), later), Some(now + day + day));
        assert_eq!(countries.on_map(now + day + day), vec![cc("FR")], "the server stops it at its end");

        let home = w.unlocking.buy_visa(me, "FR", 0, "visa-key-3", &adult(), now).await.unwrap();
        assert_eq!(home.outcome, VisaOutcome::AlreadyUnlocked);
        assert_eq!(w.wallet.spent.lock().unwrap().len(), 2);
    }

    /// Unlocking a country for good ends its visa (no refund).
    #[tokio::test]
    async fn an_unlock_ends_the_visa_and_then_no_visa_is_sold() {
        let w = world();
        let me = Uuid::now_v7();
        let now = Utc::now();
        w.unlocking.buy_visa(me, "IS", 3, "visa-key-1", &adult(), now).await.unwrap();
        let unlocked = w.unlocking.unlock(me, "IS", 15, &adult(), now).await.unwrap();
        assert_eq!(unlocked.outcome, UnlockOutcome::Unlocked);
        assert!(unlocked.countries.visas.is_empty());
        assert!(w.unlocking.countries(me).await.unwrap().visas.is_empty(), "the store dropped it");
        let again = w.unlocking.buy_visa(me, "IS", 3, "visa-key-2", &adult(), now).await.unwrap();
        assert_eq!(again.outcome, VisaOutcome::AlreadyUnlocked);
    }

    /// The gems are taken, then granting the visa fails: the retry under the
    /// same key grants the agreed end once, at the agreed price.
    #[tokio::test]
    async fn a_failure_after_the_visas_spend_is_settled_by_the_retry() {
        let w = world();
        let me = Uuid::now_v7();
        let now = Utc::now().duration_trunc(TimeDelta::milliseconds(1)).unwrap();
        *w.store.3.lock().unwrap() = true;
        assert!(w.unlocking.buy_visa(me, "JP", 3, "visa-key-1", &adult(), now).await.is_err());
        w.activity.add(cc("JP"), now.date_naive(), 99, 1, "jp").await.unwrap();
        let settled = w.unlocking.buy_visa(me, "JP", 10, "visa-key-1", &adult(), now + TimeDelta::hours(2)).await.unwrap();
        assert_eq!((settled.outcome, settled.price, settled.gems), (VisaOutcome::Granted, 3, 97), "the agreed price");
        assert_eq!(settled.until, Some(now + TimeDelta::hours(VISA_HOURS)), "the agreed end");
        assert_eq!(w.wallet.spent.lock().unwrap().len(), 1, "charged once");
    }

    #[tokio::test]
    async fn an_unpaid_visa_is_dropped_and_bad_requests_are_refused() {
        let w = world();
        let me = Uuid::now_v7();
        w.wallet.gems.lock().unwrap().insert(me, 2);
        let short = w.unlocking.buy_visa(me, "IS", 3, "visa-key-1", &adult(), Utc::now()).await.unwrap();
        assert_eq!((short.outcome, short.gems), (VisaOutcome::InsufficientGems, 2));
        assert!(w.store.visa_purchase(me, "visa-key-1").await.unwrap().is_none(), "nothing agreed is kept");
        assert!(matches!(
            w.unlocking.buy_visa(me, "IS", 3, "visa-key-1", &Spender::default(), Utc::now()).await,
            Err(GeoDiscoveryError::GemSpendingRestricted)
        ));
        assert!(matches!(
            w.unlocking.buy_visa(me, "IS", 3, "short", &adult(), Utc::now()).await,
            Err(GeoDiscoveryError::InvalidIdempotencyKey)
        ));
        assert!(w.unlocking.buy_visa(me, "ZZ", 3, "visa-key-1", &adult(), Utc::now()).await.is_err());
    }

    #[tokio::test]
    async fn minors_and_unknown_codes_are_refused() {
        let w = world();
        assert!(matches!(
            w.unlocking.unlock(Uuid::now_v7(), "IS", 15, &Spender::default(), Utc::now()).await,
            Err(GeoDiscoveryError::GemSpendingRestricted)
        ));
        assert!(w.unlocking.unlock(Uuid::now_v7(), "ZZ", 15, &adult(), Utc::now()).await.is_err());
        assert!(w.wallet.spent.lock().unwrap().is_empty());
    }
}

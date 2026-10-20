//! The economy's knobs, from the environment; the defaults are the product's
//! rules (#665).

use chrono::TimeDelta;

use crate::domain::{
    Catalog, CatalogItem, ClaimPolicy, ItemFamily, EnvelopePolicy, SettlementPolicy, StakePackOffer, StakePackPolicy, StakePolicy, DEFAULT_CONVICTION_BPS,
};

/// Gems every wallet opens with.
pub const DEFAULT_STARTER_GEMS: i64 = 100;

/// Likes a guest's device gets when it becomes a member (#827, #943): a
/// flat amount, however long it was a guest.
pub const DEFAULT_WELCOME_GIFT: i64 = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletConfig {
    pub claim:        ClaimPolicy,
    pub starter_gems: i64,
    pub stake_pack:   StakePackPolicy,
    /// Likes (#665 part 3).
    pub stakes:       StakePolicy,
    /// Stake settlement (#665 part 4, shadow mode).
    pub settlement:   SettlementPolicy,
    /// The daily curator envelope (shadow mode: provisional gems).
    pub envelope:     EnvelopePolicy,
    /// The probability each conviction level C1–C4 stands for (#888), in
    /// basis points, ascending.
    pub conviction_bps: [i64; 4],
    /// The likes a guest's device gets when it becomes a member (#827, #943).
    pub welcome_gift: i64,
    /// The gem catalogue (#892): what gems buy, and at what price.
    pub catalog:      Catalog,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            claim:        ClaimPolicy::default(),
            starter_gems: DEFAULT_STARTER_GEMS,
            stake_pack:   StakePackPolicy::default(),
            stakes:       StakePolicy::default(),
            settlement:   SettlementPolicy::default(),
            envelope:     EnvelopePolicy::default(),
            conviction_bps: DEFAULT_CONVICTION_BPS,
            welcome_gift: DEFAULT_WELCOME_GIFT,
            catalog:      Catalog::default(),
        }
    }
}

impl WalletConfig {
    /// `WALLET_CLAIM_INTERVAL_SECS` (3600), `WALLET_CLAIM_BASE_POINTS` (25),
    /// `WALLET_DAILY_CLAIM_CAP` (200), `WALLET_STARTER_GEMS` (100),
    /// `WALLET_POINTS_CAP` (2000, the balance a claim never takes past; 0 keeps
    /// the default), `WALLET_STAKE_PACKS` (`3:50,10:150,25:325`: each
    /// pack's `shots:gems`, the default pack first, distinct shots; a bad
    /// list keeps the default),
    /// `WALLET_POINTS_PER_SHOT` (100), `WALLET_STAKE_TARGET_CAP` (250),
    /// `WALLET_STAKE_HOURLY_CAP` (1000), `WALLET_STAKE_MAX_BATCH_AGE_SECS`
    /// (86400), `WALLET_SETTLEMENT_DELAY_SECS` (86400; a position's full
    /// commitment is the per-target cap), `WALLET_CURATOR_ENVELOPE_DAILY`
    /// (1000 gems), `WALLET_CURATOR_DAILY_CAP` (40 gems per account),
    /// `WALLET_ENVELOPE_TARGET_CAP_BPS` (200 = 2 % of the pool per target),
    /// `WALLET_OUTCOME_TOP_PERCENT` (15), `WALLET_CATALOG`
    /// (`theme.pink:0,theme.violet:50,theme.red:50,frame.gold:60,frame.neon:60,frame.aurora:60,banner.animated:500,card.paper:80,card.night:80,card.mint:80`:
    /// each item's `id:gems`, a
    /// bad list keeps the default), `WALLET_CONVICTION_PROBABILITIES`
    /// (`0.25,0.40,0.60,0.80`: four ascending probabilities in (0, 1), C1
    /// first), `WALLET_WELCOME_GIFT` (50). An unparsable or negative value keeps
    /// the default.
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let number = |name: &str| get(name).and_then(|v| v.trim().parse::<i64>().ok()).filter(|v| *v >= 0);
        let defaults = Self::default();
        let per_target_cap = number("WALLET_STAKE_TARGET_CAP").unwrap_or(defaults.stakes.per_target_cap);
        Self {
            claim: ClaimPolicy {
                interval:    number("WALLET_CLAIM_INTERVAL_SECS").map(TimeDelta::seconds).unwrap_or(defaults.claim.interval),
                base_points: number("WALLET_CLAIM_BASE_POINTS")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.claim.base_points),
                daily_cap:   number("WALLET_DAILY_CLAIM_CAP")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.claim.daily_cap),
                points_cap:  number("WALLET_POINTS_CAP").filter(|v| *v > 0).unwrap_or(defaults.claim.points_cap),
            },
            starter_gems: number("WALLET_STARTER_GEMS").unwrap_or(defaults.starter_gems),
            stake_pack: StakePackPolicy {
                offers:          get("WALLET_STAKE_PACKS")
                    .and_then(|v| stake_pack_offers(&v))
                    .unwrap_or_else(|| defaults.stake_pack.offers.clone()),
                points_per_shot: number("WALLET_POINTS_PER_SHOT")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.stake_pack.points_per_shot),
            },
            stakes: StakePolicy {
                per_target_cap,
                hourly_cap:     number("WALLET_STAKE_HOURLY_CAP").unwrap_or(defaults.stakes.hourly_cap),
                max_batch_age:  number("WALLET_STAKE_MAX_BATCH_AGE_SECS")
                    .map(TimeDelta::seconds)
                    .unwrap_or(defaults.stakes.max_batch_age),
                max_clock_skew: defaults.stakes.max_clock_skew,
            },
            settlement: SettlementPolicy {
                delay:         number("WALLET_SETTLEMENT_DELAY_SECS")
                    .map(TimeDelta::seconds)
                    .unwrap_or(defaults.settlement.delay),
                full_position: per_target_cap,
            },
            envelope: EnvelopePolicy {
                daily_pool:         number("WALLET_CURATOR_ENVELOPE_DAILY").unwrap_or(defaults.envelope.daily_pool),
                per_account_cap:    number("WALLET_CURATOR_DAILY_CAP").unwrap_or(defaults.envelope.per_account_cap),
                per_target_cap_bps: number("WALLET_ENVELOPE_TARGET_CAP_BPS")
                    .filter(|v| *v <= 10_000)
                    .unwrap_or(defaults.envelope.per_target_cap_bps),
                top_percent:        number("WALLET_OUTCOME_TOP_PERCENT")
                    .filter(|v| (1..=100).contains(v))
                    .unwrap_or(defaults.envelope.top_percent),
            },
            conviction_bps: get("WALLET_CONVICTION_PROBABILITIES")
                .and_then(|v| conviction_bps(&v))
                .unwrap_or(defaults.conviction_bps),
            welcome_gift: number("WALLET_WELCOME_GIFT").unwrap_or(defaults.welcome_gift),
            catalog: get("WALLET_CATALOG").and_then(|v| catalog(&v)).unwrap_or_else(|| defaults.catalog.clone()),
        }
    }
}

/// Four ascending probabilities in (0, 1), comma-separated, as basis points.
fn conviction_bps(value: &str) -> Option<[i64; 4]> {
    let bps: Vec<i64> = value
        .split(',')
        .map(|p| p.trim().parse::<f64>().ok().filter(|p| *p > 0.0 && *p < 1.0).map(|p| (p * 10_000.0).round() as i64))
        .collect::<Option<_>>()?;
    let bps: [i64; 4] = bps.try_into().ok()?;
    bps.windows(2).all(|w| w[0] < w[1]).then_some(bps)
}

/// `<item id>:<gems>,…` (#892): at least one, each id `<family>.<name>` of a
/// known family, distinct, gems ≥ 0 (0 = free, owned by every account).
fn catalog(value: &str) -> Option<Catalog> {
    let items: Vec<CatalogItem> = value
        .split(',')
        .map(|item| {
            let (id, price) = item.split_once(':')?;
            let id = id.trim();
            let family = ItemFamily::of(id).filter(|f| f.sold())?;
            let price_gems = price.trim().parse::<i64>().ok().filter(|p| *p >= 0)?;
            Some(CatalogItem { id: id.to_owned(), family, price_gems })
        })
        .collect::<Option<_>>()?;
    let distinct = items.iter().enumerate().all(|(i, a)| items[..i].iter().all(|b| b.id != a.id));
    (!items.is_empty() && distinct).then_some(Catalog { items })
}

/// `shots:gems,…`: at least one, shots > 0, gems ≥ 0, no two with the same
/// shots (an offer's id is its shots).
fn stake_pack_offers(value: &str) -> Option<Vec<StakePackOffer>> {
    let offers: Vec<StakePackOffer> = value
        .split(',')
        .map(|offer| {
            let (shots, price) = offer.split_once(':')?;
            let shots = shots.trim().parse::<i32>().ok().filter(|s| *s > 0)?;
            let price_gems = price.trim().parse::<i64>().ok().filter(|p| *p >= 0)?;
            Some(StakePackOffer { shots, price_gems })
        })
        .collect::<Option<_>>()?;
    let distinct = offers.iter().enumerate().all(|(i, a)| offers[..i].iter().all(|b| b.shots != a.shots));
    (!offers.is_empty() && distinct).then_some(offers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_product_rules_and_env_overrides_them() {
        assert_eq!(WalletConfig::from_lookup(|_| None), WalletConfig::default());
        let config = WalletConfig::from_lookup(|name| match name {
            "WALLET_STARTER_GEMS" => Some("0".into()),
            "WALLET_DAILY_CLAIM_CAP" => Some("-5".into()),
            "WALLET_WELCOME_GIFT" => Some("80".into()),
            _ => None,
        });
        assert_eq!(config.starter_gems, 0);
        assert_eq!(config.welcome_gift, 80);
        assert_eq!(config.claim.daily_cap, 200, "a negative value keeps the default");
        assert_eq!(config.claim.points_cap, 2_000);
        let capped = |v: &str| WalletConfig::from_lookup(|name| (name == "WALLET_POINTS_CAP").then(|| v.into())).claim.points_cap;
        assert_eq!(capped("500"), 500);
        assert_eq!(capped("0"), 2_000, "a zero cap keeps the default");
    }

    #[test]
    fn the_catalogue_is_item_ids_and_gems() {
        let catalog = |v: &str| WalletConfig::from_lookup(|name| (name == "WALLET_CATALOG").then(|| v.into())).catalog;
        let custom = catalog("theme.pink:0, icon.night:40");
        assert_eq!(custom.items.iter().map(|i| (i.id.as_str(), i.family, i.price_gems)).collect::<Vec<_>>(), [
            ("theme.pink", ItemFamily::Theme, 0),
            ("icon.night", ItemFamily::AppIcon, 40)
        ]);
        for bad in ["", "theme.red:5,theme.red:6", "medal.gold:10", "badge.continent_eu:0", "theme.red:-1", "theme.red"] {
            assert_eq!(catalog(bad), Catalog::default(), "{bad:?} keeps the default");
        }
    }

    #[test]
    fn stake_packs_are_shots_and_gems_with_distinct_shots() {
        let offers = |v: &str| WalletConfig::from_lookup(|name| (name == "WALLET_STAKE_PACKS").then(|| v.into())).stake_pack.offers;
        assert_eq!(offers(" 5:60 , 12:100"), [StakePackOffer { shots: 5, price_gems: 60 }, StakePackOffer { shots: 12, price_gems: 100 }]);
        for bad in ["", "3:50,3:40", "0:10", "3:-1", "3", "x:1"] {
            assert_eq!(offers(bad), StakePackPolicy::default().offers, "{bad:?} keeps the default");
        }
    }

    #[test]
    fn conviction_probabilities_are_four_ascending_ones() {
        let bps = |v: &str| WalletConfig::from_lookup(|name| (name == "WALLET_CONVICTION_PROBABILITIES").then(|| v.into())).conviction_bps;
        assert_eq!(bps("0.2, 0.4, 0.6, 0.9"), [2_000, 4_000, 6_000, 9_000]);
        for bad in ["0.4,0.2,0.6,0.8", "0.25,0.4,0.6", "0.25,0.4,0.6,1.0", "x"] {
            assert_eq!(bps(bad), DEFAULT_CONVICTION_BPS, "{bad:?} keeps the default");
        }
    }
}

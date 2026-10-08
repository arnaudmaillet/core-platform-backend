//! The economy's knobs, from the environment; the defaults are the product's
//! rules (#665).

use chrono::TimeDelta;

use crate::domain::{ClaimPolicy, StakePackPolicy, StakePolicy};

/// Gems every wallet opens with.
pub const DEFAULT_STARTER_GEMS: i64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletConfig {
    pub claim:        ClaimPolicy,
    pub starter_gems: i64,
    pub stake_pack:   StakePackPolicy,
    /// Likes (#665 part 3).
    pub stakes:       StakePolicy,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self { claim: ClaimPolicy::default(), starter_gems: DEFAULT_STARTER_GEMS, stake_pack: StakePackPolicy::default(), stakes: StakePolicy::default() }
    }
}

impl WalletConfig {
    /// `WALLET_CLAIM_INTERVAL_SECS` (3600), `WALLET_CLAIM_BASE_POINTS` (25),
    /// `WALLET_DAILY_CLAIM_CAP` (200), `WALLET_STARTER_GEMS` (100),
    /// `WALLET_STAKE_PACK_SHOTS` (3), `WALLET_STAKE_PACK_PRICE` (50 gems),
    /// `WALLET_POINTS_PER_SHOT` (100), `WALLET_STAKE_TARGET_CAP` (250),
    /// `WALLET_STAKE_HOURLY_CAP` (1000), `WALLET_STAKE_MAX_BATCH_AGE_SECS`
    /// (86400). An unparsable or negative value keeps the default.
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let number = |name: &str| get(name).and_then(|v| v.trim().parse::<i64>().ok()).filter(|v| *v >= 0);
        let defaults = Self::default();
        Self {
            claim: ClaimPolicy {
                interval:    number("WALLET_CLAIM_INTERVAL_SECS").map(TimeDelta::seconds).unwrap_or(defaults.claim.interval),
                base_points: number("WALLET_CLAIM_BASE_POINTS")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.claim.base_points),
                daily_cap:   number("WALLET_DAILY_CLAIM_CAP")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.claim.daily_cap),
            },
            starter_gems: number("WALLET_STARTER_GEMS").unwrap_or(defaults.starter_gems),
            stake_pack: StakePackPolicy {
                shots:           number("WALLET_STAKE_PACK_SHOTS")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.stake_pack.shots),
                price_gems:      number("WALLET_STAKE_PACK_PRICE").unwrap_or(defaults.stake_pack.price_gems),
                points_per_shot: number("WALLET_POINTS_PER_SHOT")
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(defaults.stake_pack.points_per_shot),
            },
            stakes: StakePolicy {
                per_target_cap: number("WALLET_STAKE_TARGET_CAP").unwrap_or(defaults.stakes.per_target_cap),
                hourly_cap:     number("WALLET_STAKE_HOURLY_CAP").unwrap_or(defaults.stakes.hourly_cap),
                max_batch_age:  number("WALLET_STAKE_MAX_BATCH_AGE_SECS")
                    .map(TimeDelta::seconds)
                    .unwrap_or(defaults.stakes.max_batch_age),
                max_clock_skew: defaults.stakes.max_clock_skew,
            },
        }
    }
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
            _ => None,
        });
        assert_eq!(config.starter_gems, 0);
        assert_eq!(config.claim.daily_cap, 200, "a negative value keeps the default");
    }
}

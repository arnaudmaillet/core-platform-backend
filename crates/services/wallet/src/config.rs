//! The economy's knobs, from the environment; the defaults are the product's
//! rules (#665).

use chrono::TimeDelta;

use crate::domain::ClaimPolicy;

/// Gems every wallet opens with.
pub const DEFAULT_STARTER_GEMS: i64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletConfig {
    pub claim:        ClaimPolicy,
    pub starter_gems: i64,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self { claim: ClaimPolicy::default(), starter_gems: DEFAULT_STARTER_GEMS }
    }
}

impl WalletConfig {
    /// `WALLET_CLAIM_INTERVAL_SECS` (3600), `WALLET_CLAIM_BASE_POINTS` (25),
    /// `WALLET_DAILY_CLAIM_CAP` (200), `WALLET_STARTER_GEMS` (100). An
    /// unparsable or negative value keeps the default.
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

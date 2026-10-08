//! Likes are points (#665): a batch of taps on a post or a comment stakes
//! that many of the account's points on it — or one shot of the ×100 pack —
//! within the target's room (250 points per account and target), the balance
//! and the hour's room (1,000 points). A plain batch is clamped to what fits;
//! a shot is whole or refused. Final: there is no unlike.

use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::{StakePackPolicy, Wallet};
use crate::error::WalletError;

/// What a stake lands on.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StakeTarget {
    Post(String),
    Comment(String),
}

impl StakeTarget {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Post(_) => "post",
            Self::Comment(_) => "comment",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Post(id) | Self::Comment(id) => id,
        }
    }

    /// `post:<id>` / `comment:<id>`: the ledger's `ref_id`.
    pub fn reference(&self) -> String {
        format!("{}:{}", self.kind(), self.id())
    }

    /// A target named by the app: a non-empty id of at most 64 characters.
    pub fn checked(self) -> Result<Self, WalletError> {
        let id = self.id();
        if id.is_empty() || id.len() > 64 {
            return Err(WalletError::InvalidSpend { reason: "a stake names one post or comment".into() });
        }
        Ok(self)
    }
}

/// What a batch asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StakeAsk {
    /// That many taps (1 point each).
    Points(i64),
    /// One shot of the ×100 pack.
    Shot,
}

/// The stakes' rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StakePolicy {
    /// Points one account may put on one target, ever.
    pub per_target_cap: i64,
    /// Points one account may stake per rolling hour.
    pub hourly_cap:     i64,
    /// How old a batch's first tap may be.
    pub max_batch_age:  TimeDelta,
    /// How far ahead of the server a device's clock may be.
    pub max_clock_skew: TimeDelta,
}

impl Default for StakePolicy {
    fn default() -> Self {
        Self {
            per_target_cap: 250,
            hourly_cap:     1_000,
            max_batch_age:  TimeDelta::hours(24),
            max_clock_skew: TimeDelta::minutes(5),
        }
    }
}

impl StakePolicy {
    /// Whether a batch whose first tap was at `first_tap_at` is too old.
    pub fn expired(&self, first_tap_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<bool, WalletError> {
        if first_tap_at > now + self.max_clock_skew {
            return Err(WalletError::InvalidSpend { reason: "first_tap_at is in the future".into() });
        }
        Ok(now - first_tap_at > self.max_batch_age)
    }
}

/// What a batch would do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StakeDecision {
    /// Stake `points` (a shot when `shot`).
    Stake { points: i64, shot: bool },
    InsufficientBalance,
    RateLimited,
    TargetCapReached,
    NoStakeShots,
    ShotDoesNotFit,
}

impl Wallet {
    /// What a batch would do, given what this account already put on the
    /// target and staked in the last hour.
    pub fn decide_stake(
        &self,
        ask: StakeAsk,
        on_target: i64,
        last_hour: i64,
        policy: &StakePolicy,
        pack: &StakePackPolicy,
    ) -> StakeDecision {
        let target_room = policy.per_target_cap - on_target;
        let hour_room = policy.hourly_cap - last_hour;
        match ask {
            // A shot: checked before the balance, never clamped.
            StakeAsk::Shot => {
                let shot = i64::from(pack.points_per_shot);
                if self.stake_shots <= 0 {
                    StakeDecision::NoStakeShots
                } else if target_room <= 0 {
                    StakeDecision::TargetCapReached
                } else if target_room < shot {
                    StakeDecision::ShotDoesNotFit
                } else if self.points < shot {
                    StakeDecision::InsufficientBalance
                } else if hour_room < shot {
                    StakeDecision::RateLimited
                } else {
                    StakeDecision::Stake { points: shot, shot: true }
                }
            }
            // A plain batch: clamped to what fits.
            StakeAsk::Points(asked) => {
                if target_room <= 0 {
                    StakeDecision::TargetCapReached
                } else if self.points <= 0 {
                    StakeDecision::InsufficientBalance
                } else if hour_room <= 0 {
                    StakeDecision::RateLimited
                } else {
                    StakeDecision::Stake { points: asked.min(target_room).min(self.points).min(hour_room), shot: false }
                }
            }
        }
    }

    /// Records a stake [`Self::decide_stake`] allowed.
    pub fn apply_stake(&mut self, points: i64, shot: bool) {
        self.points -= points;
        self.points_spent += points;
        if shot {
            self.stake_shots -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::domain::AccountId;

    fn wallet(points: i64, shots: i32) -> Wallet {
        let mut w = Wallet::opened(AccountId::from_uuid(Uuid::now_v7()), 0);
        w.points = points;
        w.stake_shots = shots;
        w
    }

    fn decide(w: &Wallet, ask: StakeAsk, on_target: i64, last_hour: i64) -> StakeDecision {
        w.decide_stake(ask, on_target, last_hour, &StakePolicy::default(), &StakePackPolicy::default())
    }

    #[test]
    fn a_batch_is_clamped_to_the_target_the_balance_and_the_hour() {
        let w = wallet(500, 0);
        assert_eq!(decide(&w, StakeAsk::Points(30), 0, 0), StakeDecision::Stake { points: 30, shot: false });
        assert_eq!(decide(&w, StakeAsk::Points(30), 240, 0), StakeDecision::Stake { points: 10, shot: false });
        assert_eq!(decide(&wallet(7, 0), StakeAsk::Points(30), 0, 0), StakeDecision::Stake { points: 7, shot: false });
        assert_eq!(decide(&w, StakeAsk::Points(30), 0, 990), StakeDecision::Stake { points: 10, shot: false });
        assert_eq!(decide(&w, StakeAsk::Points(1), 250, 0), StakeDecision::TargetCapReached);
        assert_eq!(decide(&wallet(0, 0), StakeAsk::Points(1), 0, 0), StakeDecision::InsufficientBalance);
        assert_eq!(decide(&w, StakeAsk::Points(1), 0, 1_000), StakeDecision::RateLimited);
    }

    #[test]
    fn a_shot_is_whole_or_refused() {
        let w = wallet(500, 2);
        assert_eq!(decide(&w, StakeAsk::Shot, 150, 0), StakeDecision::Stake { points: 100, shot: true });
        assert_eq!(decide(&w, StakeAsk::Shot, 151, 0), StakeDecision::ShotDoesNotFit);
        assert_eq!(decide(&w, StakeAsk::Shot, 250, 0), StakeDecision::TargetCapReached);
        assert_eq!(decide(&wallet(500, 0), StakeAsk::Shot, 0, 0), StakeDecision::NoStakeShots);
        assert_eq!(decide(&wallet(99, 1), StakeAsk::Shot, 0, 0), StakeDecision::InsufficientBalance);
        assert_eq!(decide(&w, StakeAsk::Shot, 0, 901), StakeDecision::RateLimited);
        let mut w = w;
        w.apply_stake(100, true);
        assert_eq!((w.points, w.points_spent, w.stake_shots), (400, 100, 1));
    }

    #[test]
    fn a_batch_older_than_a_day_is_expired_and_the_future_is_refused() {
        let p = StakePolicy::default();
        let now = Utc::now();
        assert!(!p.expired(now - TimeDelta::hours(23), now).unwrap());
        assert!(p.expired(now - TimeDelta::hours(25), now).unwrap());
        assert!(!p.expired(now + TimeDelta::minutes(4), now).unwrap(), "a little clock skew");
        assert!(p.expired(now + TimeDelta::hours(1), now).is_err());
    }

    #[test]
    fn targets_name_one_thing() {
        assert_eq!(StakeTarget::Post("p1".into()).reference(), "post:p1");
        assert!(StakeTarget::Comment(String::new()).checked().is_err());
    }
}

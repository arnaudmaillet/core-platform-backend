//! An account's wallet and the hourly claim — the same rules as the app's
//! mock (`WalletStore`), the server's clock deciding.

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use uuid::Uuid;

use crate::error::WalletError;

/// The account a wallet belongs to (one wallet per account, all profiles).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountId(Uuid);

impl AccountId {
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse(value: &str) -> Result<Self, WalletError> {
        Uuid::parse_str(value).map(Self).map_err(|_| WalletError::InvalidAccountId { value: value.to_owned() })
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The hourly claim's rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimPolicy {
    /// One claim per interval.
    pub interval:    TimeDelta,
    /// What an un-streaked claim pays.
    pub base_points: i32,
    /// Points claimable per UTC day.
    pub daily_cap:   i32,
}

impl Default for ClaimPolicy {
    fn default() -> Self {
        Self { interval: TimeDelta::hours(1), base_points: 25, daily_cap: 200 }
    }
}

impl ClaimPolicy {
    /// +10 % per consecutive day after the first, up to ×2.
    pub fn multiplier(streak: i32) -> f64 {
        (1.0 + 0.1 * f64::from((streak - 1).max(0))).min(2.0)
    }

    /// What a claim with `streak` pays after `claimed_today`, within the cap.
    fn amount(&self, streak: i32, claimed_today: i32) -> i32 {
        // Rounded half away from zero, like the app.
        let full = (f64::from(self.base_points) * Self::multiplier(streak)).round() as i32;
        full.min(self.daily_cap - claimed_today).max(0)
    }
}

/// The ×100 stake pack's terms: `shots` of `points_per_shot` of the buyer's
/// own points, for `price_gems`. Packs do not stack; shots never expire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StakePackPolicy {
    pub shots:           i32,
    pub price_gems:      i64,
    pub points_per_shot: i32,
}

impl Default for StakePackPolicy {
    fn default() -> Self {
        Self { shots: 3, price_gems: 50, points_per_shot: 100 }
    }
}

/// What buying a stake pack would do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackDecision {
    Buy,
    /// Shots are left: nothing charged.
    StillActive,
    InsufficientGems,
}

/// What a claim would do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimDecision {
    /// Credit `awarded`, recording `streak`.
    Claim { awarded: i32, streak: i32 },
    /// The interval has not passed.
    TooEarly { next_claim_at: DateTime<Utc> },
    /// Today's cap is reached: the next claim opens at 00:00 UTC.
    DailyCapReached { next_claim_at: DateTime<Utc> },
}

/// The claim surface as the app renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimState {
    pub available:     bool,
    pub next_claim_at: Option<DateTime<Utc>>,
    pub claim_amount:  i32,
    pub claimed_today: i32,
    pub daily_cap:     i32,
    /// The live chain; 0 once lapsed.
    pub streak_days:   i32,
}

/// An account's wallet: both balances, the claim's state and the stake pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wallet {
    pub account:       AccountId,
    pub points:        i64,
    pub gems:          i64,
    pub points_earned: i64,
    pub points_spent:  i64,
    pub gems_earned:   i64,
    pub gems_spent:    i64,
    pub last_claim_at: Option<DateTime<Utc>>,
    /// Points claimed on `claimed_day` (a stale day counts as 0).
    pub claimed_today: i32,
    pub claimed_day:   Option<NaiveDate>,
    /// The chain recorded by the last claim.
    pub streak_days:   i32,
    pub stake_shots:   i32,
}

impl Wallet {
    /// A new wallet, with the starter gems.
    pub fn opened(account: AccountId, starter_gems: i64) -> Self {
        Self {
            account,
            points: 0,
            gems: starter_gems,
            points_earned: 0,
            points_spent: 0,
            gems_earned: starter_gems,
            gems_spent: 0,
            last_claim_at: None,
            claimed_today: 0,
            claimed_day: None,
            streak_days: 0,
            stake_shots: 0,
        }
    }

    fn claimed_on(&self, today: NaiveDate) -> i32 {
        if self.claimed_day == Some(today) { self.claimed_today } else { 0 }
    }

    fn last_day(&self) -> Option<NaiveDate> {
        self.last_claim_at.map(|at| at.date_naive())
    }

    /// The chain the next claim records: held within today, continued from
    /// yesterday, restarted at 1 after a missed day.
    fn prospective_streak(&self, today: NaiveDate) -> i32 {
        match self.last_day() {
            Some(day) if day == today => self.streak_days.max(1),
            Some(day) if Some(day) == today.pred_opt() => self.streak_days.max(1) + 1,
            _ => 1,
        }
    }

    /// The chain to show: alive with a claim today or yesterday, else 0.
    fn display_streak(&self, today: NaiveDate) -> i32 {
        match self.last_day() {
            Some(day) if day == today || Some(day) == today.pred_opt() => self.streak_days.max(1),
            _ => 0,
        }
    }

    /// What a claim at `now` would do.
    pub fn decide_claim(&self, policy: &ClaimPolicy, now: DateTime<Utc>) -> ClaimDecision {
        let today = now.date_naive();
        let claimed = self.claimed_on(today);
        if claimed >= policy.daily_cap {
            return ClaimDecision::DailyCapReached { next_claim_at: next_midnight(now) };
        }
        if let Some(opens) = self.last_claim_at.map(|last| last + policy.interval)
            && now < opens
        {
            return ClaimDecision::TooEarly { next_claim_at: opens };
        }
        let streak = self.prospective_streak(today);
        ClaimDecision::Claim { awarded: policy.amount(streak, claimed), streak }
    }

    /// Records a claim [`Self::decide_claim`] allowed.
    pub fn apply_claim(&mut self, awarded: i32, streak: i32, now: DateTime<Utc>) {
        let today = now.date_naive();
        self.claimed_today = self.claimed_on(today) + awarded;
        self.claimed_day = Some(today);
        self.points += i64::from(awarded);
        self.points_earned += i64::from(awarded);
        self.streak_days = streak;
        self.last_claim_at = Some(now);
    }

    /// What buying a stake pack would do.
    pub fn decide_stake_pack(&self, policy: &StakePackPolicy) -> PackDecision {
        if self.stake_shots > 0 {
            PackDecision::StillActive
        } else if self.gems < policy.price_gems {
            PackDecision::InsufficientGems
        } else {
            PackDecision::Buy
        }
    }

    /// Records a pack [`Self::decide_stake_pack`] allowed.
    pub fn apply_stake_pack(&mut self, policy: &StakePackPolicy) {
        self.spend_gems(policy.price_gems);
        self.stake_shots = policy.shots;
    }

    /// Whether `amount` gems can be spent.
    pub fn can_spend_gems(&self, amount: i64) -> bool {
        amount > 0 && self.gems >= amount
    }

    /// Spends gems [`Self::can_spend_gems`] allowed.
    pub fn spend_gems(&mut self, amount: i64) {
        self.gems -= amount;
        self.gems_spent += amount;
    }

    /// The claim surface at `now`.
    pub fn claim_state(&self, policy: &ClaimPolicy, now: DateTime<Utc>) -> ClaimState {
        let today = now.date_naive();
        let claimed = self.claimed_on(today);
        let (available, next_claim_at) = match self.decide_claim(policy, now) {
            ClaimDecision::Claim { .. } => (true, None),
            ClaimDecision::TooEarly { next_claim_at } | ClaimDecision::DailyCapReached { next_claim_at } => {
                (false, Some(next_claim_at))
            }
        };
        ClaimState {
            available,
            next_claim_at,
            claim_amount: policy.amount(self.prospective_streak(today), claimed),
            claimed_today: claimed,
            daily_cap: policy.daily_cap,
            streak_days: self.display_streak(today),
        }
    }
}

/// The next 00:00 UTC.
fn next_midnight(now: DateTime<Utc>) -> DateTime<Utc> {
    now.date_naive().succ_opt().unwrap_or(NaiveDate::MAX).and_time(NaiveTime::MIN).and_utc()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn at(day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0).unwrap()
    }

    fn wallet() -> Wallet {
        Wallet::opened(AccountId::from_uuid(Uuid::now_v7()), 100)
    }

    fn claim(w: &mut Wallet, now: DateTime<Utc>) -> ClaimDecision {
        let decision = w.decide_claim(&ClaimPolicy::default(), now);
        if let ClaimDecision::Claim { awarded, streak } = decision {
            w.apply_claim(awarded, streak, now);
        }
        decision
    }

    #[test]
    fn a_new_wallet_claims_25_then_waits_an_hour() {
        let mut w = wallet();
        assert_eq!(claim(&mut w, at(8, 10, 0)), ClaimDecision::Claim { awarded: 25, streak: 1 });
        assert_eq!(claim(&mut w, at(8, 10, 30)), ClaimDecision::TooEarly { next_claim_at: at(8, 11, 0) });
        assert_eq!(claim(&mut w, at(8, 11, 0)), ClaimDecision::Claim { awarded: 25, streak: 1 });
        assert_eq!((w.points, w.points_earned, w.claimed_today), (50, 50, 50));
        assert_eq!(w.gems, 100, "the starter gems");
    }

    #[test]
    fn the_daily_cap_clamps_the_last_claim_and_reopens_at_midnight() {
        let mut w = wallet();
        for hour in 0..8 {
            assert!(matches!(claim(&mut w, at(8, hour, 0)), ClaimDecision::Claim { .. }));
        }
        assert_eq!(w.claimed_today, 200);
        assert_eq!(claim(&mut w, at(8, 9, 0)), ClaimDecision::DailyCapReached { next_claim_at: at(9, 0, 0) });
        let state = w.claim_state(&ClaimPolicy::default(), at(8, 9, 0));
        assert_eq!((state.available, state.claim_amount, state.next_claim_at), (false, 0, Some(at(9, 0, 0))));
        // Next day: streak 2 pays 27.5 → 28.
        assert_eq!(claim(&mut w, at(9, 0, 0)), ClaimDecision::Claim { awarded: 28, streak: 2 });
    }

    #[test]
    fn the_cap_clamps_a_streaked_claim_to_what_is_left() {
        let mut w = wallet();
        w.claimed_day = Some(at(8, 0, 0).date_naive());
        w.claimed_today = 190;
        assert_eq!(claim(&mut w, at(8, 12, 0)), ClaimDecision::Claim { awarded: 10, streak: 1 });
    }

    #[test]
    fn the_streak_grows_daily_caps_at_double_and_lapses_after_a_missed_day() {
        let mut w = wallet();
        for day in 1..=12 {
            claim(&mut w, at(day, 12, 0));
        }
        assert_eq!(w.streak_days, 12);
        assert_eq!(w.claim_state(&ClaimPolicy::default(), at(13, 12, 0)).claim_amount, 50, "×2 at most");
        assert_eq!(w.claim_state(&ClaimPolicy::default(), at(13, 12, 0)).streak_days, 12, "alive the next day");
        assert_eq!(w.claim_state(&ClaimPolicy::default(), at(14, 12, 0)).streak_days, 0, "lapsed");
        assert_eq!(claim(&mut w, at(14, 12, 0)), ClaimDecision::Claim { awarded: 25, streak: 1 });
    }

    #[test]
    fn the_claim_state_mirrors_the_decision() {
        let mut w = wallet();
        let open = w.claim_state(&ClaimPolicy::default(), at(8, 10, 0));
        assert_eq!((open.available, open.next_claim_at, open.claim_amount, open.streak_days), (true, None, 25, 0));
        claim(&mut w, at(8, 10, 0));
        let waiting = w.claim_state(&ClaimPolicy::default(), at(8, 10, 5));
        assert_eq!((waiting.available, waiting.next_claim_at, waiting.streak_days), (false, Some(at(8, 11, 0)), 1));
    }

    #[test]
    fn a_pack_needs_the_gems_and_an_empty_one() {
        let policy = StakePackPolicy::default();
        let mut w = wallet();
        assert_eq!(w.decide_stake_pack(&policy), PackDecision::Buy);
        w.apply_stake_pack(&policy);
        assert_eq!((w.gems, w.gems_spent, w.stake_shots), (50, 50, 3));
        assert_eq!(w.decide_stake_pack(&policy), PackDecision::StillActive, "packs do not stack");
        w.stake_shots = 0;
        w.gems = 49;
        assert_eq!(w.decide_stake_pack(&policy), PackDecision::InsufficientGems);
        assert!(!w.can_spend_gems(50) && w.can_spend_gems(49) && !w.can_spend_gems(0));
    }

    #[test]
    fn account_ids_are_uuids() {
        assert!(AccountId::parse("not-a-uuid").is_err());
        let id = Uuid::now_v7();
        assert_eq!(AccountId::parse(&id.to_string()).unwrap().as_uuid(), id);
    }
}

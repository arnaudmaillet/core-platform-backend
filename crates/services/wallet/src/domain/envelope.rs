//! The daily curator envelope (#665, economy charter §27–§29), shadow mode.
//! Once a UTC day is over, the positions settled that day share a fixed
//! pool of gems: a position scores only if its target ended the day in the
//! top of the day's targets (the outcome, §9), then in proportion to its
//! pre-score (earliness × weight). Caps keep one target or one account from
//! absorbing the pool (§29); what the caps hold back stays unallocated (the
//! reserve). Shadow mode: the gems are provisional — recorded, never minted.

use std::collections::HashMap;

use crate::domain::{AccountId, StakeTarget};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopePolicy {
    /// Gems the curators share per day.
    pub daily_pool:         i64,
    /// The most one account may receive from one day's pool.
    pub per_account_cap:    i64,
    /// The most one target's positions may receive together, in basis points
    /// of the pool (200 = 2 %).
    pub per_target_cap_bps: i64,
    /// A target's outcome is 1 when it ends the day in this top share of the
    /// day's targets, by like count (percent).
    pub top_percent:        i64,
}

impl Default for EnvelopePolicy {
    fn default() -> Self {
        Self { daily_pool: 1_000, per_account_cap: 40, per_target_cap_bps: 200, top_percent: 15 }
    }
}

/// A position settled during the day, as the envelope reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct DayPosition {
    pub account:             AccountId,
    pub target:              StakeTarget,
    pub count_at_settlement: i64,
    pub pre_score:           Option<f64>,
}

/// What the envelope gives a position.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Allocation {
    /// 1 when its target ended in the day's top, else 0.
    pub outcome:          i16,
    /// outcome × pre-score.
    pub score:            f64,
    /// Its share of the pool, after the caps, in whole gems.
    pub provisional_gems: i64,
}

impl EnvelopePolicy {
    /// The cap on one target's positions together, in gems.
    pub fn per_target_cap(&self) -> f64 {
        (self.daily_pool * self.per_target_cap_bps) as f64 / 10_000.0
    }

    /// Shares the day's pool among `positions`, in order.
    pub fn allocate(&self, positions: &[DayPosition]) -> Vec<Allocation> {
        let top = self.top_targets(positions);
        let scores: Vec<(i16, f64)> = positions
            .iter()
            .map(|p| {
                let outcome = i16::from(top.contains(&p.target.reference()));
                (outcome, f64::from(outcome) * p.pre_score.unwrap_or(0.0).max(0.0))
            })
            .collect();
        let total: f64 = scores.iter().map(|(_, s)| s).sum();
        let mut raw: Vec<f64> = scores
            .iter()
            .map(|(_, s)| if total > 0.0 { self.daily_pool as f64 * s / total } else { 0.0 })
            .collect();
        cap(&mut raw, positions, |p| p.target.reference(), self.per_target_cap());
        cap(&mut raw, positions, |p| p.account.as_uuid().to_string(), self.per_account_cap as f64);
        scores
            .into_iter()
            .zip(raw)
            .map(|((outcome, score), gems)| Allocation { outcome, score, provisional_gems: gems.floor() as i64 })
            .collect()
    }

    /// The day's top targets by like count (each target's highest count
    /// that day): the first `top_percent` of them, rounded up, ties at the
    /// boundary included; a target nobody liked never is.
    fn top_targets(&self, positions: &[DayPosition]) -> std::collections::HashSet<String> {
        let mut counts: HashMap<String, i64> = HashMap::new();
        for p in positions {
            let count = counts.entry(p.target.reference()).or_insert(0);
            *count = (*count).max(p.count_at_settlement);
        }
        let mut ranked: Vec<i64> = counts.values().copied().collect();
        ranked.sort_unstable_by(|a, b| b.cmp(a));
        let k = (ranked.len() * self.top_percent.max(0) as usize).div_ceil(100);
        let Some(&threshold) = ranked.get(k.saturating_sub(1)).filter(|_| k > 0) else { return Default::default() };
        counts.into_iter().filter(|(_, c)| *c >= threshold && *c > 0).map(|(t, _)| t).collect()
    }
}

/// Scales down the groups (by `key`) whose sum exceeds `limit`.
fn cap(raw: &mut [f64], positions: &[DayPosition], key: impl Fn(&DayPosition) -> String, limit: f64) {
    let mut sums: HashMap<String, f64> = HashMap::new();
    for (p, r) in positions.iter().zip(raw.iter()) {
        *sums.entry(key(p)).or_insert(0.0) += r;
    }
    for (p, r) in positions.iter().zip(raw.iter_mut()) {
        let sum = sums[&key(p)];
        if sum > limit && sum > 0.0 {
            *r *= limit / sum;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(n: u8) -> AccountId {
        AccountId::parse(&format!("0199c3a0-0000-7000-8000-0000000000{n:02}")).unwrap()
    }

    fn position(a: u8, target: &str, count: i64, pre_score: Option<f64>) -> DayPosition {
        DayPosition { account: account(a), target: StakeTarget::Post(target.into()), count_at_settlement: count, pre_score }
    }

    #[test]
    fn only_the_days_top_targets_score_and_the_pool_is_shared_by_score() {
        // 20 targets: the top 15 % is 3 (t0, t1, t2).
        let mut positions: Vec<_> = (0..20).map(|i| position(1, &format!("t{i}"), 1_000 - i, Some(0.5))).collect();
        let policy = EnvelopePolicy { per_account_cap: 1_000, per_target_cap_bps: 10_000, ..EnvelopePolicy::default() };
        positions[0].account = account(2);
        positions[1].account = account(3);
        positions[1].pre_score = Some(1.0);
        positions[2].account = account(4);
        positions[2].pre_score = None;
        let got = policy.allocate(&positions);
        assert_eq!(got.iter().filter(|a| a.outcome == 1).count(), 3);
        assert_eq!((got[0].provisional_gems, got[1].provisional_gems, got[2].provisional_gems), (333, 666, 0));
        assert!(got[3..].iter().all(|a| a.outcome == 0 && a.provisional_gems == 0), "popular is not enough: top only");
        assert!(got.iter().map(|a| a.provisional_gems).sum::<i64>() <= 1_000, "never more than the pool");
    }

    #[test]
    fn caps_keep_one_target_or_one_account_from_taking_the_pool() {
        let policy = EnvelopePolicy::default();
        assert_eq!(policy.per_target_cap(), 20.0);
        // One top target, three accounts on it: 20 gems between them.
        let alone = [position(1, "hot", 900, Some(1.0)), position(2, "hot", 900, Some(1.0)), position(3, "hot", 900, Some(2.0))];
        let got = policy.allocate(&alone);
        assert_eq!(got.iter().map(|a| a.provisional_gems).collect::<Vec<_>>(), vec![5, 5, 10]);

        // One account on many top targets: 40 at most.
        let policy = EnvelopePolicy { top_percent: 100, ..EnvelopePolicy::default() };
        let many: Vec<_> = (0..10).map(|i| position(1, &format!("t{i}"), 100, Some(1.0))).collect();
        let total: i64 = policy.allocate(&many).iter().map(|a| a.provisional_gems).sum();
        assert_eq!(total, 40);
    }

    #[test]
    fn an_empty_or_unliked_day_gives_nothing() {
        let policy = EnvelopePolicy::default();
        assert!(policy.allocate(&[]).is_empty());
        let got = policy.allocate(&[position(1, "cold", 0, Some(1.0))]);
        assert_eq!((got[0].outcome, got[0].provisional_gems), (0, 0));
        // A single target with likes is the day's top.
        let got = policy.allocate(&[position(1, "only", 3, Some(0.5))]);
        assert_eq!((got[0].outcome, got[0].provisional_gems), (1, 20), "capped at the target's 2 %");
    }
}

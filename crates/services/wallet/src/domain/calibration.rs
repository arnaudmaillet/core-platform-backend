//! The shadow-mode calibration report (#893): what a day of settlements says
//! about the economy before any gem is minted (Charter V5.3 §40–§41, the
//! Go / No-Go checks). Pure: it reads the day's counted settlements and the
//! envelope's summary, and computes.
//!
//! What it covers today:
//! * **Economy**: how concentrated the pool is (the share of the top 1, top 10
//!   and top 1 % of accounts), how many accounts get anything.
//! * **Curation**: do early stakes carry information (the top rate by
//!   earliness quartile — flat means no), is the stake just a new like (the
//!   stake-size distribution, and the share of one-point positions).
//! * **#868's tie check**: how many targets the day's top holds against the
//!   `top_percent` it was meant to hold.
//!
//! Comments, cohorts and Sybil signals come with their features (#889, #891).

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use serde::Serialize;

use crate::domain::AccountId;

/// One settlement counted in a day's envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct CalibrationRow {
    pub account:          AccountId,
    /// The target's reference (`post:<id>` / `comment:<id>`).
    pub target:           String,
    /// The position's points.
    pub points:           i64,
    /// The share of everyone else's points that came after it (`None`:
    /// its arrival is unknown).
    pub earliness:        Option<f64>,
    /// 1 when its target ended in the day's top.
    pub outcome:          i16,
    pub provisional_gems: i64,
}

/// The envelope's summary for the day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeDay {
    pub pool:      i64,
    pub allocated: i64,
    pub targets:   i64,
}

/// One bucket of positions: how many, and how many of them were on a top
/// target.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Bucket {
    pub label:     &'static str,
    pub positions: usize,
    /// Positions on a top target / positions (0 when empty).
    pub top_rate:  f64,
}

/// A day's calibration (#893).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CalibrationReport {
    pub day:       NaiveDate,
    pub positions: usize,
    pub accounts:  usize,
    pub targets:   usize,

    // ── Economy ──────────────────────────────────────────────────────────
    pub pool:              i64,
    pub allocated:         i64,
    /// Accounts given at least one gem.
    pub rewarded_accounts: usize,
    /// The share of the allocated gems the top account / top 10 / top 1 %
    /// of rewarded accounts got (0 when nothing was allocated).
    pub top1_share:        f64,
    pub top10_share:       f64,
    pub top1pct_share:     f64,

    // ── Curation ─────────────────────────────────────────────────────────
    /// Top rate by earliness quartile, earliest first, then unknown. Early
    /// stakes carry information when the earliest quartile's rate is well
    /// above the latest's.
    pub earliness:          Vec<Bucket>,
    /// Top rate by stake size. Mostly one-point positions: the stake is a
    /// like under another name.
    pub stake_sizes:        Vec<Bucket>,
    pub single_point_share: f64,

    // ── #868: ties at the top threshold ─────────────────────────────────
    /// Targets with outcome 1.
    pub top_targets:          usize,
    /// What `top_percent` of the day's targets is (rounded up).
    pub expected_top_targets: usize,
    /// top_targets / expected_top_targets: ≫ 1 means ties diluted the top
    /// (#868 then needs its tie-break).
    pub top_inflation:        f64,
}

/// The earliness quartiles (a share of later points, 1 = first), earliest
/// first.
const EARLINESS: [&str; 4] = ["0.75–1 (earliest)", "0.5–0.75", "0.25–0.5", "0–0.25 (latest)"];

/// The quartile an earliness falls in (an index into [`EARLINESS`]).
fn quartile(earliness: f64) -> usize {
    match earliness {
        e if e >= 0.75 => 0,
        e if e >= 0.5 => 1,
        e if e >= 0.25 => 2,
        _ => 3,
    }
}

/// Stake sizes, in points (the room is 250).
const SIZES: [(&str, i64, i64); 5] = [("1", 1, 1), ("2–5", 2, 5), ("6–25", 6, 25), ("26–100", 26, 100), ("101–250", 101, i64::MAX)];

impl CalibrationReport {
    /// Computes `day`'s report from its counted settlements and its envelope
    /// (`top_percent`: the envelope policy's).
    pub fn compute(day: NaiveDate, rows: &[CalibrationRow], envelope: Option<EnvelopeDay>, top_percent: i64) -> Self {
        let accounts: HashSet<_> = rows.iter().map(|r| r.account.as_uuid()).collect();
        let targets: HashSet<&str> = rows.iter().map(|r| r.target.as_str()).collect();
        let top: HashSet<&str> = rows.iter().filter(|r| r.outcome == 1).map(|r| r.target.as_str()).collect();

        let mut gems: HashMap<uuid::Uuid, i64> = HashMap::new();
        for r in rows.iter().filter(|r| r.provisional_gems > 0) {
            *gems.entry(r.account.as_uuid()).or_insert(0) += r.provisional_gems;
        }
        let mut ranked: Vec<i64> = gems.values().copied().collect();
        ranked.sort_unstable_by(|a, b| b.cmp(a));
        let allocated: i64 = ranked.iter().sum();
        let share = |n: usize| ratio(ranked.iter().take(n).sum::<i64>() as f64, allocated as f64);

        let bucket = |label, of: Vec<&CalibrationRow>| Bucket {
            label,
            positions: of.len(),
            top_rate: ratio(of.iter().filter(|r| r.outcome == 1).count() as f64, of.len() as f64),
        };
        let mut earliness: Vec<Bucket> = EARLINESS
            .iter()
            .enumerate()
            .map(|(i, &label)| bucket(label, rows.iter().filter(|r| r.earliness.map(quartile) == Some(i)).collect()))
            .collect();
        earliness.push(bucket("unknown", rows.iter().filter(|r| r.earliness.is_none()).collect()));
        let stake_sizes = SIZES
            .iter()
            .map(|&(label, low, high)| bucket(label, rows.iter().filter(|r| r.points >= low && r.points <= high).collect()))
            .collect();

        let expected = (targets.len() * top_percent.max(0) as usize).div_ceil(100);
        Self {
            day,
            positions: rows.len(),
            accounts: accounts.len(),
            targets: targets.len(),
            pool: envelope.map_or(0, |e| e.pool),
            allocated: envelope.map_or(allocated, |e| e.allocated),
            rewarded_accounts: ranked.len(),
            top1_share: share(1),
            top10_share: share(10),
            top1pct_share: share(ranked.len().div_ceil(100).max(1)),
            earliness,
            stake_sizes,
            single_point_share: ratio(rows.iter().filter(|r| r.points == 1).count() as f64, rows.len() as f64),
            top_targets: top.len(),
            expected_top_targets: expected,
            top_inflation: ratio(top.len() as f64, expected as f64),
        }
    }
}

fn ratio(part: f64, whole: f64) -> f64 {
    if whole > 0.0 { part / whole } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(account: u128, target: &str, points: i64, earliness: Option<f64>, outcome: i16, gems: i64) -> CalibrationRow {
        CalibrationRow {
            account: AccountId::from_uuid(uuid::Uuid::from_u128(account)),
            target: target.to_owned(),
            points,
            earliness,
            outcome,
            provisional_gems: gems,
        }
    }

    fn day() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 10, 12).unwrap()
    }

    #[test]
    fn concentration_counts_gems_per_account() {
        let rows = [
            row(1, "post:a", 10, Some(0.9), 1, 60),
            row(1, "post:b", 5, Some(0.8), 1, 20),
            row(2, "post:a", 1, Some(0.1), 1, 20),
            row(3, "post:c", 1, None, 0, 0),
        ];
        let r = CalibrationReport::compute(day(), &rows, Some(EnvelopeDay { pool: 1_000, allocated: 100, targets: 3 }), 15);
        assert_eq!((r.positions, r.accounts, r.targets), (4, 3, 3));
        assert_eq!(r.rewarded_accounts, 2);
        assert!((r.top1_share - 0.8).abs() < 1e-9, "account 1 got 80 of 100");
        assert!((r.top10_share - 1.0).abs() < 1e-9);
        assert!((r.top1pct_share - 0.8).abs() < 1e-9, "the top 1 % of two accounts is one account");
        assert_eq!((r.pool, r.allocated), (1_000, 100));
    }

    #[test]
    fn earliness_and_stake_size_buckets_carry_the_top_rate() {
        let rows = [
            row(1, "post:a", 1, Some(1.0), 1, 0),
            row(2, "post:b", 1, Some(0.8), 0, 0),
            row(3, "post:a", 30, Some(0.1), 1, 0),
            row(4, "post:c", 200, None, 0, 0),
        ];
        let r = CalibrationReport::compute(day(), &rows, None, 15);
        let earliest = &r.earliness[0];
        assert_eq!((earliest.positions, earliest.top_rate), (2, 0.5));
        assert_eq!(r.earliness[3].positions, 1, "0.1 is in the latest quartile");
        assert_eq!(r.earliness[4].positions, 1, "unknown arrival");
        assert_eq!(r.earliness.iter().map(|b| b.positions).sum::<usize>(), 4, "every position in one bucket");
        assert_eq!(r.stake_sizes[0].positions, 2);
        assert_eq!(r.stake_sizes[3].positions, 1);
        assert_eq!(r.stake_sizes[4].positions, 1);
        assert!((r.single_point_share - 0.5).abs() < 1e-9);
    }

    /// #868: ties at the threshold can make the "top 15 %" hold every target.
    #[test]
    fn the_tie_check_compares_the_top_with_its_share() {
        let rows: Vec<_> = (0..20).map(|i| row(i, &format!("post:{i}"), 1, Some(0.5), 1, 0)).collect();
        let r = CalibrationReport::compute(day(), &rows, None, 15);
        assert_eq!((r.top_targets, r.expected_top_targets), (20, 3));
        assert!((r.top_inflation - 20.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn an_empty_day_reports_zeros() {
        let r = CalibrationReport::compute(day(), &[], None, 15);
        assert_eq!((r.positions, r.rewarded_accounts, r.top_targets, r.expected_top_targets), (0, 0, 0, 0));
        assert_eq!((r.top1_share, r.single_point_share, r.top_inflation), (0.0, 0.0, 0.0));
    }
}

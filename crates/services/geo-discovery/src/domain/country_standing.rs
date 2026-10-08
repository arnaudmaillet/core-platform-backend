//! The country ladder (#665): every country ranked by its likes over a
//! rolling window, and what unlocking one costs from its rank.

use std::collections::HashMap;

use crate::domain::value_object::CountryCode;

/// A country's activity over the window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountryActivity {
    /// Likes on posts published there.
    pub likes: i64,
    /// Posts published there.
    pub posts: i64,
}

/// A country's place on the ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountryStanding {
    pub country: CountryCode,
    /// 1 = most liked.
    pub rank:    u32,
    pub likes:   i64,
    pub posts:   i64,
}

/// Ranks every country of `countries` by likes (most first), ties broken by
/// country code so the ladder is stable; a country without activity ranks
/// with zero.
pub fn rank(
    countries: impl IntoIterator<Item = CountryCode>,
    activity: &HashMap<CountryCode, CountryActivity>,
) -> Vec<CountryStanding> {
    let mut ladder: Vec<(CountryCode, CountryActivity)> =
        countries.into_iter().map(|c| (c, activity.get(&c).copied().unwrap_or_default())).collect();
    ladder.sort_by(|(a, x), (b, y)| y.likes.cmp(&x.likes).then_with(|| a.cmp(b)));
    ladder
        .into_iter()
        .enumerate()
        .map(|(i, (country, a))| CountryStanding {
            country,
            rank: u32::try_from(i + 1).unwrap_or(u32::MAX),
            likes: a.likes,
            posts: a.posts,
        })
        .collect()
}

/// What unlocking a country costs, from its rank (gems).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnlockPricing {
    /// Ranks 1..=`top_ranks` cost `top_price`.
    pub top_ranks: u32,
    pub top_price: i64,
    /// Ranks up to `mid_ranks` cost `mid_price`.
    pub mid_ranks: u32,
    pub mid_price: i64,
    /// Every other rank.
    pub rest_price: i64,
}

impl Default for UnlockPricing {
    fn default() -> Self {
        Self { top_ranks: 10, top_price: 50, mid_ranks: 30, mid_price: 30, rest_price: 15 }
    }
}

impl UnlockPricing {
    /// The price of a country at `rank` (the home country is free: the
    /// caller passes `home`).
    pub fn price(&self, rank: u32, home: bool) -> i64 {
        match rank {
            _ if home => 0,
            r if r <= self.top_ranks => self.top_price,
            r if r <= self.mid_ranks => self.mid_price,
            _ => self.rest_price,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cc(code: &str) -> CountryCode {
        CountryCode::try_from(code).unwrap()
    }

    #[test]
    fn the_ladder_ranks_by_likes_then_code_and_keeps_quiet_countries() {
        let activity = HashMap::from([
            (cc("FR"), CountryActivity { likes: 10, posts: 3 }),
            (cc("US"), CountryActivity { likes: 40, posts: 9 }),
            (cc("DE"), CountryActivity { likes: 10, posts: 1 }),
        ]);
        let ladder = rank([cc("FR"), cc("US"), cc("DE"), cc("JP")], &activity);
        let order: Vec<_> = ladder.iter().map(|s| (s.country.as_str().to_owned(), s.rank)).collect();
        assert_eq!(order, vec![("US".into(), 1), ("DE".into(), 2), ("FR".into(), 3), ("JP".into(), 4)]);
        assert_eq!((ladder[3].likes, ladder[3].posts), (0, 0));
    }

    #[test]
    fn prices_follow_the_rank_tiers_and_home_is_free() {
        let p = UnlockPricing::default();
        assert_eq!([p.price(1, false), p.price(10, false), p.price(11, false), p.price(30, false), p.price(31, false)], [50, 50, 30, 30, 15]);
        assert_eq!(p.price(1, true), 0);
    }
}

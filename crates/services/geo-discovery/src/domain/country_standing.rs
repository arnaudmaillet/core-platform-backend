//! The country ladder (#665): every country ranked by its likes over a
//! rolling window, and what unlocking one costs from its rank.

use std::collections::HashMap;

use crate::domain::entity::MapPostCard;
use crate::domain::value_object::{CountryCode, PostFormat};

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

/// The most a text cover's caption shows, in characters (#942).
pub const COVER_CAPTION_CHARS: usize = 200;

/// What a country's Shop cover shows (#942).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverKind {
    Image,
    Video,
    Text,
}

/// A country's Shop card background (#942): one of its posts, with no author.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountryCover {
    pub post_id:    uuid::Uuid,
    pub kind:       CoverKind,
    /// The photo, or the video's poster; empty for a text cover.
    pub poster_url: String,
    /// A text cover's caption, at most [`COVER_CAPTION_CHARS`].
    pub caption:    String,
    /// A video cover's preview loop (#963); empty without one (paint the
    /// poster).
    pub preview_url: String,
}

impl CountryCover {
    /// The cover a card makes: `None` when it has nothing to paint (a media
    /// post without a still, a text post without a caption). A card of
    /// unknown kind is a photo when it has a still, else text.
    pub fn of(card: &MapPostCard) -> Option<Self> {
        let still = !card.thumbnail_url.is_empty();
        let kind = match card.format {
            PostFormat::Photo => CoverKind::Image,
            PostFormat::Video => CoverKind::Video,
            PostFormat::TextOnly => CoverKind::Text,
            PostFormat::Unknown if still => CoverKind::Image,
            PostFormat::Unknown => CoverKind::Text,
        };
        let (poster_url, caption) = match kind {
            CoverKind::Image | CoverKind::Video if still => (card.thumbnail_url.clone(), String::new()),
            CoverKind::Text if !card.caption.trim().is_empty() => {
                (String::new(), card.caption.chars().take(COVER_CAPTION_CHARS).collect())
            }
            _ => return None,
        };
        Some(Self { post_id: card.post_id, kind, poster_url, caption, preview_url: String::new() })
    }
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
    /// A 24 h visa's price (#910), in percent of the country's unlock price,
    /// rounded, at least 1 gem: a visa never beats unlocking a country one
    /// visits often.
    pub visa_percent: i64,
}

impl Default for UnlockPricing {
    fn default() -> Self {
        Self { top_ranks: 10, top_price: 50, mid_ranks: 30, mid_price: 30, rest_price: 15, visa_percent: 20 }
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

    /// The price of a 24 h visa (#910) to a country at `rank`.
    pub fn visa_price(&self, rank: u32) -> i64 {
        ((self.price(rank, false) * self.visa_percent + 50) / 100).max(1)
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
        assert_eq!([p.visa_price(1), p.visa_price(11), p.visa_price(31)], [10, 6, 3], "#910: 20 %");
    }

    fn card(format: PostFormat, thumbnail: &str, caption: &str) -> MapPostCard {
        MapPostCard {
            post_id:           uuid::Uuid::now_v7(),
            author_id:         uuid::Uuid::now_v7(),
            author_handle:     "someone".into(),
            author_avatar_url: "https://cdn/a.jpg".into(),
            thumbnail_url:     thumbnail.into(),
            caption:           caption.into(),
            h3_index_r7:       0,
            virality_score:    0.0,
            published_at_ms:   0,
            author_tier:       0,
            lat:               None,
            lng:               None,
            format,
            cover_asset_id:    None,
        }
    }

    /// #942: a cover paints a still or quotes a caption, and names no author.
    #[test]
    fn a_cover_paints_a_still_or_quotes_a_caption() {
        let photo = CountryCover::of(&card(PostFormat::Photo, "https://cdn/p.jpg", "a caption")).unwrap();
        assert_eq!((photo.kind, photo.poster_url.as_str(), photo.caption.as_str()), (CoverKind::Image, "https://cdn/p.jpg", ""));
        assert_eq!(CountryCover::of(&card(PostFormat::Video, "https://cdn/v.jpg", "")).unwrap().kind, CoverKind::Video);
        assert_eq!(CountryCover::of(&card(PostFormat::Video, "", "")), None, "a video without a poster");

        let long = "é".repeat(250);
        let text = CountryCover::of(&card(PostFormat::TextOnly, "", &long)).unwrap();
        assert_eq!((text.kind, text.poster_url.as_str()), (CoverKind::Text, ""));
        assert_eq!(text.caption.chars().count(), COVER_CAPTION_CHARS);
        assert_eq!(CountryCover::of(&card(PostFormat::TextOnly, "", "  ")), None);

        // Unknown kind (indexed before it was kept): a still makes a photo.
        assert_eq!(CountryCover::of(&card(PostFormat::Unknown, "https://cdn/x.jpg", "")).unwrap().kind, CoverKind::Image);
        assert_eq!(CountryCover::of(&card(PostFormat::Unknown, "", "words")).unwrap().kind, CoverKind::Text);
    }
}

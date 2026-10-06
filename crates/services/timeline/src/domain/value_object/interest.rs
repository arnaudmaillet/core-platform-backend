//! Interest tags (#662): what a profile's reactions say it likes, as the
//! hashtags of the posts it reacted to, weighted and decaying with time.
//!
//! A weight is stored *inflated* to a fixed epoch — a reaction at `t` adds
//! `2^((t - EPOCH) / HALF_LIFE)` — so the store only ever increments and the
//! order between tags is the decayed order; [`decayed`] brings a stored score
//! back to today's weight. With a 30-day half-life the inflation stays far
//! inside an `f64` until the 2100s.

use std::collections::HashMap;

/// Most hashtags read from one caption.
pub const MAX_TAGS_PER_POST: usize = 10;
/// Longest tag kept (in chars); longer ones are noise.
pub const MAX_TAG_CHARS: usize = 50;
/// Most tags kept per profile (the lightest go first).
pub const MAX_INTERESTS: usize = 100;
/// The interests a For You page is ranked against.
pub const BOOST_INTERESTS: usize = 20;
/// A weight halves in this time without a new reaction.
pub const HALF_LIFE_MS: i64 = 30 * 86_400_000;
/// Below this weight a tag is no longer listed nor used.
pub const MIN_WEIGHT: f64 = 0.05;
/// A post counts once per profile in this window (unreact / react again does
/// not inflate its tags).
pub const REACTION_DEDUP_MS: i64 = 30 * 86_400_000;
/// How long a removed tag stays out, and an idle store lives.
pub const INTEREST_TTL_SECS: u64 = 180 * 86_400;

/// 2026-01-01T00:00:00Z, the inflation epoch.
const EPOCH_MS: i64 = 1_767_225_600_000;

/// One interest tag and its weight today.
#[derive(Debug, Clone, PartialEq)]
pub struct Interest {
    pub tag:    String,
    pub weight: f64,
}

/// What one reaction at `at_ms` adds to each tag of the post, inflated.
pub fn inflated_increment(at_ms: i64) -> f64 {
    2f64.powf((at_ms - EPOCH_MS) as f64 / HALF_LIFE_MS as f64)
}

/// A stored (inflated) score brought back to its weight at `now_ms`.
pub fn decayed(score: f64, now_ms: i64) -> f64 {
    score / inflated_increment(now_ms)
}

/// A tag as stored: without its `#`, lowercase, letters and digits only at the
/// ends; `None` when nothing is left or it is too long.
pub fn normalize_tag(raw: &str) -> Option<String> {
    let tag = raw
        .trim()
        .trim_start_matches('#')
        .trim_matches(|c: char| !c.is_alphanumeric())
        .to_lowercase();
    let ok = !tag.is_empty()
        && tag.chars().count() <= MAX_TAG_CHARS
        && tag.chars().all(|c| c.is_alphanumeric() || c == '_');
    ok.then_some(tag)
}

/// The `#hashtags` of a caption, normalized as search does, de-duplicated, at
/// most [`MAX_TAGS_PER_POST`] in caption order.
pub fn hashtags(caption: &str) -> Vec<String> {
    let mut tags: Vec<String> = Vec::new();
    for tag in caption.split_whitespace().filter_map(|t| t.strip_prefix('#')).filter_map(normalize_tag) {
        if !tags.contains(&tag) {
            tags.push(tag);
            if tags.len() == MAX_TAGS_PER_POST {
                break;
            }
        }
    }
    tags
}

/// How much a post's tags match the interests: the sum of their weights.
pub fn affinity(tags: &[String], interests: &HashMap<String, f64>) -> f64 {
    tags.iter().filter_map(|t| interests.get(t)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashtags_are_normalized_deduplicated_and_capped() {
        assert_eq!(hashtags("Loving #Rust and #rust, #Open_Search! #  no_tag #"), vec!["rust", "open_search"]);
        let many: String = (0..20).map(|i| format!("#t{i} ")).collect();
        assert_eq!(hashtags(&many).len(), MAX_TAGS_PER_POST);
        assert!(hashtags(&format!("#{}", "a".repeat(MAX_TAG_CHARS + 1))).is_empty());
        assert!(hashtags("plain caption").is_empty());
    }

    #[test]
    fn a_tag_given_back_by_a_client_normalizes_like_one_read_from_a_caption() {
        assert_eq!(normalize_tag("#Rust"), Some("rust".into()));
        assert_eq!(normalize_tag(" rust "), Some("rust".into()));
        assert_eq!(normalize_tag("two words"), None);
        assert_eq!(normalize_tag("#"), None);
    }

    #[test]
    fn a_weight_halves_every_half_life() {
        let at = EPOCH_MS + 400 * 86_400_000;
        let score = inflated_increment(at);
        assert!((decayed(score, at) - 1.0).abs() < 1e-9);
        assert!((decayed(score, at + HALF_LIFE_MS) - 0.5).abs() < 1e-9);
        // Two reactions a half-life apart: the newer one weighs twice the older.
        assert!((inflated_increment(at + HALF_LIFE_MS) / score - 2.0).abs() < 1e-9);
    }

    #[test]
    fn affinity_sums_the_matching_weights() {
        let interests = HashMap::from([("rust".to_owned(), 2.0), ("go".to_owned(), 0.5)]);
        assert_eq!(affinity(&["rust".into(), "go".into(), "zig".into()], &interests), 2.5);
        assert_eq!(affinity(&["zig".into()], &interests), 0.0);
    }
}

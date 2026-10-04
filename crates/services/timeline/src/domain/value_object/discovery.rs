//! The discovery feed's vocabulary: rankings, content levels, the moderation
//! state a pooled post carries, the hot score and the page cursor.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

use crate::domain::value_object::{AuthorId, PostId};
use crate::error::TimelineError;

/// How a discovery page is ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryRanking {
    /// [`TRENDING_PER_FRESH`] hot items for one fresh item.
    ForYou,
    /// Hot score, posts with some popularity only.
    Trending,
    /// Newest first.
    Recent,
    /// Geotagged near a point, by hot score.
    Nearby,
}

impl DiscoveryRanking {
    fn code(self) -> char {
        match self {
            Self::ForYou   => 'y',
            Self::Trending => 't',
            Self::Recent   => 'r',
            Self::Nearby   => 'n',
        }
    }
}

/// Who reads a discovery page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A trusted mesh caller: no audience filter.
    Internal,
    /// A client: the profiles it owns (empty for a guest or an anonymous caller).
    Profiles(Vec<String>),
}

/// social-graph's answer for one author (`CheckAccess`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentAccess {
    Visible,
    HeaderOnly,
    Hidden,
}

/// For You's mix: this many trending items for each fresh one.
pub const TRENDING_PER_FRESH: usize = 3;

/// What a discovery read may show.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContentLevel {
    /// No age-gated post.
    #[default]
    Restricted,
    /// Age-gated posts included.
    Standard,
}

/// The pool's indices. A post is in `Recent` for as long as it is pooled, and
/// in exactly one of `Fresh` (no popularity yet) or `Hot` (some popularity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiscoveryStream {
    Recent,
    Fresh,
    Hot,
    /// NEARBY's candidate list (computed per request, not stored).
    Nearby,
}

impl DiscoveryStream {
    fn code(self) -> char {
        match self {
            Self::Recent => 'r',
            Self::Fresh  => 'f',
            Self::Hot    => 'h',
            Self::Nearby => 'n',
        }
    }

    fn from_code(c: char) -> Option<Self> {
        Some(match c {
            'r' => Self::Recent,
            'f' => Self::Fresh,
            'h' => Self::Hot,
            'n' => Self::Nearby,
            _ => return None,
        })
    }
}

/// The moderation restriction in force on a post (one at a time per post, as
/// moderation versions a subject).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Restriction {
    #[default]
    None,
    Removed,
    /// Kept out of discovery surfaces.
    Limited,
    /// Shown only to audiences cleared for mature content.
    AgeGated,
}

impl Restriction {
    /// The restriction a moderation content action imposes; actor-level actions
    /// (warn, restrict_actor, suspend, ban) and no_action impose none.
    pub fn for_action(action: &str) -> Option<Self> {
        match action {
            "remove_content"   => Some(Self::Removed),
            "visibility_limit" => Some(Self::Limited),
            "age_gate"         => Some(Self::AgeGated),
            _                  => None,
        }
    }

    /// Whether the post leaves the pool altogether.
    pub fn hides(self) -> bool {
        matches!(self, Self::Removed | Self::Limited)
    }

    pub fn code(self) -> u8 {
        match self {
            Self::None     => 0,
            Self::Removed  => 1,
            Self::Limited  => 2,
            Self::AgeGated => 3,
        }
    }

    /// Unknown codes read as `Removed`: an unreadable state never shows a post.
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Self::None,
            2 => Self::Limited,
            3 => Self::AgeGated,
            _ => Self::Removed,
        }
    }
}

/// What the pool knows about one post.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryMeta {
    pub post_id:         PostId,
    /// `None` when only a moderation decision or a deletion was recorded
    /// (the post's publication has not been seen).
    pub author_id:       Option<AuthorId>,
    pub published_at_ms: Option<i64>,
    pub popularity:      f64,
    pub restriction:     Restriction,
    pub deleted:         bool,
}

impl DiscoveryMeta {
    /// Whether a reader at `level` may be shown this post (before the audience
    /// check). A post whose publication was never seen is not shown.
    pub fn shown_at(&self, level: ContentLevel) -> bool {
        if self.deleted || self.author_id.is_none() || self.published_at_ms.is_none() {
            return false;
        }
        match self.restriction {
            Restriction::None => true,
            Restriction::AgeGated => level == ContentLevel::Standard,
            Restriction::Removed | Restriction::Limited => false,
        }
    }
}

/// Seconds subtracted from publication times so hot scores stay small.
const HOT_EPOCH_SECS: f64 = 1_700_000_000.0;

/// The "hot" score: `log10(max(popularity, 1)) + (published_s - epoch) / gravity`.
///
/// It needs no recomputation as time passes: a post `gravity` seconds newer
/// ranks like one ten times more popular, so older posts sink on their own.
pub fn hot_score(popularity: f64, published_at_ms: i64, gravity_secs: f64) -> f64 {
    let popularity = if popularity.is_finite() { popularity.max(1.0) } else { 1.0 };
    popularity.log10() + (published_at_ms as f64 / 1000.0 - HOT_EPOCH_SECS) / gravity_secs.max(1.0)
}

/// A position in a stream, ordered like Redis `ZREVRANGEBYSCORE`: score
/// descending, then member descending.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamPosition {
    pub score:  f64,
    pub member: String,
}

impl StreamPosition {
    /// Whether `(score, member)` comes strictly after this position.
    pub fn is_before(&self, score: f64, member: &str) -> bool {
        score < self.score || (score == self.score && member < self.member.as_str())
    }
}

/// The discovery page cursor: the ranking it belongs to and, per stream, the
/// last position consumed.
///
/// Encoding: `base64url("d1|<ranking>|<stream>=<score>,<member>|…")`, where the
/// score is the shortest decimal that round-trips the `f64` (what Redis parses
/// back to the exact same double).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DiscoveryCursor {
    pub positions: Vec<(DiscoveryStream, StreamPosition)>,
}

impl DiscoveryCursor {
    pub fn position(&self, stream: DiscoveryStream) -> Option<&StreamPosition> {
        self.positions.iter().find(|(s, _)| *s == stream).map(|(_, p)| p)
    }

    pub fn encode(&self, ranking: DiscoveryRanking) -> String {
        let mut raw = format!("d1|{}", ranking.code());
        for (stream, p) in &self.positions {
            raw.push_str(&format!("|{}={},{}", stream.code(), p.score, p.member));
        }
        URL_SAFE_NO_PAD.encode(raw.as_bytes())
    }

    /// Fails on a malformed token or one minted for another ranking.
    pub fn decode(token: &str, ranking: DiscoveryRanking) -> Result<Self, TimelineError> {
        let invalid = || TimelineError::InvalidPageToken { token: token.to_owned() };
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        let raw = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
        let mut parts = raw.split('|');
        if parts.next() != Some("d1") {
            return Err(invalid());
        }
        let ranking_code = parts.next().and_then(|r| r.chars().next()).ok_or_else(invalid)?;
        if ranking_code != ranking.code() {
            return Err(invalid());
        }
        let mut positions = Vec::new();
        for part in parts {
            let (stream, rest) = part.split_once('=').ok_or_else(invalid)?;
            let stream = stream
                .chars()
                .next()
                .and_then(DiscoveryStream::from_code)
                .ok_or_else(invalid)?;
            let (score, member) = rest.split_once(',').ok_or_else(invalid)?;
            let score: f64 = score.parse().map_err(|_| invalid())?;
            if !score.is_finite() || member.is_empty() {
                return Err(invalid());
            }
            positions.push((stream, StreamPosition { score, member: member.to_owned() }));
        }
        Ok(Self { positions })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_score_trades_popularity_for_freshness() {
        let gravity = 45_000.0;
        let t = 1_760_000_000_000;
        // Ten times the popularity is worth exactly one gravity period.
        let older_popular = hot_score(100.0, t, gravity);
        let newer_less = hot_score(10.0, t + 45_000_000, gravity);
        assert!((older_popular - newer_less).abs() < 1e-9);
        // No popularity counts as one; more popularity always ranks higher.
        assert_eq!(hot_score(0.0, t, gravity), hot_score(1.0, t, gravity));
        assert!(hot_score(2.0, t, gravity) > hot_score(1.0, t, gravity));
        assert!(hot_score(f64::NAN, t, gravity).is_finite());
    }

    #[test]
    fn the_cursor_round_trips_exact_scores_and_is_bound_to_its_ranking() {
        let cursor = DiscoveryCursor {
            positions: vec![
                (DiscoveryStream::Hot, StreamPosition { score: 1_234.567_890_123_4, member: "p:a".into() }),
                (DiscoveryStream::Fresh, StreamPosition { score: 1_760_000_000_123.0, member: "q:b".into() }),
            ],
        };
        let token = cursor.encode(DiscoveryRanking::ForYou);
        assert_eq!(DiscoveryCursor::decode(&token, DiscoveryRanking::ForYou).unwrap(), cursor);
        assert!(DiscoveryCursor::decode(&token, DiscoveryRanking::Trending).is_err());
        assert!(DiscoveryCursor::decode("not-a-token!", DiscoveryRanking::ForYou).is_err());
        let foreign = URL_SAFE_NO_PAD.encode("1700:abc");
        assert!(DiscoveryCursor::decode(&foreign, DiscoveryRanking::ForYou).is_err());
    }

    #[test]
    fn positions_order_like_a_reverse_range() {
        let p = StreamPosition { score: 5.0, member: "m".into() };
        assert!(p.is_before(4.0, "z"));
        assert!(p.is_before(5.0, "a"));
        assert!(!p.is_before(5.0, "m"));
        assert!(!p.is_before(5.0, "n"));
        assert!(!p.is_before(6.0, "a"));
    }

    #[test]
    fn only_eligible_posts_are_shown() {
        let base = DiscoveryMeta {
            post_id:         PostId::from_uuid(uuid::Uuid::now_v7()),
            author_id:       Some(AuthorId::from_uuid(uuid::Uuid::now_v7())),
            published_at_ms: Some(1),
            popularity:      0.0,
            restriction:     Restriction::None,
            deleted:         false,
        };
        assert!(base.shown_at(ContentLevel::Restricted));
        let gated = DiscoveryMeta { restriction: Restriction::AgeGated, ..base.clone() };
        assert!(!gated.shown_at(ContentLevel::Restricted));
        assert!(gated.shown_at(ContentLevel::Standard));
        for hidden in [
            DiscoveryMeta { restriction: Restriction::Limited, ..base.clone() },
            DiscoveryMeta { restriction: Restriction::Removed, ..base.clone() },
            DiscoveryMeta { deleted: true, ..base.clone() },
            DiscoveryMeta { author_id: None, ..base.clone() },
        ] {
            assert!(!hidden.shown_at(ContentLevel::Standard));
        }
        assert_eq!(Restriction::from_code(9), Restriction::Removed);
    }
}

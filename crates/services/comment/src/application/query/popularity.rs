//! The popularity order of a comment slot (#832, #977): a post's top-level
//! comments, or one comment's replies. The slot's newest
//! [`POPULARITY_WINDOW`] are ranked by likes (engagement), ties to the
//! newest; the older ones follow newest first. The ranking is read again per
//! page, so a comment whose likes change between pages can come back: the
//! client de-duplicates by `comment_id`. Engagement down: the newest order.

use std::collections::HashMap;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use uuid::Uuid;

use crate::{
    application::port::{CommentLikes, CommentRepository, CommentSummary},
    domain::value_object::{CommentId, PostId},
    error::CommentError,
};

/// The newest comments of a slot a popularity page ranks; older ones follow,
/// newest first.
pub const POPULARITY_WINDOW: i32 = 500;

/// The order of a comment slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommentSort {
    #[default]
    Newest,
    /// Most liked first, ties to the newest.
    Popularity,
}

/// Which comments a page lists.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Slot<'a> {
    /// The post's top-level comments.
    TopLevel,
    /// One top-level comment's replies.
    Replies(&'a CommentId),
}

impl Slot<'_> {
    async fn window<R: CommentRepository + ?Sized>(
        self,
        repository: &R,
        post_id:    &PostId,
        max:        i32,
    ) -> Result<Vec<CommentSummary>, CommentError> {
        match self {
            Slot::TopLevel => repository.list_top_level_window(post_id, max).await,
            Slot::Replies(comment_id) => repository.list_reply_window(post_id, comment_id, max).await,
        }
    }

    /// A newest-first page, as `ListTopLevel` / `ListReplies` page.
    pub(crate) async fn newest<R: CommentRepository + ?Sized>(
        self,
        repository: &R,
        post_id:    &PostId,
        limit:      i32,
        token:      Option<&str>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
        match self {
            Slot::TopLevel => repository.list_top_level(post_id, limit, token).await,
            Slot::Replies(comment_id) => repository.list_replies(post_id, comment_id, limit, token).await,
        }
    }
}

/// One page of `slot` in `sort` order, before the read gate.
pub(crate) async fn page<R: CommentRepository + ?Sized>(
    repository: &R,
    likes:      &dyn CommentLikes,
    post_id:    &PostId,
    slot:       Slot<'_>,
    sort:       CommentSort,
    limit:      i32,
    token:      Option<&str>,
) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
    match sort {
        CommentSort::Newest => slot.newest(repository, post_id, limit, token).await,
        CommentSort::Popularity => {
            let cursor = token.map(PopularityCursor::decode).transpose()?;
            popular_page(repository, likes, post_id, slot, limit.clamp(1, 100), cursor).await
        }
    }
}

/// One page by popularity: the ranked window, then the older comments newest
/// first.
async fn popular_page<R: CommentRepository + ?Sized>(
    repository: &R,
    likes:      &dyn CommentLikes,
    post_id:    &PostId,
    slot:       Slot<'_>,
    limit:      i32,
    cursor:     Option<PopularityCursor>,
) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
    let after = match cursor {
        // Past the window: newest first, as the slot pages.
        Some(PopularityCursor::Older { token }) => {
            let (page, next) = slot.newest(repository, post_id, limit, Some(&token)).await?;
            return Ok((page, next.map(|token| PopularityCursor::Older { token }.encode())));
        }
        Some(PopularityCursor::Ranked(position)) => Some(position),
        None => None,
    };

    let window = slot.window(repository, post_id, POPULARITY_WINDOW).await?;
    let full = window.len() == POPULARITY_WINDOW as usize;
    let oldest = window.last().map(|comment| repository.page_token_after(comment));
    let ids: Vec<CommentId> = window.iter().map(|comment| comment.comment_id.clone()).collect();
    // Engagement down: every count 0, so the newest order.
    let counts = likes.counts(&ids).await.unwrap_or_else(|error| {
        tracing::warn!(%error, "comment likes unavailable — comments ranked newest first");
        HashMap::new()
    });

    let ranked = rank(window, &counts);
    let start = after.map_or(0, |position| ranked.partition_point(|comment| !position.precedes(comment)));
    let rest = &ranked[start..];
    let taken = rest.len().min(limit as usize);
    let mut page: Vec<CommentSummary> = rest[..taken].iter().map(|(_, comment)| comment.clone()).collect();

    if taken < rest.len() {
        let next = PopularityCursor::Ranked(Position::of(&rest[taken - 1]));
        return Ok((page, Some(next.encode())));
    }
    // The window is done: the slot's older comments follow, if any.
    match oldest.filter(|_| full) {
        None => Ok((page, None)),
        Some(token) if page.len() == limit as usize => Ok((page, Some(PopularityCursor::Older { token }.encode()))),
        Some(token) => {
            let left = limit - page.len() as i32;
            let (older, next) = slot.newest(repository, post_id, left, Some(&token)).await?;
            page.extend(older);
            Ok((page, next.map(|token| PopularityCursor::Older { token }.encode())))
        }
    }
}

/// The comments by likes, most first; ties to the newest, then by id.
fn rank(window: Vec<CommentSummary>, likes: &HashMap<CommentId, i64>) -> Vec<(i64, CommentSummary)> {
    let mut ranked: Vec<(i64, CommentSummary)> = window
        .into_iter()
        .map(|reply| (likes.get(&reply.comment_id).copied().unwrap_or(0), reply))
        .collect();
    ranked.sort_by(|(a_likes, a), (b_likes, b)| {
        b_likes
            .cmp(a_likes)
            .then(b.created_at.cmp(&a.created_at))
            .then(a.comment_id.as_uuid().cmp(&b.comment_id.as_uuid()))
    });
    ranked
}

/// A comment's place in the ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Position {
    likes: i64,
    at_ms: i64,
    id:    Uuid,
}

impl Position {
    fn of((likes, reply): &(i64, CommentSummary)) -> Self {
        Self { likes: *likes, at_ms: reply.created_at.timestamp_millis(), id: reply.comment_id.as_uuid() }
    }

    /// Whether `reply` ranks strictly after this position.
    fn precedes(&self, reply: &(i64, CommentSummary)) -> bool {
        let at = Self::of(reply);
        at.likes < self.likes
            || (at.likes == self.likes && (at.at_ms < self.at_ms || (at.at_ms == self.at_ms && at.id > self.id)))
    }
}

/// Where the next popularity page resumes: in the ranked window, or past it
/// (the slot's newest-first token).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum PopularityCursor {
    Ranked(Position),
    Older { token: String },
}

impl PopularityCursor {
    fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap_or_default())
    }

    fn decode(token: &str) -> Result<Self, CommentError> {
        let invalid = || CommentError::DomainViolation {
            field:   "page_token".to_owned(),
            message: "invalid popularity page token".to_owned(),
        };
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?).map_err(|_| invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::value_object::{CommentStatus, ProfileId};
    use chrono::{TimeZone, Utc};

    fn reply(n: u128, at_ms: i64) -> CommentSummary {
        CommentSummary {
            comment_id:  CommentId::from_uuid(Uuid::from_u128(n)),
            author_id:   ProfileId::from_uuid(Uuid::from_u128(1000)),
            parent_id:   None,
            status:      CommentStatus::Published,
            body:        Some("hi".to_owned()),
            gif_url:     None,
            gif_width:   None,
            gif_height:  None,
            created_at:  Utc.timestamp_millis_opt(at_ms).unwrap(),
            held:        false,
            reply_count: 0,
        }
    }

    fn ids(ranked: &[(i64, CommentSummary)]) -> Vec<u128> {
        ranked.iter().map(|(_, r)| r.comment_id.as_uuid().as_u128()).collect()
    }

    #[test]
    fn most_liked_first_ties_to_the_newest() {
        // Newest first, as the window comes.
        let window = vec![reply(1, 400), reply(2, 300), reply(3, 300), reply(4, 100)];
        let likes = HashMap::from([(CommentId::from_uuid(Uuid::from_u128(4)), 9), (CommentId::from_uuid(Uuid::from_u128(2)), 1)]);
        // 4 (9 likes), 2 (1 like), then the unliked: 1 (newest), then 3.
        assert_eq!(ids(&rank(window, &likes)), vec![4, 2, 1, 3]);

        // No likes at all (engagement down): the newest order, same-ms by id.
        let window = vec![reply(1, 400), reply(3, 300), reply(2, 300)];
        assert_eq!(ids(&rank(window, &HashMap::new())), vec![1, 2, 3]);
    }

    #[test]
    fn a_position_resumes_strictly_after_itself() {
        let likes = HashMap::from([(CommentId::from_uuid(Uuid::from_u128(4)), 9)]);
        let ranked = rank(vec![reply(1, 400), reply(2, 300), reply(3, 300), reply(4, 100)], &likes);
        let position = Position::of(&ranked[1]);
        let start = ranked.partition_point(|r| !position.precedes(r));
        assert_eq!(ids(&ranked[start..]), vec![2, 3]);

        // The reply at the position gained likes since: not shown again, and
        // nothing after its old place is skipped.
        let likes = HashMap::from([(CommentId::from_uuid(Uuid::from_u128(4)), 9), (CommentId::from_uuid(Uuid::from_u128(1)), 5)]);
        let moved = rank(vec![reply(1, 400), reply(2, 300), reply(3, 300), reply(4, 100)], &likes);
        let start = moved.partition_point(|r| !position.precedes(r));
        assert_eq!(ids(&moved[start..]), vec![2, 3]);
    }

    #[test]
    fn cursors_round_trip_and_garbage_is_refused() {
        let ranked = PopularityCursor::Ranked(Position { likes: 3, at_ms: 7, id: Uuid::now_v7() });
        assert_eq!(PopularityCursor::decode(&ranked.encode()).unwrap(), ranked);
        let older = PopularityCursor::Older { token: "abc".to_owned() };
        assert_eq!(PopularityCursor::decode(&older.encode()).unwrap(), older);
        assert!(PopularityCursor::decode("%%%").is_err());
        assert!(PopularityCursor::decode(&URL_SAFE_NO_PAD.encode("{}")).is_err());
    }
}

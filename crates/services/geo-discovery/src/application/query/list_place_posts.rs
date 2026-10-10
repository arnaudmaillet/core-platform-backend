//! A place's posts, page by page (#833): the place page of a city or country
//! marker, past the map's per-tile Top-K. The ids come from the place's
//! durable listing (`posts_by_tile`, `posts_by_country`); the cards — and
//! every audience rule — come from the Focus path ([`GetGeoTimelineHandler`]):
//! a guest's country, a member's countries, ghosts and city-level authors,
//! the reader's audience.

use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::{CardStore, PlaceEntry, TileRepository};
use crate::application::query::get_geo_timeline::{GetGeoTimelineHandler, GetGeoTimelineQuery};
use crate::domain::entity::MapPostCard;
use crate::domain::value_object::{MapScope, Place, Viewer};
use crate::error::GeoDiscoveryError;

/// The newest posts a popular page ranks; older ones follow, newest first.
pub const POPULAR_WINDOW: i32 = 200;
/// Most cards per page.
pub const MAX_PAGE: i32 = 50;

/// The order of a place's posts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlaceSort {
    /// Newest first.
    #[default]
    Recent,
    /// By virality score, ties to the newest.
    Popular,
}

pub struct ListPlacePostsQuery {
    pub place:      Place,
    pub sort:       PlaceSort,
    pub limit:      i32,
    pub page_token: Option<String>,
    pub viewer:     Viewer,
    pub scope:      MapScope,
}

#[derive(Debug, Default)]
pub struct PlacePostsPage {
    pub cards:           Vec<MapPostCard>,
    /// `None` on the last page.
    pub next_page_token: Option<String>,
}

impl Query for ListPlacePostsQuery {
    type Response = PlacePostsPage;
}

pub struct ListPlacePostsHandler<CS, TR> {
    pub tile_repository: Arc<TR>,
    /// The Focus path: hydration and the audience rules.
    pub cards:           Arc<GetGeoTimelineHandler<CS, TR>>,
}

impl<CS, TR> QueryHandler<ListPlacePostsQuery> for ListPlacePostsHandler<CS, TR>
where
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
{
    type Error = GeoDiscoveryError;

    async fn handle(&self, envelope: Envelope<ListPlacePostsQuery>) -> Result<PlacePostsPage, GeoDiscoveryError> {
        let q = &envelope.payload;
        let limit = q.limit.clamp(1, MAX_PAGE);
        let cursor = q.page_token.as_deref().map(PlaceCursor::decode).transpose()?;
        match (q.sort, cursor) {
            (PlaceSort::Recent, None) => self.recent(q, None, limit).await,
            (_, Some(PlaceCursor::After(after))) => self.recent(q, Some(after.into()), limit).await,
            (PlaceSort::Popular, None) => self.popular(q, None, limit).await,
            (_, Some(PlaceCursor::Ranked(position))) => self.popular(q, Some(position), limit).await,
        }
    }
}

impl<CS, TR> ListPlacePostsHandler<CS, TR>
where
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
{
    /// Newest first, strictly after `after`.
    async fn recent(
        &self,
        q:     &ListPlacePostsQuery,
        after: Option<PlaceEntry>,
        limit: i32,
    ) -> Result<PlacePostsPage, GeoDiscoveryError> {
        let entries = self.tile_repository.list_place(&q.place, after, limit).await?;
        let next = (entries.len() == limit as usize)
            .then(|| entries.last().map(|last| PlaceCursor::After((*last).into()).encode()))
            .flatten();
        let cards = self.hydrate(q, &entries).await?;
        Ok(PlacePostsPage { cards, next_page_token: next })
    }

    /// The window ranked by virality, then the older posts newest first.
    async fn popular(
        &self,
        q:     &ListPlacePostsQuery,
        after: Option<Position>,
        limit: i32,
    ) -> Result<PlacePostsPage, GeoDiscoveryError> {
        let window = self.tile_repository.list_place(&q.place, None, POPULAR_WINDOW).await?;
        let full = window.len() == POPULAR_WINDOW as usize;
        let oldest = window.last().copied();
        let ranked = rank(self.hydrate(q, &window).await?);

        let start = after.map_or(0, |position| ranked.partition_point(|card| !position.precedes(card)));
        let rest = &ranked[start..];
        let taken = rest.len().min(limit as usize);
        let mut cards: Vec<MapPostCard> = rest[..taken].to_vec();

        if taken < rest.len() {
            let next = PlaceCursor::Ranked(Position::of(&rest[taken - 1]));
            return Ok(PlacePostsPage { cards, next_page_token: Some(next.encode()) });
        }
        // The window is done: the place's older posts follow, if any.
        let Some(oldest) = oldest.filter(|_| full) else {
            return Ok(PlacePostsPage { cards, next_page_token: None });
        };
        if cards.len() == limit as usize {
            return Ok(PlacePostsPage { cards, next_page_token: Some(PlaceCursor::After(oldest.into()).encode()) });
        }
        let older = self.recent(q, Some(oldest), limit - cards.len() as i32).await?;
        cards.extend(older.cards);
        Ok(PlacePostsPage { cards, next_page_token: older.next_page_token })
    }

    /// The entries' cards, in the entries' order, as the reader may see them.
    async fn hydrate(&self, q: &ListPlacePostsQuery, entries: &[PlaceEntry]) -> Result<Vec<MapPostCard>, GeoDiscoveryError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let query = GetGeoTimelineQuery {
            post_ids: entries.iter().map(|e| e.post_id).collect(),
            viewer:   q.viewer.clone(),
            scope:    q.scope.clone(),
        };
        let mut found: HashMap<Uuid, MapPostCard> = self
            .cards
            .handle(Envelope::new(Uuid::now_v7(), query))
            .await?
            .cards
            .into_iter()
            .map(|card| (card.post_id, card))
            .collect();
        Ok(entries.iter().filter_map(|e| found.remove(&e.post_id)).collect())
    }
}

/// Most viral first; ties to the newest, then by id.
fn rank(mut cards: Vec<MapPostCard>) -> Vec<MapPostCard> {
    cards.sort_by(|a, b| {
        b.virality_score
            .total_cmp(&a.virality_score)
            .then(b.published_at_ms.cmp(&a.published_at_ms))
            .then(a.post_id.cmp(&b.post_id))
    });
    cards
}

/// A post's place in the popular ranking.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
struct Position {
    score: f32,
    at_ms: i64,
    id:    Uuid,
}

impl Position {
    fn of(card: &MapPostCard) -> Self {
        Self { score: card.virality_score, at_ms: card.published_at_ms, id: card.post_id }
    }

    /// Whether `card` ranks strictly after this position.
    fn precedes(&self, card: &MapPostCard) -> bool {
        let at = Self::of(card);
        match at.score.total_cmp(&self.score) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => at.at_ms < self.at_ms || (at.at_ms == self.at_ms && at.id > self.id),
        }
    }
}

/// A listing position on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct After {
    at_ms: i64,
    id:    Uuid,
}

impl From<After> for PlaceEntry {
    fn from(a: After) -> Self {
        Self { post_id: a.id, published_at_ms: a.at_ms }
    }
}

impl From<PlaceEntry> for After {
    fn from(e: PlaceEntry) -> Self {
        Self { at_ms: e.published_at_ms, id: e.post_id }
    }
}

/// Where the next page resumes: newest first after a post, or in the
/// popular ranking.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum PlaceCursor {
    After(After),
    Ranked(Position),
}

impl PlaceCursor {
    fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap_or_default())
    }

    fn decode(token: &str) -> Result<Self, GeoDiscoveryError> {
        let invalid = || GeoDiscoveryError::DomainViolation {
            field:   "page_token".to_owned(),
            message: "invalid place page token".to_owned(),
        };
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?).map_err(|_| invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(n: u128, score: f32, at_ms: i64) -> MapPostCard {
        MapPostCard {
            post_id:           Uuid::from_u128(n),
            author_id:         Uuid::from_u128(1000),
            author_handle:     String::new(),
            author_avatar_url: String::new(),
            thumbnail_url:     String::new(),
            caption:           String::new(),
            h3_index_r7:       0,
            virality_score:    score,
            published_at_ms:   at_ms,
            author_tier:       0,
            lat:               None,
            lng:               None,
        }
    }

    fn ids(cards: &[MapPostCard]) -> Vec<u128> {
        cards.iter().map(|c| c.post_id.as_u128()).collect()
    }

    #[test]
    fn most_viral_first_ties_to_the_newest() {
        let ranked = rank(vec![card(1, 0.0, 400), card(2, 5.0, 100), card(3, 0.0, 300), card(4, 5.0, 200)]);
        assert_eq!(ids(&ranked), vec![4, 2, 1, 3]);
    }

    #[test]
    fn a_position_resumes_strictly_after_itself() {
        let ranked = rank(vec![card(1, 0.0, 400), card(2, 5.0, 100), card(3, 0.0, 300), card(4, 5.0, 200)]);
        let position = Position::of(&ranked[1]);
        let start = ranked.partition_point(|c| !position.precedes(c));
        assert_eq!(ids(&ranked[start..]), vec![1, 3]);
    }

    #[test]
    fn cursors_round_trip_and_garbage_is_refused() {
        for cursor in [
            PlaceCursor::After(After { at_ms: 9, id: Uuid::now_v7() }),
            PlaceCursor::Ranked(Position { score: 1.5, at_ms: 9, id: Uuid::now_v7() }),
        ] {
            assert_eq!(PlaceCursor::decode(&cursor.encode()).unwrap(), cursor);
        }
        assert!(PlaceCursor::decode("%%%").is_err());
        assert!(PlaceCursor::decode(&URL_SAFE_NO_PAD.encode("{}")).is_err());
    }
}

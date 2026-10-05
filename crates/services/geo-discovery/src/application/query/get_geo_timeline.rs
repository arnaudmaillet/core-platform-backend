use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::country_access::country_limit;
use crate::application::port::{
    sharing_for_reader, visible_authors, AudienceGate, CardStore, CountryGrantStore, LocationSettingsStore,
    TileRepository,
};
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::entity::MapPostCard;
use crate::domain::value_object::{city_point, city_r7, MapScope, PostId, Viewer};
use crate::error::GeoDiscoveryError;

/// Focus path: hydrates a batch of focused pins into fully-rendered cards.
///
/// Triggered when the user taps a pin (or expands a cluster bottom-sheet). This
/// is the cold read the Radar pan path deliberately avoids: it MGETs the
/// hydrated cards from Redis and falls back to ScyllaDB point-reads for any
/// cache miss, so a card that aged out of Redis is still served.
///
/// Unresolved ids (never indexed / fully expired) are simply absent from the
/// result — the handler does not error on partial resolution.
pub struct GetGeoTimelineQuery {
    pub post_ids: Vec<Uuid>,
    /// Who is looking: a client only gets cards of authors it may see.
    pub viewer:   Viewer,
    /// Which part of the map: a guest only gets cards in its granted country.
    pub scope:    MapScope,
}

pub struct GetGeoTimelineResult {
    pub cards: Vec<MapPostCard>,
}

impl Query for GetGeoTimelineQuery {
    type Response = GetGeoTimelineResult;
}

pub struct GetGeoTimelineHandler<CS, TR> {
    pub card_store:      Arc<CS>,
    pub tile_repository: Arc<TR>,
    pub audience:        Arc<dyn AudienceGate>,
    pub grants:          Arc<dyn CountryGrantStore>,
    pub atlas:           &'static CountryAtlas,
    /// The authors' location sharing (ghost, city level).
    pub location:        Arc<dyn LocationSettingsStore>,
}

impl<CS, TR> QueryHandler<GetGeoTimelineQuery> for GetGeoTimelineHandler<CS, TR>
where
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
{
    type Error = GeoDiscoveryError;

    async fn handle(
        &self,
        envelope: Envelope<GetGeoTimelineQuery>,
    ) -> Result<GetGeoTimelineResult, GeoDiscoveryError> {
        let post_ids = &envelope.payload.post_ids;

        if post_ids.is_empty() {
            return Ok(GetGeoTimelineResult { cards: vec![] });
        }
        let limit = country_limit(self.grants.as_ref(), &envelope.payload.scope).await?;
        if limit == Some(None) {
            return Ok(GetGeoTimelineResult { cards: vec![] });
        }

        // ── Phase 1: MGET hydrated cards from Redis ───────────────────────────
        let cached = self.card_store.mget(post_ids).await?;

        let mut cards: Vec<MapPostCard> = Vec::with_capacity(post_ids.len());
        let mut miss_ids: Vec<Uuid> = Vec::new();

        for (id, opt_card) in post_ids.iter().zip(cached) {
            match opt_card {
                Some(card) => cards.push(card),
                None       => miss_ids.push(*id),
            }
        }

        // ── Phase 2: ScyllaDB fallback for cache misses (the Focus cold path) ─
        if !miss_ids.is_empty() {
            let miss_futures: Vec<_> = miss_ids.iter()
                .map(|id| {
                    let post_id = PostId::from(*id);
                    let tr = Arc::clone(&self.tile_repository);
                    async move { tr.get_card(&post_id).await }
                })
                .collect();

            let miss_results = futures::future::try_join_all(miss_futures).await?;
            for maybe_card in miss_results.into_iter().flatten() {
                cards.push(maybe_card);
            }
        }

        // ── Phase 2b: a guest's country. A card without a stored location
        //   (indexed before it was kept) cannot be placed: left out.
        if let Some(Some(country)) = limit {
            cards.retain(|c| match (c.lat, c.lng) {
                (Some(lat), Some(lng)) => self.atlas.contains(country, lat, lng),
                _ => false,
            });
        }

        // ── Phase 2c: the authors' location sharing (#657; fails closed): a
        //   ghost's cards leave the map; a city-level author's name the city's
        //   R7 cell, never their own.
        let owned: &[String] = match &envelope.payload.viewer {
            Viewer::Profiles(ids) => ids,
            Viewer::Internal => &[],
        };
        let sharing = sharing_for_reader(self.location.as_ref(), owned, cards.iter().map(|c| c.author_id)).await?;
        if !sharing.is_empty() {
            cards.retain_mut(|c| match sharing.get(&c.author_id) {
                None => true,
                Some(s) if s.ghost => false,
                // Shared precisely (with a restricted audience: Phase 3).
                Some(s) if !s.city => true,
                Some(_) => match city_r7(c.h3_index_r7) {
                    Some(r7) => {
                        c.h3_index_r7 = r7;
                        (c.lat, c.lng) = match (c.lat, c.lng) {
                            (Some(lat), Some(lng)) => city_point(lat, lng).map_or((None, None), |(a, b)| (Some(a), Some(b))),
                            _ => (None, None),
                        };
                        true
                    }
                    None => false,
                },
            });
        }

        // ── Phase 3: the reader's audience (one bulk CheckAccess; fail closed),
        //   then each author's location audience (#657).
        if let Some(visible) = visible_authors(
            self.audience.as_ref(),
            &envelope.payload.viewer,
            cards.iter().map(|c| c.author_id),
            &sharing,
        )
        .await?
        {
            cards.retain(|c| visible.contains(&c.author_id));
        }

        Ok(GetGeoTimelineResult { cards })
    }
}

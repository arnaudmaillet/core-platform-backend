use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use validate_core::{FieldViolation, Validate};

use crate::application::country_access::country_limit;
use crate::application::port::{
    sharing_for_reader, visible_authors, AudienceGate, CountryGrantStore, LocationSettingsStore, PinStore,
    SpatialIndex,
};
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::entity::RadarPin;
use crate::domain::value_object::{
    city_point, zoom_to_resolution, GeoCoordinate, H3Resolution, MapScope, Viewer,
};
use crate::error::GeoDiscoveryError;
use crate::infrastructure::h3::h3_codec;

/// Radar path: resolves the lightweight pins visible within a viewport.
///
/// Hot path, Redis-only: 2 round-trips (ZRANGEBYSCORE × N tiles → pin GET × M).
/// There is deliberately NO ScyllaDB fallback — a pin absent from Redis is
/// silently dropped (fail-open). Card hydration (author metadata, caption) is
/// the Focus path's job ([`super::get_geo_timeline`]), reached on pin tap.
pub struct QueryTileQuery {
    pub sw_lat:     f64,
    pub sw_lng:     f64,
    pub ne_lat:     f64,
    pub ne_lng:     f64,
    pub zoom_level: i32,
    /// Who is looking: a client only gets pins of authors it may see.
    pub viewer:     Viewer,
    /// Which part of the map: a guest only sees its granted country.
    pub scope:      MapScope,
}

pub struct QueryTileResult {
    pub pins:       Vec<RadarPin>,
    pub tile_count: i32,
}

impl Query for QueryTileQuery {
    type Response = QueryTileResult;
}

impl Validate for QueryTileQuery {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if !self.sw_lat.is_finite() || self.sw_lat < -90.0 || self.sw_lat > 90.0 {
            v.push(FieldViolation::new("sw_lat", "GEO-VAL-030", "sw_lat must be in [-90, 90]"));
        }
        if !self.sw_lng.is_finite() || self.sw_lng < -180.0 || self.sw_lng > 180.0 {
            v.push(FieldViolation::new("sw_lng", "GEO-VAL-031", "sw_lng must be in [-180, 180]"));
        }
        if !self.ne_lat.is_finite() || self.ne_lat < -90.0 || self.ne_lat > 90.0 {
            v.push(FieldViolation::new("ne_lat", "GEO-VAL-032", "ne_lat must be in [-90, 90]"));
        }
        if !self.ne_lng.is_finite() || self.ne_lng < -180.0 || self.ne_lng > 180.0 {
            v.push(FieldViolation::new("ne_lng", "GEO-VAL-033", "ne_lng must be in [-180, 180]"));
        }
        if self.sw_lat >= self.ne_lat {
            v.push(FieldViolation::new("viewport", "GEO-VAL-034", "sw_lat must be less than ne_lat"));
        }
        if self.zoom_level < 0 || self.zoom_level > 15 {
            v.push(FieldViolation::new("zoom_level", "GEO-VAL-035", "zoom_level must be in [0, 15]"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct QueryTileHandler<SI, PS> {
    pub spatial_index: Arc<SI>,
    pub pin_store:     Arc<PS>,
    pub audience:      Arc<dyn AudienceGate>,
    pub grants:        Arc<dyn CountryGrantStore>,
    pub atlas:         &'static CountryAtlas,
    /// The authors' location sharing (ghost, city level).
    pub location:      Arc<dyn LocationSettingsStore>,
}

impl<SI, PS> QueryHandler<QueryTileQuery> for QueryTileHandler<SI, PS>
where
    SI: SpatialIndex + 'static,
    PS: PinStore + 'static,
{
    type Error = GeoDiscoveryError;

    async fn handle(&self, envelope: Envelope<QueryTileQuery>) -> Result<QueryTileResult, GeoDiscoveryError> {
        let q = &envelope.payload;

        let sw = GeoCoordinate::new(q.sw_lat, q.sw_lng)?;
        let ne = GeoCoordinate::new(q.ne_lat, q.ne_lng)?;

        if sw.lat >= ne.lat {
            return Err(GeoDiscoveryError::InvalidViewport {
                sw_lat: q.sw_lat, sw_lng: q.sw_lng,
                ne_lat: q.ne_lat, ne_lng: q.ne_lng,
            });
        }

        // A guest without a granted country sees nothing: answer before any read.
        let limit = country_limit(self.grants.as_ref(), &q.scope).await?;
        if limit == Some(None) {
            return Ok(QueryTileResult { pins: vec![], tile_count: 0 });
        }

        let resolution  = zoom_to_resolution(q.zoom_level);
        let min_score   = resolution.virality_floor(q.zoom_level);
        let tiles       = h3_codec::viewport_cells(&sw, &ne, resolution);
        let tile_count  = tiles.len() as i32;

        // ── Phase 1: ZRANGEBYSCORE for all tiles (concurrent, one RTT per tile
        //   through fred's lock-free command queue → effectively pipelined) ────
        let tile_futures: Vec<_> = tiles.iter()
            .map(|tile| {
                let tile   = *tile;
                let si     = Arc::clone(&self.spatial_index);
                async move { si.query(tile, resolution, min_score).await }
            })
            .collect();

        let tile_results = futures::future::try_join_all(tile_futures).await?;

        // Deduplicate across tile boundaries (posts near hexagon edges appear in
        // multiple grid_disk results).
        let mut seen = std::collections::HashSet::new();
        let post_ids: Vec<uuid::Uuid> = tile_results
            .into_iter()
            .flatten()
            .filter(|id| seen.insert(*id))
            .collect();

        if post_ids.is_empty() {
            // Touch hot tiles even for empty results (keeps active-area tiles warm).
            let touch_pairs: Vec<_> = tiles.iter().map(|t| (*t, resolution)).collect();
            let _ = self.spatial_index.touch_hot_tiles(&touch_pairs).await;
            return Ok(QueryTileResult { pins: vec![], tile_count });
        }

        // ── Phase 2: pin lookup (Redis-only, single fan-out round-trip) ───────
        // No ScyllaDB fallback: the Radar pan path is fail-open. A pin absent from
        // Redis is silently dropped — the user pans again, or taps a neighbour.
        let cached = self.pin_store.mget(&post_ids).await?;
        let mut pins: Vec<RadarPin> = cached.into_iter().flatten().collect();

        // ── Phase 2b: a guest's country (borders shared with the app).
        if let Some(Some(country)) = limit {
            pins.retain(|p| self.atlas.contains(country, p.lat, p.lng));
        }

        // ── Phase 2c: the authors' location sharing (#657; fails closed). For
        //   any reader but the author — the mesh included: a ghost's posts
        //   leave the map; a city-level author's show only at the coarse band
        //   (R5), at the cell's centre.
        let owned: &[String] = match &q.viewer {
            Viewer::Profiles(ids) => ids,
            Viewer::Internal => &[],
        };
        let sharing =
            sharing_for_reader(self.location.as_ref(), owned, pins.iter().filter_map(|p| p.author_id)).await?;
        if !sharing.is_empty() {
            pins.retain_mut(|p| match p.author_id.and_then(|a| sharing.get(&a)) {
                None => true,
                Some(s) if s.ghost => false,
                // Shared precisely (with a restricted audience: Phase 3).
                Some(s) if !s.city => true,
                Some(_) if resolution != H3Resolution::R5 => false,
                Some(_) => match city_point(p.lat, p.lng) {
                    Some((lat, lng)) => {
                        (p.lat, p.lng) = (lat, lng);
                        true
                    }
                    None => false,
                },
            });
        }

        // ── Phase 3: the reader's audience (one bulk CheckAccess; fail closed),
        //   then each author's location audience (#657).
        //   A pin without a recorded author is unverifiable: dropped for a client.
        if let Some(visible) = visible_authors(
            self.audience.as_ref(),
            &q.viewer,
            pins.iter().filter_map(|p| p.author_id),
            &sharing,
        )
        .await?
        {
            pins.retain(|p| p.author_id.is_some_and(|a| visible.contains(&a)));
        }

        // Fire-and-forget: update hot_tiles scores for the queried tiles.
        let touch_pairs: Vec<_> = tiles.iter().map(|t| (*t, resolution)).collect();
        let _ = self.spatial_index.touch_hot_tiles(&touch_pairs).await;

        Ok(QueryTileResult { pins, tile_count })
    }
}

//! NEARBY's candidate pool from geo-discovery's map index (`QueryTile`, over the
//! mesh, so unfiltered — timeline applies moderation state and the audience).
//!
//! Two rings, queried together: a wide one at a regional zoom (geo returns only
//! posts above its virality floor there) and a close one at street zoom (no
//! floor, so a fresh post next door still shows).

use std::collections::HashSet;

use async_trait::async_trait;
use geo_discovery_api::geo_discovery_service_client::GeoDiscoveryServiceClient;
use geo_discovery_api::{QueryTileRequest, Viewport};
use transport::grpc::client::ResilientChannel;

use crate::application::port::NearbyPosts;
use crate::domain::value_object::PostId;
use crate::error::TimelineError;

/// Regional zoom: geo's R7 band with its popularity floor.
const WIDE_ZOOM: i32 = 9;
/// Street zoom: geo's R9 band, no floor.
const CLOSE_ZOOM: i32 = 13;
const KM_PER_DEGREE_LAT: f64 = 111.32;

pub struct GrpcNearbyPosts {
    channel:         ResilientChannel,
    radius_km:       f64,
    close_radius_km: f64,
}

impl GrpcNearbyPosts {
    pub fn new(channel: ResilientChannel, radius_km: f64, close_radius_km: f64) -> Self {
        Self { channel, radius_km, close_radius_km }
    }

    async fn ring(
        &self,
        lat:        f64,
        lng:        f64,
        radius_km:  f64,
        zoom_level: i32,
        guest:      Option<&str>,
    ) -> Result<Vec<PostId>, TimelineError> {
        let request = QueryTileRequest {
            viewport: Some(viewport(lat, lng, radius_km)),
            zoom_level,
            guest_principal: guest.unwrap_or_default().to_owned(),
        };
        let response = GeoDiscoveryServiceClient::new(self.channel.clone())
            .query_tile(request)
            .await
            .map_err(|status| TimelineError::NearbyUnavailable { reason: status.to_string() })?
            .into_inner();
        Ok(response.pins.iter().filter_map(|p| PostId::try_from(p.post_id.as_str()).ok()).collect())
    }
}

/// The box around a point, clamped to valid coordinates (no antimeridian wrap:
/// near ±180° the ring is simply cut).
fn viewport(lat: f64, lng: f64, radius_km: f64) -> Viewport {
    let d_lat = radius_km / KM_PER_DEGREE_LAT;
    let d_lng = radius_km / (KM_PER_DEGREE_LAT * lat.to_radians().cos().max(0.01));
    Viewport {
        sw_lat: (lat - d_lat).max(-90.0),
        sw_lng: (lng - d_lng).max(-180.0),
        ne_lat: (lat + d_lat).min(90.0),
        ne_lng: (lng + d_lng).min(180.0),
    }
}

#[async_trait]
impl NearbyPosts for GrpcNearbyPosts {
    async fn around(&self, lat: f64, lng: f64, guest: Option<&str>) -> Result<Vec<PostId>, TimelineError> {
        let (wide, close) = tokio::join!(
            self.ring(lat, lng, self.radius_km, WIDE_ZOOM, guest),
            self.ring(lat, lng, self.close_radius_km, CLOSE_ZOOM, guest),
        );
        let mut seen = HashSet::new();
        Ok(close?.into_iter().chain(wide?).filter(|id| seen.insert(*id)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_box_spans_the_radius_and_stays_on_the_globe() {
        let v = viewport(48.85, 2.35, 10.0);
        assert!((v.ne_lat - v.sw_lat - 20.0 / KM_PER_DEGREE_LAT).abs() < 1e-9);
        assert!(v.ne_lng - v.sw_lng > v.ne_lat - v.sw_lat, "longitude degrees are shorter away from the equator");
        let pole = viewport(89.99, 179.99, 50.0);
        assert!(pole.ne_lat <= 90.0 && pole.ne_lng <= 180.0 && pole.sw_lat < pole.ne_lat);
    }
}

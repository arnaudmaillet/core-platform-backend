use std::net::IpAddr;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{Envelope, QueryBus};

use crate::application::country_access::ResolveCountryAccess;
use crate::application::query::get_geo_timeline::GetGeoTimelineQuery;
use crate::application::query::query_tile::QueryTileQuery;
use crate::domain::value_object::{CountryAccessOutcome, MapScope, Viewer};
use transport::grpc::edge;

// ── Proto inclusion ───────────────────────────────────────────────────────────

pub use geo_discovery_api as proto;

pub use proto::geo_discovery_service_server::GeoDiscoveryServiceServer;

// ── Handler ───────────────────────────────────────────────────────────────────

pub struct GeoDiscoveryHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    query_bus:          QB,
    country_access:     Arc<ResolveCountryAccess>,
    trusted_proxy_hops: usize,
}

impl<QB> GeoDiscoveryHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(query_bus: QB, country_access: Arc<ResolveCountryAccess>, trusted_proxy_hops: usize) -> Self {
        Self { query_bus, country_access, trusted_proxy_hops }
    }
}

// ── RPC implementations ───────────────────────────────────────────────────────

impl<QB> GeoDiscoveryHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    async fn query_tile_inner(
        &self,
        request: Request<proto::QueryTileRequest>,
    ) -> Result<Response<proto::QueryTileResponse>, Status> {
        let viewer   = viewer_of(&request);
        let mut scope = scope_of(&request);
        let req      = request.into_inner();
        // A mesh caller reading for a guest (timeline's NEARBY) gets that
        // guest's limit; on the edge the caller's own token decides.
        if viewer == Viewer::Internal && !req.guest_principal.is_empty() {
            scope = MapScope::Guest(req.guest_principal.clone());
        }
        let viewport = req.viewport.ok_or_else(|| Status::invalid_argument("viewport is required"))?;

        let query = QueryTileQuery {
            sw_lat:     viewport.sw_lat,
            sw_lng:     viewport.sw_lng,
            ne_lat:     viewport.ne_lat,
            ne_lng:     viewport.ne_lng,
            zoom_level: req.zoom_level,
            viewer,
            scope,
        };

        let result = self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_to_status)?;

        let pins = result.pins
            .into_iter()
            .map(pin_to_proto)
            .collect();

        Ok(Response::new(proto::QueryTileResponse {
            pins,
            tile_count: result.tile_count,
        }))
    }

    async fn get_geo_timeline_inner(
        &self,
        request: Request<proto::GetGeoTimelineRequest>,
    ) -> Result<Response<proto::GetGeoTimelineResponse>, Status> {
        let viewer = viewer_of(&request);
        let scope = scope_of(&request);
        let req = request.into_inner();

        // Parse the requested ids, skipping any that are not valid UUIDs rather
        // than failing the whole batch (a focus request is best-effort).
        let post_ids: Vec<Uuid> = req.post_ids
            .iter()
            .filter_map(|s| Uuid::parse_str(s).ok())
            .collect();

        if post_ids.is_empty() {
            return Ok(Response::new(proto::GetGeoTimelineResponse { cards: vec![] }));
        }

        let result = self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetGeoTimelineQuery { post_ids, viewer, scope }))
            .await
            .map_err(cqrs_to_status)?;

        let cards = result.cards
            .into_iter()
            .map(card_to_proto)
            .collect();

        Ok(Response::new(proto::GetGeoTimelineResponse { cards }))
    }

    async fn get_country_access_inner(
        &self,
        request: Request<proto::GetCountryAccessRequest>,
    ) -> Result<Response<proto::GetCountryAccessResponse>, Status> {
        // The principal is the token's (a guest's `guest:<id>`, a member's
        // account); a mesh call has none.
        let principal = edge::principal(&request).map(|p| p.account_id().to_owned()).unwrap_or_default();
        let client_ip = client_ip(&request, self.trusted_proxy_hops);
        let claimed = request.into_inner().current_country;

        let outcome = self
            .country_access
            .handle(&principal, Some(claimed.as_str()), client_ip)
            .await
            .map_err(app_to_status)?;

        let (current_country, outcome) = match outcome {
            CountryAccessOutcome::Granted(country) => (country.to_string(), proto::CountryAccessOutcome::Granted),
            CountryAccessOutcome::NotSent => (String::new(), proto::CountryAccessOutcome::NotSent),
            CountryAccessOutcome::Mismatch => (String::new(), proto::CountryAccessOutcome::Mismatch),
            CountryAccessOutcome::Unverifiable => (String::new(), proto::CountryAccessOutcome::Unverifiable),
        };
        Ok(Response::new(proto::GetCountryAccessResponse { current_country, outcome: outcome as i32 }))
    }
}

// ── Proto trait implementation ─────────────────────────────────────────────────

#[tonic::async_trait]
impl<QB> proto::geo_discovery_service_server::GeoDiscoveryService for GeoDiscoveryHandler<QB>
where
    QB: QueryBus + Send + Sync + 'static,
{
    async fn query_tile(
        &self,
        request: Request<proto::QueryTileRequest>,
    ) -> Result<Response<proto::QueryTileResponse>, Status> {
        self.query_tile_inner(request).await
    }

    async fn get_geo_timeline(
        &self,
        request: Request<proto::GetGeoTimelineRequest>,
    ) -> Result<Response<proto::GetGeoTimelineResponse>, Status> {
        self.get_geo_timeline_inner(request).await
    }

    async fn get_country_access(
        &self,
        request: Request<proto::GetCountryAccessRequest>,
    ) -> Result<Response<proto::GetCountryAccessResponse>, Status> {
        self.get_country_access_inner(request).await
    }
}

// ── Conversion helpers ────────────────────────────────────────────────────────

/// The reader, from how the request arrived: the mesh is unfiltered; an
/// anonymous client has no profiles.
fn viewer_of<T>(request: &Request<T>) -> Viewer {
    match edge::viewer(request) {
        edge::Viewer::Internal => Viewer::Internal,
        edge::Viewer::Anonymous => Viewer::Profiles(Vec::new()),
        edge::Viewer::Member { profile_ids, .. } => Viewer::Profiles(profile_ids),
    }
}

/// Which part of the map the caller may see: a guest session (or an anonymous
/// caller) only its granted country; members and the mesh everything (v1).
fn scope_of<T>(request: &Request<T>) -> MapScope {
    match edge::viewer(request) {
        edge::Viewer::Internal => MapScope::All,
        edge::Viewer::Anonymous => MapScope::Guest(String::new()),
        edge::Viewer::Member { .. } => match edge::principal(request) {
            Some(p) if p.is_guest() => MapScope::Guest(p.account_id().to_owned()),
            _ => MapScope::All,
        },
    }
}

/// The client's address behind the trusted proxies (see `transport::grpc::client_ip`).
fn client_ip<T>(request: &Request<T>, trusted_hops: usize) -> Option<IpAddr> {
    transport::grpc::client_ip::request_client_ip(request, trusted_hops)
}

fn app_to_status(err: crate::error::GeoDiscoveryError) -> Status {
    use error::AppError as _;
    let msg = err.to_string();
    match err.http_status().as_u16() {
        422 | 400 => Status::failed_precondition(msg),
        503 | 502 => Status::unavailable(msg),
        _ => Status::internal(msg),
    }
}

fn pin_to_proto(pin: crate::domain::entity::RadarPin) -> proto::RadarPin {
    proto::RadarPin {
        post_id:       pin.post_id.to_string(),
        lat:           pin.lat,
        lng:           pin.lng,
        thumbnail_url: pin.thumbnail_url,
    }
}

fn card_to_proto(card: crate::domain::entity::MapPostCard) -> proto::MapPostCard {
    // Map u8 tier (0=Standard, 1=Premium, 2=VIP) to proto AuthorTier enum.
    // Proto uses +1 offset: UNSPECIFIED=0, STANDARD=1, PREMIUM=2, VIP=3.
    // We treat u8=0 as STANDARD (not UNSPECIFIED) for deterministic client rendering.
    let author_tier = match card.author_tier {
        1 => proto::AuthorTier::Premium as i32,
        2 => proto::AuthorTier::Vip as i32,
        _ => proto::AuthorTier::Standard as i32,
    };

    proto::MapPostCard {
        post_id:           card.post_id.to_string(),
        author_id:         card.author_id.to_string(),
        author_handle:     card.author_handle,
        author_avatar_url: card.author_avatar_url,
        thumbnail_url:     card.thumbnail_url,
        h3_index_r7:       card.h3_index_r7,
        virality_score:    card.virality_score,
        published_at_ms:   card.published_at_ms,
        author_tier,
        caption:           card.caption,
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

pub fn cqrs_to_status(err: cqrs::error::CqrsError) -> Status {
    use cqrs::error::CqrsError;
    match err {
        CqrsError::HandlerNotFound { type_name } => {
            Status::unimplemented(format!("no handler registered for {type_name}"))
        }
        CqrsError::DuplicateRegistration { type_name } => {
            Status::internal(format!("duplicate handler for {type_name}"))
        }
        CqrsError::Handler(boxed) => {
            use error::AppError as _;
            let msg       = boxed.to_string();
            let retryable = boxed.is_retryable();
            match boxed.http_status().as_u16() {
                403       => Status::permission_denied(msg),
                404       => Status::not_found(msg),
                409 if retryable => Status::aborted(msg),
                409       => Status::already_exists(msg),
                400 | 422 => Status::failed_precondition(msg),
                503 | 502 => Status::unavailable(msg),
                _         => Status::internal(msg),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with(xff: &[&str]) -> Request<()> {
        let mut request = Request::new(());
        for value in xff {
            request.metadata_mut().append("x-forwarded-for", value.parse().unwrap());
        }
        request
    }

    #[test]
    fn the_client_address_is_the_one_the_trusted_proxy_appended() {
        // A client spoofing the header: the ALB appends the real address last.
        let r = request_with(&["6.6.6.6, 203.0.113.9"]);
        assert_eq!(client_ip(&r, 1), Some("203.0.113.9".parse().unwrap()));
        assert_eq!(client_ip(&r, 2), Some("6.6.6.6".parse().unwrap()));
        assert_eq!(client_ip(&r, 3), None, "fewer entries than trusted hops: unknown");
        let split = request_with(&["6.6.6.6", "2001:db8::1"]);
        assert_eq!(client_ip(&split, 1), Some("2001:db8::1".parse().unwrap()));
        assert_eq!(client_ip(&request_with(&["garbage"]), 1), None);
        assert_eq!(client_ip(&Request::new(()), 1), None, "no header, no peer");
    }

    #[test]
    fn the_mesh_and_members_see_everything_guests_only_their_country() {
        assert_eq!(scope_of(&Request::new(())), MapScope::All);
    }
}

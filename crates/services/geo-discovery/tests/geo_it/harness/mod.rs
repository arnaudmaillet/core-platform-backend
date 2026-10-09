//! Integration harness: boots the shared infra, wires a real geo-discovery graph
//! against it through the production composition root, and exposes the buses for
//! assertions. Indexing is driven through the command bus (no Kafka); reads go
//! through the viewport query.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, Envelope, QueryBus};
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;

use geo_discovery::app::{App, Backends};
use geo_discovery::application::command::{
    ApplyMapVisibilityCommand, IndexPostCommand, UpdateViralityWithTilesCommand,
};
use geo_discovery::application::port::{AudienceGate, GemSpend, GemWallet, ResidenceDirectory};
use geo_discovery::domain::value_object::CountryCode;
use geo_discovery::application::query::get_geo_timeline::{GetGeoTimelineQuery, GetGeoTimelineResult};
use geo_discovery::application::query::query_tile::{QueryTileQuery, QueryTileResult};
use geo_discovery::config::GeoDiscoveryConfig;
use geo_discovery::domain::value_object::{GeoCoordinate, H3Index, H3Resolution};
use geo_discovery::error::GeoDiscoveryError;
use geo_discovery::infrastructure::persistence::ScyllaTileRepository;

pub use geo_discovery::domain::value_object::{AuthorAccess, ContentAccess, MapScope, VisibilityChange, Viewer};

pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (ScyllaDB +
/// Redis spatial-index write visibility).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "geo_discovery";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// Zoom 15 → H3 R9, whose virality floor is 0 — so the spatial filter, not a
/// score threshold, governs what a query returns.
pub const ZOOM_R9: i32 = 15;

/// The wallet (#665): 100 gems per account unless set; a spend is keyed once.
#[derive(Default)]
pub struct ScriptedWallet {
    pub gems:  Mutex<HashMap<Uuid, i64>>,
    pub spent: Mutex<Vec<(Uuid, String, i64)>>,
}

#[async_trait::async_trait]
impl GemWallet for ScriptedWallet {
    async fn gems(&self, account: Uuid) -> Result<i64, GeoDiscoveryError> {
        Ok(*self.gems.lock().unwrap().get(&account).unwrap_or(&100))
    }
    async fn spend_for_country(&self, account: Uuid, _: CountryCode, amount: i64, key: &str, _: Option<&str>) -> Result<GemSpend, GeoDiscoveryError> {
        let mut gems = self.gems.lock().unwrap();
        let balance = gems.entry(account).or_insert(100);
        let mut spent = self.spent.lock().unwrap();
        if spent.iter().any(|(a, k, _)| *a == account && k == key) {
            return Ok(GemSpend { spent: true, gems: *balance });
        }
        if *balance < amount {
            return Ok(GemSpend { spent: false, gems: *balance });
        }
        *balance -= amount;
        spent.push((account, key.to_owned(), amount));
        Ok(GemSpend { spent: true, gems: *balance })
    }
}

/// Accounts' countries of residence (#665).
#[derive(Default)]
pub struct ScriptedResidence(pub Mutex<HashMap<Uuid, CountryCode>>);

#[async_trait::async_trait]
impl ResidenceDirectory for ScriptedResidence {
    async fn residence(&self, account: Uuid) -> Result<Option<CountryCode>, GeoDiscoveryError> {
        Ok(self.0.lock().unwrap().get(&account).copied())
    }
}

/// A scripted audience check: every author `Visible` unless set; can fail.
#[derive(Default)]
pub struct ScriptedGate {
    access:    Mutex<HashMap<Uuid, ContentAccess>>,
    /// `(follows, mutual)` per author; absent: neither.
    relations: Mutex<HashMap<Uuid, (bool, bool)>>,
    down:      Mutex<bool>,
}

impl ScriptedGate {
    pub fn set(&self, author: Uuid, access: ContentAccess) {
        self.access.lock().unwrap().insert(author, access);
    }
    /// How the reader stands to `author` (#657).
    pub fn relate(&self, author: Uuid, follows: bool, mutual: bool) {
        self.relations.lock().unwrap().insert(author, (follows, mutual));
    }
    pub fn set_down(&self, down: bool) {
        *self.down.lock().unwrap() = down;
    }
}

#[async_trait::async_trait]
impl AudienceGate for ScriptedGate {
    async fn access(&self, _: &[String], authors: &[Uuid]) -> Result<HashMap<Uuid, AuthorAccess>, GeoDiscoveryError> {
        if *self.down.lock().unwrap() {
            return Err(GeoDiscoveryError::AccessCheckUnavailable { reason: "scripted outage".into() });
        }
        let (access, relations) = (self.access.lock().unwrap(), self.relations.lock().unwrap());
        Ok(authors
            .iter()
            .map(|a| {
                let (follows, mutual) = relations.get(a).copied().unwrap_or_default();
                let content = access.get(a).copied().unwrap_or(ContentAccess::Visible);
                (*a, AuthorAccess { content, follows, mutual })
            })
            .collect())
    }
}

/// A fully-wired geo-discovery service bound to ephemeral infra, plus the buses.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub gate:        Arc<ScriptedGate>,
    /// Country access from location (private addresses count as the claim).
    pub country_access: Arc<geo_discovery::application::country_access::ResolveCountryAccess>,
    /// Direct handle on the durable card store, for row-level assertions.
    pub tiles:       ScyllaTileRepository,
    /// The authors' location sharing, written as the profile-events worker does.
    pub location:    geo_discovery::infrastructure::persistence::ScyllaLocationSettingsStore,
    /// The country ladder (#665), over the real Redis activity store.
    pub standings:   Arc<geo_discovery::application::country_standings::CountryStandings>,
    /// A member's countries and unlocks (#665), over the real Scylla store.
    pub unlocking:   Arc<geo_discovery::application::country_unlocks::CountryUnlocking>,
    pub wallet:      Arc<ScriptedWallet>,
    pub residence:   Arc<ScriptedResidence>,
}

impl TestHarness {
    /// Boots/reuses the shared containers, applies migrations, and assembles the
    /// service graph (no Kafka workers).
    pub async fn start() -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let gate = Arc::new(ScriptedGate::default());
        let wallet = Arc::new(ScriptedWallet::default());
        let residence = Arc::new(ScriptedResidence::default());
        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
            redis: RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
            kafka: None,
            audience: Arc::clone(&gate) as _,
            geo_ip:   Arc::new(geo_discovery::infrastructure::geoip::MmdbGeoIp::load(
                None,
                geo_discovery::infrastructure::geoip::PrivateNetworkCountry::AsClaimed,
            )),
            wallet:    Arc::clone(&wallet) as _,
            residence: Arc::clone(&residence) as _,
        };

        let app = App::build(GeoDiscoveryConfig::from_env(), backends)
            .await
            .expect("integration: build geo-discovery app");

        let tiles = ScyllaTileRepository::new(Arc::clone(&app.scylla));
        let location =
            geo_discovery::infrastructure::persistence::ScyllaLocationSettingsStore::new(Arc::clone(&app.scylla));
        Self {
            location,
            command_bus: app.command_bus,
            query_bus: app.query_bus,
            gate,
            country_access: app.country_access,
            standings: app.standings,
            unlocking: app.unlocking,
            wallet,
            residence,
            tiles,
        }
    }

    /// Indexes a post at `(lat, lng)` with the given virality, returning its uuid.
    pub async fn index_post(&self, lat: f64, lng: f64, virality: f64) -> Uuid {
        self.index_post_full(lat, lng, virality, "", "").await
    }

    /// Indexes a post with an explicit caption and thumbnail — exercises the
    /// Focus (GetGeoTimeline) hydration path.
    pub async fn index_post_full(
        &self,
        lat:       f64,
        lng:       f64,
        virality:  f64,
        caption:   &str,
        thumbnail: &str,
    ) -> Uuid {
        self.index_post_by(Uuid::now_v7(), lat, lng, virality, caption, thumbnail).await
    }

    /// Indexes a post with a given id (to replay events out of order).
    pub async fn index_post_with_id(&self, post: Uuid, lat: f64, lng: f64) {
        let cmd = IndexPostCommand {
            post_id:           post.to_string(),
            author_id:         Uuid::now_v7().to_string(),
            author_handle:     "tester".to_owned(),
            author_avatar_url: String::new(),
            thumbnail_url:     String::new(),
            caption:           String::new(),
            lat,
            lng,
            virality_score:    5.0,
            published_at_ms:   chrono::Utc::now().timestamp_millis(),
            retention_secs:    None,
            author_tier:       0,
        };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("index_post");
    }

    /// Indexes a post at `(lat, lng)` that the map retains for `retention_secs`.
    pub async fn index_post_retained(&self, lat: f64, lng: f64, retention_secs: u64) -> Uuid {
        let post_uuid = Uuid::now_v7();
        let cmd = IndexPostCommand {
            post_id:           post_uuid.to_string(),
            author_id:         Uuid::now_v7().to_string(),
            author_handle:     "tester".to_owned(),
            author_avatar_url: String::new(),
            thumbnail_url:     String::new(),
            caption:           String::new(),
            lat,
            lng,
            virality_score:    5.0,
            published_at_ms:   chrono::Utc::now().timestamp_millis(),
            retention_secs:    Some(retention_secs),
            author_tier:       0,
        };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("index_post");
        post_uuid
    }

    /// Indexes a post at `(lat, lng)` published at `published_at_ms` (a late
    /// or repeated `post.published`).
    pub async fn index_post_published_at(&self, lat: f64, lng: f64, published_at_ms: i64) -> Uuid {
        let post_uuid = Uuid::now_v7();
        let cmd = IndexPostCommand {
            post_id:           post_uuid.to_string(),
            author_id:         Uuid::now_v7().to_string(),
            author_handle:     "tester".to_owned(),
            author_avatar_url: String::new(),
            thumbnail_url:     String::new(),
            caption:           String::new(),
            lat,
            lng,
            virality_score:    5.0,
            published_at_ms,
            retention_secs:    None,
            author_tier:       0,
        };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("index_post");
        post_uuid
    }

    /// Re-scores a post at `(lat, lng)`, as the score updater does.
    pub async fn update_score(&self, post: Uuid, lat: f64, lng: f64, score: f64) {
        let coord = GeoCoordinate::new(lat, lng).expect("coordinate");
        let tile = |res| H3Index::encode(&coord, res).as_i64();
        let cmd = UpdateViralityWithTilesCommand {
            post_id:     post.to_string(),
            new_score:   score,
            h3_index_r5: tile(H3Resolution::R5),
            h3_index_r7: tile(H3Resolution::R7),
            h3_index_r9: tile(H3Resolution::R9),
        };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("update_score");
    }

    /// Indexes a post by `author`.
    pub async fn index_post_by(
        &self,
        author:    Uuid,
        lat:       f64,
        lng:       f64,
        virality:  f64,
        caption:   &str,
        thumbnail: &str,
    ) -> Uuid {
        let post_uuid = Uuid::now_v7();
        let cmd = IndexPostCommand {
            post_id:           post_uuid.to_string(),
            author_id:         author.to_string(),
            author_handle:     "tester".to_owned(),
            author_avatar_url: String::new(),
            thumbnail_url:     thumbnail.to_owned(),
            caption:           caption.to_owned(),
            lat,
            lng,
            virality_score:    virality,
            published_at_ms:   chrono::Utc::now().timestamp_millis(),
            retention_secs:    None,
            author_tier:       0,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("index_post");
        post_uuid
    }

    /// Applies a map visibility change, as the visibility worker does.
    pub async fn change_visibility(&self, post: Uuid, change: VisibilityChange) {
        let cmd = ApplyMapVisibilityCommand { post_id: post.to_string(), change };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("visibility");
    }

    /// Focus path: hydrates the given post ids into full cards (mesh caller).
    pub async fn get_timeline(&self, post_ids: &[Uuid]) -> GetGeoTimelineResult {
        self.try_get_timeline_as(post_ids, Viewer::Internal).await.expect("get_geo_timeline")
    }

    /// Focus path as `viewer` (`Err` on an audience-check outage).
    pub async fn try_get_timeline_as(
        &self,
        post_ids: &[Uuid],
        viewer:   Viewer,
    ) -> Result<GetGeoTimelineResult, cqrs::CqrsError> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetGeoTimelineQuery { post_ids: post_ids.to_vec(), viewer, scope: MapScope::All },
            ))
            .await
    }

    /// The post ids a Radar query around `(lat, lng)` returns to `viewer`.
    pub async fn pins_near_as(&self, lat: f64, lng: f64, viewer: Viewer) -> HashSet<Uuid> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                QueryTileQuery {
                    sw_lat: lat - 0.01, sw_lng: lng - 0.01, ne_lat: lat + 0.01, ne_lng: lng + 0.01,
                    zoom_level: ZOOM_R9, viewer, scope: MapScope::All,
                },
            ))
            .await
            .map(|r: QueryTileResult| r.pins.into_iter().map(|p| p.post_id).collect())
            .expect("query_tile")
    }

    /// The post ids a Radar query around `(lat, lng)` returns for `scope`.
    pub async fn pins_near_in(&self, lat: f64, lng: f64, scope: MapScope) -> HashSet<Uuid> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                QueryTileQuery {
                    sw_lat: lat - 0.01, sw_lng: lng - 0.01, ne_lat: lat + 0.01, ne_lng: lng + 0.01,
                    zoom_level: ZOOM_R9, viewer: Viewer::Profiles(vec![]), scope,
                },
            ))
            .await
            .map(|r: QueryTileResult| r.pins.into_iter().map(|p| p.post_id).collect())
            .expect("query_tile")
    }

    /// The card ids the Focus path returns for `scope`.
    pub async fn cards_in(&self, post_ids: &[Uuid], scope: MapScope) -> HashSet<Uuid> {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                GetGeoTimelineQuery { post_ids: post_ids.to_vec(), viewer: Viewer::Profiles(vec![]), scope },
            ))
            .await
            .map(|r: GetGeoTimelineResult| r.cards.into_iter().map(|c| c.post_id).collect())
            .expect("get_geo_timeline")
    }

    /// Queries a viewport box (`sw` < `ne`) at the given zoom.
    pub async fn query_viewport(
        &self,
        sw_lat: f64,
        sw_lng: f64,
        ne_lat: f64,
        ne_lng: f64,
        zoom:   i32,
    ) -> QueryTileResult {
        self.query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                QueryTileQuery { sw_lat, sw_lng, ne_lat, ne_lng, zoom_level: zoom, viewer: Viewer::Internal, scope: MapScope::All },
            ))
            .await
            .expect("query_tile")
    }
}

/// Whether a Radar query result contains a pin for `post_uuid`.
pub fn result_contains(result: &QueryTileResult, post_uuid: &Uuid) -> bool {
    result.pins.iter().any(|p| p.post_id == *post_uuid)
}

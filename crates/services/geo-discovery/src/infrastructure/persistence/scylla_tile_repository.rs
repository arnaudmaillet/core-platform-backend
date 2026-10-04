use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::TileRepository;
use crate::domain::entity::MapPostCard;
use crate::domain::value_object::{
    H3Index, H3Resolution, MapVisibility, PostId, RetentionTtl, Suppression,
};
use crate::error::GeoDiscoveryError;
use crate::infrastructure::persistence::model::{MapCardRow, PostTileRow};

fn scylla_err(e: scylla::errors::ExecutionError) -> GeoDiscoveryError {
    GeoDiscoveryError::Scylla(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> GeoDiscoveryError {
    GeoDiscoveryError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

pub struct ScyllaTileRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaTileRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn strict_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Strict)
                .clone()
                .into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }

    fn fast_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Fast)
                .clone()
                .into_handle_with_label("fast".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }
}

#[async_trait]
impl TileRepository for ScyllaTileRepository {
    async fn insert_tile_entry(
        &self,
        h3_index:        H3Index,
        resolution:      H3Resolution,
        post_id:         &PostId,
        published_at_ms: i64,
        ttl:             RetentionTtl,
    ) -> Result<(), GeoDiscoveryError> {
        let stmt = self.strict_stmt(
            "INSERT INTO geo_discovery.posts_by_tile \
             (h3_index, resolution, published_at, post_id) \
             VALUES (?, ?, ?, ?) \
             USING TTL ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    h3_index.as_i64(),
                    resolution.as_i8(),
                    CqlTimestamp(published_at_ms),
                    post_id.as_uuid(),
                    ttl.as_scylla_ttl(),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn upsert_card(
        &self,
        card: &MapPostCard,
        ttl:  RetentionTtl,
    ) -> Result<(), GeoDiscoveryError> {
        let expires_at_ms = card.published_at_ms + ttl.as_duration().as_millis() as i64;

        let stmt = self.strict_stmt(
            "INSERT INTO geo_discovery.map_post_cards \
             (post_id, author_id, author_handle, author_avatar_url, thumbnail_url, \
              caption, h3_index_r7, virality_score, published_at, expires_at, author_tier, \
              lat, lng) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             USING TTL ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    card.post_id,
                    card.author_id,
                    card.author_handle.as_str(),
                    card.author_avatar_url.as_str(),
                    card.thumbnail_url.as_str(),
                    card.caption.as_str(),
                    card.h3_index_r7,
                    card.virality_score,
                    CqlTimestamp(card.published_at_ms),
                    CqlTimestamp(expires_at_ms),
                    card.author_tier as i8,
                    card.lat,
                    card.lng,
                    ttl.as_scylla_ttl(),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn update_card_score(
        &self,
        post_id: &PostId,
        score:   f32,
    ) -> Result<(), GeoDiscoveryError> {
        let stmt = self.strict_stmt(
            "UPDATE geo_discovery.map_post_cards \
             SET virality_score = ? \
             WHERE post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (score, post_id.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn get_card(
        &self,
        post_id: &PostId,
    ) -> Result<Option<MapPostCard>, GeoDiscoveryError> {
        // A suppressed card is absent from every read path.
        Ok(self
            .get_card_with_visibility(post_id)
            .await?
            .filter(|(_, visibility, _)| visibility.suppression == Suppression::None)
            .and_then(|(card, _, _)| card))
    }

    async fn get_card_with_visibility(
        &self,
        post_id: &PostId,
    ) -> Result<Option<(Option<MapPostCard>, MapVisibility, Option<i32>)>, GeoDiscoveryError> {
        // TTL(author_handle): the row's real remaining life (author_handle is
        // written once, with the row), not published_at + retention.
        let stmt = self.fast_stmt(
            "SELECT post_id, author_id, author_handle, author_avatar_url, thumbnail_url, \
             caption, h3_index_r7, virality_score, published_at, expires_at, author_tier, \
             suppressed, moderation_version, lat, lng, TTL(author_handle) AS ttl_secs \
             FROM geo_discovery.map_post_cards \
             WHERE post_id = ?",
        );
        let result = self.client
            .session
            .execute_unpaged(stmt, (post_id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("get_card:rows", e))?;

        let mut rows = result
            .rows::<MapCardRow>()
            .map_err(|e| row_err("get_card:iter", e))?;

        match rows.next() {
            Some(Ok(row)) => {
                let visibility = MapVisibility {
                    suppression:        Suppression::from_tinyint(row.suppressed),
                    moderation_version: row.moderation_version.unwrap_or(0),
                };
                Ok(Some((row.card(), visibility, row.ttl_secs)))
            }
            Some(Err(e))  => Err(row_err("get_card:deser", e)),
            None          => Ok(None),
        }
    }

    async fn set_visibility(
        &self,
        post_id:    &PostId,
        visibility: MapVisibility,
        ttl:        RetentionTtl,
    ) -> Result<(), GeoDiscoveryError> {
        // USING TTL = the row's remaining life, so the cells this writes expire
        // with the row instead of keeping it alive.
        let stmt = self.strict_stmt(
            "UPDATE geo_discovery.map_post_cards USING TTL ? \
             SET suppressed = ?, moderation_version = ? \
             WHERE post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    ttl.as_scylla_ttl(),
                    visibility.suppression.as_tinyint(),
                    visibility.moderation_version,
                    post_id.as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn list_tile_post_ids(
        &self,
        h3_index:   H3Index,
        resolution: H3Resolution,
        limit:      i32,
    ) -> Result<Vec<Uuid>, GeoDiscoveryError> {
        let stmt = self.fast_stmt(
            "SELECT post_id \
             FROM geo_discovery.posts_by_tile \
             WHERE h3_index = ? AND resolution = ? \
             LIMIT ?",
        );
        let rows = self.client
            .session
            .execute_unpaged(stmt, (h3_index.as_i64(), resolution.as_i8(), limit))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("list_tile:rows", e))?
            .rows::<PostTileRow>()
            .map_err(|e| row_err("list_tile:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("list_tile:deser", e))?;

        Ok(rows.into_iter().map(|r| r.post_id).collect())
    }
}

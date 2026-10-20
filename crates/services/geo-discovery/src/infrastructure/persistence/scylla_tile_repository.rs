use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{PlaceEntry, TileRepository, COUNTRY_DAYS};
use crate::domain::entity::MapPostCard;
use crate::domain::value_object::{
    CountryCode, H3Index, H3Resolution, MapVisibility, Place, PostId, RetentionTtl, Suppression,
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
              lat, lng, post_kind, cover_asset_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
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
                    card.format.code(),
                    card.cover_asset_id,
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
        // The score cell must expire with the card: a cell without a TTL would
        // outlive the row's other cells and keep a score-only row alive forever.
        // TTL(author_handle) is the card's remaining life; NULL means no card
        // (none, expired, or a tombstone), where an UPDATE would create one.
        let read = self.fast_stmt(
            "SELECT TTL(author_handle) AS ttl_secs \
             FROM geo_discovery.map_post_cards \
             WHERE post_id = ?",
        );
        let ttl_secs = self.client
            .session
            .execute_unpaged(read, (post_id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("update_card_score:rows", e))?
            .maybe_first_row::<(Option<i32>,)>()
            .map_err(|e| row_err("update_card_score:deser", e))?
            .and_then(|(ttl,)| ttl);
        let Some(remaining) = ttl_secs.filter(|s| *s > 0) else {
            return Ok(());
        };

        let stmt = self.strict_stmt(
            "UPDATE geo_discovery.map_post_cards USING TTL ? \
             SET virality_score = ? \
             WHERE post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (remaining, score, post_id.as_uuid()))
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
             suppressed, moderation_version, lat, lng, post_kind, cover_asset_id, TTL(author_handle) AS ttl_secs \
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

    async fn insert_country_entry(
        &self,
        country:         CountryCode,
        post_id:         &PostId,
        published_at_ms: i64,
        ttl:             RetentionTtl,
    ) -> Result<(), GeoDiscoveryError> {
        let stmt = self.strict_stmt(
            "INSERT INTO geo_discovery.posts_by_country (country, day, published_at, post_id) \
             VALUES (?, ?, ?, ?) USING TTL ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (country.as_str(), cql_date(day_of(published_at_ms)), CqlTimestamp(published_at_ms), post_id.as_uuid(), ttl.as_scylla_ttl()),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn list_place(
        &self,
        place: &Place,
        after: Option<PlaceEntry>,
        limit: i32,
    ) -> Result<Vec<PlaceEntry>, GeoDiscoveryError> {
        let limit = limit.max(1);
        match place {
            Place::Cell { index, resolution } => {
                let key = (index.as_i64(), resolution.as_i8());
                self.page_partition("posts_by_tile", "h3_index = ? AND resolution = ?", key, after, limit).await
            }
            Place::Country(country) => {
                let today = day_of(chrono::Utc::now().timestamp_millis());
                let first = after.map_or(today, |a| day_of(a.published_at_ms));
                let mut entries = Vec::new();
                let mut day = first;
                while entries.len() < limit as usize && (today - day).num_days() < COUNTRY_DAYS {
                    let left = limit - entries.len() as i32;
                    let after_in_day = after.filter(|_| day == first);
                    entries.extend(
                        self.page_partition("posts_by_country", "country = ? AND day = ?", (country.as_str(), cql_date(day)), after_in_day, left)
                            .await?,
                    );
                    day = day.pred_opt().expect("a day before");
                }
                Ok(entries)
            }
        }
    }
}

impl ScyllaTileRepository {
    /// One partition's posts newest first, strictly after `after`: the rest of
    /// its millisecond (ids ascend within it), then the older ones.
    async fn page_partition<A, B>(
        &self,
        table:  &str,
        key:    &str,
        (a, b): (A, B),
        after:  Option<PlaceEntry>,
        limit:  i32,
    ) -> Result<Vec<PlaceEntry>, GeoDiscoveryError>
    where
        A: scylla::serialize::value::SerializeValue + Clone,
        B: scylla::serialize::value::SerializeValue + Clone,
    {
        let base = format!("SELECT published_at, post_id FROM geo_discovery.{table} WHERE {key}");
        let rows = match after {
            None => self.place_rows(&format!("{base} LIMIT ?"), (a, b, limit)).await?,
            Some(after) => {
                let at = CqlTimestamp(after.published_at_ms);
                let mut rows = self
                    .place_rows(
                        &format!("{base} AND published_at = ? AND post_id > ? LIMIT ?"),
                        (a.clone(), b.clone(), at, after.post_id, limit),
                    )
                    .await?;
                let left = limit - rows.len() as i32;
                if left > 0 {
                    rows.extend(self.place_rows(&format!("{base} AND published_at < ? LIMIT ?"), (a, b, at, left)).await?);
                }
                rows
            }
        };
        Ok(rows.into_iter().map(|(at, post_id)| PlaceEntry { post_id, published_at_ms: at.0 }).collect())
    }

    async fn place_rows(
        &self,
        cql:    &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Vec<(CqlTimestamp, Uuid)>, GeoDiscoveryError> {
        self.client
            .session
            .execute_unpaged(self.fast_stmt(cql), values)
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("list_place:rows", e))?
            .rows::<(CqlTimestamp, Uuid)>()
            .map_err(|e| row_err("list_place:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("list_place:deser", e))
    }
}

/// The UTC day of a millisecond timestamp (`posts_by_country`'s bucket).
fn day_of(ms: i64) -> chrono::NaiveDate {
    chrono::DateTime::from_timestamp_millis(ms).unwrap_or_default().date_naive()
}

/// A CQL `date`: days since the Unix epoch, offset by 2^31.
fn cql_date(day: chrono::NaiveDate) -> scylla::value::CqlDate {
    let days = (day - chrono::NaiveDate::default()).num_days();
    scylla::value::CqlDate((days + (1_i64 << 31)) as u32)
}


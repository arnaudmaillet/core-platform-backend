use async_trait::async_trait;
use postgres_storage::TransactionManager;
use sqlx::types::Json;

use crate::application::port::AssetRepository;
use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, ContentHash, OwnerId, UploadKey};
use crate::error::MediaError;

use super::storage_err;

/// A row carrying just the stored aggregate document.
#[derive(Debug, sqlx::FromRow)]
struct AssetDocRow {
    doc: Json<Asset>,
}

/// PostgreSQL adapter for [`AssetRepository`].
#[derive(Clone)]
pub struct PgAssetRepository {
    tx: TransactionManager,
}

impl PgAssetRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl AssetRepository for PgAssetRepository {
    async fn save(&self, asset: &Asset) -> Result<(), MediaError> {
        let content_hash = asset.content_hash().map(|h| h.as_str().to_owned());
        sqlx::query(
            r#"
            INSERT INTO assets (id, owner_id, kind, state, content_hash, created_at, updated_at, doc, purge_after)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                state        = EXCLUDED.state,
                content_hash = EXCLUDED.content_hash,
                updated_at   = EXCLUDED.updated_at,
                doc          = EXCLUDED.doc,
                purge_after  = EXCLUDED.purge_after
            "#,
        )
        .bind(asset.id().as_uuid())
        .bind(asset.owner_id().as_uuid())
        .bind(asset.kind().as_str())
        .bind(asset.state().as_str())
        .bind(content_hash)
        .bind(asset.created_at())
        .bind(asset.updated_at())
        .bind(Json(asset))
        .bind(asset.purge_after())
        .execute(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(())
    }

    async fn insert_keyed(&self, asset: &Asset, key: &UploadKey) -> Result<Option<AssetId>, MediaError> {
        // The partial unique index (owner, key) over live assets arbitrates: a
        // racing insert waits for the first to commit, then does nothing. If
        // the holder is deleted between the conflict and the read, the key is
        // free again: insert once more.
        for _ in 0..2 {
            let inserted = sqlx::query(
                r#"
                INSERT INTO assets (id, owner_id, kind, state, content_hash, created_at, updated_at, doc, purge_after, upload_key)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (owner_id, upload_key) WHERE upload_key IS NOT NULL AND state <> 'deleted' DO NOTHING
                "#,
            )
            .bind(asset.id().as_uuid())
            .bind(asset.owner_id().as_uuid())
            .bind(asset.kind().as_str())
            .bind(asset.state().as_str())
            .bind(asset.content_hash().map(|h| h.as_str().to_owned()))
            .bind(asset.created_at())
            .bind(asset.updated_at())
            .bind(Json(asset))
            .bind(asset.purge_after())
            .bind(key.as_str())
            .execute(self.tx.pool())
            .await
            .map_err(storage_err)?
            .rows_affected();
            if inserted == 1 {
                return Ok(None);
            }
            if let Some(holder) = self.find_by_upload_key(&asset.owner_id(), key).await? {
                return Ok(Some(holder));
            }
        }
        Err(MediaError::ConcurrentModification)
    }

    async fn find_by_upload_key(&self, owner: &OwnerId, key: &UploadKey) -> Result<Option<AssetId>, MediaError> {
        let holder: Option<(uuid::Uuid,)> = sqlx::query_as(
            "SELECT id FROM assets WHERE owner_id = $1 AND upload_key = $2 AND state <> 'deleted'",
        )
        .bind(owner.as_uuid())
        .bind(key.as_str())
        .fetch_optional(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(holder.map(|(id,)| AssetId::from_uuid(id)))
    }

    async fn find_by_id(&self, id: &AssetId) -> Result<Option<Asset>, MediaError> {
        let row = sqlx::query_as::<_, AssetDocRow>("SELECT doc FROM assets WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(self.tx.pool())
            .await
            .map_err(storage_err)?;
        Ok(row.map(|r| r.doc.0))
    }

    async fn find_by_content_hash(&self, hash: &ContentHash) -> Result<Vec<Asset>, MediaError> {
        let rows = sqlx::query_as::<_, AssetDocRow>(
            "SELECT doc FROM assets WHERE content_hash = $1 AND state <> 'deleted'",
        )
        .bind(hash.as_str())
        .fetch_all(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(|r| r.doc.0).collect())
    }

    async fn list_by_owner(
        &self,
        owner: &OwnerId,
        limit: i64,
        after: Option<&AssetId>,
    ) -> Result<Vec<Asset>, MediaError> {
        let rows = sqlx::query_as::<_, AssetDocRow>(
            "SELECT doc FROM assets \
             WHERE owner_id = $1 AND state <> 'deleted' AND ($2::uuid IS NULL OR id > $2) \
             ORDER BY id LIMIT $3",
        )
        .bind(owner.as_uuid())
        .bind(after.map(AssetId::as_uuid))
        .bind(limit.clamp(1, 500))
        .fetch_all(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(|r| r.doc.0).collect())
    }

    async fn due_for_purge(&self, now: chrono::DateTime<chrono::Utc>, limit: i64) -> Result<Vec<Asset>, MediaError> {
        let rows = sqlx::query_as::<_, AssetDocRow>(
            "SELECT doc FROM assets WHERE purge_after <= $1 AND state <> 'deleted' \
             AND NOT COALESCE((doc->>'legal_hold')::boolean, false) ORDER BY purge_after LIMIT $2",
        )
        .bind(now)
        .bind(limit.clamp(1, 500))
        .fetch_all(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(|r| r.doc.0).collect())
    }

    async fn find_ready_by_content_hash(
        &self,
        hash: &ContentHash,
    ) -> Result<Option<Asset>, MediaError> {
        let row = sqlx::query_as::<_, AssetDocRow>(
            "SELECT doc FROM assets WHERE content_hash = $1 AND state = 'ready' LIMIT 1",
        )
        .bind(hash.as_str())
        .fetch_optional(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(row.map(|r| r.doc.0))
    }
}

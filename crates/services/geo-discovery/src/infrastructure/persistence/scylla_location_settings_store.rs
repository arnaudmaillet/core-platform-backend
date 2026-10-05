use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::LocationSettingsStore;
use crate::domain::value_object::{LocationAudience, LocationSharing};
use crate::error::GeoDiscoveryError;

/// Scylla adapter for [`LocationSettingsStore`] (`geo_discovery.location_settings`).
pub struct ScyllaLocationSettingsStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaLocationSettingsStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn stmt(&self, cql: &str, kind: ScyllaProfileKind, label: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(kind).clone().into_handle_with_label(label.to_string()),
        ));
        s.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        s
    }
}

fn scylla_err(e: scylla::errors::ExecutionError) -> GeoDiscoveryError {
    GeoDiscoveryError::Scylla(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> GeoDiscoveryError {
    GeoDiscoveryError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[async_trait]
impl LocationSettingsStore for ScyllaLocationSettingsStore {
    async fn set(&self, author_id: Uuid, sharing: LocationSharing) -> Result<(), GeoDiscoveryError> {
        let stmt = self.stmt(
            "INSERT INTO geo_discovery.location_settings (author_id, ghost, city, audience) VALUES (?, ?, ?, ?)",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (author_id, sharing.ghost, sharing.city, sharing.audience.as_tinyint()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn get_many(&self, author_ids: &[Uuid]) -> Result<HashMap<Uuid, LocationSharing>, GeoDiscoveryError> {
        #[derive(DeserializeRow)]
        struct Row {
            author_id: Uuid,
            ghost:     Option<bool>,
            city:      Option<bool>,
            audience:  Option<i8>,
        }
        let mut out = HashMap::new();
        // Bounded IN lists: a page of pins has a few hundred authors at most.
        for chunk in author_ids.chunks(100) {
            let stmt = self.stmt(
                "SELECT author_id, ghost, city, audience FROM geo_discovery.location_settings WHERE author_id IN ?",
                ScyllaProfileKind::Fast,
                "fast",
            );
            let rows = self
                .client
                .session
                .execute_unpaged(stmt, (chunk.to_vec(),))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("location_settings:rows", e))?;
            for row in rows.rows::<Row>().map_err(|e| row_err("location_settings:iter", e))? {
                let row = row.map_err(|e| row_err("location_settings:deser", e))?;
                let sharing = LocationSharing {
                    ghost:    row.ghost.unwrap_or(false),
                    city:     row.city.unwrap_or(false),
                    audience: LocationAudience::from_tinyint(row.audience),
                };
                if !sharing.is_default() {
                    out.insert(row.author_id, sharing);
                }
            }
        }
        Ok(out)
    }
}

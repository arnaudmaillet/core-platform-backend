use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla_storage::ScyllaClient;
use uuid::Uuid;

use crate::application::port::{PresenceSettings, PresenceSettingsStore};
use crate::domain::value_object::ProfileId;
use crate::error::ChatError;
use crate::infrastructure::persistence::statement::{fast, row_err, scylla_err, strict};

#[derive(DeserializeRow)]
struct Row {
    profile_id:      Uuid,
    activity_status: Option<bool>,
    read_receipts:   Option<bool>,
}

/// ScyllaDB adapter for the members' presence settings (`chat.presence_settings`).
pub struct ScyllaPresenceSettingsStore {
    client: Arc<ScyllaClient>,
}

/// Scylla's default `max_partition_key_restrictions_per_query`.
const MAX_KEYS_PER_QUERY: usize = 100;

impl ScyllaPresenceSettingsStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    async fn get_chunk(&self, profiles: &[ProfileId]) -> Result<HashMap<ProfileId, PresenceSettings>, ChatError> {
        let stmt = fast(
            &self.client,
            "SELECT profile_id, activity_status, read_receipts FROM chat.presence_settings WHERE profile_id IN ?",
        );
        let ids: Vec<Uuid> = profiles.iter().map(ProfileId::as_uuid).collect();
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (ids,))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("presence_settings", e))?;
        let mut out = HashMap::new();
        for row in rows.rows::<Row>().map_err(|e| row_err("presence_settings", e))? {
            let row = row.map_err(|e| row_err("presence_settings", e))?;
            out.insert(
                ProfileId::from_uuid(row.profile_id),
                PresenceSettings {
                    activity_status: row.activity_status.unwrap_or(true),
                    read_receipts:   row.read_receipts.unwrap_or(true),
                },
            );
        }
        Ok(out)
    }
}

#[async_trait]
impl PresenceSettingsStore for ScyllaPresenceSettingsStore {
    async fn set(&self, profile: &ProfileId, settings: PresenceSettings) -> Result<(), ChatError> {
        let stmt = strict(
            &self.client,
            "INSERT INTO chat.presence_settings (profile_id, activity_status, read_receipts) VALUES (?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(), settings.activity_status, settings.read_receipts))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn get_many(&self, profiles: &[ProfileId]) -> Result<HashMap<ProfileId, PresenceSettings>, ChatError> {
        // Scylla caps an IN at 100 partition keys; a group has up to 500 members.
        let chunks = profiles.chunks(MAX_KEYS_PER_QUERY).map(|chunk| self.get_chunk(chunk));
        let mut out = HashMap::with_capacity(profiles.len());
        for part in futures::future::try_join_all(chunks).await? {
            out.extend(part);
        }
        Ok(out)
    }
}

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{DeviceRegistry, PreferenceStore};
use crate::domain::device::{Device, DevicePlatform, PushEnvironment};
use crate::domain::preferences::NotificationPreferences;
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

fn scylla_err(e: scylla::errors::ExecutionError) -> NotificationError {
    NotificationError::Scylla(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> NotificationError {
    NotificationError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

/// ScyllaDB-backed push devices and preferences (`notification.push_devices`,
/// `push_device_tokens`, `notification_preferences`).
pub struct ScyllaPushSettings {
    client: Arc<ScyllaClient>,
}

#[derive(DeserializeRow)]
struct DeviceRow {
    device_id:     String,
    push_token:    Option<String>,
    platform:      Option<String>,
    environment:   Option<String>,
    registered_at: Option<CqlTimestamp>,
}

impl DeviceRow {
    fn into_device(self) -> Option<Device> {
        Some(Device {
            device_id:     self.device_id,
            token:         self.push_token?,
            platform:      DevicePlatform::parse(self.platform.as_deref()?)?,
            environment:   PushEnvironment::parse(self.environment.as_deref()?)?,
            registered_at: self.registered_at.and_then(|t| Utc.timestamp_millis_opt(t.0).single()).unwrap_or_default(),
        })
    }
}

#[derive(DeserializeRow)]
struct TokenRow {
    profile_id: Uuid,
    device_id:  Option<String>,
    account_id: Option<String>,
}

impl ScyllaPushSettings {
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

    fn strict(&self, cql: &str) -> Statement {
        self.stmt(cql, ScyllaProfileKind::Strict, "strict")
    }

    fn fast(&self, cql: &str) -> Statement {
        self.stmt(cql, ScyllaProfileKind::Fast, "fast")
    }

    async fn exec(&self, stmt: Statement, values: impl scylla::serialize::row::SerializeRow) -> Result<(), NotificationError> {
        self.client.session.execute_unpaged(stmt, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn device(&self, profile: &ProfileId, device_id: &str) -> Result<Option<DeviceRow>, NotificationError> {
        let stmt = self.fast(
            "SELECT device_id, push_token, platform, environment, registered_at FROM notification.push_devices \
             WHERE profile_id = ? AND device_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(), device_id))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("push_device", e))?
            .maybe_first_row::<DeviceRow>()
            .map_err(|e| row_err("push_device", e))
    }

    async fn forget(&self, profile: Uuid, device_id: &str, token: Option<&str>) -> Result<(), NotificationError> {
        self.exec(
            self.strict("DELETE FROM notification.push_devices WHERE profile_id = ? AND device_id = ?"),
            (profile, device_id),
        )
        .await?;
        if let Some(token) = token {
            self.exec(
                self.strict("DELETE FROM notification.push_device_tokens WHERE push_token = ? AND profile_id = ?"),
                (token, profile),
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DeviceRegistry for ScyllaPushSettings {
    async fn register(&self, profile: &ProfileId, account: &str, device: &Device) -> Result<(), NotificationError> {
        // The token leaves any other account's profiles.
        let holders: Vec<TokenRow> = self
            .client
            .session
            .execute_unpaged(
                self.fast("SELECT profile_id, device_id, account_id FROM notification.push_device_tokens WHERE push_token = ?"),
                (device.token.as_str(),),
            )
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("push_device_tokens", e))?
            .rows::<TokenRow>()
            .map_err(|e| row_err("push_device_tokens", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| row_err("push_device_tokens", e))?;
        for holder in holders.iter().filter(|h| h.account_id.as_deref() != Some(account)) {
            self.forget(holder.profile_id, holder.device_id.as_deref().unwrap_or_default(), Some(&device.token)).await?;
        }

        // The device's previous token, if it changed, is forgotten.
        if let Some(previous) = self.device(profile, &device.device_id).await?
            && let Some(old) = previous.push_token.filter(|t| *t != device.token)
        {
            self.exec(
                self.strict("DELETE FROM notification.push_device_tokens WHERE push_token = ? AND profile_id = ?"),
                (old, profile.as_uuid()),
            )
            .await?;
        }

        self.exec(
            self.strict(
                "INSERT INTO notification.push_devices \
                 (profile_id, device_id, push_token, platform, environment, account_id, registered_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            ),
            (
                profile.as_uuid(),
                device.device_id.as_str(),
                device.token.as_str(),
                device.platform.as_str(),
                device.environment.as_str(),
                account,
                CqlTimestamp(device.registered_at.timestamp_millis()),
            ),
        )
        .await?;
        self.exec(
            self.strict(
                "INSERT INTO notification.push_device_tokens (push_token, profile_id, device_id, account_id) \
                 VALUES (?, ?, ?, ?)",
            ),
            (device.token.as_str(), profile.as_uuid(), device.device_id.as_str(), account),
        )
        .await
    }

    async fn unregister(&self, profile: &ProfileId, device_id: &str) -> Result<(), NotificationError> {
        let token = self.device(profile, device_id).await?.and_then(|d| d.push_token);
        self.forget(profile.as_uuid(), device_id, token.as_deref()).await
    }

    async fn devices(&self, profile: &ProfileId) -> Result<Vec<Device>, NotificationError> {
        let stmt = self.fast(
            "SELECT device_id, push_token, platform, environment, registered_at FROM notification.push_devices \
             WHERE profile_id = ?",
        );
        let rows: Vec<DeviceRow> = self
            .client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("push_devices", e))?
            .rows::<DeviceRow>()
            .map_err(|e| row_err("push_devices", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| row_err("push_devices", e))?;
        Ok(rows.into_iter().filter_map(DeviceRow::into_device).collect())
    }
}

#[async_trait]
impl PreferenceStore for ScyllaPushSettings {
    async fn get(&self, profile: &ProfileId) -> Result<Option<NotificationPreferences>, NotificationError> {
        #[derive(DeserializeRow)]
        struct Row {
            preferences: Option<String>,
        }
        let row = self
            .client
            .session
            .execute_unpaged(
                self.fast("SELECT preferences FROM notification.notification_preferences WHERE profile_id = ?"),
                (profile.as_uuid(),),
            )
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("notification_preferences", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("notification_preferences", e))?;
        Ok(row.and_then(|r| r.preferences).and_then(|json| NotificationPreferences::from_json(&json)))
    }

    async fn put(&self, profile: &ProfileId, preferences: &NotificationPreferences) -> Result<(), NotificationError> {
        self.exec(
            self.strict(
                "INSERT INTO notification.notification_preferences (profile_id, preferences, updated_at) \
                 VALUES (?, ?, ?)",
            ),
            (profile.as_uuid(), preferences.to_json(), CqlTimestamp(Utc::now().timestamp_millis())),
        )
        .await
    }
}

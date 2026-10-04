//! Reading push preferences and resolving where a push goes (#654).

use std::sync::Arc;

use chrono::Utc;
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{DeviceRegistry, PreferenceStore};
use crate::domain::device::Device;
use crate::domain::preferences::{NotificationPreferences, PushCategory};
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

/// The holder's preferences (stored, or their defaults).
#[derive(Debug, Clone)]
pub struct GetPreferencesQuery {
    pub profile_id: String,
    pub minor:      bool,
}

impl Query for GetPreferencesQuery {
    type Response = NotificationPreferences;
}

pub struct GetPreferencesHandler {
    pub preferences: Arc<dyn PreferenceStore>,
}

impl QueryHandler<GetPreferencesQuery> for GetPreferencesHandler {
    type Error = NotificationError;

    async fn handle(&self, envelope: Envelope<GetPreferencesQuery>) -> Result<NotificationPreferences, NotificationError> {
        let q = &envelope.payload;
        let profile = ProfileId::try_from(q.profile_id.as_str())?;
        Ok(self
            .preferences
            .get(&profile)
            .await?
            .unwrap_or_else(|| NotificationPreferences::defaults(q.minor)))
    }
}

/// Where a push about `category` to `profile_id` goes right now: nowhere when
/// the holder's preferences hold it (category off, paused, quiet hours).
#[derive(Debug, Clone)]
pub struct ResolvePushTargetsQuery {
    pub profile_id: String,
    pub category:   PushCategory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTargets {
    pub allowed: bool,
    /// Empty when not allowed.
    pub devices: Vec<Device>,
}

impl Query for ResolvePushTargetsQuery {
    type Response = PushTargets;
}

pub struct ResolvePushTargetsHandler {
    pub devices:     Arc<dyn DeviceRegistry>,
    pub preferences: Arc<dyn PreferenceStore>,
}

impl QueryHandler<ResolvePushTargetsQuery> for ResolvePushTargetsHandler {
    type Error = NotificationError;

    async fn handle(&self, envelope: Envelope<ResolvePushTargetsQuery>) -> Result<PushTargets, NotificationError> {
        let q = &envelope.payload;
        let profile = ProfileId::try_from(q.profile_id.as_str())?;
        // No stored preferences: the adult defaults (a teen's are written at
        // their first device registration).
        let preferences = self.preferences.get(&profile).await?.unwrap_or_default();
        if !preferences.push_allowed(q.category, Utc::now()) {
            return Ok(PushTargets { allowed: false, devices: Vec::new() });
        }
        Ok(PushTargets { allowed: true, devices: self.devices.devices(&profile).await? })
    }
}

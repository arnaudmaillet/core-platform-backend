//! Devices and preferences for push (#654).

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{DeviceRegistry, PreferenceStore};
use crate::domain::device::{Device, DevicePlatform, PushEnvironment, MAX_DEVICE_ID_LEN, MAX_TOKEN_LEN};
use crate::domain::preferences::{is_time_zone, HolderAge, NotificationPreferences, PushCategory, QuietHours, MAX_PAUSE};
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

fn violation(field: &str, message: impl Into<String>) -> NotificationError {
    NotificationError::DomainViolation { field: field.to_owned(), message: message.into() }
}

fn check_time_zone(zone: Option<&str>) -> Result<(), NotificationError> {
    match zone {
        Some(z) if !is_time_zone(z) => Err(violation("timezone", format!("unknown IANA time zone {z:?}"))),
        _ => Ok(()),
    }
}

/// The stored preferences, or the holder's defaults, for their age now.
async fn current(store: &dyn PreferenceStore, profile: &ProfileId, age: HolderAge) -> Result<NotificationPreferences, NotificationError> {
    Ok(store.get(profile).await?.unwrap_or_else(|| NotificationPreferences::defaults(age)).for_age(age))
}

/// Registers (or refreshes) a device for push. A holder's first device also
/// writes their defaults (teens: quiet hours), so the push sender, which has
/// no token to read the age from, applies them; the app refreshes its device
/// at launch, which also lifts the teen quiet hours once the holder is 18.
#[derive(Debug, Clone)]
pub struct RegisterDeviceCommand {
    pub profile_id:  String,
    /// The edge token's account (empty over the mesh).
    pub account_id:  String,
    pub device_id:   String,
    pub token:       String,
    pub platform:    DevicePlatform,
    pub environment: PushEnvironment,
    /// The device's IANA zone; quiet hours are read in it.
    pub timezone:    Option<String>,
    /// The holder's age (edge token `age`; unknown over the mesh).
    pub age:         HolderAge,
    /// `device_id` is the device the session is bound to (the edge token's
    /// `did`): the token then only moves between accounts on that device.
    pub device_bound: bool,
}

impl Command for RegisterDeviceCommand {}

impl Validate for RegisterDeviceCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "NTF-VAL-010", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct RegisterDeviceHandler {
    pub devices:     Arc<dyn DeviceRegistry>,
    pub preferences: Arc<dyn PreferenceStore>,
}

impl CommandHandler<RegisterDeviceCommand> for RegisterDeviceHandler {
    type Error = NotificationError;

    async fn handle(&self, envelope: Envelope<RegisterDeviceCommand>) -> Result<(), NotificationError> {
        let cmd = &envelope.payload;
        let profile = ProfileId::try_from(cmd.profile_id.as_str())?;
        if cmd.device_id.is_empty() || cmd.device_id.len() > MAX_DEVICE_ID_LEN {
            return Err(violation("device_id", format!("1–{MAX_DEVICE_ID_LEN} characters")));
        }
        if cmd.token.is_empty() || cmd.token.len() > MAX_TOKEN_LEN || cmd.token.chars().any(char::is_whitespace) {
            return Err(violation("token", format!("1–{MAX_TOKEN_LEN} characters, no spaces")));
        }
        check_time_zone(cmd.timezone.as_deref())?;

        // A push token belongs to one installation. From a session bound to its
        // device, the token may move from another account only when it was
        // registered from this same device (the account changed on the phone),
        // never from another device: someone else's leaked token cannot be
        // pulled into this account (they would stop getting their own pushes,
        // and get this account's).
        if cmd.device_bound {
            let taken_elsewhere = self.devices.token_holders(&cmd.token).await?.into_iter().any(|holder| {
                holder.account_id.as_deref() != Some(cmd.account_id.as_str())
                    && holder.device_id.as_deref().is_some_and(|d| !d.is_empty() && d != cmd.device_id)
            });
            if taken_elsewhere {
                return Err(NotificationError::PushTokenOnAnotherDevice);
            }
        }

        let device = Device {
            device_id:     cmd.device_id.clone(),
            token:         cmd.token.clone(),
            platform:      cmd.platform,
            environment:   cmd.environment,
            registered_at: Utc::now(),
        };
        self.devices.register(&profile, &cmd.account_id, &device).await?;

        let stored = self.preferences.get(&profile).await?;
        let mut preferences = stored.clone().unwrap_or_else(|| NotificationPreferences::defaults(cmd.age)).for_age(cmd.age);
        if cmd.timezone.is_some() {
            preferences.timezone.clone_from(&cmd.timezone);
        }
        if stored.as_ref() != Some(&preferences) {
            self.preferences.put(&profile, &preferences).await?;
        }
        Ok(())
    }
}

/// Forgets a device (sign-out, push turned off in iOS settings).
#[derive(Debug, Clone)]
pub struct UnregisterDeviceCommand {
    pub profile_id: String,
    pub device_id:  String,
}

impl Command for UnregisterDeviceCommand {}

impl Validate for UnregisterDeviceCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "NTF-VAL-010", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct UnregisterDeviceHandler {
    pub devices: Arc<dyn DeviceRegistry>,
}

impl CommandHandler<UnregisterDeviceCommand> for UnregisterDeviceHandler {
    type Error = NotificationError;

    async fn handle(&self, envelope: Envelope<UnregisterDeviceCommand>) -> Result<(), NotificationError> {
        let cmd = &envelope.payload;
        let profile = ProfileId::try_from(cmd.profile_id.as_str())?;
        self.devices.unregister(&profile, &cmd.device_id).await
    }
}

/// Changes some preferences; anything left `None` / empty is unchanged.
#[derive(Debug, Clone, Default)]
pub struct UpdatePreferencesCommand {
    pub profile_id:  String,
    pub age:         HolderAge,
    pub push:        Vec<(PushCategory, bool)>,
    pub email:       Vec<(PushCategory, bool)>,
    /// `Some(None)` resumes; `Some(Some(t))` pauses until `t` (≤ 8 h ahead).
    pub pause:       Option<Option<DateTime<Utc>>>,
    /// `Some(None)` turns quiet hours off.
    pub quiet_hours: Option<Option<QuietHours>>,
    pub timezone:    Option<String>,
}

impl Command for UpdatePreferencesCommand {}

impl Validate for UpdatePreferencesCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "NTF-VAL-010", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct UpdatePreferencesHandler {
    pub preferences: Arc<dyn PreferenceStore>,
}

impl CommandHandler<UpdatePreferencesCommand> for UpdatePreferencesHandler {
    type Error = NotificationError;

    async fn handle(&self, envelope: Envelope<UpdatePreferencesCommand>) -> Result<(), NotificationError> {
        let cmd = &envelope.payload;
        let profile = ProfileId::try_from(cmd.profile_id.as_str())?;
        check_time_zone(cmd.timezone.as_deref())?;
        if let Some(Some(until)) = cmd.pause {
            // A minute of slack for the client's clock.
            if until > Utc::now() + MAX_PAUSE + Duration::minutes(1) {
                return Err(violation("paused_until", "a pause lasts at most 8 hours"));
            }
        }

        let mut preferences = current(self.preferences.as_ref(), &profile, cmd.age).await?;
        for (category, on) in &cmd.push {
            preferences.set_push(*category, *on);
        }
        for (category, on) in &cmd.email {
            preferences.set_email(*category, *on);
        }
        if let Some(pause) = cmd.pause {
            preferences.paused_until = pause;
        }
        if let Some(quiet) = cmd.quiet_hours {
            preferences.set_quiet_hours(quiet);
        }
        if cmd.timezone.is_some() {
            preferences.timezone.clone_from(&cmd.timezone);
        }
        self.preferences.put(&profile, &preferences).await
    }
}

//! A device registered for push (#654): one install, for one profile.

use chrono::{DateTime, Utc};

/// The push network a device is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevicePlatform {
    Ios,
    Android,
}

impl DevicePlatform {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ios => "ios",
            Self::Android => "android",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ios" => Some(Self::Ios),
            "android" => Some(Self::Android),
            _ => None,
        }
    }
}

/// APNs environment the token belongs to (development builds use the sandbox).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushEnvironment {
    Sandbox,
    Production,
}

impl PushEnvironment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sandbox => "sandbox",
            Self::Production => "production",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "sandbox" => Some(Self::Sandbox),
            "production" => Some(Self::Production),
            _ => None,
        }
    }
}

/// Longest accepted device id and token.
pub const MAX_DEVICE_ID_LEN: usize = 128;
pub const MAX_TOKEN_LEN: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// The install's own id (iOS: identifierForVendor), stable across tokens.
    pub device_id:     String,
    pub token:         String,
    pub platform:      DevicePlatform,
    pub environment:   PushEnvironment,
    pub registered_at: DateTime<Utc>,
}

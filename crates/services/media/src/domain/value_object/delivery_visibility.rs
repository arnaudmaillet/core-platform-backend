use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::MediaError;

/// How a delivery URL is brokered for an asset.
///
/// `Public` media gets a content-addressed, immutable CDN URL — cacheable forever,
/// because an edit produces a new asset (new hash → new URL) rather than mutating
/// in place. `Signed` media gets a short-lived signed URL minted per request, only
/// after the edge has authorized the viewer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryVisibility {
    Public,
    Signed,
}

impl DeliveryVisibility {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Signed => "signed",
        }
    }

    /// Whether a resolved URL for this visibility carries an expiry (signed URLs
    /// do; immutable public URLs do not).
    pub fn is_expiring(&self) -> bool {
        matches!(self, Self::Signed)
    }

    /// The stricter of the two (#919): an asset's visibility is never relaxed
    /// by a request — a caller may ask for SIGNED, never PUBLIC for a SIGNED
    /// asset.
    pub fn stricter(self, requested: Option<Self>) -> Self {
        match (self, requested) {
            (Self::Signed, _) | (_, Some(Self::Signed)) => Self::Signed,
            _ => Self::Public,
        }
    }
}

impl Default for DeliveryVisibility {
    /// Public is the default: most media (avatars, post images) is world-readable
    /// and served from immutable content-addressed URLs.
    fn default() -> Self {
        Self::Public
    }
}

impl fmt::Display for DeliveryVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for DeliveryVisibility {
    type Error = MediaError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "public" => Ok(Self::Public),
            "signed" => Ok(Self::Signed),
            other => Err(MediaError::DomainViolation {
                field: "delivery_visibility".into(),
                message: format!("unknown delivery visibility: '{other}'"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_public_and_non_expiring() {
        assert_eq!(DeliveryVisibility::default(), DeliveryVisibility::Public);
        assert!(!DeliveryVisibility::Public.is_expiring());
        assert!(DeliveryVisibility::Signed.is_expiring());
    }

    #[test]
    fn string_round_trip() {
        for v in [DeliveryVisibility::Public, DeliveryVisibility::Signed] {
            assert_eq!(DeliveryVisibility::try_from(v.as_str()).unwrap(), v);
        }
        assert!(DeliveryVisibility::try_from("secret").is_err());
    }
}

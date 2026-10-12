use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ProfileError;

/// A media asset an avatar or banner shows (#907): media.v1's asset id, the
/// stable key clients cache it by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MediaAssetId(Uuid);

impl MediaAssetId {
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl TryFrom<&str> for MediaAssetId {
    type Error = ProfileError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Uuid::parse_str(s).map(Self).map_err(|_| ProfileError::MediaAssetNotUsable { id: s.to_owned() })
    }
}

impl fmt::Display for MediaAssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

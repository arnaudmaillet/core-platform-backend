use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::PostError;

/// A media asset an attachment shows (#907): media.v1's asset id, the stable
/// key clients cache it by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MediaAssetId(Uuid);

impl MediaAssetId {
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl TryFrom<&str> for MediaAssetId {
    type Error = PostError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Uuid::parse_str(s).map(Self).map_err(|_| PostError::InvalidMediaAssetId(s.to_owned()))
    }
}

impl fmt::Display for MediaAssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

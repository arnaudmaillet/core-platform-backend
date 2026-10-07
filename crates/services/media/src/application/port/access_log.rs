//! Who opened which private document (#837): every staff link is recorded on
//! the audit plane before it is handed out.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::{AssetId, OwnerId};
use crate::error::MediaError;

/// A staff member was given a link to someone's private document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentView {
    pub asset_id:   AssetId,
    /// The document's owner (the account it identifies).
    pub owner:      OwnerId,
    /// The staff account the link was minted for.
    pub viewer:     String,
    pub at:         DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait AccessLog: Send + Sync + 'static {
    /// Records the view. A failure withholds the link (fail closed).
    async fn document_viewed(&self, view: &DocumentView) -> Result<(), MediaError>;
}

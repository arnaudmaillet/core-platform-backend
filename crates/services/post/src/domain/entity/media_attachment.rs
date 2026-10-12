use serde::{Deserialize, Serialize};
use crate::domain::value_object::{CdnUrl, MediaAssetId, MimeType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaAttachment {
    pub cdn_url:          CdnUrl,
    pub mime_type:        MimeType,
    pub width:            u32,
    pub height:           u32,
    pub thumbnail_url:    Option<CdnUrl>,
    pub duration_seconds: Option<f32>,
    /// The media asset it shows (#907), checked against media when written.
    /// Absent on attachments stored before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_id:         Option<MediaAssetId>,
}

impl MediaAttachment {
    pub fn is_video(&self) -> bool {
        self.mime_type.is_video()
    }
}

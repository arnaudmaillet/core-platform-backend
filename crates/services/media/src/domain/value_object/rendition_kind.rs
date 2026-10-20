use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::MediaError;

/// A derivative variant in an asset's catalog. Image variants are size buckets;
/// video variants are the adaptive-streaming `Manifest` (playback entry point) and
/// a still `Poster`. The concrete output format (image WebP/AVIF/JPEG, or streaming
/// HLS/DASH) is carried by each rendition's MIME type, not by this enum. `Original`
/// is the validated master that image renditions are derived from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenditionKind {
    Original,
    Thumbnail,
    Small,
    Medium,
    Large,
    /// A video's adaptive-streaming playback manifest (the master playlist).
    Manifest,
    /// A still poster frame for feed first-paint before playback starts; an
    /// animated avatar's still, its sheet's last frame (#948).
    Poster,
    /// An animated avatar's sprite sheet with 128 px cells (#945).
    SheetSmall,
    /// An animated avatar's sprite sheet with 256 px cells (#945).
    SheetLarge,
    /// A video's short muted preview loop (#963).
    Preview,
}

impl RenditionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Thumbnail => "thumbnail",
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
            Self::Manifest => "manifest",
            Self::Poster => "poster",
            Self::SheetSmall => "sheet_small",
            Self::SheetLarge => "sheet_large",
            Self::Preview => "preview",
        }
    }

    /// The slug used in the content-addressed key (`{kind}/{hash}/{slug}.{ext}`).
    pub fn slug(&self) -> &'static str {
        self.as_str()
    }

    /// The master is the source of truth a READY asset must always carry; the
    /// resized buckets are derived from it.
    pub fn is_original(&self) -> bool {
        matches!(self, Self::Original)
    }
}

impl fmt::Display for RenditionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for RenditionKind {
    type Error = MediaError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "original" => Ok(Self::Original),
            "thumbnail" => Ok(Self::Thumbnail),
            "small" => Ok(Self::Small),
            "medium" => Ok(Self::Medium),
            "large" => Ok(Self::Large),
            "manifest" => Ok(Self::Manifest),
            "poster" => Ok(Self::Poster),
            "sheet_small" => Ok(Self::SheetSmall),
            "sheet_large" => Ok(Self::SheetLarge),
            "preview" => Ok(Self::Preview),
            other => Err(MediaError::DomainViolation {
                field: "rendition_kind".into(),
                message: format!("unknown rendition kind: '{other}'"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_round_trip() {
        for k in [
            RenditionKind::Original,
            RenditionKind::Thumbnail,
            RenditionKind::Small,
            RenditionKind::Medium,
            RenditionKind::Large,
            RenditionKind::Manifest,
            RenditionKind::Poster,
        ] {
            assert_eq!(RenditionKind::try_from(k.as_str()).unwrap(), k);
        }
        assert!(RenditionKind::try_from("huge").is_err());
    }

    #[test]
    fn only_original_is_the_master() {
        assert!(RenditionKind::Original.is_original());
        assert!(!RenditionKind::Thumbnail.is_original());
    }
}

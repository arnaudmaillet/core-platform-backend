use serde::{Deserialize, Serialize};

/// A post's kind as a place page's kind filter sees it (#949), kept on its
/// card.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PostFormat {
    /// Not recorded (a card indexed before it was kept).
    #[default]
    Unknown,
    /// Photos (a carousel).
    Photo,
    Video,
    TextOnly,
}

impl PostFormat {
    /// From `PostPublished.kind` (post's `PostKind`).
    pub fn from_event_kind(kind: &str) -> Self {
        match kind {
            "TextOnly" => Self::TextOnly,
            "Carousel" => Self::Photo,
            "MainVideo" => Self::Video,
            _ => Self::Unknown,
        }
    }

    /// Its `map_post_cards.post_kind` value (`None` = unknown).
    pub fn code(self) -> Option<&'static str> {
        match self {
            Self::Unknown => None,
            Self::Photo => Some("photo"),
            Self::Video => Some("video"),
            Self::TextOnly => Some("text_only"),
        }
    }

    pub fn from_code(code: Option<&str>) -> Self {
        match code {
            Some("photo") => Self::Photo,
            Some("video") => Self::Video,
            Some("text_only") => Self::TextOnly,
            _ => Self::Unknown,
        }
    }
}

/// Which posts a place page keeps by kind (#949).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostKindFilter {
    #[default]
    Any,
    Media,
    TextOnly,
}

impl PostKindFilter {
    /// A post of unknown kind passes every filter: the client filters too.
    pub fn keeps(self, format: PostFormat) -> bool {
        matches!(
            (self, format),
            (Self::Any, _)
                | (_, PostFormat::Unknown)
                | (Self::Media, PostFormat::Photo | PostFormat::Video)
                | (Self::TextOnly, PostFormat::TextOnly)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_published_kind_maps_to_its_format_and_back() {
        assert_eq!(PostFormat::from_event_kind("TextOnly"), PostFormat::TextOnly);
        assert_eq!(PostFormat::from_event_kind("Carousel"), PostFormat::Photo);
        assert_eq!(PostFormat::from_event_kind("MainVideo"), PostFormat::Video);
        assert_eq!(PostFormat::from_event_kind("text"), PostFormat::Unknown);
        for format in [PostFormat::Unknown, PostFormat::Photo, PostFormat::Video, PostFormat::TextOnly] {
            assert_eq!(PostFormat::from_code(format.code()), format);
        }
        assert!(PostKindFilter::Media.keeps(PostFormat::Unknown));
        assert!(PostKindFilter::Media.keeps(PostFormat::Video));
        assert!(!PostKindFilter::Media.keeps(PostFormat::TextOnly));
        assert!(PostKindFilter::TextOnly.keeps(PostFormat::TextOnly));
    }
}

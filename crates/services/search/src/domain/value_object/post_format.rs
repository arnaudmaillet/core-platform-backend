use serde::{Deserialize, Serialize};

/// A post's kind as a search kind filter sees it (#949): from post's
/// `PostKind`, kept on the post document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostFormat {
    /// Photo, carousel or video.
    Media,
    TextOnly,
}

impl PostFormat {
    /// The document's `post_kind` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Media => "media",
            Self::TextOnly => "text_only",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "media" => Some(Self::Media),
            "text_only" => Some(Self::TextOnly),
            _ => None,
        }
    }
}

/// Which post hits a search keeps by kind (#949). Other entity kinds, and a
/// post document indexed before its kind was kept, pass every filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostKindFilter {
    #[default]
    Any,
    Media,
    TextOnly,
}

impl PostKindFilter {
    /// The kind a hit must not have; `None` keeps all.
    pub fn excluded(self) -> Option<PostFormat> {
        match self {
            Self::Any => None,
            Self::Media => Some(PostFormat::TextOnly),
            Self::TextOnly => Some(PostFormat::Media),
        }
    }

    pub fn keeps(self, format: Option<PostFormat>) -> bool {
        self.excluded().is_none_or(|excluded| format != Some(excluded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_keeps_its_kind_and_the_unknown() {
        assert!(PostKindFilter::Any.keeps(Some(PostFormat::TextOnly)));
        assert!(PostKindFilter::Media.keeps(Some(PostFormat::Media)));
        assert!(!PostKindFilter::Media.keeps(Some(PostFormat::TextOnly)));
        assert!(PostKindFilter::Media.keeps(None));
        assert!(!PostKindFilter::TextOnly.keeps(Some(PostFormat::Media)));
        for format in [PostFormat::Media, PostFormat::TextOnly] {
            assert_eq!(PostFormat::parse(format.as_str()), Some(format));
        }
    }
}

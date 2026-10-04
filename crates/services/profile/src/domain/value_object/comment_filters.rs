use serde::{Deserialize, Serialize};

use crate::error::ProfileError;

/// At most this many hidden words, each at most [`MAX_HIDDEN_WORD_LEN`] chars.
pub const MAX_HIDDEN_WORDS: usize = 200;
pub const MAX_HIDDEN_WORD_LEN: usize = 64;

/// What the owner hides from comments on their posts (#660): their own hidden
/// words (words, phrases or emoji) and the offensive-comment filter, on by
/// default. comment applies them; the commenter still sees their own comment.
/// Stored as one JSON column; a profile without one has the defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CommentFilters {
    pub hidden_words:     Vec<String>,
    pub filter_offensive: bool,
}

impl Default for CommentFilters {
    fn default() -> Self {
        Self { hidden_words: Vec::new(), filter_offensive: true }
    }
}

impl CommentFilters {
    /// Trims, lowercases and de-duplicates the words, then checks the limits.
    pub fn new(hidden_words: Vec<String>, filter_offensive: bool) -> Result<Self, ProfileError> {
        let mut words: Vec<String> = hidden_words
            .into_iter()
            .map(|w| w.trim().to_lowercase())
            .filter(|w| !w.is_empty())
            .collect();
        words.sort();
        words.dedup();
        if words.len() > MAX_HIDDEN_WORDS {
            return Err(ProfileError::DomainViolation {
                field:   "hidden_words".into(),
                message: format!("at most {MAX_HIDDEN_WORDS} hidden words"),
            });
        }
        if let Some(long) = words.iter().find(|w| w.chars().count() > MAX_HIDDEN_WORD_LEN) {
            return Err(ProfileError::DomainViolation {
                field:   "hidden_words".into(),
                message: format!("{long:?} is longer than {MAX_HIDDEN_WORD_LEN} characters"),
            });
        }
        Ok(Self { hidden_words: words, filter_offensive })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(json: Option<&str>) -> Self {
        json.and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_normalised_and_limited() {
        let f = CommentFilters::new(vec!["  Spoiler ".into(), "spoiler".into(), "".into(), "🍕".into()], true).unwrap();
        assert_eq!(f.hidden_words, vec!["spoiler".to_owned(), "🍕".to_owned()]);
        assert!(CommentFilters::new(vec!["x".repeat(65)], true).is_err());
        assert!(CommentFilters::new((0..201).map(|i| format!("w{i}")).collect(), true).is_err());
    }

    #[test]
    fn absent_means_no_words_and_the_offensive_filter_on() {
        assert_eq!(CommentFilters::from_json(None), CommentFilters::default());
        assert!(CommentFilters::default().filter_offensive);
        let f = CommentFilters::new(vec!["spoiler".into()], false).unwrap();
        assert_eq!(CommentFilters::from_json(Some(&f.to_json())), f);
    }
}

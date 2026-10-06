//! A holder's hidden words and the offensive-term list, matched against user
//! text: the comments on their posts (#660) and the message requests they get
//! (#810). Matching is case-insensitive; a word or phrase matches whole words,
//! an entry without letters or digits (an emoji) matches anywhere.

use serde::{Deserialize, Serialize};

/// A holder's filter, as profile announces it (`ProfileCommentFiltersChanged`).
/// The default (nothing announced): no hidden words, offensive filter on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentFilter {
    pub hidden_words:     Vec<String>,
    pub filter_offensive: bool,
}

impl Default for ContentFilter {
    fn default() -> Self {
        Self { hidden_words: Vec::new(), filter_offensive: true }
    }
}

/// A list of terms, matched as [`ContentFilter`] matches hidden words.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TermList {
    terms: Vec<String>,
}

impl TermList {
    pub fn new(terms: impl IntoIterator<Item = String>) -> Self {
        let mut terms: Vec<String> = terms
            .into_iter()
            .map(|t| t.trim().to_lowercase())
            .filter(|t| !t.is_empty() && !t.starts_with('#'))
            .collect();
        terms.sort();
        terms.dedup();
        Self { terms }
    }

    /// One term per line; blank lines and `#` comments skipped.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self::new(std::fs::read_to_string(path)?.lines().map(str::to_owned)))
    }

    pub fn len(&self) -> usize {
        self.terms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Does `text` contain any of the terms?
    pub fn matches(&self, text: &str) -> bool {
        any_match(&self.terms, text)
    }
}

impl ContentFilter {
    /// Is a text (a comment, a message) with this `body` hidden by this filter?
    pub fn hides(&self, body: &str, offensive: &TermList) -> bool {
        any_match(&self.hidden_words, body) || (self.filter_offensive && offensive.matches(body))
    }
}

/// Words of `text`, lowercased, separated by single spaces and padded with
/// one on each side, so a phrase matches whole words with a plain `contains`.
fn padded_words(text: &str) -> String {
    let lowered = text.to_lowercase();
    let words: Vec<&str> = lowered.split(|c: char| !c.is_alphanumeric()).filter(|w| !w.is_empty()).collect();
    format!(" {} ", words.join(" "))
}

fn any_match(terms: &[String], text: &str) -> bool {
    if terms.is_empty() || text.is_empty() {
        return false;
    }
    let lowered = text.to_lowercase();
    let words = padded_words(text);
    terms.iter().any(|term| {
        if term.chars().any(char::is_alphanumeric) {
            words.contains(&padded_words(term))
        } else {
            lowered.contains(term.as_str())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(words: &[&str], offensive: bool) -> ContentFilter {
        ContentFilter { hidden_words: words.iter().map(|w| w.to_string()).collect(), filter_offensive: offensive }
    }

    #[test]
    fn hidden_words_match_whole_words_case_insensitively() {
        let f = filter(&["spoiler", "the end"], false);
        let none = TermList::default();
        assert!(f.hides("Big SPOILER inside!", &none));
        assert!(f.hides("wait for the end...", &none));
        assert!(!f.hides("spoilers are fine", &none), "a whole word, not a prefix");
        assert!(!f.hides("the ending", &none));
        assert!(!f.hides("nothing here", &none));
    }

    #[test]
    fn an_emoji_matches_anywhere() {
        assert!(filter(&["🍕"], false).hides("lunch🍕time", &TermList::default()));
    }

    #[test]
    fn the_offensive_list_applies_only_while_the_filter_is_on() {
        let offensive = TermList::new(["# a comment line".to_owned(), "Badword".to_owned()]);
        assert_eq!(offensive.len(), 1);
        assert!(filter(&[], true).hides("what a badword", &offensive));
        assert!(!filter(&[], false).hides("what a badword", &offensive));
        assert!(!filter(&[], true).hides("all good", &offensive));
    }
}

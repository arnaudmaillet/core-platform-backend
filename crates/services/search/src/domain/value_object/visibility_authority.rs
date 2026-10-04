use serde::{Deserialize, Serialize};

/// Which authority a visibility decision comes from.
///
/// A document is searchable only when **every** authority permits it:
/// `searchable = moderation_visible AND owner_visible AND discoverable`. They are
/// independent —
/// each writes its own flag, guarded by its own version — so neither can override
/// the other. A platform-integrity moderation hide can't be undone by the profile
/// owner restoring their own visibility, and vice-versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VisibilityAuthority {
    /// Platform trust-and-safety (`moderation.v1.events`).
    Moderation,
    /// The entity's own owner (e.g. a profile masking itself, via `profile.v1.events`).
    Owner,
    /// The profile's own discoverability setting (#661, `by_handle_search`),
    /// separate from masking so neither undoes the other.
    Discovery,
}

impl VisibilityAuthority {
    /// The `_source` flag field this authority writes (and the query filters on).
    pub fn flag_field(&self) -> &'static str {
        match self {
            VisibilityAuthority::Moderation => "moderation_searchable",
            VisibilityAuthority::Owner => "owner_searchable",
            VisibilityAuthority::Discovery => "discoverable",
        }
    }

    /// The `_source` version field guarding this authority's flag.
    pub fn version_field(&self) -> &'static str {
        match self {
            VisibilityAuthority::Moderation => "moderation_visibility_version",
            VisibilityAuthority::Owner => "owner_visibility_version",
            VisibilityAuthority::Discovery => "discovery_version",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_are_distinct_per_authority() {
        assert_ne!(
            VisibilityAuthority::Moderation.flag_field(),
            VisibilityAuthority::Owner.flag_field()
        );
        assert_ne!(
            VisibilityAuthority::Moderation.version_field(),
            VisibilityAuthority::Owner.version_field()
        );
    }
}

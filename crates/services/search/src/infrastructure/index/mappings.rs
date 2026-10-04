//! Index settings + field mappings as **versioned artifacts**.
//!
//! Analyzers and mappings are a schema: changing them requires a blue-green reindex
//! (Phase 7), never an in-place edit. Only a **new** common field may be added in
//! place (`ensure_indices` puts the common properties on every start). Each kind gets its own physical index with an
//! analyzer chain tuned for its matchable fields. Common to all: the two version
//! guards (`content_version`, `visibility_version`), the moderation `searchable`
//! flag, `author_id` (for exclusion + GDPR purge), timestamps, and the coarse
//! `popularity` ranking signal.

use serde_json::{Value, json};

use crate::domain::EntityKind;

/// Bump when the mapping/analyzer definition changes (drives the reindex target
/// suffix, e.g. `search-posts-v1`).
pub const MAPPING_VERSION: &str = "v1";

/// Shared analysis block: a lowercase + asciifolding analyzer for forgiving
/// full-text match, plus an edge-ngram `autocomplete` analyzer for prefix/suggest.
fn analysis() -> Value {
    json!({
        "analyzer": {
            "folding": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"]
            },
            "autocomplete": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding", "edge_ngram_1_20"]
            }
        },
        "filter": {
            "edge_ngram_1_20": { "type": "edge_ngram", "min_gram": 1, "max_gram": 20 }
        }
    })
}

/// Fields every index carries regardless of kind.
pub(super) fn common_properties() -> Value {
    json!({
        "entity_type":                   { "type": "keyword" },
        "author_id":                     { "type": "keyword" },
        // Two independent visibility authorities; a doc is searchable only when both
        // are true. `moderation` = platform trust-and-safety; `owner` = the entity's
        // own masking. Each has its own monotonic version guard.
        "moderation_searchable":         { "type": "boolean" },
        "moderation_visibility_version": { "type": "long" },
        "owner_searchable":              { "type": "boolean" },
        "owner_visibility_version":      { "type": "long" },
        // Third authority (#661): the profile's own "findable by search" choice.
        // Added in place to existing indices (additive `_mapping` update).
        "discoverable":                  { "type": "boolean" },
        "discovery_version":             { "type": "long" },
        "content_version":               { "type": "long" },
        "created_at":                    { "type": "date" },
        "indexed_at":                    { "type": "date" },
        "popularity":                    { "type": "double" }
    })
}

/// Merge the common fields with a kind's own matchable/display fields.
fn properties_for(kind: EntityKind) -> Value {
    let mut props = common_properties();
    let specific = match kind {
        EntityKind::Profile => json!({
            // `handle` indexed three ways: exact (keyword), full-text, and prefix.
            "handle": {
                "type": "text",
                "analyzer": "folding",
                "fields": {
                    "raw": { "type": "keyword" },
                    "prefix": { "type": "text", "analyzer": "autocomplete", "search_analyzer": "folding" }
                }
            },
            "display_name": { "type": "text", "analyzer": "folding" },
            "bio":          { "type": "text", "analyzer": "folding" },
            "avatar_key":   { "type": "keyword", "index": false },
            "verified":     { "type": "boolean" }
        }),
        EntityKind::Post => json!({
            "caption":       { "type": "text", "analyzer": "folding" },
            "hashtags":      { "type": "keyword" },
            "author_handle": { "type": "text", "analyzer": "folding", "fields": { "raw": { "type": "keyword" } } },
            "thumbnail_key": { "type": "keyword", "index": false }
        }),
        EntityKind::Hashtag => json!({
            "tag": {
                "type": "text",
                "analyzer": "folding",
                "fields": {
                    "raw": { "type": "keyword" },
                    "prefix": { "type": "text", "analyzer": "autocomplete", "search_analyzer": "folding" }
                }
            },
            "post_count": { "type": "long" }
        }),
    };
    merge(&mut props, specific);
    props
}

fn merge(base: &mut Value, extra: Value) {
    if let (Some(b), Some(e)) = (base.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            b.insert(k.clone(), v.clone());
        }
    }
}

/// The full create-index body (settings + mappings) for a kind.
pub fn index_body(kind: EntityKind) -> Value {
    json!({
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "analysis": analysis()
        },
        "mappings": {
            // Reject documents with fields the mapping doesn't define — schema drift
            // should fail loudly, not silently index unmapped data.
            "dynamic": "strict",
            "properties": properties_for(kind)
        }
    })
}

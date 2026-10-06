//! The OpenSearch adapter — the production [`SearchIndex`] + [`IndexAdmin`].
//!
//! It speaks the REST API directly (reqwest + serde_json) so the two version
//! guards can be enforced with Painless scripts: a content `upsert` and a
//! visibility `set_searchable` each carry their own version field in `_source`
//! (`content_version` / `visibility_version`) and a scripted update applies the
//! write only if it is strictly newer — atomically, server-side. A content
//! re-projection preserves the stored `searchable` flag; a visibility flip that
//! races ahead of content creates a placeholder the later content write honours.
//!
//! Write paths fail **closed** (errors propagate so the consumer retries / DLQs —
//! no silent index gaps). The read path fails **open** (a partial/unavailable
//! engine yields degraded results, never an error that breaks the page).
//!
//! Behaviour is verified against a live single-node OpenSearch in Phase 6; this
//! module is compile-checked only here.

use async_trait::async_trait;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};

use super::mappings::{MAPPING_VERSION, common_properties, index_body};
use crate::application::port::{IndexAdmin, SearchIndex, WriteOutcome};
use crate::domain::{
    AuthorId, DocVersion, EntityKind, HitDisplay, IndexDocument, Searchable, SearchHit, SearchQuery,
    SearchResults, SuggestQuery, Suggestion, Suggestions, VisibilityAuthority,
};
use crate::error::SearchError;

/// Painless: apply content fields only if strictly newer; preserve BOTH visibility
/// authorities' flags across a re-projection, seeding each (and its version) only
/// when absent.
const CONTENT_SCRIPT: &str = "\
if (ctx._source.content_version != null && params.content_version <= ctx._source.content_version) { ctx.op = 'noop'; } \
else { for (entry in params.doc.entrySet()) { ctx._source[entry.getKey()] = entry.getValue(); } \
ctx._source.content_version = params.content_version; \
if (ctx._source.moderation_searchable == null) { ctx._source.moderation_searchable = params.default_searchable; } \
if (ctx._source.moderation_visibility_version == null) { ctx._source.moderation_visibility_version = 0; } \
if (ctx._source.owner_searchable == null) { ctx._source.owner_searchable = params.default_searchable; } \
if (ctx._source.owner_visibility_version == null) { ctx._source.owner_visibility_version = 0; } }";

/// Painless: flip ONE authority's flag (its field names passed as params) only if
/// strictly newer; seed a placeholder when the content has not yet arrived, leaving
/// the other authority visible by default.
const VISIBILITY_SCRIPT: &str = "\
if (ctx._source[params.version_field] != null && params.version <= ctx._source[params.version_field]) { ctx.op = 'noop'; } \
else { ctx._source[params.flag_field] = params.searchable; ctx._source[params.version_field] = params.version; \
if (ctx._source.entity_type == null) { ctx._source.entity_type = params.entity_type; } }";

/// Painless (update-by-query over an author's posts): apply their post window
/// (#664) when strictly newer — the post leaves search at `created_at +
/// window_ms`, or never (`window_ms` null).
const WINDOW_SCRIPT: &str = "\
if (ctx._source.window_version != null && params.version <= ctx._source.window_version) { ctx.op = 'noop'; } \
else { ctx._source.window_version = params.version; \
if (params.window_ms == null || ctx._source.created_at == null) { ctx._source.visible_until = null; } \
else { ctx._source.visible_until = ZonedDateTime.parse(ctx._source.created_at).toInstant().toEpochMilli() + params.window_ms; } }";

/// Boosted matchable fields for the federated multi_match (missing fields per index
/// are simply ignored by OpenSearch).
const MATCH_FIELDS: &[&str] = &[
    "handle^3",
    "display_name^2",
    "bio",
    "caption",
    "hashtags^2",
    "author_handle",
    "tag^3",
];

/// Default per-request timeout — a slow engine sheds load (the query path fails
/// OPEN to degraded results) rather than hanging the caller.
pub const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(800);

/// Connection + namespace configuration. Built from env at the composition root
/// (Phase 5).
#[derive(Debug, Clone)]
pub struct OpenSearchConfig {
    pub base_url: String,
    pub index_prefix: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Hard per-request timeout applied to the HTTP client.
    pub request_timeout: std::time::Duration,
}

impl OpenSearchConfig {
    pub fn new(base_url: impl Into<String>, index_prefix: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            index_prefix: index_prefix.into(),
            username: None,
            password: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    pub fn with_basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

pub struct OpenSearchIndex {
    client: Client,
    config: OpenSearchConfig,
}

impl OpenSearchIndex {
    pub fn new(client: Client, config: OpenSearchConfig) -> Self {
        Self { client, config }
    }

    /// Build with an HTTP client carrying the configured request timeout — the
    /// common case (composition root + tests).
    pub fn from_config(config: OpenSearchConfig) -> Self {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self::new(client, config)
    }

    /// Force a refresh so prior writes become searchable immediately. Used by the
    /// integration suite to make near-real-time indexing deterministic.
    pub async fn refresh(&self) -> Result<(), SearchError> {
        let path = format!(
            "{}-*/_refresh?ignore_unavailable=true&allow_no_indices=true",
            self.config.index_prefix
        );
        let (status, _) = self.send(Method::POST, &path, None).await?;
        if status.is_success() {
            Ok(())
        } else {
            Err(SearchError::EngineUnavailable)
        }
    }

    // ── Index / alias naming ──────────────────────────────────────────────────

    /// Liveness probe — `GET _cluster/health`. Used by the runtime readiness loop.
    pub async fn ping(&self) -> Result<(), SearchError> {
        let (status, _) = self.send(Method::GET, "_cluster/health", None).await?;
        if status.is_success() {
            Ok(())
        } else {
            Err(SearchError::EngineUnavailable)
        }
    }

    /// Recent searches (#663): one document per profile.
    fn recent_index(&self) -> String {
        format!("{}-recent-searches", self.config.index_prefix)
    }

    fn read_alias(&self, kind: EntityKind) -> String {
        format!("{}-{}", self.config.index_prefix, plural(kind))
    }

    fn write_alias(&self, kind: EntityKind) -> String {
        format!("{}-{}-write", self.config.index_prefix, plural(kind))
    }

    fn physical(&self, kind: EntityKind, suffix: &str) -> String {
        format!("{}-{}-{}", self.config.index_prefix, plural(kind), suffix)
    }

    /// Comma-joined read aliases for the requested kinds (all three if empty).
    fn read_targets(&self, kinds: &[EntityKind]) -> String {
        let kinds = if kinds.is_empty() {
            vec![EntityKind::Profile, EntityKind::Post, EntityKind::Hashtag]
        } else {
            kinds.to_vec()
        };
        kinds
            .iter()
            .map(|k| self.read_alias(*k))
            .collect::<Vec<_>>()
            .join(",")
    }

    // ── HTTP plumbing ─────────────────────────────────────────────────────────

    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), SearchError> {
        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let mut req = self.client.request(method, url);
        if let Some(b) = body {
            req = req.json(&b);
        }
        if let Some(user) = &self.config.username {
            req = req.basic_auth(user, self.config.password.clone());
        }
        let resp = req.send().await.map_err(transport_error)?;
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, value))
    }

    /// A scripted update (used by both write guards). Returns the OpenSearch
    /// `result` so the caller can distinguish a `noop` (stale) from an applied write.
    async fn scripted_update(
        &self,
        kind: EntityKind,
        id: &str,
        body: Value,
    ) -> Result<WriteOutcome, SearchError> {
        let path = format!("{}/_update/{}", self.write_alias(kind), encode(id));
        let (status, value) = self.send(Method::POST, &path, Some(body)).await?;
        if !status.is_success() {
            return Err(write_status_error(status, &value));
        }
        match value["result"].as_str() {
            Some("noop") => Ok(WriteOutcome::RejectedStale),
            _ => Ok(WriteOutcome::Applied),
        }
    }
}

#[async_trait]
impl SearchIndex for OpenSearchIndex {
    async fn upsert(&self, document: &IndexDocument) -> Result<WriteOutcome, SearchError> {
        let body = json!({
            "scripted_upsert": true,
            "upsert": {},
            "script": {
                "lang": "painless",
                "source": CONTENT_SCRIPT,
                "params": {
                    "content_version": document.version().value(),
                    "default_searchable": document.searchable().is_visible(),
                    "doc": content_source(document),
                }
            }
        });
        self.scripted_update(document.kind(), document.id(), body)
            .await
    }

    async fn set_searchable(
        &self,
        authority: VisibilityAuthority,
        kind: EntityKind,
        id: &str,
        searchable: Searchable,
        version: DocVersion,
    ) -> Result<WriteOutcome, SearchError> {
        let body = json!({
            "scripted_upsert": true,
            "upsert": {},
            "script": {
                "lang": "painless",
                "source": VISIBILITY_SCRIPT,
                "params": {
                    "flag_field": authority.flag_field(),
                    "version_field": authority.version_field(),
                    "version": version.value(),
                    "searchable": searchable.is_visible(),
                    "entity_type": kind.as_str(),
                }
            }
        });
        self.scripted_update(kind, id, body).await
    }

    async fn delete(&self, kind: EntityKind, id: &str) -> Result<(), SearchError> {
        let path = format!("{}/_doc/{}", self.write_alias(kind), encode(id));
        let (status, value) = self.send(Method::DELETE, &path, None).await?;
        // 200 deleted, 404 already-absent — both are idempotent success.
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(write_status_error(status, &value))
        }
    }

    async fn purge_by_author(&self, author_id: &AuthorId) -> Result<u64, SearchError> {
        // Deep purge across every index — no retained tombstone.
        let path = format!(
            "{}-*/_delete_by_query?conflicts=proceed&refresh=true",
            self.config.index_prefix
        );
        let body = json!({ "query": { "term": { "author_id": author_id.as_str() } } });
        let (status, value) = self.send(Method::POST, &path, Some(body)).await?;
        if !status.is_success() {
            return Err(write_status_error(status, &value));
        }
        Ok(value["deleted"].as_u64().unwrap_or(0))
    }

    async fn set_post_window(
        &self,
        author_id: &AuthorId,
        window_days: Option<u32>,
        version: DocVersion,
    ) -> Result<u64, SearchError> {
        let path = format!(
            "{}/_update_by_query?conflicts=proceed&refresh=true",
            self.write_alias(EntityKind::Post)
        );
        let body = json!({
            "query": { "term": { "author_id": author_id.as_str() } },
            "script": {
                "lang": "painless",
                "source": WINDOW_SCRIPT,
                "params": {
                    "version": version.value(),
                    "window_ms": window_days.map(|d| i64::from(d) * 86_400_000),
                }
            }
        });
        let (status, value) = self.send(Method::POST, &path, Some(body)).await?;
        if !status.is_success() {
            return Err(write_status_error(status, &value));
        }
        Ok(value["updated"].as_u64().unwrap_or(0))
    }

    async fn search(&self, query: &SearchQuery) -> Result<SearchResults, SearchError> {
        let path = format!(
            "{}/_search?ignore_unavailable=true&allow_no_indices=true",
            self.read_targets(&query.kinds)
        );
        let (status, value) = match self.send(Method::POST, &path, Some(search_body(query))).await {
            Ok(pair) => pair,
            // Fail OPEN: an unreachable engine degrades discovery, never errors the page.
            Err(_) => return Ok(degraded_empty()),
        };
        if status.is_server_error() {
            return Ok(degraded_empty());
        }
        if !status.is_success() {
            return Err(SearchError::InvalidQuery {
                reason: format!("engine rejected query ({status})"),
            });
        }
        Ok(parse_search(&value, query.page_size))
    }

    async fn suggest(&self, query: &SuggestQuery) -> Result<Suggestions, SearchError> {
        let kinds = if query.kinds.is_empty() {
            vec![EntityKind::Profile, EntityKind::Hashtag]
        } else {
            query.kinds.clone()
        };
        let path = format!(
            "{}/_search?ignore_unavailable=true&allow_no_indices=true",
            self.read_targets(&kinds)
        );
        let body = json!({
            "size": query.limit,
            "query": { "bool": {
                "must_not": visibility_must_not(),
                "must": [{ "multi_match": {
                    "query": query.prefix,
                    "type": "phrase_prefix",
                    "fields": ["handle.prefix", "tag.prefix", "handle", "tag"]
                }}]
            }}
        });
        let (status, value) = match self.send(Method::POST, &path, Some(body)).await {
            Ok(pair) => pair,
            Err(_) => return Ok(Suggestions::default()),
        };
        if !status.is_success() {
            return Ok(Suggestions::default());
        }
        Ok(parse_suggest(&value))
    }
}

#[async_trait]
impl IndexAdmin for OpenSearchIndex {
    async fn ensure_indices(&self) -> Result<(), SearchError> {
        // Recent searches (#663): one document per profile, never searched.
        let (status, value) = self
            .send(
                Method::PUT,
                &encode_path(&self.recent_index()),
                Some(json!({ "mappings": { "dynamic": false, "properties": {
                    "entries": { "type": "object", "enabled": false }
                } } })),
            )
            .await?;
        if !status.is_success() && !already_exists(&value) {
            return Err(write_status_error(status, &value));
        }
        for kind in [EntityKind::Profile, EntityKind::Post, EntityKind::Hashtag] {
            let physical = self.physical(kind, MAPPING_VERSION);
            // Create the physical index; tolerate "already exists".
            let (status, value) = self
                .send(Method::PUT, &encode_path(&physical), Some(index_body(kind)))
                .await?;
            if !status.is_success() && !already_exists(&value) {
                return Err(write_status_error(status, &value));
            }
            // Bring an existing index up to the mapping's additive fields (new
            // fields only; changing a field or an analyzer needs a reindex).
            let (status, value) = self
                .send(
                    Method::PUT,
                    &format!("{}/_mapping", encode_path(&physical)),
                    Some(json!({ "properties": common_properties() })),
                )
                .await?;
            if !status.is_success() {
                return Err(write_status_error(status, &value));
            }
            // Point both aliases at it.
            let actions = json!({ "actions": [
                { "add": { "index": physical, "alias": self.read_alias(kind) } },
                { "add": { "index": physical, "alias": self.write_alias(kind) } }
            ]});
            let (status, value) = self.send(Method::POST, "_aliases", Some(actions)).await?;
            if !status.is_success() {
                return Err(write_status_error(status, &value));
            }
        }
        Ok(())
    }

    async fn create_index_version(
        &self,
        kind: EntityKind,
        suffix: &str,
    ) -> Result<String, SearchError> {
        let physical = self.physical(kind, suffix);
        let (status, value) = self
            .send(Method::PUT, &encode_path(&physical), Some(index_body(kind)))
            .await?;
        if !status.is_success() && !already_exists(&value) {
            return Err(SearchError::ReindexFailed {
                reason: format!("create {physical} failed ({status})"),
            });
        }
        Ok(physical)
    }

    async fn swap_write_alias(
        &self,
        kind: EntityKind,
        physical_index: &str,
    ) -> Result<(), SearchError> {
        self.swap_alias(&self.write_alias(kind), physical_index).await
    }

    async fn swap_read_alias(
        &self,
        kind: EntityKind,
        physical_index: &str,
    ) -> Result<(), SearchError> {
        self.swap_alias(&self.read_alias(kind), physical_index).await
    }
}

impl OpenSearchIndex {
    /// Atomic cutover: drop the alias from wherever it points and add it to the new
    /// physical index, in a single `_aliases` transaction.
    async fn swap_alias(&self, alias: &str, physical_index: &str) -> Result<(), SearchError> {
        let actions = json!({ "actions": [
            { "remove": { "index": "*", "alias": alias } },
            { "add": { "index": physical_index, "alias": alias } }
        ]});
        let (status, _value) = self.send(Method::POST, "_aliases", Some(actions)).await?;
        if !status.is_success() {
            return Err(SearchError::AliasSwapFailed {
                reason: format!("swap {alias} -> {physical_index} failed ({status})"),
            });
        }
        Ok(())
    }
}

// ── Source / DSL builders ─────────────────────────────────────────────────────

/// The flat `_source` content/display object (no guard fields — the script sets
/// `content_version` / `visibility_version` / `searchable`).
fn content_source(doc: &IndexDocument) -> Value {
    match doc {
        IndexDocument::Profile(d) => json!({
            "entity_type": "profile",
            "author_id": d.author_id.as_str(),
            "handle": d.handle,
            "display_name": d.display_name,
            "bio": d.bio,
            "avatar_key": d.avatar_key,
            "verified": d.verified,
            "created_at": d.created_at,
            "indexed_at": d.indexed_at,
            "popularity": d.popularity.value(),
        }),
        IndexDocument::Post(d) => json!({
            "entity_type": "post",
            "author_id": d.author_id.as_str(),
            "caption": d.caption,
            "hashtags": d.hashtags,
            "author_handle": d.author_handle,
            "thumbnail_key": d.thumbnail_key,
            "created_at": d.created_at,
            "indexed_at": d.indexed_at,
            "popularity": d.popularity.value(),
            "visible_until": d.visible_until,
        }),
        IndexDocument::Hashtag(d) => json!({
            "entity_type": "hashtag",
            "tag": d.tag,
            "post_count": d.post_count,
            "indexed_at": d.indexed_at,
            "popularity": d.popularity.value(),
        }),
    }
}

fn search_body(query: &SearchQuery) -> Value {
    // Exclude docs either authority has hidden. `must_not term=false` treats a
    // missing flag as visible, so pre-flag documents still surface.
    let mut must_not = visibility_must_not();
    if !query.exclude_author_ids.is_empty() {
        let ids: Vec<&str> = query
            .exclude_author_ids
            .iter()
            .map(|a| a.as_str())
            .collect();
        must_not.push(json!({ "terms": { "author_id": ids } }));
    }
    let bool_query = json!({
        "must": [{ "multi_match": {
            "query": query.text,
            "fields": MATCH_FIELDS,
            "fuzziness": "AUTO"
        }}],
        "must_not": must_not,
    });
    json!({
        "size": query.page_size,
        "track_total_hits": true,
        "query": { "bool": bool_query },
        "sort": sort_for(query.sort),
    })
}

/// `must_not` clauses excluding any document hidden by any authority (a missing
/// flag counts as visible), and posts past their author's post window (#664; no
/// `visible_until`, no window).
fn visibility_must_not() -> Vec<Value> {
    vec![
        json!({ "term": { "moderation_searchable": false } }),
        json!({ "term": { "owner_searchable": false } }),
        json!({ "term": { "discoverable": false } }),
        json!({ "range": { "visible_until": { "lte": "now" } } }),
    ]
}

fn sort_for(sort: crate::domain::SortStrategy) -> Value {
    use crate::domain::SortStrategy::*;
    match sort {
        Relevance => json!(["_score"]),
        Recency => json!([{ "created_at": "desc" }, "_score"]),
        Popularity => json!([{ "popularity": "desc" }, "_score"]),
    }
}

// ── Response parsing ──────────────────────────────────────────────────────────

fn degraded_empty() -> SearchResults {
    SearchResults {
        hits: Vec::new(),
        next_page_token: None,
        estimated_total: 0,
        degraded: true,
    }
}

fn parse_search(value: &Value, page_size: u32) -> SearchResults {
    let hits: Vec<SearchHit> = value["hits"]["hits"]
        .as_array()
        .map(|arr| arr.iter().filter_map(hit_from_raw).collect())
        .unwrap_or_default();
    let estimated_total = value["hits"]["total"]["value"].as_u64().unwrap_or(0);
    // Any shard failure ⇒ the page is partial; surface it rather than hide it.
    let degraded = value["_shards"]["failed"].as_u64().unwrap_or(0) > 0;
    let mut hits = hits;
    hits.truncate(page_size as usize);
    SearchResults {
        hits,
        next_page_token: None,
        estimated_total,
        degraded,
    }
}

fn hit_from_raw(raw: &Value) -> Option<SearchHit> {
    let src = &raw["_source"];
    let id = raw["_id"].as_str()?.to_owned();
    let score = raw["_score"].as_f64().unwrap_or(0.0) as f32;
    let kind = EntityKind::try_from_str(src["entity_type"].as_str()?).ok()?;
    let (snippet, display) = match kind {
        EntityKind::Profile => (
            str_field(src, "bio"),
            HitDisplay::Profile {
                handle: str_field(src, "handle"),
                display_name: str_field(src, "display_name"),
                avatar_key: str_field(src, "avatar_key"),
                verified: src["verified"].as_bool().unwrap_or(false),
            },
        ),
        EntityKind::Post => (
            str_field(src, "caption"),
            HitDisplay::Post {
                author_id: str_field(src, "author_id"),
                author_handle: str_field(src, "author_handle"),
                thumbnail_key: str_field(src, "thumbnail_key"),
                created_at: src["created_at"]
                    .as_str()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_default(),
            },
        ),
        EntityKind::Hashtag => (
            String::new(),
            HitDisplay::Hashtag {
                tag: str_field(src, "tag"),
                post_count: src["post_count"].as_i64().unwrap_or(0),
            },
        ),
    };
    Some(SearchHit {
        kind,
        id,
        score,
        snippet,
        display,
    })
}

fn parse_suggest(value: &Value) -> Suggestions {
    let suggestions = value["hits"]["hits"]
        .as_array()
        .map(|arr| arr.iter().filter_map(suggestion_from_raw).collect())
        .unwrap_or_default();
    Suggestions { suggestions }
}

fn suggestion_from_raw(raw: &Value) -> Option<Suggestion> {
    let src = &raw["_source"];
    let score = raw["_score"].as_f64().unwrap_or(0.0) as f32;
    let kind = EntityKind::try_from_str(src["entity_type"].as_str()?).ok()?;
    let (text, id) = match kind {
        EntityKind::Profile => (str_field(src, "handle"), raw["_id"].as_str().map(str::to_owned)),
        EntityKind::Hashtag => (str_field(src, "tag"), None),
        EntityKind::Post => return None,
    };
    Some(Suggestion {
        kind,
        text,
        id,
        score,
    })
}

fn str_field(src: &Value, key: &str) -> String {
    src[key].as_str().unwrap_or_default().to_owned()
}

// ── Error / status helpers ────────────────────────────────────────────────────

fn transport_error(err: reqwest::Error) -> SearchError {
    if err.is_timeout() {
        SearchError::EngineTimeout
    } else {
        SearchError::EngineUnavailable
    }
}

fn write_status_error(status: StatusCode, body: &Value) -> SearchError {
    let reason = body["error"]["reason"]
        .as_str()
        .unwrap_or("unknown engine error")
        .to_owned();
    match status {
        StatusCode::NOT_FOUND => SearchError::IndexNotFound {
            index: reason.clone(),
        },
        StatusCode::TOO_MANY_REQUESTS
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::BAD_GATEWAY => SearchError::EngineUnavailable,
        StatusCode::GATEWAY_TIMEOUT => SearchError::EngineTimeout,
        _ => SearchError::BulkIndexFailed { reason },
    }
}

fn already_exists(body: &Value) -> bool {
    body["error"]["type"]
        .as_str()
        .is_some_and(|t| t.contains("resource_already_exists"))
}

fn plural(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Profile => "profiles",
        EntityKind::Post => "posts",
        EntityKind::Hashtag => "hashtags",
    }
}

/// Percent-encode a single URL path segment (ids/tags may contain non-ASCII).
fn encode(segment: &str) -> String {
    segment
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

/// Index names are constructed from a configured prefix + a fixed suffix, so they
/// are already URL-safe; this is a no-op guard for clarity at call sites.
fn encode_path(index: &str) -> String {
    index.to_owned()
}

/// Puts the new entry first, drops the same query (case-insensitive key) and
/// anything past the retention, and keeps at most `max`.
const RECORD_RECENT_SCRIPT: &str = "\
if (ctx._source.entries == null) { ctx._source.entries = []; } \
def kept = [params.entry]; \
for (e in ctx._source.entries) { \
  if (kept.size() >= params.max) { break; } \
  if (e.key != params.entry.key && e.at_ms >= params.cutoff_ms) { kept.add(e); } \
} \
ctx._source.entries = kept;";

const DELETE_RECENT_SCRIPT: &str = "\
if (ctx._source.entries != null) { ctx._source.entries.removeIf(e -> e.key == params.key); }";

#[async_trait]
impl crate::application::port::RecentSearches for OpenSearchIndex {
    async fn record(&self, profile_id: &str, query: &str, at: chrono::DateTime<chrono::Utc>) -> Result<(), SearchError> {
        use crate::domain::recent_search::{normalize_query, query_key, MAX_RECENT_SEARCHES, RECENT_SEARCH_RETENTION};
        let Some(query) = normalize_query(query) else { return Ok(()) };
        let body = json!({
            "scripted_upsert": true,
            "upsert": { "entries": [] },
            "script": {
                "lang": "painless",
                "source": RECORD_RECENT_SCRIPT,
                "params": {
                    "entry": { "q": query, "key": query_key(&query), "at_ms": at.timestamp_millis() },
                    "max": MAX_RECENT_SEARCHES,
                    "cutoff_ms": (at - RECENT_SEARCH_RETENTION).timestamp_millis(),
                }
            }
        });
        let path = format!("{}/_update/{}?retry_on_conflict=3", encode_path(&self.recent_index()), encode(profile_id));
        let (status, value) = self.send(Method::POST, &path, Some(body)).await?;
        if !status.is_success() {
            return Err(write_status_error(status, &value));
        }
        Ok(())
    }

    async fn list(
        &self,
        profile_id: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<crate::domain::recent_search::RecentSearch>, SearchError> {
        use crate::domain::recent_search::{RecentSearch, RECENT_SEARCH_RETENTION};
        let path = format!("{}/_doc/{}", encode_path(&self.recent_index()), encode(profile_id));
        let (status, value) = self.send(Method::GET, &path, None).await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if !status.is_success() {
            return Err(write_status_error(status, &value));
        }
        let cutoff = (now - RECENT_SEARCH_RETENTION).timestamp_millis();
        Ok(value["_source"]["entries"]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|e| {
                        let at_ms = e["at_ms"].as_i64()?;
                        let at = chrono::DateTime::from_timestamp_millis(at_ms)?;
                        Some(RecentSearch { query: e["q"].as_str()?.to_owned(), at }).filter(|_| at_ms >= cutoff)
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn delete(&self, profile_id: &str, query: &str) -> Result<(), SearchError> {
        use crate::domain::recent_search::{normalize_query, query_key};
        let Some(query) = normalize_query(query) else { return Ok(()) };
        let body = json!({
            "script": { "lang": "painless", "source": DELETE_RECENT_SCRIPT, "params": { "key": query_key(&query) } }
        });
        let path = format!("{}/_update/{}?retry_on_conflict=3", encode_path(&self.recent_index()), encode(profile_id));
        let (status, value) = self.send(Method::POST, &path, Some(body)).await?;
        if !status.is_success() && status != StatusCode::NOT_FOUND {
            return Err(write_status_error(status, &value));
        }
        Ok(())
    }

    async fn clear(&self, profile_id: &str) -> Result<(), SearchError> {
        let path = format!("{}/_doc/{}", encode_path(&self.recent_index()), encode(profile_id));
        let (status, value) = self.send(Method::DELETE, &path, None).await?;
        if !status.is_success() && status != StatusCode::NOT_FOUND {
            return Err(write_status_error(status, &value));
        }
        Ok(())
    }
}

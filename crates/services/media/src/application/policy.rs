use std::time::Duration as StdDuration;

use chrono::Duration;

/// Tunable policy the media handlers run under. Injected at the composition root
/// (`MediaConfig::from_env`, Phase 5); the domain ships sane defaults so behaviour
/// is well-defined from day one.
#[derive(Debug, Clone)]
pub struct MediaPolicy {
    /// How long an issued pre-signed upload ticket stays valid.
    pub upload_ticket_ttl: Duration,
    /// Lifetime of a minted signed (private) delivery URL.
    pub signed_url_ttl: Duration,
    /// **Content-hash dedup gate (fork B).** When false (the default), every upload
    /// gets a fresh asset + ticket; when true, an incoming upload whose declared
    /// SHA-256 matches an existing READY asset short-circuits to it. Off until the
    /// refcount-aware purge path has live integration coverage.
    pub dedup_enabled: bool,
    /// Hard timeout for the pre-publish moderation Screen call — a slow gate must
    /// not wedge the pipeline (fail-closed on elapse for CSAM-class).
    pub screen_timeout: StdDuration,
    /// `MEDIA_PRIVATE_DOCUMENTS_ENABLED` (default **false**, #777): whether
    /// private documents may be uploaded. Off until the infra keeps `private/`
    /// out of the CDN's origin access (core-platform-infra#37); off, a
    /// `PRIVATE_DOCUMENT` upload ticket is refused (`MED-1006`).
    pub private_documents_enabled: bool,
}

impl MediaPolicy {
    /// Production defaults: 15-minute upload window, 5-minute signed URLs, dedup
    /// OFF, 200 ms screen timeout.
    pub fn standard() -> Self {
        Self {
            upload_ticket_ttl: Duration::minutes(15),
            signed_url_ttl: Duration::minutes(5),
            dedup_enabled: false,
            screen_timeout: StdDuration::from_millis(200),
            private_documents_enabled: false,
        }
    }

    /// Deterministic defaults for unit tests: the production defaults, with
    /// private documents on (the tests exercise them; the off state has its own
    /// test).
    #[cfg(test)]
    pub fn test_default() -> Self {
        Self { private_documents_enabled: true, ..Self::standard() }
    }
}

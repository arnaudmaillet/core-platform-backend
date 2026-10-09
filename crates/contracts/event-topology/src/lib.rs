//! Fleet Kafka topic topology — the single source of truth for which service
//! **produces** and which **consumes** every Kafka stream, plus the contract
//! test that fails the build on a *phantom edge*: a consumer subscribing to a
//! topic that no producer emits.
//!
//! ## Why this crate exists
//!
//! Kafka payloads are JSON and topic names are bare string literals scattered
//! across `const TOPIC: &str = "…"` declarations. A misnamed topic or a
//! never-built producer therefore **does not fail to compile** — the consumer
//! just sits idle forever, or poison-DLQs every message. `buf breaking` guards
//! the gRPC *proto* contracts; nothing guarded the *event-streaming* contracts.
//! This registry does.
//!
//! Three real phantom edges (geo-discovery `tier_sync` on a never-produced
//! `profile.tier_changed`, geo-discovery `score_updater` on a never-produced
//! `engagement.score_updated`, and the half-migrated `post.published` family)
//! shipped silently because there was no such gate. This catches the next one.
//!
//! ## How to keep it honest
//!
//! When you add or remove a Kafka producer or consumer **in the same PR**:
//!
//! - add/remove its edge in [`PRODUCERS`] or [`CONSUMERS`];
//! - if a consumer subscribes to a topic whose producer is intentionally not
//!   built in-repo (external system, or a documented deferral), list the topic
//!   in [`DEFERRED`] **with a reason** — otherwise the test fails;
//! - if a producer's topic has no in-repo consumer (intentional headroom), list
//!   it in [`ORPHAN_PRODUCERS`] — otherwise the test fails.
//!
//! The tests reject *stale* entries too: a `DEFERRED` topic that someone has
//! since wired a producer for, or an `ORPHAN_PRODUCERS` topic that now has a
//! consumer, both fail — so the registry can't rot into a pile of excuses.
//!
//! NB: this guards topic **names and wiring**, not payload **shapes**. A
//! producer and consumer can agree on a topic and still disagree on the JSON
//! body (see the post→geo/notification payload gap). Shape compatibility is a
//! separate concern (per-stream wire-lock tests live with each service).

/// Every Kafka topic that some fleet service emits, paired with its owning
/// (producing) service. A topic should have exactly one producer service.
pub const PRODUCERS: &[(&str, &str)] = &[
    // account
    ("account.v1.events", "account"),
    // profile
    ("profile.v1.events", "profile"),
    // wallet — likes staked on posts and comments (#665: a like is a point)
    ("wallet.v1.events", "wallet"),
    // notification — per-recipient realtime push stream (consumed by realtime)
    ("notification.v1.events", "notification"),
    // post — legacy per-type topics + the unified v1 stream (mid-migration)
    ("post.published", "post"),
    ("post.updated", "post"),
    ("post.deleted", "post"),
    ("post.v1.events", "post"),
    // comment
    ("comment.created", "comment"),
    ("comment.deleted", "comment"),
    // social-graph
    ("social-graph.followed", "social-graph"),
    ("social-graph.unfollowed", "social-graph"),
    ("social-graph.blocked", "social-graph"),
    ("social-graph.author_tier_changed", "social-graph"),
    ("social-graph.follow_requested", "social-graph"),
    // chat (its own delivery plane)
    ("chat.conversation.created", "chat"),
    ("chat.conversation.published", "chat"),
    ("chat.conversation.unpublished", "chat"),
    ("chat.member.joined", "chat"),
    ("chat.member.left", "chat"),
    ("chat.message.sent", "chat"),
    // a message's push, for the members it reaches (#654)
    ("chat.message.push", "chat"),
    // counter
    ("counter.v1.popularity", "counter"),
    // moderation
    ("moderation.v1.events", "moderation"),
    // auth
    ("auth.v1.events", "auth"),
    // media
    ("media.v1.events", "media"),
    // staff views of private documents, on audit's generic lane (#837)
    ("audit.v1.events", "media"),
];

/// Every Kafka subscription in the fleet, paired with its consuming service.
/// A topic may have several consumers.
pub const CONSUMERS: &[(&str, &str)] = &[
    // account lifecycle → compliance plane + profile projection + auth's GDPR
    // erasure (account_deleted → sessions, identity links, guest records)
    ("account.v1.events", "audit"),
    ("account.v1.events", "auth"),
    ("account.v1.events", "profile"),
    // account_deleted → media erases the account's media (GDPR Art. 17, #777)
    ("account.v1.events", "media"),
    // account_deleted → the wallet erases the account's ledger (#665)
    ("account.v1.events", "wallet"),
    // profile lifecycle → search index, post author-tier denormalization,
    // social-graph audience projection (access check), geo-discovery authors'
    // location sharing (ghost / city level on the map)
    ("profile.v1.events", "search"),
    ("profile.v1.events", "post"),
    ("profile.v1.events", "social-graph"),
    ("profile.v1.events", "geo-discovery"),
    // a deleted profile's interest tags (#662) → timeline erases them
    ("profile.v1.events", "timeline"),
    // a decided verification request → media purges its private documents
    // 30 days later (#777)
    ("profile.v1.events", "media"),
    // post (legacy)
    ("post.published", "notification"),
    ("post.published", "geo-discovery"),
    ("post.deleted", "timeline"),
    ("post.deleted", "geo-discovery"),
    // post (unified v1)
    ("post.v1.events", "timeline"),
    ("post.v1.events", "search"),
    ("post.v1.events", "realtime"),
    // notification → realtime device push
    ("notification.v1.events", "realtime"),
    // comment
    ("comment.created", "notification"),
    ("comment.created", "engagement"),
    ("comment.deleted", "engagement"),
    // likes are points (#665): the wallet's stakes → counter's like magnitude,
    // the "X liked" notice (a liker's first batch), the interest tags (#662:
    // a first like teaches the post's hashtags), each country's likes (the
    // ladder that prices unlocks)
    ("wallet.v1.events", "counter"),
    ("wallet.v1.events", "notification"),
    ("wallet.v1.events", "timeline"),
    ("wallet.v1.events", "geo-discovery"),
    // follows, follow requests and their approval (#755)
    ("social-graph.followed", "notification"),
    ("social-graph.follow_requested", "notification"),
    // family supervision notices (#670)
    ("account.v1.events", "notification"),
    // chat messages' pushes (#654)
    ("chat.message.push", "notification"),
    // likes are points (#665): the wallet's stakes become the like counts
    ("wallet.v1.events", "engagement"),
    // a deleted account's likes: who liked goes, the counts stay (#665)
    ("account.v1.events", "engagement"),
    // whether each profile shows its Likes tab (#829)
    ("profile.v1.events", "engagement"),
    // social-graph edges → timeline fan-out, profile tier ownership
    ("social-graph.followed", "timeline"),
    ("social-graph.unfollowed", "timeline"),
    ("social-graph.author_tier_changed", "profile"),
    // profile hidden words / offensive filter → comment reads
    ("profile.v1.events", "comment"),
    // chat visibility teardown (self-consume)
    ("chat.conversation.unpublished", "chat"),
    // each member's inbox (#656), projected from chat's own facts (self-consume)
    ("chat.message.sent", "chat"),
    ("chat.conversation.created", "chat"),
    ("chat.member.joined", "chat"),
    ("chat.member.left", "chat"),
    // profile presence settings (activity status, read receipts) → chat
    ("profile.v1.events", "chat"),
    // counter popularity → realtime broadcast + geo virality re-score +
    // timeline discovery hot score
    ("counter.v1.popularity", "realtime"),
    ("counter.v1.popularity", "geo-discovery"),
    ("counter.v1.popularity", "timeline"),
    // auth lifecycle → compliance plane
    ("auth.v1.events", "audit"),
    // moderation decisions → compliance plane, search visibility, media takedown,
    // post read restriction, map suppression, discovery feed suppression,
    // appeal outcomes told to the appellant's profiles
    ("moderation.v1.events", "audit"),
    ("moderation.v1.events", "search"),
    ("moderation.v1.events", "media"),
    ("moderation.v1.events", "post"),
    ("moderation.v1.events", "geo-discovery"),
    ("moderation.v1.events", "timeline"),
    ("moderation.v1.events", "notification"),
    // media lifecycle → Plane-B processing pipeline (self-consume)
    ("media.v1.events", "media"),
    // audit generic ingest lane (media's private-document views, #837)
    ("audit.v1.events", "audit"),
    // moderation intake lanes (see DEFERRED — external producers)
    ("moderation.reports", "moderation"),
    ("moderation.signals", "moderation"),
    // counter telemetry + follow folds (see DEFERRED — producers not built)
    ("view.v1.events", "counter"),
    ("impression.v1.events", "counter"),
    ("click.v1.events", "counter"),
    ("social-graph.follows", "counter"),
];

/// Topics a consumer subscribes to whose producer is **intentionally** not built
/// in-repo: external systems, or documented roadmap deferrals. Each entry needs
/// a reason. Keeps the phantom-edge test honest without hiding the gap.
pub const DEFERRED: &[(&str, &str)] = &[
    (
        "moderation.reports",
        "External user-report intake — produced by the client/edge, not a fleet \
         service.",
    ),
    (
        "moderation.signals",
        "External ML-classifier signals — produced off-fleet.",
    ),
    (
        "view.v1.events",
        "Upstream view telemetry producer not yet built (counter-analytics \
         blueprint deferral).",
    ),
    (
        "impression.v1.events",
        "Upstream impression telemetry producer not yet built (counter \
         deferral).",
    ),
    (
        "click.v1.events",
        "Upstream click telemetry producer not yet built (counter deferral).",
    ),
    (
        "social-graph.follows",
        "Counter wants a single combined follow stream; social-graph emits the \
         split past-tense social-graph.followed/.unfollowed instead. Combined \
         producer is deferred — TRACKED NAMING MISMATCH, not just a missing emitter.",
    ),
];

/// Topics a producer emits that have **no** in-repo consumer: intentional
/// headroom or read-path-enforced concerns. Each entry needs a reason.
pub const ORPHAN_PRODUCERS: &[(&str, &str)] = &[
    (
        "post.updated",
        "No stream consumer — search/timeline/realtime act on post.v1.events \
         PostUpdated; the legacy per-type topic is emitted for completeness.",
    ),
    (
        "social-graph.blocked",
        "Block is enforced on the gRPC read path; no stream consumer yet.",
    ),
    (
        "chat.conversation.published",
        "Chat delivery-plane headroom.",
    ),
];

/// The set of valid service names, to catch typos in the tables above.
pub const SERVICES: &[&str] = &[
    "account",
    "profile",
    "social-graph",
    "post",
    "engagement",
    "comment",
    "geo-discovery",
    "notification",
    "timeline",
    "chat",
    "auth",
    "moderation",
    "search",
    "media",
    "counter",
    "realtime",
    "audit",
    "wallet",
];

// ---------------------------------------------------------------------------------------------
// Event-catalog generation
//
// docs/domain/EVENT_CATALOG.md carries a machine-generated "topic wiring" block (topics →
// producer → consumers, plus the deferred and orphan tables) rendered from the registry above, so
// the wiring half of the catalog cannot drift from the contract tests. Humans author only the
// *semantic* tables (what each event means); this block owns the *wiring*.
//
// Regenerate with `cargo run -p event-topology --bin gen-event-catalog`
// (or `tools/event-catalog/sync.sh --write`). The golden test below fails the build if the block
// committed in the doc no longer matches the registry.
// ---------------------------------------------------------------------------------------------

/// Opening marker of the generated block in EVENT_CATALOG.md (and its translations).
pub const CATALOG_BEGIN: &str =
    "<!-- BEGIN GENERATED: topic-wiring · source crates/contracts/event-topology · do not edit by hand -->";
/// Closing marker of the generated block.
pub const CATALOG_END: &str = "<!-- END GENERATED: topic-wiring -->";

/// Consumers of `topic`, in registry (source) order.
fn consumers_of(topic: &str) -> Vec<&'static str> {
    CONSUMERS
        .iter()
        .filter(|(t, _)| *t == topic)
        .map(|(_, s)| *s)
        .collect()
}

fn code_list(items: &[&str]) -> String {
    if items.is_empty() {
        "—".to_string()
    } else {
        items
            .iter()
            .map(|s| format!("`{s}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Render the complete generated topic-wiring block (markers included) from the registry.
/// Deterministic: ordering follows the source order of [`PRODUCERS`] / [`DEFERRED`] /
/// [`ORPHAN_PRODUCERS`].
pub fn render_catalog_block() -> String {
    let mut out = String::new();
    out.push_str(CATALOG_BEGIN);
    out.push('\n');
    out.push_str(
        "> ⚙️ Generated from the event-topology registry \
         (`crates/contracts/event-topology`). Do not edit by hand — change the registry and run \
         `cargo run -p event-topology --bin gen-event-catalog` (or `tools/event-catalog/sync.sh \
         --write`). The *meaning* of each event is authored in the semantic sections below.\n\n",
    );

    out.push_str("### Produced topics → consumers\n\n");
    out.push_str("| Topic | Producer | Consumers |\n|---|---|---|\n");
    for (topic, producer) in PRODUCERS {
        let consumers = consumers_of(topic);
        let cell = if consumers.is_empty() {
            "— *(orphan — see below)*".to_string()
        } else {
            code_list(&consumers)
        };
        out.push_str(&format!("| `{topic}` | `{producer}` | {cell} |\n"));
    }

    out.push_str("\n### Deferred — consumed, producer intentionally not in-repo\n\n");
    out.push_str("| Topic | Consumer(s) | Why |\n|---|---|---|\n");
    for (topic, reason) in DEFERRED {
        out.push_str(&format!(
            "| `{topic}` | {} | {reason} |\n",
            code_list(&consumers_of(topic))
        ));
    }

    out.push_str("\n### Orphan producers — produced, no in-repo consumer\n\n");
    out.push_str("| Topic | Producer | Why |\n|---|---|---|\n");
    for (topic, reason) in ORPHAN_PRODUCERS {
        let producer = PRODUCERS
            .iter()
            .find(|(t, _)| t == topic)
            .map(|(_, s)| *s)
            .unwrap_or("—");
        out.push_str(&format!("| `{topic}` | `{producer}` | {reason} |\n"));
    }

    out.push('\n');
    out.push_str(CATALOG_END);
    out
}

/// Extract the `CATALOG_BEGIN..=CATALOG_END` block from a document, if present.
pub fn extract_catalog_block(doc: &str) -> Option<&str> {
    let start = doc.find(CATALOG_BEGIN)?;
    let end = doc[start..].find(CATALOG_END)? + start + CATALOG_END.len();
    Some(&doc[start..end])
}

// ---------------------------------------------------------------------------------------------
// Broker provisioning views
//
// Brokers run with auto.create.topics.enable=false (MSK policy — auto-created topics inherit
// broker defaults nobody chose), so every topic must exist before the fleet produces or
// subscribes. The topic-provisioner app derives the broker's topic set from these views; the
// registry stays the single source of truth for what exists on the wire.
// ---------------------------------------------------------------------------------------------

/// Every stream topic the brokers must have: the deduplicated union of produced and consumed
/// topics. DEFERRED subscriptions are included on purpose — their producers are external or
/// roadmap, but the consumer's subscription needs the topic to exist once auto-creation is off.
///
/// Dead-letter counterparts are NOT listed here; derive them from
/// [`consumed_stream_topics`] with the consumer runtime's `<topic>.dlq` convention
/// (`transport::kafka::DLQ_SUFFIX`) — only consumed topics can dead-letter.
pub fn all_stream_topics() -> Vec<&'static str> {
    let mut topics: Vec<&'static str> = PRODUCERS
        .iter()
        .chain(CONSUMERS.iter())
        .map(|(topic, _)| *topic)
        .collect();
    topics.sort_unstable();
    topics.dedup();
    topics
}

/// Topics kept shorter than the broker default (in ms), and their `.dlq`: a
/// topic carrying user content that no erasure can reach on the broker ages out
/// fast instead.
pub const RETENTION: &[(&str, i64)] = &[
    // A message's push carries its first 100 characters (#654); the consumer
    // skips anything older than a day anyway.
    ("chat.message.push", 24 * 3_600_000),
    // Every message's full body: chat keeps it in its own store, where
    // erasure and moderation reach it. The broker copy only feeds the inbox
    // projection, so two days of lag is the recovery window.
    ("chat.message.sent", 2 * 24 * 3_600_000),
];

/// The retention of `topic` (a `.dlq` follows its topic), when it is not the
/// broker default.
pub fn retention_ms(topic: &str) -> Option<i64> {
    let base = topic.strip_suffix(".dlq").unwrap_or(topic);
    RETENTION.iter().find(|(t, _)| *t == base).map(|(_, ms)| *ms)
}

/// Deduplicated set of consumed topics — the subscriptions whose `run_consumer` loops can
/// dead-letter, i.e. the topics that need a `<topic>.dlq` counterpart on the broker.
pub fn consumed_stream_topics() -> Vec<&'static str> {
    let mut topics: Vec<&'static str> = CONSUMERS.iter().map(|(topic, _)| *topic).collect();
    topics.sort_unstable();
    topics.dedup();
    topics
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn produced_topics() -> HashSet<&'static str> {
        PRODUCERS.iter().map(|(t, _)| *t).collect()
    }
    fn consumed_topics() -> HashSet<&'static str> {
        CONSUMERS.iter().map(|(t, _)| *t).collect()
    }
    fn deferred_topics() -> HashSet<&'static str> {
        DEFERRED.iter().map(|(t, _)| *t).collect()
    }
    fn orphan_topics() -> HashSet<&'static str> {
        ORPHAN_PRODUCERS.iter().map(|(t, _)| *t).collect()
    }

    /// The provisioning view must cover every wired topic exactly once — the
    /// broker set the topic-provisioner creates is complete and duplicate-free.
    #[test]
    fn all_stream_topics_is_the_deduplicated_union_of_the_registry() {
        let all = all_stream_topics();

        let mut expected: HashSet<&str> = produced_topics();
        expected.extend(consumed_topics());
        assert_eq!(all.iter().copied().collect::<HashSet<_>>(), expected);

        let mut deduped = all.clone();
        deduped.dedup();
        assert_eq!(all, deduped, "provisioning view contains duplicates");
    }

    /// Only consumed topics dead-letter; the DLQ view must match CONSUMERS.
    #[test]
    fn consumed_stream_topics_matches_the_consumer_table() {
        assert_eq!(
            consumed_stream_topics().into_iter().collect::<HashSet<_>>(),
            consumed_topics()
        );
    }

    /// The headline guard: nothing may consume a topic that no producer emits,
    /// unless the gap is explicitly deferred with a reason.
    #[test]
    fn every_consumed_topic_has_a_producer_or_is_deferred() {
        let produced = produced_topics();
        let deferred = deferred_topics();

        let phantom: Vec<_> = consumed_topics()
            .into_iter()
            .filter(|t| !produced.contains(t) && !deferred.contains(t))
            .collect();

        assert!(
            phantom.is_empty(),
            "PHANTOM CONSUMER EDGE(S): these topics are consumed but no service \
             produces them and they are not in DEFERRED — either wire a producer, \
             fix the topic name, or add a DEFERRED entry with a reason: {phantom:?}"
        );
    }

    /// A deferral that has since been fulfilled (a producer now emits it) must be
    /// removed from DEFERRED so the registry stays truthful.
    #[test]
    fn no_deferred_topic_is_actually_produced() {
        let produced = produced_topics();
        let stale: Vec<_> = deferred_topics()
            .into_iter()
            .filter(|t| produced.contains(t))
            .collect();

        assert!(
            stale.is_empty(),
            "STALE DEFERRAL: these topics now have a producer — remove them from \
             DEFERRED: {stale:?}"
        );
    }

    /// Every produced-but-unconsumed topic must be acknowledged as an intentional
    /// orphan; a *new* orphan (someone added a producer with no consumer and no
    /// allowlist entry) fails the build.
    #[test]
    fn every_produced_topic_is_consumed_or_an_acknowledged_orphan() {
        let consumed = consumed_topics();
        let orphans = orphan_topics();

        let unaccounted: Vec<_> = produced_topics()
            .into_iter()
            .filter(|t| !consumed.contains(t) && !orphans.contains(t))
            .collect();

        assert!(
            unaccounted.is_empty(),
            "UNCONSUMED PRODUCER: these topics are produced but nobody consumes \
             them and they are not in ORPHAN_PRODUCERS — wire a consumer or \
             acknowledge the orphan with a reason: {unaccounted:?}"
        );
    }

    /// An orphan that has since gained a consumer must leave the allowlist.
    #[test]
    fn no_orphan_producer_is_actually_consumed() {
        let consumed = consumed_topics();
        let stale: Vec<_> = orphan_topics()
            .into_iter()
            .filter(|t| consumed.contains(t))
            .collect();

        assert!(
            stale.is_empty(),
            "STALE ORPHAN: these topics now have a consumer — remove them from \
             ORPHAN_PRODUCERS: {stale:?}"
        );
    }

    /// Topic ownership is singular: exactly one service may produce a topic.
    #[test]
    fn each_topic_has_a_single_producer() {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut dup: Vec<&str> = Vec::new();
        for (topic, _service) in PRODUCERS {
            if !seen.insert(topic) {
                dup.push(topic);
            }
        }
        assert!(
            dup.is_empty(),
            "DUPLICATE PRODUCER: these topics are claimed by more than one \
             producer service: {dup:?}"
        );
    }

    /// A topic can't be both a documented deferral and a documented orphan.
    #[test]
    fn deferred_and_orphan_sets_are_disjoint() {
        let overlap: Vec<_> = deferred_topics()
            .intersection(&orphan_topics())
            .copied()
            .collect();
        assert!(
            overlap.is_empty(),
            "A topic is in both DEFERRED and ORPHAN_PRODUCERS: {overlap:?}"
        );
    }

    /// Every service name in the edge tables must be a known service (typo guard).
    #[test]
    fn all_referenced_services_are_known() {
        let known: HashSet<&str> = SERVICES.iter().copied().collect();
        let unknown: Vec<_> = PRODUCERS
            .iter()
            .chain(CONSUMERS.iter())
            .map(|(_, s)| *s)
            .filter(|s| !known.contains(s))
            .collect();
        assert!(
            unknown.is_empty(),
            "UNKNOWN SERVICE NAME in registry (typo?): {unknown:?}"
        );
    }

    /// DEFERRED and ORPHAN_PRODUCERS reasons must be non-empty — no silent excuses.
    #[test]
    fn every_exception_carries_a_reason() {
        for (topic, reason) in DEFERRED.iter().chain(ORPHAN_PRODUCERS.iter()) {
            assert!(
                !reason.trim().is_empty(),
                "topic {topic:?} is excepted without a reason"
            );
        }
    }

    // --- generated event-catalog block stays in sync with the registry -----------------------

    const EVENT_CATALOG: &str =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../../docs/domain/EVENT_CATALOG.md"));
    const EVENT_CATALOG_FR: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../docs/domain/EVENT_CATALOG.fr.md"
    ));

    /// The generated wiring block committed in the English catalog must match the registry.
    #[test]
    fn generated_block_matches_registry_en() {
        let committed = extract_catalog_block(EVENT_CATALOG)
            .expect("BEGIN/END markers missing in docs/domain/EVENT_CATALOG.md");
        assert_eq!(
            committed,
            render_catalog_block(),
            "docs/domain/EVENT_CATALOG.md generated block is STALE — \
             run `tools/event-catalog/sync.sh --write` and commit"
        );
    }

    /// The French mirror carries the identical (language-invariant) wiring block.
    #[test]
    fn generated_block_matches_registry_fr() {
        let committed = extract_catalog_block(EVENT_CATALOG_FR)
            .expect("BEGIN/END markers missing in docs/domain/EVENT_CATALOG.fr.md");
        assert_eq!(
            committed,
            render_catalog_block(),
            "docs/domain/EVENT_CATALOG.fr.md generated block is STALE — \
             run `tools/event-catalog/sync.sh --write` and commit"
        );
    }

    /// A retention names a registry topic, and covers its DLQ.
    #[test]
    fn retentions_name_registry_topics() {
        let topics = all_stream_topics();
        for (topic, ms) in RETENTION {
            assert!(topics.contains(topic), "{topic} is not in the registry");
            assert!(*ms > 0);
        }
        assert_eq!(retention_ms("chat.message.push.dlq"), Some(24 * 3_600_000));
        assert_eq!(retention_ms("chat.message.sent"), Some(2 * 24 * 3_600_000));
        assert_eq!(retention_ms("chat.member.joined"), None);
    }
}

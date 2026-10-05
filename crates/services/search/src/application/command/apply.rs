//! The write-side use case: project an inbound source event and apply the
//! resulting mutation to the index. This is the single handler every ingestion
//! consumer drives (the per-topic split lives in the Phase-4 decode layer, not
//! here) — the projector already unifies all source events into one mutation
//! vocabulary.
//!
//! Like `auth`/`moderation`, it is a plain application-service struct (not a
//! `cqrs::CommandHandler`) so it can return a rich [`ApplyOutcome`] the consumer
//! uses for metrics; it takes a [`cqrs::Envelope`] (correlation id for tracing) and
//! an injected `now`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::port::{SearchIndex, WriteOutcome};
use crate::domain::{IndexMutation, SkipReason, SourceEvent, project};
use crate::error::SearchError;

/// What applying a source event did to the index. Every variant is a success the
/// consumer commits; the distinctions exist for observability.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    /// A document's content was indexed/replaced.
    Indexed,
    /// A moderation-visibility flag was flipped.
    VisibilityUpdated,
    /// A document was hard-deleted.
    Deleted,
    /// A GDPR purge removed this many documents.
    Purged(u64),
    /// An author's post window was applied to this many of their posts.
    WindowApplied(u64),
    /// The engine's external-version guard rejected a stale/out-of-order write.
    StaleIgnored,
    /// The event mapped to no index change (e.g. a non-indexable moderated entity).
    Skipped(SkipReason),
}

pub struct ProjectionHandler {
    index: Arc<dyn SearchIndex>,
}

impl ProjectionHandler {
    pub fn new(index: Arc<dyn SearchIndex>) -> Self {
        Self { index }
    }

    pub async fn apply(
        &self,
        envelope: Envelope<SourceEvent>,
        now: DateTime<Utc>,
    ) -> Result<ApplyOutcome, SearchError> {
        let outcome = match project(envelope.payload, now)? {
            IndexMutation::Upsert(document) => match self.index.upsert(&document).await? {
                WriteOutcome::Applied => ApplyOutcome::Indexed,
                WriteOutcome::RejectedStale => ApplyOutcome::StaleIgnored,
            },
            IndexMutation::SetSearchable {
                authority,
                kind,
                id,
                searchable,
                version,
            } => match self
                .index
                .set_searchable(authority, kind, &id, searchable, version)
                .await?
            {
                WriteOutcome::Applied => ApplyOutcome::VisibilityUpdated,
                WriteOutcome::RejectedStale => ApplyOutcome::StaleIgnored,
            },
            IndexMutation::Delete { kind, id } => {
                self.index.delete(kind, &id).await?;
                ApplyOutcome::Deleted
            }
            IndexMutation::PurgeByAuthor { author_id } => {
                let removed = self.index.purge_by_author(&author_id).await?;
                ApplyOutcome::Purged(removed)
            }
            IndexMutation::SetPostWindow {
                author_id,
                window_days,
                version,
            } => {
                let updated = self.index.set_post_window(&author_id, window_days, version).await?;
                ApplyOutcome::WindowApplied(updated)
            }
            IndexMutation::Skip(reason) => ApplyOutcome::Skipped(reason),
        };
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use error::AppError;
    use uuid::Uuid;

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::application::fakes::{Fixture, post_event, profile_event};
    use crate::domain::{
        ComplianceEvent, EntityDeletion, EntityKind, ModerationEvent, PostEvent, ProfileEvent,
        VisibilityChange,
    };

    fn env(event: SourceEvent) -> Envelope<SourceEvent> {
        Envelope::new(Uuid::now_v7(), event)
    }

    #[tokio::test]
    async fn moderation_and_owner_visibility_are_independent() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        let t1 = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
        let t2 = Utc.timestamp_opt(1_700_000_200, 0).unwrap();

        h.apply(env(profile_event("prof-1", "alice", 1)), fx.now())
            .await
            .unwrap();
        assert!(fx.index.is_visible(EntityKind::Profile, "prof-1"));

        // Moderation hides it.
        h.apply(
            env(SourceEvent::Moderation(ModerationEvent::VisibilityRevoked(
                VisibilityChange {
                    kind: Some(EntityKind::Profile),
                    id: "prof-1".to_owned(),
                    occurred_at: t1,
                },
            ))),
            fx.now(),
        )
        .await
        .unwrap();
        assert!(!fx.index.is_visible(EntityKind::Profile, "prof-1"));

        // The owner "restores" their own visibility — must NOT override moderation.
        h.apply(
            env(SourceEvent::Profile(ProfileEvent::OwnerRestored {
                profile_id: "prof-1".to_owned(),
                occurred_at: t2,
            })),
            fx.now(),
        )
        .await
        .unwrap();
        assert!(
            !fx.index.is_visible(EntityKind::Profile, "prof-1"),
            "an owner restore must not lift a moderation hide"
        );

        // Moderation lifts its own hide → both authorities now permit → visible.
        h.apply(
            env(SourceEvent::Moderation(ModerationEvent::VisibilityRestored(
                VisibilityChange {
                    kind: Some(EntityKind::Profile),
                    id: "prof-1".to_owned(),
                    occurred_at: t2,
                },
            ))),
            fx.now(),
        )
        .await
        .unwrap();
        assert!(fx.index.is_visible(EntityKind::Profile, "prof-1"));
    }

    #[tokio::test]
    async fn moderation_cannot_lift_an_owner_mask() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        let t1 = Utc.timestamp_opt(1_700_000_100, 0).unwrap();

        h.apply(env(profile_event("prof-1", "alice", 1)), fx.now())
            .await
            .unwrap();
        // Owner masks themselves.
        h.apply(
            env(SourceEvent::Profile(ProfileEvent::OwnerHidden {
                profile_id: "prof-1".to_owned(),
                occurred_at: t1,
            })),
            fx.now(),
        )
        .await
        .unwrap();
        assert!(!fx.index.is_visible(EntityKind::Profile, "prof-1"));

        // A moderation "restore" must not reveal an owner-masked profile.
        h.apply(
            env(SourceEvent::Moderation(ModerationEvent::VisibilityRestored(
                VisibilityChange {
                    kind: Some(EntityKind::Profile),
                    id: "prof-1".to_owned(),
                    occurred_at: t1,
                },
            ))),
            fx.now(),
        )
        .await
        .unwrap();
        assert!(
            !fx.index.is_visible(EntityKind::Profile, "prof-1"),
            "a moderation restore must not lift an owner mask"
        );
    }

    #[tokio::test]
    async fn publishes_then_serves_the_document() {
        let fx = Fixture::new();
        let out = fx
            .projection_handler()
            .apply(env(post_event("post-1", "acct-1", "hello rust", 1)), fx.now())
            .await
            .unwrap();
        assert_eq!(out, ApplyOutcome::Indexed);
        assert!(fx.index.is_visible(EntityKind::Post, "post-1"));
    }

    #[tokio::test]
    async fn stale_edit_is_ignored_by_the_version_guard() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        h.apply(env(post_event("post-1", "acct-1", "v5", 5)), fx.now())
            .await
            .unwrap();
        // An older revision arrives late.
        let out = h
            .apply(env(post_event("post-1", "acct-1", "v3 (late)", 3)), fx.now())
            .await
            .unwrap();
        assert_eq!(out, ApplyOutcome::StaleIgnored);
        assert_eq!(fx.index.caption(EntityKind::Post, "post-1").unwrap(), "v5");
    }

    #[tokio::test]
    async fn moderation_hide_survives_a_later_content_edit() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        h.apply(env(post_event("post-1", "acct-1", "orig", 1)), fx.now())
            .await
            .unwrap();
        // Moderation hides it…
        let occurred = fx.now();
        h.apply(
            env(SourceEvent::Moderation(ModerationEvent::VisibilityRevoked(
                VisibilityChange {
                    kind: Some(EntityKind::Post),
                    id: "post-1".to_owned(),
                    occurred_at: occurred,
                },
            ))),
            fx.now(),
        )
        .await
        .unwrap();
        assert!(!fx.index.is_visible(EntityKind::Post, "post-1"));
        // …then a newer content edit arrives. The doc updates but stays hidden.
        let out = h
            .apply(env(post_event("post-1", "acct-1", "edited", 2)), fx.now())
            .await
            .unwrap();
        assert_eq!(out, ApplyOutcome::Indexed);
        assert_eq!(fx.index.caption(EntityKind::Post, "post-1").unwrap(), "edited");
        assert!(
            !fx.index.is_visible(EntityKind::Post, "post-1"),
            "a content re-projection must not un-hide a moderated document"
        );
    }

    #[tokio::test]
    async fn hide_racing_ahead_of_content_is_honoured_on_arrival() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        // The hide arrives BEFORE the post is ever indexed (cross-topic reorder).
        h.apply(
            env(SourceEvent::Moderation(ModerationEvent::VisibilityRevoked(
                VisibilityChange {
                    kind: Some(EntityKind::Post),
                    id: "post-1".to_owned(),
                    occurred_at: fx.now(),
                },
            ))),
            fx.now(),
        )
        .await
        .unwrap();
        // Now the content lands; it must come up already hidden.
        h.apply(env(post_event("post-1", "acct-1", "content", 1)), fx.now())
            .await
            .unwrap();
        assert!(
            !fx.index.is_visible(EntityKind::Post, "post-1"),
            "a hide that raced ahead of content must be honoured once content arrives"
        );
    }

    #[tokio::test]
    async fn delete_removes_the_document() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        h.apply(env(post_event("post-1", "acct-1", "x", 1)), fx.now())
            .await
            .unwrap();
        let out = h
            .apply(
                env(SourceEvent::Post(PostEvent::Deleted(EntityDeletion {
                    id: "post-1".to_owned(),
                }))),
                fx.now(),
            )
            .await
            .unwrap();
        assert_eq!(out, ApplyOutcome::Deleted);
        assert!(!fx.index.contains(EntityKind::Post, "post-1"));
    }

    #[tokio::test]
    async fn gdpr_purge_removes_all_of_an_authors_documents() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        h.apply(env(post_event("post-1", "acct-1", "a", 1)), fx.now())
            .await
            .unwrap();
        h.apply(env(post_event("post-2", "acct-1", "b", 1)), fx.now())
            .await
            .unwrap();
        h.apply(env(post_event("post-3", "acct-2", "c", 1)), fx.now())
            .await
            .unwrap();
        let out = h
            .apply(
                env(SourceEvent::Compliance(ComplianceEvent::ActorPurged {
                    author_id: "acct-1".to_owned(),
                })),
                fx.now(),
            )
            .await
            .unwrap();
        assert_eq!(out, ApplyOutcome::Purged(2));
        assert!(!fx.index.contains(EntityKind::Post, "post-1"));
        assert!(!fx.index.contains(EntityKind::Post, "post-2"));
        assert!(fx.index.contains(EntityKind::Post, "post-3"));
    }

    #[tokio::test]
    async fn non_indexable_moderation_target_is_skipped() {
        let fx = Fixture::new();
        let out = fx
            .projection_handler()
            .apply(
                env(SourceEvent::Moderation(ModerationEvent::VisibilityRevoked(
                    VisibilityChange {
                        kind: None, // account-level action — no search index
                        id: "acct-1".to_owned(),
                        occurred_at: fx.now(),
                    },
                ))),
                fx.now(),
            )
            .await
            .unwrap();
        assert_eq!(out, ApplyOutcome::Skipped(SkipReason::NotIndexable));
    }

    #[tokio::test]
    async fn a_post_window_applies_to_the_authors_posts_newest_change_wins() {
        let fx = Fixture::new();
        let h = fx.projection_handler();
        for (post, author) in [("post-1", "acct-1"), ("post-2", "acct-1"), ("post-3", "acct-2")] {
            h.apply(env(post_event(post, author, "hello", 1)), fx.now()).await.unwrap();
        }
        let window = |days: Option<u32>, secs: i64| {
            env(SourceEvent::Profile(ProfileEvent::PostWindowChanged {
                profile_id: "acct-1".to_owned(),
                window_days: days,
                occurred_at: Utc.timestamp_opt(secs, 0).unwrap(),
            }))
        };

        let out = h.apply(window(Some(30), 1_700_000_200), fx.now()).await.unwrap();
        assert_eq!(out, ApplyOutcome::WindowApplied(2));
        let end = fx.index.visible_until("post-1").expect("windowed");
        assert_eq!(end, crate::application::fakes::t0() + chrono::Duration::days(30));
        assert_eq!(fx.index.visible_until("post-3"), None, "another author's post");

        // Older than the applied change: ignored.
        let out = h.apply(window(None, 1_700_000_100), fx.now()).await.unwrap();
        assert_eq!(out, ApplyOutcome::WindowApplied(0));
        assert!(fx.index.visible_until("post-1").is_some());

        let out = h.apply(window(None, 1_700_000_300), fx.now()).await.unwrap();
        assert_eq!(out, ApplyOutcome::WindowApplied(2));
        assert_eq!(fx.index.visible_until("post-1"), None);
    }

    #[tokio::test]
    async fn malformed_event_is_a_non_retryable_error() {
        let fx = Fixture::new();
        let err = fx
            .projection_handler()
            .apply(env(profile_event("", "ghost", 1)), fx.now())
            .await
            .unwrap_err();
        assert_eq!(err.error_code(), "SCH-3002");
        assert!(!err.is_retryable());
    }
}

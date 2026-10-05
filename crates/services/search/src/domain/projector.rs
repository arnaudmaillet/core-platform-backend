//! The projector — the pure heart of the service: one decoded [`SourceEvent`] in,
//! one [`IndexMutation`] out. No I/O, no engine awareness; `now` is injected so the
//! transform is fully deterministic and exhaustively testable.
//!
//! Ordering correctness is **not** the projector's job — it stamps each document
//! with a [`DocVersion`] and lets the engine's external-version guard arbitrate
//! out-of-order writes downstream. The projector's contracts are: full-document
//! re-projection on edits (never partial patches), moderation hides as a retained
//! `searchable=false` flip (not a delete), hard deletes and GDPR purges as distinct
//! removals, and benign non-indexable events folded into `Skip`.

use chrono::{DateTime, Utc};

use super::document::{HashtagDoc, IndexDocument, PostDoc, ProfileDoc};
use super::event::{
    ComplianceEvent, HashtagEvent, ModerationEvent, PostEvent, PostSnapshot, ProfileEvent,
    ProfileSnapshot, SourceEvent, VisibilityChange,
};
use super::mutation::{IndexMutation, SkipReason};
use super::value_object::{
    AuthorId, DocVersion, EntityKind, PopularityScore, Searchable, VisibilityAuthority,
};
use crate::error::SearchError;

/// Project a decoded source event into an index mutation. `now` is the indexing
/// instant (audit only); document ordering uses event-time / source revision.
pub fn project(event: SourceEvent, now: DateTime<Utc>) -> Result<IndexMutation, SearchError> {
    match event {
        SourceEvent::Post(e) => project_post(e, now),
        SourceEvent::Profile(e) => project_profile(e, now),
        SourceEvent::Hashtag(e) => project_hashtag(e, now),
        SourceEvent::Moderation(e) => project_moderation(e),
        SourceEvent::Compliance(e) => project_compliance(e),
    }
}

fn project_post(event: PostEvent, now: DateTime<Utc>) -> Result<IndexMutation, SearchError> {
    match event {
        // Publish and edit both re-project the WHOLE document (idempotent upsert).
        PostEvent::Published(s) | PostEvent::Updated(s) => {
            let doc = post_doc(s, now)?;
            Ok(IndexMutation::Upsert(IndexDocument::Post(doc)))
        }
        PostEvent::Deleted(d) => Ok(IndexMutation::Delete {
            kind: EntityKind::Post,
            id: non_empty_id(d.id)?,
        }),
    }
}

fn project_profile(event: ProfileEvent, now: DateTime<Utc>) -> Result<IndexMutation, SearchError> {
    match event {
        ProfileEvent::Upserted(s) => {
            let doc = profile_doc(s, now)?;
            Ok(IndexMutation::Upsert(IndexDocument::Profile(doc)))
        }
        ProfileEvent::Deleted(d) => Ok(IndexMutation::Delete {
            kind: EntityKind::Profile,
            id: non_empty_id(d.id)?,
        }),
        // Owner masking is a distinct visibility authority from moderation — it
        // flips the owner flag, leaving any moderation hide untouched.
        ProfileEvent::OwnerHidden {
            profile_id,
            occurred_at,
        } => owner_visibility(profile_id, occurred_at, Searchable::HIDDEN),
        ProfileEvent::OwnerRestored {
            profile_id,
            occurred_at,
        } => owner_visibility(profile_id, occurred_at, Searchable::VISIBLE),
        ProfileEvent::DiscoverabilityChanged {
            profile_id,
            searchable,
            occurred_at,
        } => Ok(IndexMutation::SetSearchable {
            authority: VisibilityAuthority::Discovery,
            kind: EntityKind::Profile,
            id: non_empty_id(profile_id)?,
            searchable: if searchable { Searchable::VISIBLE } else { Searchable::HIDDEN },
            version: DocVersion::from_event_time(occurred_at),
        }),
        ProfileEvent::PostWindowChanged {
            profile_id,
            window_days,
            occurred_at,
        } => Ok(IndexMutation::SetPostWindow {
            author_id: AuthorId::new(profile_id)?,
            window_days,
            version: DocVersion::from_event_time(occurred_at),
        }),
    }
}

fn owner_visibility(
    profile_id: String,
    occurred_at: DateTime<Utc>,
    searchable: Searchable,
) -> Result<IndexMutation, SearchError> {
    Ok(IndexMutation::SetSearchable {
        authority: VisibilityAuthority::Owner,
        kind: EntityKind::Profile,
        id: non_empty_id(profile_id)?,
        searchable,
        version: DocVersion::from_event_time(occurred_at),
    })
}

fn project_hashtag(event: HashtagEvent, now: DateTime<Utc>) -> Result<IndexMutation, SearchError> {
    match event {
        HashtagEvent::Observed(s) => {
            let tag = non_empty(s.tag, "tag")?;
            let doc = HashtagDoc {
                tag,
                post_count: s.post_count,
                searchable: Searchable::VISIBLE,
                popularity: PopularityScore::ZERO,
                indexed_at: now,
                version: DocVersion::new(s.revision),
            };
            Ok(IndexMutation::Upsert(IndexDocument::Hashtag(doc)))
        }
    }
}

fn project_moderation(event: ModerationEvent) -> Result<IndexMutation, SearchError> {
    match event {
        ModerationEvent::VisibilityRevoked(v) => Ok(visibility(v, Searchable::HIDDEN)),
        ModerationEvent::VisibilityRestored(v) => Ok(visibility(v, Searchable::VISIBLE)),
    }
}

fn project_compliance(event: ComplianceEvent) -> Result<IndexMutation, SearchError> {
    match event {
        ComplianceEvent::ActorPurged { author_id } => Ok(IndexMutation::PurgeByAuthor {
            author_id: AuthorId::new(author_id)?,
        }),
    }
}

/// A moderation visibility flip targets a search index only when the moderated
/// entity maps to one; otherwise it is a benign skip. The version is derived from
/// the moderation event time so a stale flip can't overwrite a newer state.
fn visibility(change: VisibilityChange, searchable: Searchable) -> IndexMutation {
    match change.kind {
        Some(kind) => IndexMutation::SetSearchable {
            authority: VisibilityAuthority::Moderation,
            kind,
            id: change.id,
            searchable,
            version: DocVersion::from_event_time(change.occurred_at),
        },
        None => IndexMutation::Skip(SkipReason::NotIndexable),
    }
}

fn post_doc(s: PostSnapshot, now: DateTime<Utc>) -> Result<PostDoc, SearchError> {
    Ok(PostDoc {
        post_id: non_empty(s.post_id, "post_id")?,
        author_id: AuthorId::new(s.author_id)?,
        author_handle: s.author_handle,
        caption: s.caption,
        hashtags: s.hashtags,
        thumbnail_key: s.thumbnail_key,
        searchable: Searchable::VISIBLE,
        popularity: PopularityScore::ZERO,
        created_at: s.created_at,
        indexed_at: now,
        version: DocVersion::new(s.revision),
        visible_until: s.visible_until,
    })
}

fn profile_doc(s: ProfileSnapshot, now: DateTime<Utc>) -> Result<ProfileDoc, SearchError> {
    let profile_id = non_empty(s.profile_id, "profile_id")?;
    Ok(ProfileDoc {
        // A profile is its own author, so exclusion/purge work uniformly.
        author_id: AuthorId::new(profile_id.clone())?,
        profile_id,
        handle: s.handle,
        display_name: s.display_name,
        bio: s.bio,
        avatar_key: s.avatar_key,
        verified: s.verified,
        searchable: Searchable::VISIBLE,
        popularity: PopularityScore::ZERO,
        created_at: s.created_at,
        indexed_at: now,
        version: DocVersion::new(s.revision),
    })
}

fn non_empty(value: String, field: &'static str) -> Result<String, SearchError> {
    if value.trim().is_empty() {
        return Err(SearchError::MissingProjectionField {
            field: field.to_owned(),
        });
    }
    Ok(value)
}

fn non_empty_id(value: String) -> Result<String, SearchError> {
    if value.trim().is_empty() {
        return Err(SearchError::InvalidIdentifier("id".to_owned()));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use error::AppError;

    use super::*;
    use crate::domain::event::{EntityDeletion, HashtagSnapshot};

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_500, 0).unwrap()
    }

    fn created() -> DateTime<Utc> {
        Utc.timestamp_opt(1_699_000_000, 0).unwrap()
    }

    fn post_snapshot() -> PostSnapshot {
        PostSnapshot {
            post_id: "post-1".to_owned(),
            author_id: "acct-9".to_owned(),
            author_handle: "alice".to_owned(),
            caption: "hello #rust world".to_owned(),
            hashtags: vec!["rust".to_owned()],
            thumbnail_key: "thumbs/post-1.jpg".to_owned(),
            created_at: created(),
            visible_until: None,
            revision: 7,
        }
    }

    fn profile_snapshot() -> ProfileSnapshot {
        ProfileSnapshot {
            profile_id: "prof-1".to_owned(),
            handle: "alice".to_owned(),
            display_name: "Alice".to_owned(),
            bio: "rustacean".to_owned(),
            avatar_key: "avatars/prof-1.jpg".to_owned(),
            verified: true,
            created_at: created(),
            revision: 3,
        }
    }

    #[test]
    fn published_post_upserts_a_visible_document() {
        let m = project(SourceEvent::Post(PostEvent::Published(post_snapshot())), now()).unwrap();
        match m {
            IndexMutation::Upsert(IndexDocument::Post(doc)) => {
                assert_eq!(doc.post_id, "post-1");
                assert_eq!(doc.author_id.as_str(), "acct-9");
                assert_eq!(doc.caption, "hello #rust world");
                assert_eq!(doc.hashtags, vec!["rust".to_owned()]);
                assert!(doc.searchable.is_visible());
                assert_eq!(doc.popularity, PopularityScore::ZERO);
                assert_eq!(doc.version.value(), 7);
                assert_eq!(doc.created_at, created());
                assert_eq!(doc.indexed_at, now());
            }
            other => panic!("expected post upsert, got {other:?}"),
        }
    }

    #[test]
    fn edited_post_fully_reprojects_with_new_version() {
        let mut s = post_snapshot();
        s.caption = "edited caption".to_owned();
        s.revision = 12;
        let m = project(SourceEvent::Post(PostEvent::Updated(s)), now()).unwrap();
        match m {
            IndexMutation::Upsert(IndexDocument::Post(doc)) => {
                assert_eq!(doc.caption, "edited caption");
                assert_eq!(doc.version.value(), 12);
            }
            other => panic!("expected post upsert, got {other:?}"),
        }
    }

    #[test]
    fn deleted_post_maps_to_delete() {
        let m = project(
            SourceEvent::Post(PostEvent::Deleted(EntityDeletion {
                id: "post-1".to_owned(),
            })),
            now(),
        )
        .unwrap();
        assert_eq!(
            m,
            IndexMutation::Delete {
                kind: EntityKind::Post,
                id: "post-1".to_owned(),
            }
        );
    }

    #[test]
    fn profile_upsert_is_its_own_author() {
        let m = project(
            SourceEvent::Profile(ProfileEvent::Upserted(profile_snapshot())),
            now(),
        )
        .unwrap();
        match m {
            IndexMutation::Upsert(IndexDocument::Profile(doc)) => {
                assert_eq!(doc.profile_id, "prof-1");
                assert_eq!(doc.author_id.as_str(), "prof-1");
                assert_eq!(doc.version.value(), 3);
                assert!(doc.verified);
            }
            other => panic!("expected profile upsert, got {other:?}"),
        }
    }

    #[test]
    fn hashtag_observed_upserts_with_coarse_count() {
        let m = project(
            SourceEvent::Hashtag(HashtagEvent::Observed(HashtagSnapshot {
                tag: "rust".to_owned(),
                post_count: 4096,
                revision: 100,
            })),
            now(),
        )
        .unwrap();
        match m {
            IndexMutation::Upsert(IndexDocument::Hashtag(doc)) => {
                assert_eq!(doc.tag, "rust");
                assert_eq!(doc.post_count, 4096);
                assert_eq!(doc.version.value(), 100);
            }
            other => panic!("expected hashtag upsert, got {other:?}"),
        }
    }

    #[test]
    fn moderation_hide_flips_searchable_off_keeping_the_doc() {
        let occurred = Utc.timestamp_opt(1_700_000_400, 0).unwrap();
        let m = project_moderation(ModerationEvent::VisibilityRevoked(VisibilityChange {
            kind: Some(EntityKind::Post),
            id: "post-1".to_owned(),
            occurred_at: occurred,
        }))
        .unwrap();
        assert_eq!(
            m,
            IndexMutation::SetSearchable {
                authority: VisibilityAuthority::Moderation,
                kind: EntityKind::Post,
                id: "post-1".to_owned(),
                searchable: Searchable::HIDDEN,
                version: DocVersion::from_event_time(occurred),
            }
        );
    }

    #[test]
    fn owner_masking_flips_the_owner_flag_not_moderation() {
        let occurred = Utc.timestamp_opt(1_700_000_400, 0).unwrap();
        let m = project(
            SourceEvent::Profile(ProfileEvent::OwnerHidden {
                profile_id: "prof-1".to_owned(),
                occurred_at: occurred,
            }),
            now(),
        )
        .unwrap();
        assert_eq!(
            m,
            IndexMutation::SetSearchable {
                authority: VisibilityAuthority::Owner,
                kind: EntityKind::Profile,
                id: "prof-1".to_owned(),
                searchable: Searchable::HIDDEN,
                version: DocVersion::from_event_time(occurred),
            }
        );
    }

    #[test]
    fn discoverability_flips_its_own_flag() {
        let occurred = Utc.timestamp_opt(1_700_000_500, 0).unwrap();
        let m = project(
            SourceEvent::Profile(ProfileEvent::DiscoverabilityChanged {
                profile_id: "prof-1".to_owned(),
                searchable: false,
                occurred_at: occurred,
            }),
            now(),
        )
        .unwrap();
        assert_eq!(
            m,
            IndexMutation::SetSearchable {
                authority: VisibilityAuthority::Discovery,
                kind: EntityKind::Profile,
                id: "prof-1".to_owned(),
                searchable: Searchable::HIDDEN,
                version: DocVersion::from_event_time(occurred),
            }
        );
    }

    #[test]
    fn a_post_window_change_reaches_the_authors_posts() {
        let occurred = Utc.timestamp_opt(1_700_000_500, 0).unwrap();
        let m = project(
            SourceEvent::Profile(ProfileEvent::PostWindowChanged {
                profile_id: "prof-1".to_owned(),
                window_days: Some(30),
                occurred_at: occurred,
            }),
            now(),
        )
        .unwrap();
        assert_eq!(
            m,
            IndexMutation::SetPostWindow {
                author_id: AuthorId::new("prof-1").unwrap(),
                window_days: Some(30),
                version: DocVersion::from_event_time(occurred),
            }
        );
    }

    #[test]
    fn owner_restore_flips_owner_flag_visible() {
        let m = project(
            SourceEvent::Profile(ProfileEvent::OwnerRestored {
                profile_id: "prof-1".to_owned(),
                occurred_at: now(),
            }),
            now(),
        )
        .unwrap();
        match m {
            IndexMutation::SetSearchable {
                authority,
                searchable,
                ..
            } => {
                assert_eq!(authority, VisibilityAuthority::Owner);
                assert!(searchable.is_visible());
            }
            other => panic!("expected owner SetSearchable, got {other:?}"),
        }
    }

    #[test]
    fn appeal_reversal_restores_searchable() {
        let occurred = Utc.timestamp_opt(1_700_000_450, 0).unwrap();
        let m = project_moderation(ModerationEvent::VisibilityRestored(VisibilityChange {
            kind: Some(EntityKind::Profile),
            id: "prof-1".to_owned(),
            occurred_at: occurred,
        }))
        .unwrap();
        match m {
            IndexMutation::SetSearchable {
                searchable, kind, ..
            } => {
                assert!(searchable.is_visible());
                assert_eq!(kind, EntityKind::Profile);
            }
            other => panic!("expected SetSearchable, got {other:?}"),
        }
    }

    #[test]
    fn moderation_on_non_indexable_entity_is_skipped() {
        let m = project_moderation(ModerationEvent::VisibilityRevoked(VisibilityChange {
            kind: None, // e.g. an account- or chat-message-level action
            id: "acct-9".to_owned(),
            occurred_at: now(),
        }))
        .unwrap();
        assert_eq!(m, IndexMutation::Skip(SkipReason::NotIndexable));
    }

    #[test]
    fn gdpr_purge_targets_the_author() {
        let m = project(
            SourceEvent::Compliance(ComplianceEvent::ActorPurged {
                author_id: "acct-9".to_owned(),
            }),
            now(),
        )
        .unwrap();
        match m {
            IndexMutation::PurgeByAuthor { author_id } => {
                assert_eq!(author_id.as_str(), "acct-9");
            }
            other => panic!("expected purge, got {other:?}"),
        }
    }

    #[test]
    fn malformed_post_missing_id_is_a_terminal_error() {
        let mut s = post_snapshot();
        s.post_id = "  ".to_owned();
        let err = project(SourceEvent::Post(PostEvent::Published(s)), now()).unwrap_err();
        assert_eq!(err.error_code(), "SCH-3002");
        assert!(!err.is_retryable(), "a malformed event must not be retried");
    }

    #[test]
    fn post_missing_author_is_invalid_identifier() {
        let mut s = post_snapshot();
        s.author_id = String::new();
        let err = project(SourceEvent::Post(PostEvent::Published(s)), now()).unwrap_err();
        assert_eq!(err.error_code(), "SCH-9002");
    }

    #[test]
    fn purge_with_blank_author_is_rejected() {
        let err = project(
            SourceEvent::Compliance(ComplianceEvent::ActorPurged {
                author_id: "   ".to_owned(),
            }),
            now(),
        )
        .unwrap_err();
        assert_eq!(err.error_code(), "SCH-9002");
    }
}

//! #988 over live Scylla: a post's comments as one timeline — top-level
//! comments and replies alike, newest first, paged without repeats — each
//! reply quoting the comment it answers; tombstones, purges, held comments and
//! the read gate apply to the timeline and its quotes; comments from before
//! the timeline existed are found after the backfill.

use std::time::Duration;

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use comment::application::command::review_held::ReviewHeldCommentCommand;
use comment::application::port::CommentRepository;
use comment::application::query::list_timeline::TimelineComment;

use crate::comment_it::harness::{self, CommentStatus, ProfileId, TestHarness, Viewer};

fn viewer(id: &str) -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(id).unwrap()])
}

/// Comments a few milliseconds apart, so the newest-first order is theirs.
async fn create(h: &TestHarness, post: &str, parent: Option<&str>, author: &str) -> String {
    tokio::time::sleep(Duration::from_millis(2)).await;
    h.create(post, parent, author).await
}

fn ids(timeline: &[TimelineComment]) -> Vec<String> {
    timeline.iter().map(|t| t.comment.comment_id.as_str()).collect()
}

fn find<'a>(timeline: &'a [TimelineComment], id: &str) -> &'a TimelineComment {
    timeline.iter().find(|t| t.comment.comment_id.as_str() == id).unwrap_or_else(|| panic!("{id} not on the timeline"))
}

#[tokio::test]
async fn a_posts_comments_and_replies_read_as_one_timeline_newest_first() {
    let h = TestHarness::start().await;
    let post = harness::random_post();
    let (alice, bob) = (harness::random_author(), harness::random_author());
    let t1 = create(&h, &post, None, &alice).await;
    let t2 = create(&h, &post, None, &bob).await;
    let r1 = create(&h, &post, Some(&t1), &bob).await;
    let t3 = create(&h, &post, None, &alice).await;
    let r2 = create(&h, &post, Some(&t1), &alice).await;

    let expected = vec![r2.clone(), t3.clone(), r1.clone(), t2.clone(), t1.clone()];
    // Page by page (2 at a time): every comment once, in order.
    assert_eq!(h.all_timeline(&post, 2, Viewer::Internal).await, expected);
    let (first, next) = h.timeline_page(&post, 2, None, Viewer::Internal).await;
    assert_eq!(ids(&first), expected[..2]);
    assert!(next.is_some());

    // A reply names and quotes its parent — read on its own when the page
    // does not hold it.
    let reply = find(&first, &r2);
    assert_eq!(reply.comment.parent_id.as_ref().map(|p| p.as_str()), Some(t1.clone()));
    let quote = reply.reply_to.as_ref().expect("a reply quotes its parent");
    assert_eq!(quote.comment_id.as_str(), t1);
    let quoted = quote.quoted.as_ref().expect("a published parent is quoted");
    assert_eq!(quoted.author_id.as_str(), alice);
    assert_eq!(quoted.excerpt, "a comment");
    assert!(!quoted.has_gif);
    // A top-level comment quotes nothing.
    assert!(find(&first, &t3).reply_to.is_none());
    assert!(find(&first, &t3).comment.parent_id.is_none());

    // A parent with replies goes as a tombstone (on the timeline, unquoted);
    // a leaf reply is purged (off the timeline).
    h.delete(&t1, &alice).await;
    h.delete(&r2, &alice).await;
    let (all, _) = h.timeline_page(&post, 10, None, Viewer::Internal).await;
    assert_eq!(ids(&all), vec![t3, r1.clone(), t2, t1.clone()]);
    let tombstone = find(&all, &t1);
    assert_eq!(tombstone.comment.status, CommentStatus::Deleted);
    assert!(tombstone.comment.body.is_none(), "a tombstone keeps no text");
    let orphan = find(&all, &r1).reply_to.as_ref().unwrap();
    assert_eq!(orphan.comment_id.as_str(), t1);
    assert!(orphan.quoted.is_none(), "a deleted parent is not quoted");
}

#[tokio::test]
async fn the_read_gate_applies_to_the_timeline_and_its_quotes() {
    let h = TestHarness::start().await;
    let post = harness::random_post();
    let (alice, mallory, reader) = (harness::random_author(), harness::random_author(), harness::random_author());
    let from_mallory = create(&h, &post, None, &mallory).await;
    let answer = create(&h, &post, Some(&from_mallory), &alice).await;
    h.gate.hide_author(&mallory);

    let (seen, _) = h.timeline_page(&post, 10, None, viewer(&reader)).await;
    assert_eq!(ids(&seen), vec![answer.clone()], "the hidden author's comment is not on the reader's timeline");
    assert!(find(&seen, &answer).reply_to.as_ref().unwrap().quoted.is_none(), "nor is it quoted");
    // The mesh sees everything.
    let (all, _) = h.timeline_page(&post, 10, None, Viewer::Internal).await;
    assert!(find(&all, &answer).reply_to.as_ref().unwrap().quoted.is_some());

    h.gate.post_unreadable(&post);
    let (none, next) = h.timeline_page(&post, 10, None, viewer(&reader)).await;
    assert!(none.is_empty() && next.is_none());
}

#[tokio::test]
async fn a_held_comment_is_on_its_authors_and_the_owners_timeline_only() {
    let h = TestHarness::start().await;
    let (owner, commenter, reader) = (harness::random_author(), harness::random_author(), harness::random_author());
    let post = harness::random_post();
    h.gate.set_post_author(&post, &ProfileId::try_from(owner.as_str()).unwrap());
    let from_owner = create(&h, &post, None, &owner).await;
    h.gate.limit(&post);
    let held = create(&h, &post, Some(&from_owner), &commenter).await;

    let (seen, _) = h.timeline_page(&post, 10, None, viewer(&reader)).await;
    assert_eq!(ids(&seen), vec![from_owner.clone()]);
    for profile in [&owner, &commenter] {
        let (seen, _) = h.timeline_page(&post, 10, None, viewer(profile)).await;
        assert_eq!(ids(&seen), vec![held.clone(), from_owner.clone()]);
        assert!(find(&seen, &held).comment.held);
    }

    let cmd = ReviewHeldCommentCommand { comment_id: held.clone(), owner_id: owner.clone(), approve: true };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("approve");
    let (seen, _) = h.timeline_page(&post, 10, None, viewer(&reader)).await;
    assert_eq!(ids(&seen), vec![held.clone(), from_owner]);
    assert!(!find(&seen, &held).comment.held);
}

/// A comment as written before `comments_timeline` existed: in
/// `comment.comments` only.
async fn old_comment(h: &TestHarness, post: &str, parent: Option<&str>, author: &str, at_ms: i64) -> String {
    let id = Uuid::now_v7();
    let parent = parent.map(|p| Uuid::parse_str(p).unwrap()).unwrap_or_else(Uuid::nil);
    h.scylla
        .session
        .execute_unpaged(
            "INSERT INTO comment.comments (comment_id, post_id, author_id, parent_id, status, body, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 0, 'an old one', ?, ?)",
            (
                id,
                Uuid::parse_str(post).unwrap(),
                Uuid::parse_str(author).unwrap(),
                parent,
                scylla::value::CqlTimestamp(at_ms),
                scylla::value::CqlTimestamp(at_ms),
            ),
        )
        .await
        .unwrap();
    id.to_string()
}

#[tokio::test]
async fn comments_from_before_the_timeline_are_found_after_the_backfill() {
    let h = TestHarness::start().await;
    let post = harness::random_post();
    let author = harness::random_author();
    let now = chrono::Utc::now().timestamp_millis();
    let top = old_comment(&h, &post, None, &author, now - 3_000).await;
    let reply = old_comment(&h, &post, Some(&top), &author, now - 2_000).await;
    let deleted = old_comment(&h, &post, None, &author, now - 1_000).await;
    let newer = create(&h, &post, Some(&top), &author).await;

    // A tombstone of an older comment lands before the backfill: its row is
    // incomplete, and not listed — the rest of the timeline reads.
    let deleted_id = comment::domain::value_object::CommentId::try_from(deleted.as_str()).unwrap();
    let mut tombstoned = h.repository.find_by_id(&deleted_id).await.unwrap().unwrap();
    tombstoned.delete(true).unwrap();
    h.repository.soft_delete(&tombstoned).await.unwrap();
    assert_eq!(h.all_timeline(&post, 10, Viewer::Internal).await, vec![newer.clone()]);

    let written = h.repository.backfill_timeline().await.expect("backfill");
    assert!(written >= 3);
    let (all, _) = h.timeline_page(&post, 10, None, Viewer::Internal).await;
    assert_eq!(ids(&all), vec![newer.clone(), deleted.clone(), reply.clone(), top.clone()]);
    let tombstone = find(&all, &deleted);
    assert_eq!(tombstone.comment.status, CommentStatus::Deleted, "the backfill never undoes a tombstone");
    assert!(tombstone.comment.body.is_none());
    let quoted = find(&all, &reply).reply_to.as_ref().unwrap().quoted.as_ref().expect("its parent is quoted");
    assert_eq!(quoted.excerpt, "an old one");

    // Idempotent.
    h.repository.backfill_timeline().await.expect("again");
    assert_eq!(h.all_timeline(&post, 10, Viewer::Internal).await, vec![newer, deleted, reply, top]);
}

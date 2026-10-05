//! #653 over the live ScyllaDB: a profile's own comments, newest first, across
//! posts — what the GDPR data export reads. A tombstoned comment shows as its
//! tombstone, a purged one is gone; comments from before the index existed are
//! found once the backfill ran.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use comment::application::port::CommentRepository;
use comment::application::query::list_by_author::ListCommentsByAuthorQuery;
use comment::domain::aggregate::Comment;
use comment::domain::value_object::ProfileId;

use crate::comment_it::harness::{self, CommentStatus, TestHarness};

async fn by_author(h: &TestHarness, author: &str, limit: i32, page_token: Option<String>) -> (Vec<Comment>, Option<String>) {
    h.query_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            ListCommentsByAuthorQuery { author_id: author.to_owned(), limit, page_token },
        ))
        .await
        .expect("list by author")
}

async fn all_by_author(h: &TestHarness, author: &str) -> Vec<String> {
    let (mut ids, mut token) = (Vec::new(), None);
    loop {
        let (page, next) = by_author(h, author, 2, token).await;
        ids.extend(page.iter().map(|c| c.id().as_str()));
        match next {
            Some(next) => token = Some(next),
            None => return ids,
        }
    }
}

#[tokio::test]
async fn a_profiles_comments_are_listed_newest_first_across_posts() {
    let h = TestHarness::start().await;
    let (author, other) = (harness::random_author(), harness::random_author());
    let (post_a, post_b) = (harness::random_post(), harness::random_post());

    let first = h.create(&post_a, None, &author).await;
    let second = h.create(&post_b, None, &author).await;
    let reply = h.create(&post_a, Some(&first), &author).await;
    h.create(&post_a, None, &other).await;
    let leaf = h.create(&post_b, None, &author).await;

    assert_eq!(all_by_author(&h, &author).await, vec![leaf.clone(), reply.clone(), second.clone(), first.clone()]);

    // A tombstone stays (as one); a purged leaf is gone.
    h.delete(&first, &author).await;
    h.delete(&leaf, &author).await;
    let (page, _) = by_author(&h, &author, 10, None).await;
    let ids: Vec<String> = page.iter().map(|c| c.id().as_str()).collect();
    assert_eq!(ids, vec![reply, second, first.clone()]);
    let tombstone = page.iter().find(|c| c.id().as_str() == first).unwrap();
    assert_eq!(tombstone.status(), CommentStatus::Deleted);
    assert!(tombstone.body().is_none(), "a tombstone keeps no text");
}

#[tokio::test]
async fn comments_from_before_the_index_are_found_after_the_backfill() {
    let h = TestHarness::start().await;
    let author = harness::random_author();
    let old = h.create(&harness::random_post(), None, &author).await;
    // As if written before comments_by_author existed.
    h.scylla
        .session
        .execute_unpaged(
            "DELETE FROM comment.comments_by_author WHERE author_id = ?",
            (Uuid::parse_str(&author).unwrap(),),
        )
        .await
        .unwrap();
    assert!(all_by_author(&h, &author).await.is_empty());

    let written = h.repository.backfill_author_index().await.expect("backfill");
    assert!(written >= 1);
    assert_eq!(all_by_author(&h, &author).await, vec![old]);
    // Idempotent.
    h.repository.backfill_author_index().await.expect("again");
    let author_id = ProfileId::try_from(author.as_str()).unwrap();
    assert_eq!(h.repository.list_by_author(&author_id, 10, None).await.unwrap().0.len(), 1);
}

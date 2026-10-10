//! #876 over live Scylla: a retried CreatePost with the same idempotency key
//! creates one post and both calls answer its id (the key's LWT claim, its
//! completion and its release).

use post::error::PostError;

use crate::post_it::harness::{random_id, TestHarness};

#[tokio::test]
async fn a_retried_create_with_the_same_key_creates_one_post() {
    let h = TestHarness::start().await;
    let author = random_id();
    let key = random_id();

    let first = h.create_keyed(&author, &key).await.expect("first create");
    let retry = h.create_keyed(&author, &key).await.expect("retry");
    assert!(!first.replayed);
    assert!(retry.replayed);
    assert_eq!(retry.post_id, first.post_id, "the retry answers the first post");
    assert_eq!(h.list(&author).await.len(), 1);

    // Another key, another author with the same key: new posts.
    let other = h.create_keyed(&author, &random_id()).await.expect("other key");
    assert_ne!(other.post_id, first.post_id);
    let someone_else = random_id();
    let theirs = h.create_keyed(&someone_else, &key).await.expect("same key, other author");
    assert_ne!(theirs.post_id, first.post_id);
    assert_eq!(h.list(&author).await.len(), 2);
    assert_eq!(h.list(&someone_else).await.len(), 1);
}

/// Taps at once: one post, and every call answers it or is told to retry.
#[tokio::test]
async fn concurrent_creates_with_one_key_create_one_post() {
    let h = TestHarness::start().await;
    let author = random_id();
    let key = random_id();

    let calls = (0..8).map(|_| h.create_keyed(&author, &key));
    let outcomes = futures::future::join_all(calls).await;

    let ids: std::collections::HashSet<_> =
        outcomes.iter().filter_map(|o| o.as_ref().ok()).map(|c| c.post_id.as_str()).collect();
    assert_eq!(ids.len(), 1, "{outcomes:?}");
    assert!(
        outcomes.iter().all(|o| matches!(o, Ok(_) | Err(PostError::CreateInFlight))),
        "{outcomes:?}",
    );
    assert_eq!(h.list(&author).await.len(), 1);
}

#[tokio::test]
async fn a_refused_create_frees_its_key_and_a_malformed_key_is_refused() {
    let h = TestHarness::start().await;
    let author = random_id();
    let key = random_id();

    // A caption over the limit is refused: the key is freed for the retry.
    let too_long = "x".repeat(10_000);
    assert!(h.try_create_keyed_captioned(&author, &key, &too_long).await.is_err());
    let retry = h.create_keyed(&author, &key).await.expect("retry after a refusal");
    assert!(!retry.replayed);

    assert!(matches!(h.create_keyed(&author, "bad key!").await, Err(PostError::InvalidIdempotencyKey)));
    assert_eq!(h.list(&author).await.len(), 1);
}

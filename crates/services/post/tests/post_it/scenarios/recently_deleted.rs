//! Scenario — "Recently deleted" (#663) over the real store: a deleted post is
//! listed for its author and can be restored, coming back published (and
//! announced again) or a draft; a restored post leaves the list.

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use post::application::command::restore_post::RestorePostCommand;
use post::application::query::list_recently_deleted::ListRecentlyDeletedQuery;

use crate::post_it::harness::{self, Post, PostStatus, TestHarness, Viewer};

async fn recently_deleted(h: &TestHarness, author: &str) -> Vec<String> {
    let query = ListRecentlyDeletedQuery { profile_id: author.to_owned(), limit: 50, page_token: None };
    let (posts, _next): (Vec<Post>, Option<String>) =
        h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("list_recently_deleted");
    posts.iter().map(|p| p.id().as_str()).collect()
}

async fn restore(h: &TestHarness, post_id: &str, author: &str) -> Result<(), cqrs::CqrsError> {
    let cmd = RestorePostCommand { post_id: post_id.to_owned(), profile_id: author.to_owned() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

#[tokio::test]
async fn a_deleted_post_is_listed_and_restored_as_it_was() {
    let h = TestHarness::start().await;
    let author = harness::random_id();
    let (published, draft) = (harness::random_id(), harness::random_id());
    h.create(&published, &author).await;
    h.publish(&published, &author).await;
    h.create(&draft, &author).await;
    h.delete(&published, &author).await;
    h.delete(&draft, &author).await;

    let listed = recently_deleted(&h, &author).await;
    assert_eq!(listed, vec![draft.clone(), published.clone()], "newest deletion first");
    assert!(h.get_as(&published, Viewer::Anonymous).await.is_err());

    // Only the author restores.
    assert!(restore(&h, &published, &harness::random_id()).await.is_err());
    let announced = h.publisher.count("published");
    restore(&h, &published, &author).await.expect("restore");
    assert_eq!(h.get(&published).await.unwrap().status(), PostStatus::Published);
    assert!(h.get_as(&published, Viewer::Anonymous).await.is_ok(), "public again");
    assert_eq!(h.publisher.count("published"), announced + 1, "announced again");

    restore(&h, &draft, &author).await.expect("restore draft");
    assert_eq!(h.get(&draft).await.unwrap().status(), PostStatus::Draft);

    assert!(recently_deleted(&h, &author).await.is_empty());
    assert!(restore(&h, &draft, &author).await.is_err(), "not deleted any more");
}

//! Scenario — the real `GrpcReadGate` over real tonic transport, against
//! in-process `post` and `social-graph` peers, with the real comment service on
//! a live ScyllaDB. Every other scenario scripts the gate; this one proves the
//! cross-service rules hold on the wire: the post's own state (draft, removed,
//! age-gated, outside its author's window), the author's audience, blocks,
//! restrictions (hidden from others, announced quietly), interaction settings
//! and limits, the CheckAccess caps, and a fail-closed outage.

use error::AppError;
use post_api::{ModerationRestriction, PostStatus, PostView};
use social_graph_api::ContentAccess;

use crate::comment_it::harness::{self, ProfileId, TestHarness, Viewer};

fn viewer(id: &str) -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(id).unwrap()])
}

fn published(post: &str, owner: &str) -> PostView {
    PostView {
        post_id: post.to_owned(),
        profile_id: owner.to_owned(),
        status: PostStatus::Published as i32,
        moderation: ModerationRestriction::None as i32,
        ..Default::default()
    }
}

fn ids(page: &[comment::application::port::CommentSummary]) -> Vec<String> {
    let mut ids: Vec<String> = page.iter().map(|c| c.author_id.as_str()).collect();
    ids.sort();
    ids
}

fn sorted(authors: &[&String]) -> Vec<String> {
    let mut ids: Vec<String> = authors.iter().map(|a| (*a).clone()).collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn blocks_and_restrictions_shape_what_each_reader_sees() {
    let (h, peers) = TestHarness::start_over_grpc().await;
    let (owner, friend, restricted, blocked, reader) = (
        harness::random_author(),
        harness::random_author(),
        harness::random_author(),
        harness::random_author(),
        harness::random_author(),
    );
    let post = harness::random_post();
    peers.posts.put(published(&post, &owner));
    peers.graph.restrictions.lock().unwrap().insert((owner.clone(), restricted.clone()));
    // The reader and `blocked` blocked each other: hidden both ways.
    peers.graph.access.lock().unwrap().insert((reader.clone(), blocked.clone()), ContentAccess::Hidden);

    let from_friend = h.create(&post, None, &friend).await;
    let from_restricted = h.create(&post, None, &restricted).await;
    h.create(&post, None, &blocked).await;
    h.create(&post, None, &owner).await;

    // A third party: no restricted commenter, no one they blocked.
    let seen = h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap();
    assert_eq!(ids(&seen), sorted(&[&friend, &owner]));
    // The owner sees every comment; the restricted profile sees its own.
    assert_eq!(h.try_list_top_level_as(&post, viewer(&owner)).await.unwrap().len(), 4);
    assert_eq!(
        ids(&h.try_list_top_level_as(&post, viewer(&restricted)).await.unwrap()),
        sorted(&[&friend, &restricted, &blocked, &owner])
    );
    assert!(h.get_as(&from_restricted, viewer(&reader)).await.is_err());

    // The restricted profile's comment was announced quietly (#659), the
    // friend's normally.
    assert!(h.published.created(&from_restricted).expect("announced").quiet);
    assert!(!h.published.created(&from_friend).expect("announced").quiet);
}

#[tokio::test]
async fn a_post_others_may_not_read_hides_its_comments_and_takes_none_from_them() {
    let (h, peers) = TestHarness::start_over_grpc().await;
    let (owner, reader) = (harness::random_author(), harness::random_author());

    let cases: Vec<(&str, PostView)> = vec![
        ("draft", PostView { status: PostStatus::Draft as i32, ..published("", &owner) }),
        ("removed", PostView { moderation: ModerationRestriction::Removed as i32, ..published("", &owner) }),
        ("outside the window", PostView { outside_window: true, ..published("", &owner) }),
    ];
    for (case, view) in cases {
        let post = harness::random_post();
        h.gate.set_post_author(&post, &ProfileId::try_from(owner.as_str()).unwrap());
        peers.posts.put(PostView { post_id: post.clone(), ..view });
        // Only its author comments; only its author reads the comments.
        let own = h.create(&post, None, &owner).await;
        assert!(h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap().is_empty(), "{case}");
        assert!(h.get_as(&own, viewer(&reader)).await.is_err(), "{case}");
        assert_eq!(h.try_list_top_level_as(&post, viewer(&owner)).await.unwrap().len(), 1, "{case}");
        let refused = h.try_create(&post, &reader).await.expect_err(case);
        assert_eq!(refused.error_code(), "CMT-1004", "{case}");
    }

    // A post that does not exist: the same, indistinguishable.
    let missing = harness::random_post();
    assert!(h.try_list_top_level_as(&missing, viewer(&reader)).await.unwrap().is_empty());
    assert_eq!(h.try_create(&missing, &reader).await.unwrap_err().error_code(), "CMT-1004");

    // An author the reader may not see (private, not followed): the same.
    let private_owner = harness::random_author();
    let post = harness::random_post();
    peers.posts.put(published(&post, &private_owner));
    h.create(&post, None, &private_owner).await;
    peers.graph.access.lock().unwrap().insert((reader.clone(), private_owner.clone()), ContentAccess::HeaderOnly);
    assert!(h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap().is_empty());
    assert_eq!(h.try_create(&post, &reader).await.unwrap_err().error_code(), "CMT-1004");
}

#[tokio::test]
async fn an_age_gated_posts_comments_are_for_readers_cleared_for_mature_content() {
    let (h, peers) = TestHarness::start_over_grpc().await;
    let (owner, commenter, reader) = (harness::random_author(), harness::random_author(), harness::random_author());
    let post = harness::random_post();
    peers.posts.put(PostView { moderation: ModerationRestriction::AgeGated as i32, ..published(&post, &owner) });
    h.create(&post, None, &commenter).await;

    assert_eq!(h.try_list_top_level_rated(&post, viewer(&reader), true).await.unwrap().len(), 1, "an adult");
    assert!(h.try_list_top_level_rated(&post, viewer(&reader), false).await.unwrap().is_empty(), "13–17 / guest");
    assert_eq!(h.try_list_top_level_rated(&post, viewer(&owner), false).await.unwrap().len(), 1, "its author");
}

#[tokio::test]
async fn interaction_settings_and_limits_reach_the_comment() {
    let (h, peers) = TestHarness::start_over_grpc().await;
    let (owner, refused, limited, reader) = (
        harness::random_author(),
        harness::random_author(),
        harness::random_author(),
        harness::random_author(),
    );
    let post = harness::random_post();
    peers.posts.put(published(&post, &owner));
    peers.graph.interaction.lock().unwrap().insert(refused.clone(), (false, false));
    peers.graph.interaction.lock().unwrap().insert(limited.clone(), (true, true));

    // The author's settings refuse this profile (or a block): CMT-1005.
    assert_eq!(h.try_create(&post, &refused).await.unwrap_err().error_code(), "CMT-1005");

    // A temporary limit covers this profile: stored, held, not announced.
    let held = h.create(&post, None, &limited).await;
    assert!(h.published.created(&held).is_none());
    assert!(h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap().is_empty());
    let for_owner = h.try_list_top_level_as(&post, viewer(&owner)).await.unwrap();
    assert!(for_owner.len() == 1 && for_owner[0].held);
}

#[tokio::test]
async fn a_full_page_of_commenters_is_checked_within_the_caps() {
    let (h, peers) = TestHarness::start_over_grpc().await;
    let (owner, reader) = (harness::random_author(), harness::random_author());
    let post = harness::random_post();
    peers.posts.put(published(&post, &owner));
    for _ in 0..100 {
        h.create(&post, None, &harness::random_author()).await;
    }

    peers.graph.access_calls.lock().unwrap().clear();
    let page = h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap();
    assert_eq!(page.len(), 100, "nobody hidden");
    // 100 commenters + the post's author = 101 targets: split, never refused.
    let calls = peers.graph.access_calls.lock().unwrap().clone();
    assert!(calls.len() >= 2, "{calls:?}");
    assert!(calls.iter().all(|(viewers, targets)| *viewers <= 20 && *targets <= 100), "{calls:?}");
    assert_eq!(calls.iter().map(|(_, t)| t).sum::<usize>(), 101);
}

#[tokio::test]
async fn an_outage_of_either_peer_fails_closed() {
    let (h, peers) = TestHarness::start_over_grpc().await;
    let (owner, reader) = (harness::random_author(), harness::random_author());
    let post = harness::random_post();
    peers.posts.put(published(&post, &owner));
    h.create(&post, None, &owner).await;

    *peers.posts.down.lock().unwrap() = true;
    let Err(outage) = h.try_list_top_level_as(&post, viewer(&reader)).await else {
        panic!("a read during an outage must fail closed");
    };
    assert_eq!(outage.error_code(), "CMT-5001");
    assert_eq!(h.try_create(&post, &reader).await.unwrap_err().error_code(), "CMT-5001");

    *peers.posts.down.lock().unwrap() = false;
    *peers.graph.down.lock().unwrap() = true;
    let Err(outage) = h.try_list_top_level_as(&post, viewer(&reader)).await else {
        panic!("a read during a social-graph outage must fail closed");
    };
    assert_eq!(outage.error_code(), "CMT-5001");
    assert_eq!(h.try_create(&post, &reader).await.unwrap_err().error_code(), "CMT-5001");

    // Back up: readable again.
    *peers.graph.down.lock().unwrap() = false;
    assert_eq!(h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap().len(), 1);
}

//! Scenario — the post owner's comment filter (#660) over the real store: a
//! comment with one of the owner's hidden words, or an offensive term while
//! the offensive filter is on, is seen by its author only — not by the owner,
//! not by anyone else, in lists or point reads.

use comment::domain::comment_filter::CommentFilter;

use crate::comment_it::harness::{self, ProfileId, TestHarness, Viewer, OFFENSIVE_TERM};

fn viewer(id: &str) -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(id).unwrap()])
}

fn ids(list: &[comment::application::port::CommentSummary]) -> Vec<String> {
    list.iter().map(|c| c.comment_id.as_str()).collect()
}

#[tokio::test]
async fn filtered_comments_are_seen_by_their_author_only() {
    let h = TestHarness::start().await;
    let (owner, commenter, reader) = (harness::random_author(), harness::random_author(), harness::random_author());
    let owner_id = ProfileId::try_from(owner.as_str()).unwrap();
    let post = harness::random_post();
    h.gate.set_post_author(&post, &owner_id);
    h.filters
        .set(&owner_id, &CommentFilter { hidden_words: vec!["spoiler".into()], filter_offensive: true })
        .await
        .expect("filters");

    let fine = h.create_with_body(&post, &commenter, "great shot").await;
    let spoiler = h.create_with_body(&post, &commenter, "Big SPOILER: he dies").await;
    let offensive = h.create_with_body(&post, &commenter, &format!("you {OFFENSIVE_TERM}")).await;

    for who in [&owner, &reader] {
        let seen = h.try_list_top_level_as(&post, viewer(who)).await.unwrap();
        assert_eq!(ids(&seen), vec![fine.clone()], "only the clean comment");
        assert!(h.get_as(&spoiler, viewer(who)).await.is_err());
    }
    // The commenter still sees all of its comments.
    assert_eq!(h.try_list_top_level_as(&post, viewer(&commenter)).await.unwrap().len(), 3);
    assert!(h.get_as(&spoiler, viewer(&commenter)).await.is_ok());

    // Turning the offensive filter off brings that comment back; hidden words stay.
    h.filters
        .set(&owner_id, &CommentFilter { hidden_words: vec!["spoiler".into()], filter_offensive: false })
        .await
        .expect("filters");
    let seen = ids(&h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap());
    assert!(seen.contains(&offensive) && !seen.contains(&spoiler));
}

//! #661 over the real tables: people one may know — friends of friends,
//! ranked by how many of one's followees follow them; never oneself, a
//! followed profile, a block either way, a private or hidden profile, or one
//! that turned suggestions off.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::AudienceFact;
use social_graph::application::query::{SuggestProfilesQuery, SuggestionPage};

use crate::social_graph_it::harness::{self, ProfileId, TestHarness};

async fn page(h: &TestHarness, me: &ProfileId, limit: i32, page_token: Option<String>) -> SuggestionPage {
    let query = SuggestProfilesQuery { profile_id: me.as_str(), limit, page_token };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.unwrap()
}

async fn suggest(h: &TestHarness, me: &ProfileId) -> Vec<(ProfileId, u32)> {
    let found = page(h, me, 20, None).await;
    assert_eq!(found.next_page_token, None, "one page holds them all");
    found.suggestions.into_iter().map(|s| (s.profile_id, s.mutual_count)).collect()
}

#[tokio::test]
async fn friends_of_friends_ranked_and_filtered() {
    let h = TestHarness::start().await;
    let p = harness::random_profile;
    let (me, a, b) = (p(), p(), p());
    let (popular, known, already, blocked, private, hidden, opted_out) = (p(), p(), p(), p(), p(), p(), p());
    h.follow(&me, &a).await;
    h.follow(&me, &b).await;
    h.follow(&me, &already).await;
    // a and b both follow `popular`; only a follows the others.
    for target in [&popular, &known, &already, &blocked, &private, &hidden, &opted_out, &me] {
        h.follow(&a, target).await;
    }
    h.follow(&b, &popular).await;

    // Announced suggestible, as profile does at creation (#661, fails closed).
    for profile in [&popular, &known, &already, &blocked, &private, &hidden, &opted_out] {
        h.audience(profile, AudienceFact::Suggestible(true)).await;
    }
    h.block(&blocked, &me).await;
    h.audience(&private, AudienceFact::Private(true)).await;
    h.audience(&hidden, AudienceFact::Hidden(true)).await;
    h.audience(&opted_out, AudienceFact::Suggestible(false)).await;

    let found = suggest(&h, &me).await;
    assert_eq!(found, vec![(popular, 2), (known, 1)], "ranked by mutual followees; the rest filtered out");

    // Turning suggestions back on brings the profile back.
    h.audience(&opted_out, AudienceFact::Suggestible(true)).await;
    let found = suggest(&h, &me).await;
    assert!(found.iter().any(|(id, _)| *id == opted_out));

    // Never announced (a lagging or lost creation event): not suggested.
    let silent = p();
    h.follow(&a, &silent).await;
    assert!(!suggest(&h, &me).await.iter().any(|(id, _)| *id == silent), "fails closed");

    // Nobody followed: nothing to suggest.
    assert!(suggest(&h, &p()).await.is_empty());
}

/// #834: pages of the same ranking, each resuming after the last; the
/// filtered profiles stay out of every page; the last token is empty.
#[tokio::test]
async fn suggestions_page_through_the_whole_ranking() {
    let h = TestHarness::start().await;
    let p = harness::random_profile;
    let (me, a, b) = (p(), p(), p());
    h.follow(&me, &a).await;
    h.follow(&me, &b).await;

    // Seven suggestible friends of friends (two followed by both a and b),
    // and three filtered out between them.
    let shown: Vec<ProfileId> = (0..7).map(|_| p()).collect();
    for (i, target) in shown.iter().enumerate() {
        h.follow(&a, target).await;
        if i < 2 {
            h.follow(&b, target).await;
        }
        h.audience(target, AudienceFact::Suggestible(true)).await;
    }
    let (private, opted_out, blocked) = (p(), p(), p());
    for target in [&private, &opted_out, &blocked] {
        h.follow(&a, target).await;
    }
    h.audience(&private, AudienceFact::Suggestible(true)).await;
    h.audience(&private, AudienceFact::Private(true)).await;
    h.audience(&blocked, AudienceFact::Suggestible(true)).await;
    h.block(&me, &blocked).await;

    let everything = suggest(&h, &me).await;
    assert_eq!(everything.len(), 7);

    let mut paged = Vec::new();
    let mut token = None;
    let mut pages = 0;
    loop {
        let found = page(&h, &me, 2, token).await;
        pages += 1;
        assert!(found.suggestions.len() <= 2);
        paged.extend(found.suggestions.into_iter().map(|s| (s.profile_id, s.mutual_count)));
        match found.next_page_token {
            Some(next) => token = Some(next),
            None => break,
        }
        assert!(pages < 10, "the pages end");
    }
    assert_eq!(paged, everything, "the same ranking, each profile once");
    assert_eq!(paged[..2].iter().map(|(_, n)| *n).collect::<Vec<_>>(), vec![2, 2]);

    // A garbled token is refused.
    let query = SuggestProfilesQuery { profile_id: me.as_str(), limit: 2, page_token: Some("%%%".to_owned()) };
    let refused: Result<SuggestionPage, _> = h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await;
    assert!(refused.is_err());
}

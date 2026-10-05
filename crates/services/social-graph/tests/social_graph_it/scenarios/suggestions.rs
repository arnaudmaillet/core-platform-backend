//! #661 over the real tables: people one may know — friends of friends,
//! ranked by how many of one's followees follow them; never oneself, a
//! followed profile, a block either way, a private or hidden profile, or one
//! that turned suggestions off.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::AudienceFact;
use social_graph::application::query::{SuggestProfilesQuery, Suggestion};

use crate::social_graph_it::harness::{self, ProfileId, TestHarness};

async fn suggest(h: &TestHarness, me: &ProfileId) -> Vec<(ProfileId, u32)> {
    let query = SuggestProfilesQuery { profile_id: me.as_str(), limit: 20 };
    let found: Vec<Suggestion> = h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.unwrap();
    found.into_iter().map(|s| (s.profile_id, s.mutual_count)).collect()
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

    // Nobody followed: nothing to suggest.
    assert!(suggest(&h, &p()).await.is_empty());
}

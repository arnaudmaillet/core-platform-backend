//! Scenario — interest tags (#662) against a live Redis: a first reaction
//! teaches the post's hashtags, they rank a For You page, and the holder can
//! remove one (it stays out), reset them all, and a deleted profile loses them.

use std::time::{SystemTime, UNIX_EPOCH};

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use timeline::application::command::apply_discovery_signal::{ApplyDiscoverySignalCommand, DiscoverySignal};
use timeline::application::command::manage_interests::{RemoveInterestCommand, ResetInterestsCommand};
use timeline::application::query::get_discovery_feed::{DiscoveryPage, GetDiscoveryFeedQuery};
use timeline::application::query::list_interests::ListInterestsQuery;
use timeline::domain::value_object::{ContentLevel, DiscoveryRanking, Interest, ProfileId, Viewer};

use crate::timeline_it::harness::{self, HarnessOptions, TestHarness};

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

async fn signal(h: &TestHarness, signal: DiscoverySignal) {
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ApplyDiscoverySignalCommand { signal }))
        .await
        .expect("discovery signal");
}

async fn publish(h: &TestHarness, caption: &str, published_at_ms: i64) -> String {
    let post_id = Uuid::now_v7().to_string();
    signal(h, DiscoverySignal::Published {
        post_id:   post_id.clone(),
        author_id: harness::random_author().as_uuid().to_string(),
        published_at_ms,
        tags:      timeline::domain::value_object::interest::hashtags(caption),
    })
    .await;
    post_id
}

async fn react(h: &TestHarness, reader: &ProfileId, post_id: &str) {
    signal(h, DiscoverySignal::Reacted { post_id: post_id.into(), profile_id: reader.to_string(), at_ms: now_ms() }).await;
}

async fn interests(h: &TestHarness, reader: &ProfileId) -> Vec<Interest> {
    let query = ListInterestsQuery { profile_id: reader.to_string() };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("list interests")
}

async fn tags(h: &TestHarness, reader: &ProfileId) -> Vec<String> {
    interests(h, reader).await.into_iter().map(|i| i.tag).collect()
}

async fn for_you(h: &TestHarness, reader: Option<ProfileId>) -> DiscoveryPage {
    let query = GetDiscoveryFeedQuery {
        ranking:         DiscoveryRanking::ForYou,
        viewer:          Viewer::Profiles(vec![]),
        guest:           None,
        content_level:   ContentLevel::Restricted,
        lat:             None,
        lng:             None,
        limit:           50,
        page_token:      None,
        personalize_for: reader,
    };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("discovery page")
}

/// The order `mine` comes in on the first For You page.
fn order(page: &DiscoveryPage, mine: &[&String]) -> Vec<String> {
    page.items.iter().map(|e| e.post_id.to_string()).filter(|id| mine.contains(&id)).collect()
}

#[tokio::test]
async fn interest_tags_rank_for_you_and_stay_under_the_holders_control() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let reader = harness::random_profile();
    // Tags no other test uses (the pool is shared).
    let tag = format!("it{}", Uuid::now_v7().simple());
    let tag2 = format!("it{}", Uuid::now_v7().simple());
    let t = now_ms();
    // The newest posts of the pool: both on the first page, `plain` first.
    let plain = publish(&h, "nothing to see", t).await;
    let tagged = publish(&h, &format!("look #{tag}"), t - 1_000).await;
    let other = publish(&h, &format!("again #{tag} #{tag2}"), t - 2_000).await;
    let mine = [&plain, &tagged];

    // Nothing learnt yet; a reaction to a post the pool never saw teaches nothing.
    assert!(tags(&h, &reader).await.is_empty());
    react(&h, &reader, &Uuid::now_v7().to_string()).await;
    assert!(tags(&h, &reader).await.is_empty());

    // A first reaction teaches the post's tags, once (a redelivery changes nothing).
    react(&h, &reader, &tagged).await;
    react(&h, &reader, &tagged).await;
    let learnt = interests(&h, &reader).await;
    assert_eq!(learnt.len(), 1, "{learnt:?}");
    assert_eq!(learnt[0].tag, tag);
    assert!((learnt[0].weight - 1.0).abs() < 0.01, "one reaction, weight {}", learnt[0].weight);

    // It ranks the reader's For You page; another reader's is unranked.
    let page = for_you(&h, Some(reader)).await;
    assert!(page.personalized);
    assert_eq!(order(&page, &mine), vec![tagged.clone(), plain.clone()]);
    let page = for_you(&h, None).await;
    assert!(!page.personalized);
    assert_eq!(order(&page, &mine), vec![plain.clone(), tagged.clone()]);

    // Removed: no longer listed nor ranking, and later reactions do not bring
    // it back (they still teach the other tags).
    let cmd = RemoveInterestCommand { profile_id: reader.to_string(), tag: format!("#{tag}") };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("remove");
    assert!(tags(&h, &reader).await.is_empty());
    let page = for_you(&h, Some(reader)).await;
    assert!(!page.personalized);
    assert_eq!(order(&page, &mine), vec![plain.clone(), tagged.clone()]);
    react(&h, &reader, &other).await;
    assert_eq!(tags(&h, &reader).await, vec![tag2.clone()]);

    // Reset: everything forgotten, the removed tag and the counted posts included.
    let cmd = ResetInterestsCommand { profile_id: reader.to_string() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("reset");
    assert!(tags(&h, &reader).await.is_empty());
    react(&h, &reader, &other).await;
    let mut relearnt = tags(&h, &reader).await;
    relearnt.sort();
    let mut both = vec![tag.clone(), tag2.clone()];
    both.sort();
    assert_eq!(relearnt, both);

    // A deleted profile's interests go with it, the counted posts too.
    signal(&h, DiscoverySignal::ProfileErased { profile_id: reader.to_string() }).await;
    assert!(tags(&h, &reader).await.is_empty());
    react(&h, &reader, &other).await;
    assert_eq!(tags(&h, &reader).await.len(), 2);
}

#[tokio::test]
async fn opting_out_erases_the_interests_and_stops_learning_until_opted_back_in() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let reader = harness::random_profile();
    let tag = format!("it{}", Uuid::now_v7().simple());
    let t = now_ms();
    let first = publish(&h, &format!("one #{tag}"), t).await;
    let second = publish(&h, &format!("two #{tag}"), t - 1_000).await;
    let personalization = |on| DiscoverySignal::Personalization { profile_id: reader.to_string(), on };

    react(&h, &reader, &first).await;
    assert_eq!(tags(&h, &reader).await, vec![tag.clone()]);

    // Off (profile FeedSettings.non_personalized, or a teen's default): what
    // was learnt is erased, nothing new is learnt, For You is not ranked —
    // whatever the request says.
    signal(&h, personalization(false)).await;
    assert!(tags(&h, &reader).await.is_empty());
    react(&h, &reader, &second).await;
    assert!(tags(&h, &reader).await.is_empty());
    assert!(!for_you(&h, Some(reader)).await.personalized);

    // The holder's reset keeps the opt-out.
    let cmd = ResetInterestsCommand { profile_id: reader.to_string() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("reset");
    react(&h, &reader, &second).await;
    assert!(tags(&h, &reader).await.is_empty());

    // Back on: learning resumes from scratch (redelivered "off" or "on" is idempotent).
    signal(&h, personalization(true)).await;
    signal(&h, personalization(true)).await;
    react(&h, &reader, &first).await;
    assert_eq!(tags(&h, &reader).await, vec![tag.clone()]);
    assert!(for_you(&h, Some(reader)).await.personalized);

    // Erasure takes the opt-out with it.
    signal(&h, personalization(false)).await;
    signal(&h, DiscoverySignal::ProfileErased { profile_id: reader.to_string() }).await;
    react(&h, &reader, &second).await;
    assert_eq!(tags(&h, &reader).await, vec![tag]);
}

//! #653 over the live ScyllaDB ledger: a profile's reactions, paged by post —
//! what the GDPR data export reads. Written and removed together with the
//! post's ledger row; reactions from before the index existed are found once
//! the backfill ran.

use std::sync::Arc;

use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};
use uuid::Uuid;

use engagement::application::port::ReactionLedger;
use engagement::domain::value_object::{PostId, ProfileId, ReactionKind};
use engagement::infrastructure::persistence::ScyllaReactionLedger;

const KEYSPACE: &str = "engagement";
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

async fn ledger() -> (ScyllaReactionLedger, Arc<scylla_storage::ScyllaClient>) {
    let contact = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
    let client = Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    );
    (ScyllaReactionLedger::new(Arc::clone(&client)), client)
}

fn post() -> PostId {
    PostId::from_uuid(Uuid::now_v7())
}

#[tokio::test]
async fn a_profiles_reactions_page_by_post_and_follow_the_ledger() {
    let (ledger, _) = ledger().await;
    let (me, other) = (ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap(), ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap());
    let posts: Vec<PostId> = (0..5).map(|_| post()).collect();
    for (i, p) in posts.iter().enumerate() {
        ledger.upsert(p, &me, ReactionKind::Heart, 1, 1_000 + i as i64).await.unwrap();
    }
    ledger.upsert(&posts[0], &other, ReactionKind::Fire, 1, 2_000).await.unwrap();
    // A changed reaction is one row, its latest kind.
    ledger.upsert(&posts[1], &me, ReactionKind::Clap, 1, 3_000).await.unwrap();

    let first = ledger.list_by_profile(&me, 3, None).await.unwrap();
    assert_eq!(first.len(), 3);
    let rest = ledger.list_by_profile(&me, 3, Some(&first[2].post_id)).await.unwrap();
    assert_eq!(rest.len(), 2);
    let seen: Vec<PostId> = first.iter().chain(&rest).map(|r| r.post_id.clone()).collect();
    assert_eq!(seen, posts, "ordered by post id (v7: by time), all of mine, none of theirs");
    assert_eq!(first[1].kind, ReactionKind::Clap);
    assert_eq!(first[1].reacted_at_ms, 3_000);

    // Removed from both tables at once.
    ledger.remove(&posts[0], &me).await.unwrap();
    let after = ledger.list_by_profile(&me, 10, None).await.unwrap();
    assert_eq!(after.len(), 4);
    assert!(ledger.scan_for_recovery(&posts[0]).await.unwrap().iter().all(|r| r.profile_id != Uuid::parse_str(&me.as_str()).unwrap()));
}

#[tokio::test]
async fn reactions_from_before_the_index_are_found_after_the_backfill() {
    let (ledger, client) = ledger().await;
    let me = ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap();
    let p = post();
    ledger.upsert(&p, &me, ReactionKind::Rocket, 1, 5_000).await.unwrap();
    client
        .session
        .execute_unpaged(
            "DELETE FROM engagement.reactions_by_profile WHERE profile_id = ?",
            (Uuid::parse_str(&me.as_str()).unwrap(),),
        )
        .await
        .unwrap();
    assert!(ledger.list_by_profile(&me, 10, None).await.unwrap().is_empty());

    assert!(ledger.backfill_profile_index().await.unwrap() >= 1);
    ledger.backfill_profile_index().await.unwrap();
    let found = ledger.list_by_profile(&me, 10, None).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!((found[0].post_id.clone(), found[0].kind), (p, ReactionKind::Rocket));
}

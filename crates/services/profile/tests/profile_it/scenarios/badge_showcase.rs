//! Scenario — badge showcases (#912) over ScyllaDB: a profile shows badges its
//! account earned, in order, with when; an unearned one is refused; an empty
//! list clears it.

use cqrs::error::CqrsError;
use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use profile::application::command::SetBadgeShowcaseCommand;

use crate::profile_it::harness::{self, TestHarness};

async fn show(h: &TestHarness, id: &str, badges: &[&str]) -> Result<(), CqrsError> {
    let cmd = SetBadgeShowcaseCommand { profile_id: id.to_owned(), badge_ids: badges.iter().map(|b| (*b).to_owned()).collect() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

#[tokio::test]
async fn a_profile_shows_badges_its_account_earned() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let id = h.get_by_handle(&handle).await.expect("created").id;
    h.wallet.granted.lock().unwrap().extend([
        (account.clone(), "badge.continent_eu".to_owned()),
        (account.clone(), "badge.continent_an".to_owned()),
    ]);

    match show(&h, &id, &["badge.continent_as"]).await {
        Err(CqrsError::Handler(e)) => assert_eq!(e.error_code(), "PRF-6007"),
        other => panic!("expected PRF-6007, got {other:?}"),
    }
    show(&h, &id, &["badge.continent_an", "badge.continent_eu"]).await.expect("earned badges");
    let shown: Vec<_> = h.get_by_handle(&handle).await.unwrap().badges.into_iter().map(|b| (b.badge_id, b.awarded_at.timestamp())).collect();
    assert_eq!(shown, [("badge.continent_an".to_owned(), 86_400), ("badge.continent_eu".to_owned(), 86_400)], "in order, with when");

    show(&h, &id, &[]).await.expect("cleared");
    assert!(h.get_by_handle(&handle).await.unwrap().badges.is_empty());
}

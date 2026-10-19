//! Scenario — profile slots (#916) over ScyllaDB: one free, a create past the
//! slots refused, a slot bought once per slot number (raced or retried), kept
//! when a profile is deleted, an older account's profiles grandfathered, and
//! erased with the account.

use cqrs::error::CqrsError;
use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use profile::application::command::EraseAccountVerificationsCommand;
use profile::application::profile_slots::SlotOutcome;
use profile::domain::value_object::AccountId;

use crate::profile_it::harness::{self, TestHarness};

fn code(result: Result<(), CqrsError>) -> Option<&'static str> {
    match result {
        Err(CqrsError::Handler(e)) => Some(e.error_code()),
        _ => None,
    }
}

#[tokio::test]
async fn a_second_profile_needs_a_bought_slot_kept_after_a_delete() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let account_id = AccountId::try_from(account.as_str()).unwrap();
    let bus = || std::sync::Arc::clone(&h.command_bus);

    h.create(&account, &harness::random_handle(), "First").await;
    let refused = harness::dispatch_create(bus(), &account, &harness::random_handle(), "Second").await;
    assert_eq!(code(refused), Some("PRF-6001"), "one free slot");

    let state = h.slots.state(&account_id).await.unwrap();
    assert_eq!((state.slots, state.used, state.next_price), (1, 1, Some(100)));
    let moved = h.slots.buy(&account_id, 50, true, None).await.unwrap();
    assert_eq!((moved.outcome, moved.price), (SlotOutcome::PriceChanged, 100));
    assert!(matches!(h.slots.buy(&account_id, 100, false, None).await, Err(e) if e.error_code() == "PRF-6002"));

    // Raced: both answer BOUGHT, the slot is charged and counted once.
    let (a, b) = tokio::join!(h.slots.buy(&account_id, 100, true, None), h.slots.buy(&account_id, 100, true, None));
    assert_eq!((a.unwrap().outcome, b.unwrap().outcome), (SlotOutcome::Bought, SlotOutcome::Bought));
    let state = h.slots.state(&account_id).await.unwrap();
    assert_eq!((state.slots, state.next_price), (2, Some(200)));
    let mine = account_id.as_uuid().to_string();
    assert_eq!(h.wallet.spent.lock().unwrap().iter().filter(|(k, _)| k.starts_with(&mine)).count(), 1);

    let second = harness::random_handle();
    harness::dispatch_create(bus(), &account, &second, "Second").await.expect("the bought slot");
    let id = h.get_by_handle(&second).await.expect("created").id;
    harness::dispatch_delete(bus(), &id).await.expect("delete");
    let state = h.slots.state(&account_id).await.unwrap();
    assert_eq!((state.slots, state.used), (2, 1), "a deleted profile frees its slot, the slot stays");
    harness::dispatch_create(bus(), &account, &harness::random_handle(), "Again").await.expect("the slot is reused");

    // Erased with the account.
    let erase = EraseAccountVerificationsCommand { account_id: account.clone() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), erase)).await.expect("erase");
    assert_eq!(h.slots.store.get(&account_id).await.unwrap(), None);
}

/// An account holding more profiles than its slots from before slots existed
/// keeps them, recorded the first time they are counted.
#[tokio::test]
async fn an_older_accounts_profiles_are_grandfathered() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let account_id = AccountId::try_from(account.as_str()).unwrap();
    // Three profiles from before slots: created under a temporary row, then
    // the row dropped.
    h.slots.store.init(&account_id, 3).await.unwrap();
    for name in ["A", "B", "C"] {
        h.create(&account, &harness::random_handle(), name).await;
    }
    h.slots.store.erase(&account_id).await.unwrap();

    let state = h.slots.state(&account_id).await.unwrap();
    assert_eq!((state.slots, state.used, state.next_price), (3, 3, Some(400)));
    assert_eq!(h.slots.store.get(&account_id).await.unwrap(), Some(3), "recorded for good");
}

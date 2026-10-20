//! Scenario — paid handle changes (#911) over ScyllaDB: the first change is
//! free, later ones cost gems at the shown price, a taken handle is never
//! charged, a retry changes nothing, a caller who may not spend gems waits 30
//! days, and the count is erased with the account.

use cqrs::error::CqrsError;
use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use profile::application::command::{ChangeHandleCommand, EraseAccountVerificationsCommand};
use profile::application::handle_changes::HandlePayer;
use profile::domain::value_object::AccountId;

use crate::profile_it::harness::{self, TestHarness};

fn code(result: Result<(), CqrsError>) -> Option<&'static str> {
    match result {
        Err(CqrsError::Handler(e)) => Some(e.error_code()),
        Err(_) => Some("other"),
        Ok(()) => None,
    }
}

async fn change(h: &TestHarness, id: &str, to: &str, price: i64, adult: bool) -> Result<(), CqrsError> {
    let cmd = ChangeHandleCommand {
        profile_id:     id.to_owned(),
        new_handle:     to.to_owned(),
        expected_price: price,
        payer:          Some(HandlePayer { adult, token: None }),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

#[tokio::test]
async fn the_first_change_is_free_and_later_ones_cost_gems_once() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let account_id = AccountId::try_from(account.as_str()).unwrap();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let id = h.get_by_handle(&handle).await.expect("created").id;

    let first = harness::random_handle();
    assert_eq!(code(change(&h, &id, &first, 0, true).await), None, "the first change is free");
    let quote = h.handle_changes.quote(&account_id, &HandlePayer { adult: true, token: None }, chrono::Utc::now()).await.unwrap();
    assert_eq!((quote.price, quote.changes), (100, 1));

    let second = harness::random_handle();
    assert_eq!(code(change(&h, &id, &second, 0, true).await), Some("PRF-6004"), "the shown price is checked");
    // A taken handle is refused before any charge.
    let taken = harness::random_handle();
    h.create(&harness::random_account_id(), &taken, "Bob").await;
    assert_eq!(code(change(&h, &id, &taken, 100, true).await), Some("PRF-1002"));
    let mine = account_id.as_uuid().to_string();
    let charges = || h.wallet.spent.lock().unwrap().iter().filter(|(k, _)| k.starts_with(&mine)).count();
    assert_eq!(charges(), 0, "nothing charged");

    assert_eq!(code(change(&h, &id, &second, 100, true).await), None);
    assert_eq!(code(change(&h, &id, &second, 100, true).await), None, "a retry changes nothing");
    assert_eq!(charges(), 1, "charged once");
    assert_eq!(h.get_by_handle(&second).await.expect("renamed").id, id);

    let erase = EraseAccountVerificationsCommand { account_id: account.clone() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), erase)).await.expect("erase");
    assert_eq!(h.handle_changes.store.get(&account_id).await.unwrap(), None);
}

/// A caller who may not spend gems changes for free, once per 30 days.
#[tokio::test]
async fn a_minor_changes_for_free_once_per_cooldown() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Teen").await;
    let id = h.get_by_handle(&handle).await.expect("created").id;

    assert_eq!(code(change(&h, &id, &harness::random_handle(), 0, false).await), None);
    assert_eq!(code(change(&h, &id, &harness::random_handle(), 0, false).await), Some("PRF-6005"), "30 days");
    assert!(h.wallet.spent.lock().unwrap().iter().all(|(k, _)| !k.starts_with(&account)));
}

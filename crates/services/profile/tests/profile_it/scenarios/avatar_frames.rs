//! Scenario — avatar frames (#913) over ScyllaDB: a profile wears a frame its
//! account owns, the profile and the account's list read it back, another
//! frame is refused, and taking it off clears both.

use cqrs::error::CqrsError;
use cqrs::{CommandBus, Envelope};
use error::AppError;
use uuid::Uuid;

use profile::application::command::EquipAvatarFrameCommand;
use profile::domain::value_object::AccountId;

use crate::profile_it::harness::{self, TestHarness};

async fn equip(h: &TestHarness, id: &str, frame: Option<&str>) -> Result<(), CqrsError> {
    let cmd = EquipAvatarFrameCommand { profile_id: id.to_owned(), frame_id: frame.map(str::to_owned) };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

#[tokio::test]
async fn a_profile_wears_an_owned_frame_seen_on_its_reads() {
    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let account_id = AccountId::try_from(account.as_str()).unwrap();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let id = h.get_by_handle(&handle).await.expect("created").id;

    // The harness wallet owns `frame.gold` only.
    match equip(&h, &id, Some("frame.neon")).await {
        Err(CqrsError::Handler(e)) => assert_eq!(e.error_code(), "PRF-6007"),
        other => panic!("expected PRF-6007, got {other:?}"),
    }
    equip(&h, &id, Some("frame.gold")).await.expect("an owned frame");
    assert_eq!(h.get_by_handle(&handle).await.unwrap().avatar_frame_id.as_deref(), Some("frame.gold"));
    let (list, _) = h.repository.list_by_account(&account_id, 50, None).await.unwrap();
    assert_eq!(list[0].avatar_frame_id.as_deref(), Some("frame.gold"), "the account's list too");

    equip(&h, &id, None).await.expect("taken off");
    assert_eq!(h.get_by_handle(&handle).await.unwrap().avatar_frame_id, None);
    let (list, _) = h.repository.list_by_account(&account_id, 50, None).await.unwrap();
    assert_eq!(list[0].avatar_frame_id, None);
}

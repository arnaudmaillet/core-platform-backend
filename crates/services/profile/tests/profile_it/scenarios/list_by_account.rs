//! Scenario — `ListProfilesByAccount` round-trips against ScyllaDB.
//!
//! Regression for the CQL `LIMIT` bind: the limit was cast to `i64`, which the
//! driver serializes as `BigInt` while CQL `LIMIT` is a native `int` — so every
//! call failed type-checking ("expected BigInt") before the query ran. Nothing
//! exercised the repository method, so it slipped through. These tests drive the
//! real bind end-to-end (one profile, and the zero-row case that still binds LIMIT).

use uuid::Uuid;

use profile::domain::value_object::AccountId;

use crate::profile_it::harness::{self, TestHarness};

#[tokio::test]
async fn list_by_account_returns_the_accounts_profile() {
    let h = TestHarness::start().await;

    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;

    let account_id = AccountId::from_uuid(Uuid::parse_str(&account).expect("valid account uuid"));
    let (summaries, _next) = h
        .repository
        .list_by_account(&account_id, 50, None)
        .await
        .expect("list_by_account must not error on the LIMIT bind");

    assert_eq!(summaries.len(), 1, "the created profile is listed");
    assert_eq!(summaries[0].handle, handle);
}

#[tokio::test]
async fn list_by_account_is_ok_when_the_account_has_no_profiles() {
    let h = TestHarness::start().await;

    // Binds and executes the same LIMIT-carrying query with zero matching rows —
    // this is what surfaced the type mismatch regardless of result count.
    let account_id = AccountId::from_uuid(Uuid::now_v7());
    let (summaries, next) = h
        .repository
        .list_by_account(&account_id, 50, None)
        .await
        .expect("list_by_account must not error even with zero rows");

    assert!(summaries.is_empty(), "no profiles for a fresh account");
    assert!(next.is_none(), "no next page when empty");
}

/// The account's profile list reads its own index (`profiles_by_account`):
/// every command that changes what it shows — display name, handle,
/// visibility, status — keeps it in step.
#[tokio::test]
async fn the_account_list_follows_profile_updates() {
    use cqrs::{CommandBus, Envelope};
    use profile::application::command::{
        ChangeHandleCommand, HideProfileCommand, RestoreProfileCommand, SetVisibilityCommand,
    };

    let h = TestHarness::start().await;
    let account = harness::random_account_id();
    let handle = harness::random_handle();
    h.create(&account, &handle, "Alice").await;
    let profile_id = h.get_by_handle(&handle).await.expect("created").id;
    let account_id = AccountId::from_uuid(Uuid::parse_str(&account).expect("valid account uuid"));
    let summary = || async {
        let (summaries, _) = h.repository.list_by_account(&account_id, 50, None).await.expect("list");
        summaries.into_iter().next().expect("the profile is listed")
    };

    h.update_display(&profile_id, "Alice B.").await;
    assert_eq!(summary().await.display_name, "Alice B.");

    let renamed = harness::random_handle();
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ChangeHandleCommand { profile_id: profile_id.clone(), new_handle: renamed.clone(), ..Default::default() }))
        .await
        .expect("change handle");
    assert_eq!(summary().await.handle, renamed);

    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), SetVisibilityCommand { profile_id: profile_id.clone(), visibility: "private".into() }))
        .await
        .expect("set visibility");
    assert_eq!(summary().await.visibility, "private");

    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), HideProfileCommand {
            profile_id:        profile_id.clone(),
            masking_reason:    "content_policy_violation".into(),
            suspension_reason: None,
        }))
        .await
        .expect("hide");
    assert_eq!(summary().await.status, "hidden");

    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), RestoreProfileCommand { profile_id: profile_id.clone() }))
        .await
        .expect("restore");
    assert_eq!(summary().await.status, "active");
}

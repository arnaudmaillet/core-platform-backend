//! Scenario — viewer-aware profile reads and account-level masking, over the
//! real store and cache.
//!
//! A suspended account's profiles (all of them, not one) disappear for everyone
//! but their owner; reactivation brings back exactly the ones the suspension
//! hid. Non-owners never see the account id that would link an account's
//! profiles together.

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use profile::application::command::{
    HideAccountProfilesCommand, HideProfileCommand, RestoreAccountProfilesCommand,
};

use crate::profile_it::harness::{self, TestHarness, Viewer};

#[tokio::test]
async fn suspension_hides_every_profile_of_the_account_until_reactivation() {
    let h = TestHarness::start().await;

    let account = harness::random_account_id();
    let (main, alt, policy_hidden) =
        (harness::random_handle(), harness::random_handle(), harness::random_handle());
    for handle in [&main, &alt, &policy_hidden] {
        h.create(&account, handle, "Alice").await;
    }
    let owner = Viewer::Account(account.clone());
    let stranger = Viewer::Account(harness::random_account_id());

    // Non-owners get the header without the owner-only fields.
    let seen = h.get_by_handle_as(&main, stranger.clone()).await.expect("active is visible");
    assert!(seen.account_id.is_empty(), "the account id would link profiles");
    assert_eq!(h.get_by_handle_as(&main, owner.clone()).await.unwrap().account_id, account);

    // One profile hidden by a content-policy decision before the suspension.
    let policy_id = h.get_by_handle(&policy_hidden).await.unwrap().id;
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), HideProfileCommand {
            profile_id: policy_id,
            masking_reason: "content_policy_violation".into(),
            suspension_reason: None,
        }))
        .await
        .expect("hide one profile");

    // Suspension: every profile of the account is hidden from others…
    let suspend = HideAccountProfilesCommand {
        account_id: account.clone(),
        masking_reason: "account_suspended".into(),
        suspension_reason: Some("spam".into()),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), suspend.clone())).await.expect("suspend");
    // …and a redelivery is a no-op, not an error.
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), suspend)).await.expect("redelivered");
    for handle in [&main, &alt, &policy_hidden] {
        assert!(h.get_by_handle_as(handle, Viewer::Anonymous).await.is_none(), "{handle}");
        assert!(h.get_by_handle_as(handle, stranger.clone()).await.is_none(), "{handle}");
        assert!(h.get_by_handle_as(handle, owner.clone()).await.is_some(), "the owner still sees it");
    }

    // Reactivation restores what the suspension hid, not the policy hide.
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), RestoreAccountProfilesCommand { account_id: account }))
        .await
        .expect("reactivate");
    assert!(h.get_by_handle_as(&main, Viewer::Anonymous).await.is_some());
    assert!(h.get_by_handle_as(&alt, Viewer::Anonymous).await.is_some());
    assert!(h.get_by_handle_as(&policy_hidden, Viewer::Anonymous).await.is_none());
}

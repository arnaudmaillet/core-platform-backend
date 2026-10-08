//! #670 part 2 over live Scylla: a supervised teen's profiles are tightened to
//! their supervisor's floors at once, loosening is refused while the floors
//! hold (stricter stays allowed), and lifted floors leave the settings as
//! they are, unlocked.

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use profile::application::command::{
    ApplySupervisionFloorCommand, SetDiscoverySettingsCommand, SetInteractionSettingsCommand, SetVisibilityCommand,
};
use profile::domain::value_object::{InteractionAudience, InteractionSettings, SupervisionFloor};

use crate::profile_it::harness::{self, TestHarness};

#[tokio::test]
async fn floors_tighten_lock_and_lift() {
    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Teen").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    assert_eq!(profile.visibility, "public");

    let floor = SupervisionFloor {
        private_account:    true,
        messages:           Some(InteractionAudience::Mutuals),
        comments:           Some(InteractionAudience::Followers),
        hidden_from_search: true,
    };
    let apply = ApplySupervisionFloorCommand { account_id: account.clone(), floor: Some(floor) };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), apply.clone())).await.expect("apply");
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), apply)).await.expect("redelivered");

    let tightened = h.get_by_id(&profile.id).await.unwrap();
    assert_eq!(tightened.visibility, "private");
    assert_eq!((tightened.interaction.messages, tightened.interaction.comments), (InteractionAudience::Mutuals, InteractionAudience::Followers));
    let discovery = tightened.discovery.expect("owner view");
    assert!(!discovery.by_handle_search && !discovery.in_suggestions && discovery.by_qr);

    // Loosening is refused; stricter is fine; what no floor covers is free.
    let public = SetVisibilityCommand { profile_id: profile.id.clone(), visibility: "public".into() };
    let err = h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), public.clone())).await.unwrap_err();
    assert!(err.to_string().contains("set by your supervisor"), "{err}");
    let open = SetInteractionSettingsCommand {
        profile_id: profile.id.clone(),
        settings: InteractionSettings::default(),
        allow_remix: None,
        allow_sound_reuse: None,
    };
    assert!(h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), open)).await.is_err());
    let stricter = SetInteractionSettingsCommand {
        profile_id: profile.id.clone(),
        settings: InteractionSettings { messages: InteractionAudience::NoOne, comments: InteractionAudience::Mutuals, ..tightened.interaction },
        allow_remix: None,
        allow_sound_reuse: None,
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), stricter)).await.expect("stricter is allowed");
    let found = SetDiscoverySettingsCommand { profile_id: profile.id.clone(), by_handle_search: Some(true), ..Default::default() };
    assert!(h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), found)).await.is_err());
    let qr = SetDiscoverySettingsCommand { profile_id: profile.id.clone(), by_qr: Some(false), ..Default::default() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), qr)).await.expect("not covered by a floor");

    // A profile the teen creates now is born at the floors.
    let second = harness::random_handle();
    h.create(&account, &second, "Teen 2").await;
    let born = h.get_by_handle(&second).await.expect("created");
    assert_eq!((born.visibility.as_str(), born.interaction.messages), ("private", InteractionAudience::Mutuals));

    // Lifted: the settings stay, and the teen may loosen them again.
    let lift = ApplySupervisionFloorCommand { account_id: account.clone(), floor: None };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), lift)).await.expect("lift");
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().visibility, "private", "kept as is");
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), public)).await.expect("unlocked");
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().visibility, "public");
}

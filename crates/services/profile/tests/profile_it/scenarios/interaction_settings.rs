//! Scenario — interaction settings over the real store: written through the
//! new column, read back (also past the cache), and announced on the wire.

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use profile::application::command::SetInteractionSettingsCommand;
use profile::domain::value_object::{InteractionAudience, InteractionSettings};

use crate::profile_it::harness::{self, TestHarness};

#[tokio::test]
async fn interaction_settings_round_trip_and_are_announced() {
    let h = TestHarness::start().await;
    let handle = harness::random_handle();
    h.create(&harness::random_account_id(), &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    assert_eq!(profile.interaction, InteractionSettings::default(), "defaults until set");

    let quiet = InteractionSettings {
        comments: InteractionAudience::Mutuals,
        mentions: InteractionAudience::Followers,
        messages: InteractionAudience::NoOne,
        allow_downloads: false,
        show_like_counts: false,
    };
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            SetInteractionSettingsCommand { profile_id: profile.id.clone(), settings: quiet },
        ))
        .await
        .expect("set");

    assert_eq!(h.get_by_id(&profile.id).await.unwrap().interaction, quiet);
    assert!(h.publisher.published().iter().any(|t| t == "ProfileInteractionSettingsChanged"));
}

#[tokio::test]
async fn location_settings_round_trip_owner_only_and_are_announced() {
    use profile::application::command::SetLocationSettingsCommand;
    use profile::domain::value_object::{LocationPrecision, LocationSettings, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");

    let ghost = LocationSettings { ghost: true, precision: LocationPrecision::City };
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            SetLocationSettingsCommand { profile_id: profile.id.clone(), settings: ghost },
        ))
        .await
        .expect("set");

    assert_eq!(h.get_by_id(&profile.id).await.unwrap().location, Some(ghost));
    // Owner-only: nobody else learns that the profile ghosts the map.
    let other = Viewer::Account(harness::random_account_id());
    assert_eq!(h.get_by_handle_as(&handle, other).await.unwrap().location, None);
    assert!(h.publisher.published().iter().any(|t| t == "ProfileLocationSettingsChanged"));
}

#[tokio::test]
async fn discovery_settings_update_partially_stay_owner_only_and_are_announced() {
    use profile::application::command::SetDiscoverySettingsCommand;
    use profile::domain::value_object::{DiscoverySettings, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    assert_eq!(profile.discovery, Some(DiscoverySettings::default()), "everything on until set");

    // Only the listed flags change.
    let cmd = SetDiscoverySettingsCommand {
        profile_id: profile.id.clone(),
        by_handle_search: Some(false),
        read_receipts: Some(false),
        ..Default::default()
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("set");

    let expected = DiscoverySettings { by_handle_search: false, read_receipts: false, ..DiscoverySettings::default() };
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().discovery, Some(expected));
    let other = Viewer::Account(harness::random_account_id());
    assert_eq!(h.get_by_handle_as(&handle, other).await.unwrap().discovery, None, "owner-only");
    assert!(h.publisher.published().iter().any(|t| t == "ProfileDiscoverySettingsChanged"));
}

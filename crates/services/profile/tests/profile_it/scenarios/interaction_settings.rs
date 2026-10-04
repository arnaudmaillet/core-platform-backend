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

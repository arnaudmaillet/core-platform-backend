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
        allow_remix: false,
        allow_sound_reuse: true,
        limit: None,
    };
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            SetInteractionSettingsCommand {
                profile_id: profile.id.clone(),
                settings: quiet,
                allow_remix: Some(quiet.allow_remix),
                allow_sound_reuse: Some(quiet.allow_sound_reuse),
            },
        ))
        .await
        .expect("set");

    assert_eq!(h.get_by_id(&profile.id).await.unwrap().interaction, quiet);
    assert!(h.publisher.published().iter().any(|t| t == "ProfileInteractionSettingsChanged"));
}

#[tokio::test]
async fn location_settings_round_trip_owner_only_and_are_announced() {
    use profile::application::command::SetLocationSettingsCommand;
    use profile::domain::value_object::{LocationAudience, LocationPrecision, LocationSettings, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");

    let set = |audience, on_new_posts| SetLocationSettingsCommand {
        profile_id: profile.id.clone(),
        ghost: true,
        precision: LocationPrecision::City,
        audience,
        on_new_posts,
    };
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), set(Some(LocationAudience::Followers), Some(false))))
        .await
        .expect("set");
    let ghost = LocationSettings {
        ghost: true,
        precision: LocationPrecision::City,
        audience: LocationAudience::Followers,
        on_new_posts: false,
    };
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().location, Some(ghost));

    // An older client sets ghost and precision only: the audience and the
    // new-posts preference stay as stored (#657).
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), set(None, None))).await.expect("set");
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

#[tokio::test]
async fn comment_filters_are_normalised_owner_only_and_announced() {
    use profile::application::command::SetCommentFiltersCommand;
    use profile::domain::value_object::{CommentFilters, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    assert_eq!(profile.comment_filters, Some(CommentFilters::default()), "offensive filter on by default");

    let cmd = SetCommentFiltersCommand {
        profile_id: profile.id.clone(),
        hidden_words: vec![" Spoiler ".into(), "spoiler".into(), "🍕".into()],
        filter_offensive: false,
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("set");

    let stored = h.get_by_id(&profile.id).await.unwrap().comment_filters.expect("owner sees them");
    assert_eq!(stored.hidden_words, vec!["spoiler".to_owned(), "🍕".to_owned()]);
    assert!(!stored.filter_offensive);
    let other = Viewer::Account(harness::random_account_id());
    assert_eq!(h.get_by_handle_as(&handle, other).await.unwrap().comment_filters, None, "owner-only");
    assert!(h.publisher.published().iter().any(|t| t == "ProfileCommentFiltersChanged"));
}

#[tokio::test]
async fn tab_settings_update_partially_stay_owner_only_and_are_announced() {
    use profile::application::command::SetTabSettingsCommand;
    use profile::domain::value_object::{PostWindow, TabSettings, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    assert_eq!(profile.tab_settings, Some(TabSettings::default()));

    let cmd = SetTabSettingsCommand {
        profile_id: profile.id.clone(),
        post_window: Some(PostWindow::OneMonth),
        show_likes: Some(false),
        ..Default::default()
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("set");

    let expected = TabSettings { post_window: PostWindow::OneMonth, show_likes: false, ..TabSettings::default() };
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().tab_settings, Some(expected));
    let other = Viewer::Account(harness::random_account_id());
    assert_eq!(h.get_by_handle_as(&handle, other).await.unwrap().tab_settings, None, "owner-only");
    assert!(h.publisher.published().iter().any(|t| t == "ProfileTabSettingsChanged"));
}

#[tokio::test]
async fn feed_settings_round_trip_owner_only() {
    use profile::application::command::SetFeedSettingsCommand;
    use profile::domain::value_object::{FeedSettings, SensitiveContent, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    assert_eq!(profile.feed_settings, Some(FeedSettings::default()), "less by default");

    let standard = FeedSettings { sensitive_content: SensitiveContent::Standard };
    let cmd = SetFeedSettingsCommand { profile_id: profile.id.clone(), settings: standard };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("set");

    assert_eq!(h.get_by_id(&profile.id).await.unwrap().feed_settings, Some(standard));
    let other = Viewer::Account(harness::random_account_id());
    assert_eq!(h.get_by_handle_as(&handle, other).await.unwrap().feed_settings, None, "owner-only");
}

#[tokio::test]
async fn account_type_and_a_verification_request_through_review() {
    use cqrs::QueryBus;
    use profile::application::command::{DecideVerificationCommand, RequestVerificationCommand, SetAccountTypeCommand};
    use profile::application::query::{GetVerificationRequestQuery, ListPendingVerificationsQuery};
    use profile::domain::entity::{VerificationRequest, VerificationStatus};
    use profile::domain::value_object::{BusinessInfo, ProfileId, ProfileKind, VerificationKind, Viewer};

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");

    // A brand with a public contact card, seen by everyone.
    let card = BusinessInfo::new("Bakery".into(), Some("hello@bakery.fr".into()), None).unwrap();
    let cmd = SetAccountTypeCommand { profile_id: profile.id.clone(), kind: ProfileKind::Brand, business: Some(card.clone()) };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("account type");
    let seen = h.get_by_handle_as(&handle, Viewer::Account(harness::random_account_id())).await.unwrap();
    assert_eq!(seen.profile_kind, "brand");
    assert_eq!(seen.business_info, Some(card));

    // Ask, get queued, be rejected with a reason, ask again, be approved.
    let ask = || RequestVerificationCommand {
        profile_id: profile.id.clone(),
        category:   VerificationKind::Business,
        documents:  vec!["media/kbis.pdf".into()],
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), ask())).await.expect("request");
    assert!(h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), ask())).await.is_err(), "already pending");

    let pending: (Vec<(ProfileId, VerificationRequest)>, Option<String>) = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ListPendingVerificationsQuery { limit: 100, page_token: None }))
        .await
        .unwrap();
    assert!(pending.0.iter().any(|(id, _)| id.to_string() == profile.id));

    let decide = |approve: bool, reason: Option<&str>| DecideVerificationCommand {
        profile_id: profile.id.clone(),
        approve,
        reason: reason.map(str::to_owned),
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), decide(false, Some("Blurry document")))).await.expect("reject");
    let status: Option<VerificationRequest> = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetVerificationRequestQuery { profile_id: profile.id.clone() }))
        .await
        .unwrap();
    let status = status.expect("a request");
    assert_eq!(status.status, VerificationStatus::Rejected);
    assert_eq!(status.reason.as_deref(), Some("Blurry document"));

    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), ask())).await.expect("ask again");
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), decide(true, None))).await.expect("approve");
    assert!(h.get_by_id(&profile.id).await.unwrap().verified, "the outcome reaches the profile");
    assert!(h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), ask())).await.is_err(), "already verified");
}

#[tokio::test]
async fn an_interaction_limit_is_set_kept_by_other_changes_and_cleared() {
    use profile::application::command::{SetInteractionLimitCommand, SetInteractionSettingsCommand};
    use profile::domain::value_object::{InteractionLimit, InteractionSettings, LimitAudience};

    let h = TestHarness::start().await;
    let handle = harness::random_handle();
    h.create(&harness::random_account_id(), &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");

    let now = chrono::Utc::now();
    let limit = InteractionLimit::new(LimitAudience::RecentFollowers, now + chrono::Duration::days(3), now).unwrap();
    assert!(InteractionLimit::new(LimitAudience::NonFollowers, now + chrono::Duration::weeks(5), now).is_err(), "≤ 4 weeks");
    let set = SetInteractionLimitCommand { profile_id: profile.id.clone(), limit: Some(limit) };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), set)).await.expect("set limit");
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().interaction.limit, Some(limit));

    // Changing the other settings keeps the limit.
    let other = SetInteractionSettingsCommand {
        profile_id: profile.id.clone(),
        settings: InteractionSettings::default(),
        allow_remix: None,
        allow_sound_reuse: None,
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), other)).await.expect("settings");
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().interaction.limit, Some(limit));

    let clear = SetInteractionLimitCommand { profile_id: profile.id.clone(), limit: None };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), clear)).await.expect("clear");
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().interaction.limit, None);
}

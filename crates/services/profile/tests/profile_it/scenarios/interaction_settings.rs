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
        minor: false,
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

    // A minor: mutuals at most — a wider audience, asked for or kept, is
    // refused and nothing changes; mutuals is fine.
    let as_minor = |audience| SetLocationSettingsCommand { minor: true, ..set(audience, None) };
    for wider in [Some(LocationAudience::Everyone), Some(LocationAudience::Followers), None] {
        let refused = h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), as_minor(wider))).await;
        assert!(refused.is_err(), "{wider:?}: the stored audience is followers, wider than mutuals");
    }
    assert_eq!(h.get_by_id(&profile.id).await.unwrap().location, Some(ghost), "unchanged");
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), as_minor(Some(LocationAudience::Mutuals))))
        .await
        .expect("mutuals is fine");
    let location = h.get_by_id(&profile.id).await.unwrap().location.unwrap();
    assert_eq!(location.audience, LocationAudience::Mutuals);
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

    // #661: a holder not known to be 18+ may turn suggestions off, never on.
    let as_minor = |in_suggestions| SetDiscoverySettingsCommand {
        profile_id: profile.id.clone(),
        in_suggestions: Some(in_suggestions),
        minor: true,
        ..Default::default()
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), as_minor(false))).await.expect("off is fine");
    assert!(h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), as_minor(true))).await.is_err(), "on is refused");
    assert!(!h.get_by_id(&profile.id).await.unwrap().discovery.unwrap().in_suggestions);
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

    let standard = FeedSettings { sensitive_content: SensitiveContent::Standard, non_personalized: true };
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
        documents:  vec!["https://bakery.fr/kbis".into()],
        private_documents: Vec::new(),
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

/// #777: private documents are checked with media, the decision is announced
/// with them (media starts their retention), and a deleted account's requests
/// are erased.
#[tokio::test]
async fn verification_evidence_is_checked_announced_and_erased_with_the_account() {
    use cqrs::QueryBus;
    use profile::application::command::{
        DecideVerificationCommand, EraseAccountVerificationsCommand, RequestVerificationCommand,
    };
    use profile::application::query::GetVerificationRequestQuery;
    use profile::domain::entity::VerificationRequest;
    use profile::domain::value_object::VerificationKind;

    let h = TestHarness::start().await;
    let (account, handle) = (harness::random_account_id(), harness::random_handle());
    h.create(&account, &handle, "Alice").await;
    let profile = h.get_by_handle(&handle).await.expect("created");
    let ask = |documents: Vec<String>, private_documents: Vec<String>| RequestVerificationCommand {
        profile_id: profile.id.clone(),
        category: VerificationKind::Business,
        documents,
        private_documents,
    };
    let get = || async {
        let found: Option<VerificationRequest> = h
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetVerificationRequestQuery { profile_id: profile.id.clone() }))
            .await
            .unwrap();
        found
    };

    // Someone else's document, a plain-http link: refused, nothing stored.
    let foreign = h.documents.upload(&harness::random_account_id());
    let refusals = [
        (vec![], vec![foreign], "is not one of your private documents"),
        (vec!["http://alice.example".to_owned()], vec![], "https URL"),
    ];
    for (links, docs, why) in refusals {
        let err = h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), ask(links, docs))).await.unwrap_err();
        assert!(err.to_string().contains(why), "{err}");
    }
    assert!(get().await.is_none());

    // The owner's own document plus a link.
    let mine = h.documents.upload(&account);
    let cmd = ask(vec!["https://alice.example/press".into()], vec![mine.clone()]);
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("request");
    assert_eq!(get().await.expect("pending").private_documents, vec![mine.clone()]);

    let decide = DecideVerificationCommand { profile_id: profile.id.clone(), approve: false, reason: Some("Unreadable".into()) };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), decide)).await.expect("reject");
    let decided = h.publisher.decided();
    assert_eq!(decided.len(), 1);
    assert_eq!(decided[0].account_id.to_string(), account);
    assert!(!decided[0].approved);
    assert_eq!(decided[0].private_documents, vec![mine]);

    // `account_deleted`: the request goes with the account (twice: idempotent).
    for _ in 0..2 {
        let erase = EraseAccountVerificationsCommand { account_id: account.clone() };
        h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), erase)).await.expect("erase");
    }
    assert!(get().await.is_none(), "erased");
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

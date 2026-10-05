//! Scenario — push devices and preferences over the real tables (#654): a
//! category turned off stops that push, a token follows its account, a
//! device's token can change, and a teen's first registration writes the
//! quiet-hours default.

use cqrs::{CommandBus, Envelope};
use tonic::Request;
use uuid::Uuid;

use notification::application::command::push_settings::RegisterDeviceCommand;
use notification::application::command::push_settings::{RegistrationCaller, UpdatePreferencesCommand};
use notification::domain::device::{DevicePlatform, PushEnvironment};
use notification::domain::preferences::{HolderAge, QuietHours};

use crate::notification_it::harness::{proto, TestHarness};

fn register(profile: &str, device: &str, token: &str) -> Request<proto::RegisterDeviceRequest> {
    Request::new(proto::RegisterDeviceRequest {
        profile_id:  profile.to_owned(),
        device_id:   device.to_owned(),
        token:       token.to_owned(),
        platform:    proto::DevicePlatform::Ios as i32,
        environment: proto::PushEnvironment::Production as i32,
        timezone:    "Europe/Paris".into(),
    })
}

async fn targets(h: &TestHarness, profile: &str, category: proto::PushCategory) -> (bool, Vec<String>) {
    let response = h
        .handler
        .resolve_push_targets(Request::new(proto::ResolvePushTargetsRequest {
            profile_id: profile.to_owned(),
            category:   category as i32,
        }))
        .await
        .expect("resolve_push_targets")
        .into_inner();
    (response.allowed, response.devices.into_iter().map(|d| d.token).collect())
}

fn id() -> String {
    Uuid::now_v7().to_string()
}

#[tokio::test]
async fn a_category_turned_off_stops_its_push_and_tokens_follow_their_device() {
    let h = TestHarness::start().await;
    let (profile, token) = (id(), id());
    h.handler.register_device(register(&profile, "phone", &token)).await.expect("register");

    assert_eq!(targets(&h, &profile, proto::PushCategory::Likes).await, (true, vec![token.clone()]));

    // Likes off: that push stops, the others still go.
    let prefs = h
        .handler
        .update_notification_preferences(Request::new(proto::UpdateNotificationPreferencesRequest {
            profile_id: profile.clone(),
            categories: vec![proto::CategoryChannels {
                category: proto::PushCategory::Likes as i32,
                push:     false,
                email:    true,
            }],
            ..Default::default()
        }))
        .await
        .expect("update")
        .into_inner();
    let likes = prefs.categories.iter().find(|c| c.category == proto::PushCategory::Likes as i32).unwrap();
    assert!(!likes.push && likes.email);
    assert_eq!(prefs.timezone, "Europe/Paris", "set by the registration");
    assert_eq!(targets(&h, &profile, proto::PushCategory::Likes).await, (false, vec![]));
    assert!(targets(&h, &profile, proto::PushCategory::Comments).await.0);

    // The device gets a new token: only the new one is used.
    let fresh = id();
    h.handler.register_device(register(&profile, "phone", &fresh)).await.expect("re-register");
    assert_eq!(targets(&h, &profile, proto::PushCategory::Comments).await, (true, vec![fresh.clone()]));

    // A pause beyond 8 h is refused; a pause holds every push.
    let pause = |ms: i64| {
        Request::new(proto::UpdateNotificationPreferencesRequest {
            profile_id: profile.clone(),
            paused_until_ms: Some(ms),
            ..Default::default()
        })
    };
    let now = chrono::Utc::now().timestamp_millis();
    assert!(h.handler.update_notification_preferences(pause(now + 9 * 3_600_000)).await.is_err());
    h.handler.update_notification_preferences(pause(now + 3_600_000)).await.expect("pause");
    assert_eq!(targets(&h, &profile, proto::PushCategory::Comments).await, (false, vec![]));
    h.handler.update_notification_preferences(pause(0)).await.expect("resume");
    assert!(targets(&h, &profile, proto::PushCategory::Comments).await.0);

    // Unregistered: nowhere to send.
    h.handler
        .unregister_device(Request::new(proto::UnregisterDeviceRequest {
            profile_id: profile.clone(),
            device_id:  "phone".into(),
        }))
        .await
        .expect("unregister");
    assert_eq!(targets(&h, &profile, proto::PushCategory::Comments).await, (true, vec![]));
}

#[tokio::test]
async fn a_token_registered_for_another_account_leaves_the_previous_one() {
    let h = TestHarness::start().await;
    let (alice, bob, token) = (id(), id(), id());
    let as_account = |profile: &str, account: &str| RegisterDeviceCommand {
        profile_id:  profile.to_owned(),
        account_id:  account.to_owned(),
        device_id:   "shared-phone".into(),
        token:       token.clone(),
        platform:    DevicePlatform::Ios,
        environment: PushEnvironment::Production,
        timezone:    None,
        age:         HolderAge::Adult,
        caller:      RegistrationCaller::Mesh,
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), as_account(&alice, "acct-a"))).await.expect("alice");
    assert_eq!(targets(&h, &alice, proto::PushCategory::Messages).await.1, vec![token.clone()]);

    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), as_account(&bob, "acct-b"))).await.expect("bob");
    assert!(targets(&h, &alice, proto::PushCategory::Messages).await.1.is_empty(), "alice's pushes stop");
    assert_eq!(targets(&h, &bob, proto::PushCategory::Messages).await.1, vec![token]);
}

#[tokio::test]
async fn a_teens_first_registration_writes_quiet_hours() {
    let h = TestHarness::start().await;
    let teen = id();
    let cmd = RegisterDeviceCommand {
        profile_id:  teen.clone(),
        account_id:  "acct-teen".into(),
        device_id:   "phone".into(),
        token:       id(),
        platform:    DevicePlatform::Ios,
        environment: PushEnvironment::Sandbox,
        timezone:    Some("Europe/Paris".into()),
        age:         HolderAge::Teen,
        caller:      RegistrationCaller::Mesh,
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("register");

    // Read back over the mesh (no token): the stored default, not the adult one.
    let prefs = h
        .handler
        .get_notification_preferences(Request::new(proto::GetNotificationPreferencesRequest { profile_id: teen }))
        .await
        .expect("get")
        .into_inner();
    let quiet = prefs.quiet_hours.expect("quiet hours");
    assert!(quiet.enabled);
    assert_eq!((quiet.start_minute, quiet.end_minute), (22 * 60, 7 * 60));
}

#[tokio::test]
async fn teen_quiet_hours_lift_at_18_unless_the_holder_chose_them() {
    let h = TestHarness::start().await;
    let register = |profile: &str, age: HolderAge| RegisterDeviceCommand {
        profile_id:  profile.to_owned(),
        account_id:  "acct-teen".into(),
        device_id:   "phone".into(),
        token:       format!("token-{profile}"),
        platform:    DevicePlatform::Ios,
        environment: PushEnvironment::Production,
        timezone:    None,
        age,
        caller:      RegistrationCaller::Mesh,
    };
    let quiet = |profile: String| {
        let h = &h;
        async move {
            h.handler
                .get_notification_preferences(Request::new(proto::GetNotificationPreferencesRequest { profile_id: profile }))
                .await
                .expect("get")
                .into_inner()
                .quiet_hours
                .is_some_and(|q| q.enabled)
        }
    };

    // The default: a mesh refresh (age unknown) leaves it; the app's refresh
    // once the token says 18+ lifts it.
    let (defaulted, chosen) = (id(), id());
    for profile in [&defaulted, &chosen] {
        h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), register(profile, HolderAge::Teen))).await.expect("teen");
    }
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), register(&defaulted, HolderAge::Unknown))).await.expect("mesh");
    assert!(quiet(defaulted.clone()).await, "unknown age: unchanged");
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), register(&defaulted, HolderAge::Adult))).await.expect("adult");
    assert!(!quiet(defaulted).await, "lifted at 18");

    // Set by the teen themselves: kept at 18.
    let set = UpdatePreferencesCommand {
        profile_id:  chosen.clone(),
        age:         HolderAge::Teen,
        quiet_hours: Some(QuietHours::new(23 * 60, 6 * 60)),
        ..Default::default()
    };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), set)).await.expect("set");
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), register(&chosen, HolderAge::Adult))).await.expect("adult");
    assert!(quiet(chosen).await, "their own choice stays");
}

fn from_session(profile: &str, account: &str, device: &str, token: &str, session_device: Option<&str>) -> RegisterDeviceCommand {
    RegisterDeviceCommand {
        profile_id:   profile.to_owned(),
        account_id:   account.to_owned(),
        device_id:    device.to_owned(),
        token:        token.to_owned(),
        platform:     DevicePlatform::Ios,
        environment:  PushEnvironment::Production,
        timezone:     None,
        age:          HolderAge::Adult,
        caller:       RegistrationCaller::Edge { session_device: session_device.map(str::to_owned) },
    }
}

fn bound(profile: &str, account: &str, device: &str, token: &str) -> RegisterDeviceCommand {
    from_session(profile, account, device, token, Some(device))
}

/// #725: from a session bound to its device, a push token only moves between
/// accounts on the device it was registered from; another device's token is
/// refused (NTF-2004) and stays with its holder.
#[tokio::test]
async fn a_push_token_moves_between_accounts_only_on_its_own_device() {
    use error::AppError;

    let h = TestHarness::start().await;
    let (alice, bob, mallory, token) = (id(), id(), id(), id());
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), bound(&alice, "acct-a", "phone-1", &token))).await.expect("alice");

    // Another device claims alice's token: refused, alice keeps her pushes.
    let err = h
        .command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), bound(&mallory, "acct-m", "phone-9", &token)))
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), "NTF-2004");
    assert_eq!(targets(&h, &alice, proto::PushCategory::Messages).await.1, vec![token.clone()]);

    // A session bound to no device at all (its client skipped `did` at login)
    // is refused too: the holder decides, not the caller.
    let err = h
        .command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), from_session(&mallory, "acct-m", "phone-1", &token, None)))
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), "NTF-2004", "a did-less session cannot take a device-bound token");
    assert_eq!(targets(&h, &alice, proto::PushCategory::Messages).await.1, vec![token.clone()]);

    // Bob signs in on alice's phone: the token moves to bob.
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), bound(&bob, "acct-b", "phone-1", &token))).await.expect("same phone");
    assert!(targets(&h, &alice, proto::PushCategory::Messages).await.1.is_empty());
    assert_eq!(targets(&h, &bob, proto::PushCategory::Messages).await.1, vec![token]);
}

/// On the edge the device is the session's: a request for another device id
/// is refused before anything is stored.
#[tokio::test]
async fn the_edge_registers_only_the_sessions_device() {
    let h = TestHarness::start().await;
    let profile = id();
    let raw: auth_context::OidcClaims = serde_json::from_value(serde_json::json!({
        "sub": "acct-edge", "exp": 4_102_444_800_i64, "did": "ios-install-1", "pids": [profile.clone()]
    }))
    .unwrap();
    let principal = transport::grpc::edge::EdgePrincipal::new(std::sync::Arc::new(auth_context::CurrentPrincipal {
        user_id: auth_context::PrincipalId::new("acct-edge"),
        tenant_id: None,
        permissions: vec![],
        raw_claims: raw,
    }));
    let request = |device_id: &str| {
        let mut request = Request::new(proto::RegisterDeviceRequest {
            profile_id: profile.clone(),
            device_id: device_id.to_owned(),
            token: id(),
            platform: proto::DevicePlatform::Ios as i32,
            environment: proto::PushEnvironment::Production as i32,
            timezone: String::new(),
        });
        request.extensions_mut().insert(principal.clone());
        request
    };
    let status = h.handler.register_device(request("another-device")).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    h.handler.register_device(request("ios-install-1")).await.expect("its own device");
}


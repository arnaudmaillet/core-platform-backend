//! Guest sessions (guest mode, B1) against real Postgres + Redis: an
//! installation starts an anonymous session, gets a read-only edge token, and
//! refreshes it like a member — without any account being created or looked up.

use tonic::Code;

use crate::auth_it::harness::{random_user, Harness};

#[tokio::test]
async fn a_guest_session_starts_refreshes_and_introspects_read_only() {
    let h = Harness::start().await;
    let started = h.start_guest("install-guest-1").await.expect("start guest");
    let tokens = started.tokens.unwrap();

    // Recorded: the session row is a guest's, and the device is kept for the
    // once-per-device welcome gift.
    assert_eq!(h.session_kind(&tokens.session_id).await, "guest");
    assert_eq!(h.guest_device(&started.guest_id).await.as_deref(), Some("install-guest-1"));

    // The token is read-only.
    let seen = h.introspect(&tokens.access_token).await.expect("introspect");
    assert!(seen.active);
    assert_eq!(seen.account_id, started.guest_id);
    assert_eq!(seen.permissions, vec!["read:public".to_owned()]);

    // Refresh rotates it like a member's, and stays read-only.
    let refreshed = h
        .refresh_on(&tokens.refresh_token, "install-guest-1")
        .await
        .expect("guest refresh")
        .tokens
        .unwrap();
    assert_eq!(refreshed.session_id, tokens.session_id);
    let seen = h.introspect(&refreshed.access_token).await.unwrap();
    assert_eq!(seen.permissions, vec!["read:public".to_owned()]);
}

#[tokio::test]
async fn a_guest_session_needs_a_device_and_members_now_read_public_too() {
    let h = Harness::start().await;
    let err = h.start_guest("").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");

    let member = h.login(&random_user()).await.unwrap().tokens.unwrap();
    let seen = h.introspect(&member.access_token).await.unwrap();
    assert!(seen.permissions.contains(&"read:public".to_owned()), "{:?}", seen.permissions);
    assert!(seen.permissions.contains(&"posts:write".to_owned()));
}

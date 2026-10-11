//! Happy-path session lifecycle: login issues a verifiable, introspectable
//! session; a single logout blacklists it so its edge token goes inactive.

use tonic::Code;

use crate::auth_it::harness::{random_user, Harness};

#[tokio::test]
async fn login_introspect_then_logout_invalidates_the_token() {
    let h = Harness::start().await;
    let user = random_user();

    let login = h.login(&user).await.expect("login");
    let tokens = login.tokens.expect("token pair");
    assert_eq!(tokens.token_type, "Bearer");
    assert!(!tokens.access_token.is_empty());
    assert!(!tokens.refresh_token.is_empty());

    // The just-issued access token introspects as active.
    let view = h.introspect(&tokens.access_token).await.expect("introspect");
    assert!(view.active);
    assert_eq!(view.account_id, login.account_id);
    assert_eq!(view.session_id, tokens.session_id);

    // It is durably persisted.
    assert_eq!(h.count_active_sessions(&login.account_id).await, 1);
    assert_eq!(h.count_subject_links(&login.account_id).await, 1);

    // Logging out blacklists the session ⇒ the edge token is now inactive.
    let out = h.logout(&tokens.session_id).await.expect("logout");
    assert!(out.success);
    assert!(!h.introspect(&tokens.access_token).await.expect("introspect").active);
    assert_eq!(h.count_active_sessions(&login.account_id).await, 0);

    // Logout is idempotent.
    assert!(h.logout(&tokens.session_id).await.expect("idempotent logout").success);
}

#[tokio::test]
async fn second_login_same_user_reuses_the_account_link() {
    let h = Harness::start().await;
    let user = random_user();

    let first = h.login(&user).await.expect("first login");
    let second = h.login(&user).await.expect("second login");

    assert_eq!(first.account_id, second.account_id);
    assert_eq!(h.count_subject_links(&first.account_id).await, 1, "exactly one link");
    assert_eq!(h.count_active_sessions(&first.account_id).await, 2, "two sessions");
}

#[tokio::test]
async fn introspecting_garbage_is_inactive_not_an_error() {
    let h = Harness::start().await;
    let view = h.introspect("not-a-token").await.expect("introspect never errors on bad tokens");
    assert!(!view.active);
}

#[tokio::test]
async fn logout_unknown_session_is_not_found() {
    let h = Harness::start().await;
    let status = h.logout(&uuid::Uuid::now_v7().to_string()).await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
}

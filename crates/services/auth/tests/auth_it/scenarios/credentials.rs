//! The holder's credentials over real Postgres + Redis: a password change signs
//! the other devices out (their refresh dies) and keeps this one; the step-up
//! re-mints this session's token with a fresh `auth_time`.

use std::sync::Arc;

use base64::Engine;
use tonic::{Code, Request};

use auth::infrastructure::grpc::handler::proto;

use crate::auth_it::harness::{random_user, Harness};

/// The edge principal of an access token, as the edge layer would attach it.
fn as_holder<T>(message: T, account_id: &str, session_id: &str) -> Request<T> {
    let claims = serde_json::json!({ "sub": account_id, "sid": session_id, "exp": 4_102_444_800_i64 });
    let raw: auth_context::OidcClaims = serde_json::from_value(claims).unwrap();
    let mut request = Request::new(message);
    request.extensions_mut().insert(transport::grpc::edge::EdgePrincipal::new(Arc::new(
        auth_context::CurrentPrincipal {
            user_id: auth_context::PrincipalId::new(account_id),
            tenant_id: None,
            permissions: vec![],
            raw_claims: raw,
        },
    )));
    request
}

fn payload(token: &str) -> serde_json::Value {
    let part = token.split('.').nth(1).unwrap();
    serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(part).unwrap())
        .unwrap()
}

#[tokio::test]
async fn a_password_change_signs_the_other_devices_out_and_keeps_this_one() {
    let h = Harness::start().await;
    let user = random_user();
    let phone = h.login(&user).await.expect("login phone");
    let laptop = h.login(&user).await.expect("login laptop");
    let account = phone.account_id.clone();
    let phone_tokens = phone.tokens.unwrap();
    let laptop_tokens = laptop.tokens.unwrap();
    // A login is a fresh credential proof.
    assert!(payload(&phone_tokens.access_token)["auth_time"].is_i64());

    let out = h
        .handler
        .change_password(as_holder(
            proto::ChangePasswordRequest {
                current_password: "pw".into(),
                new_password: "a much longer password".into(),
                sign_out_other_sessions: true,
            },
            &account,
            &phone_tokens.session_id,
        ))
        .await
        .expect("change password")
        .into_inner();
    assert_eq!(out.sessions_revoked, 1);
    assert_eq!(
        h.credentials.set.lock().unwrap().as_slice(),
        &[(user.clone(), "a much longer password".to_owned())]
    );

    // The laptop is signed out; the phone carries on.
    let err = h.refresh(&laptop_tokens.refresh_token).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    let refreshed = h.refresh(&phone_tokens.refresh_token).await.expect("phone refresh").tokens.unwrap();
    assert!(payload(&refreshed.access_token).get("auth_time").is_none(), "a refresh proves nothing");

    // Step-up from the phone: a fresh token for the same session, auth_time = now.
    let stepped = h
        .handler
        .verify_credentials(as_holder(
            proto::VerifyCredentialsRequest {
                credential: Some(proto::verify_credentials_request::Credential::Password("pw".into())),
            },
            &account,
            &phone_tokens.session_id,
        ))
        .await
        .expect("step up")
        .into_inner();
    let claims = payload(&stepped.access_token);
    assert_eq!(claims["sid"], phone_tokens.session_id.as_str());
    assert!(claims["auth_time"].is_i64());
    assert!(stepped.step_up_expires_in > 0 && stepped.step_up_expires_in <= 300);

    // The signed-out laptop session cannot step up.
    let err = h
        .handler
        .verify_credentials(as_holder(
            proto::VerifyCredentialsRequest {
                credential: Some(proto::verify_credentials_request::Credential::Password("pw".into())),
            },
            &account,
            &laptop_tokens.session_id,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

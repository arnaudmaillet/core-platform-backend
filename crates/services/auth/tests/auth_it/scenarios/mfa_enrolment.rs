//! #649 over the real graph through the gRPC edge handlers: the holder turns
//! two-step sign-in on (behind a recent credential proof) with the first code
//! of a new seed, gets backup codes once, and their other device is signed
//! out; the next sign-in asks for a code; regenerating replaces the codes;
//! turning it off brings one-step sign-in back.

use std::sync::Arc;

use chrono::Utc;
use tonic::{Code, Request};

use auth::domain::value_object::{step_of, TotpSecret};
use auth::infrastructure::grpc::handler::proto;

use crate::auth_it::harness::{random_user, Harness};

/// The edge principal of the holder's token; `fresh` adds `auth_time = now`
/// (a credential just proven).
fn as_holder<T>(message: T, account_id: &str, session_id: &str, fresh: bool) -> Request<T> {
    let mut claims = serde_json::json!({ "sub": account_id, "sid": session_id, "exp": 4_102_444_800_i64 });
    if fresh {
        claims["auth_time"] = serde_json::json!(Utc::now().timestamp());
    }
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

#[tokio::test]
async fn two_step_sign_in_is_turned_on_regenerated_and_off_from_the_settings() {
    let h = Harness::start().await;
    let user = random_user();
    let here = h.login(&user).await.unwrap();
    let elsewhere = h.login(&user).await.unwrap();
    let (account, session) = (here.account_id.clone(), here.tokens.unwrap().session_id);
    let other_refresh = elsewhere.tokens.unwrap().refresh_token;

    // Starting needs a recent credential proof.
    let stale = h
        .handler
        .start_mfa_enrollment(as_holder(proto::StartMfaEnrollmentRequest {}, &account, &session, false))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::PermissionDenied, "{stale:?}");
    let started = h
        .handler
        .start_mfa_enrollment(as_holder(proto::StartMfaEnrollmentRequest {}, &account, &session, true))
        .await
        .unwrap()
        .into_inner();
    assert!(started.otpauth_uri.starts_with("otpauth://totp/Core%20Platform:") && started.expires_in > 0);
    let seed = TotpSecret::from_bytes(data_encoding::BASE32_NOPAD.decode(started.secret.as_bytes()).unwrap()).unwrap();

    // The first code turns it on: backup codes once, the other device out.
    let confirm = |code: String| {
        as_holder(proto::ConfirmMfaEnrollmentRequest { code }, &account, &session, false)
    };
    let codes = h.handler.confirm_mfa_enrollment(confirm(seed.code_at(step_of(Utc::now())))).await.unwrap().into_inner();
    assert_eq!(codes.backup_codes.len(), 10);
    assert_eq!(codes.sessions_revoked, 1);
    assert!(h.refresh(&other_refresh).await.is_err(), "the other device is signed out");
    let again = h.handler.confirm_mfa_enrollment(confirm("123456".into())).await.unwrap_err();
    assert_eq!(again.code(), Code::Unauthenticated, "no enrolment waits anymore: {again:?}");

    // Signing in now takes a code: a backup code from the set.
    let challenge = h.login(&user).await.unwrap();
    assert!(challenge.mfa_required);
    h.handler
        .complete_login(Request::new(proto::CompleteLoginRequest {
            mfa_token: challenge.mfa_token,
            code: codes.backup_codes[0].clone(),
            passkey: None,
        }))
        .await
        .expect("a backup code from the settings");

    // Regenerating replaces them (behind a step-up too).
    let fresh = h
        .handler
        .regenerate_backup_codes(as_holder(proto::RegenerateBackupCodesRequest {}, &account, &session, true))
        .await
        .unwrap()
        .into_inner();
    let challenge = h.login(&user).await.unwrap();
    let old = h
        .handler
        .complete_login(Request::new(proto::CompleteLoginRequest {
            mfa_token: challenge.mfa_token.clone(),
            code: codes.backup_codes[1].clone(),
            passkey: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(old.code(), Code::Unauthenticated, "the old set is gone");
    h.handler
        .complete_login(Request::new(proto::CompleteLoginRequest {
            mfa_token: challenge.mfa_token,
            code: fresh.backup_codes[0].clone(),
            passkey: None,
        }))
        .await
        .expect("the new set");

    // Turning it off: one-step sign-in again.
    let stale = h.handler.disable_mfa(as_holder(proto::DisableMfaRequest {}, &account, &session, false)).await;
    assert_eq!(stale.unwrap_err().code(), Code::PermissionDenied);
    h.handler.disable_mfa(as_holder(proto::DisableMfaRequest {}, &account, &session, true)).await.unwrap();
    let plain = h.login(&user).await.unwrap();
    assert!(!plain.mfa_required && plain.tokens.is_some());
}

//! #808 over the real graph through the gRPC edge handlers: the holder
//! registers a passkey (behind a recent credential proof) with a software
//! authenticator, lists it (kept in Postgres), cannot redeem the challenge
//! twice, and removes it (behind a step-up too).

use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use tonic::{Code, Request};
use uuid::Uuid;

use auth::domain::value_object::webauthn::testing::{SoftAuthenticator, PASSKEY};
use auth::infrastructure::grpc::handler::proto;

use crate::auth_it::harness::{random_user, Harness, PASSKEY_RP_ID};

/// The edge principal of the holder's token; `fresh` adds `auth_time = now`.
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
async fn a_passkey_is_registered_listed_and_removed() {
    let h = Harness::start().await;
    let login = h.login(&random_user()).await.unwrap();
    let (account, session) = (login.account_id.clone(), login.tokens.unwrap().session_id);
    let origin = format!("https://{PASSKEY_RP_ID}");
    let auth = SoftAuthenticator::new();

    // Starting needs a recent credential proof.
    let stale = h
        .handler
        .start_passkey_registration(as_holder(proto::StartPasskeyRegistrationRequest {}, &account, &session, false))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::PermissionDenied, "{stale:?}");
    let options = h
        .handler
        .start_passkey_registration(as_holder(proto::StartPasskeyRegistrationRequest {}, &account, &session, true))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(options.rp_id, PASSKEY_RP_ID);
    assert_eq!(URL_SAFE_NO_PAD.decode(&options.user_id).unwrap(), Uuid::parse_str(&account).unwrap().as_bytes());
    let challenge = URL_SAFE_NO_PAD.decode(&options.challenge).unwrap();

    let finish = proto::FinishPasskeyRegistrationRequest {
        challenge:          options.challenge.clone(),
        client_data_json:   SoftAuthenticator::client_data("webauthn.create", &challenge, &origin),
        attestation_object: auth.attestation(PASSKEY_RP_ID, PASSKEY),
        name:               "iPhone".into(),
    };
    let made = h
        .handler
        .finish_passkey_registration(as_holder(finish.clone(), &account, &session, false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(made.name, "iPhone");
    assert!(made.synced && made.created_at.is_some() && made.last_used_at.is_none());

    // Single use.
    let replay = h.handler.finish_passkey_registration(as_holder(finish, &account, &session, false)).await.unwrap_err();
    assert_eq!(replay.code(), Code::Unauthenticated, "{replay:?}");

    let listed = h
        .handler
        .list_passkeys(as_holder(proto::ListPasskeysRequest {}, &account, &session, false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.passkeys, vec![made.clone()]);
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM passkeys WHERE account_id = $1")
        .bind(Uuid::parse_str(&account).unwrap())
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(stored, 1);

    // Removing needs a recent proof too.
    let remove = proto::RemovePasskeyRequest { credential_id: made.credential_id.clone() };
    let stale = h.handler.remove_passkey(as_holder(remove.clone(), &account, &session, false)).await.unwrap_err();
    assert_eq!(stale.code(), Code::PermissionDenied, "{stale:?}");
    let left = h.handler.remove_passkey(as_holder(remove.clone(), &account, &session, true)).await.unwrap().into_inner();
    assert!(left.passkeys.is_empty());
    let gone = h.handler.remove_passkey(as_holder(remove, &account, &session, true)).await.unwrap_err();
    assert_eq!(gone.code(), Code::NotFound, "{gone:?}");
}

//! #667 over the real graph through the gRPC handlers: the (future) OAuth
//! server records a grant on the mesh; the holder lists it and withdraws it
//! on the edge; a withdrawn grant can no longer be used until consented to
//! again. (Their consent events are unit-tested: the harness logs events.)

use std::sync::Arc;

use tonic::{Code, Request};

use auth::infrastructure::grpc::handler::proto;

use crate::auth_it::harness::{random_user, Harness};

/// The edge principal of the holder's token.
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

fn record(account: &str, scopes: &[&str]) -> Request<proto::RecordAppAuthorizationRequest> {
    Request::new(proto::RecordAppAuthorizationRequest {
        account_id:   account.to_owned(),
        app_id:       "partner.app".into(),
        display_name: "Partner".into(),
        icon_url:     "https://partner.example/icon.png".into(),
        scopes:       scopes.iter().map(|s| (*s).to_owned()).collect(),
    })
}

fn note_use(account: &str) -> Request<proto::NoteAppUseRequest> {
    Request::new(proto::NoteAppUseRequest { account_id: account.to_owned(), app_id: "partner.app".into() })
}

#[tokio::test]
async fn an_app_is_authorised_listed_used_and_withdrawn() {
    let h = Harness::start().await;
    let login = h.login(&random_user()).await.unwrap();
    let (account, session) = (login.account_id.clone(), login.tokens.unwrap().session_id);
    let list = || as_holder(proto::ListAuthorizedAppsRequest {}, &account, &session);
    assert!(h.handler.list_authorized_apps(list()).await.unwrap().into_inner().apps.is_empty());

    let granted = h.handler.record_app_authorization(record(&account, &["profile:read", "email"])).await.unwrap().into_inner();
    assert_eq!(granted.scopes, vec!["email", "profile:read"]);
    // The same consent again: unchanged.
    let again = h.handler.record_app_authorization(record(&account, &["email", "profile:read"])).await.unwrap().into_inner();
    assert_eq!(again.granted_at, granted.granted_at);

    h.handler.note_app_use(note_use(&account)).await.unwrap();
    let apps = h.handler.list_authorized_apps(list()).await.unwrap().into_inner().apps;
    assert_eq!((apps.len(), apps[0].app_id.as_str(), apps[0].display_name.as_str()), (1, "partner.app", "Partner"));
    assert!(apps[0].last_used_at.is_some());

    let revoke = || as_holder(proto::RevokeAppAuthorizationRequest { app_id: "partner.app".into() }, &account, &session);
    assert!(h.handler.revoke_app_authorization(revoke()).await.unwrap().into_inner().apps.is_empty());
    h.handler.revoke_app_authorization(revoke()).await.expect("idempotent");
    let unknown = as_holder(proto::RevokeAppAuthorizationRequest { app_id: "never.app".into() }, &account, &session);
    assert_eq!(h.handler.revoke_app_authorization(unknown).await.unwrap_err().code(), Code::NotFound);

    // Withdrawn: its tokens must be refused.
    assert_eq!(h.handler.note_app_use(note_use(&account)).await.unwrap_err().code(), Code::NotFound);
    // Consented to again: a new grant.
    let renewed = h.handler.record_app_authorization(record(&account, &["email"])).await.unwrap().into_inner();
    assert!(renewed.granted_at.unwrap().seconds >= granted.granted_at.unwrap().seconds);
    h.handler.note_app_use(note_use(&account)).await.unwrap();

    // Listing needs the holder's own live session.
    let stranger = as_holder(proto::ListAuthorizedAppsRequest {}, &account, &uuid::Uuid::now_v7().to_string());
    assert!(h.handler.list_authorized_apps(stranger).await.is_err());
    assert_eq!(h.handler.list_authorized_apps(Request::new(proto::ListAuthorizedAppsRequest {})).await.unwrap_err().code(), Code::Unauthenticated);
}

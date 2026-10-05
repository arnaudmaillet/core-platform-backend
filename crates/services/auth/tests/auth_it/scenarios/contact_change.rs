//! #651 over real Postgres + Redis: a holder changes their email with a code
//! sent to the new address; the IdP user's email follows, and the passwordless
//! code link is re-keyed in `subject_links` (the old address no longer signs
//! in). On the edge, the RPC needs a recent credential proof.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use cqrs::Envelope;
use tonic::{Code, Request};
use uuid::Uuid;

use auth::application::command::{
    ChangeContactCommand, ChangeContactHandler, StartVerificationCommand, VerificationCodes, VerificationPolicy,
};
use auth::application::port::{CodeSender, SubjectLinkRepository, VerificationChannel};
use auth::domain::aggregate::SubjectLink;
use auth::domain::value_object::{AccountId, IdpSubject, EMAIL_CODE_ISSUER};
use auth::error::AuthError;
use auth::infrastructure::cache::{RedisSessionCache, RedisVerificationStore};
use auth::infrastructure::event::LogEventPublisher;
use auth::infrastructure::grpc::handler::proto;
use auth::infrastructure::persistence::{PgSessionRepository, PgSubjectLinkRepository};
use postgres_storage::TransactionManager;

use crate::auth_it::harness::{random_user, Harness};

/// Keeps every code and every contact-changed notice.
#[derive(Default)]
struct Outbox {
    codes:   Mutex<Vec<String>>,
    notices: Mutex<Vec<String>>,
}

#[async_trait]
impl CodeSender for Outbox {
    async fn send(&self, _: VerificationChannel, _: &str, code: &str, _: Option<&str>) -> Result<(), AuthError> {
        self.codes.lock().unwrap().push(code.to_owned());
        Ok(())
    }

    async fn send_contact_changed_notice(&self, _: VerificationChannel, email: &str, _: Option<&str>) -> Result<(), AuthError> {
        self.notices.lock().unwrap().push(email.to_owned());
        Ok(())
    }
}

#[tokio::test]
async fn a_changed_email_moves_the_idp_user_and_re_keys_the_code_link() {
    let h = Harness::start().await;
    let user = random_user();
    let session = h.login(&user).await.expect("login");
    let account = AccountId::try_from(session.account_id.as_str()).unwrap();
    let session_id = session.tokens.unwrap().session_id;

    let tx = TransactionManager::new(h.pool.clone());
    let links = Arc::new(PgSubjectLinkRepository::new(tx.clone()));
    let old_link = IdpSubject::new(EMAIL_CODE_ISSUER, format!("{user}@old.example")).unwrap();
    links.save(&SubjectLink::establish(old_link.clone(), account, Utc::now(), Uuid::now_v7())).await.unwrap();
    h.directory.contacts.lock().unwrap().insert(
        account,
        auth::application::port::ContactDetails { email: Some(format!("{user}@old.example")), phone: None },
    );

    let outbox = Arc::new(Outbox::default());
    let codes = Arc::new(VerificationCodes::new(
        Arc::new(RedisVerificationStore::new(h.redis.clone())),
        Arc::clone(&outbox) as _,
        VerificationPolicy::default(),
    ));
    let handler = ChangeContactHandler::new(
        Arc::clone(&codes),
        Arc::clone(&h.directory) as _,
        Arc::clone(&links) as _,
        Arc::clone(&h.credentials) as _,
        Arc::new(PgSessionRepository::new(tx)) as _,
        Arc::new(RedisSessionCache::new(h.redis.clone())) as _,
        Arc::new(LogEventPublisher) as _,
    );

    let new_email = format!("{user}@new.example");
    let started = codes
        .start(StartVerificationCommand {
            channel: VerificationChannel::Email,
            destination: new_email.clone(),
            locale: None,
            client_ip: None,
        })
        .await
        .unwrap();
    let code = outbox.codes.lock().unwrap().last().cloned().unwrap();
    let cmd = ChangeContactCommand {
        account_id: account.as_str(),
        session_id,
        challenge_id: started.challenge_id,
        code,
        client_ip: None,
        locale: None,
    };
    handler.handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now()).await.expect("change email");

    // The password account's IdP user follows (the login's subject is the user).
    assert_eq!(h.credentials.emails.lock().unwrap().clone(), vec![(user.clone(), new_email.clone())]);
    // Postgres: codes to the new address sign in; the old address no longer does.
    let new_link = IdpSubject::new(EMAIL_CODE_ISSUER, new_email.clone()).unwrap();
    assert_eq!(links.find_by_subject(&new_link).await.unwrap().map(|l| l.account_id()), Some(account));
    assert!(links.find_by_subject(&old_link).await.unwrap().is_none());
    // The old address is told.
    assert_eq!(outbox.notices.lock().unwrap().clone(), vec![format!("{user}@old.example")]);
}

/// The edge route: an account-takeover vector, so without a recent credential
/// proof it is refused before the code is even looked at.
#[tokio::test]
async fn the_edge_needs_a_recent_credential_proof() {
    let h = Harness::start().await;
    let user = random_user();
    let session = h.login(&user).await.expect("login");
    let tokens = session.tokens.unwrap();
    let call = |auth_time: Option<i64>| {
        let mut claims = serde_json::json!({ "sub": session.account_id, "sid": tokens.session_id, "exp": 4_102_444_800_i64 });
        if let Some(at) = auth_time {
            claims["auth_time"] = serde_json::json!(at);
        }
        let raw: auth_context::OidcClaims = serde_json::from_value(claims).unwrap();
        let mut request = Request::new(proto::ChangeContactRequest {
            challenge_id: "nope".into(),
            code: "000000".into(),
            locale: String::new(),
        });
        request.extensions_mut().insert(transport::grpc::edge::EdgePrincipal::new(Arc::new(
            auth_context::CurrentPrincipal {
                user_id: auth_context::PrincipalId::new(session.account_id.as_str()),
                tenant_id: None,
                permissions: vec![],
                raw_claims: raw,
            },
        )));
        request
    };

    let stale = h.handler.change_contact(call(None)).await.unwrap_err();
    assert!(!stale.message().contains("verification code"), "refused before the code: {stale:?}");
    // With a fresh proof the code itself is judged (a wrong one here: AUT-5011).
    let fresh = h.handler.change_contact(call(Some(Utc::now().timestamp()))).await.unwrap_err();
    assert_eq!(fresh.code(), Code::Unauthenticated, "{fresh:?}");
    assert!(fresh.message().contains("verification code"), "{fresh:?}");
}

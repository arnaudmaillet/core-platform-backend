//! #649 over real Postgres: what an account's sessions — any status — say of a
//! device, read before a sign-in to tell a new device apart.

use tonic::Request;

use auth::application::port::SessionRepository;
use auth::domain::value_object::AccountId;
use auth::infrastructure::grpc::handler::proto;
use auth::infrastructure::persistence::PgSessionRepository;
use postgres_storage::TransactionManager;

use crate::auth_it::harness::{random_user, Harness};

async fn login_from(h: &Harness, user: &str, device_id: &str) -> String {
    let request = Request::new(proto::LoginRequest {
        device: Some(proto::DeviceContext {
            user_agent: "App/1.0".into(),
            ip_address: String::new(),
            device_id: device_id.into(),
        }),
        grant_type: proto::GrantType::Password as i32,
        credential: Some(proto::login_request::Credential::Password(proto::PasswordGrant {
            username: user.to_owned(),
            password: "pw".to_owned(),
        })),
        guest_refresh_token: String::new(),
    });
    h.handler.login(request).await.expect("login").into_inner().account_id
}

#[tokio::test]
async fn an_accounts_sessions_tell_a_known_device_from_a_new_one() {
    let h = Harness::start().await;
    let sessions = PgSessionRepository::new(TransactionManager::new(h.pool.clone()));
    let user = random_user();
    let account = AccountId::try_from(login_from(&h, &user, "phone").await.as_str()).unwrap();

    let phone = sessions.device_history(&account, "phone").await.unwrap();
    assert!(phone.any_session && phone.seen_device && !phone.is_new_device());
    let laptop = sessions.device_history(&account, "laptop").await.unwrap();
    assert!(laptop.is_new_device(), "{laptop:?}");

    // Another account's history is its own.
    let stranger = AccountId::try_from(login_from(&h, &random_user(), "laptop").await.as_str()).unwrap();
    assert!(sessions.device_history(&stranger, "phone").await.unwrap().is_new_device());
    let nobody = AccountId::from_uuid(uuid::Uuid::now_v7());
    let empty = sessions.device_history(&nobody, "phone").await.unwrap();
    assert!(!empty.any_session && !empty.is_new_device(), "a first sign-in is not announced");
}

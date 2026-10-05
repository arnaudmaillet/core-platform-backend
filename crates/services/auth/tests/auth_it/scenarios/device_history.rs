//! #649 over real Postgres: what an account's sessions — any status — say of a
//! device, read before a sign-in to tell a new device apart.

use tonic::Request;

use auth::application::port::{RECENT_SESSIONS, SessionRepository};
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

    let phone = sessions.device_history(&account, Some("phone")).await.unwrap();
    assert!(phone.any_session && phone.seen_device && !phone.announces(true));
    let laptop = sessions.device_history(&account, Some("laptop")).await.unwrap();
    assert!(laptop.announces(true), "{laptop:?}");

    // Another account's history is its own.
    let stranger = AccountId::try_from(login_from(&h, &random_user(), "laptop").await.as_str()).unwrap();
    assert!(sessions.device_history(&stranger, Some("phone")).await.unwrap().announces(true));
    let nobody = AccountId::from_uuid(uuid::Uuid::now_v7());
    let empty = sessions.device_history(&nobody, Some("phone")).await.unwrap();
    assert!(!empty.any_session && !empty.announces(true), "a first sign-in is not announced");
    assert!(!sessions.device_history(&nobody, None).await.unwrap().announces(false));
}

/// The device id is client-written: a sign-in that leaves it out is new to an
/// account whose own client sends one — only an account whose latest sessions
/// all came without one is spared the noise.
#[tokio::test]
async fn a_sign_in_without_a_device_id_is_new_unless_the_holders_client_sends_none() {
    let h = Harness::start().await;
    let sessions = PgSessionRepository::new(TransactionManager::new(h.pool.clone()));
    let user = random_user();
    let account = AccountId::try_from(login_from(&h, &user, "phone").await.as_str()).unwrap();

    let deviceless = sessions.device_history(&account, None).await.unwrap();
    assert!(!deviceless.seen_device && !deviceless.recent_without_device_id);
    assert!(deviceless.announces(false), "leaving the id out does not silence the alert");

    // An old client that never sends one: once its latest sessions all lack
    // an id, another such sign-in is the holder's own.
    for _ in 0..RECENT_SESSIONS {
        login_from(&h, &user, "").await;
    }
    let old_client = sessions.device_history(&account, None).await.unwrap();
    assert!(old_client.recent_without_device_id && !old_client.announces(false), "{old_client:?}");
    // A sign-in that does carry an id is still compared with the devices seen.
    assert!(sessions.device_history(&account, Some("laptop")).await.unwrap().announces(true));
    assert!(!sessions.device_history(&account, Some("phone")).await.unwrap().announces(true));

    // The holder's client starts sending one: a deviceless sign-in is new again.
    login_from(&h, &user, "phone").await;
    assert!(sessions.device_history(&account, None).await.unwrap().announces(false));
}

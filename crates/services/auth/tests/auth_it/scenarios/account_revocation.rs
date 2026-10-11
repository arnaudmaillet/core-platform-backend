//! #884 against real Postgres + Redis: a suspension or a revoked role, as the
//! account-event consumer hands them to `AccountRevocation`, signs the account
//! out everywhere — every token minted before is inactive, no session is left
//! and the refresh tokens are dead. A suspension `account` no longer reports
//! (the account is active again) changes nothing.

use std::sync::Arc;

use chrono::Utc;
use postgres_storage::TransactionManager;
use uuid::Uuid;

use auth::application::command::{AccountChange, AccountRevocation, LogoutAllSessionsHandler};
use auth::application::SessionPolicy;
use auth::domain::value_object::AccountId;
use auth::infrastructure::cache::RedisSessionCache;
use auth::infrastructure::event::LogEventPublisher;
use auth::infrastructure::persistence::{PgRefreshTokenRepository, PgSessionRepository};

use crate::auth_it::harness::{random_user, Harness};

fn revocation(h: &Harness) -> AccountRevocation {
    let tx = TransactionManager::new(h.pool.clone());
    let minutes = chrono::Duration::minutes;
    AccountRevocation::new(
        Arc::new(LogoutAllSessionsHandler::new(
            Arc::new(PgSessionRepository::new(tx.clone())),
            Arc::new(PgRefreshTokenRepository::new(tx)),
            Arc::new(RedisSessionCache::new(h.redis.clone())),
            Arc::new(LogEventPublisher),
            SessionPolicy::new(minutes(10), minutes(30), chrono::Duration::hours(8), chrono::Duration::days(7)),
        )),
        h.directory.clone(),
    )
}

#[tokio::test]
async fn a_suspension_or_a_revoked_role_signs_the_account_out_everywhere() {
    let h = Harness::start().await;
    let revocation = revocation(&h);

    for change in [AccountChange::Suspended, AccountChange::RoleRevoked] {
        let user = random_user();
        let a = h.login(&user).await.expect("login a");
        let b = h.login(&user).await.expect("login b");
        let account = a.account_id.clone();
        let (a, b) = (a.tokens.unwrap(), b.tokens.unwrap());
        let account_id = AccountId::from_uuid(Uuid::parse_str(&account).unwrap());
        if change == AccountChange::Suspended {
            h.directory.suspended.lock().unwrap().push(account_id);
        }

        let out = revocation.apply(&account_id, change, Uuid::now_v7(), Utc::now()).await.unwrap();

        assert_eq!(out.expect("acted on").sessions_revoked, 2, "{change:?}");
        assert!(!h.introspect(&a.access_token).await.unwrap().active, "{change:?}");
        assert!(!h.introspect(&b.access_token).await.unwrap().active, "{change:?}");
        assert_eq!(h.count_active_sessions(&account).await, 0, "{change:?}");
        assert!(h.refresh(&a.refresh_token).await.is_err(), "{change:?}: the refresh token died too");
    }
}

#[tokio::test]
async fn a_suspension_lifted_since_keeps_the_sessions() {
    let h = Harness::start().await;
    let a = h.login(&random_user()).await.expect("login");
    let account = a.account_id.clone();
    let tokens = a.tokens.unwrap();
    let account_id = AccountId::from_uuid(Uuid::parse_str(&account).unwrap());

    let out = revocation(&h).apply(&account_id, AccountChange::Suspended, Uuid::now_v7(), Utc::now()).await.unwrap();

    assert!(out.is_none(), "the account is active again");
    assert!(h.introspect(&tokens.access_token).await.unwrap().active);
    assert_eq!(h.count_active_sessions(&account).await, 1);
}

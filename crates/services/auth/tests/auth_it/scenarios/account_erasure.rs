//! GDPR erasure against real Postgres + Redis: `account_deleted` makes auth delete
//! the account's sessions, refresh tokens, passkeys and identity links, and the guest it
//! was before signing up (with that guest's sessions). Other accounts are untouched
//! and a replay erases nothing more.

use std::sync::Arc;

use postgres_storage::TransactionManager;
use uuid::Uuid;

use auth::application::command::AccountErasure;
use auth::application::port::ErasedAccount;
use auth::domain::value_object::AccountId;
use auth::infrastructure::cache::RedisSessionCache;
use auth::infrastructure::persistence::{PgAccountEraser, PgSubjectLinkRepository};

use crate::auth_it::harness::{random_user, Harness};

async fn count(h: &Harness, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(id).fetch_one(&h.pool).await.unwrap()
}

#[tokio::test]
async fn an_erased_account_leaves_nothing_in_auth() {
    let h = Harness::start().await;

    // The member, signed in (session + refresh token + identity link), and the
    // guest it was before signing up on its device.
    let tokens = h.login(&random_user()).await.expect("login").tokens.unwrap();
    let member = h.introspect(&tokens.access_token).await.unwrap().account_id;
    let member = Uuid::parse_str(&member).unwrap();
    let guest = h.start_guest("it-erasure-device").await.expect("guest");
    let guest_id = Uuid::parse_str(&guest.guest_id).unwrap();
    sqlx::query("UPDATE guest_principals SET upgraded_to_account_id = $2, upgraded_at = now() WHERE guest_id = $1")
        .bind(guest_id)
        .bind(member)
        .execute(&h.pool)
        .await
        .unwrap();
    // A passkey (#808).
    sqlx::query(
        "INSERT INTO passkeys (account_id, credential_id, public_key, name, aaguid, backup_eligible, backed_up, \
         created_at) VALUES ($1, $2, $3, 'iPhone', $4, true, true, now())",
    )
    .bind(member)
    .bind(vec![1u8, 2, 3])
    .bind(vec![4u8; 65])
    .bind(Uuid::nil())
    .execute(&h.pool)
    .await
    .unwrap();
    // A third-party app authorisation (#667).
    sqlx::query(
        "INSERT INTO app_authorizations (account_id, app_id, display_name, scopes, granted_at) \
         VALUES ($1, 'partner.app', 'Partner', ARRAY['email'], now())",
    )
    .bind(member)
    .execute(&h.pool)
    .await
    .unwrap();
    // Someone else, signed in too.
    let other = h.login(&random_user()).await.expect("login").tokens.unwrap();
    let other = Uuid::parse_str(&h.introspect(&other.access_token).await.unwrap().account_id).unwrap();

    let tx = TransactionManager::new(h.pool.clone());
    let erasure = AccountErasure::new(
        Arc::new(PgAccountEraser::new(tx.clone())),
        Arc::new(RedisSessionCache::new(h.redis.clone())),
        Arc::new(PgSubjectLinkRepository::new(tx)),
        h.credentials.clone(),
    );
    let erased = erasure.erase(&AccountId::from_uuid(member)).await.unwrap();
    assert_eq!(erased, ErasedAccount { sessions: 2, links: 1, guests: 1 });
    assert_eq!(h.credentials.deleted.lock().unwrap().len(), 1, "the IdP user is deleted too");

    for (sql, id) in [
        ("SELECT COUNT(*) FROM sessions WHERE account_id = $1", member),
        ("SELECT COUNT(*) FROM refresh_tokens WHERE account_id = $1", member),
        ("SELECT COUNT(*) FROM subject_links WHERE account_id = $1", member),
        ("SELECT COUNT(*) FROM passkeys WHERE account_id = $1", member),
        ("SELECT COUNT(*) FROM app_authorizations WHERE account_id = $1", member),
        ("SELECT COUNT(*) FROM guest_principals WHERE guest_id = $1", guest_id),
        ("SELECT COUNT(*) FROM sessions WHERE account_id = $1", guest_id),
        ("SELECT COUNT(*) FROM refresh_tokens WHERE account_id = $1", guest_id),
    ] {
        assert_eq!(count(&h, sql, id).await, 0, "{sql}");
    }
    // The member's tokens no longer work; the other account is untouched.
    assert!(h.refresh(&tokens.refresh_token).await.is_err());
    assert_eq!(count(&h, "SELECT COUNT(*) FROM subject_links WHERE account_id = $1", other).await, 1);
    assert_eq!(count(&h, "SELECT COUNT(*) FROM sessions WHERE account_id = $1", other).await, 1);

    // A replayed account_deleted erases nothing more.
    let again = erasure.erase(&AccountId::from_uuid(member)).await.unwrap();
    assert_eq!(again, ErasedAccount::default());
}

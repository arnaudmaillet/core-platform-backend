//! Guest data retention against real Postgres: guests that never became an
//! account and have no live session go after the retention, with the guest
//! sessions (and refresh tokens) that ended by then; live, recent and upgraded
//! guests stay.

use std::sync::Arc;

use chrono::{Duration, Utc};
use postgres_storage::TransactionManager;
use uuid::Uuid;

use auth::application::command::GuestRetention;
use auth::infrastructure::persistence::PgGuestRegistry;

use crate::auth_it::harness::Harness;

async fn guest_exists(h: &Harness, guest_id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM guest_principals WHERE guest_id = $1")
        .bind(guest_id)
        .fetch_one(&h.pool)
        .await
        .unwrap()
        == 1
}

async fn insert_guest(h: &Harness, days_ago: i64, upgraded: bool) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO guest_principals (guest_id, device_id, first_seen_at, upgraded_to_account_id, upgraded_at)
         VALUES ($1, 'it-device', $2, $3, $4)",
    )
    .bind(id)
    .bind(Utc::now() - Duration::days(days_ago))
    .bind(upgraded.then(Uuid::now_v7))
    .bind(upgraded.then(Utc::now))
    .execute(&h.pool)
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn stale_guests_and_ended_guest_sessions_are_deleted_live_and_upgraded_ones_stay() {
    let h = Harness::start().await;

    // A guest browsing now (live session), first seen long ago.
    let live = h.start_guest("it-retention-live").await.expect("start guest");
    let live_id = Uuid::parse_str(&live.guest_id).unwrap();
    sqlx::query("UPDATE guest_principals SET first_seen_at = $2 WHERE guest_id = $1")
        .bind(live_id)
        .bind(Utc::now() - Duration::days(120))
        .execute(&h.pool)
        .await
        .unwrap();

    // A guest whose session ended long ago (its row and refresh token too).
    let ended = h.start_guest("it-retention-ended").await.expect("start guest");
    let ended_id = Uuid::parse_str(&ended.guest_id).unwrap();
    let ended_session = Uuid::parse_str(&ended.tokens.unwrap().session_id).unwrap();
    sqlx::query("UPDATE guest_principals SET first_seen_at = $2 WHERE guest_id = $1")
        .bind(ended_id)
        .bind(Utc::now() - Duration::days(120))
        .execute(&h.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sessions SET issued_at = $2, expires_at = $2, absolute_expiry = $2 WHERE id = $1")
        .bind(ended_session)
        .bind(Utc::now() - Duration::days(100))
        .execute(&h.pool)
        .await
        .unwrap();

    let stale = insert_guest(&h, 100, false).await;
    let recent = insert_guest(&h, 10, false).await;
    let upgraded = insert_guest(&h, 200, true).await;

    let retention = GuestRetention::new(
        Arc::new(PgGuestRegistry::new(TransactionManager::new(h.pool.clone()))),
        Duration::days(90),
    );
    assert!(retention.run_once(Utc::now()).await.unwrap() >= 2);

    assert!(!guest_exists(&h, stale).await, "a stale guest goes");
    assert!(!guest_exists(&h, ended_id).await, "a guest whose session ended long ago goes");
    assert!(guest_exists(&h, live_id).await, "a guest with a live session stays");
    assert!(guest_exists(&h, recent).await, "a recent guest stays");
    assert!(guest_exists(&h, upgraded).await, "a guest that became an account stays");

    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions WHERE id = $1")
        .bind(ended_session)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    let tokens: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM refresh_tokens WHERE session_id = $1")
        .bind(ended_session)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!((sessions, tokens), (0, 0), "the ended guest session and its refresh token are gone");
}

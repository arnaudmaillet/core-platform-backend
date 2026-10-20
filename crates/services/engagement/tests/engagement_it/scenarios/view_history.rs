//! #944 over live Scylla: a profile's viewing history — most recently viewed
//! first, each post once (a re-view moves it to the top, a quick re-view
//! changes nothing), one entry removed, all cleared, listed only where the
//! authority confirms it — exported and forgotten with the profile or the
//! account.

use std::sync::Arc;

use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use uuid::Uuid;

use engagement::application::port::{ViewCursor, ViewHistory};
use engagement::infrastructure::persistence::ScyllaViewHistory;

async fn scylla() -> Arc<ScyllaClient> {
    let contact = test_support::containers::scylla_ready("engagement", concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).await;
    Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    )
}

fn ids() -> String {
    Uuid::now_v7().to_string()
}

/// µs, a minute apart (past the re-view debounce).
fn at(minute: i64) -> i64 {
    1_700_000_000_000_000 + minute * 60_000_000
}

async fn tab(history: &ScyllaViewHistory, profile: &str) -> Vec<String> {
    history.list(profile, 100, None).await.unwrap().0.into_iter().map(|v| v.post_id).collect()
}

#[tokio::test]
async fn a_history_lists_each_post_once_newest_first_and_a_review_moves_it_up() {
    let history = ScyllaViewHistory::new(scylla().await);
    let (account, profile) = (ids(), ids());
    let posts: Vec<String> = (0..3).map(|_| ids()).collect();
    for (i, post) in posts.iter().enumerate() {
        history.record(&account, &profile, post, at(i as i64 * 2)).await.unwrap();
    }
    assert_eq!(tab(&history, &profile).await, vec![posts[2].clone(), posts[1].clone(), posts[0].clone()]);

    // Re-viewed: on top, once.
    history.record(&account, &profile, &posts[0], at(10)).await.unwrap();
    assert_eq!(tab(&history, &profile).await, vec![posts[0].clone(), posts[2].clone(), posts[1].clone()]);
    // posts[1], viewed long ago, moves up; posts[0], viewed 30 s ago (within
    // the debounce), changes nothing.
    history.record(&account, &profile, &posts[1], at(10) + 30_000_000).await.unwrap();
    history.record(&account, &profile, &posts[0], at(10) + 30_000_000).await.unwrap();
    let (page, next) = history.list(&profile, 2, None).await.unwrap();
    assert_eq!(page.iter().map(|v| v.post_id.clone()).collect::<Vec<_>>(), vec![posts[1].clone(), posts[0].clone()]);
    assert_eq!(page[1].viewed_at_ms, at(10) / 1_000, "a quick re-view keeps the time");
    let next = next.expect("a full page is followed");
    assert_eq!(ViewCursor::decode(&next.encode()), Some(next.clone()));
    let (rest, end) = history.list(&profile, 2, Some(&next)).await.unwrap();
    assert_eq!(rest.iter().map(|v| v.post_id.clone()).collect::<Vec<_>>(), vec![posts[2].clone()]);
    assert!(end.is_none());

    history.remove(&account, &profile, &posts[2], at(20)).await.unwrap();
    history.remove(&account, &profile, &posts[2], at(21)).await.unwrap();
    assert_eq!(tab(&history, &profile).await, vec![posts[1].clone(), posts[0].clone()], "removed, idempotently");

    history.clear(&account, &profile, at(30)).await.unwrap();
    assert!(tab(&history, &profile).await.is_empty(), "cleared");
    assert!(history.list_by_account(&account, 100, None).await.unwrap().is_empty());
    // Viewed after the clear: a new history.
    history.record(&account, &profile, &posts[2], at(40)).await.unwrap();
    assert_eq!(tab(&history, &profile).await, vec![posts[2].clone()]);
}

/// A history row the authority doesn't confirm (two re-views racing) is not
/// listed, and the account's erasure still takes it.
#[tokio::test]
async fn a_row_the_authority_does_not_confirm_is_not_listed() {
    let client = scylla().await;
    let history = ScyllaViewHistory::new(Arc::clone(&client));
    let (account, profile, post) = (ids(), ids(), ids());
    history.record(&account, &profile, &post, at(1)).await.unwrap();
    let mut stray = scylla::statement::unprepared::Statement::new(
        "INSERT INTO engagement.view_history_by_profile (profile_id, viewed_at, post_id, account_id) VALUES (?, ?, ?, ?)",
    );
    stray.set_timestamp(Some(at(5)));
    let stray_at = scylla::value::CqlTimestamp(at(5) / 1_000);
    client.session.execute_unpaged(stray, (&profile, stray_at, &post, Uuid::parse_str(&account).unwrap())).await.unwrap();
    let listed = history.list(&profile, 100, None).await.unwrap().0;
    assert_eq!(listed.len(), 1, "listed once");
    assert_eq!(listed[0].viewed_at_ms, at(1) / 1_000, "at the confirmed time");

    history.forget_account(&account, at(10)).await.unwrap();
    let left = scylla::statement::unprepared::Statement::new(
        "SELECT post_id FROM engagement.view_history_by_profile WHERE profile_id = ? AND viewed_at > ? ALLOW FILTERING",
    );
    let rows = client.session.execute_unpaged(left, (&profile, scylla::value::CqlTimestamp(0))).await.unwrap();
    assert_eq!(rows.into_rows_result().unwrap().rows_num(), 0, "no trace of the profile's history");
}

#[tokio::test]
async fn an_accounts_history_is_exported_and_forgotten_with_a_profile_or_the_account() {
    let history = ScyllaViewHistory::new(scylla().await);
    let account = ids();
    let (gone, kept) = (ids(), ids());
    let (a, b, c) = (ids(), ids(), ids());
    history.record(&account, &gone, &a, at(1)).await.unwrap();
    history.record(&account, &kept, &b, at(2)).await.unwrap();
    history.record(&account, &kept, &c, at(3)).await.unwrap();

    let exported = history.list_by_account(&account, 2, None).await.unwrap();
    assert_eq!(exported.len(), 2);
    let last = exported.last().unwrap();
    let rest = history.list_by_account(&account, 2, Some((&last.profile_id, &last.post_id))).await.unwrap();
    assert_eq!(exported.len() + rest.len(), 3, "every profile's history, paged");

    history.forget_profile(&gone, at(10)).await.unwrap();
    assert!(tab(&history, &gone).await.is_empty());
    assert_eq!(history.list_by_account(&account, 100, None).await.unwrap().len(), 2, "the other profile's stay");
    history.forget_profile(&ids(), at(10)).await.unwrap(); // never viewed: a no-op

    history.forget_account(&account, at(20)).await.unwrap();
    assert!(tab(&history, &kept).await.is_empty());
    assert!(history.list_by_account(&account, 100, None).await.unwrap().is_empty());
    // A view stamped before the erasure (a retry landing late) stays gone.
    history.record(&account, &kept, &b, at(15)).await.unwrap();
    assert!(tab(&history, &kept).await.is_empty());
}

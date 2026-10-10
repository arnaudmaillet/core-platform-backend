//! #872 over live Scylla: a profile's saved posts — most recently saved
//! first, idempotent, unsaved for good, listed only where the authority
//! confirms them — and forgotten with the profile or the account.

use std::sync::Arc;

use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use uuid::Uuid;

use engagement::application::port::{SavedCursor, SavedPosts};
use engagement::infrastructure::persistence::ScyllaSavedPosts;

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

/// µs, a second apart.
fn at(second: i64) -> i64 {
    1_700_000_000_000_000 + second * 1_000_000
}

async fn tab(saves: &ScyllaSavedPosts, profile: &str) -> Vec<String> {
    saves.list(profile, 100, None).await.unwrap().0.into_iter().map(|s| s.post_id).collect()
}

#[tokio::test]
async fn saves_list_newest_first_stay_put_when_saved_again_and_unsave_for_good() {
    let saves = ScyllaSavedPosts::new(scylla().await);
    let (account, profile) = (ids(), ids());
    let posts: Vec<String> = (0..3).map(|_| ids()).collect();
    for (i, post) in posts.iter().enumerate() {
        saves.save(&account, &profile, post, at(i as i64)).await.unwrap();
    }
    assert_eq!(tab(&saves, &profile).await, vec![posts[2].clone(), posts[1].clone(), posts[0].clone()]);

    // Saved again (a retry, another device): its first time stays.
    saves.save(&account, &profile, &posts[0], at(10)).await.unwrap();
    let (page, next) = saves.list(&profile, 2, None).await.unwrap();
    assert_eq!(page.iter().map(|s| s.post_id.clone()).collect::<Vec<_>>(), vec![posts[2].clone(), posts[1].clone()]);
    assert_eq!(page[0].saved_at_ms, at(2) / 1_000);
    let next = next.expect("a full page is followed");
    assert_eq!(SavedCursor::decode(&next.encode()), Some(next.clone()));
    let (rest, end) = saves.list(&profile, 2, Some(&next)).await.unwrap();
    assert_eq!(rest.iter().map(|s| s.post_id.clone()).collect::<Vec<_>>(), vec![posts[0].clone()]);
    assert!(end.is_none());

    saves.unsave(&account, &profile, &posts[1], at(20)).await.unwrap();
    saves.unsave(&account, &profile, &posts[1], at(21)).await.unwrap();
    assert_eq!(tab(&saves, &profile).await, vec![posts[2].clone(), posts[0].clone()], "unsaved, idempotently");
    // Saved anew after the unsave: at its new time, on top.
    saves.save(&account, &profile, &posts[1], at(30)).await.unwrap();
    assert_eq!(tab(&saves, &profile).await, vec![posts[1].clone(), posts[2].clone(), posts[0].clone()]);
}

/// A tab row the authority doesn't confirm (two saves racing leave the
/// loser's) is not listed.
#[tokio::test]
async fn a_tab_row_the_authority_does_not_confirm_is_not_listed() {
    let client = scylla().await;
    let saves = ScyllaSavedPosts::new(Arc::clone(&client));
    let (account, profile, post) = (ids(), ids(), ids());
    saves.save(&account, &profile, &post, at(1)).await.unwrap();
    let mut stray = scylla::statement::unprepared::Statement::new(
        "INSERT INTO engagement.saved_posts_by_profile (profile_id, saved_at, post_id, account_id) VALUES (?, ?, ?, ?)",
    );
    stray.set_timestamp(Some(at(5)));
    let stray_at = scylla::value::CqlTimestamp(at(5) / 1_000);
    client.session.execute_unpaged(stray, (&profile, stray_at, &post, Uuid::parse_str(&account).unwrap())).await.unwrap();
    let listed = saves.list(&profile, 100, None).await.unwrap().0;
    assert_eq!(listed.len(), 1, "listed once");
    assert_eq!(listed[0].saved_at_ms, at(1) / 1_000, "at the confirmed time");

    // Unsaved: the authority has nothing left for the profile, the stray
    // row stays unlisted, and the account's erasure still takes it.
    saves.unsave(&account, &profile, &post, at(6)).await.unwrap();
    assert!(saves.list(&profile, 100, None).await.unwrap().0.is_empty());
    saves.forget_account(&account, at(10)).await.unwrap();
    let left = scylla::statement::unprepared::Statement::new(
        "SELECT post_id FROM engagement.saved_posts_by_profile WHERE profile_id = ? AND saved_at > ? ALLOW FILTERING",
    );
    let rows = client.session.execute_unpaged(left, (&profile, scylla::value::CqlTimestamp(0))).await.unwrap();
    assert_eq!(rows.into_rows_result().unwrap().rows_num(), 0, "no trace of the profile's saves");
}

#[tokio::test]
async fn an_accounts_saves_are_exported_and_forgotten_with_a_profile_or_the_account() {
    let saves = ScyllaSavedPosts::new(scylla().await);
    let account = ids();
    let (gone, kept) = (ids(), ids());
    let (a, b, c) = (ids(), ids(), ids());
    saves.save(&account, &gone, &a, at(1)).await.unwrap();
    saves.save(&account, &kept, &b, at(2)).await.unwrap();
    saves.save(&account, &kept, &c, at(3)).await.unwrap();

    let exported = saves.list_by_account(&account, 2, None).await.unwrap();
    assert_eq!(exported.len(), 2);
    let last = exported.last().unwrap();
    let rest = saves.list_by_account(&account, 2, Some((&last.profile_id, &last.post_id))).await.unwrap();
    assert_eq!(exported.len() + rest.len(), 3, "every profile's saves, paged");

    saves.forget_profile(&gone, at(10)).await.unwrap();
    assert!(tab(&saves, &gone).await.is_empty());
    assert_eq!(saves.list_by_account(&account, 100, None).await.unwrap().len(), 2, "the other profile's stay");
    saves.forget_profile(&ids(), at(10)).await.unwrap(); // never saved: a no-op

    saves.forget_account(&account, at(20)).await.unwrap();
    assert!(tab(&saves, &kept).await.is_empty());
    assert!(saves.list_by_account(&account, 100, None).await.unwrap().is_empty());
    // A save stamped before the erasure (a retry landing late) stays gone.
    saves.save(&account, &kept, &b, at(15)).await.unwrap();
    assert!(tab(&saves, &kept).await.is_empty());
}

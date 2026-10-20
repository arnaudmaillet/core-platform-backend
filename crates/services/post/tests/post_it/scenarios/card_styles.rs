//! Scenario — post card styles (#914) over ScyllaDB: the author applies an
//! owned style; the post and the profile's list read it back; the default
//! card clears both tables.

use std::sync::Arc;

use async_trait::async_trait;

use post::application::card_styles::{CardStyles, DEFAULT_NEUTRAL_WINDOW};
use post::application::port::ItemOwnership;
use post::domain::value_object::PostId;
use post::error::PostError;

use crate::post_it::harness::{self, ProfileId, TestHarness};

/// Every account owns `card.paper`.
struct OwnsPaper;

#[async_trait]
impl ItemOwnership for OwnsPaper {
    async fn owns(&self, _: &str, item: &str) -> Result<bool, PostError> {
        Ok(item == "card.paper")
    }
}

#[tokio::test]
async fn a_style_is_stored_on_the_post_and_its_profile_list() {
    let h = TestHarness::start().await;
    let (post_id, author) = (harness::random_id(), harness::random_id());
    h.create(&post_id, &author).await;
    h.publish(&post_id, &author).await;
    let styles = CardStyles {
        posts:          Arc::clone(&h.repository) as _,
        wallet:         Some(Arc::new(OwnsPaper)),
        neutral_window: DEFAULT_NEUTRAL_WINDOW,
    };
    let (id, pid) = (PostId::try_from(post_id.as_str()).unwrap(), ProfileId::try_from(author.as_str()).unwrap());

    assert!(matches!(styles.apply(&id, &pid, Some("acct"), Some("card.night".into())).await, Err(PostError::ItemNotOwned { .. })));
    styles.apply(&id, &pid, Some("acct"), Some("card.paper".into())).await.expect("an owned style");
    assert_eq!(h.get(&post_id).await.unwrap().card_style_id(), Some("card.paper"));
    assert_eq!(h.list(&author).await[0].card_style_id.as_deref(), Some("card.paper"), "the profile's list too");

    styles.apply(&id, &pid, None, None).await.expect("the default card");
    assert_eq!(h.get(&post_id).await.unwrap().card_style_id(), None);
    assert_eq!(h.list(&author).await[0].card_style_id, None);
}

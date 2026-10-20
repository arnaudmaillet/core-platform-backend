//! Post card styles (#914): a catalogue item an author's account buys once
//! and applies, free, to their own posts (one per post, changeable,
//! removable; deleting the post consumes nothing). Other viewers see a style
//! only once the post is past its experiment window, so that the control and
//! test cohorts both see the default card (charter V5.3 §8–9) — except on the
//! author's profile and the post detail (the owner's agreed exception). The
//! author always sees it.

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};

use crate::application::port::{ItemOwnership, PostRepository};
use crate::domain::aggregate::{is_card_style_id, Post};
use crate::domain::value_object::{PostId, ProfileId};
use crate::error::PostError;

/// Until #889 defines it: the stake settlement delay (server policy).
pub const DEFAULT_NEUTRAL_WINDOW: TimeDelta = TimeDelta::hours(24);

pub struct CardStyles {
    pub posts:  Arc<dyn PostRepository>,
    /// `None`: a style cannot be applied (`WalletUnavailable`), only removed.
    pub wallet: Option<Arc<dyn ItemOwnership>>,
    /// How long after publication other viewers see the default card.
    pub neutral_window: TimeDelta,
}

impl CardStyles {
    /// Applies `style` (or the default card, `None`) to `post_id`, authored by
    /// `author`, for the edge caller's `account`.
    pub async fn apply(
        &self,
        post_id: &PostId,
        author: &ProfileId,
        account: Option<&str>,
        style: Option<String>,
    ) -> Result<(), PostError> {
        let mut post = self.posts.find_by_id(post_id).await?.ok_or_else(|| PostError::PostNotFound { post_id: post_id.as_str() })?;
        if post.profile_id().as_uuid() != author.as_uuid() {
            return Err(PostError::AuthorMismatch { post_id: post_id.as_str(), caller_id: author.as_str() });
        }
        if post.card_style_id() == style.as_deref() {
            return Ok(());
        }
        if let Some(style) = &style {
            if !is_card_style_id(style) {
                return Err(PostError::DomainViolation { field: "card_style_id".into(), message: format!("'{style}' is not a card style") });
            }
            let wallet = self.wallet.as_ref().ok_or_else(|| PostError::WalletUnavailable { reason: "not configured".into() })?;
            let account = account.ok_or_else(|| PostError::ItemNotOwned { item: style.clone() })?;
            if !wallet.owns(account, style).await? {
                return Err(PostError::ItemNotOwned { item: style.clone() });
            }
        }
        post.set_card_style(style)?;
        self.posts.update_card_style(&post).await
    }

    /// Whether a reader that is not the author sees `post`'s style now.
    pub fn shown_to_others(&self, post: &Post, now: DateTime<Utc>) -> bool {
        post.published_at().is_some_and(|at| at + self.neutral_window <= now)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::application::port::PostSummary;
    use crate::domain::value_object::{Caption, PostKind};

    /// One published post by `author`; its stored style.
    struct OnePost {
        id:     PostId,
        author: ProfileId,
        style:  Mutex<Option<String>>,
    }

    impl OnePost {
        fn post(&self) -> Post {
            let mut post = Post::create(
                self.id.clone(), self.author.clone(), PostKind::TextOnly, Caption::new("hi").unwrap(), vec![], None, None, None, None,
            )
            .unwrap();
            post.publish().unwrap();
            post.with_card_style(self.style.lock().unwrap().clone())
        }
    }

    #[async_trait]
    impl PostRepository for OnePost {
        async fn insert(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_content(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_lifecycle(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_moderation(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_card_style(&self, post: &Post) -> Result<(), PostError> {
            *self.style.lock().unwrap() = post.card_style_id().map(str::to_owned);
            Ok(())
        }
        async fn find_by_id(&self, id: &PostId) -> Result<Option<Post>, PostError> {
            Ok((id.as_uuid() == self.id.as_uuid()).then(|| self.post()))
        }
        async fn list_by_profile(&self, _: &ProfileId, _: i32, _: Option<&str>) -> Result<(Vec<PostSummary>, Option<String>), PostError> {
            unreachable!()
        }
    }

    /// The account `acct` owns `card.paper`.
    struct Owns;

    #[async_trait]
    impl ItemOwnership for Owns {
        async fn owns(&self, account: &str, item: &str) -> Result<bool, PostError> {
            Ok(account == "acct" && item == "card.paper")
        }
    }

    fn styles() -> (CardStyles, Arc<OnePost>) {
        let post = Arc::new(OnePost {
            id:     PostId::from_uuid(Uuid::now_v7()),
            author: ProfileId::from_uuid(Uuid::now_v7()),
            style:  Mutex::new(None),
        });
        let styles = CardStyles { posts: Arc::clone(&post) as _, wallet: Some(Arc::new(Owns)), neutral_window: DEFAULT_NEUTRAL_WINDOW };
        (styles, post)
    }

    #[tokio::test]
    async fn an_author_applies_an_owned_style_to_their_own_post() {
        let (s, post) = styles();
        let stranger = ProfileId::from_uuid(Uuid::now_v7());
        let paper = || Some("card.paper".to_owned());
        assert!(matches!(s.apply(&post.id, &stranger, Some("acct"), paper()).await, Err(PostError::AuthorMismatch { .. })));
        assert!(matches!(s.apply(&post.id, &post.author, Some("other"), paper()).await, Err(PostError::ItemNotOwned { .. })));
        assert!(matches!(s.apply(&post.id, &post.author, Some("acct"), Some("theme.red".into())).await, Err(PostError::DomainViolation { .. })));
        s.apply(&post.id, &post.author, Some("acct"), paper()).await.unwrap();
        assert_eq!(post.style.lock().unwrap().as_deref(), Some("card.paper"));
        s.apply(&post.id, &post.author, None, None).await.expect("the default card needs no ownership");
        assert_eq!(*post.style.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn others_see_the_style_only_past_the_window() {
        let (s, post) = styles();
        let read = post.post();
        let published = read.published_at().unwrap();
        assert!(!s.shown_to_others(&read, published + TimeDelta::hours(23)));
        assert!(s.shown_to_others(&read, published + TimeDelta::hours(24)));
        let draft = Post::create(
            PostId::from_uuid(Uuid::now_v7()), post.author.clone(), PostKind::TextOnly, Caption::new("x").unwrap(), vec![], None, None, None, None,
        )
        .unwrap();
        assert!(!s.shown_to_others(&draft, Utc::now() + TimeDelta::days(9)), "never published");
    }
}

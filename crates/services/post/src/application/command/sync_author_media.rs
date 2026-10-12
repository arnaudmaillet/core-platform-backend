//! A profile switched between public and private (#919 step 3): every one of
//! its posts' images follows — SIGNED for a private author, PUBLIC again for a
//! public one. Videos follow once signed videos are on (#921,
//! `POST_SIGNED_VIDEOS_ENABLED`: media plays them through signed cookies);
//! until then they stay as they are.

use crate::application::port::{MediaCatalog, PostRepository, SetDelivery};
use crate::domain::value_object::ProfileId;
use crate::error::PostError;

/// Posts read per page of the walk.
const PAGE: i32 = 100;

/// What a walk did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MediaSync {
    pub changed:   usize,
    /// Still processing: media refuses until READY. Such an asset was
    /// uploaded moments before the switch; a later switch catches it.
    pub not_ready: usize,
    /// Assets that cannot go SIGNED yet (videos before #921 is on), left as
    /// they are.
    pub videos:    usize,
}

/// Delivers every asset of `author`'s posts SIGNED (`private`) or PUBLIC.
/// Idempotent, so a redelivered event walks again harmlessly; a media outage
/// is an error, for the consumer to retry.
pub async fn sync_author_media(
    posts: &dyn PostRepository,
    media: &dyn MediaCatalog,
    author: &ProfileId,
    private: bool,
) -> Result<MediaSync, PostError> {
    let mut sync = MediaSync::default();
    let mut token: Option<String> = None;
    loop {
        let (page, next) = posts.list_by_profile(author, PAGE, token.as_deref()).await?;
        for summary in page {
            let Some(post) = posts.find_by_id(&summary.post_id).await? else { continue };
            for asset in post.attachments().iter().filter_map(|a| a.asset_id) {
                let Some(facts) = media.asset(&asset).await? else { continue };
                if !facts.signable {
                    sync.videos += 1;
                    continue;
                }
                if facts.signed == private {
                    continue;
                }
                match media.set_delivery(&asset, Some(&facts.owner_account_id), private).await? {
                    SetDelivery::Done => sync.changed += 1,
                    SetDelivery::NotReady => sync.not_ready += 1,
                    SetDelivery::Gone => {}
                }
            }
        }
        match next {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    if sync.not_ready > 0 {
        tracing::warn!(author = %author.as_str(), not_ready = sync.not_ready, "media still processing kept its delivery");
    }
    Ok(sync)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::port::{MediaAssetFacts, PostSummary};
    use crate::domain::aggregate::Post;
    use crate::domain::entity::MediaAttachment;
    use crate::domain::value_object::{
        Caption, CdnUrl, MediaAssetId, MimeType, ModerationRestriction, PostId, PostKind, PostStatus,
    };

    /// One author's posts (built on each read: `Post` is not `Clone`), two per
    /// page.
    struct Posts(ProfileId, Vec<(PostId, Vec<Option<MediaAssetId>>)>);

    impl Posts {
        fn build(&self, id: &PostId, assets: &[Option<MediaAssetId>]) -> Post {
            post(id.clone(), &self.0, assets.iter().map(|a| attachment(*a)).collect())
        }
    }

    #[async_trait]
    impl PostRepository for Posts {
        async fn insert(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_content(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_lifecycle(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn update_moderation(&self, _: &Post) -> Result<(), PostError> { unreachable!() }
        async fn find_by_id(&self, id: &PostId) -> Result<Option<Post>, PostError> {
            Ok(self.1.iter().find(|(p, _)| p == id).map(|(p, assets)| self.build(p, assets)))
        }
        async fn list_by_profile(
            &self,
            _: &ProfileId,
            _: i32,
            token: Option<&str>,
        ) -> Result<(Vec<PostSummary>, Option<String>), PostError> {
            let start: usize = token.map_or(0, |t| t.parse().unwrap());
            let page: Vec<PostSummary> = self.1.iter().skip(start).take(2).map(|(p, _)| PostSummary {
                post_id:    p.clone(),
                kind:       PostKind::Carousel,
                status:     PostStatus::Published,
                moderation: ModerationRestriction::None,
                created_at: chrono::Utc::now(),
                is_repost:  false,
                has_place:  false,
            }).collect();
            let next = (start + 2 < self.1.len()).then(|| (start + 2).to_string());
            Ok((page, next))
        }
    }

    /// Every asset an image owned by `acct`, PUBLIC; the ones listed in
    /// `.2` videos.
    struct Media(Mutex<Vec<(String, bool)>>, Option<SetDelivery>, Vec<MediaAssetId>);

    #[async_trait]
    impl MediaCatalog for Media {
        async fn asset(&self, id: &MediaAssetId) -> Result<Option<MediaAssetFacts>, PostError> {
            Ok(Some(MediaAssetFacts {
                owner_account_id: "acct".into(),
                attachable:       true,
                usable:           true,
                signed:           false,
                ready:            true,
                signable:         !self.2.contains(id),
            }))
        }
        async fn set_delivery(&self, id: &MediaAssetId, owner: Option<&str>, signed: bool) -> Result<SetDelivery, PostError> {
            assert_eq!(owner, Some("acct"), "as its owner");
            self.0.lock().unwrap().push((id.as_str(), signed));
            self.1.ok_or(PostError::MediaUnavailable { reason: "down".into() })
        }
    }

    fn attachment(asset: Option<MediaAssetId>) -> MediaAttachment {
        MediaAttachment {
            cdn_url:          CdnUrl::new("https://cdn.example/a.jpg").unwrap(),
            mime_type:        MimeType::new("image/jpeg").unwrap(),
            width:            10,
            height:           10,
            thumbnail_url:    None,
            duration_seconds: None,
            asset_id:         asset,
        }
    }

    fn asset() -> MediaAssetId {
        MediaAssetId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap()
    }

    fn post(id: PostId, author: &ProfileId, attachments: Vec<MediaAttachment>) -> Post {
        Post::create(
            id,
            author.clone(),
            PostKind::Carousel,
            Caption::new("").unwrap(),
            attachments,
            None,
            None,
            None,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn every_posts_media_follows_the_authors_privacy() {
        let author = ProfileId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap();
        let assets: Vec<MediaAssetId> = (0..5).map(|_| asset()).collect();
        let posts = Posts(author.clone(), vec![
            (PostId::new_v7(), vec![Some(assets[0]), Some(assets[1])]),
            (PostId::new_v7(), vec![Some(assets[2]), None]),
            (PostId::new_v7(), vec![Some(assets[3]), Some(assets[4])]),
        ]);
        let media = Media(Mutex::default(), Some(SetDelivery::Done), vec![assets[4]]);

        let sync = sync_author_media(&posts, &media, &author, true).await.unwrap();

        assert_eq!(sync, MediaSync { changed: 4, not_ready: 0, videos: 1 }, "across pages; the video is left");
        let mut changed: Vec<(String, bool)> = media.0.lock().unwrap().clone();
        changed.sort();
        let mut expected: Vec<(String, bool)> = assets[..4].iter().map(|a| (a.as_str(), true)).collect();
        expected.sort();
        assert_eq!(changed, expected);

        // Public again: nothing to do for assets already PUBLIC (the fake's).
        let back = Media(Mutex::default(), Some(SetDelivery::Done), vec![]);
        assert_eq!(sync_author_media(&posts, &back, &author, false).await.unwrap().changed, 0);
    }

    #[tokio::test]
    async fn a_media_outage_is_an_error_to_retry() {
        let author = ProfileId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap();
        let posts = Posts(author.clone(), vec![(PostId::new_v7(), vec![Some(asset()), Some(asset())])]);
        let err = sync_author_media(&posts, &Media(Mutex::default(), None, vec![]), &author, true).await.unwrap_err();
        assert!(matches!(err, PostError::MediaUnavailable { .. }));
    }
}

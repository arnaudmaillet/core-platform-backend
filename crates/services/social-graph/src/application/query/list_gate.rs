//! The gate in front of a profile's follower / following lists: the access
//! rule (private profile, blocks, hidden), then the owner's list audience.

use crate::application::port::SocialGraphRepository;
use crate::domain::access::{ContentAccess, Viewer};
use crate::domain::interaction::InteractionAudience;
use crate::domain::list_privacy::FollowList;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// May `viewer` read `owner`'s `list`? The owner and the mesh always may.
/// Anyone else needs content access to the owner, then a place in the owner's
/// audience for that list, judged by the viewer's best-placed profile.
pub async fn may_read_list(
    repo: &dyn SocialGraphRepository,
    viewer: &Viewer,
    owner: &ProfileId,
    list: FollowList,
) -> Result<bool, SocialGraphError> {
    let Viewer::Profiles(viewers) = viewer else {
        return Ok(true);
    };
    if viewer.sees_everything_of(owner) {
        return Ok(true);
    }
    let (facts, privacy) = tokio::join!(
        repo.load_access_facts(viewers, std::slice::from_ref(owner)),
        repo.load_list_privacy(owner),
    );
    if facts?.access(viewers, owner) != ContentAccess::Visible {
        return Ok(false);
    }
    let audience = privacy?.audience(list);
    match audience {
        InteractionAudience::Everyone => return Ok(true),
        InteractionAudience::NoOne => return Ok(false),
        InteractionAudience::Followers | InteractionAudience::Mutuals => {}
    }
    for profile in viewers {
        let relation = repo.load_relation(profile, owner).await?;
        if audience.admits(
            relation.actor_follows_target_since().is_some(),
            relation.target_follows_actor_since().is_some(),
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

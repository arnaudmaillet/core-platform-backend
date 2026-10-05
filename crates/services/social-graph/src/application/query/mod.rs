pub mod check_access;
pub mod check_interaction;
pub mod get_list_privacy;
pub mod get_relation_status;
pub mod list_blocks;
pub mod list_follow_requests;
pub mod list_followers;
pub mod list_gate;
pub mod list_following;
pub mod mutes;
pub mod restrictions;

pub use check_access::{CheckAccessHandler, CheckAccessQuery};
pub use check_interaction::{CheckInteractionHandler, CheckInteractionQuery};
pub use get_list_privacy::{GetListPrivacyHandler, GetListPrivacyQuery};
pub use get_relation_status::{GetRelationStatusQuery, GetRelationStatusHandler};
pub use list_blocks::{ListBlocksQuery, ListBlocksHandler};
pub use list_follow_requests::{FollowRequestsPage, ListFollowRequestsHandler, ListFollowRequestsQuery};
pub use list_followers::{ListFollowersQuery, ListFollowersHandler};
pub use list_following::{ListFollowingQuery, ListFollowingHandler};
pub use mutes::{ListMutesHandler, ListMutesQuery, MutedProfilesHandler, MutedProfilesQuery};
pub use restrictions::{ListRestrictedHandler, ListRestrictedQuery, RestrictedAmongHandler, RestrictedAmongQuery};

use crate::domain::entity::FollowEdge;

/// A page of a follower / following list.
#[derive(Debug, Clone, Default)]
pub struct FollowListPage {
    pub edges:           Vec<FollowEdge>,
    pub next_page_token: Option<String>,
    /// The reader may not see this list (a private profile they do not
    /// follow, a block, a hidden profile, or the owner's list privacy): empty.
    pub hidden:          bool,
}

impl FollowListPage {
    pub fn hidden() -> Self {
        Self { hidden: true, ..Self::default() }
    }
}

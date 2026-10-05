pub mod author_tier_changed;
pub mod follow_requested;
pub mod profile_blocked;
pub mod profile_followed;
pub mod profile_unblocked;
pub mod profile_unfollowed;

pub use author_tier_changed::AuthorTierChanged;
pub use follow_requested::FollowRequested;
pub use profile_blocked::ProfileBlocked;
pub use profile_followed::ProfileFollowed;
pub use profile_unblocked::ProfileUnblocked;
pub use profile_unfollowed::ProfileUnfollowed;

#[derive(Debug, Clone)]
pub enum DomainEvent {
    ProfileFollowed(ProfileFollowed),
    /// A follow request to a private profile (#755: its owner is told).
    FollowRequested(FollowRequested),
    ProfileUnfollowed(ProfileUnfollowed),
    ProfileBlocked(ProfileBlocked),
    ProfileUnblocked(ProfileUnblocked),
    /// The author-tier signal — emitted on a follower-count tier crossing.
    AuthorTierChanged(AuthorTierChanged),
}

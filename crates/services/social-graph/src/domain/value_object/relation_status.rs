/// The combined relationship status between an actor and a target profile,
/// observed from the actor's point of view.
///
/// # Invariants
///
/// Block states and follow states are mutually exclusive. A block severs any
/// existing follow in both directions and prevents new follows, so `Blocking`
/// and `BlockedBy` can never coexist with any follow-based variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationStatus {
    /// No relationship exists between actor and target.
    None,
    /// Actor follows target; target does not follow actor back.
    Following,
    /// Target follows actor; actor does not follow target.
    FollowedBy,
    /// Both profiles follow each other (implicit "friendship").
    MutualFollow,
    /// Actor has blocked target.
    Blocking,
    /// Target has blocked actor.
    BlockedBy,
    /// Actor asked to follow target (a private profile); awaiting approval.
    Requested,
}

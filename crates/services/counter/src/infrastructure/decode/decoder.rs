//! Pure wire → [`Observation`] distillation. One inbound event yields zero or more
//! observations (a view becomes both a `View` sum and a `UniqueViewer` member; a
//! follow becomes a `Follower` on the followee and a `Following` on the follower).
//!
//! This is engine-free and fully unit-tested; the consumer wiring (Phase 5) owns
//! deserialization (so poison bytes dead-letter before reaching here) and then
//! calls these `map_*` functions on the already-decoded wire enum.

use chrono::{DateTime, TimeZone, Utc};

use crate::domain::{EntityId, EntityKind, EntityRef, MemberId, Metric, Observation};
use crate::error::CounterError;
use crate::infrastructure::decode::wire::{FollowWire, HitWire, WalletWire};

fn at(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_else(Utc::now)
}

fn entity(kind: &str, id: &str) -> Result<EntityRef, CounterError> {
    Ok(EntityRef::new(
        EntityKind::try_from_str(kind)?,
        EntityId::new(id)?,
    ))
}

/// `view.v1.events` → a `View` sum (`+1`) plus, when the viewer is known, a
/// `UniqueViewer` member for the HyperLogLog.
pub fn map_view(wire: HitWire) -> Result<Vec<Observation>, CounterError> {
    let e = entity(&wire.entity_type, &wire.entity_id)?;
    let mut out = vec![Observation::sum(e.clone(), Metric::View, 1, at(wire.occurred_at_ms))?];
    if let Some(actor) = wire.actor_id {
        out.push(Observation::unique(
            e,
            Metric::UniqueViewer,
            MemberId::new(actor)?,
            at(wire.occurred_at_ms),
        )?);
    }
    Ok(out)
}

/// `impression.v1.events` → an `Impression` sum plus, when the actor is known, a
/// `Reach` member (unique accounts reached).
pub fn map_impression(wire: HitWire) -> Result<Vec<Observation>, CounterError> {
    let e = entity(&wire.entity_type, &wire.entity_id)?;
    let mut out = vec![Observation::sum(
        e.clone(),
        Metric::Impression,
        1,
        at(wire.occurred_at_ms),
    )?];
    if let Some(actor) = wire.actor_id {
        out.push(Observation::unique(
            e,
            Metric::Reach,
            MemberId::new(actor)?,
            at(wire.occurred_at_ms),
        )?);
    }
    Ok(out)
}

/// `click.v1.events` → a `Click` sum (`+1`). Clicks are not deduplicated.
pub fn map_click(wire: HitWire) -> Result<Vec<Observation>, CounterError> {
    let e = entity(&wire.entity_type, &wire.entity_id)?;
    Ok(vec![Observation::sum(
        e,
        Metric::Click,
        1,
        at(wire.occurred_at_ms),
    )?])
}

/// `wallet.v1.events` `stake_committed` → a `Like` magnitude on the post or
/// comment: its `points` (#665: a like is a point; a stake is final, never
/// negative). Approximate like every counter sum — a redelivered stake counts
/// twice, a crash loses a window (see the consumer module) — and a popularity
/// input: the exact like count is engagement's. Other wallet events: nothing.
pub fn map_stake(wire: WalletWire) -> Result<Vec<Observation>, CounterError> {
    match wire {
        WalletWire::StakeCommitted(e) if e.points > 0 => Ok(vec![Observation::sum(
            entity(&e.target_kind, &e.target_id)?,
            Metric::Like,
            e.points,
            e.staked_at,
        )?]),
        WalletWire::StakeCommitted(_) | WalletWire::Other => Ok(Vec::new()),
    }
}

/// A social-graph follow change → a `Follower` magnitude on the followee and a
/// `Following` magnitude on the follower (`+1` follow, `-1` unfollow), so both
/// counts stay consistent from a single event.
pub fn map_follow(wire: FollowWire) -> Result<Vec<Observation>, CounterError> {
    let (change, amount) = match wire {
        FollowWire::Followed(c) => (c, 1),
        FollowWire::Unfollowed(c) => (c, -1),
    };
    let when = at(change.occurred_at_ms);
    Ok(vec![
        Observation::sum(
            entity("profile", &change.followee_id)?,
            Metric::Follower,
            amount,
            when,
        )?,
        Observation::sum(
            entity("profile", &change.follower_id)?,
            Metric::Following,
            amount,
            when,
        )?,
    ])
}

#[cfg(test)]
mod tests {
    use error::AppError;

    use super::*;
    use crate::infrastructure::decode::wire::{
        FollowChangeWire,
    };

    #[test]
    fn view_with_viewer_yields_view_and_unique() {
        let obs = map_view(HitWire {
            entity_type: "post".into(),
            entity_id: "p1".into(),
            actor_id: Some("viewer-1".into()),
            occurred_at_ms: 1_000,
        })
        .unwrap();
        assert_eq!(obs.len(), 2);
        assert_eq!(obs[0].metric, Metric::View);
        assert_eq!(obs[0].amount, 1);
        assert_eq!(obs[1].metric, Metric::UniqueViewer);
        assert_eq!(obs[1].unique_member.as_ref().unwrap().as_str(), "viewer-1");
    }

    #[test]
    fn view_without_viewer_is_sum_only() {
        let obs = map_view(HitWire {
            entity_type: "post".into(),
            entity_id: "p1".into(),
            actor_id: None,
            occurred_at_ms: 1_000,
        })
        .unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].metric, Metric::View);
    }

    #[test]
    fn unknown_entity_kind_is_rejected() {
        let err = map_view(HitWire {
            entity_type: "account".into(),
            entity_id: "a1".into(),
            actor_id: None,
            occurred_at_ms: 1,
        })
        .unwrap_err();
        assert_eq!(err.error_code(), "CTR-9001");
    }

    /// The wallet's JSON as it publishes it (#665), read back as counter does.
    fn stake(json: &str) -> WalletWire {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_stake_adds_its_points_to_the_targets_likes() {
        let obs = map_stake(stake(
            r#"{"type":"stake_committed","account_id":"a","profile_id":"p","target_kind":"comment","target_id":"c1",
                "author_profile_id":"x","points":30,"total":30,"first":true,"stake_key":"stake:k",
                "staked_at":"2026-10-08T12:00:00Z"}"#,
        ))
        .unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!((obs[0].metric, obs[0].amount), (Metric::Like, 30));
        assert_eq!(obs[0].entity.kind, crate::domain::value_object::EntityKind::Comment);
    }

    #[test]
    fn other_wallet_events_count_nothing() {
        assert!(map_stake(stake(r#"{"type":"something_new","x":1}"#)).unwrap().is_empty());
    }

    #[test]
    fn follow_updates_both_sides() {
        let obs = map_follow(FollowWire::Followed(FollowChangeWire {
            follower_id: "alice".into(),
            followee_id: "bob".into(),
            occurred_at_ms: 1,
        }))
        .unwrap();
        assert_eq!(obs.len(), 2);
        // followee gains a Follower
        assert_eq!(obs[0].metric, Metric::Follower);
        assert_eq!(obs[0].entity.id.as_str(), "bob");
        assert_eq!(obs[0].amount, 1);
        // follower gains a Following
        assert_eq!(obs[1].metric, Metric::Following);
        assert_eq!(obs[1].entity.id.as_str(), "alice");
    }

    #[test]
    fn unfollow_decrements_both_sides() {
        let obs = map_follow(FollowWire::Unfollowed(FollowChangeWire {
            follower_id: "alice".into(),
            followee_id: "bob".into(),
            occurred_at_ms: 1,
        }))
        .unwrap();
        assert_eq!(obs[0].amount, -1);
        assert_eq!(obs[1].amount, -1);
    }
}

//! One-off backfill of every profile's tab settings (#873). The services
//! that apply them — post (window, Reposts, Places), engagement (Likes) —
//! learn them from `ProfileTabSettingsChanged` on `profile.v1.events`, from
//! the earliest offset still on the topic. A setting changed before the
//! topic's retention would never reach them and read as the default
//! (shown). This re-announces every profile whose settings differ from the
//! defaults, once (`PROFILE_BACKFILL_TAB_SETTINGS=true`).
//!
//! Re-announcing is harmless (the projections are last-writer-wins). A
//! holder changing their settings while the pass runs could be overtaken by
//! the pass's older value: run it once, at first deployment.
//!
//! Likewise every private profile's visibility (#927,
//! `PROFILE_BACKFILL_VISIBILITY=true`): post makes a private author's images
//! SIGNED (#919) from `ProfileVisibilityChanged`, which profiles private
//! before that never re-announced. Public is every projection's default.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::application::port::{EventPublisher, ProfileRepository};
use crate::domain::event::{DomainEvent, TabSettingsChanged, VisibilityChanged};
use crate::domain::value_object::{ProfileVisibility, TabSettings};
use crate::error::ProfileError;

/// Profiles read per page.
const PAGE: i32 = 500;

/// Re-announces every non-default tab setting; returns how many.
pub async fn backfill_tab_settings(
    repository: &Arc<dyn ProfileRepository>,
    publisher: &Arc<dyn EventPublisher>,
    now: DateTime<Utc>,
) -> Result<usize, ProfileError> {
    let defaults = TabSettings::default();
    let (mut announced, mut after) = (0, None);
    loop {
        let (page, next) = repository.tab_settings_page(after.as_ref(), PAGE).await?;
        for (profile_id, settings) in page {
            if settings == defaults {
                continue;
            }
            let event = TabSettingsChanged { profile_id, settings, occurred_at: now, correlation_id: Uuid::now_v7() };
            publisher.publish(&DomainEvent::TabSettingsChanged(event)).await?;
            announced += 1;
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(announced),
        }
    }
}

/// Re-announces every private profile's visibility (#927); returns how many.
pub async fn backfill_visibility(
    repository: &Arc<dyn ProfileRepository>,
    publisher: &Arc<dyn EventPublisher>,
    now: DateTime<Utc>,
) -> Result<usize, ProfileError> {
    let (mut announced, mut after) = (0, None);
    loop {
        let (page, next) = repository.visibility_page(after.as_ref(), PAGE).await?;
        for (profile_id, visibility) in page {
            if visibility != ProfileVisibility::Private {
                continue;
            }
            let event = VisibilityChanged { profile_id, visibility, occurred_at: now, correlation_id: Uuid::now_v7() };
            publisher.publish(&DomainEvent::VisibilityChanged(event)).await?;
            announced += 1;
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(announced),
        }
    }
}

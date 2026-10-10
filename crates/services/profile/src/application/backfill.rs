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

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::application::port::{EventPublisher, ProfileRepository};
use crate::domain::event::{DomainEvent, TabSettingsChanged};
use crate::domain::value_object::TabSettings;
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

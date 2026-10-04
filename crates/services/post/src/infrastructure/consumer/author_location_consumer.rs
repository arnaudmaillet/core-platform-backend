use std::sync::Arc;

use serde::Deserialize;
use tracing::{error, info};

use error::AppError;
use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::{AuthorLocationStore, AuthorWindowStore, ReuseDefaults, ReuseRegistry};
use crate::domain::value_object::{LocationSharing, ProfileId};

/// Lenient read DTO for `profile.v1.events` (the internally-tagged
/// `{"type": ...}` stream). Only `ProfileLocationSettingsChanged` and
/// `ProfileTabSettingsChanged` are acted on; all other variants deserialize
/// and are skipped.
#[derive(Debug, Deserialize)]
struct ProfileV1Event {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    profile_id: String,
    #[serde(default)]
    ghost:      Option<bool>,
    /// `precise` | `city`.
    #[serde(default)]
    precision:  Option<String>,
    /// `all` | `six_months` | `one_month` | `three_days`.
    #[serde(default)]
    post_window: Option<String>,
    /// Remix / sound reuse defaults (#669); absent on older events ⇒ allowed.
    #[serde(default)]
    allow_remix: Option<bool>,
    #[serde(default)]
    allow_sound_reuse: Option<bool>,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Record(ProfileId, LocationSharing),
    /// The author's post window in days (`None`: every post).
    Window(ProfileId, Option<u32>),
    /// The author's remix / sound reuse defaults.
    Reuse(ProfileId, ReuseDefaults),
    Poison(String),
}

fn outcome(event: &ProfileV1Event) -> Outcome {
    let tab = match event.event_type.as_str() {
        "ProfileLocationSettingsChanged" => false,
        "ProfileTabSettingsChanged" => true,
        "ProfileInteractionSettingsChanged" => false,
        _ => return Outcome::Skip,
    };
    let profile_id = match ProfileId::try_from(event.profile_id.as_str()) {
        Ok(id) => id,
        Err(e) => return Outcome::Poison(e.to_string()),
    };
    if event.event_type == "ProfileInteractionSettingsChanged" {
        return Outcome::Reuse(profile_id, ReuseDefaults {
            allow_remix:       event.allow_remix.unwrap_or(true),
            allow_sound_reuse: event.allow_sound_reuse.unwrap_or(true),
        });
    }
    if tab {
        return match event.post_window.as_deref() {
            Some("all") => Outcome::Window(profile_id, None),
            Some("six_months") => Outcome::Window(profile_id, Some(183)),
            Some("one_month") => Outcome::Window(profile_id, Some(30)),
            Some("three_days") => Outcome::Window(profile_id, Some(3)),
            other => Outcome::Poison(format!("post_window {other:?}")),
        };
    }
    let city = match event.precision.as_deref() {
        Some("city") => true,
        Some("precise") => false,
        other => return Outcome::Poison(format!("precision {other:?}")),
    };
    let Some(ghost) = event.ghost else {
        return Outcome::Poison("no ghost flag".into());
    };
    Outcome::Record(profile_id, LocationSharing { ghost, city })
}

/// Runs the authors' settings projection consumer on the shared at-least-once
/// runner.
///
/// Consumes `profile.v1.events` and upserts `profile_id → sharing` on each
/// `ProfileLocationSettingsChanged` (#657) and `profile_id → post window` on
/// each `ProfileTabSettingsChanged` (#664); everything else commits as a no-op.
/// The upserts are last-writer-wins (per-profile order from the topic key), so
/// redelivery is harmless. The reads apply them for anyone but the author.
pub async fn run_author_location_consumer(
    consumer: KafkaConsumerHandle,
    store: Arc<dyn AuthorLocationStore>,
    windows: Arc<dyn AuthorWindowStore>,
    reuse: Arc<dyn ReuseRegistry>,
    producer: KafkaProducerHandle,
) {
    info!("post author-location consumer started");

    let policy = RetryPolicy::default();
    let result = run_consumer::<ProfileV1Event, _>(&consumer, &producer, &policy, move |event| {
        let (store, windows, reuse) = (Arc::clone(&store), Arc::clone(&windows), Arc::clone(&reuse));
        Box::pin(async move { process_event(store.as_ref(), windows.as_ref(), reuse.as_ref(), event).await })
    })
    .await;

    if let Err(e) = result {
        error!(error = %e, "post author-location consumer stopped");
    }
}

async fn process_event(
    store: &dyn AuthorLocationStore,
    windows: &dyn AuthorWindowStore,
    reuse: &dyn ReuseRegistry,
    event: &ProfileV1Event,
) -> ProcessOutcome {
    let written = match outcome(event) {
        Outcome::Skip => return ProcessOutcome::Done,
        Outcome::Poison(reason) => return ProcessOutcome::Reject(reason),
        Outcome::Record(profile_id, sharing) => store.set(&profile_id, sharing).await,
        Outcome::Window(profile_id, days) => windows.set(&profile_id, days).await,
        Outcome::Reuse(profile_id, defaults) => reuse.set_defaults(&profile_id, defaults).await,
    };
    match written {
        Ok(())                     => ProcessOutcome::Done,
        Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
        Err(e)                     => ProcessOutcome::Reject(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use profile::infrastructure::publisher::wire::ProfileEventWire;
    use uuid::Uuid;

    use super::*;

    /// Serialized with profile's own wire type, read back as the consumer does.
    fn wire(event: ProfileEventWire) -> ProfileV1Event {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn location_settings_are_read_from_profiles_own_wire() {
        let id = Uuid::now_v7().to_string();
        let event = wire(ProfileEventWire::ProfileLocationSettingsChanged {
            profile_id: id.clone(),
            ghost: true,
            precision: "city".into(),
            occurred_at_ms: 1,
        });
        assert_eq!(
            outcome(&event),
            Outcome::Record(
                ProfileId::try_from(id.as_str()).unwrap(),
                LocationSharing { ghost: true, city: true },
            ),
        );

        let other = wire(ProfileEventWire::ProfileUpdated { profile_id: id, occurred_at_ms: 1 });
        assert_eq!(outcome(&other), Outcome::Skip);
    }

    #[test]
    fn the_post_window_is_read_from_profiles_own_wire() {
        let id = Uuid::now_v7().to_string();
        let event = wire(ProfileEventWire::ProfileTabSettingsChanged {
            profile_id: id.clone(),
            post_window: "one_month".into(),
            show_likes: true,
            show_saved: false,
            show_reposts: true,
            show_places: true,
            occurred_at_ms: 1,
        });
        assert_eq!(outcome(&event), Outcome::Window(ProfileId::try_from(id.as_str()).unwrap(), Some(30)));
    }

    #[test]
    fn reuse_defaults_are_read_from_profiles_own_wire() {
        let id = Uuid::now_v7().to_string();
        let event = wire(ProfileEventWire::ProfileInteractionSettingsChanged {
            profile_id: id.clone(),
            comments: "everyone".into(),
            mentions: "everyone".into(),
            messages: "everyone".into(),
            allow_downloads: true,
            show_like_counts: true,
            allow_remix: true,
            allow_sound_reuse: false,
            limit_audience: None,
            limit_until_ms: None,
            occurred_at_ms: 1,
        });
        assert_eq!(
            outcome(&event),
            Outcome::Reuse(
                ProfileId::try_from(id.as_str()).unwrap(),
                ReuseDefaults { allow_remix: true, allow_sound_reuse: false },
            ),
        );
    }

    #[test]
    fn an_unknown_precision_is_poison_not_precise() {
        let json = r#"{"type":"ProfileLocationSettingsChanged","profile_id":"0192f1b4-7c3e-7000-8000-000000000001","ghost":false,"precision":"street"}"#;
        let event: ProfileV1Event = serde_json::from_str(json).unwrap();
        assert!(matches!(outcome(&event), Outcome::Poison(_)));
    }
}

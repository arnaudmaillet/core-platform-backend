//! Inbound consumers — the async command path. Each runs on the shared
//! at-least-once [`run_consumer`](transport::kafka::consumer::run_consumer) runner
//! (manual commit, bounded retry + jitter, DLQ on poison/exhaustion) and is
//! self-spawned + supervised in [`crate::service`].
//!
//! * `process_consumer` — consumes `media.v1.events`, reacting to `AssetUploaded`
//!   by running the Plane B transformation pipeline. (This is the worker role; it
//!   can be scaled as a separate deployment of the same image.)
//! * `moderation_consumer` — consumes `moderation.v1.events`, applying takedowns /
//!   restores to the byte plane.
//! * `account_consumer` — consumes `account.v1.events`: `account_deleted` erases
//!   the account's media (GDPR Art. 17, #777).

pub mod account_consumer;
pub mod moderation_consumer;
pub mod process_consumer;
pub mod transcode_consumer;

pub use account_consumer::run_account_consumer;
pub use moderation_consumer::run_moderation_consumer;
pub use process_consumer::run_process_consumer;
pub use transcode_consumer::run_transcode_consumer;

use crate::error::MediaError;

/// Lets the at-least-once runner classify a failure: delegate to the error's own
/// [`AppError::is_retryable`](error::AppError::is_retryable) verdict — a transient
/// store / screen fault retries (e.g. `ScreenUnavailable` keeps the asset awaiting
/// processing); a decode / domain fault is poison and dead-letters immediately.
impl transport::kafka::consumer::ClassifyError for MediaError {
    fn is_retryable(&self) -> bool {
        <Self as error::AppError>::is_retryable(self)
    }
}

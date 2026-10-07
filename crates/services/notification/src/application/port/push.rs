//! Push delivery ports (#654): who sends a push, what a sender is called, and
//! the fire-and-forget hook every notification write calls.

use std::sync::Arc;

use async_trait::async_trait;

use crate::application::port::NotificationPayload;
use crate::domain::device::Device;
use crate::domain::push_message::PushMessage;
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

/// What the push network said about a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Delivered,
    /// The token is no longer valid (app uninstalled, token rotated): the
    /// device is forgotten.
    TokenGone,
}

/// A push network (APNs).
#[async_trait]
pub trait PushSender: Send + Sync + 'static {
    async fn send(&self, device: &Device, message: &PushMessage) -> Result<PushOutcome, NotificationError>;
}

/// A sender's display name, for the alert. Fail-open: `None` when unknown or
/// unreachable (the alert then names no one).
#[async_trait]
pub trait SenderNames: Send + Sync + 'static {
    async fn display_name(&self, profile: &ProfileId) -> Option<String>;
}

/// Called once per notification written for the first time. Returns at once:
/// a push never delays nor fails the write.
pub trait PushNotifier: Send + Sync + 'static {
    fn notify(&self, notification: Arc<NotificationPayload>);
}

/// Push off (no APNs key): nothing is sent.
pub struct NoPush;

impl PushNotifier for NoPush {
    fn notify(&self, _notification: Arc<NotificationPayload>) {}
}

/// No name lookup: alerts name no one.
pub struct NoSenderNames;

#[async_trait]
impl SenderNames for NoSenderNames {
    async fn display_name(&self, _profile: &ProfileId) -> Option<String> {
        None
    }
}

use async_trait::async_trait;

use crate::application::port::{CodeSender, VerificationChannel};
use crate::error::AuthError;

/// Local development: logs the code instead of sending it
/// (`AUTH_VERIFICATION_SENDER=log`). Never configure it in a deployed env.
pub struct LogCodeSender;

#[async_trait]
impl CodeSender for LogCodeSender {
    async fn send(
        &self,
        channel: VerificationChannel,
        destination: &str,
        code: &str,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        tracing::warn!(channel = channel.as_str(), destination, code, "one-time code (log sender — local only)");
        Ok(())
    }
}

/// No transport configured: codes cannot be sent (FAILED_PRECONDITION).
pub struct UnconfiguredCodeSender;

#[async_trait]
impl CodeSender for UnconfiguredCodeSender {
    async fn send(&self, channel: VerificationChannel, _: &str, _: &str, _: Option<&str>) -> Result<(), AuthError> {
        Err(AuthError::VerificationChannelUnavailable { channel: channel.as_str().to_owned() })
    }
}

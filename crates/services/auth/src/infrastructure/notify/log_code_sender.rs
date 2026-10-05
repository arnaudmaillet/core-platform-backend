use async_trait::async_trait;

use crate::application::port::{CodeSender, MfaChange, VerificationChannel};
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

    async fn send_lockout_notice(
        &self,
        channel: VerificationChannel,
        destination: &str,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        tracing::warn!(channel = channel.as_str(), destination, "lockout notice (log sender — local only)");
        Ok(())
    }

    async fn send_contact_changed_notice(
        &self,
        changed: VerificationChannel,
        email: &str,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        tracing::warn!(changed = changed.as_str(), email, "contact-changed notice (log sender — local only)");
        Ok(())
    }

    async fn send_new_login_notice(
        &self,
        email: &str,
        device: Option<&str>,
        ip: Option<&str>,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        tracing::warn!(email, device, ip, "new sign-in notice (log sender — local only)");
        Ok(())
    }

    async fn send_mfa_changed_notice(
        &self,
        email: &str,
        change: MfaChange,
        _locale: Option<&str>,
    ) -> Result<(), AuthError> {
        tracing::warn!(email, ?change, "two-step sign-in change notice (log sender — local only)");
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

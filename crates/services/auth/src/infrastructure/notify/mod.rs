//! Delivery of one-time codes.

mod code_message;
mod log_code_sender;
pub mod sigv4;
mod smtp_code_sender;
mod sns_code_sender;

use std::sync::Arc;

use async_trait::async_trait;

pub use log_code_sender::{LogCodeSender, UnconfiguredCodeSender};
pub use smtp_code_sender::{SmtpCodeSender, SmtpConfig};
pub use sns_code_sender::{SnsCodeSender, SnsConfig};

use crate::application::port::{CodeSender, VerificationChannel};
use crate::error::AuthError;

/// Routes a code to the sender of its channel (email, SMS); a channel with
/// none is unavailable.
pub struct ChannelCodeSender {
    pub email: Option<Arc<dyn CodeSender>>,
    pub sms:   Option<Arc<dyn CodeSender>>,
}

#[async_trait]
impl CodeSender for ChannelCodeSender {
    async fn send(
        &self,
        channel: VerificationChannel,
        destination: &str,
        code: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        let sender = match channel {
            VerificationChannel::Email => self.email.as_ref(),
            VerificationChannel::Sms => self.sms.as_ref(),
        };
        match sender {
            Some(sender) => sender.send(channel, destination, code, locale).await,
            None => Err(AuthError::VerificationChannelUnavailable { channel: channel.as_str().to_owned() }),
        }
    }

    /// Email only: an SMS notice would cost money on every lockout and could be
    /// triggered at will against any number.
    async fn send_lockout_notice(
        &self,
        channel: VerificationChannel,
        destination: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        match (channel, &self.email) {
            (VerificationChannel::Email, Some(sender)) => sender.send_lockout_notice(channel, destination, locale).await,
            _ => Ok(()),
        }
    }

    async fn send_contact_changed_notice(
        &self,
        changed: VerificationChannel,
        email: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        match &self.email {
            Some(sender) => sender.send_contact_changed_notice(changed, email, locale).await,
            None => Ok(()),
        }
    }

    async fn send_new_login_notice(
        &self,
        email: &str,
        device: Option<&str>,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        match &self.email {
            Some(sender) => sender.send_new_login_notice(email, device, locale).await,
            None => Ok(()),
        }
    }
}

//! Email codes through an SMTP relay — Amazon SES's SMTP interface
//! (`email-smtp.<region>.amazonaws.com:587`, STARTTLS, SMTP credentials of an
//! IAM user with `ses:SendRawEmail`), from a verified sender identity.

use async_trait::async_trait;
use lettre::message::{header::ContentType, Mailbox};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use super::code_message::{code_message, contact_changed_notice_message, lockout_notice_message, new_login_notice_message};
use crate::application::port::{CodeSender, VerificationChannel};
use crate::error::AuthError;

#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host:     String,
    pub port:     u16,
    pub username: String,
    pub password: String,
    /// `"Name <no-reply@example.com>"` or a bare address (a verified SES identity).
    pub from:     String,
    /// How long a code lives, for the message.
    pub code_ttl_minutes: i64,
}

pub struct SmtpCodeSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from:      Mailbox,
    code_ttl_minutes: i64,
}

impl SmtpCodeSender {
    pub fn new(config: SmtpConfig) -> anyhow::Result<Self> {
        let from: Mailbox = config.from.parse().map_err(|e| anyhow::anyhow!("invalid AUTH_SMTP_FROM: {e}"))?;
        let transport = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.host)
            .map_err(|e| anyhow::anyhow!("invalid AUTH_SMTP_HOST: {e}"))?
            .port(config.port)
            .credentials(Credentials::new(config.username, config.password))
            .timeout(Some(std::time::Duration::from_secs(10)))
            .build();
        Ok(Self { transport, from, code_ttl_minutes: config.code_ttl_minutes })
    }
}

#[async_trait]
impl CodeSender for SmtpCodeSender {
    async fn send(
        &self,
        channel: VerificationChannel,
        destination: &str,
        code: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        if channel != VerificationChannel::Email {
            return Err(AuthError::VerificationChannelUnavailable { channel: channel.as_str().to_owned() });
        }
        let (subject, body) = code_message(code, self.code_ttl_minutes, locale);
        self.mail(destination, subject, body).await
    }

    async fn send_lockout_notice(
        &self,
        channel: VerificationChannel,
        destination: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        if channel != VerificationChannel::Email {
            return Ok(());
        }
        let (subject, body) = lockout_notice_message(locale);
        self.mail(destination, subject, body).await
    }

    async fn send_contact_changed_notice(
        &self,
        changed: VerificationChannel,
        email: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        let (subject, body) = contact_changed_notice_message(changed == VerificationChannel::Email, locale);
        self.mail(email, subject, body).await
    }

    async fn send_new_login_notice(
        &self,
        email: &str,
        device: Option<&str>,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        let (subject, body) = new_login_notice_message(device, locale);
        self.mail(email, subject, body).await
    }
}

impl SmtpCodeSender {
    async fn mail(&self, destination: &str, subject: String, body: String) -> Result<(), AuthError> {
        let to: Mailbox = destination.parse().map_err(|_| AuthError::DomainViolation {
            field: "destination".into(),
            message: "not an email address".into(),
        })?;
        let message = Message::builder()
            .from(self.from.clone())
            .to(to)
            .subject(subject)
            .header(ContentType::TEXT_PLAIN)
            .body(body)
            .map_err(|e| {
                tracing::error!(error = %e, "code email could not be built");
                AuthError::VerificationSendFailed
            })?;
        self.transport.send(message).await.map(|_| ()).map_err(|e| {
            tracing::error!(error = %e, "code email not sent");
            AuthError::VerificationSendFailed
        })
    }
}

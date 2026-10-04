//! SMS codes through Amazon SNS (`Publish` to a phone number, transactional),
//! signed with SigV4 and static credentials.

use async_trait::async_trait;
use chrono::Utc;

use super::code_message::sms_message;
use super::sigv4::{authorization, uri_encode, Credentials, SignableRequest};
use crate::application::port::{CodeSender, VerificationChannel};
use crate::error::AuthError;

#[derive(Debug, Clone)]
pub struct SnsConfig {
    pub region:            String,
    pub access_key_id:     String,
    pub secret_access_key: String,
    /// Alphanumeric sender id where the destination country supports it.
    pub sender_id:         Option<String>,
    pub code_ttl_minutes:  i64,
}

pub struct SnsCodeSender {
    http:   reqwest::Client,
    config: SnsConfig,
}

impl SnsCodeSender {
    pub fn new(http: reqwest::Client, config: SnsConfig) -> Self {
        Self { http, config }
    }

    /// The form body of a transactional `Publish` to `phone`.
    fn body(&self, phone: &str, message: &str) -> String {
        let mut params: Vec<(String, String)> = vec![
            ("Action".into(), "Publish".into()),
            ("Version".into(), "2010-03-31".into()),
            ("PhoneNumber".into(), phone.into()),
            ("Message".into(), message.into()),
            ("MessageAttributes.entry.1.Name".into(), "AWS.SNS.SMS.SMSType".into()),
            ("MessageAttributes.entry.1.Value.DataType".into(), "String".into()),
            ("MessageAttributes.entry.1.Value.StringValue".into(), "Transactional".into()),
        ];
        if let Some(sender) = self.config.sender_id.as_deref().filter(|s| !s.is_empty()) {
            params.push(("MessageAttributes.entry.2.Name".into(), "AWS.SNS.SMS.SenderID".into()));
            params.push(("MessageAttributes.entry.2.Value.DataType".into(), "String".into()));
            params.push(("MessageAttributes.entry.2.Value.StringValue".into(), sender.into()));
        }
        params.iter().map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v))).collect::<Vec<_>>().join("&")
    }
}

#[async_trait]
impl CodeSender for SnsCodeSender {
    async fn send(
        &self,
        channel: VerificationChannel,
        destination: &str,
        code: &str,
        locale: Option<&str>,
    ) -> Result<(), AuthError> {
        if channel != VerificationChannel::Sms {
            return Err(AuthError::VerificationChannelUnavailable { channel: channel.as_str().to_owned() });
        }
        let host = format!("sns.{}.amazonaws.com", self.config.region);
        let body = self.body(destination, &sms_message(code, self.config.code_ttl_minutes, locale));
        let content_type = "application/x-www-form-urlencoded; charset=utf-8";
        let amz_date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let auth = authorization(
            &SignableRequest {
                method: "POST",
                host: &host,
                path: "/",
                query: "",
                content_type,
                body: body.as_bytes(),
                amz_date: &amz_date,
                region: &self.config.region,
                service: "sns",
            },
            &Credentials {
                access_key_id: &self.config.access_key_id,
                secret_access_key: &self.config.secret_access_key,
            },
        );
        let response = self
            .http
            .post(format!("https://{host}/"))
            .header("content-type", content_type)
            .header("x-amz-date", &amz_date)
            .header("authorization", auth)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "SNS unreachable");
                AuthError::VerificationSendFailed
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            tracing::error!(%status, detail = %detail.chars().take(300).collect::<String>(), "SNS refused the SMS");
            return Err(AuthError::VerificationSendFailed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_publish_body_is_a_transactional_sms() {
        let sender = SnsCodeSender::new(reqwest::Client::new(), SnsConfig {
            region: "eu-west-3".into(),
            access_key_id: "AKID".into(),
            secret_access_key: "secret".into(),
            sender_id: Some("Core".into()),
            code_ttl_minutes: 10,
        });
        let body = sender.body("+33612345678", "123456 est ton code");
        assert!(body.starts_with("Action=Publish&Version=2010-03-31&PhoneNumber=%2B33612345678"));
        assert!(body.contains("Message=123456%20est%20ton%20code"));
        assert!(body.contains("StringValue=Transactional"));
        assert!(body.contains("AWS.SNS.SMS.SenderID") && body.contains("StringValue=Core"));
    }
}

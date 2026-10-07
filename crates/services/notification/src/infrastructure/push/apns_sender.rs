//! [`PushSender`] over APNs' HTTP/2 provider API, with token-based auth: an
//! ES256 JWT signed with the team's `.p8` key, reused for 50 minutes (APNs
//! refuses one older than an hour, and one refreshed more than every 20).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::json;

use crate::application::port::{PushOutcome, PushSender};
use crate::domain::device::{Device, PushEnvironment};
use crate::domain::push_message::PushMessage;
use crate::error::NotificationError;

const PRODUCTION_URL: &str = "https://api.push.apple.com";
const SANDBOX_URL: &str = "https://api.sandbox.push.apple.com";
/// How long a provider token is reused.
const TOKEN_LIFETIME: Duration = Duration::from_secs(50 * 60);
/// A push undelivered within a day is dropped (a stale "Alice liked…" helps no one).
const EXPIRATION_SECS: i64 = 24 * 3600;

/// `NOTIFICATION_APNS_*`: all four set turns push on.
#[derive(Clone)]
pub struct ApnsConfig {
    /// The `.p8` key's PEM (PKCS#8, P-256).
    pub key_pem: Vec<u8>,
    pub key_id:  String,
    pub team_id: String,
    /// The app's bundle id (`apns-topic`).
    pub topic:   String,
}

impl std::fmt::Debug for ApnsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApnsConfig")
            .field("key_id", &self.key_id)
            .field("team_id", &self.team_id)
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct ProviderClaims<'a> {
    iss: &'a str,
    iat: i64,
}

pub struct ApnsSender {
    http:           reqwest::Client,
    key:            EncodingKey,
    key_id:         String,
    team_id:        String,
    topic:          String,
    token:          Mutex<Option<(String, Instant)>>,
    production_url: String,
    sandbox_url:    String,
}

impl ApnsSender {
    pub fn new(config: ApnsConfig) -> Result<Self, NotificationError> {
        let key = EncodingKey::from_ec_pem(&config.key_pem)
            .map_err(|e| NotificationError::PushDeliveryFailed { reason: format!("APNs key: {e}") })?;
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .http2_prior_knowledge()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| NotificationError::PushDeliveryFailed { reason: format!("APNs client: {e}") })?;
        Ok(Self {
            http,
            key,
            key_id: config.key_id,
            team_id: config.team_id,
            topic: config.topic,
            token: Mutex::new(None),
            production_url: PRODUCTION_URL.to_owned(),
            sandbox_url: SANDBOX_URL.to_owned(),
        })
    }

    /// The provider token, signed anew once [`TOKEN_LIFETIME`] has passed.
    fn provider_token(&self) -> Result<String, NotificationError> {
        let mut cached = self.token.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((token, _)) = cached.as_ref().filter(|(_, at)| at.elapsed() < TOKEN_LIFETIME) {
            return Ok(token.clone());
        }
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());
        let claims = ProviderClaims { iss: &self.team_id, iat: chrono::Utc::now().timestamp() };
        let token = jsonwebtoken::encode(&header, &claims, &self.key)
            .map_err(|e| NotificationError::PushDeliveryFailed { reason: format!("APNs token: {e}") })?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(token)
    }

    fn forget_provider_token(&self) {
        *self.token.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
}

/// The APNs request body: a localized alert, the badge, and the ids the app
/// opens the notification with.
pub fn payload(message: &PushMessage) -> serde_json::Value {
    let mut alert = serde_json::Map::new();
    if let Some(title) = &message.title {
        alert.insert("title".into(), json!(title));
    }
    if let Some(key) = message.title_loc_key {
        alert.insert("title-loc-key".into(), json!(key));
    }
    if let Some(body) = &message.body {
        alert.insert("body".into(), json!(body));
    }
    if let Some(key) = &message.loc_key {
        alert.insert("loc-key".into(), json!(key));
        alert.insert("loc-args".into(), json!(message.loc_args));
    }
    let mut aps = json!({
        "alert": alert,
        "sound": "default",
        "thread-id": message.thread_id,
    });
    if let Some(badge) = message.badge {
        aps["badge"] = json!(badge);
    }
    json!({
        "aps": aps,
        "notification_id": message.notification_id,
        "kind": message.kind,
        "subject_kind": message.subject_kind,
        "subject_id": message.subject_id,
    })
}

/// What an APNs answer means for the device. Only a token APNs calls
/// unregistered (410) or malformed (`BadDeviceToken`) is forgotten: a
/// configuration fault (`DeviceTokenNotForTopic`, a bad provider token) must
/// not wipe every registration.
pub fn classify(status: u16, body: &str) -> Result<PushOutcome, NotificationError> {
    let reason = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("reason").and_then(|r| r.as_str()).map(str::to_owned))
        .unwrap_or_default();
    match (status, reason.as_str()) {
        (200, _) => Ok(PushOutcome::Delivered),
        (410, _) | (400, "BadDeviceToken") => Ok(PushOutcome::TokenGone),
        _ => Err(NotificationError::PushDeliveryFailed { reason: format!("APNs {status} {reason}") }),
    }
}

#[async_trait]
impl PushSender for ApnsSender {
    async fn send(&self, device: &Device, message: &PushMessage) -> Result<PushOutcome, NotificationError> {
        let base = match device.environment {
            PushEnvironment::Production => &self.production_url,
            PushEnvironment::Sandbox => &self.sandbox_url,
        };
        let expiration = chrono::Utc::now().timestamp() + EXPIRATION_SECS;
        let response = self
            .http
            .post(format!("{base}/3/device/{}", device.token))
            .bearer_auth(self.provider_token()?)
            .header("apns-topic", &self.topic)
            .header("apns-push-type", "alert")
            .header("apns-priority", "10")
            .header("apns-expiration", expiration.to_string())
            .header("apns-collapse-id", &message.collapse_id)
            .json(&payload(message))
            .send()
            .await
            .map_err(|e| NotificationError::PushDeliveryFailed { reason: format!("APNs unreachable: {e}") })?;
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        let outcome = classify(status, &body);
        if status == 403 {
            // ExpiredProviderToken / InvalidProviderToken: sign a new one next time.
            self.forget_provider_token();
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{DecodingKey, Validation};
    use p256::ecdsa::SigningKey;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};

    use super::*;
    use crate::domain::push_message::PushSubject;
    use crate::domain::value_object::NotificationKind;

    fn message(badge: Option<u32>) -> PushMessage {
        PushMessage::new(PushSubject {
            notification_id: "n-1".into(),
            kind:            NotificationKind::Comment,
            subject_kind:    "post",
            subject_id:      "p-1".into(),
            sender_count:    1,
            sender_name:     Some("Alice"),
            badge,
        })
    }

    #[test]
    fn the_payload_is_a_localized_alert_with_the_badge_and_ids() {
        let body = payload(&message(Some(7)));
        assert_eq!(body["aps"]["alert"]["loc-key"], "NTF_PUSH_COMMENT");
        assert_eq!(body["aps"]["alert"]["loc-args"], json!(["Alice"]));
        assert_eq!(body["aps"]["badge"], 7);
        assert_eq!(body["aps"]["thread-id"], "p-1");
        assert_eq!((body["notification_id"].as_str(), body["subject_kind"].as_str()), (Some("n-1"), Some("post")));
        assert!(payload(&message(None))["aps"].get("badge").is_none());
        assert!(body["aps"]["alert"].get("title").is_none() && body["aps"]["alert"].get("body").is_none());

        let chat = payload(&PushMessage::chat_message(crate::domain::push_message::ChatMessage {
            message_id:      "m-1",
            conversation_id: "c-1",
            media:           false,
            preview:         "salut",
            sender_name:     Some("Alice"),
        }));
        assert_eq!(chat["aps"]["alert"], json!({ "title": "Alice", "body": "salut" }));
        assert_eq!((chat["kind"].as_str(), chat["subject_id"].as_str()), (Some("message"), Some("c-1")));
        assert!(chat["aps"].get("badge").is_none());
    }

    #[test]
    fn only_a_dead_token_is_forgotten() {
        assert_eq!(classify(200, "").unwrap(), PushOutcome::Delivered);
        assert_eq!(classify(410, r#"{"reason":"Unregistered"}"#).unwrap(), PushOutcome::TokenGone);
        assert_eq!(classify(400, r#"{"reason":"BadDeviceToken"}"#).unwrap(), PushOutcome::TokenGone);
        for (status, body) in [(400, r#"{"reason":"DeviceTokenNotForTopic"}"#), (403, r#"{"reason":"ExpiredProviderToken"}"#), (503, "")] {
            assert!(classify(status, body).is_err(), "{status} {body}");
        }
    }

    #[test]
    fn the_provider_token_is_es256_signed_for_the_team_and_reused() {
        let signing = SigningKey::random(&mut rand_core::OsRng);
        let key_pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes().to_vec();
        let public_pem = signing.verifying_key().to_public_key_pem(LineEnding::LF).unwrap();
        let sender = ApnsSender::new(ApnsConfig {
            key_pem,
            key_id:  "KEY123".into(),
            team_id: "TEAM456".into(),
            topic:   "app.wynn".into(),
        })
        .unwrap();

        let token = sender.provider_token().unwrap();
        let header = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!((header.alg, header.kid.as_deref()), (Algorithm::ES256, Some("KEY123")));
        let mut validation = Validation::new(Algorithm::ES256);
        validation.required_spec_claims.clear();
        validation.validate_exp = false;
        let claims = jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &DecodingKey::from_ec_pem(public_pem.as_bytes()).unwrap(),
            &validation,
        )
        .unwrap()
        .claims;
        assert_eq!(claims["iss"], "TEAM456");
        assert_eq!(sender.provider_token().unwrap(), token, "reused");
        sender.forget_provider_token();
        assert!(sender.token.lock().unwrap().is_none());
    }
}

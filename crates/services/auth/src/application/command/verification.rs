//! One-time codes (passwordless email sign-up and sign-in, guest mode B4b).
//!
//! `StartVerification` sends a 6-digit code to an address; `SignUp` / `Login`
//! present it back with its challenge id. Codes are stored hashed, expire, allow
//! a handful of attempts, and are single use. The answer to StartVerification is
//! the same whether or not the address has an account: what the address leads
//! to (a new account, an existing one, another sign-in method) is only told to
//! whoever enters the code — someone who controls the address.

use std::sync::Arc;

use chrono::Duration;
use rand::Rng;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use validate_core::{FieldViolation, Validate};

use crate::application::ensure_valid;
use crate::application::port::{
    CodeSender, ConsumeOutcome, PendingChallenge, SendAdmission, SendLimits, VerificationChannel,
    VerificationStore, VerifiedDestination,
};
use crate::error::AuthError;

/// Code lifetime, attempts, the per-address send budget, and the per-address
/// failure ceiling.
///
/// The ceiling bounds online guessing against one address: with 5 sends an hour
/// and 5 tries a code, an attacker would otherwise get 25 guesses an hour on a
/// million codes, every hour. After `max_failures` wrong codes within
/// `failure_window` (across challenges), the address gets no new code and even
/// a right one is refused until the window ends.
#[derive(Debug, Clone)]
pub struct VerificationPolicy {
    pub ttl:            Duration,
    pub max_attempts:   u32,
    pub per_hour:       u32,
    pub per_day:        u32,
    pub resend:         Duration,
    pub max_failures:   u32,
    pub failure_window: Duration,
}

impl Default for VerificationPolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::minutes(10),
            max_attempts: 5,
            per_hour: 5,
            per_day: 20,
            resend: Duration::seconds(30),
            max_failures: 15,
            failure_window: Duration::hours(24),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartVerificationCommand {
    pub channel:     VerificationChannel,
    pub destination: String,
    pub locale:      Option<String>,
}

impl Validate for StartVerificationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.channel == VerificationChannel::Email && normalize_email(&self.destination).is_none() {
            return Err(vec![FieldViolation::new(
                "destination",
                "AUT-VAL-040",
                "destination must be an email address",
            )]);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedVerification {
    pub challenge_id:      String,
    pub expires_in_secs:   i64,
    pub resend_after_secs: i64,
}

/// The address an email code is sent to, normalized (trimmed, lower-cased), or
/// `None` when it is not one.
pub fn normalize_email(raw: &str) -> Option<String> {
    let email = raw.trim().to_lowercase();
    if email.len() > 254 || email.chars().any(char::is_whitespace) {
        return None;
    }
    let (local, domain) = email.split_once('@')?;
    let valid = !local.is_empty()
        && !domain.contains('@')
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.');
    valid.then_some(email)
}

/// `SHA-256(challenge_id ":" code)` hex: what is stored and compared.
pub fn code_hash(challenge_id: &str, code: &str) -> String {
    Sha256::digest(format!("{challenge_id}:{}", code.trim()).as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The send-budget key of an address: its hash (no address in key names).
fn destination_key(channel: VerificationChannel, destination: &str) -> String {
    Sha256::digest(format!("{}:{destination}", channel.as_str()).as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Sends and checks one-time codes.
pub struct VerificationCodes {
    store:  Arc<dyn VerificationStore>,
    sender: Arc<dyn CodeSender>,
    policy: VerificationPolicy,
}

impl VerificationCodes {
    pub fn new(store: Arc<dyn VerificationStore>, sender: Arc<dyn CodeSender>, policy: VerificationPolicy) -> Self {
        Self { store, sender, policy }
    }

    pub async fn start(&self, cmd: StartVerificationCommand) -> Result<StartedVerification, AuthError> {
        ensure_valid(&cmd)?;
        let destination = match cmd.channel {
            VerificationChannel::Email => normalize_email(&cmd.destination).ok_or_else(|| {
                AuthError::DomainViolation { field: "destination".into(), message: "not an email address".into() }
            })?,
            // Phone accounts need `account` to make the email optional first.
            VerificationChannel::Sms => {
                return Err(AuthError::VerificationChannelUnavailable { channel: "sms".into() });
            }
        };

        let key = destination_key(cmd.channel, &destination);
        // A locked address gets no new code until its failure window ends.
        let (failures, window_left) = self.store.failures(&key).await?;
        if failures >= self.policy.max_failures {
            return Err(AuthError::VerificationRateLimited { retry_after_secs: window_left.max(1) });
        }
        let limits =
            SendLimits { per_hour: self.policy.per_hour, per_day: self.policy.per_day, resend: self.policy.resend };
        match self.store.admit_send(&key, limits).await? {
            SendAdmission::Allowed => {}
            SendAdmission::Refused { retry_after_secs } => {
                return Err(AuthError::VerificationRateLimited { retry_after_secs });
            }
        }

        let challenge_id = Uuid::now_v7().to_string();
        let code = format!("{:06}", rand::rng().random_range(0..1_000_000u32));
        let challenge = PendingChallenge {
            challenge_id: challenge_id.clone(),
            channel: cmd.channel,
            destination: destination.clone(),
            destination_key: key,
            code_hash: code_hash(&challenge_id, &code),
        };
        self.store.save(&challenge, self.policy.ttl, self.policy.max_attempts).await?;
        if let Err(e) = self.sender.send(cmd.channel, &destination, &code, cmd.locale.as_deref()).await {
            let _ = self.store.discard(&challenge_id).await;
            return Err(e);
        }

        Ok(StartedVerification {
            challenge_id,
            expires_in_secs: self.policy.ttl.num_seconds(),
            resend_after_secs: self.policy.resend.num_seconds(),
        })
    }

    /// The address a code proves, consuming the challenge. Any failure is the
    /// same [`AuthError::VerificationCodeInvalid`]; a wrong code counts against
    /// the address, and a locked address is refused even with the right code.
    pub async fn verify(&self, challenge_id: &str, code: &str) -> Result<VerifiedDestination, AuthError> {
        if challenge_id.trim().is_empty() || code.trim().is_empty() {
            return Err(AuthError::VerificationCodeInvalid);
        }
        match self.store.consume(challenge_id.trim(), &code_hash(challenge_id.trim(), code)).await? {
            ConsumeOutcome::Verified { destination, destination_key } => {
                let (failures, _) = self.store.failures(&destination_key).await?;
                if failures >= self.policy.max_failures {
                    tracing::warn!("a right code for a locked address was refused");
                    return Err(AuthError::VerificationCodeInvalid);
                }
                Ok(destination)
            }
            ConsumeOutcome::Miss { destination_key } => {
                let failures = self.store.record_failure(&destination_key, self.policy.failure_window).await?;
                if failures == self.policy.max_failures {
                    tracing::warn!("an address reached its code failure ceiling and is locked");
                }
                Err(AuthError::VerificationCodeInvalid)
            }
            ConsumeOutcome::Unknown => Err(AuthError::VerificationCodeInvalid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{InMemoryVerificationStore, RecordingCodeSender};

    fn codes(sender: Arc<RecordingCodeSender>, store: Arc<InMemoryVerificationStore>) -> VerificationCodes {
        VerificationCodes::new(store, sender, VerificationPolicy { per_hour: 2, max_failures: 7, ..VerificationPolicy::default() })
    }

    fn email(destination: &str) -> StartVerificationCommand {
        StartVerificationCommand {
            channel: VerificationChannel::Email,
            destination: destination.into(),
            locale: Some("fr-FR".into()),
        }
    }

    #[test]
    fn addresses_normalize_or_are_refused() {
        assert_eq!(normalize_email("  Ada@Example.COM "), Some("ada@example.com".into()));
        for bad in ["", "ada", "ada@", "@example.com", "ada@example", "a b@example.com", "a@b@c.com", "ada@.com"] {
            assert_eq!(normalize_email(bad), None, "{bad}");
        }
    }

    #[tokio::test]
    async fn a_code_is_sent_then_proves_the_address_once() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = codes(Arc::clone(&sender), Arc::clone(&store));

        let started = codes.start(email(" Ada@Example.com")).await.unwrap();
        assert_eq!(started.expires_in_secs, 600);
        let (to, code, locale) = sender.last().unwrap();
        assert_eq!(to, "ada@example.com");
        assert_eq!(code.len(), 6);
        assert_eq!(locale.as_deref(), Some("fr-FR"));

        assert!(matches!(codes.verify(&started.challenge_id, "000000x").await, Err(AuthError::VerificationCodeInvalid)));
        let proven = codes.verify(&started.challenge_id, &code).await.unwrap();
        assert_eq!(proven.destination, "ada@example.com");
        // Single use.
        assert!(matches!(codes.verify(&started.challenge_id, &code).await, Err(AuthError::VerificationCodeInvalid)));
    }

    #[tokio::test]
    async fn wrong_codes_burn_the_challenge() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = codes(Arc::clone(&sender), store);
        let started = codes.start(email("bob@example.com")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let wrong = if code == "000000" { "111111" } else { "000000" };
        for _ in 0..5 {
            assert!(codes.verify(&started.challenge_id, wrong).await.is_err());
        }
        assert!(matches!(codes.verify(&started.challenge_id, &code).await, Err(AuthError::VerificationCodeInvalid)));
    }

    #[tokio::test]
    async fn sends_are_budgeted_per_address_sms_is_not_available_failures_leave_nothing() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = codes(Arc::clone(&sender), Arc::clone(&store));
        codes.start(email("eve@example.com")).await.unwrap();
        codes.start(email("EVE@example.com")).await.unwrap();
        assert!(matches!(
            codes.start(email("eve@example.com")).await,
            Err(AuthError::VerificationRateLimited { .. })
        ));
        assert!(codes.start(email("other@example.com")).await.is_ok(), "another address has its own budget");

        let sms = StartVerificationCommand { channel: VerificationChannel::Sms, destination: "+33600000000".into(), locale: None };
        assert!(matches!(codes.start(sms).await, Err(AuthError::VerificationChannelUnavailable { .. })));

        sender.fail();
        let before = store.len();
        assert!(matches!(codes.start(email("zed@example.com")).await, Err(AuthError::VerificationSendFailed)));
        assert_eq!(store.len(), before, "an unsent code is not kept");
    }

    #[tokio::test]
    async fn too_many_wrong_codes_lock_the_address_even_for_a_right_code() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = VerificationCodes::new(
            Arc::clone(&store) as _,
            Arc::clone(&sender) as _,
            VerificationPolicy { per_hour: 10, max_failures: 7, ..VerificationPolicy::default() },
        );
        // 5 wrong on the first challenge, 2 on the second: 7 = the ceiling.
        let first = codes.start(email("tim@example.com")).await.unwrap();
        let (_, code1, _) = sender.last().unwrap();
        let wrong = |c: &str| if c == "000000" { "111111".to_owned() } else { "000000".to_owned() };
        for _ in 0..5 {
            let _ = codes.verify(&first.challenge_id, &wrong(&code1)).await;
        }
        let second = codes.start(email("tim@example.com")).await.unwrap();
        let (_, code2, _) = sender.last().unwrap();
        for _ in 0..2 {
            let _ = codes.verify(&second.challenge_id, &wrong(&code2)).await;
        }
        // The right code is refused now, and no new code is sent.
        assert!(matches!(codes.verify(&second.challenge_id, &code2).await, Err(AuthError::VerificationCodeInvalid)));
        assert!(matches!(
            codes.start(email("tim@example.com")).await,
            Err(AuthError::VerificationRateLimited { .. })
        ));
        // Another address is unaffected.
        assert!(codes.start(email("una@example.com")).await.is_ok());
    }
}

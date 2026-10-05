//! One-time codes (passwordless email sign-up and sign-in, guest mode B4b).
//!
//! `StartVerification` sends a 6-digit code to an address; `SignUp` / `Login`
//! present it back with its challenge id. Codes are stored hashed, expire, allow
//! a handful of attempts, and are single use. The answer to StartVerification is
//! the same whether or not the address has an account: what the address leads
//! to (a new account, an existing one, another sign-in method) is only told to
//! whoever enters the code — someone who controls the address.
//!
//! SMS costs money per message and is the toll-fraud vector (SMS pumping), so
//! it has two more gates: the number must be a valid mobile number of a country
//! on the SMS allow-list, and every SMS counts against a global daily budget.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::Duration;
use rand::Rng;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use validate_core::{FieldViolation, Validate};

use crate::application::ensure_valid;
use crate::application::port::{
    CodeSender, ConsumeOutcome, PendingChallenge, SendAdmission, SendLimits, SmsBudget, SmsReservation,
    VerificationChannel, VerificationStore, VerifiedDestination,
};
use crate::error::AuthError;

/// Code lifetime, attempts, the per-address send budget, and the failure
/// ceilings.
///
/// The ceilings bound online guessing against one address: with 5 sends an hour
/// and 5 tries a code, an attacker would otherwise get 25 guesses an hour on a
/// million codes, every hour. Wrong codes count per address **and client IP**:
/// after `max_failures_per_ip` within `failure_window` (across challenges), that
/// IP gets no new code for the address and even a right one is refused — the
/// guesser locks itself out, not the owner on another network. Only
/// `max_failures` wrong codes for the address from anywhere (a distributed
/// attack) lock it for everyone.
///
/// SMS also needs the number's country in `sms_countries` (ISO 3166-1 alpha-2)
/// and room in two budgets per UTC day: `sms_country_daily_budget` for that
/// country, then `sms_daily_budget` for the whole service. The country budget
/// is checked first, so pumping one prefix exhausts that country only.
#[derive(Debug, Clone)]
pub struct VerificationPolicy {
    pub ttl:              Duration,
    pub max_attempts:     u32,
    pub per_hour:         u32,
    pub per_day:          u32,
    pub resend:           Duration,
    pub max_failures:     u32,
    pub max_failures_per_ip: u32,
    pub failure_window:   Duration,
    pub sms_countries:    BTreeSet<String>,
    pub sms_daily_budget: u32,
    pub sms_country_daily_budget: u32,
}

/// Where SMS codes may go: the launch markets. It MUST equal the SNS protect
/// configuration's allow-list (core-platform-infra `global/messaging/sms`,
/// `allowed_countries`), or a code is "sent" to a number SNS silently drops.
/// US / CA wait for a registered toll-free or 10DLC number.
pub const SMS_LAUNCH_COUNTRIES: [&str; 37] = [
    // EU 27
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU",
    "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK",
    // EEA (non-EU), the UK, Switzerland
    "IS", "LI", "NO", "GB", "CH",
    // French overseas departments
    "GP", "GF", "MQ", "RE", "YT",
];

/// The default SMS budget per UTC day, for the whole service. Sized against the
/// SNS monthly spend limit (≈ limit / 30 / price per SMS): exhausting SNS's
/// limit would stop every SMS until the month ends.
pub const DEFAULT_SMS_DAILY_BUDGET: u32 = 50;

/// The default SMS budget per UTC day for one destination country: half the
/// service's, so one attacked prefix leaves the other countries their SMS.
pub const DEFAULT_SMS_COUNTRY_DAILY_BUDGET: u32 = 25;

impl Default for VerificationPolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::minutes(10),
            max_attempts: 5,
            per_hour: 5,
            per_day: 20,
            resend: Duration::seconds(30),
            max_failures: 50,
            max_failures_per_ip: 15,
            failure_window: Duration::hours(24),
            sms_countries: SMS_LAUNCH_COUNTRIES.iter().map(|c| (*c).to_owned()).collect(),
            sms_daily_budget: DEFAULT_SMS_DAILY_BUDGET,
            sms_country_daily_budget: DEFAULT_SMS_COUNTRY_DAILY_BUDGET,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartVerificationCommand {
    pub channel:     VerificationChannel,
    pub destination: String,
    pub locale:      Option<String>,
    /// The caller's address as the transport saw it (never a request field);
    /// `None` when unknown.
    pub client_ip:   Option<String>,
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
        if self.channel == VerificationChannel::Sms && normalize_phone(&self.destination).is_none() {
            return Err(vec![FieldViolation::new(
                "destination",
                "AUT-VAL-041",
                "destination must be an international phone number (+ and country code)",
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

/// A phone number in E.164 (`+` then 8–15 digits), from what people type
/// (spaces, dashes, dots and parentheses dropped; a leading `00` read as `+`),
/// or `None`.
pub fn normalize_phone(raw: &str) -> Option<String> {
    let compact: String = raw.chars().filter(|c| !matches!(c, ' ' | '-' | '.' | '(' | ')')).collect();
    let digits = compact.strip_prefix('+').or_else(|| compact.strip_prefix("00"))?;
    let valid = (8..=15).contains(&digits.len())
        && digits.chars().all(|c| c.is_ascii_digit())
        && !digits.starts_with('0');
    valid.then(|| format!("+{digits}"))
}

/// The country (ISO 3166-1 alpha-2) of an E.164 number that can receive an SMS
/// — a valid mobile number (or one the numbering plan cannot tell from a fixed
/// line) — or `None` (invalid, fixed line, premium rate, shared cost, VoIP…).
/// Territories that share a calling code resolve to their own country (Jersey
/// is `JE`, not `GB`; Mayotte is `YT`), as SNS resolves them.
pub fn sms_country(e164: &str) -> Option<String> {
    use phonenumber::{metadata::DATABASE, Type};
    let number = phonenumber::parse(None, e164).ok()?;
    if !number.is_valid() || !matches!(number.number_type(&DATABASE), Type::Mobile | Type::FixedLineOrMobile) {
        return None;
    }
    number.country().id().map(|id| id.as_ref().to_owned())
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

/// The failure-count key of an address seen from one client IP (hashed; an
/// unknown IP is its own bucket).
fn ip_failure_key(destination_key: &str, client_ip: Option<&str>) -> String {
    let ip: String = Sha256::digest(client_ip.unwrap_or("unknown").as_bytes())
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{destination_key}:{ip}")
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
        let (destination, sms_country) = match cmd.channel {
            VerificationChannel::Email => (
                normalize_email(&cmd.destination).ok_or_else(|| AuthError::DomainViolation {
                    field: "destination".into(),
                    message: "not an email address".into(),
                })?,
                None,
            ),
            VerificationChannel::Sms => {
                let number = normalize_phone(&cmd.destination).ok_or_else(|| AuthError::DomainViolation {
                    field: "destination".into(),
                    message: "not a phone number".into(),
                })?;
                // Only from the number's format: telling it apart enumerates nothing.
                match sms_country(&number) {
                    Some(country) if self.policy.sms_countries.contains(&country) => (number, Some(country)),
                    _ => return Err(AuthError::SmsDestinationNotSupported),
                }
            }
        };

        let key = destination_key(cmd.channel, &destination);
        // A locked address (from everywhere, or from this IP) gets no new code
        // until its failure window ends.
        if let Some(retry_after_secs) = self.locked(&key, cmd.client_ip.as_deref()).await? {
            return Err(AuthError::VerificationRateLimited { retry_after_secs });
        }
        let limits =
            SendLimits { per_hour: self.policy.per_hour, per_day: self.policy.per_day, resend: self.policy.resend };
        match self.store.admit_send(&key, limits).await? {
            SendAdmission::Allowed => {}
            SendAdmission::Refused { retry_after_secs } => {
                return Err(AuthError::VerificationRateLimited { retry_after_secs });
            }
        }
        // Last gate, so only an SMS that would go out spends the budget.
        if let Some(country) = &sms_country {
            let budget =
                SmsBudget { daily: self.policy.sms_daily_budget, country_daily: self.policy.sms_country_daily_budget };
            match self.store.reserve_sms(country, budget).await? {
                SmsReservation::Reserved => {}
                SmsReservation::CountryExhausted => {
                    tracing::error!(
                        country = country.as_str(),
                        budget = budget.country_daily,
                        "the daily SMS budget of a country is spent: no SMS code there until the next UTC day \
                         (possible SMS pumping)"
                    );
                    return Err(AuthError::SmsBudgetExhausted);
                }
                SmsReservation::Exhausted => {
                    tracing::error!(
                        budget = budget.daily,
                        "the daily SMS budget is spent: no SMS code until the next UTC day (possible SMS pumping)"
                    );
                    return Err(AuthError::SmsBudgetExhausted);
                }
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
            // An SMS that did not go out costs nothing: give its budget back.
            if let Some(country) = &sms_country {
                let _ = self.store.refund_sms(country).await;
            }
            return Err(e);
        }

        Ok(StartedVerification {
            challenge_id,
            expires_in_secs: self.policy.ttl.num_seconds(),
            resend_after_secs: self.policy.resend.num_seconds(),
        })
    }

    /// Seconds until `key` is unlocked for `client_ip`, if it is locked: for
    /// everyone (`max_failures`), or for that IP (`max_failures_per_ip`).
    async fn locked(&self, key: &str, client_ip: Option<&str>) -> Result<Option<i64>, AuthError> {
        let (failures, window_left) = self.store.failures(key).await?;
        if failures >= self.policy.max_failures {
            return Ok(Some(window_left.max(1)));
        }
        let (from_ip, window_left) = self.store.failures(&ip_failure_key(key, client_ip)).await?;
        Ok((from_ip >= self.policy.max_failures_per_ip).then_some(window_left.max(1)))
    }

    /// The address a code proves, consuming the challenge. Any failure is the
    /// same [`AuthError::VerificationCodeInvalid`]; a wrong code counts against
    /// the address and against it from `client_ip`, and a locked address is
    /// refused even with the right code.
    pub async fn verify(
        &self,
        challenge_id: &str,
        code: &str,
        client_ip: Option<&str>,
    ) -> Result<VerifiedDestination, AuthError> {
        if challenge_id.trim().is_empty() || code.trim().is_empty() {
            return Err(AuthError::VerificationCodeInvalid);
        }
        match self.store.consume(challenge_id.trim(), &code_hash(challenge_id.trim(), code)).await? {
            ConsumeOutcome::Verified { destination, destination_key } => {
                if self.locked(&destination_key, client_ip).await?.is_some() {
                    tracing::warn!("a right code for a locked address was refused");
                    return Err(AuthError::VerificationCodeInvalid);
                }
                Ok(destination)
            }
            ConsumeOutcome::Miss { destination_key } => {
                let window = self.policy.failure_window;
                let from_ip = self.store.record_failure(&ip_failure_key(&destination_key, client_ip), window).await?;
                let failures = self.store.record_failure(&destination_key, window).await?;
                if from_ip == self.policy.max_failures_per_ip {
                    tracing::warn!("an address reached its code failure ceiling from one IP: locked for that IP");
                }
                if failures == self.policy.max_failures {
                    tracing::warn!("an address reached its code failure ceiling from everywhere: locked for everyone");
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
        VerificationCodes::new(store, sender, VerificationPolicy { per_hour: 2, max_failures: 7, max_failures_per_ip: 7, ..VerificationPolicy::default() })
    }

    fn email(destination: &str) -> StartVerificationCommand {
        StartVerificationCommand {
            channel: VerificationChannel::Email,
            destination: destination.into(),
            locale: Some("fr-FR".into()),
            client_ip: None,
        }
    }

    fn sms(destination: &str) -> StartVerificationCommand {
        StartVerificationCommand { channel: VerificationChannel::Sms, destination: destination.into(), locale: None, client_ip: None }
    }

    #[test]
    fn the_sms_allow_list_is_the_sns_one() {
        // core-platform-infra global/messaging/sms `allowed_countries`, in order.
        let infra = "AT BE BG CY CZ DE DK EE ES FI FR GR HR HU IE IT LT LU LV MT NL PL PT RO SE SI SK \
                     IS LI NO GB CH GP GF MQ RE YT";
        assert_eq!(SMS_LAUNCH_COUNTRIES.join(" "), infra.split_whitespace().collect::<Vec<_>>().join(" "));
        for country in SMS_LAUNCH_COUNTRIES {
            assert!(country.parse::<phonenumber::country::Id>().is_ok(), "{country}");
        }
    }

    #[test]
    fn sms_numbers_resolve_to_their_country_mobiles_only() {
        for (number, country) in [
            ("+33612345678", "FR"),
            ("+447400123456", "GB"),
            ("+491701234567", "DE"),
            ("+41781234567", "CH"),
            ("+262639012345", "YT"),
            ("+262692123456", "RE"),
            ("+590690123456", "GP"),
            ("+447797123456", "JE"),
            ("+447624123456", "IM"),
            ("+12025550123", "US"),
        ] {
            assert_eq!(sms_country(number).as_deref(), Some(country), "{number}");
        }
        // A fixed line, a premium-rate line, a number that does not exist.
        for number in ["+33142685300", "+33899123456", "+33012345678"] {
            assert_eq!(sms_country(number), None, "{number}");
        }
    }

    #[tokio::test]
    async fn sms_goes_only_to_allowed_countries_within_the_daily_budget() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = VerificationCodes::new(
            Arc::clone(&store) as _,
            Arc::clone(&sender) as _,
            VerificationPolicy { sms_daily_budget: 2, sms_country_daily_budget: 2, ..VerificationPolicy::default() },
        );
        // Off the allow-list (the US, Jersey under +44), or not a mobile: refused
        // before anything is counted or sent.
        for number in ["+1 202 555 0123", "+44 7797 123456", "+33 1 42 68 53 00"] {
            assert!(matches!(codes.start(sms(number)).await, Err(AuthError::SmsDestinationNotSupported)), "{number}");
        }
        assert!(sender.last().is_none());

        assert!(codes.start(sms("+33 6 12 34 56 78")).await.is_ok());
        assert!(codes.start(sms("+44 7400 123456")).await.is_ok());
        // The budget is global: a third number is refused, email still goes.
        assert!(matches!(codes.start(sms("+49 170 1234567")).await, Err(AuthError::SmsBudgetExhausted)));
        assert_eq!(sender.last().unwrap().0, "+447400123456");
        assert!(codes.start(email("ada@example.com")).await.is_ok());
    }

    #[tokio::test]
    async fn one_country_spends_its_own_budget_and_a_failed_sms_is_refunded() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = VerificationCodes::new(
            Arc::clone(&store) as _,
            Arc::clone(&sender) as _,
            VerificationPolicy { sms_daily_budget: 10, sms_country_daily_budget: 2, ..VerificationPolicy::default() },
        );
        // A failed send gives its unit back.
        sender.fail();
        assert!(matches!(codes.start(sms("+33 6 12 34 56 01")).await, Err(AuthError::VerificationSendFailed)));
        sender.recover();
        assert!(codes.start(sms("+33 6 12 34 56 02")).await.is_ok());
        assert!(codes.start(sms("+33 6 12 34 56 03")).await.is_ok());
        // France is spent; Germany is not.
        assert!(matches!(codes.start(sms("+33 6 12 34 56 04")).await, Err(AuthError::SmsBudgetExhausted)));
        assert!(codes.start(sms("+49 170 1234567")).await.is_ok());
        assert_eq!(store.sms_sent_today(), 3, "the refused French SMS is not counted against the service");
    }

    #[test]
    fn numbers_normalize_to_e164_or_are_refused() {
        assert_eq!(normalize_phone("+33 6 12 34 56 78"), Some("+33612345678".into()));
        assert_eq!(normalize_phone("0033 (6) 12-34-56-78"), Some("+33612345678".into()));
        for bad in ["", "0612345678", "+33", "+0612345678", "+33 6 12 34 5a 78", "+1234567890123456"] {
            assert_eq!(normalize_phone(bad), None, "{bad}");
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

        assert!(matches!(codes.verify(&started.challenge_id, "000000x", None).await, Err(AuthError::VerificationCodeInvalid)));
        let proven = codes.verify(&started.challenge_id, &code, None).await.unwrap();
        assert_eq!(proven.destination, "ada@example.com");
        // Single use.
        assert!(matches!(codes.verify(&started.challenge_id, &code, None).await, Err(AuthError::VerificationCodeInvalid)));
    }

    #[tokio::test]
    async fn wrong_codes_burn_the_challenge() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = codes(Arc::clone(&sender), store);
        let started = codes.start(email("bob@example.com")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let wrong = if code == "000000" { "111111" } else { "000000" };
        for _ in 0..5 {
            assert!(codes.verify(&started.challenge_id, wrong, None).await.is_err());
        }
        assert!(matches!(codes.verify(&started.challenge_id, &code, None).await, Err(AuthError::VerificationCodeInvalid)));
    }

    #[tokio::test]
    async fn sends_are_budgeted_per_address_sms_goes_out_failures_leave_nothing() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = codes(Arc::clone(&sender), Arc::clone(&store));
        codes.start(email("eve@example.com")).await.unwrap();
        codes.start(email("EVE@example.com")).await.unwrap();
        assert!(matches!(
            codes.start(email("eve@example.com")).await,
            Err(AuthError::VerificationRateLimited { .. })
        ));
        assert!(codes.start(email("other@example.com")).await.is_ok(), "another address has its own budget");

        // SMS goes through its own sender (the recording one takes any channel).
        assert!(codes.start(sms("+33 6 12 34 56 78")).await.is_ok());
        assert_eq!(sender.last().unwrap().0, "+33612345678");

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
            VerificationPolicy { per_hour: 10, max_failures: 7, max_failures_per_ip: 7, ..VerificationPolicy::default() },
        );
        // 5 wrong on the first challenge, 2 on the second: 7 = the ceiling.
        let first = codes.start(email("tim@example.com")).await.unwrap();
        let (_, code1, _) = sender.last().unwrap();
        let wrong = |c: &str| if c == "000000" { "111111".to_owned() } else { "000000".to_owned() };
        for _ in 0..5 {
            let _ = codes.verify(&first.challenge_id, &wrong(&code1), None).await;
        }
        let second = codes.start(email("tim@example.com")).await.unwrap();
        let (_, code2, _) = sender.last().unwrap();
        for _ in 0..2 {
            let _ = codes.verify(&second.challenge_id, &wrong(&code2), None).await;
        }
        // The right code is refused now, and no new code is sent.
        assert!(matches!(codes.verify(&second.challenge_id, &code2, None).await, Err(AuthError::VerificationCodeInvalid)));
        assert!(matches!(
            codes.start(email("tim@example.com")).await,
            Err(AuthError::VerificationRateLimited { .. })
        ));
        // Another address is unaffected.
        assert!(codes.start(email("una@example.com")).await.is_ok());
    }

    #[tokio::test]
    async fn a_guesser_locks_itself_out_not_the_owner_until_the_address_wide_ceiling() {
        let (sender, store) = (Arc::new(RecordingCodeSender::default()), Arc::new(InMemoryVerificationStore::default()));
        let codes = VerificationCodes::new(
            Arc::clone(&store) as _,
            Arc::clone(&sender) as _,
            VerificationPolicy { per_hour: 100, per_day: 100, max_failures: 8, max_failures_per_ip: 3, ..VerificationPolicy::default() },
        );
        let from = |ip: &str| StartVerificationCommand {
            channel: VerificationChannel::Email,
            destination: "owner@example.com".into(),
            locale: None,
            client_ip: Some(ip.into()),
        };
        let wrong = |c: &str| if c == "000000" { "111111".to_owned() } else { "000000".to_owned() };

        // The attacker burns 3 wrong codes from its IP: locked out there.
        let attack = codes.start(from("6.6.6.6")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        for _ in 0..3 {
            let _ = codes.verify(&attack.challenge_id, &wrong(&code), Some("6.6.6.6")).await;
        }
        assert!(matches!(codes.start(from("6.6.6.6")).await, Err(AuthError::VerificationRateLimited { .. })));

        // The owner, on another network, still signs in.
        let owner = codes.start(from("203.0.113.9")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        assert!(codes.verify(&owner.challenge_id, &code, Some("203.0.113.9")).await.is_ok());

        // A distributed attack (8 wrong codes from many IPs) locks it for everyone.
        for n in 0..5 {
            let ip = format!("198.51.100.{n}");
            let c = codes.start(from(&ip)).await.unwrap();
            let (_, code, _) = sender.last().unwrap();
            let _ = codes.verify(&c.challenge_id, &wrong(&code), Some(&ip)).await;
        }
        assert!(matches!(codes.start(from("203.0.113.9")).await, Err(AuthError::VerificationRateLimited { .. })));
    }
}

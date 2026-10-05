//! The holder's two-step sign-in settings (#649): turning it on (a new seed,
//! confirmed by its first code, then backup codes shown once), turning it
//! off, and regenerating the backup codes. Turning it on and regenerating
//! sign the account's other sessions out; every change is emailed to the
//! account's address. The step-up (a recent credential proof) is the edge's.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use super::credentials::caller_session;
use super::mfa::MfaVerifier;
use super::verification::VerificationCodes;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    AccountDirectory, EventPublisher, MfaChange, RefreshTokenRepository, SessionCache, SessionRepository,
};
use crate::domain::value_object::{
    generate_backup_codes, normalize_backup_code, AccountId, RevocationReason, SessionId, SessionStatus, TotpSecret,
};
use crate::error::AuthError;

/// How long a started enrolment waits for its first code.
pub const ENROLMENT_TTL_SECS: u64 = 10 * 60;

/// What the caller acts on: their account and session, from the verified
/// token — never the request.
#[derive(Debug, Clone)]
pub struct MfaCaller {
    pub account_id: String,
    pub session_id: String,
}

/// A started enrolment: what the holder's authenticator app takes.
#[derive(Debug, Clone)]
pub struct StartedMfaEnrolment {
    /// The seed in base32, to type in.
    pub secret: String,
    /// The `otpauth://` URI, for a QR code.
    pub otpauth_uri: String,
    pub expires_in_secs: i64,
}

/// Backup codes, shown once.
#[derive(Clone)]
pub struct BackupCodes {
    pub codes: Vec<String>,
    /// Other sessions signed out.
    pub sessions_revoked: i32,
}

impl std::fmt::Debug for BackupCodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupCodes").field("sessions_revoked", &self.sessions_revoked).finish_non_exhaustive()
    }
}

pub struct MfaSettingsHandler {
    mfa: Arc<MfaVerifier>,
    directory: Arc<dyn AccountDirectory>,
    sessions: Arc<dyn SessionRepository>,
    refresh_tokens: Arc<dyn RefreshTokenRepository>,
    cache: Arc<dyn SessionCache>,
    publisher: Arc<dyn EventPublisher>,
    policy: SessionPolicy,
    /// The email transport for change notices; `None`: none are sent.
    codes: Option<Arc<VerificationCodes>>,
    /// The service's name in the authenticator app.
    issuer: String,
}

impl MfaSettingsHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mfa: Arc<MfaVerifier>,
        directory: Arc<dyn AccountDirectory>,
        sessions: Arc<dyn SessionRepository>,
        refresh_tokens: Arc<dyn RefreshTokenRepository>,
        cache: Arc<dyn SessionCache>,
        publisher: Arc<dyn EventPublisher>,
        policy: SessionPolicy,
        issuer: impl Into<String>,
    ) -> Self {
        Self { mfa, directory, sessions, refresh_tokens, cache, publisher, policy, codes: None, issuer: issuer.into() }
    }

    /// Emails every change to the account's address.
    pub fn with_codes(mut self, codes: Arc<VerificationCodes>) -> Self {
        self.codes = Some(codes);
        self
    }

    async fn caller(&self, caller: &MfaCaller, now: DateTime<Utc>) -> Result<(AccountId, SessionId), AuthError> {
        let account_id = AccountId::try_from(caller.account_id.as_str())?;
        let session_id = SessionId::try_from(caller.session_id.as_str())?;
        caller_session(&self.sessions, &self.cache, &account_id, &session_id, now).await?;
        Ok((account_id, session_id))
    }

    /// A new seed for the holder's authenticator app, kept (sealed) until its
    /// first code confirms it. Starting again replaces it.
    pub async fn start(&self, envelope: Envelope<MfaCaller>, now: DateTime<Utc>) -> Result<StartedMfaEnrolment, AuthError> {
        let (account_id, _) = self.caller(&envelope.payload, now).await?;
        if self.directory.mfa_secret(&account_id).await?.enrolled {
            return Err(AuthError::MfaAlreadyEnabled);
        }
        let seed = TotpSecret::generate();
        let sealed = self.mfa.cipher().seal(seed.as_bytes())?;
        self.mfa.store().save_pending_enrollment(&account_id, &sealed, ENROLMENT_TTL_SECS).await?;
        let label = self.label(&account_id).await;
        Ok(StartedMfaEnrolment {
            secret: seed.base32(),
            otpauth_uri: seed.otpauth_uri(&self.issuer, &label),
            expires_in_secs: ENROLMENT_TTL_SECS as i64,
        })
    }

    /// The enrolment's first code: two-step sign-in is on, the backup codes
    /// are handed out (once), the other sessions end.
    pub async fn confirm(
        &self,
        envelope: Envelope<(MfaCaller, String)>,
        now: DateTime<Utc>,
    ) -> Result<BackupCodes, AuthError> {
        let correlation_id = envelope.correlation_id;
        let (caller, code) = envelope.payload;
        let (account_id, session_id) = self.caller(&caller, now).await?;
        let sealed = self.mfa.store().pending_enrollment(&account_id).await?.ok_or(AuthError::MfaChallengeInvalid)?;
        let seed = TotpSecret::from_bytes(self.mfa.cipher().open(&sealed)?)?;
        self.mfa.check_new_seed(&account_id, &seed, &code, now).await?;

        let (codes, hashes) = self.backup_codes()?;
        self.directory.enroll_mfa(&account_id, &sealed, &hashes).await?;
        self.mfa.store().discard_pending_enrollment(&account_id).await?;
        let sessions_revoked = self.sign_out_others(&account_id, &session_id, now, correlation_id).await?;
        self.notify(account_id, MfaChange::Enabled);
        Ok(BackupCodes { codes, sessions_revoked })
    }

    /// Turns two-step sign-in off.
    pub async fn disable(&self, envelope: Envelope<MfaCaller>, now: DateTime<Utc>) -> Result<(), AuthError> {
        let (account_id, _) = self.caller(&envelope.payload, now).await?;
        self.directory.revoke_mfa(&account_id).await?;
        self.notify(account_id, MfaChange::Disabled);
        Ok(())
    }

    /// A new set of backup codes (the old ones stop working); the other
    /// sessions end.
    pub async fn regenerate(&self, envelope: Envelope<MfaCaller>, now: DateTime<Utc>) -> Result<BackupCodes, AuthError> {
        let correlation_id = envelope.correlation_id;
        let (account_id, session_id) = self.caller(&envelope.payload, now).await?;
        let (codes, hashes) = self.backup_codes()?;
        self.directory.replace_recovery_codes(&account_id, &hashes).await?;
        let sessions_revoked = self.sign_out_others(&account_id, &session_id, now, correlation_id).await?;
        self.notify(account_id, MfaChange::BackupCodesRegenerated);
        Ok(BackupCodes { codes, sessions_revoked })
    }

    /// Fresh backup codes and their hashes.
    fn backup_codes(&self) -> Result<(Vec<String>, Vec<String>), AuthError> {
        let codes = generate_backup_codes();
        let hashes = codes
            .iter()
            .map(|code| {
                let normal = normalize_backup_code(code).expect("generated codes are well-formed");
                self.mfa.cipher().code_hash(&normal)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((codes, hashes))
    }

    /// The holder in their authenticator app's list: their email, else phone.
    async fn label(&self, account_id: &AccountId) -> String {
        match self.directory.contact(account_id).await {
            Ok(contact) => contact.email.or(contact.phone).unwrap_or_else(|| "account".into()),
            Err(_) => "account".into(),
        }
    }

    async fn sign_out_others(
        &self,
        account_id: &AccountId,
        keep: &SessionId,
        now: DateTime<Utc>,
        correlation_id: uuid::Uuid,
    ) -> Result<i32, AuthError> {
        let mut revoked = 0;
        for mut other in self.sessions.list_active_by_account(account_id).await? {
            if other.id() == *keep || other.status() != SessionStatus::Active {
                continue;
            }
            let other_id = other.id();
            other.revoke(now, RevocationReason::MfaChanged, correlation_id)?;
            self.sessions.save(&other).await?;
            self.cache.blacklist_session(&other_id, self.policy.access_ttl).await?;
            self.refresh_tokens.revoke_all_for_session(&other_id).await?;
            for event in &other.drain_events() {
                self.publisher.publish(event).await?;
            }
            revoked += 1;
        }
        Ok(revoked)
    }

    /// Emails the account's address about `change`, in the background (best
    /// effort: the change is done whatever the mail does).
    fn notify(&self, account_id: AccountId, change: MfaChange) {
        let (Some(codes), directory) = (self.codes.clone(), Arc::clone(&self.directory)) else { return };
        tokio::spawn(async move {
            match directory.contact(&account_id).await {
                Ok(contact) => {
                    if let Some(email) = contact.email
                        && let Err(error) = codes.notify_mfa_changed(&email, change, None).await
                    {
                        tracing::warn!(%error, ?change, "two-step change notice not sent");
                    }
                }
                Err(error) => tracing::warn!(%error, "no two-step change notice: contact unreadable"),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{IssuedSession, LoginCommand, LoginOutcome, CompleteLoginCommand};
    use crate::application::command::verification::VerificationPolicy;
    use crate::application::fakes::{t0, Fixture, InMemoryVerificationStore, RecordingCodeSender};
    use crate::application::port::{AuthnGrant, ContactDetails};
    use crate::domain::value_object::{step_of, DeviceFingerprint};
    use uuid::Uuid;

    struct World {
        fx: Fixture,
        sender: Arc<RecordingCodeSender>,
    }

    impl World {
        fn new() -> Self {
            Self { fx: Fixture::new(), sender: Arc::new(RecordingCodeSender::default()) }
        }

        fn handler(&self) -> MfaSettingsHandler {
            let codes = Arc::new(VerificationCodes::new(
                Arc::new(InMemoryVerificationStore::default()),
                Arc::clone(&self.sender) as _,
                VerificationPolicy::default(),
            ));
            MfaSettingsHandler::new(
                Arc::clone(&self.fx.mfa),
                Arc::clone(&self.fx.directory) as _,
                Arc::clone(&self.fx.sessions) as _,
                Arc::clone(&self.fx.refresh_tokens) as _,
                Arc::clone(&self.fx.cache) as _,
                Arc::clone(&self.fx.publisher) as _,
                self.fx.policy.clone(),
                "Core Platform",
            )
            .with_codes(codes)
        }

        async fn login(&self) -> LoginOutcome {
            let cmd = LoginCommand {
                grant: AuthnGrant::Password { username: "user".into(), password: "pw".into() },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            };
            self.fx.login_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap()
        }
    }

    fn on(session: &IssuedSession) -> Envelope<MfaCaller> {
        Envelope::new(
            Uuid::now_v7(),
            MfaCaller { account_id: session.account_id.as_str(), session_id: session.session_id.as_str() },
        )
    }

    fn with_code(session: &IssuedSession, code: String) -> Envelope<(MfaCaller, String)> {
        Envelope::new(Uuid::now_v7(), (on(session).payload, code))
    }

    fn seed_of(started: &StartedMfaEnrolment) -> TotpSecret {
        TotpSecret::from_bytes(data_encoding::BASE32_NOPAD.decode(started.secret.as_bytes()).unwrap()).unwrap()
    }

    /// Turning it on: a seed, its first code, backup codes once, the other
    /// sessions out, the address told — and the next sign-in asks for a code.
    #[tokio::test]
    async fn turning_it_on_takes_the_first_code_and_signs_the_others_out() {
        let w = World::new();
        let here = w.login().await.issued().unwrap();
        let elsewhere = w.login().await.issued().unwrap();
        w.fx.directory.with_contact(here.account_id, ContactDetails { email: Some("me@example.com".into()), phone: None });
        let handler = w.handler();

        let started = handler.start(on(&here), t0()).await.unwrap();
        assert!(started.otpauth_uri.starts_with("otpauth://totp/Core%20Platform:me%40example.com?secret="));
        let seed = seed_of(&started);
        let wrong = handler.confirm(with_code(&here, "000000".into()), t0()).await;
        if seed.code_at(step_of(t0())) != "000000" {
            assert!(matches!(wrong, Err(AuthError::MfaCodeInvalid)), "{wrong:?}");
        }
        let backup = handler.confirm(with_code(&here, seed.code_at(step_of(t0()))), t0()).await.unwrap();
        assert_eq!(backup.codes.len(), 10);
        assert_eq!(backup.sessions_revoked, 1);
        let other = w.fx.sessions.find_by_id(&elsewhere.session_id).await.unwrap().unwrap();
        assert_eq!(other.status(), SessionStatus::Revoked, "the other device is signed out");
        let mine = w.fx.sessions.find_by_id(&here.session_id).await.unwrap().unwrap();
        assert_eq!(mine.status(), SessionStatus::Active, "this one stays");

        // The pending enrolment is gone; starting again is refused.
        assert!(matches!(handler.confirm(with_code(&here, "123456".into()), t0()).await, Err(AuthError::MfaChallengeInvalid)));
        assert!(matches!(handler.start(on(&here), t0()).await, Err(AuthError::MfaAlreadyEnabled)));

        // The next sign-in needs the code — a backup code handed out works.
        let LoginOutcome::SecondFactorRequired(challenge) = w.login().await else { panic!("two-step") };
        let complete = CompleteLoginCommand { mfa_token: challenge.mfa_token, code: backup.codes[3].clone() };
        w.fx.login_handler().complete(Envelope::new(Uuid::now_v7(), complete), t0()).await.expect("a backup code");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(w.sender.mfa_notices(), vec![("me@example.com".to_owned(), MfaChange::Enabled)]);
    }

    #[tokio::test]
    async fn regenerating_replaces_the_codes_and_turning_it_off_tells_the_address() {
        let w = World::new();
        let here = w.login().await.issued().unwrap();
        w.fx.directory.with_contact(here.account_id, ContactDetails { email: Some("me@example.com".into()), phone: None });
        let handler = w.handler();
        assert!(matches!(handler.regenerate(on(&here), t0()).await, Err(AuthError::MfaNotEnabled)));
        assert!(matches!(handler.disable(on(&here), t0()).await, Err(AuthError::MfaNotEnabled)));

        w.fx.enroll_mfa(here.account_id);
        let fresh = handler.regenerate(on(&here), t0()).await.unwrap();
        assert_eq!(w.fx.directory.recovery_codes_left(&here.account_id), 10);
        assert!(!fresh.codes.iter().any(|c| c == "abcde-fghjk"), "new codes");
        handler.disable(on(&here), t0()).await.unwrap();
        assert!(w.login().await.issued().is_some(), "off: one step again");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            w.sender.mfa_notices(),
            vec![
                ("me@example.com".to_owned(), MfaChange::BackupCodesRegenerated),
                ("me@example.com".to_owned(), MfaChange::Disabled),
            ]
        );
    }
}

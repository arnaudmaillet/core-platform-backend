use std::sync::Arc;

use base64::Engine;
use chrono::{DateTime, Utc};
use cqrs::Envelope;
use rand::RngCore;
use validate_core::{FieldViolation, Validate};

use crate::application::command::member_session::MemberSessions;
use crate::application::command::mfa::MfaVerifier;
use crate::application::command::passkeys::{PasskeyAssertion, PasskeySignIn};
use crate::application::command::verification::VerificationCodes;
use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    AccountActivation, AccountDirectory, AccountSnapshot, AuthnGrant, EventPublisher, FederatedTokenVerifier, GuestRegistry,
    IdentityProvider, PendingLogin, ProfileDirectory, RefreshTokenRepository, SessionCache, SessionRepository,
    SubjectLinkRepository, TokenMinter,
};
use crate::domain::aggregate::SubjectLink;
use crate::application::port::VerificationChannel;
use crate::domain::value_object::{
    AccountId, DeviceFingerprint, IdpSubject, EMAIL_CODE_ISSUER, PASSKEY_ISSUER, PHONE_CODE_ISSUER,
};
use crate::error::AuthError;

/// Establish a session by brokering a credential to the IdP, or by verifying a
/// native Sign in with Apple / Google id_token.
#[derive(Debug, Clone)]
pub struct LoginCommand {
    pub grant: AuthnGrant,
    pub device: DeviceFingerprint,
    /// The guest session this device was using: it ends on success.
    pub guest_refresh_token: Option<String>,
    /// The caller's address as the transport saw it — what code lockouts key
    /// on. Never the client-written `device` IP.
    pub client_ip: Option<String>,
}

impl Validate for LoginCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        match &self.grant {
            AuthnGrant::AuthorizationCode { code, redirect_uri, .. } => {
                if code.trim().is_empty() {
                    v.push(FieldViolation::new("code", "AUT-VAL-001", "code must not be empty"));
                }
                if redirect_uri.trim().is_empty() {
                    v.push(FieldViolation::new(
                        "redirect_uri",
                        "AUT-VAL-002",
                        "redirect_uri must not be empty",
                    ));
                }
            }
            AuthnGrant::Password { username, password } => {
                if username.trim().is_empty() {
                    v.push(FieldViolation::new(
                        "username",
                        "AUT-VAL-003",
                        "username must not be empty",
                    ));
                }
                if password.is_empty() {
                    v.push(FieldViolation::new(
                        "password",
                        "AUT-VAL-004",
                        "password must not be empty",
                    ));
                }
            }
            AuthnGrant::IdToken { id_token, nonce, .. } => {
                if id_token.trim().is_empty() {
                    v.push(FieldViolation::new("id_token", "AUT-VAL-030", "id_token must not be empty"));
                }
                if nonce.trim().is_empty() {
                    v.push(FieldViolation::new("nonce", "AUT-VAL-031", "nonce must not be empty"));
                }
            }
            AuthnGrant::Code { challenge_id, code } => {
                if challenge_id.trim().is_empty() || code.trim().is_empty() {
                    v.push(FieldViolation::new(
                        "verification_code",
                        "AUT-VAL-036",
                        "challenge_id and code are required",
                    ));
                }
            }
            AuthnGrant::Passkey(assertion) => {
                if assertion.challenge.trim().is_empty()
                    || assertion.client_data_json.is_empty()
                    || assertion.authenticator_data.is_empty()
                    || assertion.signature.is_empty()
                {
                    v.push(FieldViolation::new(
                        "passkey",
                        "AUT-VAL-042",
                        "a passkey assertion needs its challenge, client data, authenticator data and signature",
                    ));
                }
            }
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

/// The result of a successful login (or refresh). The plaintext refresh token is
/// present exactly once — only its hash is ever persisted.
#[derive(Debug, Clone)]
pub struct IssuedSession {
    pub account_id: AccountId,
    pub session_id: crate::domain::value_object::SessionId,
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_in: i64,
    /// True when this call established the IdP-subject → account link.
    pub first_link: bool,
    /// True when this login resumed an account its holder had deactivated.
    pub reactivated: bool,
}

/// Orchestrates login: authenticate → resolve/link account → gate active → issue
/// session + tokens. Persists durably, then publishes events.
pub struct LoginHandler {
    idp: Arc<dyn IdentityProvider>,
    directory: Arc<dyn AccountDirectory>,
    links: Arc<dyn SubjectLinkRepository>,
    publisher: Arc<dyn EventPublisher>,
    members: MemberSessions,
    /// Native Sign in with Apple / Google; `None` refuses id_token grants.
    federated: Option<Arc<dyn FederatedTokenVerifier>>,
    /// Where a retired guest session's upgrade is recorded.
    guests: Option<Arc<dyn GuestRegistry>>,
    /// Email one-time codes; `None` refuses code grants.
    codes: Option<Arc<VerificationCodes>>,
    /// Two-step sign-in (#649). `None` (no seed key): an account with it on
    /// cannot sign in — fail-closed, never skipped.
    mfa: Option<Arc<MfaVerifier>>,
    /// Passkeys (#808); `None` refuses passkey grants and second steps.
    passkeys: Option<Arc<PasskeySignIn>>,
}

impl LoginHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        idp: Arc<dyn IdentityProvider>,
        directory: Arc<dyn AccountDirectory>,
        profiles: Arc<dyn ProfileDirectory>,
        links: Arc<dyn SubjectLinkRepository>,
        sessions: Arc<dyn SessionRepository>,
        refresh_tokens: Arc<dyn RefreshTokenRepository>,
        cache: Arc<dyn SessionCache>,
        minter: Arc<dyn TokenMinter>,
        publisher: Arc<dyn EventPublisher>,
        policy: SessionPolicy,
    ) -> Self {
        Self {
            idp,
            directory,
            links,
            publisher: Arc::clone(&publisher),
            members: MemberSessions { profiles, sessions, refresh_tokens, cache, minter, publisher, policy },
            federated: None,
            guests: None,
            codes: None,
            mfa: None,
            passkeys: None,
        }
    }

    /// Enables signing in with a passkey: as the first factor (two factors in
    /// one — no second step follows) and as the second step.
    pub fn with_passkeys(mut self, passkeys: Arc<PasskeySignIn>) -> Self {
        self.passkeys = Some(passkeys);
        self
    }

    /// The identity a passkey session stands for: the account's first link
    /// (so a password change or a password step-up works from it), else the
    /// passkey itself.
    async fn passkey_subject(&self, account_id: &AccountId, assertion: &PasskeyAssertion) -> Result<IdpSubject, AuthError> {
        match self.links.find_by_account(account_id).await?.into_iter().next() {
            Some(link) => Ok(link.subject().clone()),
            None => IdpSubject::new(
                PASSKEY_ISSUER,
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&assertion.credential_id),
            ),
        }
    }

    /// Enables the second step for accounts with two-step sign-in on.
    pub fn with_mfa(mut self, mfa: Arc<MfaVerifier>) -> Self {
        self.mfa = Some(mfa);
        self
    }

    /// Is this sign-in from a device the account never used (an account that
    /// did sign in before, #649)? One without a device id counts as new — the
    /// id is client-written — unless the holder's own client sends none (see
    /// [`DeviceHistory::announces`](crate::application::port::DeviceHistory::announces)).
    /// Only with the code transports to announce it and the history readable:
    /// anything else is "no" — an alert never fails or slows a sign-in.
    async fn is_new_device(&self, account_id: &AccountId, device: &DeviceFingerprint) -> bool {
        if self.codes.is_none() {
            return false;
        }
        match self.members.sessions.device_history(account_id, device.device_id()).await {
            Ok(history) => history.announces(device.device_id().is_some()),
            Err(error) => {
                tracing::warn!(%error, "device history unreadable; no new-sign-in alert");
                false
            }
        }
    }

    /// Emails the account's address about the new device (its user agent and
    /// IP), in the background (best effort, off the sign-in's path).
    fn announce_new_device(&self, account_id: AccountId, user_agent: Option<String>, ip: Option<String>) {
        let (Some(codes), directory) = (self.codes.clone(), Arc::clone(&self.directory)) else { return };
        tokio::spawn(async move {
            let email = match directory.contact(&account_id).await {
                Ok(contact) => contact.email,
                Err(error) => {
                    tracing::warn!(%error, "no new-sign-in alert: contact unreadable");
                    return;
                }
            };
            if let Some(email) = email
                && let Err(error) = codes.notify_new_login(&email, user_agent.as_deref(), ip.as_deref(), None).await
            {
                tracing::warn!(%error, "new-sign-in alert not sent");
            }
        });
    }

    /// Enables passwordless sign-in with an email one-time code.
    pub fn with_codes(mut self, codes: Arc<VerificationCodes>) -> Self {
        self.codes = Some(codes);
        self
    }

    /// Enables id_token grants (native Sign in with Apple / Google) and the
    /// guest-session hand-over.
    pub fn with_federated(
        mut self,
        verifier: Arc<dyn FederatedTokenVerifier>,
        guests: Arc<dyn GuestRegistry>,
    ) -> Self {
        self.federated = Some(verifier);
        self.guests = Some(guests);
        self
    }

    pub async fn handle(
        &self,
        envelope: Envelope<LoginCommand>,
        now: DateTime<Utc>,
    ) -> Result<LoginOutcome, AuthError> {
        ensure_valid(&envelope.payload)?;
        let cmd = envelope.payload;
        let correlation_id = envelope.correlation_id;

        // 1–2. Who is signing in, and their account.
        let mut by_passkey = false;
        let (subject, account_id, needs_link) = match cmd.grant {
            // A passkey (#808): the assertion names its account (user handle).
            AuthnGrant::Passkey(assertion) => {
                let passkeys = self.passkeys.as_ref().ok_or(AuthError::PasskeysUnavailable)?;
                let account_id = passkeys.verify(&assertion, None, now).await?;
                by_passkey = true;
                (self.passkey_subject(&account_id, &assertion).await?, account_id, false)
            }
            // A native id_token: verified here. Its account was linked at
            // SignUp; an identity with none is told to sign up.
            AuthnGrant::IdToken { provider, id_token, nonce } => {
                let verifier = self.federated.as_ref().ok_or_else(|| AuthError::FederatedProviderNotConfigured {
                    provider: provider.as_str().to_owned(),
                })?;
                let identity = verifier.verify(provider, &id_token, &nonce).await?;
                let subject = IdpSubject::new(identity.issuer, identity.subject)?;
                let link = self.links.find_by_subject(&subject).await?.ok_or(AuthError::NoAccountForIdentity)?;
                (subject, link.account_id(), false)
            }
            // A one-time code proving the address of a passwordless account.
            AuthnGrant::Code { challenge_id, code } => {
                let codes = self
                    .codes
                    .as_ref()
                    .ok_or_else(|| AuthError::VerificationChannelUnavailable { channel: "email".into() })?;
                let proven = codes.verify(&challenge_id, &code, cmd.client_ip.as_deref()).await?;
                let issuer = match proven.channel {
                    VerificationChannel::Email => EMAIL_CODE_ISSUER,
                    VerificationChannel::Sms => PHONE_CODE_ISSUER,
                };
                let subject = IdpSubject::new(issuer, proven.destination)?;
                let link = self.links.find_by_subject(&subject).await?.ok_or(AuthError::NoAccountForIdentity)?;
                (subject, link.account_id(), false)
            }
            // Broker the credential to the IdP and normalize the identity. The
            // link itself is only established *after* the active gate, so an
            // inactive account never produces a spurious SubjectLinked event.
            grant => {
                let claims = self.idp.authenticate(grant).await?;
                let subject = IdpSubject::new(claims.issuer, claims.subject)?;
                match self.links.find_by_subject(&subject).await? {
                    Some(link) => (subject, link.account_id(), false),
                    None => {
                        let account_id = self.directory.resolve_or_provision(&subject).await?;
                        (subject, account_id, true)
                    }
                }
            }
        };

        // 3. The account must be one that may sign in (a suspended one never).
        let snapshot = self.directory.lookup(&account_id).await?;
        if let AccountActivation::Inactive { reason } = &snapshot.activation {
            return Err(AuthError::AccountNotActive { current: reason.clone() });
        }
        let proven = ProvenSignIn {
            account_id,
            subject,
            needs_link,
            device: cmd.device,
            guest_refresh_token: cmd.guest_refresh_token,
        };

        // 4. Two-step sign-in on: nothing happens until the second factor —
        //    no session, no resumed account, no first link (#649). A passkey
        //    is both factors already (possession + device unlock).
        if snapshot.mfa_enrolled && !by_passkey {
            let mfa = self.mfa.as_ref().ok_or(AuthError::MfaUnavailable)?;
            return self.challenge(mfa, proven, now).await.map(LoginOutcome::SecondFactorRequired);
        }
        self.finish(proven, snapshot, now, correlation_id).await.map(LoginOutcome::Issued)
    }

    /// The second step (#649): the code for the sign-in `mfa_token` names, or
    /// one of the account's passkeys (#808). Wrong codes count against the
    /// account (the challenge stays usable until it expires or the account
    /// locks); the right one issues the session, once.
    pub async fn complete(
        &self,
        envelope: Envelope<CompleteLoginCommand>,
        now: DateTime<Utc>,
    ) -> Result<IssuedSession, AuthError> {
        let cmd = envelope.payload;
        let mfa = self.mfa.as_ref().ok_or(AuthError::MfaUnavailable)?;
        let token_hash = challenge_hash(&cmd.mfa_token);
        let pending = mfa.store().pending_login(&token_hash).await?.ok_or(AuthError::MfaChallengeInvalid)?;
        match &cmd.passkey {
            Some(assertion) => {
                let passkeys = self.passkeys.as_ref().ok_or(AuthError::PasskeysUnavailable)?;
                passkeys.verify(assertion, Some(&pending.account_id), now).await?;
            }
            None => mfa.check(&pending.account_id, &cmd.code, now).await?,
        }
        // Single use: of two completions racing, one issues a session.
        let pending = mfa.store().take_pending_login(&token_hash).await?.ok_or(AuthError::MfaChallengeInvalid)?;

        // The account is re-read: suspended meanwhile, it does not sign in.
        let snapshot = self.directory.lookup(&pending.account_id).await?;
        if let AccountActivation::Inactive { reason } = &snapshot.activation {
            return Err(AuthError::AccountNotActive { current: reason.clone() });
        }
        let proven = ProvenSignIn {
            account_id: pending.account_id,
            subject: IdpSubject::new(pending.issuer, pending.subject)?,
            needs_link: pending.needs_link,
            device: pending.device,
            guest_refresh_token: pending.guest_refresh_token,
        };
        self.finish(proven, snapshot, now, envelope.correlation_id).await
    }

    /// Parks a proven sign-in until its second factor: the challenge token
    /// goes to the client, only its hash is kept.
    async fn challenge(
        &self,
        mfa: &MfaVerifier,
        proven: ProvenSignIn,
        now: DateTime<Utc>,
    ) -> Result<MfaChallenge, AuthError> {
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        let mfa_token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let ttl = mfa.policy().challenge_ttl_secs;
        let pending = PendingLogin {
            account_id: proven.account_id,
            issuer: proven.subject.issuer().to_owned(),
            subject: proven.subject.subject().to_owned(),
            needs_link: proven.needs_link,
            device: proven.device,
            guest_refresh_token: proven.guest_refresh_token,
            started_at: now,
        };
        mfa.store().save_pending_login(&challenge_hash(&mfa_token), &pending, ttl).await?;
        Ok(MfaChallenge { account_id: proven.account_id, mfa_token, expires_in_secs: ttl as i64 })
    }

    /// Everything after the credential (and its second factor, if any): a
    /// deactivated account resumes, a first link is made, the session is
    /// issued, a new device is announced, the guest this device was retires.
    async fn finish(
        &self,
        proven: ProvenSignIn,
        snapshot: AccountSnapshot,
        now: DateTime<Utc>,
        correlation_id: uuid::Uuid,
    ) -> Result<IssuedSession, AuthError> {
        let ProvenSignIn { account_id, subject, needs_link, device, guest_refresh_token } = proven;
        // Signing back in is how a holder undoes their own deactivation: the
        // credential was just proven, so the account resumes (a suspension is
        // never lifted here).
        let age_bracket = snapshot.age_bracket;
        let (permissions, reactivated) = match snapshot.activation {
            AccountActivation::Active => (snapshot.permissions, false),
            AccountActivation::Deactivated => {
                self.directory.resume_deactivated(&account_id).await?;
                (snapshot.permissions, true)
            }
            AccountActivation::Inactive { reason } => {
                return Err(AuthError::AccountNotActive { current: reason });
            }
        };

        // Establish the immutable subject → account link on first login.
        let mut first_link = false;
        if needs_link {
            let mut link = SubjectLink::establish(subject.clone(), account_id, now, correlation_id);
            self.links.save(&link).await?;
            for event in &link.drain_events() {
                self.publisher.publish(event).await?;
            }
            first_link = true;
        }

        // Issue the session, its refresh token and the edge access token —
        // after reading whether this device ever signed in to the account.
        let new_device = self.is_new_device(&account_id, &device).await;
        let (user_agent, ip) = (device.user_agent().map(str::to_owned), device.ip_address().map(str::to_owned));
        let issued = self
            .members
            .issue(account_id, subject, device, permissions, age_bracket, now, correlation_id)
            .await?;
        if new_device {
            self.announce_new_device(account_id, user_agent, ip);
        }

        // The guest this device was is now this member.
        if let (Some(token), Some(guests)) = (guest_refresh_token.as_deref(), &self.guests) {
            self.members.retire_guest(guests.as_ref(), token, account_id, now, correlation_id).await;
        }

        Ok(IssuedSession {
            account_id,
            session_id: issued.session_id,
            access_token: issued.access_token,
            refresh_token: issued.refresh_token,
            access_expires_in: issued.access_expires_in,
            first_link,
            reactivated,
        })
    }
}

/// A credential proven for an account, before what follows from it.
struct ProvenSignIn {
    account_id: AccountId,
    subject: IdpSubject,
    needs_link: bool,
    device: DeviceFingerprint,
    guest_refresh_token: Option<String>,
}

/// What a sign-in comes to.
#[derive(Debug, Clone)]
pub enum LoginOutcome {
    /// Signed in.
    Issued(IssuedSession),
    /// Two-step sign-in is on: the credential is proven, the session waits for
    /// the second factor ([`LoginHandler::complete`]).
    SecondFactorRequired(MfaChallenge),
}

impl LoginOutcome {
    /// The session, when the sign-in issued one.
    pub fn issued(self) -> Option<IssuedSession> {
        match self {
            Self::Issued(session) => Some(session),
            Self::SecondFactorRequired(_) => None,
        }
    }
}

/// A sign-in waiting for its second factor (#649).
#[derive(Debug, Clone)]
pub struct MfaChallenge {
    pub account_id: AccountId,
    /// Opaque, single use; only its hash is stored.
    pub mfa_token: String,
    pub expires_in_secs: i64,
}

/// The second step: the code (TOTP or backup) for a pending sign-in, or a
/// passkey assertion (then `code` is ignored).
#[derive(Clone)]
pub struct CompleteLoginCommand {
    pub mfa_token: String,
    pub code: String,
    pub passkey: Option<PasskeyAssertion>,
}

impl std::fmt::Debug for CompleteLoginCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteLoginCommand").finish_non_exhaustive()
    }
}

fn challenge_hash(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::Generation;
    use uuid::Uuid;

    fn password_login() -> Envelope<LoginCommand> {
        Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: AuthnGrant::Password {
                    username: "user".into(),
                    password: "secret".into(),
                },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            },
        )
    }

    #[tokio::test]
    async fn first_login_links_account_and_issues_tokens() {
        let fx = Fixture::new();
        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();

        assert!(issued.first_link);
        assert!(!issued.access_token.is_empty());
        assert!(!issued.refresh_token.is_empty());
        assert_eq!(issued.access_expires_in, 600); // 10-minute access TTL
        assert_eq!(fx.sessions.count(), 1);
        // SubjectLinked then SessionIssued, in that order.
        assert_eq!(fx.publisher.event_types(), vec!["auth.subject_linked", "auth.session_issued"]);
    }

    #[tokio::test]
    async fn second_login_same_subject_does_not_relink() {
        let fx = Fixture::new();
        let first = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        let second = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();

        assert!(first.first_link);
        assert!(!second.first_link, "subject already linked");
        assert_eq!(first.account_id, second.account_id);
        assert_eq!(fx.sessions.count(), 2);
        // Only one subject_linked across both logins.
        let linked = fx.publisher.event_types().iter().filter(|t| **t == "auth.subject_linked").count();
        assert_eq!(linked, 1);
    }

    #[tokio::test]
    async fn login_rejected_for_inactive_account() {
        let fx = Fixture::new();
        let subject = IdpSubject::new("https://idp.test", "sub-123").unwrap();
        let account = AccountId::from_uuid(Uuid::now_v7());
        fx.directory.with_account(
            &subject,
            account,
            AccountActivation::Inactive { reason: "suspended".into() },
            vec![],
        );

        let err = fx.login_handler().handle(password_login(), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::AccountNotActive { .. }));
        // No session, and crucially no SubjectLinked event for an inactive account.
        assert_eq!(fx.sessions.count(), 0);
        assert_eq!(fx.publisher.count(), 0);
        assert!(fx.directory.resumed().is_empty(), "a suspension is never lifted by login");
    }

    #[tokio::test]
    async fn login_resumes_a_self_deactivated_account() {
        let fx = Fixture::new();
        let subject = IdpSubject::new("https://idp.test", "sub-123").unwrap();
        let account = AccountId::from_uuid(Uuid::now_v7());
        fx.directory.with_account(&subject, account, AccountActivation::Deactivated, vec![]);

        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();

        assert!(issued.reactivated, "the app welcomes the holder back");
        assert_eq!(fx.directory.resumed(), vec![account]);
        assert_eq!(fx.sessions.count(), 1);

        // The next login finds it active: nothing more to resume.
        let again = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        assert!(!again.reactivated);
        assert_eq!(fx.directory.resumed(), vec![account]);
    }

    #[tokio::test]
    async fn login_fails_when_the_account_cannot_be_resumed() {
        let fx = Fixture::new();
        let subject = IdpSubject::new("https://idp.test", "sub-123").unwrap();
        let account = AccountId::from_uuid(Uuid::now_v7());
        fx.directory.with_account(&subject, account, AccountActivation::Deactivated, vec![]);
        fx.directory.refuse_resume();

        let err = fx.login_handler().handle(password_login(), t0()).await.unwrap_err();

        assert!(matches!(err, AuthError::AccountNotActive { .. }));
        assert_eq!(fx.sessions.count(), 0);
        assert_eq!(fx.publisher.count(), 0);
    }

    #[tokio::test]
    async fn login_propagates_idp_failure() {
        let mut fx = Fixture::new();
        fx.idp = std::sync::Arc::new(crate::application::fakes::StubIdentityProvider::failing());
        let err = fx.login_handler().handle(password_login(), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::IdpAuthenticationFailed));
    }

    #[tokio::test]
    async fn login_validation_rejects_empty_credential() {
        let fx = Fixture::new();
        let env = Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: AuthnGrant::AuthorizationCode {
                    code: "".into(),
                    redirect_uri: "".into(),
                    code_verifier: "v".into(),
                },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            },
        );
        let err = fx.login_handler().handle(env, t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::Validation(_)));
    }

    #[tokio::test]
    async fn login_mints_the_owned_profiles_into_the_token() {
        use crate::domain::value_object::ProfileId;
        let fx = Fixture::new();
        let subject = IdpSubject::new("https://idp.test", "sub-123").unwrap();
        let account = AccountId::from_uuid(Uuid::now_v7());
        let profiles = vec![ProfileId::from_uuid(Uuid::now_v7()), ProfileId::from_uuid(Uuid::now_v7())];
        fx.directory.with_account(&subject, account, AccountActivation::Active, vec![]);
        fx.profiles.with_profiles(account, profiles.clone());

        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        let claims = fx.minter.verify_access(&issued.access_token).await.unwrap();
        assert_eq!(claims.account_id, account);
        assert_eq!(claims.profile_ids, profiles);
    }

    #[tokio::test]
    async fn login_survives_a_profile_directory_outage_with_no_profile_grants() {
        let mut fx = Fixture::new();
        fx.profiles = std::sync::Arc::new(crate::application::fakes::StubProfileDirectory::failing());

        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        let claims = fx.minter.verify_access(&issued.access_token).await.unwrap();
        assert!(claims.profile_ids.is_empty(), "outage degrades to no profile grants");
        assert_eq!(fx.sessions.count(), 1, "the session is still issued");
    }

    #[tokio::test]
    async fn session_is_issued_under_current_generation() {
        let fx = Fixture::new();
        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        let session = fx.sessions.find_by_id(&issued.session_id).await.unwrap().unwrap();
        assert_eq!(session.generation(), Generation::INITIAL);
    }

    /// #649: a sign-in from a device the account never used is emailed to the
    /// account's address — not the account's very first sign-in, not a known
    /// device. One without a device id is new: the id is client-written.
    #[tokio::test]
    async fn a_sign_in_from_a_new_device_is_emailed_to_the_account() {
        use crate::application::command::verification::{VerificationCodes, VerificationPolicy};
        use crate::application::fakes::{InMemoryVerificationStore, RecordingCodeSender};
        use crate::application::port::ContactDetails;

        let fx = Fixture::new();
        fx.idp.with_password("pw");
        let sender = Arc::new(RecordingCodeSender::default());
        let codes = Arc::new(VerificationCodes::new(
            Arc::new(InMemoryVerificationStore::default()),
            Arc::clone(&sender) as _,
            VerificationPolicy::default(),
        ));
        let handler = fx.login_handler().with_codes(codes);
        let login = |device_id: Option<&str>| {
            Envelope::new(
                Uuid::now_v7(),
                LoginCommand {
                    grant: AuthnGrant::Password { username: "user".into(), password: "pw".into() },
                    device: DeviceFingerprint::new(
                        Some("App/1.0 iPhone".into()),
                        Some("203.0.113.7".into()),
                        device_id.map(str::to_owned),
                    ),
                    guest_refresh_token: None,
                    client_ip: None,
                },
            )
        };
        let settle = || tokio::time::sleep(std::time::Duration::from_millis(50));

        // The very first sign-in: nothing to compare with, no alert.
        let first = handler.handle(login(Some("phone")), t0()).await.unwrap().issued().unwrap();
        fx.directory.with_contact(first.account_id, ContactDetails { email: Some("me@example.com".into()), phone: None });
        settle().await;
        assert!(sender.login_notices().is_empty());

        // The same phone again: nothing to tell.
        handler.handle(login(Some("phone")), t0()).await.unwrap();
        settle().await;
        assert!(sender.login_notices().is_empty());
        let told = || ("me@example.com".to_owned(), Some("App/1.0 iPhone".to_owned()), Some("203.0.113.7".to_owned()));

        // No device id, from a holder whose client sends one: told.
        handler.handle(login(None), t0()).await.unwrap();
        settle().await;
        assert_eq!(sender.login_notices(), vec![told()]);

        // A new device: the account's address is told, naming the device.
        handler.handle(login(Some("laptop")), t0()).await.unwrap();
        settle().await;
        assert_eq!(sender.login_notices(), vec![told(), told()]);
    }

    /// #649: with two-step sign-in on, a proven password issues nothing — no
    /// session, no resumed account — until the code; the challenge works once.
    #[tokio::test]
    async fn two_step_sign_in_waits_for_the_code_then_issues_once() {
        use crate::domain::value_object::step_of;
        let fx = Fixture::new();
        let first = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        let account = first.account_id;
        let seed = fx.enroll_mfa(account);
        fx.directory.set_activation(&account, AccountActivation::Deactivated);
        let sessions_before = fx.sessions.count();

        let LoginOutcome::SecondFactorRequired(challenge) =
            fx.login_handler().handle(password_login(), t0()).await.unwrap()
        else {
            panic!("a second factor is required")
        };
        assert_eq!(challenge.account_id, account);
        assert!(challenge.expires_in_secs > 0 && challenge.mfa_token.len() >= 43);
        assert_eq!(fx.sessions.count(), sessions_before, "no session before the code");
        assert!(fx.directory.resumed().is_empty(), "nothing resumes before the code");

        let complete = |code: String| {
            Envelope::new(Uuid::now_v7(), CompleteLoginCommand { mfa_token: challenge.mfa_token.clone(), code, passkey: None })
        };
        let wrong = fx.login_handler().complete(complete("000000".into()), t0()).await.unwrap_err();
        // (A 1-in-a-million chance the wrong code is right is accepted here.)
        if !matches!(wrong, AuthError::MfaCodeInvalid) {
            assert_eq!(seed.code_at(step_of(t0())), "000000", "{wrong:?}");
        }
        let issued = fx.login_handler().complete(complete(seed.code_at(step_of(t0()))), t0()).await.unwrap();
        assert_eq!(issued.account_id, account);
        assert!(issued.reactivated, "the deactivated account resumed after the code");
        assert_eq!(fx.sessions.count(), sessions_before + 1);

        let again = fx.login_handler().complete(complete("abcde-fghjk".into()), t0()).await.unwrap_err();
        assert!(matches!(again, AuthError::MfaChallengeInvalid), "single use: {again:?}");
        let unknown = Envelope::new(Uuid::now_v7(), CompleteLoginCommand { mfa_token: "nope".into(), code: "abcde-fghjk".into(), passkey: None });
        assert!(matches!(fx.login_handler().complete(unknown, t0()).await, Err(AuthError::MfaChallengeInvalid)));
    }

    /// Without the seed key (no MFA wired), an account with it on cannot sign
    /// in: fail-closed, never a session without the second factor.
    #[tokio::test]
    async fn two_step_sign_in_fails_closed_without_the_key() {
        let fx = Fixture::new();
        let first = fx.login_handler().handle(password_login(), t0()).await.unwrap().issued().unwrap();
        fx.enroll_mfa(first.account_id);
        let bare = super::LoginHandler::new(
            Arc::clone(&fx.idp) as _,
            Arc::clone(&fx.directory) as _,
            Arc::clone(&fx.profiles) as _,
            Arc::clone(&fx.links) as _,
            Arc::clone(&fx.sessions) as _,
            Arc::clone(&fx.refresh_tokens) as _,
            Arc::clone(&fx.cache) as _,
            Arc::clone(&fx.minter) as _,
            Arc::clone(&fx.publisher) as _,
            fx.policy.clone(),
        );
        let err = bare.handle(password_login(), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::MfaUnavailable), "{err:?}");
    }
}

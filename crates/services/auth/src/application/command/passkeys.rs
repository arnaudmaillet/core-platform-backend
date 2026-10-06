//! The holder's passkeys (#808): registering one (a challenge, then the
//! authenticator's response), listing them, removing one, and signing in with
//! one ([`PasskeySignIn`]: a first factor, the second step, or a step-up).
//! Registering and removing are emailed to the account's address; starting a
//! registration and removing need a recent credential proof (the edge's
//! step-up).
//!
//! A registration challenge is bound to the account that asked for it: it is
//! kept (single use, five minutes) under the hash of the account and the
//! challenge, so only that account can redeem it. A sign-in challenge is
//! anyone's (single use, five minutes): the assertion names its account.
//!
//! **Which account a sign-in is.** Credential ids are only unique per account
//! (anyone's authenticator can claim any id), so an account is never found
//! from a credential id: it is the assertion's user handle (the account id
//! the passkey was made with), or the account the step already belongs to,
//! and the credential must be one of *that* account's.

use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Duration, SubsecRound, Utc};
use cqrs::Envelope;
use rand::RngCore;
use sha2::{Digest, Sha256};

use super::credentials::caller_session;
use super::mfa_settings::{notify_security_change, MfaCaller};
use super::verification::VerificationCodes;
use crate::application::port::{
    AccountDirectory, FederatedNonceStore, MfaChange, PasskeyRepository, SessionCache, SessionRepository,
    StoredPasskey,
};
use crate::domain::value_object::webauthn::{verify_assertion, verify_registration, RelyingParty};
use crate::domain::value_object::{AccountId, SessionId};
use crate::error::AuthError;

/// Most passkeys one account holds.
pub const MAX_PASSKEYS: usize = 10;
/// How long a ceremony's challenge waits for the authenticator.
pub const PASSKEY_CHALLENGE_TTL_SECS: i64 = 300;
/// Longest passkey name kept (in chars).
pub const MAX_PASSKEY_NAME_CHARS: usize = 64;

/// What the client hands its platform authenticator to create a passkey
/// (`PublicKeyCredentialCreationOptions`, ES256, resident key and user
/// verification required, no attestation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasskeyRegistrationOptions {
    /// base64url, no padding.
    pub challenge:           String,
    pub rp_id:               String,
    /// The WebAuthn user handle (base64url of the account id's 16 bytes):
    /// opaque, and what a passkey sign-in returns to name the account.
    pub user_id:             String,
    /// Shown by the authenticator: the account's email or phone.
    pub user_name:           String,
    /// The account's passkeys (base64url ids), so an authenticator does not
    /// make a second one.
    pub exclude_credentials: Vec<String>,
    pub expires_in_secs:     i64,
}

/// The authenticator's response to a registration.
#[derive(Debug, Clone)]
pub struct PasskeyRegistration {
    /// The challenge the options carried.
    pub challenge:          String,
    pub client_data_json:   Vec<u8>,
    pub attestation_object: Vec<u8>,
    /// The holder's name for it; empty ⇒ "Passkey".
    pub name:               String,
}

/// A passkey as the holder sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasskeyView {
    /// base64url, no padding.
    pub credential_id: String,
    pub name:          String,
    pub created_at:    DateTime<Utc>,
    pub last_used_at:  Option<DateTime<Utc>>,
    /// Synced across the holder's devices (e.g. iCloud Keychain).
    pub synced:        bool,
}

impl From<&StoredPasskey> for PasskeyView {
    fn from(p: &StoredPasskey) -> Self {
        Self {
            credential_id: URL_SAFE_NO_PAD.encode(&p.credential_id),
            name:          p.name.clone(),
            created_at:    p.created_at,
            last_used_at:  p.last_used_at,
            synced:        p.backed_up,
        }
    }
}

/// The key a registration challenge is kept under: bound to its account.
fn registration_key(account_id: &AccountId, challenge: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(account_id.as_uuid().as_bytes());
    hash.update(challenge);
    data_encoding::HEXLOWER.encode(&hash.finalize())
}

/// A passkey's name: trimmed, whitespace collapsed, at most
/// [`MAX_PASSKEY_NAME_CHARS`]; "Passkey" when nothing is left.
pub fn normalize_passkey_name(raw: &str) -> String {
    let name: String = raw.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(MAX_PASSKEY_NAME_CHARS).collect();
    if name.is_empty() { "Passkey".to_owned() } else { name }
}

/// What the client hands its platform authenticator to sign in
/// (`PublicKeyCredentialRequestOptions`: discoverable, user verification
/// required, no allow list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasskeySignInOptions {
    /// base64url, no padding.
    pub challenge:       String,
    pub rp_id:           String,
    pub expires_in_secs: i64,
}

/// The authenticator's assertion, as the client sends it.
#[derive(Clone, PartialEq, Eq)]
pub struct PasskeyAssertion {
    /// The options' challenge.
    pub challenge:          String,
    pub credential_id:      Vec<u8>,
    pub client_data_json:   Vec<u8>,
    pub authenticator_data: Vec<u8>,
    pub signature:          Vec<u8>,
    /// The user handle the passkey was made with (the account id's bytes);
    /// may be empty on a second step or a step-up.
    pub user_handle:        Vec<u8>,
}

impl std::fmt::Debug for PasskeyAssertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasskeyAssertion").finish_non_exhaustive()
    }
}

fn sign_in_key(challenge: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(challenge))
}

/// Signing in with a passkey: the challenge, then the assertion's check.
pub struct PasskeySignIn {
    rp:         RelyingParty,
    passkeys:   Arc<dyn PasskeyRepository>,
    challenges: Arc<dyn FederatedNonceStore>,
}

impl PasskeySignIn {
    pub fn new(rp: RelyingParty, passkeys: Arc<dyn PasskeyRepository>, challenges: Arc<dyn FederatedNonceStore>) -> Self {
        Self { rp, passkeys, challenges }
    }

    /// A challenge for an assertion (single use, five minutes).
    pub async fn start(&self) -> Result<PasskeySignInOptions, AuthError> {
        let mut challenge = [0u8; 32];
        rand::rng().fill_bytes(&mut challenge);
        self.challenges.issue(&sign_in_key(&challenge), Duration::seconds(PASSKEY_CHALLENGE_TTL_SECS)).await?;
        Ok(PasskeySignInOptions {
            challenge:       URL_SAFE_NO_PAD.encode(challenge),
            rp_id:           self.rp.id.clone(),
            expires_in_secs: PASSKEY_CHALLENGE_TTL_SECS,
        })
    }

    /// Checks `assertion` (its challenge used up either way) and returns the
    /// account it signs in. `expected`: the account a second step or a
    /// step-up belongs to — the passkey must be that account's.
    pub async fn verify(
        &self,
        assertion: &PasskeyAssertion,
        expected: Option<&AccountId>,
        now: DateTime<Utc>,
    ) -> Result<AccountId, AuthError> {
        let challenge = URL_SAFE_NO_PAD
            .decode(assertion.challenge.trim_end_matches('='))
            .map_err(|_| AuthError::PasskeyChallengeInvalid)?;
        if !self.challenges.consume(&sign_in_key(&challenge)).await? {
            return Err(AuthError::PasskeyChallengeInvalid);
        }
        let named = match assertion.user_handle.as_slice() {
            [] => None,
            handle => Some(
                uuid::Uuid::from_slice(handle)
                    .map(AccountId::from_uuid)
                    .map_err(|_| AuthError::PasskeyAssertionFailed)?,
            ),
        };
        let account_id = match (named, expected) {
            (Some(named), Some(expected)) if named != *expected => return Err(AuthError::PasskeyAssertionFailed),
            (Some(account), _) | (None, Some(&account)) => account,
            (None, None) => return Err(AuthError::PasskeyAssertionFailed),
        };
        let passkey = self
            .passkeys
            .find(&account_id, &assertion.credential_id)
            .await?
            .ok_or(AuthError::PasskeyAssertionFailed)?;
        let used = verify_assertion(
            &self.rp,
            &challenge,
            &passkey.public_key,
            passkey.sign_count,
            &assertion.client_data_json,
            &assertion.authenticator_data,
            &assertion.signature,
        )
        .map_err(|reason| {
            tracing::info!(%reason, account.id = %account_id.as_str(), "passkey sign-in refused");
            AuthError::PasskeyAssertionFailed
        })?;
        self.passkeys.record_use(&account_id, &assertion.credential_id, used.sign_count, used.backed_up, now).await?;
        Ok(account_id)
    }
}

pub struct PasskeyHandler {
    rp:            RelyingParty,
    passkeys:      Arc<dyn PasskeyRepository>,
    registrations: Arc<dyn FederatedNonceStore>,
    sessions:      Arc<dyn SessionRepository>,
    cache:         Arc<dyn SessionCache>,
    directory:     Arc<dyn AccountDirectory>,
    /// The email transport for change notices; `None`: none are sent.
    codes:         Option<Arc<VerificationCodes>>,
}

impl PasskeyHandler {
    pub fn new(
        rp: RelyingParty,
        passkeys: Arc<dyn PasskeyRepository>,
        registrations: Arc<dyn FederatedNonceStore>,
        sessions: Arc<dyn SessionRepository>,
        cache: Arc<dyn SessionCache>,
        directory: Arc<dyn AccountDirectory>,
    ) -> Self {
        Self { rp, passkeys, registrations, sessions, cache, directory, codes: None }
    }

    /// Emails every change to the account's address.
    pub fn with_codes(mut self, codes: Arc<VerificationCodes>) -> Self {
        self.codes = Some(codes);
        self
    }

    pub fn relying_party(&self) -> &RelyingParty {
        &self.rp
    }

    async fn caller(&self, caller: &MfaCaller, now: DateTime<Utc>) -> Result<AccountId, AuthError> {
        let account_id = AccountId::try_from(caller.account_id.as_str())?;
        let session_id = SessionId::try_from(caller.session_id.as_str())?;
        caller_session(&self.sessions, &self.cache, &account_id, &session_id, now).await?;
        Ok(account_id)
    }

    /// A challenge for a new passkey, bound to the caller's account.
    pub async fn start_registration(
        &self,
        envelope: Envelope<MfaCaller>,
        now: DateTime<Utc>,
    ) -> Result<PasskeyRegistrationOptions, AuthError> {
        let account_id = self.caller(&envelope.payload, now).await?;
        let held = self.passkeys.list(&account_id).await?;
        if held.len() >= MAX_PASSKEYS {
            return Err(AuthError::PasskeyLimitReached);
        }
        let mut challenge = [0u8; 32];
        rand::rng().fill_bytes(&mut challenge);
        self.registrations
            .issue(&registration_key(&account_id, &challenge), Duration::seconds(PASSKEY_CHALLENGE_TTL_SECS))
            .await?;
        let user_name = match self.directory.contact(&account_id).await {
            Ok(contact) => contact.email.or(contact.phone).unwrap_or_else(|| "account".into()),
            Err(_) => "account".into(),
        };
        Ok(PasskeyRegistrationOptions {
            challenge: URL_SAFE_NO_PAD.encode(challenge),
            rp_id: self.rp.id.clone(),
            user_id: URL_SAFE_NO_PAD.encode(account_id.as_uuid().as_bytes()),
            user_name,
            exclude_credentials: held.iter().map(|p| URL_SAFE_NO_PAD.encode(&p.credential_id)).collect(),
            expires_in_secs: PASSKEY_CHALLENGE_TTL_SECS,
        })
    }

    /// The authenticator's response: verified against the caller's challenge
    /// (used up either way), then stored.
    pub async fn finish_registration(
        &self,
        envelope: Envelope<(MfaCaller, PasskeyRegistration)>,
        now: DateTime<Utc>,
    ) -> Result<PasskeyView, AuthError> {
        let (caller, registration) = envelope.payload;
        let account_id = self.caller(&caller, now).await?;
        let challenge = URL_SAFE_NO_PAD
            .decode(registration.challenge.trim_end_matches('='))
            .map_err(|_| AuthError::PasskeyChallengeInvalid)?;
        if !self.registrations.consume(&registration_key(&account_id, &challenge)).await? {
            return Err(AuthError::PasskeyChallengeInvalid);
        }
        let made = verify_registration(
            &self.rp,
            &challenge,
            &registration.client_data_json,
            &registration.attestation_object,
        )
        .map_err(|reason| {
            tracing::info!(%reason, account.id = %account_id.as_str(), "passkey registration refused");
            AuthError::PasskeyRejected
        })?;
        let stored = StoredPasskey {
            credential_id:   made.credential_id,
            public_key:      made.public_key,
            sign_count:      made.sign_count,
            name:            normalize_passkey_name(&registration.name),
            aaguid:          uuid::Uuid::from_bytes(made.aaguid),
            backup_eligible: made.backup_eligible,
            backed_up:       made.backed_up,
            // Postgres keeps microseconds: the response and every later read agree.
            created_at:      now.trunc_subsecs(6),
            last_used_at:    None,
        };
        self.passkeys.add(&account_id, &stored, MAX_PASSKEYS).await?;
        notify_security_change(self.codes.clone(), Arc::clone(&self.directory), account_id, MfaChange::PasskeyAdded);
        Ok(PasskeyView::from(&stored))
    }

    /// The caller's passkeys, oldest first.
    pub async fn list(&self, envelope: Envelope<MfaCaller>, now: DateTime<Utc>) -> Result<Vec<PasskeyView>, AuthError> {
        let account_id = self.caller(&envelope.payload, now).await?;
        Ok(self.passkeys.list(&account_id).await?.iter().map(PasskeyView::from).collect())
    }

    /// Removes one of the caller's passkeys; returns those left.
    pub async fn remove(
        &self,
        envelope: Envelope<(MfaCaller, String)>,
        now: DateTime<Utc>,
    ) -> Result<Vec<PasskeyView>, AuthError> {
        let (caller, credential_id) = envelope.payload;
        let account_id = self.caller(&caller, now).await?;
        let credential_id = URL_SAFE_NO_PAD
            .decode(credential_id.trim_end_matches('='))
            .map_err(|_| AuthError::PasskeyNotFound)?;
        if !self.passkeys.remove(&account_id, &credential_id).await? {
            return Err(AuthError::PasskeyNotFound);
        }
        notify_security_change(
            self.codes.clone(),
            Arc::clone(&self.directory),
            account_id,
            MfaChange::PasskeyRemoved,
        );
        Ok(self.passkeys.list(&account_id).await?.iter().map(PasskeyView::from).collect())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::application::command::verification::VerificationPolicy;
    use crate::application::command::{IssuedSession, LoginCommand};
    use crate::application::fakes::{
        t0, Fixture, InMemoryNonceStore, InMemoryPasskeyRepository, InMemoryVerificationStore, RecordingCodeSender,
    };
    use crate::application::port::{AuthnGrant, ContactDetails};
    use crate::domain::value_object::webauthn::testing::{SoftAuthenticator, PASSKEY};
    use crate::domain::value_object::DeviceFingerprint;

    const RP_ID: &str = "example.app";
    const ORIGIN: &str = "https://example.app";

    struct World {
        fx:       Fixture,
        passkeys: Arc<InMemoryPasskeyRepository>,
        sender:   Arc<RecordingCodeSender>,
        handler:  PasskeyHandler,
    }

    impl World {
        fn new() -> Self {
            let fx = Fixture::new();
            let passkeys = Arc::new(InMemoryPasskeyRepository::default());
            let sender = Arc::new(RecordingCodeSender::default());
            let codes = Arc::new(VerificationCodes::new(
                Arc::new(InMemoryVerificationStore::default()),
                Arc::clone(&sender) as _,
                VerificationPolicy::default(),
            ));
            let handler = PasskeyHandler::new(
                RelyingParty::new(RP_ID, vec![]),
                Arc::clone(&passkeys) as _,
                Arc::new(InMemoryNonceStore::default()),
                Arc::clone(&fx.sessions) as _,
                Arc::clone(&fx.cache) as _,
                Arc::clone(&fx.directory) as _,
            )
            .with_codes(codes);
            Self { fx, passkeys, sender, handler }
        }

        async fn login(&self) -> IssuedSession {
            let cmd = LoginCommand {
                grant: AuthnGrant::Password { username: "user".into(), password: "pw".into() },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            };
            self.fx.login_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap().issued().unwrap()
        }

        /// Starts and finishes a registration with `auth` for `session`.
        async fn register(&self, session: &IssuedSession, auth: &SoftAuthenticator) -> Result<PasskeyView, AuthError> {
            let options = self.handler.start_registration(on(session), t0()).await?;
            let challenge = URL_SAFE_NO_PAD.decode(&options.challenge).unwrap();
            self.handler
                .finish_registration(
                    Envelope::new(Uuid::now_v7(), (on(session).payload, PasskeyRegistration {
                        challenge:          options.challenge,
                        client_data_json:   SoftAuthenticator::client_data("webauthn.create", &challenge, ORIGIN),
                        attestation_object: auth.attestation(RP_ID, PASSKEY),
                        name:               "  My   iPhone ".into(),
                    })),
                    t0(),
                )
                .await
        }
    }

    fn on(session: &IssuedSession) -> Envelope<MfaCaller> {
        Envelope::new(
            Uuid::now_v7(),
            MfaCaller { account_id: session.account_id.as_str(), session_id: session.session_id.as_str() },
        )
    }

    async fn settle() {
        // The change notices go out on a spawned task.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn a_name_is_tidied_and_never_empty() {
        assert_eq!(normalize_passkey_name("  My   iPhone "), "My iPhone");
        assert_eq!(normalize_passkey_name("   "), "Passkey");
        assert_eq!(normalize_passkey_name(&"x".repeat(100)).chars().count(), MAX_PASSKEY_NAME_CHARS);
    }

    #[tokio::test]
    async fn a_passkey_is_registered_listed_and_removed_with_notices() {
        let w = World::new();
        let session = w.login().await;
        w.fx.directory.with_contact(session.account_id, ContactDetails { email: Some("me@example.com".into()), phone: None });
        let auth = SoftAuthenticator::new();

        let options = w.handler.start_registration(on(&session), t0()).await.unwrap();
        assert_eq!(options.rp_id, RP_ID);
        assert_eq!(options.user_name, "me@example.com");
        assert_eq!(URL_SAFE_NO_PAD.decode(&options.user_id).unwrap(), session.account_id.as_uuid().as_bytes());

        let made = w.register(&session, &auth).await.unwrap();
        assert_eq!(made.name, "My iPhone");
        assert!(made.synced);
        assert_eq!(made.credential_id, URL_SAFE_NO_PAD.encode(&auth.credential_id));
        let listed = w.handler.list(on(&session), t0()).await.unwrap();
        assert_eq!(listed, vec![made.clone()]);
        // The next registration excludes it.
        let options = w.handler.start_registration(on(&session), t0()).await.unwrap();
        assert_eq!(options.exclude_credentials, vec![made.credential_id.clone()]);
        // The same authenticator again: refused.
        assert!(matches!(w.register(&session, &auth).await, Err(AuthError::PasskeyAlreadyRegistered)));

        let left = w
            .handler
            .remove(Envelope::new(Uuid::now_v7(), (on(&session).payload, made.credential_id.clone())), t0())
            .await
            .unwrap();
        assert!(left.is_empty());
        let again = w.handler.remove(Envelope::new(Uuid::now_v7(), (on(&session).payload, made.credential_id)), t0()).await;
        assert!(matches!(again, Err(AuthError::PasskeyNotFound)));

        settle().await;
        let notices: Vec<MfaChange> = w.sender.mfa_notices().into_iter().map(|(_, change)| change).collect();
        assert!(notices.contains(&MfaChange::PasskeyAdded) && notices.contains(&MfaChange::PasskeyRemoved), "{notices:?}");
    }

    #[tokio::test]
    async fn a_challenge_is_single_use_and_only_its_own_accounts() {
        let w = World::new();
        let mine = w.login().await;
        w.fx.idp.set_subject("sub-456");
        let theirs = w.login().await;
        assert_ne!(mine.account_id, theirs.account_id);
        let auth = SoftAuthenticator::new();
        let options = w.handler.start_registration(on(&mine), t0()).await.unwrap();
        let challenge = URL_SAFE_NO_PAD.decode(&options.challenge).unwrap();
        let response = |options: &PasskeyRegistrationOptions| PasskeyRegistration {
            challenge:          options.challenge.clone(),
            client_data_json:   SoftAuthenticator::client_data("webauthn.create", &challenge, ORIGIN),
            attestation_object: auth.attestation(RP_ID, PASSKEY),
            name:               String::new(),
        };

        let stolen = w
            .handler
            .finish_registration(Envelope::new(Uuid::now_v7(), (on(&theirs).payload, response(&options))), t0())
            .await;
        assert!(matches!(stolen, Err(AuthError::PasskeyChallengeInvalid)), "{stolen:?}");
        // A refused response still uses the challenge up.
        let mut bad = response(&options);
        bad.attestation_object = auth.attestation("evil.app", PASSKEY);
        let refused = w.handler.finish_registration(Envelope::new(Uuid::now_v7(), (on(&mine).payload, bad)), t0()).await;
        assert!(matches!(refused, Err(AuthError::PasskeyRejected)), "{refused:?}");
        let replay =
            w.handler.finish_registration(Envelope::new(Uuid::now_v7(), (on(&mine).payload, response(&options))), t0()).await;
        assert!(matches!(replay, Err(AuthError::PasskeyChallengeInvalid)), "{replay:?}");
        assert!(w.passkeys.is_empty());
    }

    /// Registers `auth` for `session` and returns a sign-in over the same
    /// repository.
    async fn registered(w: &World, session: &IssuedSession, auth: &SoftAuthenticator) -> PasskeySignIn {
        w.register(session, auth).await.unwrap();
        PasskeySignIn::new(
            RelyingParty::new(RP_ID, vec![]),
            Arc::clone(&w.passkeys) as _,
            Arc::new(InMemoryNonceStore::default()),
        )
    }

    /// An assertion by `auth` for a fresh challenge of `sign_in`.
    async fn assertion(
        sign_in: &PasskeySignIn,
        auth: &mut SoftAuthenticator,
        user_handle: Vec<u8>,
        flags: u8,
    ) -> PasskeyAssertion {
        let options = sign_in.start().await.unwrap();
        let challenge = URL_SAFE_NO_PAD.decode(&options.challenge).unwrap();
        let client = SoftAuthenticator::client_data("webauthn.get", &challenge, ORIGIN);
        let (authenticator_data, signature) = auth.assert(RP_ID, flags, &client, false);
        PasskeyAssertion {
            challenge: options.challenge,
            credential_id: auth.credential_id.clone(),
            client_data_json: client,
            authenticator_data,
            signature,
            user_handle,
        }
    }

    #[tokio::test]
    async fn a_sign_in_names_its_account_by_user_handle_and_checks_the_key() {
        let w = World::new();
        let session = w.login().await;
        let mut auth = SoftAuthenticator::new();
        let sign_in = registered(&w, &session, &auth).await;
        let handle = session.account_id.as_uuid().as_bytes().to_vec();

        let ok = assertion(&sign_in, &mut auth, handle.clone(), PASSKEY).await;
        assert_eq!(sign_in.verify(&ok, None, t0()).await.unwrap(), session.account_id);
        assert_eq!(w.passkeys.list(&session.account_id).await.unwrap()[0].last_used_at, Some(t0()));
        // Single use.
        assert!(matches!(sign_in.verify(&ok, None, t0()).await, Err(AuthError::PasskeyChallengeInvalid)));

        // The second step / step-up of the same account: with or without a handle.
        let step = assertion(&sign_in, &mut auth, Vec::new(), PASSKEY).await;
        assert_eq!(sign_in.verify(&step, Some(&session.account_id), t0()).await.unwrap(), session.account_id);

        // No handle and no account: refused (an id never names an account).
        let anonymous = assertion(&sign_in, &mut auth, Vec::new(), PASSKEY).await;
        assert!(matches!(sign_in.verify(&anonymous, None, t0()).await, Err(AuthError::PasskeyAssertionFailed)));
        // Another account's step, or a handle naming another account.
        let other = AccountId::from_uuid(Uuid::now_v7());
        let wrong = assertion(&sign_in, &mut auth, handle.clone(), PASSKEY).await;
        assert!(matches!(sign_in.verify(&wrong, Some(&other), t0()).await, Err(AuthError::PasskeyAssertionFailed)));
        let elsewhere = assertion(&sign_in, &mut auth, other.as_uuid().as_bytes().to_vec(), PASSKEY).await;
        assert!(matches!(sign_in.verify(&elsewhere, None, t0()).await, Err(AuthError::PasskeyAssertionFailed)));
        // Another key under this credential id, or no user verification.
        let mut impostor = SoftAuthenticator::new();
        impostor.credential_id = auth.credential_id.clone();
        let forged = assertion(&sign_in, &mut impostor, handle.clone(), PASSKEY).await;
        assert!(matches!(sign_in.verify(&forged, None, t0()).await, Err(AuthError::PasskeyAssertionFailed)));
        let weak = assertion(&sign_in, &mut auth, handle, crate::domain::value_object::webauthn::testing::PRESENT_ONLY).await;
        assert!(matches!(sign_in.verify(&weak, None, t0()).await, Err(AuthError::PasskeyAssertionFailed)));
    }

    /// A passkey signs in without a second step, even with two-step sign-in
    /// on; it is also the second step after a password, and a step-up.
    #[tokio::test]
    async fn a_passkey_signs_in_completes_a_second_step_and_steps_up() {
        use crate::application::command::{CompleteLoginCommand, LoginOutcome, StepUpCredential, VerifyCredentialsCommand};
        use crate::application::port::SessionRepository;

        let w = World::new();
        let session = w.login().await;
        let mut auth = SoftAuthenticator::new();
        let sign_in = Arc::new(registered(&w, &session, &auth).await);
        w.fx.enroll_mfa(session.account_id);
        let login = w.fx.login_handler().with_passkeys(Arc::clone(&sign_in));
        let handle = session.account_id.as_uuid().as_bytes().to_vec();
        let device = DeviceFingerprint::default();
        let grant = |a: PasskeyAssertion| LoginCommand {
            grant: AuthnGrant::Passkey(a),
            device: device.clone(),
            guest_refresh_token: None,
            client_ip: None,
        };

        // First factor: signed in at once, as the account's own identity.
        let a = assertion(&sign_in, &mut auth, handle.clone(), PASSKEY).await;
        let issued = match login.handle(Envelope::new(Uuid::now_v7(), grant(a)), t0()).await.unwrap() {
            LoginOutcome::Issued(issued) => issued,
            other => panic!("no second step after a passkey: {other:?}"),
        };
        assert_eq!(issued.account_id, session.account_id);
        let stored = w.fx.sessions.find_by_id(&issued.session_id).await.unwrap().unwrap();
        assert_eq!(stored.subject().issuer(), "https://idp.test", "the account's link, so a password step-up works");
        let refused = assertion(&sign_in, &mut auth, Vec::new(), PASSKEY).await;
        assert!(matches!(
            login.handle(Envelope::new(Uuid::now_v7(), grant(refused)), t0()).await,
            Err(AuthError::PasskeyAssertionFailed)
        ));

        // The second step after a password.
        let password = LoginCommand {
            grant: AuthnGrant::Password { username: "user".into(), password: "pw".into() },
            device: device.clone(),
            guest_refresh_token: None,
            client_ip: None,
        };
        let challenge = match login.handle(Envelope::new(Uuid::now_v7(), password), t0()).await.unwrap() {
            LoginOutcome::SecondFactorRequired(challenge) => challenge,
            other => panic!("two-step sign-in is on: {other:?}"),
        };
        let a = assertion(&sign_in, &mut auth, Vec::new(), PASSKEY).await;
        let complete = CompleteLoginCommand { mfa_token: challenge.mfa_token, code: String::new(), passkey: Some(a) };
        let done = login.complete(Envelope::new(Uuid::now_v7(), complete), t0()).await.unwrap();
        assert_eq!(done.account_id, session.account_id);

        // A step-up.
        let verify = w.fx.verify_credentials_handler().with_passkeys(Arc::clone(&sign_in));
        let a = assertion(&sign_in, &mut auth, handle, PASSKEY).await;
        let cmd = VerifyCredentialsCommand {
            account_id: done.account_id.as_str(),
            session_id: done.session_id.as_str(),
            credential: StepUpCredential::Passkey(a),
        };
        verify.handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap();
    }

    #[tokio::test]
    async fn an_account_holds_at_most_ten() {
        let w = World::new();
        let session = w.login().await;
        for _ in 0..MAX_PASSKEYS {
            w.register(&session, &SoftAuthenticator::new()).await.unwrap();
        }
        assert!(matches!(w.handler.start_registration(on(&session), t0()).await, Err(AuthError::PasskeyLimitReached)));
    }
}

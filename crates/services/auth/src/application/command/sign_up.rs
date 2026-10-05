//! Sign-up with native Sign in with Apple / Google (guest mode B4a).
//!
//! The app collects the provider credential first (no server call), then the
//! date of birth and the consent, then calls SignUp with all of it: under the
//! minimum age nothing is created. The account is created through `account`
//! (which enforces the minimum age), linked to the provider identity, and a
//! member session opens. The profile comes next (`profile.CreateProfile`, then
//! a Refresh so the token's `pids` carry it).
//!
//! One person, one account: an identity that already has an account, or whose
//! provider-verified email belongs to one created with another method, gets
//! that method back instead of a duplicate. Apple private-relay addresses never
//! match. Only the email inside a verified token is ever looked up, so nobody can
//! probe addresses they do not control.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use crate::application::command::member_session::MemberSessions;
use crate::application::command::verification::VerificationCodes;
use crate::application::ensure_valid;
use crate::application::port::{
    AccountActivation, AccountDirectory, EventPublisher, FederatedTokenVerifier, GuestRegistry, NewAccount,
    SignUpConsent, SubjectLinkRepository,
};
use crate::domain::aggregate::SubjectLink;
use crate::domain::value_object::{
    AccountId, DeviceFingerprint, FederatedProvider, IdpSubject, SignInMethod, EMAIL_CODE_ISSUER,
    PHONE_CODE_ISSUER,
};
use crate::application::port::VerificationChannel;

/// How the person proves who they are.
#[derive(Debug, Clone)]
pub enum SignUpCredential {
    /// Native Sign in with Apple / Google.
    IdToken { provider: FederatedProvider, id_token: String, nonce: String },
    /// A one-time code sent to an email address or by SMS (passwordless account).
    Code { challenge_id: String, code: String },
}

/// What a credential proved.
struct Proven {
    subject:        IdpSubject,
    email:          Option<String>,
    email_verified: bool,
    private_relay:  bool,
    /// A number a code proved (E.164).
    phone:          Option<String>,
    /// How this identity signs in.
    method:         SignInMethod,
}
use crate::error::AuthError;

#[derive(Debug, Clone)]
pub struct SignUpCommand {
    pub credential:          SignUpCredential,
    /// ISO 8601 (YYYY-MM-DD).
    pub date_of_birth:       String,
    pub consent:             SignUpConsent,
    /// ISO 3166-1 alpha-2, if known.
    pub home_country:        Option<String>,
    pub device:              DeviceFingerprint,
    pub guest_refresh_token: Option<String>,
    /// The caller's address as the transport saw it — what code lockouts key
    /// on. Never the client-written `device` IP.
    pub client_ip: Option<String>,
}

impl Validate for SignUpCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        match &self.credential {
            SignUpCredential::IdToken { id_token, nonce, .. } => {
                if id_token.trim().is_empty() {
                    v.push(FieldViolation::new("id_token", "AUT-VAL-030", "id_token must not be empty"));
                }
                if nonce.trim().is_empty() {
                    v.push(FieldViolation::new("nonce", "AUT-VAL-031", "nonce must not be empty"));
                }
            }
            SignUpCredential::Code { challenge_id, code } => {
                if challenge_id.trim().is_empty() || code.trim().is_empty() {
                    v.push(FieldViolation::new(
                        "verification_code",
                        "AUT-VAL-036",
                        "challenge_id and code are required",
                    ));
                }
            }
        }
        if NaiveDate::parse_from_str(&self.date_of_birth, "%Y-%m-%d").is_err() {
            v.push(FieldViolation::new(
                "date_of_birth",
                "AUT-VAL-032",
                "date_of_birth must be an ISO 8601 date (YYYY-MM-DD)",
            ));
        }
        if self.consent.policy_version.trim().is_empty() || self.consent.policy_version.len() > 64 {
            v.push(FieldViolation::new(
                "consent.policy_version",
                "AUT-VAL-033",
                "the privacy-policy version shown is required (≤ 64 chars)",
            ));
        }
        if !self.consent.data_processing {
            v.push(FieldViolation::new(
                "consent.data_processing",
                "AUT-VAL-034",
                "consent to data processing is required to create an account",
            ));
        }
        if let Some(country) = &self.home_country
            && !(country.len() == 2 && country.chars().all(|c| c.is_ascii_alphabetic()))
        {
            v.push(FieldViolation::new(
                "home_country",
                "AUT-VAL-035",
                "home_country must be an ISO 3166-1 alpha-2 code",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

/// What SignUp did.
#[derive(Debug, Clone)]
pub enum SignUpOutcome {
    SignedUp {
        account_id:        AccountId,
        session_id:        crate::domain::value_object::SessionId,
        access_token:      String,
        refresh_token:     String,
        access_expires_in: i64,
    },
    /// The identity, or its verified email, has an account: sign in with this.
    ExistingAccount { method: SignInMethod },
}

pub struct SignUpHandler {
    verifier:  Arc<dyn FederatedTokenVerifier>,
    directory: Arc<dyn AccountDirectory>,
    links:     Arc<dyn SubjectLinkRepository>,
    publisher: Arc<dyn EventPublisher>,
    guests:    Arc<dyn GuestRegistry>,
    members:   MemberSessions,
    /// Email one-time codes; `None` refuses code sign-ups.
    codes:     Option<Arc<VerificationCodes>>,
}

impl SignUpHandler {
    pub fn new(
        verifier: Arc<dyn FederatedTokenVerifier>,
        directory: Arc<dyn AccountDirectory>,
        links: Arc<dyn SubjectLinkRepository>,
        guests: Arc<dyn GuestRegistry>,
        members: MemberSessions,
    ) -> Self {
        Self { verifier, directory, links, publisher: Arc::clone(&members.publisher), guests, members, codes: None }
    }

    /// Enables passwordless email sign-up (one-time codes).
    pub fn with_codes(mut self, codes: Arc<VerificationCodes>) -> Self {
        self.codes = Some(codes);
        self
    }

    async fn prove(&self, credential: &SignUpCredential, client_ip: Option<&str>) -> Result<Proven, AuthError> {
        match credential {
            SignUpCredential::IdToken { provider, id_token, nonce } => {
                let identity = self.verifier.verify(*provider, id_token, nonce).await?;
                Ok(Proven {
                    subject: IdpSubject::new(identity.issuer, identity.subject)?,
                    email: identity.email,
                    email_verified: identity.email_verified,
                    private_relay: identity.private_relay,
                    phone: None,
                    method: (*provider).into(),
                })
            }
            SignUpCredential::Code { challenge_id, code } => {
                let codes = self
                    .codes
                    .as_ref()
                    .ok_or_else(|| AuthError::VerificationChannelUnavailable { channel: "email".into() })?;
                let proven = codes.verify(challenge_id, code, client_ip).await?;
                Ok(match proven.channel {
                    VerificationChannel::Email => Proven {
                        subject: IdpSubject::new(EMAIL_CODE_ISSUER, proven.destination.clone())?,
                        email: Some(proven.destination),
                        email_verified: true,
                        private_relay: false,
                        phone: None,
                        method: SignInMethod::EmailCode,
                    },
                    VerificationChannel::Sms => Proven {
                        subject: IdpSubject::new(PHONE_CODE_ISSUER, proven.destination.clone())?,
                        email: None,
                        email_verified: false,
                        private_relay: false,
                        phone: Some(proven.destination),
                        method: SignInMethod::PhoneCode,
                    },
                })
            }
        }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<SignUpCommand>,
        now: DateTime<Utc>,
    ) -> Result<SignUpOutcome, AuthError> {
        ensure_valid(&envelope.payload)?;
        let cmd = envelope.payload;
        let correlation_id = envelope.correlation_id;

        // 1. Who this is: per the provider, or per the code sent to the address.
        let identity = self.prove(&cmd.credential, cmd.client_ip.as_deref()).await?;
        let subject = identity.subject.clone();

        // 2. This very identity already has an account: sign in instead.
        if self.links.find_by_subject(&subject).await?.is_some() {
            return Ok(SignUpOutcome::ExistingAccount { method: identity.method });
        }

        // An account needs an email or a phone number.
        if identity.email.is_none() && identity.phone.is_none() {
            return Err(AuthError::IdTokenWithoutEmail);
        }

        // 3. Its verified email, or its proven number, belongs to an account
        //    made with another method. (An account made for this subject by an
        //    earlier, interrupted sign-up is finished below instead.)
        let holder = match (&identity.email, &identity.phone) {
            (Some(email), _) if identity.email_verified && !identity.private_relay => {
                self.directory.find_by_email(email).await?
            }
            (_, Some(phone)) => self.directory.find_by_phone(phone).await?,
            _ => None,
        };
        if let Some(holder) = holder.filter(|h| h.identity_id != subject.to_string()) {
            let method = self
                .links
                .find_by_account(&holder.account_id)
                .await?
                .first()
                .map(|link| SignInMethod::from_issuer(link.subject().issuer()))
                .unwrap_or(SignInMethod::Password);
            return Ok(SignUpOutcome::ExistingAccount { method });
        }

        // 4. Create the account (the minimum age is enforced there: nothing is
        //    created below it) and link the identity to it.
        let account_id = self
            .directory
            .provision(&NewAccount {
                subject: subject.clone(),
                email: identity.email.clone(),
                email_verified: identity.email_verified,
                phone: identity.phone.clone(),
                phone_verified: identity.phone.is_some(),
                date_of_birth: cmd.date_of_birth,
                country: cmd.home_country.map(|c| c.to_ascii_uppercase()),
                consent: cmd.consent,
            })
            .await?;
        let mut link = SubjectLink::establish(subject.clone(), account_id, now, correlation_id);
        match self.links.save(&link).await {
            Ok(()) => {
                for event in &link.drain_events() {
                    self.publisher.publish(event).await?;
                }
            }
            // A concurrent sign-up of the same identity linked it first.
            Err(AuthError::SubjectAlreadyLinked { .. }) => {}
            Err(e) => return Err(e),
        }

        // 5. A member session — the account must be active by now (the provider
        //    vouched for the email); otherwise it waits on its verification.
        let snapshot = self.directory.lookup(&account_id).await?;
        if let AccountActivation::Inactive { reason } = snapshot.activation {
            return Err(AuthError::AccountNotActive { current: reason });
        }
        let issued = self
            .members
            .issue(account_id, subject, cmd.device, snapshot.permissions, snapshot.age_bracket, now, correlation_id)
            .await?;

        // 6. The guest this device was is now this member.
        if let Some(token) = cmd.guest_refresh_token.as_deref() {
            self.members.retire_guest(self.guests.as_ref(), token, account_id, now, correlation_id).await;
        }

        Ok(SignUpOutcome::SignedUp {
            account_id,
            session_id: issued.session_id,
            access_token: issued.access_token,
            refresh_token: issued.refresh_token,
            access_expires_in: issued.access_expires_in,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::Datelike;
    use uuid::Uuid;

    use super::*;
    use crate::application::command::StartGuestSessionCommand;
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::{FederatedIdentity, SessionRepository};
    use crate::domain::value_object::{SessionKind, SessionStatus, APPLE_ISSUER};

    /// Verifies `token-<sub>` as that Apple / Google subject with the given email.
    #[derive(Default)]
    struct StubVerifier {
        identities: Mutex<HashMap<String, FederatedIdentity>>,
    }

    impl StubVerifier {
        fn knows(&self, token: &str, provider: FederatedProvider, sub: &str, email: Option<&str>, relay: bool) {
            let issuer = match provider {
                FederatedProvider::Apple => APPLE_ISSUER,
                FederatedProvider::Google => "https://accounts.google.com",
            };
            self.identities.lock().unwrap().insert(token.to_owned(), FederatedIdentity {
                provider,
                issuer: issuer.to_owned(),
                subject: sub.to_owned(),
                email: email.map(str::to_owned),
                email_verified: true,
                private_relay: relay,
            });
        }
    }

    #[async_trait]
    impl FederatedTokenVerifier for StubVerifier {
        async fn verify(&self, _: FederatedProvider, id_token: &str, _: &str) -> Result<FederatedIdentity, AuthError> {
            self.identities
                .lock()
                .unwrap()
                .get(id_token)
                .cloned()
                .ok_or(AuthError::IdTokenRejected { reason: "unknown".into() })
        }
    }

    fn members(fx: &Fixture) -> MemberSessions {
        MemberSessions {
            profiles: Arc::clone(&fx.profiles) as _,
            sessions: Arc::clone(&fx.sessions) as _,
            refresh_tokens: Arc::clone(&fx.refresh_tokens) as _,
            cache: Arc::clone(&fx.cache) as _,
            minter: Arc::clone(&fx.minter) as _,
            publisher: Arc::clone(&fx.publisher) as _,
            policy: fx.policy.clone(),
        }
    }

    fn handler(fx: &Fixture, verifier: &Arc<StubVerifier>) -> SignUpHandler {
        SignUpHandler::new(
            Arc::clone(verifier) as _,
            Arc::clone(&fx.directory) as _,
            Arc::clone(&fx.links) as _,
            Arc::clone(&fx.guests) as _,
            members(fx),
        )
    }

    fn adult_dob() -> String {
        format!("{}-01-15", Utc::now().year() - 30)
    }

    fn sign_up(token: &str, dob: String, guest: Option<String>) -> Envelope<SignUpCommand> {
        Envelope::new(Uuid::now_v7(), SignUpCommand {
            credential: SignUpCredential::IdToken {
                provider: FederatedProvider::Apple,
                id_token: token.to_owned(),
                nonce: "n".into(),
            },
            date_of_birth: dob,
            consent: SignUpConsent {
                policy_version: "2026-10".into(),
                data_processing: true,
                marketing: false,
                analytics: true,
            },
            home_country: Some("fr".into()),
            device: DeviceFingerprint::default(),
            guest_refresh_token: guest,
            client_ip: None,
        })
    }

    #[tokio::test]
    async fn a_new_identity_gets_an_account_a_link_and_a_session() {
        let fx = Fixture::new();
        let verifier = Arc::new(StubVerifier::default());
        verifier.knows("t1", FederatedProvider::Apple, "apple-1", Some("ada@example.com"), false);

        let outcome = handler(&fx, &verifier).handle(sign_up("t1", adult_dob(), None), t0()).await.unwrap();
        let SignUpOutcome::SignedUp { account_id, access_token, .. } = outcome else {
            panic!("expected a sign-up, got {outcome:?}");
        };
        assert!(!access_token.is_empty());

        let created = fx.directory.provisioned();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].email.as_deref(), Some("ada@example.com"));
        assert_eq!(created[0].country.as_deref(), Some("FR"), "home country, upper-cased");
        assert!(created[0].consent.data_processing && created[0].consent.analytics && !created[0].consent.marketing);
        let link = fx.links.find_by_subject(&IdpSubject::new(APPLE_ISSUER, "apple-1").unwrap()).await.unwrap();
        assert_eq!(link.map(|l| l.account_id()), Some(account_id));
        assert_eq!(fx.publisher.event_types(), vec!["auth.subject_linked", "auth.session_issued"]);

        // Signing up again with the same identity: sign in instead.
        let again = handler(&fx, &verifier).handle(sign_up("t1", adult_dob(), None), t0()).await.unwrap();
        assert!(matches!(again, SignUpOutcome::ExistingAccount { method: SignInMethod::Apple }));
        assert_eq!(fx.directory.provisioned().len(), 1);
    }

    #[tokio::test]
    async fn a_verified_email_held_by_another_method_points_to_it_relay_addresses_never_match() {
        let fx = Fixture::new();
        let verifier = Arc::new(StubVerifier::default());
        let keycloak = IdpSubject::new("https://sso.example/realms/core", "kc-1").unwrap();
        let existing = AccountId::from_uuid(Uuid::now_v7());
        fx.directory.with_email("bob@example.com", &keycloak, existing);
        fx.links.save(&SubjectLink::establish(keycloak, existing, t0(), Uuid::now_v7())).await.unwrap();

        verifier.knows("t2", FederatedProvider::Apple, "apple-2", Some("Bob@Example.com"), false);
        let outcome = handler(&fx, &verifier).handle(sign_up("t2", adult_dob(), None), t0()).await.unwrap();
        assert!(matches!(outcome, SignUpOutcome::ExistingAccount { method: SignInMethod::Password }));
        assert!(fx.directory.provisioned().is_empty(), "no duplicate");

        // The same address through a private relay is someone new to us.
        fx.directory.with_email("x@privaterelay.appleid.com", &IdpSubject::new("https://sso.example/realms/core", "kc-9").unwrap(), AccountId::from_uuid(Uuid::now_v7()));
        verifier.knows("t3", FederatedProvider::Apple, "apple-3", Some("x@privaterelay.appleid.com"), true);
        let relay = handler(&fx, &verifier).handle(sign_up("t3", adult_dob(), None), t0()).await;
        // Not matched: provisioning is attempted (and the stub refuses the
        // duplicate address, as account's unique index would).
        assert!(matches!(relay, Err(AuthError::EmailAlreadyRegistered)));
    }

    #[tokio::test]
    async fn under_the_minimum_age_nothing_is_created() {
        let fx = Fixture::new();
        let verifier = Arc::new(StubVerifier::default());
        verifier.knows("t4", FederatedProvider::Apple, "apple-4", Some("kid@example.com"), false);
        let dob = format!("{}-01-01", Utc::now().year() - 10);
        let err = handler(&fx, &verifier).handle(sign_up("t4", dob, None), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::AgeBelowMinimum));
        assert!(fx.links.find_by_subject(&IdpSubject::new(APPLE_ISSUER, "apple-4").unwrap()).await.unwrap().is_none());
        assert_eq!(fx.sessions.count(), 0);
    }

    #[tokio::test]
    async fn the_request_is_validated_before_any_call() {
        let fx = Fixture::new();
        let verifier = Arc::new(StubVerifier::default());
        let mut no_consent = sign_up("t", adult_dob(), None);
        no_consent.payload.consent.data_processing = false;
        let mut bad_dob = sign_up("t", "15/01/1990".into(), None);
        bad_dob.payload.credential = SignUpCredential::IdToken {
            provider: FederatedProvider::Apple,
            id_token: "t".into(),
            nonce: String::new(),
        };
        for env in [no_consent, bad_dob] {
            assert!(handler(&fx, &verifier).handle(env, t0()).await.is_err());
        }
        assert!(fx.directory.provisioned().is_empty());
    }

    #[tokio::test]
    async fn the_guest_session_of_the_device_ends_and_is_linked() {
        let fx = Fixture::new();
        let guest = fx
            .start_guest_handler()
            .handle(
                Envelope::new(Uuid::now_v7(), StartGuestSessionCommand {
                    device: DeviceFingerprint::new(None, None, Some("device-1".into())),
                    attestation: None,
                    attest_key_id: None,
                    attest_challenge: None,
                    locale: None,
                    region_hint: None,
                    current_country: None,
                }),
                t0(),
            )
            .await
            .unwrap();
        let verifier = Arc::new(StubVerifier::default());
        verifier.knows("t5", FederatedProvider::Google, "g-5", Some("eve@example.com"), false);

        let outcome = handler(&fx, &verifier)
            .handle(sign_up("t5", adult_dob(), Some(guest.refresh_token.clone())), t0())
            .await
            .unwrap();
        let SignUpOutcome::SignedUp { account_id, .. } = outcome else { panic!("{outcome:?}") };

        let guest_session = fx.sessions.find_by_id(&guest.session_id).await.unwrap().unwrap();
        assert_eq!(guest_session.kind(), SessionKind::Guest);
        assert_eq!(guest_session.status(), SessionStatus::Revoked);
        assert_eq!(*fx.guests.upgrades.lock().unwrap(), vec![(guest.account_id, account_id)]);

        // A member's refresh token is never treated as a guest's.
        let member_refresh = {
            let SignUpOutcome::SignedUp { refresh_token, .. } = handler(&fx, &{
                let v = Arc::new(StubVerifier::default());
                v.knows("t6", FederatedProvider::Google, "g-6", Some("zed@example.com"), false);
                v
            })
            .handle(sign_up("t6", adult_dob(), None), t0())
            .await
            .unwrap() else { panic!() };
            refresh_token
        };
        let before = fx.guests.upgrades.lock().unwrap().len();
        let v = Arc::new(StubVerifier::default());
        v.knows("t7", FederatedProvider::Google, "g-7", Some("amy@example.com"), false);
        handler(&fx, &v).handle(sign_up("t7", adult_dob(), Some(member_refresh)), t0()).await.unwrap();
        assert_eq!(fx.guests.upgrades.lock().unwrap().len(), before);
    }

    #[tokio::test]
    async fn an_id_token_login_needs_a_signed_up_identity() {
        use crate::application::command::LoginCommand;
        use crate::application::port::AuthnGrant;

        let fx = Fixture::new();
        let verifier = Arc::new(StubVerifier::default());
        verifier.knows("t8", FederatedProvider::Apple, "apple-8", Some("liz@example.com"), false);
        let login = |guest: Option<String>| {
            Envelope::new(Uuid::now_v7(), LoginCommand {
                grant: AuthnGrant::IdToken {
                    provider: FederatedProvider::Apple,
                    id_token: "t8".into(),
                    nonce: "n".into(),
                },
                device: DeviceFingerprint::default(),
                guest_refresh_token: guest,
                client_ip: None,
            })
        };
        let handler = fx.login_handler().with_federated(Arc::clone(&verifier) as _, Arc::clone(&fx.guests) as _);

        // No account yet: the app goes on with SignUp.
        let err = handler.handle(login(None), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::NoAccountForIdentity));

        let SignUpOutcome::SignedUp { account_id, .. } =
            super::tests::handler(&fx, &verifier).handle(sign_up("t8", adult_dob(), None), t0()).await.unwrap()
        else {
            panic!("expected a sign-up")
        };
        let issued = handler.handle(login(None), t0()).await.unwrap().issued().unwrap();
        assert_eq!(issued.account_id, account_id);
        assert!(!issued.first_link, "linked at sign-up");

        // Without a verifier, id_token grants are refused.
        let err = fx.login_handler().handle(login(None), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::FederatedProviderNotConfigured { .. }));
    }

    #[tokio::test]
    async fn an_email_code_signs_up_a_passwordless_account_and_signs_it_back_in() {
        use crate::application::command::verification::{StartVerificationCommand, VerificationPolicy};
        use crate::application::command::LoginCommand;
        use crate::application::fakes::{InMemoryVerificationStore, RecordingCodeSender};
        use crate::application::port::{AuthnGrant, VerificationChannel};

        let fx = Fixture::new();
        let sender = Arc::new(RecordingCodeSender::default());
        let codes = Arc::new(VerificationCodes::new(
            Arc::new(InMemoryVerificationStore::default()),
            Arc::clone(&sender) as _,
            VerificationPolicy::default(),
        ));
        let verifier = Arc::new(StubVerifier::default());
        let handler = handler(&fx, &verifier).with_codes(Arc::clone(&codes));
        let start = |to: &str| StartVerificationCommand {
            channel: VerificationChannel::Email,
            destination: to.into(),
            locale: None,
            client_ip: None,
        };
        let by_code = |challenge_id: String, code: String| {
            let mut env = sign_up("unused", adult_dob(), None);
            env.payload.credential = SignUpCredential::Code { challenge_id, code };
            env
        };

        let started = codes.start(start("Mia@Example.com")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let outcome = handler.handle(by_code(started.challenge_id.clone(), code.clone()), t0()).await.unwrap();
        let SignUpOutcome::SignedUp { account_id, .. } = outcome else { panic!("{outcome:?}") };
        let created = fx.directory.provisioned();
        assert_eq!(created.last().unwrap().email.as_deref(), Some("mia@example.com"));
        assert!(created.last().unwrap().email_verified, "the code proved the address");
        assert_eq!(created.last().unwrap().subject, IdpSubject::new(EMAIL_CODE_ISSUER, "mia@example.com").unwrap());

        // The code is spent.
        let reused = handler.handle(by_code(started.challenge_id, code), t0()).await;
        assert!(matches!(reused, Err(AuthError::VerificationCodeInvalid)));

        // A second sign-up for the address: sign in with a code instead.
        let again = codes.start(start("mia@example.com")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let outcome = handler.handle(by_code(again.challenge_id, code), t0()).await.unwrap();
        assert!(matches!(outcome, SignUpOutcome::ExistingAccount { method: SignInMethod::EmailCode }));

        // Signing back in with a code.
        let login = fx.login_handler().with_codes(Arc::clone(&codes));
        let next = codes.start(start("mia@example.com")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let issued = login
            .handle(
                Envelope::new(Uuid::now_v7(), LoginCommand {
                    grant: AuthnGrant::Code { challenge_id: next.challenge_id, code },
                    device: DeviceFingerprint::default(),
                    guest_refresh_token: None,
                    client_ip: None,
                }),
                t0(),
            )
            .await
            .unwrap()
            .issued()
            .unwrap();
        assert_eq!(issued.account_id, account_id);

        // An address with no account: told only after the code (sign up).
        let stranger = codes.start(start("nobody@example.com")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let err = login
            .handle(
                Envelope::new(Uuid::now_v7(), LoginCommand {
                    grant: AuthnGrant::Code { challenge_id: stranger.challenge_id, code },
                    device: DeviceFingerprint::default(),
                    guest_refresh_token: None,
                    client_ip: None,
                }),
                t0(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::NoAccountForIdentity));
    }

    #[tokio::test]
    async fn an_sms_code_signs_up_a_phone_only_account() {
        use crate::application::command::verification::{StartVerificationCommand, VerificationPolicy};
        use crate::application::fakes::{InMemoryVerificationStore, RecordingCodeSender};
        use crate::application::port::VerificationChannel;
        use crate::domain::value_object::PHONE_CODE_ISSUER;

        let fx = Fixture::new();
        let sender = Arc::new(RecordingCodeSender::default());
        let codes = Arc::new(VerificationCodes::new(
            Arc::new(InMemoryVerificationStore::default()),
            Arc::clone(&sender) as _,
            VerificationPolicy::default(),
        ));
        let handler = handler(&fx, &Arc::new(StubVerifier::default())).with_codes(Arc::clone(&codes));
        let by_sms = |number: &str| StartVerificationCommand {
            channel: VerificationChannel::Sms,
            destination: number.into(),
            locale: None,
            client_ip: None,
        };
        let sign_up_with = |challenge_id: String, code: String| {
            let mut env = sign_up("unused", adult_dob(), None);
            env.payload.credential = SignUpCredential::Code { challenge_id, code };
            env
        };

        let started = codes.start(by_sms("+33 6 11 22 33 44")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let outcome = handler.handle(sign_up_with(started.challenge_id, code), t0()).await.unwrap();
        assert!(matches!(outcome, SignUpOutcome::SignedUp { .. }), "{outcome:?}");
        let created = fx.directory.provisioned().pop().unwrap();
        assert_eq!(created.email, None, "a phone-only account");
        assert_eq!(created.phone.as_deref(), Some("+33611223344"));
        assert!(created.phone_verified);
        assert_eq!(created.subject, IdpSubject::new(PHONE_CODE_ISSUER, "+33611223344").unwrap());

        // The same number again: sign in with an SMS code instead.
        let again = codes.start(by_sms("+33611223344")).await.unwrap();
        let (_, code, _) = sender.last().unwrap();
        let outcome = handler.handle(sign_up_with(again.challenge_id, code), t0()).await.unwrap();
        assert!(matches!(outcome, SignUpOutcome::ExistingAccount { method: SignInMethod::PhoneCode }));
    }
}

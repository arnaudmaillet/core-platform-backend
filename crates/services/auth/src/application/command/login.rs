use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use crate::application::command::member_session::MemberSessions;
use crate::application::command::verification::VerificationCodes;
use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    AccountActivation, AccountDirectory, AuthnGrant, EventPublisher, FederatedTokenVerifier, GuestRegistry,
    IdentityProvider, ProfileDirectory, RefreshTokenRepository, SessionCache, SessionRepository,
    SubjectLinkRepository, TokenMinter,
};
use crate::domain::aggregate::SubjectLink;
use crate::application::port::VerificationChannel;
use crate::domain::value_object::{AccountId, DeviceFingerprint, IdpSubject, EMAIL_CODE_ISSUER, PHONE_CODE_ISSUER};
use crate::error::AuthError;

/// Establish a session by brokering a credential to the IdP, or by verifying a
/// native Sign in with Apple / Google id_token.
#[derive(Debug, Clone)]
pub struct LoginCommand {
    pub grant: AuthnGrant,
    pub device: DeviceFingerprint,
    /// The guest session this device was using: it ends on success.
    pub guest_refresh_token: Option<String>,
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
        }
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
    ) -> Result<IssuedSession, AuthError> {
        ensure_valid(&envelope.payload)?;
        let cmd = envelope.payload;
        let correlation_id = envelope.correlation_id;

        // 1–2. Who is signing in, and their account.
        let (subject, account_id, needs_link) = match cmd.grant {
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
                let proven = codes.verify(&challenge_id, &code).await?;
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

        // 3. Gate issuance on the account being active; read authoritative perms.
        //    Signing back in is how a holder undoes their own deactivation: the
        //    credential was just proven, so the account resumes (a suspension
        //    is never lifted here).
        let snapshot = self.directory.lookup(&account_id).await?;
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

        // 4. Establish the immutable subject → account link on first login.
        let mut first_link = false;
        if needs_link {
            let mut link = SubjectLink::establish(subject.clone(), account_id, now, correlation_id);
            self.links.save(&link).await?;
            for event in &link.drain_events() {
                self.publisher.publish(event).await?;
            }
            first_link = true;
        }

        // 5. Issue the session, its refresh token and the edge access token.
        let issued = self
            .members
            .issue(account_id, subject, cmd.device, permissions, age_bracket, now, correlation_id)
            .await?;

        // 6. The guest this device was is now this member.
        if let (Some(token), Some(guests)) = (cmd.guest_refresh_token.as_deref(), &self.guests) {
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
            },
        )
    }

    #[tokio::test]
    async fn first_login_links_account_and_issues_tokens() {
        let fx = Fixture::new();
        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap();

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
        let first = fx.login_handler().handle(password_login(), t0()).await.unwrap();
        let second = fx.login_handler().handle(password_login(), t0()).await.unwrap();

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

        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap();

        assert!(issued.reactivated, "the app welcomes the holder back");
        assert_eq!(fx.directory.resumed(), vec![account]);
        assert_eq!(fx.sessions.count(), 1);

        // The next login finds it active: nothing more to resume.
        let again = fx.login_handler().handle(password_login(), t0()).await.unwrap();
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

        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap();
        let claims = fx.minter.verify_access(&issued.access_token).await.unwrap();
        assert_eq!(claims.account_id, account);
        assert_eq!(claims.profile_ids, profiles);
    }

    #[tokio::test]
    async fn login_survives_a_profile_directory_outage_with_no_profile_grants() {
        let mut fx = Fixture::new();
        fx.profiles = std::sync::Arc::new(crate::application::fakes::StubProfileDirectory::failing());

        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap();
        let claims = fx.minter.verify_access(&issued.access_token).await.unwrap();
        assert!(claims.profile_ids.is_empty(), "outage degrades to no profile grants");
        assert_eq!(fx.sessions.count(), 1, "the session is still issued");
    }

    #[tokio::test]
    async fn session_is_issued_under_current_generation() {
        let fx = Fixture::new();
        let issued = fx.login_handler().handle(password_login(), t0()).await.unwrap();
        let session = fx.sessions.find_by_id(&issued.session_id).await.unwrap().unwrap();
        assert_eq!(session.generation(), Generation::INITIAL);
    }
}

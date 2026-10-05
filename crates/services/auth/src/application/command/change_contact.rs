//! A signed-in holder changes their email or phone (#651). They prove the new
//! address with a one-time code (`StartVerification` to it, then the code
//! here), on a session that proved a credential recently (the edge's step-up).
//! Everything that signs them in follows the address: the account's contact
//! (`account.ChangeEmail` / `ChangePhone`), the IdP user's email for a
//! password account, and the passwordless sign-in link (`urn:core-platform:
//! email|phone`). The address on file before is told, so a takeover is visible
//! to the real owner.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use super::credentials::caller_session;
use super::verification::VerificationCodes;
use crate::application::port::{
    AccountDirectory, CredentialAdmin, EventPublisher, SessionCache, SessionRepository,
    SubjectLinkRepository, VerificationChannel,
};
use crate::domain::aggregate::SubjectLink;
use crate::domain::value_object::{
    AccountId, IdpSubject, SessionId, APPLE_ISSUER, EMAIL_CODE_ISSUER, GOOGLE_ISSUERS, PHONE_CODE_ISSUER,
};
use crate::error::AuthError;

#[derive(Clone)]
pub struct ChangeContactCommand {
    pub account_id:   String,
    pub session_id:   String,
    /// The challenge `StartVerification` opened for the new address.
    pub challenge_id: String,
    pub code:         String,
    /// The caller's address as the transport saw it.
    pub client_ip:    Option<String>,
    /// The notice to the old address is written in it.
    pub locale:       Option<String>,
}

impl std::fmt::Debug for ChangeContactCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeContactCommand")
            .field("account_id", &self.account_id)
            .field("session_id", &self.session_id)
            .field("challenge_id", &self.challenge_id)
            .finish_non_exhaustive()
    }
}

/// The address now on the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedContact {
    pub channel:     VerificationChannel,
    pub destination: String,
}

/// The issuer of the passwordless sign-in link for `channel`.
fn code_issuer(channel: VerificationChannel) -> &'static str {
    match channel {
        VerificationChannel::Email => EMAIL_CODE_ISSUER,
        VerificationChannel::Sms => PHONE_CODE_ISSUER,
    }
}

/// A link our IdP (Keycloak) manages: not a passwordless code link, not Apple
/// or Google (their email is theirs).
fn idp_managed(subject: &IdpSubject) -> bool {
    let issuer = subject.issuer();
    issuer != EMAIL_CODE_ISSUER
        && issuer != PHONE_CODE_ISSUER
        && issuer != APPLE_ISSUER
        && !GOOGLE_ISSUERS.contains(&issuer)
}

pub struct ChangeContactHandler {
    codes:     Arc<VerificationCodes>,
    accounts:  Arc<dyn AccountDirectory>,
    links:     Arc<dyn SubjectLinkRepository>,
    admin:     Arc<dyn CredentialAdmin>,
    sessions:  Arc<dyn SessionRepository>,
    cache:     Arc<dyn SessionCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl ChangeContactHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        codes: Arc<VerificationCodes>,
        accounts: Arc<dyn AccountDirectory>,
        links: Arc<dyn SubjectLinkRepository>,
        admin: Arc<dyn CredentialAdmin>,
        sessions: Arc<dyn SessionRepository>,
        cache: Arc<dyn SessionCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { codes, accounts, links, admin, sessions, cache, publisher }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<ChangeContactCommand>,
        now: DateTime<Utc>,
    ) -> Result<ChangedContact, AuthError> {
        let correlation_id = envelope.correlation_id;
        let cmd = envelope.payload;
        let account_id = AccountId::try_from(cmd.account_id.as_str())?;
        let session_id = SessionId::try_from(cmd.session_id.as_str())?;
        caller_session(&self.sessions, &self.cache, &account_id, &session_id, now).await?;

        // The new address, proven (the challenge is spent).
        let proven = self.codes.verify(&cmd.challenge_id, &cmd.code, cmd.client_ip.as_deref()).await?;
        let (channel, destination) = (proven.channel, proven.destination);
        let changed = ChangedContact { channel, destination: destination.clone() };

        let before = self.accounts.contact(&account_id).await?;
        let current = match channel {
            VerificationChannel::Email => before.email.as_deref(),
            VerificationChannel::Sms => before.phone.as_deref(),
        };
        if current.is_some_and(|c| c.eq_ignore_ascii_case(&destination)) {
            return Ok(changed);
        }
        let holder = match channel {
            VerificationChannel::Email => self.accounts.find_by_email(&destination).await?,
            VerificationChannel::Sms => self.accounts.find_by_phone(&destination).await?,
        };
        if holder.is_some_and(|h| h.account_id != account_id) {
            return Err(match channel {
                VerificationChannel::Email => AuthError::EmailAlreadyRegistered,
                VerificationChannel::Sms => AuthError::PhoneAlreadyRegistered,
            });
        }

        let links = self.links.find_by_account(&account_id).await?;
        // The IdP first: if it refuses (the address is another IdP user's),
        // nothing has moved.
        let mut idp_moved: Vec<&IdpSubject> = Vec::new();
        if channel == VerificationChannel::Email {
            for link in links.iter().filter(|l| idp_managed(l.subject())) {
                self.admin.set_email(link.subject(), &destination).await?;
                idp_moved.push(link.subject());
            }
        }
        if let Err(error) = self.accounts.change_contact(&account_id, channel, &destination).await {
            // The account refused (a race on the address since the check): put
            // the IdP back, so the two never disagree (best effort; a retry
            // with a fresh code heals the rest).
            if let Some(old) = before.email.as_deref() {
                for subject in idp_moved {
                    if let Err(restore) = self.admin.set_email(subject, old).await {
                        tracing::error!(%restore, "IdP email not restored after a refused account change");
                    }
                }
            }
            return Err(error);
        }

        // A passwordless account signs in by the new address, not the old one:
        // the new link first, so a failure leaves the old one working.
        let issuer = code_issuer(channel);
        for old in links.iter().filter(|l| l.subject().issuer() == issuer) {
            let mut link = SubjectLink::establish(IdpSubject::new(issuer, destination.clone())?, account_id, now, correlation_id);
            match self.links.save(&link).await {
                Ok(()) | Err(AuthError::SubjectAlreadyLinked { .. }) => {}
                Err(e) => return Err(e),
            }
            for event in &link.drain_events() {
                self.publisher.publish(event).await?;
            }
            self.links.delete(old.subject()).await?;
        }

        // Tell the address on file before (email only: an SMS costs money and is
        // a pumping vector — a phone change tells the email on file).
        if let Some(email) = before.email.filter(|e| !e.eq_ignore_ascii_case(&destination))
            && let Err(error) = self.codes.notify_contact_changed(channel, &email, cmd.locale.as_deref()).await
        {
            tracing::warn!(%error, "contact-changed notice not sent");
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::verification::{StartVerificationCommand, VerificationPolicy};
    use crate::application::fakes::{t0, Fixture, InMemoryVerificationStore, RecordingCodeSender};
    use crate::application::port::ContactDetails;
    use uuid::Uuid;

    struct World {
        fx:      Fixture,
        sender:  Arc<RecordingCodeSender>,
        codes:   Arc<VerificationCodes>,
        handler: ChangeContactHandler,
    }

    fn world() -> World {
        let fx = Fixture::new();
        let sender = Arc::new(RecordingCodeSender::default());
        let codes = Arc::new(VerificationCodes::new(
            Arc::new(InMemoryVerificationStore::default()),
            Arc::clone(&sender) as _,
            VerificationPolicy::default(),
        ));
        let handler = ChangeContactHandler::new(
            Arc::clone(&codes),
            Arc::clone(&fx.directory) as _,
            Arc::clone(&fx.links) as _,
            Arc::clone(&fx.credentials) as _,
            Arc::clone(&fx.sessions) as _,
            Arc::clone(&fx.cache) as _,
            Arc::clone(&fx.publisher) as _,
        );
        World { fx, sender, codes, handler }
    }

    /// A member signed in with a password (the IdP link `https://idp.test`),
    /// who also signs in with codes at `old@example.com`.
    async fn member(w: &World) -> (AccountId, SessionId) {
        use crate::application::command::LoginCommand;
        use crate::application::port::AuthnGrant;
        use crate::domain::value_object::DeviceFingerprint;
        w.fx.idp.with_password("pw");
        let cmd = LoginCommand {
            grant: AuthnGrant::Password { username: "user".into(), password: "pw".into() },
            device: DeviceFingerprint::default(),
            guest_refresh_token: None,
            client_ip: None,
        };
        let issued = w.fx.login_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap().issued().unwrap();
        let code_link = IdpSubject::new(EMAIL_CODE_ISSUER, "old@example.com").unwrap();
        w.fx.links.save(&SubjectLink::establish(code_link, issued.account_id, t0(), Uuid::now_v7())).await.unwrap();
        w.fx.directory.with_contact(issued.account_id, ContactDetails { email: Some("old@example.com".into()), phone: None });
        (issued.account_id, issued.session_id)
    }

    async fn code_to(w: &World, channel: VerificationChannel, to: &str) -> (String, String) {
        let started = w
            .codes
            .start(StartVerificationCommand { channel, destination: to.into(), locale: None, client_ip: None })
            .await
            .unwrap();
        (started.challenge_id, w.sender.last().unwrap().1)
    }

    fn change(on: &(AccountId, SessionId), (challenge_id, code): (String, String)) -> Envelope<ChangeContactCommand> {
        Envelope::new(
            Uuid::now_v7(),
            ChangeContactCommand {
                account_id: on.0.as_str(),
                session_id: on.1.as_str(),
                challenge_id,
                code,
                client_ip: None,
                locale: Some("fr".into()),
            },
        )
    }

    #[tokio::test]
    async fn a_proven_email_moves_everything_that_signs_the_holder_in() {
        let w = world();
        let me = member(&w).await;
        let proof = code_to(&w, VerificationChannel::Email, "New@Example.com").await;

        let changed = w.handler.handle(change(&me, proof), t0()).await.unwrap();
        assert_eq!(changed.destination, "new@example.com");
        // The account, the IdP user, and the code link follow the address.
        assert_eq!(w.fx.directory.contact_of(&me.0).email.as_deref(), Some("new@example.com"));
        let idp = IdpSubject::new("https://idp.test", "sub-123").unwrap();
        assert_eq!(w.fx.credentials.emails_set(), vec![(idp, "new@example.com".to_owned())]);
        let new_link = IdpSubject::new(EMAIL_CODE_ISSUER, "new@example.com").unwrap();
        let old_link = IdpSubject::new(EMAIL_CODE_ISSUER, "old@example.com").unwrap();
        assert!(w.fx.links.find_by_subject(&new_link).await.unwrap().is_some(), "codes to the new address sign in");
        assert!(w.fx.links.find_by_subject(&old_link).await.unwrap().is_none(), "the old address no longer does");
        // The old address is told.
        assert_eq!(w.sender.contact_notices(), vec![(VerificationChannel::Email, "old@example.com".to_owned())]);
    }

    #[tokio::test]
    async fn a_proven_phone_moves_the_account_and_tells_the_email_on_file() {
        let w = world();
        let me = member(&w).await;
        let proof = code_to(&w, VerificationChannel::Sms, "+33612345678").await;

        w.handler.handle(change(&me, proof), t0()).await.unwrap();
        assert_eq!(w.fx.directory.contact_of(&me.0).phone.as_deref(), Some("+33612345678"));
        assert!(w.fx.credentials.emails_set().is_empty(), "the IdP keeps its email");
        assert_eq!(w.sender.contact_notices(), vec![(VerificationChannel::Sms, "old@example.com".to_owned())]);
    }

    #[tokio::test]
    async fn another_accounts_address_or_a_wrong_code_changes_nothing() {
        let w = world();
        let me = member(&w).await;
        let other = IdpSubject::new(EMAIL_CODE_ISSUER, "taken@example.com").unwrap();
        w.fx.directory.with_email("taken@example.com", &other, AccountId::from_uuid(Uuid::now_v7()));

        let proof = code_to(&w, VerificationChannel::Email, "taken@example.com").await;
        let err = w.handler.handle(change(&me, proof), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::EmailAlreadyRegistered), "{err:?}");

        let (challenge, _) = code_to(&w, VerificationChannel::Email, "new@example.com").await;
        let err = w.handler.handle(change(&me, (challenge, "000000".into())), t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::VerificationCodeInvalid), "{err:?}");

        assert_eq!(w.fx.directory.contact_of(&me.0).email.as_deref(), Some("old@example.com"));
        assert!(w.fx.credentials.emails_set().is_empty());
        assert!(w.sender.contact_notices().is_empty());
    }

    #[tokio::test]
    async fn a_refused_account_change_puts_the_idp_email_back() {
        let w = world();
        let me = member(&w).await;
        let proof = code_to(&w, VerificationChannel::Email, "new@example.com").await;
        w.fx.directory.refuse_contact_changes();

        assert!(w.handler.handle(change(&me, proof), t0()).await.is_err());
        let idp = IdpSubject::new("https://idp.test", "sub-123").unwrap();
        assert_eq!(
            w.fx.credentials.emails_set(),
            vec![(idp.clone(), "new@example.com".to_owned()), (idp, "old@example.com".to_owned())],
            "moved, then put back"
        );
        assert!(w.sender.contact_notices().is_empty(), "nothing changed: nobody is told");
    }

    #[tokio::test]
    async fn only_the_callers_own_live_member_session_may_change_it() {
        let w = world();
        let me = member(&w).await;
        let proof = code_to(&w, VerificationChannel::Email, "new@example.com").await;
        let someone_else = (AccountId::from_uuid(Uuid::now_v7()), me.1);
        assert!(w.handler.handle(change(&someone_else, proof), t0()).await.is_err());
        assert_eq!(w.fx.directory.contact_of(&me.0).email.as_deref(), Some("old@example.com"));
    }
}

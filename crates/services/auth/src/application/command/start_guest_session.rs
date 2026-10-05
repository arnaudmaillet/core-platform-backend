use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use uuid::Uuid;
use validate_core::{FieldViolation, Validate};

use crate::application::command::guest_attestation::{AttestationProof, GuestAttestation};
use crate::application::command::IssuedSession;
use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    GuestRecord, GuestRegistry, RefreshTokenRepository, SessionCache, SessionRepository,
    TokenMinter,
};
use crate::domain::aggregate::{RefreshToken, RefreshTokenIssueParams, Session, SessionIssueParams};
use crate::domain::value_object::{AccountId, DeviceFingerprint, IdpSubject, Permission, SessionKind};
use crate::error::AuthError;

/// The synthetic IdP issuer of guest sessions (sessions carry an IdP subject;
/// a guest has none, so it is its own subject under this issuer).
pub const GUEST_ISSUER: &str = "urn:core-platform:guest";

/// Start an anonymous guest session for an app installation (guest mode).
#[derive(Debug, Clone)]
pub struct StartGuestSessionCommand {
    /// Must carry the installation's `device_id`: the session is bound to it,
    /// the token's `did` comes from it, and the welcome gift is once per device.
    pub device: DeviceFingerprint,
    /// App Attest: base64 of the attestation object, for `attest_challenge`
    /// and the key `attest_key_id` (verified per `AUTH_APP_ATTEST_MODE`).
    pub attestation: Option<String>,
    pub attest_key_id: Option<String>,
    pub attest_challenge: Option<String>,
    pub locale: Option<String>,
    /// Store / region hint for discovery.
    pub region_hint: Option<String>,
    /// ISO country derived on the device; verified against GeoIP later (B10).
    pub current_country: Option<String>,
}

impl Validate for StartGuestSessionCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.device.device_id().is_none_or(|d| d.trim().is_empty()) {
            return Err(vec![FieldViolation::new(
                "device.device_id",
                "AUT-VAL-020",
                "a guest session needs the installation's device_id",
            )]);
        }
        Ok(())
    }
}

/// Issues a guest session: a fresh guest id, a session of kind `Guest`, a
/// refresh token, and an edge token with `read:public` only and no profiles.
/// No account is created or looked up. The session's `SessionIssued` event is
/// not published: the audit plane records accounts, and a guest is none.
pub struct StartGuestSessionHandler {
    sessions: Arc<dyn SessionRepository>,
    refresh_tokens: Arc<dyn RefreshTokenRepository>,
    cache: Arc<dyn SessionCache>,
    minter: Arc<dyn TokenMinter>,
    guests: Arc<dyn GuestRegistry>,
    policy: SessionPolicy,
    /// Kill switch: `StartGuestSession` is a credential-free, edge-public write
    /// into this TIER-0 store; until the abuse controls (B5) front it, an
    /// environment can turn it off.
    enabled: bool,
    /// App Attest (B5b); `None` = not configured (as `off`).
    attestation: Option<Arc<GuestAttestation>>,
}

impl StartGuestSessionHandler {
    pub fn new(
        sessions: Arc<dyn SessionRepository>,
        refresh_tokens: Arc<dyn RefreshTokenRepository>,
        cache: Arc<dyn SessionCache>,
        minter: Arc<dyn TokenMinter>,
        guests: Arc<dyn GuestRegistry>,
        policy: SessionPolicy,
        enabled: bool,
    ) -> Self {
        Self { sessions, refresh_tokens, cache, minter, guests, policy, enabled, attestation: None }
    }

    /// Checks App Attest per its mode before a guest session is issued.
    pub fn with_attestation(mut self, attestation: Arc<GuestAttestation>) -> Self {
        self.attestation = Some(attestation);
        self
    }

    pub async fn handle(
        &self,
        envelope: Envelope<StartGuestSessionCommand>,
        now: DateTime<Utc>,
    ) -> Result<IssuedSession, AuthError> {
        if !self.enabled {
            return Err(AuthError::GuestSessionsDisabled);
        }
        ensure_valid(&envelope.payload)?;
        let cmd = envelope.payload;
        let correlation_id = envelope.correlation_id;

        // A genuine install of our app (App Attest), before anything is written.
        let attested = match &self.attestation {
            Some(attestation) => {
                let proof = match (&cmd.attest_key_id, &cmd.attestation, &cmd.attest_challenge) {
                    (Some(key_id), Some(attestation), Some(challenge)) => Some(AttestationProof {
                        key_id: key_id.clone(),
                        attestation: attestation.clone(),
                        challenge: challenge.clone(),
                    }),
                    _ => None,
                };
                attestation.check(proof, now).await?
            }
            None => None,
        };

        let guest_id = AccountId::from_uuid(Uuid::now_v7());
        let subject = IdpSubject::new(GUEST_ISSUER, guest_id.as_str())?;

        self.guests
            .record(&GuestRecord {
                guest_id,
                device_id: cmd.device.device_id().unwrap_or_default().to_owned(),
                attestation_sent: cmd.attestation.as_deref().is_some_and(|a| !a.is_empty()),
                attest_key_id: attested.map(|device| device.key_id),
                locale: cmd.locale,
                region_hint: cmd.region_hint,
                current_country: cmd.current_country,
                first_seen_at: now,
            })
            .await?;

        let generation = self.cache.current_generation(&guest_id).await?;
        let mut session = Session::issue(SessionIssueParams {
            kind: SessionKind::Guest,
            account_id: guest_id,
            subject,
            generation,
            device: cmd.device,
            issued_at: now,
            expires_at: now + self.policy.session_ttl,
            absolute_expiry: now + self.policy.absolute_ttl,
            correlation_id,
        })?;
        self.sessions.save(&session).await?;
        let _ = session.drain_events(); // not an account: nothing to audit

        let generated = self.minter.generate_refresh()?;
        let refresh = RefreshToken::issue(RefreshTokenIssueParams {
            session_id: session.id(),
            account_id: guest_id,
            token_hash: generated.hash,
            issued_at: now,
            expires_at: now + self.policy.refresh_ttl,
        })?;
        self.refresh_tokens.save(&refresh).await?;

        let claims = session.mint_access_token(
            now,
            self.policy.access_ttl,
            vec![Permission::read_public()],
            Vec::new(),
        )?;
        let access_token = self.minter.mint_access(&claims).await?;

        Ok(IssuedSession {
            account_id: guest_id,
            session_id: session.id(),
            access_token,
            refresh_token: generated.plaintext,
            access_expires_in: claims.expires_in_secs(now),
            first_link: false,
            reactivated: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::RefreshCommand;
    use crate::application::fakes::Fixture;
    use crate::application::port::TokenMinter;
    use crate::domain::value_object::READ_PUBLIC;

    fn cmd(device_id: Option<&str>) -> StartGuestSessionCommand {
        StartGuestSessionCommand {
            device: DeviceFingerprint::new(None, None, device_id.map(str::to_owned)),
            attestation: Some("assertion".into()),
            attest_key_id: None,
            attest_challenge: None,
            locale: Some("fr-FR".into()),
            region_hint: Some("FR".into()),
            current_country: None,
        }
    }

    #[tokio::test]
    async fn a_guest_session_reads_public_only_and_records_the_device() {
        let fx = Fixture::new();
        let now = Utc::now();
        let issued = fx
            .start_guest_handler()
            .handle(Envelope::new(Uuid::now_v7(), cmd(Some("install-1"))), now)
            .await
            .unwrap();

        let claims = fx.minter.verify_access(&issued.access_token).await.unwrap();
        assert_eq!(claims.kind, SessionKind::Guest);
        assert_eq!(claims.account_id, issued.account_id);
        assert_eq!(claims.permissions, vec![Permission::read_public()]);
        assert!(claims.profile_ids.is_empty());
        assert_eq!(claims.device_id.as_deref(), Some("install-1"));

        let records = fx.guests.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].guest_id, issued.account_id);
        assert_eq!(records[0].device_id, "install-1");
        assert!(records[0].attestation_sent);
        assert_eq!(fx.publisher.count(), 0, "no account, nothing published");
        // No account was looked up or created.
        assert!(fx.directory.lookups().is_empty());
    }

    #[tokio::test]
    async fn enforced_app_attest_gates_the_session_and_records_the_attested_key() {
        use crate::application::command::guest_attestation::{
            AttestMode, AttestedDevice, DeviceAttestationVerifier, DeviceQuota, GuestAttestation,
        };
        use crate::application::fakes::InMemoryNonceStore;

        struct Genuine;
        impl DeviceAttestationVerifier for Genuine {
            fn verify(&self, proof: &AttestationProof, _: DateTime<Utc>) -> Result<AttestedDevice, String> {
                Ok(AttestedDevice { key_id: proof.key_id.clone(), environment: "production".into() })
            }
        }
        struct Unlimited;
        #[async_trait::async_trait]
        impl DeviceQuota for Unlimited {
            async fn admit(&self, _: &str, _: u32) -> Result<bool, AuthError> {
                Ok(true)
            }
        }

        let fx = Fixture::new();
        let attestation = Arc::new(GuestAttestation::new(
            AttestMode::Enforce,
            Arc::new(Genuine),
            Arc::new(InMemoryNonceStore::default()),
            Arc::new(Unlimited),
            5,
        ));
        let handler = fx.start_guest_handler().with_attestation(Arc::clone(&attestation));

        // No attestation: refused before anything is written.
        let err = handler.handle(Envelope::new(Uuid::now_v7(), cmd(Some("install-1"))), Utc::now()).await.unwrap_err();
        assert!(matches!(err, AuthError::DeviceAttestationRequired));
        assert!(fx.guests.records.lock().unwrap().is_empty());

        // Attested for a challenge of ours: the session starts, the key is kept.
        let challenge = attestation.start().await.unwrap().challenge;
        let mut attested = cmd(Some("install-1"));
        attested.attest_key_id = Some("key-1".into());
        attested.attest_challenge = Some(challenge);
        handler.handle(Envelope::new(Uuid::now_v7(), attested), Utc::now()).await.unwrap();
        assert_eq!(fx.guests.records.lock().unwrap()[0].attest_key_id.as_deref(), Some("key-1"));
    }

    #[tokio::test]
    async fn a_guest_refresh_keeps_read_public_and_never_asks_the_directory() {
        let fx = Fixture::new();
        let now = Utc::now();
        let issued = fx
            .start_guest_handler()
            .handle(Envelope::new(Uuid::now_v7(), cmd(Some("install-1"))), now)
            .await
            .unwrap();
        let refreshed = fx
            .refresh_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    RefreshCommand {
                        refresh_token: issued.refresh_token,
                        device: DeviceFingerprint::new(None, None, Some("install-1".into())),
                    },
                ),
                now,
            )
            .await
            .unwrap();
        let claims = fx.minter.verify_access(&refreshed.access_token).await.unwrap();
        assert_eq!(claims.kind, SessionKind::Guest);
        assert_eq!(claims.permissions.iter().map(Permission::as_str).collect::<Vec<_>>(), vec![READ_PUBLIC]);
        assert!(claims.profile_ids.is_empty());
        assert!(fx.directory.lookups().is_empty());
    }

    #[tokio::test]
    async fn the_kill_switch_refuses_guest_sessions_and_writes_nothing() {
        let fx = Fixture::new();
        let handler = StartGuestSessionHandler::new(
            Arc::clone(&fx.sessions) as _,
            Arc::clone(&fx.refresh_tokens) as _,
            Arc::clone(&fx.cache) as _,
            Arc::clone(&fx.minter) as _,
            Arc::clone(&fx.guests) as _,
            fx.policy.clone(),
            false,
        );
        let err = handler
            .handle(Envelope::new(Uuid::now_v7(), cmd(Some("install-1"))), Utc::now())
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::GuestSessionsDisabled));
        assert!(fx.guests.records.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_guest_session_needs_a_device_id() {
        let fx = Fixture::new();
        for device in [None, Some("  ")] {
            let err = fx
                .start_guest_handler()
                .handle(Envelope::new(Uuid::now_v7(), cmd(device)), Utc::now())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("device_id"), "{err}");
        }
    }
}

//! Cross-crate proof: an edge token **minted** by this service's `Es256TokenMinter`
//! is **verified** by the very `auth-context` decoder every downstream service runs
//! on its inbound path. This is the contract that makes the split-token hot path
//! work — issuance and verification must agree on algorithm, claims, and `kid`.
//!
//! No container required; it's pure crypto + claim mapping, so it runs in the
//! default `cargo test -p auth`.

use std::collections::HashMap;

use auth::application::SessionPolicy;
use auth::application::port::TokenMinter;
use auth::domain::value_object::{
    AccessTokenClaims, AccountId, Generation, Permission, SessionId, SessionKind,
};
use auth::infrastructure::token::{Es256TokenMinter, EsKeyMaterial};

use auth_context::{
    AuthContextConfig, ClaimsExtractor, CurrentPrincipal, JwksCache, JwtDecoder,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey};
use serde::Deserialize;
use uuid::Uuid;

const KID: &str = "auth-es256-1";
const ISSUER: &str = "https://auth.core-platform";
const AUDIENCE: &str = "core-platform";

/// Generates an ephemeral P-256 keypair (PKCS#8 private PEM, SPKI public PEM).
/// No key material is hardcoded.
fn keypair() -> (Vec<u8>, Vec<u8>) {
    use p256::ecdsa::SigningKey;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
    let signing = SigningKey::random(&mut rand_core::OsRng);
    let private_pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes().to_vec();
    let public_pem = signing.verifying_key().to_public_key_pem(LineEnding::LF).unwrap().into_bytes();
    (private_pem, public_pem)
}

/// The claim subset a downstream service cares about. `auth-context` deserializes
/// the token into this `C`, then the extractor lifts it to a `CurrentPrincipal`.
#[derive(Debug, Clone, Deserialize)]
struct EdgeClaims {
    sub: String,
    #[serde(default)]
    perms: Vec<String>,
}

struct EdgeExtractor;

impl ClaimsExtractor<EdgeClaims> for EdgeExtractor {
    fn extract(
        &self,
        raw: EdgeClaims,
    ) -> Result<CurrentPrincipal<EdgeClaims>, auth_context::AuthError> {
        Ok(CurrentPrincipal {
            user_id: auth_context::PrincipalId(raw.sub.clone()),
            tenant_id: None,
            permissions: raw.perms.iter().cloned().map(auth_context::Permission).collect(),
            raw_claims: raw,
        })
    }
}

fn minter(private_pem: &[u8], public_pem: &[u8], audience: &str) -> Es256TokenMinter {
    Es256TokenMinter::from_pem(EsKeyMaterial {
        private_pem: private_pem.to_vec(),
        public_pem: public_pem.to_vec(),
        key_id: KID.to_owned(),
        issuer: ISSUER.to_owned(),
        audience: audience.to_owned(),
    })
    .unwrap()
}

async fn decoder(public_pem: &[u8]) -> JwtDecoder<EdgeClaims, EdgeExtractor> {
    let cache = JwksCache::new();
    let mut keys = HashMap::new();
    keys.insert(KID.to_owned(), DecodingKey::from_ec_pem(public_pem).unwrap());
    cache.replace(keys).await;

    let config = AuthContextConfig {
        expected_issuer: Some(ISSUER.to_owned()),
        expected_audience: Some(AUDIENCE.to_owned()),
        ..AuthContextConfig::default()
    };
    JwtDecoder::with_algorithms(&config, cache, EdgeExtractor, vec![Algorithm::ES256])
}

#[tokio::test]
async fn minted_edge_token_is_verified_by_auth_context() {
    let _ = SessionPolicy::new(
        Duration::minutes(10),
        Duration::minutes(30),
        Duration::hours(8),
        Duration::days(7),
    );

    let account = AccountId::from_uuid(Uuid::now_v7());
    let session = SessionId::new();
    let claims = AccessTokenClaims {
        account_id: account,
        session_id: session,
        generation: Generation::from_i64(7),
        permissions: vec![Permission::new("posts:write"), Permission::new("ROLE_ADMIN")],
        profile_ids: Vec::new(),
        device_id: None,
        kind: SessionKind::Member,
        auth_time: None,
        issued_at: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(10),
    };

    let (private_pem, public_pem) = keypair();
    let token = minter(&private_pem, &public_pem, AUDIENCE).mint_access(&claims).await.expect("mint");
    let principal =
        decoder(&public_pem).await.decode(&token).await.expect("auth-context verifies the token");

    assert_eq!(principal.user_id.0, account.as_str());
    let perms: Vec<&str> = principal.permissions.iter().map(|p| p.0.as_str()).collect();
    assert_eq!(perms, vec!["posts:write", "ROLE_ADMIN"]);
}

#[tokio::test]
async fn auth_context_rejects_a_token_for_a_different_audience() {
    // Same key (so the signature is valid) but a different audience — our decoder
    // must reject it on the `aud` check.
    let (private_pem, public_pem) = keypair();
    let other = minter(&private_pem, &public_pem, "some-other-service");

    let claims = AccessTokenClaims {
        account_id: AccountId::from_uuid(Uuid::now_v7()),
        session_id: SessionId::new(),
        generation: Generation::INITIAL,
        permissions: vec![],
        profile_ids: Vec::new(),
        device_id: None,
        kind: SessionKind::Member,
        auth_time: None,
        issued_at: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(10),
    };
    let token = other.mint_access(&claims).await.unwrap();
    assert!(decoder(&public_pem).await.decode(&token).await.is_err());
}

/// The realtime handshake reads the device from `did` on the **fleet** edge
/// decoder (`auth_context::edge`), not on a bespoke claim struct. Prove the claim
/// survives that exact path, under the constant realtime defaults to.
#[tokio::test]
async fn fleet_edge_decoder_sees_the_did_claim_realtime_requires() {
    let (private_pem, public_pem) = keypair();
    let claims = AccessTokenClaims {
        account_id: AccountId::from_uuid(Uuid::now_v7()),
        session_id: SessionId::new(),
        generation: Generation::INITIAL,
        permissions: vec![],
        profile_ids: Vec::new(),
        device_id: Some("ios-install-1".to_owned()),
        kind: SessionKind::Member,
        auth_time: None,
        issued_at: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(10),
    };
    let token = minter(&private_pem, &public_pem, AUDIENCE).mint_access(&claims).await.unwrap();

    let cache = JwksCache::new();
    let mut keys = HashMap::new();
    keys.insert(KID.to_owned(), DecodingKey::from_ec_pem(&public_pem).unwrap());
    cache.replace(keys).await;
    let config = AuthContextConfig {
        expected_issuer: Some(ISSUER.to_owned()),
        expected_audience: Some(AUDIENCE.to_owned()),
        ..AuthContextConfig::default()
    };
    let principal = auth_context::edge::edge_decoder(&config, cache)
        .decode(&token)
        .await
        .expect("fleet edge decoder verifies the token");

    let did = principal
        .raw_claims
        .extra
        .get(auth_context::edge::EDGE_DEVICE_CLAIM)
        .and_then(|v| v.as_str());
    assert_eq!(did, Some("ios-install-1"));
}

/// The step-up gate reads `auth_time` off the **fleet** edge decoder
/// (`auth_context::edge::auth_time`): a token minted after a credential proof
/// carries it, a refreshed one does not.
#[tokio::test]
async fn fleet_edge_decoder_sees_auth_time_only_when_minted_after_a_credential_proof() {
    let (private_pem, public_pem) = keypair();
    let proved_at = Utc::now() - Duration::seconds(30);
    let base = AccessTokenClaims {
        account_id: AccountId::from_uuid(Uuid::now_v7()),
        session_id: SessionId::new(),
        generation: Generation::INITIAL,
        permissions: vec![],
        profile_ids: Vec::new(),
        device_id: None,
        kind: SessionKind::Member,
        auth_time: Some(proved_at),
        issued_at: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(10),
    };
    let minter = minter(&private_pem, &public_pem, AUDIENCE);

    let cache = JwksCache::new();
    let mut keys = HashMap::new();
    keys.insert(KID.to_owned(), DecodingKey::from_ec_pem(&public_pem).unwrap());
    cache.replace(keys).await;
    let config = AuthContextConfig {
        expected_issuer: Some(ISSUER.to_owned()),
        expected_audience: Some(AUDIENCE.to_owned()),
        ..AuthContextConfig::default()
    };
    let decoder = auth_context::edge::edge_decoder(&config, cache);

    let stepped_up = decoder.decode(&minter.mint_access(&base).await.unwrap()).await.unwrap();
    assert_eq!(auth_context::edge::auth_time(&stepped_up.raw_claims), Some(proved_at.timestamp()));

    let refreshed = AccessTokenClaims { auth_time: None, ..base };
    let plain = decoder.decode(&minter.mint_access(&refreshed).await.unwrap()).await.unwrap();
    assert_eq!(auth_context::edge::auth_time(&plain.raw_claims), None);
}

/// A guest token as the fleet edge decoder sees it: `kind = "guest"`, a
/// `guest:`-prefixed subject, `read:public` only, no profiles.
#[tokio::test]
async fn fleet_edge_decoder_recognises_a_guest_token() {
    let (private_pem, public_pem) = keypair();
    let guest = AccountId::from_uuid(Uuid::now_v7());
    let claims = AccessTokenClaims {
        account_id: guest,
        session_id: SessionId::new(),
        generation: Generation::INITIAL,
        permissions: vec![Permission::read_public()],
        profile_ids: Vec::new(),
        device_id: Some("install-1".to_owned()),
        kind: SessionKind::Guest,
        auth_time: None,
        issued_at: Utc::now(),
        expires_at: Utc::now() + Duration::minutes(10),
    };
    let token = minter(&private_pem, &public_pem, AUDIENCE).mint_access(&claims).await.unwrap();

    let cache = JwksCache::new();
    let mut keys = HashMap::new();
    keys.insert(KID.to_owned(), DecodingKey::from_ec_pem(&public_pem).unwrap());
    cache.replace(keys).await;
    let config = AuthContextConfig {
        expected_issuer: Some(ISSUER.to_owned()),
        expected_audience: Some(AUDIENCE.to_owned()),
        ..AuthContextConfig::default()
    };
    let principal = auth_context::edge::edge_decoder(&config, cache).decode(&token).await.unwrap();

    assert!(auth_context::edge::is_guest(&principal.raw_claims));
    assert_eq!(principal.user_id.0, format!("guest:{}", guest.as_str()));
    assert!(principal.has_permission("read:public"));
    assert_eq!(auth_context::edge::profile_ids(&principal.raw_claims).count(), 0);
}

/// The step-up window auth reports to clients is the one verifiers enforce.
#[test]
fn the_step_up_window_auth_reports_is_the_one_verifiers_enforce() {
    assert_eq!(auth::application::command::STEP_UP_WINDOW_SECS, auth_context::edge::STEP_UP_MAX_AGE_SECS);
}

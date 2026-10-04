use std::collections::HashMap;

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use jsonwebtoken::{decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::pkcs8::DecodePublicKey;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::application::port::{GeneratedRefresh, TokenMinter};
use crate::domain::value_object::{
    AccessTokenClaims, AccountId, AgeBracket, Generation, Permission, ProfileId, RefreshTokenHash,
    SessionId,
    SessionKind,
};
use crate::error::AuthError;

/// PEM key material + token addressing for the ES256 minter's **active** key.
pub struct EsKeyMaterial {
    /// PKCS#8 (or SEC1) PEM of the P-256 signing key.
    pub private_pem: Vec<u8>,
    /// SPKI PEM of the corresponding public key (also published via JWKS).
    pub public_pem: Vec<u8>,
    /// `kid` stamped into the header so verifiers select the right JWKS key.
    pub key_id: String,
    pub issuer: String,
    pub audience: String,
}

/// A public key still **accepted for verification** during a rotation, after it
/// has stopped being the active signing key. Tokens minted under it stay valid
/// until they expire; it is also published in the JWKS so downstream verifiers
/// keep accepting them.
pub struct EsVerifyingKey {
    pub key_id: String,
    pub public_pem: Vec<u8>,
}

/// The wire claim set. Field names follow JWT conventions so `auth-context` (and
/// any standard verifier) reads them without bespoke handling.
#[derive(Debug, Serialize, Deserialize)]
struct EdgeClaims {
    /// Internal account id (never the IdP subject).
    sub: String,
    /// Session id.
    sid: String,
    /// Revocation generation (`gen` is a reserved keyword in edition 2024).
    #[serde(rename = "gen")]
    generation: i64,
    iss: String,
    aud: String,
    iat: i64,
    exp: i64,
    /// Normalized permissions.
    perms: Vec<String>,
    /// Profiles the account owns (see `AccessTokenClaims::profile_ids`). Defaults
    /// to empty so tokens minted before the claim existed still verify.
    #[serde(default)]
    pids: Vec<String>,
    /// The device the session is bound to (`auth_context::edge::EDGE_DEVICE_CLAIM`);
    /// the realtime gateway rejects a handshake without it. Omitted when the
    /// client sent no device id, rather than minted as an empty string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    did: Option<String>,
    /// `"guest"` on a guest token (`auth_context::edge::EDGE_KIND_CLAIM`);
    /// absent on a member's, so member tokens are unchanged on the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    /// When the holder last proved a credential (`auth_context::edge::EDGE_AUTH_TIME_CLAIM`);
    /// omitted on refreshed tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_time: Option<i64>,
    /// The holder's age bracket (`auth_context::edge::EDGE_AGE_CLAIM`); omitted
    /// when unknown and on guest tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    age: Option<String>,
}

/// A guest token's `sub`: the guest id under a prefix no account id can take,
/// so a guest can never pass for an account in `require_account`.
const GUEST_SUB_PREFIX: &str = "guest:";

/// A verifying key plus the SPKI PEM it was built from (retained so the JWKS can
/// be regenerated without a second source of truth).
struct VerifyEntry {
    decoding_key: DecodingKey,
    public_pem: Vec<u8>,
}

/// ES256 implementation of [`TokenMinter`] with a verifying **key ring** for
/// zero-downtime rotation: one active key signs; any key in the ring (selected by
/// the token's `kid`) verifies.
pub struct Es256TokenMinter {
    encoding_key: EncodingKey,
    /// Header for newly minted tokens — carries the active `kid`.
    header: Header,
    /// kid → verifying key. Includes the active key plus any retiring keys.
    verifying: HashMap<String, VerifyEntry>,
    validation: Validation,
    issuer: String,
    audience: String,
}

impl Es256TokenMinter {
    /// Builds a minter whose ring contains only the active key.
    pub fn from_pem(material: EsKeyMaterial) -> Result<Self, AuthError> {
        Self::from_key_ring(material, Vec::new())
    }

    /// Builds a minter that signs with `active` and additionally **verifies**
    /// tokens minted under any `retiring` key — the rotation window.
    pub fn from_key_ring(
        active: EsKeyMaterial,
        retiring: Vec<EsVerifyingKey>,
    ) -> Result<Self, AuthError> {
        let encoding_key = EncodingKey::from_ec_pem(&active.private_pem)
            .map_err(|_| AuthError::SigningKeyUnavailable)?;

        let mut verifying = HashMap::new();
        verifying.insert(active.key_id.clone(), verify_entry(&active.public_pem)?);
        for key in retiring {
            verifying.insert(key.key_id, verify_entry(&key.public_pem)?);
        }

        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_issuer(std::slice::from_ref(&active.issuer));
        validation.set_audience(std::slice::from_ref(&active.audience));
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(active.key_id);

        Ok(Self {
            encoding_key,
            header,
            verifying,
            validation,
            issuer: active.issuer,
            audience: active.audience,
        })
    }

    /// The JWKS for every verifying key in the ring — what downstream services
    /// (via `auth-context`) fetch to validate edge tokens. Publish it at the
    /// service's well-known JWKS URL.
    pub fn jwks(&self) -> Result<Jwks, AuthError> {
        let mut keys: Vec<Jwk> = self
            .verifying
            .iter()
            .map(|(kid, entry)| jwk_from_pem(kid, &entry.public_pem))
            .collect::<Result<_, _>>()?;
        keys.sort_by(|a, b| a.kid.cmp(&b.kid)); // deterministic ordering
        Ok(Jwks { keys })
    }

    /// The JWKS as a JSON string.
    pub fn jwks_json(&self) -> Result<String, AuthError> {
        serde_json::to_string(&self.jwks()?)
            .map_err(|e| AuthError::DomainViolation { field: "jwks".into(), message: e.to_string() })
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }
}

fn verify_entry(public_pem: &[u8]) -> Result<VerifyEntry, AuthError> {
    let decoding_key =
        DecodingKey::from_ec_pem(public_pem).map_err(|_| AuthError::SigningKeyUnavailable)?;
    Ok(VerifyEntry { decoding_key, public_pem: public_pem.to_vec() })
}

/// A single JSON Web Key (P-256 / ES256).
#[derive(Debug, Serialize)]
pub struct Jwk {
    pub kty: &'static str,
    pub crv: &'static str,
    #[serde(rename = "use")]
    pub use_: &'static str,
    pub alg: &'static str,
    pub kid: String,
    pub x: String,
    pub y: String,
}

/// A JSON Web Key Set.
#[derive(Debug, Serialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

fn jwk_from_pem(kid: &str, public_pem: &[u8]) -> Result<Jwk, AuthError> {
    let pem = std::str::from_utf8(public_pem)
        .map_err(|_| AuthError::SigningKeyUnavailable)?;
    let public_key = p256::PublicKey::from_public_key_pem(pem)
        .map_err(|_| AuthError::SigningKeyUnavailable)?;
    let point = public_key.to_encoded_point(false);
    let x = point.x().ok_or(AuthError::SigningKeyUnavailable)?;
    let y = point.y().ok_or(AuthError::SigningKeyUnavailable)?;

    Ok(Jwk {
        kty: "EC",
        crv: "P-256",
        use_: "sig",
        alg: "ES256",
        kid: kid.to_owned(),
        x: Es256TokenMinter::b64(x),
        y: Es256TokenMinter::b64(y),
    })
}

#[async_trait]
impl TokenMinter for Es256TokenMinter {
    async fn mint_access(&self, claims: &AccessTokenClaims) -> Result<String, AuthError> {
        let (sub, kind) = match claims.kind {
            SessionKind::Member => (claims.account_id.as_str(), None),
            SessionKind::Guest => (
                format!("{GUEST_SUB_PREFIX}{}", claims.account_id.as_str()),
                Some(SessionKind::Guest.as_str().to_owned()),
            ),
        };
        let edge = EdgeClaims {
            sub,
            sid: claims.session_id.as_str(),
            generation: claims.generation.value(),
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            iat: claims.issued_at.timestamp(),
            exp: claims.expires_at.timestamp(),
            perms: claims.permissions.iter().map(|p| p.as_str().to_owned()).collect(),
            pids: claims.profile_ids.iter().map(ProfileId::as_str).collect(),
            did: claims.device_id.clone(),
            kind,
            auth_time: claims.auth_time.map(|t| t.timestamp()),
            age: claims.age_bracket.map(|a| a.as_claim().to_owned()),
        };
        encode(&self.header, &edge, &self.encoding_key).map_err(|_| AuthError::TokenSigningFailed)
    }

    async fn verify_access(&self, token: &str) -> Result<AccessTokenClaims, AuthError> {
        // Select the verifying key by the token's `kid` (key-ring rotation).
        let header = decode_header(token).map_err(|_| AuthError::IdpTokenRejected)?;
        let kid = header.kid.ok_or(AuthError::IdpTokenRejected)?;
        let entry = self.verifying.get(&kid).ok_or(AuthError::IdpTokenRejected)?;

        let data = decode::<EdgeClaims>(token, &entry.decoding_key, &self.validation)
            .map_err(|_| AuthError::IdpTokenRejected)?;
        let c = data.claims;

        let kind = match c.kind.as_deref() {
            None => SessionKind::Member,
            Some(k) => SessionKind::try_from(k).map_err(|_| AuthError::IdpTokenRejected)?,
        };
        let raw_id = match kind {
            SessionKind::Member => c.sub.as_str(),
            SessionKind::Guest => c.sub.strip_prefix(GUEST_SUB_PREFIX).ok_or(AuthError::IdpTokenRejected)?,
        };
        let account_id = AccountId::try_from(raw_id)?;
        let session_id = SessionId::try_from(c.sid.as_str())?;
        let to_utc = |secs: i64| {
            Utc.timestamp_opt(secs, 0).single().ok_or_else(|| AuthError::DomainViolation {
                field: "token.timestamp".into(),
                message: "out-of-range timestamp claim".into(),
            })
        };
        let issued_at: DateTime<Utc> = to_utc(c.iat)?;
        let expires_at: DateTime<Utc> = to_utc(c.exp)?;
        let auth_time = c.auth_time.map(to_utc).transpose()?;
        let permissions = c.perms.into_iter().map(Permission::new).collect();
        let profile_ids = c
            .pids
            .iter()
            .map(|s| ProfileId::try_from(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| AuthError::IdpTokenRejected)?;

        Ok(AccessTokenClaims {
            account_id,
            session_id,
            generation: Generation::from_i64(c.generation),
            permissions,
            profile_ids,
            device_id: c.did,
            kind,
            auth_time,
            age_bracket: c.age.as_deref().and_then(AgeBracket::from_claim),
            issued_at,
            expires_at,
        })
    }

    fn generate_refresh(&self) -> Result<GeneratedRefresh, AuthError> {
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        let plaintext = Self::b64(&bytes);
        let hash = self.hash_refresh(&plaintext)?;
        Ok(GeneratedRefresh { plaintext, hash })
    }

    fn hash_refresh(&self, plaintext: &str) -> Result<RefreshTokenHash, AuthError> {
        let digest = Sha256::digest(plaintext.as_bytes());
        RefreshTokenHash::new(Self::b64(&digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use uuid::Uuid;

    /// Generates an ephemeral P-256 keypair as (PKCS#8 private PEM, SPKI public
    /// PEM). No key material is ever hardcoded — keys live only for the test.
    fn keypair() -> (Vec<u8>, Vec<u8>) {
        use p256::ecdsa::SigningKey;
        use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
        let signing = SigningKey::random(&mut rand_core::OsRng);
        let private_pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes().to_vec();
        let public_pem =
            signing.verifying_key().to_public_key_pem(LineEnding::LF).unwrap().into_bytes();
        (private_pem, public_pem)
    }

    fn material(private: Vec<u8>, public: Vec<u8>, kid: &str) -> EsKeyMaterial {
        EsKeyMaterial {
            private_pem: private,
            public_pem: public,
            key_id: kid.to_owned(),
            issuer: "https://auth.test".into(),
            audience: "core-platform".into(),
        }
    }

    fn single_key_minter() -> Es256TokenMinter {
        let (private_pem, public_pem) = keypair();
        Es256TokenMinter::from_pem(material(private_pem, public_pem, "k1")).unwrap()
    }

    fn claims_at(now: DateTime<Utc>, ttl: Duration) -> AccessTokenClaims {
        AccessTokenClaims {
            account_id: AccountId::from_uuid(Uuid::now_v7()),
            session_id: SessionId::new(),
            generation: Generation::from_i64(3),
            permissions: vec![Permission::new("posts:write")],
            profile_ids: vec![ProfileId::from_uuid(Uuid::now_v7())],
            device_id: Some("ios-install-1".into()),
            kind: SessionKind::Member,
            auth_time: None,
            age_bracket: None,
            issued_at: now,
            expires_at: now + ttl,
        }
    }

    #[tokio::test]
    async fn mint_then_verify_round_trips_claims() {
        let minter = single_key_minter();
        let now = Utc::now();
        let claims = claims_at(now, Duration::minutes(10));
        let token = minter.mint_access(&claims).await.unwrap();
        let back = minter.verify_access(&token).await.unwrap();
        assert_eq!(back.account_id, claims.account_id);
        assert_eq!(back.session_id, claims.session_id);
        assert_eq!(back.generation, claims.generation);
        assert_eq!(back.permissions, claims.permissions);
        assert_eq!(back.profile_ids, claims.profile_ids);
        assert_eq!(back.device_id, claims.device_id);
    }

    #[tokio::test]
    async fn pids_ride_the_wire_as_a_json_array_of_uuids() {
        let minter = single_key_minter();
        let claims = claims_at(Utc::now(), Duration::minutes(10));
        let token = minter.mint_access(&claims).await.unwrap();
        let payload = token.split('.').nth(1).unwrap();
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["pids"], serde_json::json!([claims.profile_ids[0].as_str()]));
        assert_eq!(json["perms"], serde_json::json!(["posts:write"]));
        assert_eq!(json["did"], serde_json::json!("ios-install-1"));
    }

    #[tokio::test]
    async fn a_guest_token_carries_its_kind_and_a_prefixed_sub_and_round_trips() {
        let minter = single_key_minter();
        let claims = AccessTokenClaims {
            kind: SessionKind::Guest,
            permissions: vec![Permission::read_public()],
            profile_ids: Vec::new(),
            ..claims_at(Utc::now(), Duration::minutes(10))
        };
        let token = minter.mint_access(&claims).await.unwrap();
        let payload = token.split('.').nth(1).unwrap();
        let json: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload).unwrap(),
        )
        .unwrap();
        assert_eq!(json["kind"], "guest");
        assert_eq!(json["sub"], format!("guest:{}", claims.account_id.as_str()));
        assert_eq!(json["perms"], serde_json::json!(["read:public"]));
        assert_eq!(json["pids"], serde_json::json!([]));

        let back = minter.verify_access(&token).await.unwrap();
        assert_eq!(back.kind, SessionKind::Guest);
        assert_eq!(back.account_id, claims.account_id);
    }

    #[tokio::test]
    async fn a_member_token_has_no_kind_claim() {
        let minter = single_key_minter();
        let token = minter.mint_access(&claims_at(Utc::now(), Duration::minutes(10))).await.unwrap();
        let payload = token.split('.').nth(1).unwrap();
        let json: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload).unwrap(),
        )
        .unwrap();
        assert!(json.get("kind").is_none());
        assert_eq!(minter.verify_access(&token).await.unwrap().kind, SessionKind::Member);
    }

    #[tokio::test]
    async fn did_is_omitted_when_the_session_has_no_device_id() {
        let minter = single_key_minter();
        let claims =
            AccessTokenClaims { device_id: None, ..claims_at(Utc::now(), Duration::minutes(10)) };
        let token = minter.mint_access(&claims).await.unwrap();
        let payload = token.split('.').nth(1).unwrap();
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json.get("did").is_none(), "no empty-string device claim");
        assert_eq!(minter.verify_access(&token).await.unwrap().device_id, None);
    }

    #[tokio::test]
    async fn expired_token_is_rejected() {
        let minter = single_key_minter();
        let past = Utc::now() - Duration::hours(1);
        let token = minter.mint_access(&claims_at(past, Duration::minutes(10))).await.unwrap();
        assert!(matches!(minter.verify_access(&token).await.unwrap_err(), AuthError::IdpTokenRejected));
    }

    #[tokio::test]
    async fn tampered_token_is_rejected() {
        let minter = single_key_minter();
        let mut token = minter.mint_access(&claims_at(Utc::now(), Duration::minutes(10))).await.unwrap();
        token.push('x');
        assert!(matches!(minter.verify_access(&token).await.unwrap_err(), AuthError::IdpTokenRejected));
    }

    #[tokio::test]
    async fn rotated_key_still_verifies_old_tokens() {
        let (k1_private, k1_public) = keypair();
        let (k2_private, k2_public) = keypair();

        // A token minted under k1 (the retiring key)…
        let old_minter =
            Es256TokenMinter::from_pem(material(k1_private, k1_public.clone(), "k1")).unwrap();
        let token = old_minter.mint_access(&claims_at(Utc::now(), Duration::minutes(10))).await.unwrap();

        // …is still accepted by a minter whose ACTIVE key is k2 but whose ring
        // retains k1 — the rotation window.
        let rotated = Es256TokenMinter::from_key_ring(
            material(k2_private, k2_public, "k2"),
            vec![EsVerifyingKey { key_id: "k1".into(), public_pem: k1_public }],
        )
        .unwrap();
        let back = rotated.verify_access(&token).await.unwrap();
        assert_eq!(back.generation, Generation::from_i64(3));

        // New tokens are minted under k2.
        let fresh = rotated.mint_access(&claims_at(Utc::now(), Duration::minutes(10))).await.unwrap();
        assert_eq!(decode_header(&fresh).unwrap().kid.as_deref(), Some("k2"));
    }

    #[tokio::test]
    async fn token_with_unknown_kid_is_rejected() {
        // k2-signed token presented to a minter that only knows k1.
        let (k2_private, k2_public) = keypair();
        let k2 = Es256TokenMinter::from_pem(material(k2_private, k2_public, "k2")).unwrap();
        let token = k2.mint_access(&claims_at(Utc::now(), Duration::minutes(10))).await.unwrap();
        let k1_only = single_key_minter();
        assert!(matches!(k1_only.verify_access(&token).await.unwrap_err(), AuthError::IdpTokenRejected));
    }

    #[test]
    fn jwks_publishes_every_ring_key() {
        let (_k1_private, k1_public) = keypair();
        let (k2_private, k2_public) = keypair();
        let minter = Es256TokenMinter::from_key_ring(
            material(k2_private, k2_public, "k2"),
            vec![EsVerifyingKey { key_id: "k1".into(), public_pem: k1_public }],
        )
        .unwrap();
        let jwks = minter.jwks().unwrap();
        assert_eq!(jwks.keys.len(), 2);
        let kids: Vec<&str> = jwks.keys.iter().map(|k| k.kid.as_str()).collect();
        assert_eq!(kids, vec!["k1", "k2"]); // sorted
        let k = &jwks.keys[0];
        assert_eq!(k.kty, "EC");
        assert_eq!(k.crv, "P-256");
        assert_eq!(k.alg, "ES256");
        assert!(!k.x.is_empty() && !k.y.is_empty());
    }

    #[test]
    fn refresh_hash_is_deterministic_and_redacted() {
        let minter = single_key_minter();
        let generated = minter.generate_refresh().unwrap();
        assert_eq!(
            minter.hash_refresh(&generated.plaintext).unwrap().as_str(),
            generated.hash.as_str()
        );
        assert_ne!(minter.hash_refresh("other").unwrap().as_str(), generated.hash.as_str());
    }
}

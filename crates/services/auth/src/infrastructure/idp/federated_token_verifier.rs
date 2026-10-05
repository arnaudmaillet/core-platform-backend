//! Native Sign in with Apple / Google: verifies the provider's id_token here,
//! against its published JWKS (Apple `https://appleid.apple.com/auth/keys`,
//! Google `https://www.googleapis.com/oauth2/v3/certs`), with no other call to
//! the provider.
//!
//! Checked: the signature (by `kid`; keys fetched on first use and re-fetched,
//! at most once a minute, when an unknown `kid` shows up — a provider rotation;
//! [`JwksFederatedTokenVerifier::spawn_refresh`] also re-fetches them in the
//! background, so the first sign-in after boot pays no fetch and a key the
//! provider withdrew stops verifying),
//! the issuer, the audience (the app's client ids — a token minted for another
//! app is refused), expiry (60 s leeway) and the nonce (the raw value the app
//! generated must match the claim, or its SHA-256 hex: what the app hands Apple).
//! A provider with no client id configured is off.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use auth_context::{JwksCache, JwksClient};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::application::port::{FederatedIdentity, FederatedTokenVerifier};
use crate::domain::value_object::{FederatedProvider, APPLE_ISSUER, GOOGLE_ISSUERS};
use crate::error::AuthError;

/// Shortest interval between two key re-fetches for one provider.
const REFETCH_INTERVAL: Duration = Duration::from_secs(60);

/// Clock skew tolerated on `exp` / `iat`.
const LEEWAY_SECS: u64 = 60;

pub const APPLE_JWKS_URL: &str = "https://appleid.apple.com/auth/keys";
pub const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";

/// One provider's verification settings and key cache.
pub struct ProviderKeys {
    issuers:    Vec<String>,
    audiences:  Vec<String>,
    algorithms: Vec<Algorithm>,
    client:     Option<JwksClient>,
    cache:      JwksCache,
    last_fetch: Mutex<Option<Instant>>,
}

impl ProviderKeys {
    /// Production settings: RS256, keys from `jwks_url`.
    pub fn new(issuers: Vec<String>, audiences: Vec<String>, jwks_url: &str, timeout: Duration) -> Self {
        Self {
            issuers,
            audiences,
            algorithms: vec![Algorithm::RS256],
            client: Some(JwksClient::new(jwks_url, timeout)),
            cache: JwksCache::new(),
            last_fetch: Mutex::new(None),
        }
    }

    /// Keys fetched from `jwks_url` with the given algorithms (tests against a
    /// local JWKS server, with throwaway EC keys).
    #[cfg(test)]
    fn fetching(issuers: Vec<String>, audiences: Vec<String>, algorithms: Vec<Algorithm>, jwks_url: &str) -> Self {
        Self {
            issuers,
            audiences,
            algorithms,
            client: Some(JwksClient::new(jwks_url, Duration::from_secs(2))),
            cache: JwksCache::new(),
            last_fetch: Mutex::new(None),
        }
    }

    /// Fixed keys, no fetching (tests).
    pub async fn with_keys(
        issuers: Vec<String>,
        audiences: Vec<String>,
        algorithms: Vec<Algorithm>,
        keys: HashMap<String, DecodingKey>,
    ) -> Self {
        let cache = JwksCache::new();
        cache.replace(keys).await;
        Self { issuers, audiences, algorithms, client: None, cache, last_fetch: Mutex::new(None) }
    }

    /// Re-fetches the whole key set and replaces the cache (keys the provider
    /// withdrew go). On failure the last good keys stay. Fixed keys: no-op.
    pub async fn refresh(&self) -> Result<(), AuthError> {
        let Some(client) = &self.client else { return Ok(()) };
        let keys = client.fetch().await.map_err(|e| {
            tracing::warn!(error = %e, "federated JWKS refresh failed; keeping the last keys");
            AuthError::IdpUnavailable
        })?;
        if keys.is_empty() {
            tracing::warn!("federated JWKS refresh returned no keys; keeping the last keys");
            return Err(AuthError::IdpUnavailable);
        }
        self.cache.replace(keys).await;
        *self.last_fetch.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        Ok(())
    }

    async fn key(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        if let Some(key) = self.cache.get(kid).await {
            return Ok(key);
        }
        let Some(client) = &self.client else {
            return Err(rejected("unknown key id"));
        };
        let due = {
            let mut last = self.last_fetch.lock().unwrap_or_else(|e| e.into_inner());
            let due = last.is_none_or(|at| at.elapsed() >= REFETCH_INTERVAL);
            if due {
                *last = Some(Instant::now());
            }
            due
        };
        if due {
            let keys = client.fetch().await.map_err(|e| {
                tracing::warn!(error = %e, "federated JWKS fetch failed");
                AuthError::IdpUnavailable
            })?;
            self.cache.replace(keys).await;
        }
        self.cache.get(kid).await.ok_or_else(|| rejected("unknown key id"))
    }
}

fn rejected(reason: &str) -> AuthError {
    AuthError::IdTokenRejected { reason: reason.to_owned() }
}

/// Apple sends some booleans as strings ("true").
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Flag {
    Bool(bool),
    Text(String),
}

impl Flag {
    fn is_true(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            Self::Text(s) => s.eq_ignore_ascii_case("true"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    iss:              String,
    sub:              String,
    #[serde(default)]
    email:            Option<String>,
    #[serde(default)]
    email_verified:   Option<Flag>,
    #[serde(default)]
    is_private_email: Option<Flag>,
    #[serde(default)]
    nonce:            Option<String>,
}

/// The nonce the token carries is the app's raw nonce, or its SHA-256 hex.
fn nonce_matches(claim: &str, raw: &str) -> bool {
    if claim == raw {
        return true;
    }
    let digest = Sha256::digest(raw.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    claim.eq_ignore_ascii_case(&hex)
}

pub struct JwksFederatedTokenVerifier {
    providers: HashMap<FederatedProvider, ProviderKeys>,
}

impl JwksFederatedTokenVerifier {
    pub fn new() -> Self {
        Self { providers: HashMap::new() }
    }

    /// Enables a provider. Without one, its tokens are refused as not configured.
    pub fn with(mut self, provider: FederatedProvider, keys: ProviderKeys) -> Self {
        self.providers.insert(provider, keys);
        self
    }

    /// Fetches every provider's keys now, then every `every` (warm cache at
    /// boot, rotations and withdrawals picked up without waiting for an unknown
    /// `kid`). Failures are logged and retried on the next tick.
    pub fn spawn_refresh(self: &std::sync::Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let verifier = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(every.max(Duration::from_secs(60)));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                for (provider, keys) in &verifier.providers {
                    if keys.refresh().await.is_ok() {
                        tracing::debug!(provider = ?provider, "federated JWKS refreshed");
                    }
                }
            }
        })
    }

    /// Apple with the given client ids (bundle / services ids); `None` when empty.
    pub fn apple(audiences: Vec<String>, timeout: Duration) -> Option<ProviderKeys> {
        (!audiences.is_empty())
            .then(|| ProviderKeys::new(vec![APPLE_ISSUER.to_owned()], audiences, APPLE_JWKS_URL, timeout))
    }

    /// Google with the given OAuth client ids; `None` when empty.
    pub fn google(audiences: Vec<String>, timeout: Duration) -> Option<ProviderKeys> {
        (!audiences.is_empty()).then(|| {
            ProviderKeys::new(
                GOOGLE_ISSUERS.iter().map(|s| (*s).to_owned()).collect(),
                audiences,
                GOOGLE_JWKS_URL,
                timeout,
            )
        })
    }
}

impl Default for JwksFederatedTokenVerifier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FederatedTokenVerifier for JwksFederatedTokenVerifier {
    async fn verify(
        &self,
        provider: FederatedProvider,
        id_token: &str,
        nonce: &str,
    ) -> Result<FederatedIdentity, AuthError> {
        let keys = self.providers.get(&provider).ok_or_else(|| AuthError::FederatedProviderNotConfigured {
            provider: provider.as_str().to_owned(),
        })?;
        if nonce.trim().is_empty() {
            return Err(rejected("nonce required"));
        }

        let header = decode_header(id_token).map_err(|_| rejected("malformed token"))?;
        if !keys.algorithms.contains(&header.alg) {
            return Err(rejected("unexpected algorithm"));
        }
        let kid = header.kid.ok_or_else(|| rejected("missing key id"))?;
        let key = keys.key(&kid).await?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&keys.issuers);
        validation.set_audience(&keys.audiences);
        validation.leeway = LEEWAY_SECS;
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = decode::<IdTokenClaims>(id_token, &key, &validation)
            .map_err(|e| {
                tracing::info!(provider = provider.as_str(), error = %e, "id_token rejected");
                rejected("verification failed")
            })?
            .claims;

        if !claims.nonce.as_deref().is_some_and(|claim| nonce_matches(claim, nonce)) {
            return Err(rejected("nonce mismatch"));
        }

        Ok(FederatedIdentity {
            provider,
            issuer: claims.iss,
            subject: claims.sub,
            email: claims.email.filter(|e| !e.trim().is_empty()),
            email_verified: claims.email_verified.is_some_and(|f| f.is_true()),
            private_relay: claims.is_private_email.is_some_and(|f| f.is_true()),
        })
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{encode, EncodingKey, Header};
    use p256::ecdsa::SigningKey;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
    use serde_json::json;

    use super::*;

    const AUD: &str = "com.example.app";

    struct Keypair {
        encoding: EncodingKey,
        decoding: DecodingKey,
    }

    /// An ephemeral P-256 keypair (no key material in the repo).
    fn keypair() -> Keypair {
        let signing = SigningKey::random(&mut rand_core::OsRng);
        let private_pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap();
        let public_pem = signing.verifying_key().to_public_key_pem(LineEnding::LF).unwrap();
        Keypair {
            encoding: EncodingKey::from_ec_pem(private_pem.as_bytes()).unwrap(),
            decoding: DecodingKey::from_ec_pem(public_pem.as_bytes()).unwrap(),
        }
    }

    async fn verifier(kp: &Keypair) -> JwksFederatedTokenVerifier {
        let keys = ProviderKeys::with_keys(
            vec![APPLE_ISSUER.to_owned()],
            vec![AUD.to_owned()],
            vec![Algorithm::ES256],
            HashMap::from([("k1".to_owned(), kp.decoding.clone())]),
        )
        .await;
        JwksFederatedTokenVerifier::new().with(FederatedProvider::Apple, keys)
    }

    fn token(kp: &Keypair, kid: &str, claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(kid.to_owned());
        encode(&header, &claims, &kp.encoding).unwrap()
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn apple_claims(nonce: &str) -> serde_json::Value {
        json!({
            "iss": APPLE_ISSUER, "aud": AUD, "sub": "001234.abcd",
            "exp": now() + 600, "iat": now(),
            "email": "x@privaterelay.appleid.com", "email_verified": "true",
            "is_private_email": "true", "nonce": nonce,
        })
    }

    fn sha256_hex(s: &str) -> String {
        Sha256::digest(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
    }

    #[tokio::test]
    async fn a_valid_token_yields_the_identity() {
        let kp = keypair();
        let v = verifier(&kp).await;
        // Apple carries the SHA-256 of the raw nonce.
        let t = token(&kp, "k1", apple_claims(&sha256_hex("raw-nonce")));
        let id = v.verify(FederatedProvider::Apple, &t, "raw-nonce").await.unwrap();
        assert_eq!(id.issuer, APPLE_ISSUER);
        assert_eq!(id.subject, "001234.abcd");
        assert!(id.email_verified);
        assert!(id.private_relay);
        // The raw nonce itself is accepted too (Google).
        let t = token(&kp, "k1", apple_claims("raw-nonce"));
        assert!(v.verify(FederatedProvider::Apple, &t, "raw-nonce").await.is_ok());
    }

    #[tokio::test]
    async fn anything_off_is_rejected() {
        let kp = keypair();
        let v = verifier(&kp).await;
        let good = apple_claims("n");
        let rejected = |r: Result<FederatedIdentity, AuthError>| matches!(r, Err(AuthError::IdTokenRejected { .. }));

        let mut wrong_aud = good.clone();
        wrong_aud["aud"] = json!("com.other.app");
        let mut wrong_iss = good.clone();
        wrong_iss["iss"] = json!("https://evil.example");
        let mut expired = good.clone();
        expired["exp"] = json!(now() - 3_600);
        for claims in [wrong_aud, wrong_iss, expired] {
            assert!(rejected(v.verify(FederatedProvider::Apple, &token(&kp, "k1", claims), "n").await));
        }
        assert!(rejected(v.verify(FederatedProvider::Apple, &token(&kp, "k1", good.clone()), "other").await));
        assert!(rejected(v.verify(FederatedProvider::Apple, &token(&kp, "k1", good.clone()), "").await));
        assert!(rejected(v.verify(FederatedProvider::Apple, &token(&kp, "zz", good.clone()), "n").await));
        // Signed by someone else.
        let forged = token(&keypair(), "k1", good.clone());
        assert!(rejected(v.verify(FederatedProvider::Apple, &forged, "n").await));
        assert!(rejected(v.verify(FederatedProvider::Apple, "not.a.token", "n").await));
        // A provider with no client id is off.
        assert!(matches!(
            v.verify(FederatedProvider::Google, &token(&kp, "k1", good), "n").await,
            Err(AuthError::FederatedProviderNotConfigured { .. })
        ));
    }

    /// A throwaway P-256 key: its token signer and its public JWK.
    fn ec_jwk(kid: &str) -> (EncodingKey, serde_json::Value) {
        use base64::Engine;
        let signing = SigningKey::random(&mut rand_core::OsRng);
        let point = signing.verifying_key().to_encoded_point(false);
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap();
        (
            EncodingKey::from_ec_pem(pem.as_bytes()).unwrap(),
            json!({ "kty": "EC", "crv": "P-256", "kid": kid, "alg": "ES256",
                    "x": b64(point.x().unwrap()), "y": b64(point.y().unwrap()) }),
        )
    }

    /// A one-route JWKS server answering `body` (or a 500 when `None`).
    async fn jwks_server(body: std::sync::Arc<Mutex<Option<String>>>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else { return };
                let mut buf = [0u8; 2048];
                let _ = socket.read(&mut buf).await;
                let current = body.lock().unwrap().clone();
                let response = match current {
                    Some(json) => format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{json}",
                        json.len()
                    ),
                    None => "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned(),
                };
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{addr}/keys")
    }

    #[tokio::test]
    async fn a_refresh_picks_up_rotations_drops_withdrawn_keys_and_survives_outages() {
        let (old_signer, old_jwk) = ec_jwk("old");
        let (new_signer, new_jwk) = ec_jwk("new");
        let body = std::sync::Arc::new(Mutex::new(Some(json!({ "keys": [old_jwk] }).to_string())));
        let url = jwks_server(std::sync::Arc::clone(&body)).await;
        let keys = ProviderKeys::fetching(vec![APPLE_ISSUER.to_owned()], vec![AUD.to_owned()], vec![Algorithm::ES256], &url);
        let signed = |signer: &EncodingKey, kid: &str| {
            let mut header = Header::new(Algorithm::ES256);
            header.kid = Some(kid.to_owned());
            encode(&header, &apple_claims("n"), signer).unwrap()
        };
        let verifier = std::sync::Arc::new(JwksFederatedTokenVerifier::new().with(FederatedProvider::Apple, keys));
        let apple = &verifier.providers[&FederatedProvider::Apple];

        // Warm: the old key verifies with no fetch on the sign-in path.
        apple.refresh().await.unwrap();
        assert!(verifier.verify(FederatedProvider::Apple, &signed(&old_signer, "old"), "n").await.is_ok());

        // The provider rotates: the old key is withdrawn, the new one published.
        *body.lock().unwrap() = Some(json!({ "keys": [new_jwk] }).to_string());
        apple.refresh().await.unwrap();
        assert!(verifier.verify(FederatedProvider::Apple, &signed(&new_signer, "new"), "n").await.is_ok());
        assert!(
            matches!(
                verifier.verify(FederatedProvider::Apple, &signed(&old_signer, "old"), "n").await,
                Err(AuthError::IdTokenRejected { .. })
            ),
            "a withdrawn key no longer verifies"
        );

        // An outage keeps the last good keys.
        *body.lock().unwrap() = None;
        assert!(apple.refresh().await.is_err());
        assert!(verifier.verify(FederatedProvider::Apple, &signed(&new_signer, "new"), "n").await.is_ok());
    }
}

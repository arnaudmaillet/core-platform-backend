use jsonwebtoken::{encode, Algorithm, Header};
use serde_json::json;

use auth_context::OidcClaims;

use super::TestKeyPair;

/// Helper that mints JWTs signed with a [`TestKeyPair`], covering the common
/// claim shapes needed across test modules.
///
/// All methods produce real RS256 signatures — there is no mocking of the
/// cryptographic layer.
pub struct TokenFactory<'a> {
    key: &'a TestKeyPair,
}

impl<'a> TokenFactory<'a> {
    pub fn new(key: &'a TestKeyPair) -> Self {
        Self { key }
    }

    /// A well-formed, non-expired RS256 token with standard OIDC claims.
    pub fn valid(
        &self,
        sub: &str,
        issuer: &str,
        audience: &str,
        scope: &str,
    ) -> String {
        let now = now_unix();
        let claims = OidcClaims {
            sub: sub.to_owned(),
            iss: Some(issuer.to_owned()),
            aud: Some(json!(audience)),
            exp: now + 3600,
            nbf: Some(now - 5),
            iat: Some(now),
            jti: None,
            scope: Some(scope.to_owned()),
            realm_access: None,
            resource_access: None,
            permissions: None,
            groups: None,
            tid: None,
            extra: Default::default(),
        };
        self.sign(claims)
    }

    /// A token whose `exp` is 60 seconds in the past (no clock-skew leeway
    /// is applied by the factory — leeway is the decoder's responsibility).
    pub fn expired(&self, sub: &str, issuer: &str, audience: &str) -> String {
        let past = now_unix() - 60;
        let claims = OidcClaims {
            sub: sub.to_owned(),
            iss: Some(issuer.to_owned()),
            aud: Some(json!(audience)),
            exp: past,
            nbf: None,
            iat: Some(past - 3600),
            jti: None,
            scope: None,
            realm_access: None,
            resource_access: None,
            permissions: None,
            groups: None,
            tid: None,
            extra: Default::default(),
        };
        self.sign(claims)
    }

    /// A valid token but signed with a different key whose `kid` is not in the
    /// test cache, simulating an unknown-kid scenario.
    pub fn unknown_kid(&self, sub: &str, issuer: &str, audience: &str) -> String {
        let other = TestKeyPair::generate();
        let factory = TokenFactory::new(&other);
        // Override the header kid to a value not registered in the cache.
        factory.valid(sub, issuer, audience, "openid")
    }

    /// A valid token structure but with `iss` replaced by an untrusted value.
    pub fn wrong_issuer(&self, sub: &str, audience: &str) -> String {
        self.valid(sub, "https://evil.example.com", audience, "openid")
    }

    /// A valid token structure but with `aud` set to a different audience.
    pub fn wrong_audience(&self, sub: &str, issuer: &str) -> String {
        self.valid(sub, issuer, "wrong-audience", "openid")
    }

    /// A valid token with `realm_access.roles`.
    pub fn with_realm_roles(
        &self,
        sub: &str,
        issuer: &str,
        audience: &str,
        roles: Vec<String>,
    ) -> String {
        use auth_context::RealmAccess;
        let now = now_unix();
        let claims = OidcClaims {
            sub: sub.to_owned(),
            iss: Some(issuer.to_owned()),
            aud: Some(json!(audience)),
            exp: now + 3600,
            nbf: Some(now - 5),
            iat: Some(now),
            jti: None,
            scope: Some("openid".to_owned()),
            realm_access: Some(RealmAccess { roles: Some(roles) }),
            resource_access: None,
            permissions: None,
            groups: None,
            tid: None,
            extra: Default::default(),
        };
        self.sign(claims)
    }

    /// A token with a tenant identifier in the `tid` claim.
    pub fn with_tenant(
        &self,
        sub: &str,
        issuer: &str,
        audience: &str,
        tenant_id: &str,
    ) -> String {
        let now = now_unix();
        let claims = OidcClaims {
            sub: sub.to_owned(),
            iss: Some(issuer.to_owned()),
            aud: Some(json!(audience)),
            exp: now + 3600,
            nbf: Some(now - 5),
            iat: Some(now),
            jti: None,
            scope: Some("openid".to_owned()),
            realm_access: None,
            resource_access: None,
            permissions: None,
            groups: None,
            tid: Some(tenant_id.to_owned()),
            extra: Default::default(),
        };
        self.sign(claims)
    }

    fn sign(&self, claims: OidcClaims) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.key.kid.clone());

        encode(&header, &claims, &self.key.encoding_key)
            .expect("test token signing failed — encoding key is invalid")
    }
}

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs() as i64
}

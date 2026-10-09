//! **Mesh service identity** (#852): which service is calling, on the mesh.
//!
//! Each caller pod holds a Kubernetes *projected* ServiceAccount token (audience
//! `core-platform-mesh`, rotated by the kubelet) and sends it on mesh calls; the
//! callee verifies it here against the cluster's service-account issuer — RS256,
//! issuer and audience checked — and reads the ServiceAccount it names. The
//! callee then decides which services may call a given RPC (the gate lives in
//! `transport::grpc::mesh`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use jsonwebtoken::{decode, decode_header, Algorithm, Validation};
use serde::Deserialize;

use crate::{AuthError, JwksCache, JwksClient, JwksRefresher};

/// The audience every mesh token is minted for.
pub const MESH_AUDIENCE: &str = "core-platform-mesh";

/// A verified mesh caller: the namespace and ServiceAccount its token names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshCaller {
    pub namespace:       String,
    pub service_account: String,
}

/// Where and how to verify mesh tokens.
#[derive(Debug, Clone)]
pub struct MeshTokenConfig {
    /// The issuer's JWKS: in-cluster `https://kubernetes.default.svc/openid/v1/jwks`
    /// (with `ca_file` and `bearer_file`), or the issuer's public keys.
    pub jwks_url:         String,
    pub issuer:           String,
    pub audience:         String,
    pub ca_file:          Option<PathBuf>,
    pub bearer_file:      Option<PathBuf>,
    pub refresh_interval: Duration,
    pub max_backoff:      Duration,
    pub fetch_timeout:    Duration,
    pub clock_skew:       Duration,
}

/// A Kubernetes ServiceAccount token's claims, as far as the mesh reads them.
#[derive(Debug, Deserialize)]
struct ServiceAccountClaims {
    #[serde(default)]
    sub: String,
    #[serde(rename = "kubernetes.io", default)]
    kubernetes: Option<KubernetesClaims>,
}

#[derive(Debug, Deserialize)]
struct KubernetesClaims {
    #[serde(default)]
    namespace:      String,
    serviceaccount: Option<ServiceAccountRef>,
}

#[derive(Debug, Deserialize)]
struct ServiceAccountRef {
    name: String,
}

/// Verifies mesh tokens against a (refreshed) JWKS cache.
pub struct MeshDecoder {
    cache:      JwksCache,
    validation: Validation,
}

impl MeshDecoder {
    /// A decoder over `cache` (pure: no task spawned; tests seed the cache).
    pub fn new(issuer: &str, audience: &str, clock_skew: Duration, cache: JwksCache) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.leeway = clock_skew.as_secs();
        Self { cache, validation }
    }

    /// The caller `token` names.
    ///
    /// # Errors
    ///
    /// As [`crate::JwtDecoder::decode`]; [`AuthError::ClaimsExtractionFailed`]
    /// when the token names no ServiceAccount.
    pub async fn decode(&self, token: &str) -> Result<MeshCaller, AuthError> {
        let header = decode_header(token).map_err(|e| AuthError::MalformedToken(e.to_string()))?;
        let kid = header.kid.ok_or(AuthError::MissingKid)?;
        let key = self.cache.get(&kid).await.ok_or_else(|| AuthError::UnknownKid(kid.clone()))?;
        let claims = decode::<ServiceAccountClaims>(token, &key, &self.validation)
            .map_err(crate::decoder::jwt::map_jwt_error)?
            .claims;
        caller_of(&claims)
            .ok_or_else(|| AuthError::ClaimsExtractionFailed("the token names no ServiceAccount".into()))
    }
}

/// The ServiceAccount a token names: `kubernetes.io.serviceaccount.name`, or
/// its `sub` (`system:serviceaccount:<namespace>:<name>`).
fn caller_of(claims: &ServiceAccountClaims) -> Option<MeshCaller> {
    if let Some(k8s) = &claims.kubernetes
        && let Some(sa) = &k8s.serviceaccount
        && !sa.name.is_empty()
    {
        return Some(MeshCaller { namespace: k8s.namespace.clone(), service_account: sa.name.clone() });
    }
    let rest = claims.sub.strip_prefix("system:serviceaccount:")?;
    let (namespace, name) = rest.split_once(':')?;
    (!name.is_empty()).then(|| MeshCaller { namespace: namespace.to_owned(), service_account: name.to_owned() })
}

/// Builds the mesh decoder and starts its JWKS refresher (detached).
///
/// # Errors
///
/// [`AuthError::JwksUnavailable`] when the CA file cannot be loaded.
pub fn spawn_mesh_decoder(config: &MeshTokenConfig) -> Result<Arc<MeshDecoder>, AuthError> {
    let cache = JwksCache::new();
    let client = JwksClient::with_ca_and_bearer(
        config.jwks_url.clone(),
        config.fetch_timeout,
        config.ca_file.clone(),
        config.bearer_file.clone(),
    )?;
    let _refresher = JwksRefresher::spawn(client, cache.clone(), config.refresh_interval, config.max_backoff);
    Ok(Arc::new(MeshDecoder::new(&config.issuer, &config.audience, config.clock_skew, cache)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(json: serde_json::Value) -> ServiceAccountClaims {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn the_caller_is_the_service_account_the_token_names() {
        let projected = claims(serde_json::json!({
            "sub": "system:serviceaccount:core:staging-account-server",
            "kubernetes.io": { "namespace": "core", "serviceaccount": { "name": "staging-account-server", "uid": "u" } }
        }));
        assert_eq!(
            caller_of(&projected),
            Some(MeshCaller { namespace: "core".into(), service_account: "staging-account-server".into() })
        );
        let sub_only = claims(serde_json::json!({ "sub": "system:serviceaccount:core:geo-discovery-server" }));
        assert_eq!(caller_of(&sub_only).unwrap().service_account, "geo-discovery-server");
        assert_eq!(caller_of(&claims(serde_json::json!({ "sub": "acct-1" }))), None, "a user token is no caller");
        assert_eq!(caller_of(&claims(serde_json::json!({ "sub": "system:serviceaccount:core:" }))), None);
    }
}

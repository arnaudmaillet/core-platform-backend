//! Mesh caller identity (#852): a Kubernetes ServiceAccount token, RS256,
//! verified against the issuer's keys — issuer and audience checked.

mod common;

use std::time::Duration;

use auth_context::mesh::{MeshCaller, MeshDecoder, MESH_AUDIENCE};
use auth_context::{AuthError, JwksCache};
use common::{TestKeyPair, TokenFactory};

const ISS: &str = "https://oidc.eks.example/id/CLUSTER";
const SA: &str = "system:serviceaccount:core:staging-geo-discovery-server";

async fn decoder(key: &TestKeyPair) -> MeshDecoder {
    let cache = JwksCache::new();
    cache.replace(key.as_cache_map()).await;
    MeshDecoder::new(ISS, MESH_AUDIENCE, Duration::from_secs(5), cache)
}

#[tokio::test]
async fn a_valid_token_names_its_service_account() {
    let key = TestKeyPair::generate();
    let token = TokenFactory::new(&key).valid(SA, ISS, MESH_AUDIENCE, "");
    assert_eq!(
        decoder(&key).await.decode(&token).await.unwrap(),
        MeshCaller { namespace: "core".into(), service_account: "staging-geo-discovery-server".into() }
    );
}

#[tokio::test]
async fn another_audience_issuer_key_or_an_expired_token_is_refused() {
    let key = TestKeyPair::generate();
    let tokens = TokenFactory::new(&key);
    let d = decoder(&key).await;
    assert!(matches!(d.decode(&tokens.wrong_audience(SA, ISS)).await, Err(AuthError::InvalidAudience)));
    assert!(matches!(d.decode(&tokens.wrong_issuer(SA, MESH_AUDIENCE)).await, Err(AuthError::InvalidIssuer)));
    assert!(matches!(d.decode(&tokens.expired(SA, ISS, MESH_AUDIENCE)).await, Err(AuthError::TokenExpired)));
    let stranger = TestKeyPair::generate();
    let forged = TokenFactory::new(&stranger).valid(SA, ISS, MESH_AUDIENCE, "");
    assert!(matches!(d.decode(&forged).await, Err(AuthError::UnknownKid(_))));
    // A user's token (no ServiceAccount) is no mesh caller.
    let user = tokens.valid("acct-1", ISS, MESH_AUDIENCE, "");
    assert!(matches!(d.decode(&user).await, Err(AuthError::ClaimsExtractionFailed(_))));
}

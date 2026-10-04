//! Client-edge layer: method allow-list + ES256 edge-token authentication. Drives
//! `EdgeService` as a plain tower service with tokens minted under a per-test P-256
//! key (the same key stack as the `auth` service's minter) — no live server, no
//! network JWKS fetch (the cache is seeded directly).

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use auth_context::{edge_decoder, AuthContextConfig, JwksCache, JwtDecoder, OidcClaimsExtractor};
use http::header::HeaderName;
use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header};
use p256::ecdsa::SigningKey;
use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
use serde_json::json;
use tonic::body::Body;
use tower::{Layer, ServiceExt};
use transport::grpc::edge::{
    authenticated, permission, public, EdgeAnonymous, EdgePrincipal, EdgeRule,
};
use transport::grpc::layer::edge::{EdgeGuard, EdgeLayer};

const ISSUER: &str = "https://auth.test";
const AUDIENCE: &str = "core-platform";
const KID: &str = "test-es256-1";
const ID_HEADER: &str = "x-edge-user";

/// A per-test ES256 key: the private half signs, the public half is what the
/// JWKS would publish.
struct TestKey {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

fn test_key() -> TestKey {
    let signing = SigningKey::random(&mut rand_core::OsRng);
    let private_pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap();
    let public_pem = signing.verifying_key().to_public_key_pem(LineEnding::LF).unwrap();
    TestKey {
        encoding: EncodingKey::from_ec_pem(private_pem.as_bytes()).unwrap(),
        decoding: DecodingKey::from_ec_pem(public_pem.as_bytes()).unwrap(),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Mints an edge token the way `auth` does: `sub` = account id, `sid`, `gen`,
/// `perms`, `pids`.
fn mint(key: &TestKey, sub: &str, pids: &[&str], perms: &[&str], exp: i64, kid: &str) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_owned());
    let claims = json!({
        "sub": sub, "sid": "sess-1", "gen": 1,
        "iss": ISSUER, "aud": AUDIENCE,
        "iat": now(), "exp": exp,
        "perms": perms, "pids": pids,
    });
    encode(&header, &claims, &key.encoding).unwrap()
}

async fn guard(key: &TestKey, policy: &'static [EdgeRule]) -> Arc<EdgeGuard> {
    let cfg = AuthContextConfig {
        jwks_url: String::new(),
        expected_issuer: Some(ISSUER.into()),
        expected_audience: Some(AUDIENCE.into()),
        ..AuthContextConfig::default()
    };
    let cache = JwksCache::new();
    let mut keys = HashMap::new();
    keys.insert(KID.to_owned(), key.decoding.clone());
    cache.replace(keys).await;
    let decoder = Arc::new(JwtDecoder::with_algorithms(
        &cfg,
        cache,
        OidcClaimsExtractor::platform_edge(),
        vec![Algorithm::ES256],
    ));
    Arc::new(EdgeGuard::new(decoder, policy))
}

fn layer(guard: Arc<EdgeGuard>) -> EdgeLayer {
    EdgeLayer::new(guard, HeaderName::from_static(ID_HEADER))
}

/// The inner gRPC service stand-in: echoes what the edge layer attached as response
/// headers so the tests can assert on it. Kept as a macro so each call site keeps
/// the concrete `ServiceFn` type (see traffic_layer.rs).
macro_rules! echo_service {
    () => {
        tower::service_fn(|req: http::Request<Body>| async move {
            let mut resp = http::Response::new(Body::empty());
            if let Some(id) = req.headers().get(ID_HEADER) {
                resp.headers_mut().insert("x-seen-identity", id.clone());
            }
            if let Some(p) = req.extensions().get::<EdgePrincipal>() {
                resp.headers_mut()
                    .insert("x-seen-principal", p.account_id().parse().unwrap());
                resp.headers_mut().insert(
                    "x-seen-pids",
                    p.profile_ids().collect::<Vec<_>>().join(",").parse().unwrap(),
                );
            }
            if req.extensions().get::<EdgeAnonymous>().is_some() {
                resp.headers_mut().insert("x-seen-anonymous", "1".parse().unwrap());
            }
            if let Some(p) = auth_context::current_principal() {
                resp.headers_mut()
                    .insert("x-seen-task-local", p.user_id().as_str().parse().unwrap());
            }
            Ok::<_, Infallible>(resp)
        })
    };
}

fn req(path: &str, bearer: Option<&str>) -> http::Request<Body> {
    let mut b = http::Request::builder().uri(path);
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).unwrap()
}

fn grpc_status(resp: &http::Response<Body>) -> Option<&str> {
    resp.headers().get("grpc-status").and_then(|v| v.to_str().ok())
}

fn header<'a>(resp: &'a http::Response<Body>, name: &str) -> Option<&'a str> {
    resp.headers().get(name).and_then(|v| v.to_str().ok())
}

static POLICY: &[EdgeRule] = &[
    public("/auth.v1.AuthService/Login"),
    authenticated("/post.v1.PostService/CreatePost"),
    permission("/audit.v1.AuditService/Export", "audit:export"),
];

#[tokio::test]
async fn unlisted_methods_are_unimplemented_even_with_a_valid_token() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let token = mint(&key, "acct-1", &[], &[], now() + 600, KID);

    let resp = svc.oneshot(req("/post.v1.PostService/InternalOnly", Some(&token))).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("12")); // UNIMPLEMENTED
    assert!(header(&resp, "x-seen-principal").is_none(), "inner never called");
}

#[tokio::test]
async fn health_always_passes_without_a_token() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let resp = svc.oneshot(req("/grpc.health.v1.Health/Check", None)).await.unwrap();
    assert!(grpc_status(&resp).is_none());
}

#[tokio::test]
async fn public_methods_pass_without_a_token_and_without_a_principal() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let resp = svc.oneshot(req("/auth.v1.AuthService/Login", None)).await.unwrap();
    assert!(grpc_status(&resp).is_none());
    assert!(header(&resp, "x-seen-principal").is_none());
    // Marked anonymous, so a handler cannot mistake it for a trusted mesh call.
    assert_eq!(header(&resp, "x-seen-anonymous"), Some("1"));
}

#[tokio::test]
async fn authenticated_methods_reject_a_missing_token() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let resp = svc.oneshot(req("/post.v1.PostService/CreatePost", None)).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("16")); // UNAUTHENTICATED
}

#[tokio::test]
async fn a_valid_token_attaches_the_principal_header_and_task_local() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let token = mint(&key, "acct-1", &["p-1", "p-2"], &[], now() + 600, KID);

    let resp = svc.oneshot(req("/post.v1.PostService/CreatePost", Some(&token))).await.unwrap();
    assert!(grpc_status(&resp).is_none(), "forwarded");
    assert_eq!(header(&resp, "x-seen-identity"), Some("acct-1"));
    assert_eq!(header(&resp, "x-seen-principal"), Some("acct-1"));
    assert_eq!(header(&resp, "x-seen-pids"), Some("p-1,p-2"));
    assert_eq!(header(&resp, "x-seen-task-local"), Some("acct-1"));
    assert!(header(&resp, "x-seen-anonymous").is_none());
}

#[tokio::test]
async fn a_client_supplied_identity_header_is_stripped() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());

    // Public method, forged header: the inner service must not see it.
    let mut r = req("/auth.v1.AuthService/Login", None);
    r.headers_mut().insert(ID_HEADER, "victim".parse().unwrap());
    let resp = svc.clone().oneshot(r).await.unwrap();
    assert!(header(&resp, "x-seen-identity").is_none());

    // Authenticated method, forged header: overwritten by the verified subject.
    let token = mint(&key, "acct-1", &[], &[], now() + 600, KID);
    let mut r = req("/post.v1.PostService/CreatePost", Some(&token));
    r.headers_mut().insert(ID_HEADER, "victim".parse().unwrap());
    let resp = svc.oneshot(r).await.unwrap();
    assert_eq!(header(&resp, "x-seen-identity"), Some("acct-1"));
}

#[tokio::test]
async fn expired_wrong_key_wrong_audience_and_garbage_are_unauthenticated() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let path = "/post.v1.PostService/CreatePost";

    let expired = mint(&key, "acct-1", &[], &[], now() - 120, KID);
    let resp = svc.clone().oneshot(req(path, Some(&expired))).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("16"));
    // tonic percent-encodes grpc-message.
    assert_eq!(header(&resp, "grpc-message"), Some("token%20expired"));

    let other = test_key();
    let forged = mint(&other, "acct-1", &[], &[], now() + 600, KID);
    let resp = svc.clone().oneshot(req(path, Some(&forged))).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("16"));

    let unknown_kid = mint(&key, "acct-1", &[], &[], now() + 600, "rotated-away");
    let resp = svc.clone().oneshot(req(path, Some(&unknown_kid))).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("16"));

    let resp = svc.clone().oneshot(req(path, Some("not.a.jwt"))).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("16"));

    let mut r = req(path, None);
    r.headers_mut().insert("authorization", "Basic abc".parse().unwrap());
    let resp = svc.oneshot(r).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("16"));
}

#[tokio::test]
async fn permission_rules_gate_on_the_perms_claim() {
    let key = test_key();
    let svc = layer(guard(&key, POLICY).await).layer(echo_service!());
    let path = "/audit.v1.AuditService/Export";

    let without = mint(&key, "ops-1", &[], &["audit:read"], now() + 600, KID);
    let resp = svc.clone().oneshot(req(path, Some(&without))).await.unwrap();
    assert_eq!(grpc_status(&resp), Some("7")); // PERMISSION_DENIED

    let with = mint(&key, "ops-1", &[], &["audit:read", "audit:export"], now() + 600, KID);
    let resp = svc.oneshot(req(path, Some(&with))).await.unwrap();
    assert!(grpc_status(&resp).is_none());
    assert_eq!(header(&resp, "x-seen-principal"), Some("ops-1"));
}

#[tokio::test]
async fn disabled_layer_is_passthrough_and_keeps_headers() {
    let svc = EdgeLayer::disabled().layer(echo_service!());
    let mut r = req("/anything/Goes", None);
    // On the mesh listener the header is left alone (a future mesh may inject it).
    r.headers_mut().insert(ID_HEADER, "mesh-caller".parse().unwrap());
    let resp = svc.oneshot(r).await.unwrap();
    assert!(grpc_status(&resp).is_none());
    assert_eq!(header(&resp, "x-seen-identity"), Some("mesh-caller"));
    assert!(header(&resp, "x-seen-principal").is_none());
    assert!(header(&resp, "x-seen-anonymous").is_none());
}

/// Regression: the PRODUCTION decoder builder (`auth_context::edge_decoder`, what
/// `spawn_edge_decoder` uses) must accept a real ES256 edge token. The first
/// staging cycle rejected every token as "invalid": the builder listed RS256 next
/// to ES256 and `jsonwebtoken` refuses a validation list whose algorithms are
/// not all of the decoding key's family (InvalidAlgorithm). The other tests seed
/// their own `[ES256]` list and could not catch it.
#[tokio::test]
async fn the_production_decoder_builder_accepts_a_real_es256_token() {
    let key = test_key();
    let cfg = AuthContextConfig {
        jwks_url: String::new(),
        expected_issuer: Some(ISSUER.into()),
        expected_audience: Some(AUDIENCE.into()),
        ..AuthContextConfig::default()
    };
    let cache = JwksCache::new();
    let mut keys = HashMap::new();
    keys.insert(KID.to_owned(), key.decoding.clone());
    cache.replace(keys).await;
    let guard = Arc::new(EdgeGuard::new(Arc::new(edge_decoder(&cfg, cache)), POLICY));
    let svc = layer(guard).layer(echo_service!());

    let token = mint(&key, "acct-1", &["p-1"], &["user"], now() + 600, KID);
    let resp = svc.oneshot(req("/post.v1.PostService/CreatePost", Some(&token))).await.unwrap();
    assert!(grpc_status(&resp).is_none(), "production decoder must accept the token: {:?}", header(&resp, "grpc-message"));
    assert_eq!(header(&resp, "x-seen-principal"), Some("acct-1"));
}

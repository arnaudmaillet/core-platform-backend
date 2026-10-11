# `auth-context` — Provider-agnostic JWT validation & task-local identity propagation

> **Crate Card**
>
> | | |
> |---|---|
> | **Role** | `platform` — inbound security translator (token → typed principal) |
> | **Package** | `auth-context` (dir: `crates/platform/auth-context`) |
> | **Consumed by** | service inbound boundaries / the gateway (anything that authenticates a request) |
> | **Depends on** | `jsonwebtoken`, `tokio`, `reqwest` (JWKS), `tracing`, `uuid`, `cqrs` (optional) |
> | **Stability** | stable contract |
> | **Feature flags** | `cqrs-integration` (off — enables `inject_into_envelope` + a `cqrs` dep) |
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |

---

## 🎯 Overview & role

`auth-context` is the platform's security translator: it sits at a service's inbound boundary, converts
an opaque Bearer token into a strongly-typed `CurrentPrincipal<C>`, and makes that identity available
through the whole async call-stack **without** threading it through every function signature.

**Architectural boundary** — it does authentication (verify + extract + propagate), not authorization
policy and not transport. Public keys are fetched **once** from the OIDC JWKS endpoint by a background
task and cached; every token verification is then a pure CPU operation on the hot path.

**Core objectives:** provider agnosticism (`realm_access.roles`, Auth0 `permissions`, Okta
`groups`, any OIDC issuer via the `ClaimsExtractor<C>` strategy); type-safe identity (raw claims travel
as the generic `C`); transparent propagation via `tokio::task_local!`.

---

## 📐 Architecture & key decisions

```
Bearer token ─► decode_header() (kid, no crypto) ─► JwksCache::get(kid) (O(1) RwLock read)
   ─► jsonwebtoken::decode() (RS256/ES256 + exp/nbf/iss/aud) ─► ClaimsExtractor::extract ─► CurrentPrincipal<C>
   ─► with_principal(p, fut) (bind task_local) ─► inject_into_span() / inject_into_envelope(e)

JwksRefresher::spawn(): loop { fetch → replace(keys), delay=interval | err → WARN, delay=backoff(2×, ≤max) }
```

- **JWKS cached, refreshed in the background** — verification never makes a network call; a background
  loop refreshes keys and backs off (1s → `max_backoff`) on failure, so a flaky IdP doesn't fail the
  hot path (stale keys keep working).
- **Pluggable claims extraction** — `ClaimsExtractor<C>` is the one strategy seam, so a new IdP or a
  service-account flow is a new extractor, not a fork of the decoder.
- **Task-local, not signature-threaded** — `with_principal` binds the identity to a `task_local!` so
  business logic reads `current_principal()` instead of passing it everywhere.

---

## 🔌 Public API & contract

```rust
pub struct PrincipalId(pub String);       // wraps the JWT `sub`
pub struct Permission(pub String);        // normalised, e.g. "posts:write" / "ROLE_ADMIN"
pub struct CurrentPrincipal<C> { pub user_id: PrincipalId, pub tenant_id: Option<String>, pub permissions: Vec<Permission>, pub raw_claims: C }

pub trait ClaimsExtractor<C>: Send + Sync + 'static { fn extract(&self, raw: C) -> Result<CurrentPrincipal<C>, AuthError>; }

impl JwksCache { pub fn new() -> Self; pub async fn get(&self, kid: &str) -> Option<DecodingKey>; pub async fn replace(&self, keys: HashMap<String, DecodingKey>); /* len/is_empty */ }
impl<C, E: ClaimsExtractor<C>> JwtDecoder<C, E> { pub fn new(&AuthContextConfig, JwksCache, E) -> Self; pub async fn decode(&self, token: &str) -> Result<CurrentPrincipal<C>, AuthError>; }

pub fn with_principal<P: AnyPrincipal + 'static, Fut: Future>(principal: Arc<P>, future: Fut) -> impl Future<Output = Fut::Output>;
pub fn current_principal() -> Option<Arc<dyn AnyPrincipal>>;
pub fn inject_into_span();                                              // always available
#[cfg(feature = "cqrs-integration")] pub fn inject_into_envelope<T>(envelope: &mut cqrs::Envelope<T>);

pub enum AuthError { InvalidSignature, TokenExpired, TokenNotYetValid, MissingKid, UnknownKid(String),
                     JwksUnavailable(String), MalformedToken(String), InvalidAudience, InvalidIssuer, ClaimsExtractionFailed(String) }
```

> **Contract notes:** the task-local principal is **not** propagated to `tokio::spawn`ed sub-tasks —
> re-bind with `with_principal()` inside the spawned future. `JwtDecoder::decode` is sync-on-executor
> (no `spawn_blocking` needed for ≤ 4096-bit RSA).

---

## 📦 Integration

```toml
[dependencies]
auth-context = { workspace = true }                       # + features = ["cqrs-integration"] for envelope injection
```

```rust
let cache = JwksCache::new();
let _refresher = JwksRefresher::spawn(JwksClient::new(&cfg.jwks_url, cfg.fetch_timeout),
                                      cache.clone(), cfg.refresh_interval, cfg.max_backoff);  // warms immediately
let decoder = Arc::new(JwtDecoder::new(&cfg, cache, OidcClaimsExtractor::new(Default::default())));

// per request:
let principal = Arc::new(decoder.decode(&bearer).await?);
with_principal(principal, async {
    inject_into_span();
    handle_request().await
}).await
```

---

## ⚙️ Configuration & feature flags

| Variable | Required | Default | Description |
|---|---|---|---|
| `JWKS_URL` | **Yes** | — | Full JWKS endpoint URL |
| `OIDC_ISSUER` / `OIDC_AUDIENCE` | Recommended | `None` | Expected `iss` / `aud` (disabling not recommended in prod) |
| `AUTH_REFRESH_INTERVAL_SECS` | No | `300` | JWKS cache refresh period |
| `AUTH_MAX_BACKOFF_SECS` | No | `60` | Max backoff on JWKS fetch failure |
| `AUTH_CLOCK_SKEW_SECS` | No | `5` | Leeway on `exp`/`nbf` |
| `AUTH_FETCH_TIMEOUT_SECS` | No | `10` | Per-request JWKS HTTP timeout |

**Feature flags:** `cqrs-integration` (off by default) — enables `inject_into_envelope()` and a
dependency on `cqrs`.

---

## 🔭 Observability

`tracing` events: JWKS cache refreshed (`INFO` `key_count`/`next_refresh_secs`), refresh failed (`WARN`
`error`/`retry_after_secs`), key loaded (`DEBUG`), undecodable key skipped (`WARN`).

Suggested alerts: `JwksCache::is_empty()` for > 30s ⇒ **P1** (can't authenticate anything); JWKS-fetch
`WARN` > 3/min ⇒ P2; `UnknownKid` > 1% ⇒ P3 (key-rotation lag); `TokenExpired` > 5% ⇒ P3.

---

## 🧪 Testing

```bash
cargo test   -p auth-context                          # in-process RSA keygen + pre-seeded cache, no live IdP
cargo test   -p auth-context --features cqrs-integration
cargo clippy -p auth-context --all-targets
```

---

## 🚨 Gotchas / FAQ

> The sharp edges. One entry per real trap.

**1. `UnknownKid` on every request right after deploy.**
The refresher hasn't completed its first fetch, or the JWKS set lacks the token's `kid`. Check for
`"JWKS refresh failed"` WARN, verify `JWKS_URL` resolves from inside the container, and confirm the IdP
publishes the `kid` it signs with.

**2. `InvalidSignature` after key rotation.**
The IdP issued new-key tokens before the refresher picked up the new key. Lower
`AUTH_REFRESH_INTERVAL_SECS` during rotation; ensure the IdP publishes the new key in JWKS *before*
signing with it (standard OIDC order).

**3. `ClaimsExtractionFailed: 'sub' absent` on machine-to-machine tokens.**
Client-credentials tokens may omit `sub` or set it to the client id. Implement a custom
`ClaimsExtractor<C>` mapping `azp`/`client_id` → `user_id` for service-account flows.

**4. The principal is missing inside a `tokio::spawn`.**
`task_local!` storage doesn't cross a spawn boundary — wrap the spawned future in `with_principal(...)`
again (clone the `Arc` first).

**5. `OsRng` import error when generating keys in tests.**
Use `rsa::rand_core::OsRng` — the `rand_core` split means the top-level `rand_core::OsRng` won't satisfy
the `rsa` bound.

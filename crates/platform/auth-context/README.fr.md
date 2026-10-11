---
i18n:
  source: ./README.md
  source_sha256: 8d8bad9b253bef2c59dbd55ead7ea2deef2f72220993d65dff16c30b39efaf88
  translated_at: 2026-10-11
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, signatures, identifiants) sont volontairement laissés en anglais.

# `auth-context` — Validation JWT agnostique du fournisseur & propagation d'identité en task-local

> **Fiche crate**
>
> | | |
> |---|---|
> | **Rôle** | `platform` — traducteur de sécurité en entrée (token → principal typé) |
> | **Package** | `auth-context` (dir : `crates/platform/auth-context`) |
> | **Consommé par** | les frontières d'entrée des services / la passerelle (tout ce qui authentifie une requête) |
> | **Dépend de** | `jsonwebtoken`, `tokio`, `reqwest` (JWKS), `tracing`, `uuid`, `cqrs` (optionnel) |
> | **Stabilité** | contrat stable |
> | **Feature flags** | `cqrs-integration` (off — active `inject_into_envelope` + une dép `cqrs`) |
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |

---

## 🎯 Vue d'ensemble & rôle

`auth-context` est le traducteur de sécurité de la plateforme : il se place à la frontière d'entrée d'un
service, convertit un Bearer token opaque en un `CurrentPrincipal<C>` fortement typé, et rend cette
identité disponible à travers toute la pile d'appel async **sans** la passer dans chaque signature de
fonction.

**Frontière architecturale** — il fait l'authentification (vérifier + extraire + propager), pas la
politique d'autorisation et pas le transport. Les clés publiques sont récupérées **une fois** depuis le
endpoint JWKS OIDC par une tâche d'arrière-plan et mises en cache ; chaque vérification de token est
ensuite une opération CPU pure sur le chemin chaud.

**Objectifs fondamentaux :** agnosticisme du fournisseur (`realm_access.roles`, Auth0
`permissions`, Okta `groups`, tout émetteur OIDC via la stratégie `ClaimsExtractor<C>`) ; identité
type-safe (les claims bruts voyagent comme le générique `C`) ; propagation transparente via
`tokio::task_local!`.

---

## 📐 Architecture & décisions clés

```
Bearer token ─► decode_header() (kid, no crypto) ─► JwksCache::get(kid) (O(1) RwLock read)
   ─► jsonwebtoken::decode() (RS256/ES256 + exp/nbf/iss/aud) ─► ClaimsExtractor::extract ─► CurrentPrincipal<C>
   ─► with_principal(p, fut) (bind task_local) ─► inject_into_span() / inject_into_envelope(e)

JwksRefresher::spawn(): loop { fetch → replace(keys), delay=interval | err → WARN, delay=backoff(2×, ≤max) }
```

- **JWKS en cache, rafraîchi en arrière-plan** — la vérification ne fait jamais d'appel réseau ; une
  boucle d'arrière-plan rafraîchit les clés et fait du backoff (1s → `max_backoff`) en cas d'échec, donc
  un IdP capricieux ne fait pas échouer le chemin chaud (les clés périmées continuent de fonctionner).
- **Extraction de claims branchable** — `ClaimsExtractor<C>` est l'unique couture stratégique, donc un
  nouvel IdP ou un flow service-account est un nouvel extracteur, pas un fork du décodeur.
- **Task-local, pas en signature** — `with_principal` lie l'identité à un `task_local!` pour que la
  logique métier lise `current_principal()` au lieu de la passer partout.

---

## 🔌 API publique & contrat

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

> **Contrat :** le principal task-local n'est **pas** propagé aux sous-tâches `tokio::spawn` — le re-lier
> avec `with_principal()` à l'intérieur de la future lancée. `JwtDecoder::decode` est sync-sur-l'exécuteur
> (pas de `spawn_blocking` nécessaire pour du RSA ≤ 4096 bits).

---

## 📦 Intégration

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

**Feature flags :** `cqrs-integration` (off par défaut) — active `inject_into_envelope()` et une dépendance
sur `cqrs`.

---

## 🔭 Observabilité

Événements `tracing` : cache JWKS rafraîchi (`INFO` `key_count`/`next_refresh_secs`), refresh échoué
(`WARN` `error`/`retry_after_secs`), clé chargée (`DEBUG`), clé indécodable ignorée (`WARN`).

Alertes suggérées : `JwksCache::is_empty()` pendant > 30s ⇒ **P1** (impossible d'authentifier quoi que ce
soit) ; fetch JWKS `WARN` > 3/min ⇒ P2 ; `UnknownKid` > 1% ⇒ P3 (retard de rotation de clé) ;
`TokenExpired` > 5% ⇒ P3.

---

## 🧪 Tests

```bash
cargo test   -p auth-context                          # in-process RSA keygen + pre-seeded cache, no live IdP
cargo test   -p auth-context --features cqrs-integration
cargo clippy -p auth-context --all-targets
```

---

## 🚨 Pièges / FAQ

> Les arêtes vives. Une entrée par piège réel.

**1. `UnknownKid` sur chaque requête juste après le déploiement.**
Le refresher n'a pas terminé son premier fetch, ou l'ensemble JWKS n'a pas le `kid` du token. Vérifier les
WARN `"JWKS refresh failed"`, que `JWKS_URL` résout depuis le conteneur, et que l'IdP publie le `kid` qu'il
signe.

**2. `InvalidSignature` après rotation de clé.**
L'IdP a émis des tokens nouvelle-clé avant que le refresher n'ait pris la nouvelle clé. Baisser
`AUTH_REFRESH_INTERVAL_SECS` pendant la rotation ; s'assurer que l'IdP publie la nouvelle clé dans JWKS
*avant* de signer avec (ordre OIDC standard).

**3. `ClaimsExtractionFailed: 'sub' absent` sur des tokens machine-to-machine.**
Les tokens client-credentials peuvent omettre `sub` ou le mettre à l'id client. Implémenter un
`ClaimsExtractor<C>` custom mappant `azp`/`client_id` → `user_id` pour les flows service-account.

**4. Le principal est absent dans un `tokio::spawn`.**
Le stockage `task_local!` ne traverse pas une frontière de spawn — envelopper la future lancée à nouveau
dans `with_principal(...)` (cloner l'`Arc` d'abord).

**5. Erreur d'import `OsRng` en générant des clés dans les tests.**
Utiliser `rsa::rand_core::OsRng` — le split de `rand_core` fait que le `rand_core::OsRng` de premier niveau
ne satisfait pas le bound de `rsa`.

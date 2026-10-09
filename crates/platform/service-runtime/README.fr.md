---
i18n:
  source: ./README.md
  source_sha256: bdca5951f1b9abc2862027ff413d6fce0841c66fb246b7f8a1f92b3065a8dee1
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, signatures, identifiants) sont volontairement laissés en anglais.

# `service-runtime` — Le bootstrap unifié de la flotte : implémenter un trait, obtenir un service déployable

> **Fiche crate**
>
> | | |
> |---|---|
> | **Rôle** | `platform` — la séquence de boot partagée que chaque service exécute |
> | **Package** | `service-runtime` (dir : `crates/platform/service-runtime`) |
> | **Consommé par** | chaque binaire `crates/apps/<svc>-server` (via `serve::<S>(addr)`) |
> | **Dépend de** | `tonic`, `telemetry`, `infra-config`, `traffic`, `health`, `error` |
> | **Stabilité** | contrat stable (trait `Service`) |
> | **Feature flags** | aucun |
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |

---

## 🎯 Vue d'ensemble & rôle

`service-runtime` est le bootstrap unifié de la flotte. Chaque service exécute la **même** séquence de
boot en implémentant un trait (`Service`) ; le binaire déployable est alors un one-liner. `serve::<S>(addr)`
possède tous les concerns à l'échelle du processus pour qu'aucun service ne les ré-implémente.

**Frontière architecturale** — le runtime possède l'infrastructure (télémétrie, config, couches d'entrée,
santé, arrêt) ; le service possède le domaine (câblage, services gRPC, sondes). La séparation est
**imposée par une couture `RoutesBuilder` à type effacé** : `register` ne voit jamais la pile de couches
Tower que le runtime enroule autour de lui.

```
telemetry::init (logs + OTLP traces + metrics; guard kept)
 └─ infra-config load (infrastructure.toml → InfraRegistry, fail-closed at boot)
   └─ spawn_watcher (hot-reload: resilience / cache / traffic / telemetry)
     └─ S::build(infra)                       (service composition root)
       └─ gRPC server: InboundTraceLayer (outer) + TrafficLayer (inner)
         ├─ health service (driven by S::health_probes)
         └─ S::register(routes)               (service's own gRPC services)
           └─ readiness loop + traffic prune loop
             └─ serve_with_shutdown (SIGTERM/SIGINT-drained)
```

---

## 📐 Architecture & décisions clés

| Concern | Propriétaire |
|---|---|
| Init télémétrie, OTLP, dials log/sampling | **runtime** (`serve`) |
| Chargement config + watcher de hot-reload | **runtime** |
| Couches trace + rate-limit en entrée, boucle de prune | **runtime** |
| Santé gRPC, boucle de readiness, arrêt gracieux | **runtime** |
| Câblage domaine (repos, caches, bus, workers) | **service** (`build`) |
| Services gRPC concrets + réflexion | **service** (`register`) |
| Sondes backend | **service** (`health_probes`) |

- **Un trait, surface totale** — ajouter un service à la flotte = un `service.rs` implémentant `Service` +
  un crate `apps/<svc>-server` d'~10 lignes. Rien d'autre.
- **Couture à type effacé** — `register(&mut RoutesBuilder)` garde les types de couches Tower hors de la
  signature du service, donc le runtime peut changer la pile de couches à l'échelle de la flotte sans
  toucher aux services.
- **La santé reflète les vraies dépendances** — avec des sondes, un service démarre `NOT_SERVING` et passe
  `SERVING` seulement après que toutes les sondes passent (et inversement à tout échec), donc le readiness
  K8s suit la joignabilité des dépendances, pas la simple liveness du processus.

---

## 🔌 API publique & contrat

```rust
#[async_trait::async_trait]
pub trait Service: Sized {
    const NAME: &'static str;
    const VERSION: &'static str;
    const GRPC_SERVICE_NAME: &'static str;     // the concrete server's NamedService::NAME (health key)

    async fn build(infra: Arc<InfraRegistry>) -> anyhow::Result<Self>;   // composition root
    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> { vec![] }       // default: none
    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()>;  // gRPC services + reflection
}

pub async fn serve<S: Service>(addr: SocketAddr) -> anyhow::Result<()>;
pub use health::{HealthProbe, FnProbe};
pub use infra_config::InfraRegistry;
```

Le binaire déployable :

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("CHAT_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_owned()).parse()?;
    service_runtime::serve::<ChatService>(addr).await
}
```

> **Contrat :** `GRPC_SERVICE_NAME` **doit** égaler le `NamedService::NAME` du serveur tonic concret —
> c'est la clé sous laquelle le service de santé reporte ; une incohérence laisse le service bloqué
> `NOT_SERVING` du point de vue client. L'`infra` de `build` porte les registries hot-reloadables ; les
> services ne consommant pas de policy externalisée peuvent l'ignorer.

---

## 📦 Intégration

```toml
[dependencies]
service-runtime = { workspace = true }
```

Voir l'impl `Service` et le binaire en §API publique — c'est toute la surface d'intégration.

---

## ⚙️ Configuration & feature flags

| Variable | Default | Effect |
|---|---|---|
| `INFRA_CONFIG_PATH` | `infrastructure.toml` | Externalized-config document path |
| `HEALTH_PROBE_INTERVAL_SECS` | `10` | Readiness poll cadence |
| `TRAFFIC_PRUNE_INTERVAL_SECS` | `60` | Rate-limiter memory-bounding cadence |
| `GRPC_TRUSTED_PROXY_HOPS` | `1` | Proxys qui ajoutent à `X-Forwarded-For` avant les listeners (l'ALB) ; les limites `per_ip` utilisent l'entrée à autant de places depuis la droite. À augmenter si un proxy CDN/WAF est placé devant |
| `MESH_CALLER_GATE` | non défini (`off`) | `mesh_gate_from_env()` (#852) : `off` / `log` / `enforce` — si une RPC mesh gardée vérifie le jeton de ServiceAccount de son appelant |
| `MESH_JWKS_URL` · `MESH_TOKEN_ISSUER` · `MESH_TOKEN_AUDIENCE` | non défini · non défini · `core-platform-mesh` | où et comment les jetons mesh sont vérifiés (dans le cluster : `https://kubernetes.default.svc/openid/v1/jwks`) |
| `MESH_JWKS_CA_FILE` · `MESH_JWKS_BEARER_FILE` | non défini | la CA du JWKS du cluster et le jeton bearer envoyé à chaque récupération |
| `MESH_NAMESPACE` | le namespace du pod | les appelants doivent être dans ce namespace (par défaut : le `namespace` du ServiceAccount monté) |
| `MESH_TOKEN_FILE` | non défini | côté appelant (`MeshTokenInterceptor::from_env`) : le jeton projeté du pod ; non défini → rien n'est envoyé |

Les `*_GRPC_ADDR` + tuning par service vivent dans le README de chaque service. La télémétrie honore
`RUST_LOG` / `OTEL_*` au boot ; les dials live sont ensuite pilotés par la section `[telemetry]`
d'`infrastructure.toml`. Aucune feature cargo.

**Retuning à chaud** — comme le runtime lance le watcher de config, un push d'`infrastructure.toml` retune
la flotte sans redémarrage (`[telemetry]` filtre de log + sampling ; `[traffic]` rps/quotas ;
`[resilience]` timeouts/breakers).

---

## 🧪 Tests

```bash
cargo test   -p service-runtime
cargo clippy -p service-runtime --all-targets
```

---

## 🚨 Pièges / FAQ

> Les arêtes vives. Une entrée par piège réel.

**1. Le service build et serve mais les clients le voient `NOT_SERVING` indéfiniment.**
`GRPC_SERVICE_NAME` ne correspond pas au `NamedService::NAME` du serveur concret. Le définir via l'impl
`NamedService` (`<MyServer<…> as tonic::server::NamedService>::NAME`) pour que la clé de santé s'aligne.

**2. Le service ne devient jamais ready.**
Une sonde de `health_probes()` ne passe jamais — le runtime le garde `NOT_SERVING` jusqu'à ce que toutes
passent. Vérifier le `check()` de la sonde contre le backend live ; rappeler que toute `Err` de sonde
rétrograde l'ensemble du service.

**3. Un changement de config n'a pas pris effet sans redémarrage.**
Seul le *contenu* des profils fait du hot-reload ; les changements de topologie nécessitent un redémarrage
(voir `infra-config`). Confirmer que le watcher est vivant (le runtime le possède) et que
`INFRA_CONFIG_PATH` pointe sur le document monté.

**4. Des types de couches ont fuité dans ma signature `register`.**
Ils ne devraient pas — `register` ne voit que `&mut RoutesBuilder`. Si vous essayez d'y ajouter une couche
Tower, vous êtes à la mauvaise couture ; le runtime possède la pile de couches.

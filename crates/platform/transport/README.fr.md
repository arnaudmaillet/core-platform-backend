---
i18n:
  source: ./README.md
  source_sha256: 33c263a3493112bf931f62d6d67ff66348e54ed7b39b693f7273c1e0c95c049b
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, signatures, identifiants) sont volontairement laissés en anglais.

# `transport` — Couche de communication de la plateforme : gRPC + Kafka avec propagation de trace automatique

> **Fiche crate**
>
> | | |
> |---|---|
> | **Rôle** | `platform` — la couche de communication inter-services partagée (sans logique métier) |
> | **Package** | `transport` (dir : `crates/platform/transport`) |
> | **Consommé par** | chaque service (clients/serveurs gRPC, producteurs/consommateurs Kafka) |
> | **Dépend de** | `tonic`/`tower`, `rdkafka`, `resilience`, `traffic`, `telemetry`, `error`, `opentelemetry` |
> | **Stabilité** | contrat stable (`run_consumer` est un standard de flotte obligatoire) |
> | **Feature flags** | `integration-kafka` (suite de tests broker live) |
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |

---

## 🎯 Vue d'ensemble & rôle

`transport` est l'unique crate partagé que chaque service utilise pour la communication inter-processus,
sur deux paradigmes derrière une API opinionnée : **gRPC synchrone** (Tonic + Tower, TLS/mTLS optionnel,
circuit-breaking, timeout) et **Kafka asynchrone** (rdkafka, enveloppes typées, at-least-once). Sa
propriété définissante : **les deux transports auto-propagent W3C TraceContext**
(`traceparent`/`tracestate`) à travers chaque RPC et chaque message Kafka, produisant des traces de bout
en bout sans boilerplate.

**Frontière architecturale** — infrastructure pure, **sans logique métier**. Il possède le fil, la
plomberie de trace, le câblage des couches resilience/traffic, et le runtime de consommation ; il ne
possède pas de types de domaine, schémas, ni logique de handler.

---

## 📐 Architecture & décisions clés

```
caller span ─[gRPC]─ OutboundTraceLayer: inject_context → HeaderMap ──HTTP/2──►
            └[Kafka]─ producer.publish: inject_context → OwnedHeaders ──record──►
receiver    ─[gRPC]─ InboundTraceLayer: extract_context ← HeaderMap → span.set_parent(remote)
            └[Kafka]─ consumer.stream: extract_context ← BorrowedHeaders → set_parent

gRPC client stack: TimeoutLayer → CircuitBreakerLayer → OutboundTraceLayer → tonic Channel  (→ ResilientChannel)
gRPC server stack: InboundTraceLayer (outer, traces even throttled reqs) → TrafficLayer (ingress limit) → handler
```

- **Pas de `RetryLayer` au niveau transport** — les corps HTTP/2 sont des streams ; en rejouer un signifie
  bufferiser le payload complet (coût prohibitif). Appliquer le retry à la **couche applicative** (autour
  de l'appel client tonic généré, pas du channel). Kafka a l'at-least-once à la place, via `run_consumer`.
- **La config de résilience est hot-reloadable** — `ResilientChannel` lit les valeurs
  circuit-breaker/timeout depuis les handles `ArcSwap` du `ResilienceProfile` d'origine, donc un push
  `infra-config` reconfigure un channel live sans rebuild.
- **Le traffic est câblé mais inerte jusqu'à configuration** — `TrafficLayer` est toujours dans le type
  serveur mais no-op tant qu'aucun `TrafficRegistry` n'est fourni ; `service-runtime` fait ce câblage. Le
  mode shadow (`enforce=false`) charge les cellules sans rejeter, donc on observe
  `infra_traffic_throttled_total{status="shadow"}` puis on bascule `enforce=true` via ConfigMap sans
  redéploiement.
- **La propagation de trace dépend de `telemetry::init()`** — il enregistre le propagateur global ;
  `inject/extract_context` sont des no-op silencieux sans lui. Versions OTel épinglées sur celles de
  `telemetry` pour un contexte wire-compatible.

---

## 🔌 API publique & contrat

### Erreurs

```rust
pub enum TransportError { Grpc(GrpcTransportError), Kafka(KafkaTransportError), Codec(CodecError),
                          CircuitOpen, Timeout(Duration), MaxRetriesExhausted(u32) }
// From<tonic::transport::Error | tonic::Status | Grpc/Kafka/CodecError>; flatten helpers
//   from_resilience_connect(ResilienceError<tonic::transport::Error>) / from_resilience(ResilienceError<TransportError>)
pub enum GrpcTransportError { Connect(_), Status { code, message }, InvalidMetadata(String), Tls(String) }
pub fn grpc_severity(code: tonic::Code) -> error::Severity;   // Internal/DataLoss/Unknown→Critical, Unavailable/Deadline/ResourceExhausted→High, …
```

### gRPC client / server

```rust
impl GrpcClientBuilder {
    pub fn new(GrpcClientConfig) -> Self;
    pub async fn connect(self) -> Result<Channel, TransportError>;                              // raw, no middleware
    pub async fn build_traced(self) -> Result<OutboundTraceService<Channel>, TransportError>;   // + trace inject
    pub async fn build_resilient(self, &ResilienceProfile) -> Result<ResilientChannel, _>;      // trace+CB+timeout, hot-reloadable
    pub async fn build_from_registry(self, &ResilienceRegistry) -> Result<ResilientChannel, _>; // resolve via config.dependency
}
pub type ResilientChannel = BoxCloneService<http::Request<tonic::body::Body>, http::Response<tonic::body::Body>, TransportError>; // Clone

impl GrpcServerBuilder {
    pub fn new(GrpcServerConfig) -> Self;
    pub fn with_traffic(self, Arc<infra_config::TrafficRegistry>) -> Self;   // enable ingress limiting
    pub fn build(self) -> Result<TracedGrpcServer, TransportError>;          // InboundTraceLayer + TrafficLayer pre-installed
}
```

### Identité des appelants mesh (`grpc::mesh`, #852)

La NetworkPolicy est L4 : elle laisse un service atteindre un port, jamais une seule RPC. Une RPC
sensible réservée au mesh (gems dépensées, signalements d'un membre, export d'un wallet) vérifie aussi
**quel service** l'appelle :

```rust
// appelé : un des appelants prévus de la RPC (noms d'app ; un préfixe d'overlay correspond aussi)
self.mesh.require(&request, "SpendGems", &["geo-discovery-server"]).await?;
// appelant : son jeton de ServiceAccount projeté sur chaque appel (MESH_TOKEN_FILE)
WalletServiceClient::with_interceptor(channel, MeshTokenInterceptor::from_env());
```

Le jeton est le jeton de ServiceAccount projeté par Kubernetes de l'appelant (audience
`core-platform-mesh`), envoyé en `x-mesh-token` et vérifié contre l'émetteur du cluster
(`auth_context::mesh`). Modes (`MeshGateMode`) : `off` (rien de vérifié), `log` (un mauvais appelant
est journalisé, laissé passer), `enforce` (`PERMISSION_DENIED`) ; sans vérificateur, tout appelant est
inconnu. `edge::StaffGate::verified` vérifie aussi le jeton edge d'un **utilisateur final** qu'un
service transmet sur un appel mesh (le wallet vérifie qui dépense des gems).

### Kafka

```rust
pub struct EventEnvelope<T> { pub topic: String, pub key: String, pub payload: T, pub headers: HashMap<String,String>, pub timestamp_ms: Option<i64> }
pub trait PublishablePayload: Serialize + Send + Sync + 'static {}   // blanket
pub trait ConsumablePayload: DeserializeOwned + Send + Sync + 'static {}

#[derive(Clone)] pub struct KafkaProducerHandle;   // Arc-backed FutureProducer
impl KafkaProducerHandle { pub async fn publish<T: PublishablePayload>(&self, EventEnvelope<T>) -> Result<(), _>; pub async fn publish_raw(&self, topic, key, payload: &[u8], headers) -> Result<(), _>; }

pub struct ConsumedMessage<T> { /* topic, partition, offset, key, headers, timestamp_ms, raw_payload: Vec<u8>, payload: Result<T, TransportError> */ }
impl KafkaConsumerHandle {
    pub fn stream<T: ConsumablePayload>(&self) -> impl Stream<Item = Result<ConsumedMessage<T>, TransportError>> + '_; // decode err = payload Err, does NOT abort stream
    pub fn commit<T>(&self, &ConsumedMessage<T>) -> Result<(), TransportError>;   // commits offset+1 (manual commit by default)
}
```

### Propagation & codec

```rust
pub fn inject_context<C: Injector>(carrier: &mut C);                      // current span → carrier
pub fn extract_context<C: Extractor>(carrier: &C) -> opentelemetry::Context;
pub fn set_parent(span: &tracing::Span, cx: opentelemetry::Context);
// Carriers: GrpcHeaderInjector/Extractor (http::HeaderMap), KafkaHeaderInjector (OwnedHeaders) / Extractor (BorrowedHeaders)
pub fn json_encode/json_decode · proto_encode/proto_decode -> Result<_, CodecError>;   // CodecError → TransportError::Codec
```

> **Contrat :** `telemetry::init()` **doit** s'exécuter avant tout appel transport.
> `ResilientChannel` / `OutboundTraceService` / `KafkaProducerHandle` sont tous `Clone` à bas coût. Le
> commit du consommateur est de la responsabilité de l'appelant (`enable_auto_commit = false` par défaut)
> — mais en pratique vous ne l'appelez pas directement : utiliser `run_consumer` (ci-dessous).

---

## 📨 Standard du runtime de consommation (OBLIGATOIRE)

> **Chaque consommateur Kafka de la plateforme tourne sur `run_consumer`.** Ne pas écrire à la main la
> boucle stream/commit — le runner possède la machine à états par-message pour que retry, dead-lettering
> et gestion d'offset se comportent identiquement dans tous les services.

```rust
pub enum ProcessOutcome { Done, Retry(String), Reject(String) }
pub trait ClassifyError { fn is_retryable(&self) -> bool; }
impl ProcessOutcome { pub fn from_result<E: ClassifyError + Display>(r: Result<(), E>) -> Self; }
pub struct RetryPolicy { pub max_attempts: u32, pub base_backoff: Duration, pub max_backoff: Duration }
// Default: 5 attempts · 100 ms base · 30 s cap · exponential backoff + equal jitter.

pub async fn run_consumer<T: ConsumablePayload, F>(handle: &KafkaConsumerHandle, producer: &KafkaProducerHandle /* DLQ */,
    policy: &RetryPolicy, process: F) -> Result<(), TransportError>;   // F: for<'a> Fn(&'a T) -> Pin<Box<dyn Future<Output=ProcessOutcome> + Send + 'a>>
```

**Sémantique de livraison (le standard) :**

| Outcome | Action du runner |
|---|---|
| `Done` (succès ou skip intentionnel) | commit l'offset |
| `Retry` (transitoire) | backoff+jitter en place jusqu'à `max_attempts`, **puis** dead-letter + commit |
| `Reject` (permanent/poison) | dead-letter immédiatement + commit |
| échec de décodage (`payload = Err`) | dead-letter immédiatement + commit |
| erreur broker/stream **ou échec de publication DLQ** | renvoie `Err` **sans commit** → l'appelant reconstruit + reprend au dernier offset committé (sans perte) |

Committer seulement après un résultat terminal (succès *ou* dead-letter réussi) évacue un message poison
sans jamais le perdre. Les enregistrements DLQ portent
`x-dlq-origin-topic`/`-partition`/`-offset`/`-reason` (`decode`/`reject`/`retry-exhausted`)/`-error`/`-attempts`/`-failed-at-ms`
+ le contexte de trace.

**Règles d'écriture de worker :** (1) `enable_auto_commit = false` ; (2) `impl ClassifyError for
YourError` — les fautes transitoires storage/cache sont retryable, validation/mauvaise-donnée non
(déléguer à `AppError::is_retryable` quand disponible) ; (3) replier les skips intentionnels dans `Ok`
pour qu'ils committent au lieu d'inonder la DLQ ; (4) posséder le handle via `Arc<Self>` pour que la
closure par-message capture un handle possédé et que la future ne borrow que l'événement. `process`
reçoit seulement l'événement décodé, pas le topic — exécuter une boucle mono-topic par topic
(`group_id` partagé) quand la logique dépend de l'origine. **L'idempotence est de la responsabilité du
consommateur** (at-least-once = vraie re-livraison).

> **Papercut HRTB :** le bound de `process` est higher-ranked (`for<'a> Fn(&'a T) -> …'a`). Passer la
> closure **inline** et l'inférence fonctionne. Si vous la `let`-bindez *et* que sa future ne borrow pas
> l'événement, le compilateur infère une seule lifetime concrète et la rejette — la router via une
> coercition identité (`fn classify<T, F>(p: F) -> F where F: for<'a> Fn(&'a T) -> …`) pour forcer
> l'inférence expected-typed. La suite `tests/` utilise exactement ce helper.

---

## 📦 Intégration

```toml
[dependencies]
transport = { workspace = true }
```

```rust
let _guard = telemetry::init(TelemetryConfig::from_env("my-service", env!("CARGO_PKG_VERSION")))?; // FIRST

// gRPC server (InboundTraceLayer pre-installed)
let server = GrpcServerBuilder::new(GrpcServerConfig::default()).build()?.add_service(MyServiceServer::new(svc));

// gRPC client (resilient channel resolved from the hot-reloaded registry)
let channel = GrpcClientBuilder::new(GrpcClientConfig::new("https://dep:50051").with_dependency("post-command"))
    .build_from_registry(&registry).await?;
let mut client = DependencyServiceClient::new(channel);

// Kafka producer + consumer (consumer should run under run_consumer — see standard above)
producer.publish(EventEnvelope::new("domain.events", "entity-123", MyEvent { /*…*/ })).await?;
```

---

## ⚙️ Configuration & feature flags

**Env Kafka (`KafkaClientConfig::from_env`) :** `KAFKA_BROKERS` (défaut `localhost:9092`),
`KAFKA_SECURITY_PROTOCOL` (`PLAINTEXT`|`SASL_SSL`), `KAFKA_SASL_MECHANISM`/`USERNAME`/`PASSWORD`,
`KAFKA_DEBUG`. La config OTLP est possédée par `telemetry`.

**gRPC se configure programmatiquement** (sans env). Défauts : client `connect_timeout` 5s, resilience
`timeout` 10s ; serveur `addr` `0.0.0.0:50051`, `tls` None, `enable_reflection` false. **Défauts
producteur :** `acks=all`, `compression=snappy`, `linger_ms=5`, `max_in_flight=5`. **Défauts
consommateur :** `auto_offset_reset=Latest`, `enable_auto_commit=false`, `heartbeat_interval_ms=3000`,
`session_timeout_ms=10000`.

**Feature flags :** `integration-kafka` — gate la suite de tests broker live (Docker uniquement ; off par
défaut).

---

## 🔭 Observabilité

Spans auto : `grpc.server` (`rpc.system=grpc`, `rpc.method`) ; le client gRPC injecte
`traceparent`/`tracestate` ; le consommateur Kafka pose le contexte distant extrait comme parent. OTel
épinglé : `opentelemetry 0.27`, `tracing-opentelemetry 0.28` (alignés sur `telemetry`).

Alertes suggérées : taux `CircuitOpen` ⇒ critique ; `Timeout` > 1% ⇒ high ;
`KafkaTransportError::Producer` non nul ⇒ high ; lag du consumer-group > SLA ⇒ high ; `Codec` non nul ⇒
medium (mismatch de schéma). Les instruments meter Prometheus sont un TODO planifié.

---

## 🧪 Tests

```bash
cargo test   -p transport                          # hermetic unit tests, no Docker
cargo clippy -p transport --all-targets
cargo test   -p transport --features integration-kafka   # live run_consumer suite (Scenarios A–K, ~16s)
```

La suite d'intégration est autonome : `tests/harness/mod.rs` lance un conteneur éphémère
`apache/kafka-native` (KRaft) via `testcontainers` (topics/groups namespacés en UUIDv7, pré-création
explicite, une primitive de poll `await_until` — jamais `sleep`) ; `tests/consumer_runtime.rs` tient les
scénarios, y compris la preuve at-least-once « échec de dead-letter ⇒ pas de commit + re-livraison ».

---

## 🚨 Pièges / FAQ

> Les arêtes vives. Une entrée par piège réel. (Invariants architecturaux à préserver par les contributeurs.)

**1. Les spans apparaissent déconnectés / `traceparent` manquant.**
`telemetry::init()` n'a pas tourné avant le premier appel transport — le propagateur global n'est pas
enregistré, donc `inject/extract_context` sont des no-op. L'initialiser d'abord et garder `_guard` vivant
pour tout `main`.

**2. `TransportError::CircuitOpen` — requêtes rejetées sans atteindre le distant.**
Le `CircuitBreakerLayer` a trip sur des échecs amont répétés. Vérifier la santé/sonde gRPC de l'amont et
les logs `Status { code: Unavailable }` récents ; le circuit semi-ouvre et ferme à la récupération. Régler
`CircuitBreakerConfig` ou ajouter `RetryLayer` **à la couche applicative** pour les pics transitoires.

**3. Le consommateur Kafka ne reçoit rien au démarrage.**
`auto_offset_reset = Latest` (défaut) sans offset committé ⇒ il attend à la pointe de la partition et
saute le backlog. Utiliser `Earliest` pour rejeu/bootstrap. Cause secondaire : deux instances partageant
un `group_id` — vérifier l'assignation avec `kafka-consumer-groups --describe`.

**4. `tonic::body::BoxBody` ne compile pas dans une signature publique.**
Il est **privé** dans tonic 0.14.x — ne jamais le nommer dans des types publics. Utiliser
`tonic::body::Body` (comme `ResilientChannel`).

**5. `KafkaHeaderInjector` et le contrat `&mut self` d'`Injector`.**
`OwnedHeaders::insert` est un builder *consommant* ; l'injector déplace la possession via
`std::mem::replace` pour satisfaire `&mut self`. Préserver ce pattern si vous y touchez.

**6. `InboundTraceLayer` a changé mon type de future mais `OutboundTraceLayer` non.**
L'inbound enveloppe la future dans `Instrument` (→ `BoxFuture`) ; l'outbound est sans coût (`type Future =
S::Future`). Attendu — ne pas essayer de rendre l'inbound sans coût.

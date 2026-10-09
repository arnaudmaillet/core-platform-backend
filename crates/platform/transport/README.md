# `transport` — Platform communication layer: gRPC + Kafka with automatic trace propagation

> **Crate Card**
>
> | | |
> |---|---|
> | **Role** | `platform` — the shared inter-service communication layer (no business logic) |
> | **Package** | `transport` (dir: `crates/platform/transport`) |
> | **Consumed by** | every service (gRPC clients/servers, Kafka producers/consumers) |
> | **Depends on** | `tonic`/`tower`, `rdkafka`, `resilience`, `traffic`, `telemetry`, `error`, `opentelemetry` |
> | **Stability** | stable contract (`run_consumer` is a mandatory fleet standard) |
> | **Feature flags** | `integration-kafka` (live-broker test suite) |
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |

---

## 🎯 Overview & role

`transport` is the single shared crate every service uses for inter-process communication, over two
paradigms behind one opinionated API: **synchronous gRPC** (Tonic + Tower, optional TLS/mTLS,
circuit-breaking, timeout) and **asynchronous Kafka** (rdkafka, typed envelopes, at-least-once). Its
defining property: **both transports auto-propagate W3C TraceContext** (`traceparent`/`tracestate`)
across every RPC and every Kafka message, producing end-to-end traces with zero boilerplate.

**Architectural boundary** — pure infrastructure, **no business logic**. It owns the wire, the trace
plumbing, the resilience/traffic layer wiring, and the consumer runtime; it does not own domain types,
schemas, or handler logic.

---

## 📐 Architecture & key decisions

```
caller span ─[gRPC]─ OutboundTraceLayer: inject_context → HeaderMap ──HTTP/2──►
            └[Kafka]─ producer.publish: inject_context → OwnedHeaders ──record──►
receiver    ─[gRPC]─ InboundTraceLayer: extract_context ← HeaderMap → span.set_parent(remote)
            └[Kafka]─ consumer.stream: extract_context ← BorrowedHeaders → set_parent

gRPC client stack: TimeoutLayer → CircuitBreakerLayer → OutboundTraceLayer → tonic Channel  (→ ResilientChannel)
gRPC server stack: InboundTraceLayer (outer, traces even throttled reqs) → TrafficLayer (ingress limit) → handler
```

- **No `RetryLayer` at the transport level** — HTTP/2 bodies are streams; replaying one means buffering
  the full payload (cost-prohibitive). Apply retry at the **application layer** (around the generated
  tonic client call, not the channel). Kafka gets at-least-once instead, via `run_consumer`.
- **Resilience config is hot-reloadable** — `ResilientChannel` reads circuit-breaker/timeout values
  from the originating `ResilienceProfile`'s `ArcSwap` handles, so an `infra-config` push reconfigures
  a live channel with no rebuild.
- **Traffic is wired but inert until configured** — `TrafficLayer` is always in the server type but a
  no-op until a `TrafficRegistry` is supplied; `service-runtime` does that wiring. Shadow mode
  (`enforce=false`) charges cells without rejecting, so you watch `infra_traffic_throttled_total{status="shadow"}`
  then flip `enforce=true` via ConfigMap with no redeploy.
- **Trace propagation depends on `telemetry::init()`** — it registers the global propagator;
  `inject/extract_context` are silent no-ops without it. OTel versions are pinned to `telemetry`'s for
  wire-compatible context.

---

## 🔌 Public API & contract

### Errors

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

### Mesh caller identity (`grpc::mesh`, #852)

The NetworkPolicy is L4: it lets a service reach a port, never one RPC. A sensitive mesh-only RPC
(gems spent, someone's reports, a wallet export) also checks **which service** calls it:

```rust
// callee: one of the RPC's intended callers (app names; an overlay prefix matches too)
self.mesh.require(&request, "SpendGems", &["geo-discovery-server"]).await?;
// caller: its projected ServiceAccount token on every call (MESH_TOKEN_FILE)
WalletServiceClient::with_interceptor(channel, MeshTokenInterceptor::from_env());
```

The token is the caller's Kubernetes projected ServiceAccount token (audience `core-platform-mesh`),
sent as `x-mesh-token` and verified against the cluster's issuer (`auth_context::mesh`). Modes
(`MeshGateMode`): `off` (nothing checked), `log` (a wrong caller is logged, let through), `enforce`
(`PERMISSION_DENIED`); without a verifier, every caller is unknown. `edge::StaffGate::verified` also
checks an **end user's** edge token a service forwards on a mesh call (the wallet checks who spends
gems).

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

> **Contract notes:** `telemetry::init()` **must** run before any transport call. `ResilientChannel` /
> `OutboundTraceService` / `KafkaProducerHandle` are all cheaply `Clone`. Consumer commit is the
> caller's responsibility (`enable_auto_commit = false` by default) — but in practice you don't call it
> directly: use `run_consumer` (below).

---

## 📨 Consumer runtime standard (MANDATORY)

> **Every Kafka consumer in the platform runs on `run_consumer`.** Do not hand-roll the stream/commit
> loop — the runner owns the per-message state machine so retry, dead-lettering, and offset management
> behave identically across all services.

```rust
pub enum ProcessOutcome { Done, Retry(String), Reject(String) }
pub trait ClassifyError { fn is_retryable(&self) -> bool; }
impl ProcessOutcome { pub fn from_result<E: ClassifyError + Display>(r: Result<(), E>) -> Self; }
pub struct RetryPolicy { pub max_attempts: u32, pub base_backoff: Duration, pub max_backoff: Duration }
// Default: 5 attempts · 100 ms base · 30 s cap · exponential backoff + equal jitter.

pub async fn run_consumer<T: ConsumablePayload, F>(handle: &KafkaConsumerHandle, producer: &KafkaProducerHandle /* DLQ */,
    policy: &RetryPolicy, process: F) -> Result<(), TransportError>;   // F: for<'a> Fn(&'a T) -> Pin<Box<dyn Future<Output=ProcessOutcome> + Send + 'a>>
```

**Delivery semantics (the standard):**

| Outcome | Runner action |
|---|---|
| `Done` (success or intentional skip) | commit the offset |
| `Retry` (transient) | in-place backoff+jitter up to `max_attempts`, **then** dead-letter + commit |
| `Reject` (permanent/poison) | dead-letter immediately + commit |
| decode failure (`payload = Err`) | dead-letter immediately + commit |
| broker/stream error **or DLQ publish failure** | return `Err` **without committing** → caller rebuilds + resumes from last committed offset (no loss) |

Committing only after a terminal outcome (success *or* successful dead-letter) evacuates a poison
message without ever losing it. DLQ records carry `x-dlq-origin-topic`/`-partition`/`-offset`/`-reason`
(`decode`/`reject`/`retry-exhausted`)/`-error`/`-attempts`/`-failed-at-ms` + the trace context.

**Worker authoring rules:** (1) `enable_auto_commit = false`; (2) `impl ClassifyError for YourError` —
transient storage/cache faults retryable, validation/bad-data not (delegate to `AppError::is_retryable`
where available); (3) fold intentional skips into `Ok` so they commit instead of flooding the DLQ;
(4) own the handle via `Arc<Self>` so the per-message closure captures an owned handle and the future
borrows only the event. `process` receives only the decoded event, not the topic — run one single-topic
loop per topic (shared `group_id`) when logic depends on origin. **Idempotency is the consumer's
responsibility** (at-least-once = real redelivery).

> **HRTB papercut:** `process`'s bound is higher-ranked (`for<'a> Fn(&'a T) -> …'a`). Pass the closure
> **inline** and inference works. If you `let`-bind it *and* its future doesn't borrow the event, the
> compiler infers one concrete lifetime and rejects it — route it through an identity coercion
> (`fn classify<T, F>(p: F) -> F where F: for<'a> Fn(&'a T) -> …`) to force the expected-typed inference.
> The `tests/` suite uses exactly this helper.

---

## 📦 Integration

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

**Kafka env (`KafkaClientConfig::from_env`):** `KAFKA_BROKERS` (default `localhost:9092`),
`KAFKA_SECURITY_PROTOCOL` (`PLAINTEXT`|`SASL_SSL`), `KAFKA_SASL_MECHANISM`/`USERNAME`/`PASSWORD`,
`KAFKA_DEBUG`. OTLP config is owned by `telemetry`.

**gRPC is configured programmatically** (no env). Defaults: client `connect_timeout` 5s, resilience
`timeout` 10s; server `addr` `0.0.0.0:50051`, `tls` None, `enable_reflection` false. **Producer
defaults:** `acks=all`, `compression=snappy`, `linger_ms=5`, `max_in_flight=5`. **Consumer defaults:**
`auto_offset_reset=Latest`, `enable_auto_commit=false`, `heartbeat_interval_ms=3000`,
`session_timeout_ms=10000`.

**Feature flags:** `integration-kafka` — gates the live-broker test suite (Docker-only; off by default).

---

## 🔭 Observability

Auto spans: `grpc.server` (`rpc.system=grpc`, `rpc.method`); gRPC client injects `traceparent`/`tracestate`;
Kafka consumer sets the extracted remote context as parent. Pinned OTel: `opentelemetry 0.27`,
`tracing-opentelemetry 0.28` (match `telemetry`).

Suggested alerts: `CircuitOpen` rate ⇒ critical; `Timeout` > 1% ⇒ high; `KafkaTransportError::Producer`
nonzero ⇒ high; consumer-group lag > SLA ⇒ high; `Codec` nonzero ⇒ medium (schema mismatch). Prometheus
meter instruments are a planned TODO.

---

## 🧪 Testing

```bash
cargo test   -p transport                          # hermetic unit tests, no Docker
cargo clippy -p transport --all-targets
cargo test   -p transport --features integration-kafka   # live run_consumer suite (Scenarios A–K, ~16s)
```

The integration suite is self-contained: `tests/harness/mod.rs` boots one ephemeral
`apache/kafka-native` (KRaft) container via `testcontainers` (UUIDv7-namespaced topics/groups, explicit
pre-creation, an `await_until` poll primitive — never `sleep`); `tests/consumer_runtime.rs` holds the
scenarios incl. the at-least-once "failed dead-letter ⇒ no commit + redelivery" proof.

---

## 🚨 Gotchas / FAQ

> The sharp edges. One entry per real trap. (Architectural invariants contributors must preserve.)

**1. Spans appear disconnected / `traceparent` missing.**
`telemetry::init()` didn't run before the first transport call — the global propagator isn't
registered, so `inject/extract_context` are no-ops. Init first and keep `_guard` alive for all of `main`.

**2. `TransportError::CircuitOpen` — requests rejected without reaching the remote.**
The `CircuitBreakerLayer` tripped on repeated upstream failures. Check the upstream's health/gRPC probe
and recent `Status { code: Unavailable }` logs; the circuit half-opens and closes on recovery. Tune
`CircuitBreakerConfig` or add `RetryLayer` **at the application layer** for transient spikes.

**3. Kafka consumer receives nothing at startup.**
`auto_offset_reset = Latest` (default) with no committed offset ⇒ it waits at the partition tip and
skips the backlog. Use `Earliest` for replay/bootstrap. Secondary cause: two instances sharing a
`group_id` — verify assignment with `kafka-consumer-groups --describe`.

**4. `tonic::body::BoxBody` won't compile in a public signature.**
It is **private** in tonic 0.14.x — never name it in public types. Use `tonic::body::Body` (as
`ResilientChannel` does).

**5. `KafkaHeaderInjector` and the `&mut self` `Injector` contract.**
`OwnedHeaders::insert` is a *consuming* builder; the injector moves ownership out via
`std::mem::replace` to satisfy `&mut self`. Preserve that pattern if you touch it.

**6. `InboundTraceLayer` changed my future type but `OutboundTraceLayer` didn't.**
Inbound wraps the future in `Instrument` (→ `BoxFuture`); outbound is zero-cost (`type Future =
S::Future`). Expected — don't try to make inbound zero-cost.

# `service-runtime` — The unified fleet bootstrap: implement one trait, get a deployable service

> **Crate Card**
>
> | | |
> |---|---|
> | **Role** | `platform` — the shared boot sequence every service runs |
> | **Package** | `service-runtime` (dir: `crates/platform/service-runtime`) |
> | **Consumed by** | every `crates/apps/<svc>-server` binary (via `serve::<S>(addr)`) |
> | **Depends on** | `tonic`, `telemetry`, `infra-config`, `traffic`, `health`, `error` |
> | **Stability** | stable contract (`Service` trait) |
> | **Feature flags** | none |
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |

---

## 🎯 Overview & role

`service-runtime` is the unified fleet bootstrap. Every service runs the **same** boot sequence by
implementing one trait (`Service`); the deployable binary is then a one-liner. `serve::<S>(addr)` owns
all the process-wide concerns so no service re-implements them.

**Architectural boundary** — the runtime owns infrastructure (telemetry, config, ingress layers,
health, shutdown); the service owns domain (wiring, gRPC services, probes). The split is **enforced by
a type-erased `RoutesBuilder` seam**: `register` never sees the Tower layer stack the runtime wraps
around it.

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

## 📐 Architecture & key decisions

| Concern | Owner |
|---|---|
| Telemetry init, OTLP, log/sampling dials | **runtime** (`serve`) |
| Config load + hot-reload watcher | **runtime** |
| Ingress trace + rate-limit layers, prune loop | **runtime** |
| gRPC health, readiness loop, graceful shutdown | **runtime** |
| Domain wiring (repos, caches, buses, workers) | **service** (`build`) |
| Concrete gRPC services + reflection | **service** (`register`) |
| Backend probes | **service** (`health_probes`) |

- **One trait, total surface** — adding a service to the fleet = a `service.rs` implementing `Service`
  + an `apps/<svc>-server` crate that's ~10 lines. Nothing else.
- **Type-erased seam** — `register(&mut RoutesBuilder)` keeps the Tower layer types out of the
  service's signature, so the runtime can change the layer stack fleet-wide without touching services.
- **Health reflects real dependencies** — with probes, a service starts `NOT_SERVING` and flips to
  `SERVING` only after all probes pass (and back on any failure), so K8s readiness tracks dependency
  reachability, not mere process liveness.

---

## 🔌 Public API & contract

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

The deployable binary:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("CHAT_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_owned()).parse()?;
    service_runtime::serve::<ChatService>(addr).await
}
```

> **Contract notes:** `GRPC_SERVICE_NAME` **must** equal the concrete tonic server's
> `NamedService::NAME` — it's the key the health service reports under; a mismatch leaves the service
> stuck `NOT_SERVING` from the client's view. `build`'s `infra` carries the hot-reloadable registries;
> services that don't consume externalized policy may ignore it.

---

## 📦 Integration

```toml
[dependencies]
service-runtime = { workspace = true }
```

See the `Service` impl and binary in §Public API — that is the whole integration surface.

---

## ⚙️ Configuration & feature flags

| Variable | Default | Effect |
|---|---|---|
| `INFRA_CONFIG_PATH` | `infrastructure.toml` | Externalized-config document path |
| `HEALTH_PROBE_INTERVAL_SECS` | `10` | Readiness poll cadence |
| `TRAFFIC_PRUNE_INTERVAL_SECS` | `60` | Rate-limiter memory-bounding cadence |
| `GRPC_TRUSTED_PROXY_HOPS` | `1` | Proxies appending to `X-Forwarded-For` before the listeners (the ALB); `per_ip` rate limits key on the entry that many places from the right. Bump it when a CDN/WAF proxy is put in front |
| `MESH_CALLER_GATE` | unset (`off`) | `mesh_gate_from_env()` (#852): `off` / `log` / `enforce` — whether a gated mesh RPC checks its caller's ServiceAccount token |
| `MESH_JWKS_URL` · `MESH_TOKEN_ISSUER` · `MESH_TOKEN_AUDIENCE` | unset · unset · `core-platform-mesh` | where and how mesh tokens are verified (in-cluster: `https://kubernetes.default.svc/openid/v1/jwks`) |
| `MESH_JWKS_CA_FILE` · `MESH_JWKS_BEARER_FILE` | unset | the in-cluster JWKS's CA and the bearer token sent with each fetch |
| `MESH_NAMESPACE` | this pod's namespace | callers must be in this namespace (default: the mounted ServiceAccount's `namespace`) |
| `MESH_TOKEN_FILE` | unset | the caller side (`MeshTokenInterceptor::from_env`): this pod's projected token; unset → none sent |

Per-service `*_GRPC_ADDR` + tuning live in each service's README. Telemetry honours `RUST_LOG` /
`OTEL_*` at boot; live dials are then driven by the `[telemetry]` section of `infrastructure.toml`. No
cargo features.

**Live retuning** — because the runtime spawns the config watcher, an `infrastructure.toml` push
retunes the fleet with no restart (`[telemetry]` log filter + sampling; `[traffic]` rps/quotas;
`[resilience]` timeouts/breakers).

---

## 🧪 Testing

```bash
cargo test   -p service-runtime
cargo clippy -p service-runtime --all-targets
```

---

## 🚨 Gotchas / FAQ

> The sharp edges. One entry per real trap.

**1. The service builds and serves but clients see it `NOT_SERVING` forever.**
`GRPC_SERVICE_NAME` doesn't match the concrete server's `NamedService::NAME`. Set it via the
`NamedService` impl (`<MyServer<…> as tonic::server::NamedService>::NAME`) so the health key lines up.

**2. The service never becomes ready.**
A `health_probes()` probe never passes — the runtime keeps it `NOT_SERVING` until all probes succeed.
Check the probe's `check()` against the live backend; remember any probe `Err` demotes the whole service.

**3. A config change didn't take effect without a restart.**
Only profile *contents* hot-reload; topology changes need a restart (see `infra-config`). Confirm the
watcher is alive (the runtime owns it) and `INFRA_CONFIG_PATH` points at the mounted document.

**4. Layer types leaked into my `register` signature.**
They shouldn't — `register` only sees `&mut RoutesBuilder`. If you're trying to add a Tower layer
there, you're at the wrong seam; the runtime owns the layer stack.

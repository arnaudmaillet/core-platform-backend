# `realtime` — Hold millions of live client connections, deliver every event to the exact device the instant it happens, and own none of the truth

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **On-call / escalation** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — the live surface a hyperscale app feels broken without, but **derived and fail-open**: not a System of Record, not in any synchronous write path; an outage degrades to "reconnect and re-sync", it never loses or blocks a message |
> | **Deployable** | **two** binaries — `crates/apps/realtime-gateway` (stateful edge, holds the connections) **and** `crates/apps/realtime-dispatcher` (stateless fan-out worker). Library crate: `crates/services/realtime` |
> | **Listeners** | **two planes** (the fleet's first) — a public **WSS** client plane (default `:8443`, behind an L4 LB) on the gateway, and an internal **gRPC** health/control plane (`:50066` gateway · `:50067` dispatcher) |
> | **Datastores** | **Redis** only — the connection/presence registry (`user → node` routing) + the node-hop Pub/Sub fabric. Owns no entity, persists no message |
> | **Async** | consumes `notification.v1.events` (targeted), `counter.v1.popularity` + `post.v1.events` (public broadcast) (Kafka). Publishes nothing of record. `chat` stays on its own live plane (coexist) |
> | **Upstream callers** | end-user clients over WSS (mobile / web) — **not** internal services |
> | **Downstream deps** | Redis, Kafka, and `auth` (edge-token verification via the `auth-context` library — a verify, not a call). Message/notification durability stays in `chat` / `notification` |
> | **SLO** | `<TODO>` avail · event→device p99 `< <TODO ~250> ms` for online users · connection setup p99 `< <TODO> ms` |

---

## 🎯 Overview & Service Role

`realtime` is the platform's **client-facing live delivery plane**: it terminates millions of long-lived, multiplexed client connections, fans internal events out to the exact device that should see them, and owns **no** entity. Every byte it forwards is already durable in its owning service — `chat` persisted the message, `notification` persisted the badge, `counter` holds the magnitude. It is a System-of-**Connection / Delivery**, never a System of Record.

The hard problem it solves is twofold. First, **the inverted-load trap of polling**: at hyperscale, clients polling the edge couple core-service QPS to the count of *idle* users — every empty poll pays a full TLS + auth + fan-out cost to deliver nothing, pointing a self-inflicted DDoS at the internal mesh. Push inverts this so internal work tracks *events*, not eyeballs. Second, **connection sprawl**: `chat` and `notification` each grew their own client-facing streaming, so a single device holds multiple sockets with redundant heartbeats waking the radio independently. Realtime collapses that into **one multiplexed socket per device** and reduces those services to event *producers*.

**Core objectives:** (1) one persistent, multiplexed connection per device — battery- and firewall-friendly; (2) events travel core → device with no new synchronous call into the mesh (Kafka in, targeted fan-out out); (3) the plane is a **structural bulkhead** — millions of flaky connections terminate here and never reach the gRPC mesh, which only sees a bounded set of stable gateway peers; (4) posture is **fail-open** — a delivery miss costs latency, never data, because durability lives in the SoRs and clients re-sync on reconnect.

| Concern | Path | Latency contract | Notes |
|---|---|---|---|
| **Client transport** | WSS on `:443`/`:8443`, multiplexed envelope | persistent | one socket per device; logical channels (`dm` / `notif` / `presence` / `counter:<id>`) |
| **Ingestion** | async Kafka consumers (`run_consumer`) in `realtime-dispatcher` | none (off the write path) | resolve recipient → owning node → targeted publish |
| **Last hop** | registry lookup → node-hop (Redis Pub/Sub) → socket write | sub-second for online users | best-effort; a miss is re-synced from the SoR on reconnect |

---

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), Kafka for ingestion, Redis for the routing fabric. The defining structural choice is **two deployables**: the stateful edge gateway and the stateless fan-out dispatcher share a domain crate but no process, deployment, or failure domain. A gateway node OOMing on connections must never be able to brown out a core service.

```
                       Internet — millions of mobile/web clients
                                     │  WSS :443
                              ┌──────┴───────┐
                              │  L4 Load Bal │   (no L7 WS termination — avoid the ephemeral-port wall)
                              └──────┬───────┘
                  ┌──────────────────┼──────────────────┐
            ┌─────┴─────┐      ┌─────┴─────┐      ┌──────┴────┐
            │ gateway 0 │ ···  │ gateway 7 │ ···  │ gateway N │   realtime-gateway (stateful edge)
            └─────┬─────┘      └─────┬─────┘      └──────┬────┘   autoscale on conns + mem (NOT CPU)
                  │  registry writes / node:{id} subscribe   │
                  └────────────────┬─────────────────────────┘
                            ┌──────┴──────┐        ┌────────────────┐
                            │    Redis    │◄───────│   dispatcher   │  realtime-dispatcher
                            │ registry +  │ publish│  run_consumer  │  (stateless fan-out)
                            │  pub/sub    │  to    └───────┬────────┘
                            └─────────────┘ node:{id}      │ consume
                                                     ┌─────┴──────┐
                                                     │   Kafka    │  chat · notification ·
                                                     │ (existing) │  counter.v1.popularity · post.v1
                                                     └─────┬──────┘
                                          ┌────────────────┴────────────────┐
                                          │  core gRPC mesh (SoRs) — shielded │  emit-once; never sees a client
                                          └───────────────────────────────────┘
```

**The internal→external bridge is three stages.** (A) Core services **emit once** to Kafka — the plane adds no new synchronous call inward. (B) The dispatcher resolves the recipient against the **connection registry** (`presence:{user_id}` → the node(s) holding that user's live sockets) — *targeted* delivery, never broadcast-and-filter. (C) The last hop publishes the event to the owning node's Redis channel; the gateway hands it to the connection's bounded mailbox and writes the socket. Core to device = one Kafka hop + one Redis hop + one socket write.

> **Invariants** (and where enforced):
> - **The plane stores nothing.** It owns the connection and the ephemeral routing registry, never content. A registry miss (recipient offline) is a no-op — durability and the locked-phone push path belong to `chat` / `notification` — domain + application.
> - **Authenticate once, at the handshake.** The edge token is verified at the WS upgrade via `auth-context`; never re-verified per frame (that would reintroduce the per-message mesh cost the plane exists to kill) — infrastructure boundary. Its **revocation** is re-checked on the heartbeat (#903): one cached lookup per socket per tick, not per frame.
> - **The only authorization is channel-scope ownership.** A connection may subscribe only to channels scoped to its pinned identity (Alice → `dm:alice`, never `dm:bob`). Fine-grained "is this content visible" was authorized upstream when the event was emitted — domain.
> - **A slow consumer is shed, not buffered.** Each connection has a bounded send queue; on overflow the plane drops/disconnects rather than letting one bad network balloon node memory — infrastructure.
> - **Fail-open, always.** A lost live event is never a lost message; the client re-syncs from the SoR via its sequence token on reconnect — application.

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (successful handshakes / non-`UNAVAILABLE`) | `<TODO 99.9%>` | 30d rolling | `<metric>` |
| Event→device latency p99 (online users) | `< <TODO 250> ms` | 1h | `<metric>` |
| Connection setup p99 | `< <TODO> ms` | 1h | `<metric>` |
| Per-connection memory ceiling | `< <TODO> KB` | live | RSS / open_connections |

**Error budget:** `<TODO>`. **On burn:** `<freeze rollout | page>`. Because `realtime` is fail-open, the *availability* objective covers live-delivery degradation (reconnect-and-re-sync), not durability — durability is owned by the upstream SoRs and is out of this service's budget.

---

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

**Downstream — what `realtime` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Redis | connection/presence registry + node-hop Pub/Sub | dispatcher can't resolve / publish | **Soft** — live delivery stalls; clients re-sync from SoRs on reconnect; no data lost |
| Kafka | upstream event ingestion | fan-out stops advancing | **Soft** — live updates lag; nothing lost (manual commit); clients catch up via SoR |
| `auth` (via `auth-context`) | edge-token verification at handshake | new connections can't authenticate | **Hard for new connects** — existing connections unaffected until token expiry |

**Upstream — who depends on `realtime` (your blast radius if YOU fail):**

| Caller | Uses | User-visible impact if `realtime` is down |
|---|---|---|
| end-user clients | the live WSS stream | DMs / notifications / live counters stop arriving *instantly* — they appear on next app open or reconnect (re-synced from `chat` / `notification`); **nothing is lost** |

> **Critical path?** **No** — derived, async, fail-open. `realtime` is never in the synchronous path of sending a message, persisting a notification, or any write. It accelerates delivery; it does not own it.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### Client transport — WSS multiplexed envelope *(Phase 1)*

The client-facing surface is **not** gRPC. Clients connect over **WebSocket Secure** and exchange a compact, length-prefixed binary **envelope** — `{ stream_seq, channel, ack_required, payload }` — that imposes logical channels (`dm`, `notif`, `presence`, `counter:<id>`, `control`) over one physical socket. The handshake carries the edge token; thereafter frames are not re-authenticated. `WebTransport` over HTTP/3 is a designed-in, deferred successor lane the client negotiates and falls back from.

### Internal gRPC — health / control *(Phase 1)*

The internal plane is operational only: health + reflection on `:50066` (gateway) / `:50067` (dispatcher), plus the optional dispatcher↔gateway delivery RPC if the node-hop fabric moves off Redis Pub/Sub. **There is no client-facing gRPC and no domain write RPC.**

### Rust ports (hexagonal contract) *(Phase 3)*

```rust
#[async_trait] pub trait ConnectionRegistry { /* bind · resolve(user → nodes) · evict — the routing fabric */ }
#[async_trait] pub trait NodeChannel        { /* subscribe(node) · publish(node, event) — the last hop */ }
#[async_trait] pub trait TokenVerifier      { /* verify the edge token at handshake (auth-context) */ }
#[async_trait] pub trait EventSource        { /* the upstream Kafka streams the dispatcher fans out */ }
```

### Error contract

Every fault implements `error::AppError` with a stable `RTM-XXXX` code, mapped to gRPC `Status` / HTTP by the shared `error` crate:

| Range | Class |
|---|---|
| `RTM-1xxx` | connection handshake / authentication |
| `RTM-2xxx` | transport / framing / protocol |
| `RTM-3xxx` | subscription authorization (channel-scope ownership) |
| `RTM-4xxx` | delivery-fabric availability (fail-open core; retryable) |
| `RTM-5xxx` | connection lifecycle / backpressure |
| `RTM-8xxx` | inbound event decode / routing (dispatcher) |
| `RTM-9xxx` | cross-cutting (domain/parse, event consumption) |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

> Kafka topics are an API. A schema change in a consumed topic breaks delivery exactly like a proto change.

**Publishes:** none of record. (Presence, if surfaced, is internal liveness only — see the blueprint; it is not a System-of-Record stream.)

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| `notification.v1.events` | `realtime-notif-fanout` | **targeted** — deliver notification/badge updates to the recipient's `notif:<user>` channel | DLQ `notification.v1.events.dlq` |
| `counter.v1.popularity` | `realtime-counter-fanout` | **broadcast** — deliver live engagement spikes to viewers subscribed to `counter:<entity>` | DLQ `counter.v1.popularity.dlq` |
| `post.v1.events` | `realtime-post-fanout` | **broadcast** — deliver post lifecycle to subscribers of the author's `feed:<profile_id>` | DLQ `post.v1.events.dlq` |

> **Two fan-out modes.** *Targeted* events (notification — and DM/presence) name a recipient user; the dispatcher resolves them via the registry and hops to the owning node(s). *Broadcast* events (counter / feed) name an entity, not a user; the dispatcher publishes them once to the fleet broadcast channel, and every node delivers to its local connections subscribed to that channel. `chat.message.sent` is **not** consumed — it carries no member list and chat already runs its own live plane (the coexist-first decision); wiring it is a deliberate consolidation, not a decode.

> **Runtime contract (mandatory):** all dispatcher consumers run under `run_consumer` — manual commit after a terminal outcome, bounded retry with backoff + jitter, DLQ on exhaustion/poison, rebuild-from-last-committed-offset on broker error. **Idempotency:** fan-out is naturally idempotent — a redelivered event re-resolves the registry and re-delivers; duplicate live frames are harmless (the client dedupes on `stream_seq`). An unroutable/unknown event (`RTM-8002` / `RTM-8003`) is folded into `Ok` so the offset still commits.

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Redis registry/pub-sub down | live delivery stalls | **fail-open** — events queue/retry; clients re-sync from SoRs on reconnect; no loss | restore Redis; fan-out resumes |
| Kafka unavailable | live updates lag | dispatcher idles; offsets uncommitted → no loss | restore brokers; catch up |
| `auth` unreachable | new handshakes fail | existing connections unaffected; new connects rejected (`RTM-1001`) until recovery | restore `auth`; verify `auth-context` config |
| Slow/wedged client | one connection's queue fills | **shed** — drop oldest / disconnect (`RTM-5001`); node memory protected | none (by design) |
| Half-open connection | FD + registry slot leak | heartbeat reaper times it out (`RTM-5002`), frees the slot, flips presence offline | none (by design); watch reaper metrics |
| Node rollout / drain | connections must move | stop-accept → reconnect control frame **with jittered backoff** (`RTM-5003`) → drain | none (by design); watch reconnect-herd metrics |
| Reconnect thundering herd | auth handshake spike on deploy | client jittered backoff + the L4 LB spread absorb it | confirm jitter config; stagger rollouts |

**Backpressure & limits:** bounded per-connection send queue (shed on overflow); per-connection subscription cap; inbound frame-size cap; heartbeat deadline reaping; autoscale on connections + memory (an idle-but-full node is ~0% CPU — CPU-based autoscaling under-provisions into OOM).

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
realtime = { path = "crates/services/realtime" }
```

Library-only. Will implement [`service_runtime::Service`](../../platform/service-runtime/README.md) **twice** (Phase 5): `realtime::service::RealtimeGatewayService` (the `realtime-gateway` binary — the WSS accept loop, registry writes, node-hop subscription, drain hook, internal gRPC health) and `realtime::service::RealtimeDispatcherService` (the `realtime-dispatcher` binary — the supervised `run_consumer` fan-out loops, no domain RPC). Telemetry, config + hot-reload, health, and graceful shutdown are owned by the runtime.

> **Build status:** **complete through Phase 7 + the public-broadcast fan-out path.** 58 unit tests plus a live `integration-realtime` suite (real Redis) cover the domain, the proto codec, the bounded-shed mailbox, the node-local routing table (targeted + broadcast delivery, shed-under-pressure, graceful `broadcast_drain`), the upstream decode mappings (notification / counter / post), and the end-to-end bridges (targeted: fan-out → registry → node hop → socket; broadcast: fan-out → fleet channel → channel index → socket). Security-reviewed: no token or PII in logs, the opaque payload is never logged, and there is no panic in the accept or delivery hot path.
>
> **Deferred (explicit, not gaps):** the live WebSocket-accept-loop + auth/JWKS integration test (needs a live IdP and a browser-grade WS client — the routing fabric *is* covered live); the live Kafka dispatcher IT (the `run_consumer` runner is covered by `transport`'s own suite); the WebTransport / HTTP-3 transport lane; presence as a product-facing stream; cross-region / multi-PoP routing; the internal-gRPC `NodeChannel` backpressure variant; the `SIGTERM` → `broadcast_drain` runtime wiring; and the `chat` + `notification` client-streaming consolidation (coexist-first).
>
> **Authorization (deployment requirement):** `realtime` authenticates the edge token once at the WS handshake (via `auth-context`) and authorizes subscriptions only by channel-scope ownership. It performs no content-level authorization — events are authorized upstream at emit time.

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `realtime`-specific variables *(filled per phase)*

| Variable | Required | Default | Description |
|---|---|---|---|
| `REALTIME_GATEWAY_WS_ADDR` | No | `0.0.0.0:8443` | public WSS client listen address (gateway) |
| `REALTIME_GATEWAY_GRPC_ADDR` | No | `0.0.0.0:50066` | gateway internal gRPC health/control address |
| `REALTIME_DISPATCHER_GRPC_ADDR` | No | `0.0.0.0:50067` | dispatcher health/reflection address (no domain RPC) |
| `REALTIME_HEARTBEAT_INTERVAL_MS` | No | `<TODO 30000>` | app-level ping cadence (keeps NAT binding, reaps half-open) |
| `REALTIME_HEARTBEAT_TIMEOUT_MS` | No | `<TODO 90000>` | pong deadline before a connection is reaped |
| `REALTIME_SEND_QUEUE_CAP` | No | `<TODO>` | per-connection outbound queue depth before shedding |
| `REALTIME_MAX_SUBSCRIPTIONS` | No | `<TODO>` | per-connection channel-subscription cap |
| `REALTIME_MAX_FRAME_BYTES` | No | `<TODO>` | inbound frame size cap |
| `REALTIME_NODE_ID` | No | `<hostname>` | this gateway node's identity for the registry + node-hop channel |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `REDIS_URL` | **Yes** | — | connection/presence registry + node-hop Pub/Sub |
| `KAFKA_BROKERS` | **Yes** (dispatcher) | — | upstream event ingestion |
| `<auth-context verification config>` | **Yes** | — | edge-token (ES256) verification at handshake |
| `EDGE_REVOCATION_REDIS_HOSTS` | No (recommended) | — | `auth`'s Redis (see `service-runtime`, #903): a token `auth` revoked is refused at the handshake (`RTM-1001`), and a live socket is re-checked on every heartbeat and closed (`ReauthRequired`, `RTM-1001`) once its token is revoked — sign-out, suspension, revoked role. Fail open (TIER-1): an unreadable state keeps the socket. Unset ⇒ a revoked token keeps its socket |

### Compile-time features
- `integration-realtime` *(Phase 6)* — gates the live, container-backed integration suite (real Redis + Kafka).

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

- **Two deployables, scaled independently.** `realtime-gateway` scales with concurrent connection count + memory; `realtime-dispatcher` scales with event throughput. Released together (same image/tag), rolled and autoscaled separately.
- **No schema migrations.** The plane owns no durable store — only ephemeral Redis routing state (TTL'd, self-healing).
- **Graceful drain is a release requirement.** On rollout the gateway must stop-accept, emit a reconnect control frame with client-side jittered backoff, then drain — without jitter, every deploy triggers a reconnect thundering-herd into the auth handshake path.
- **L4 load balancing**, not L7: terminating WS at an L7 proxy mirrors millions of connections and hits the ephemeral-port wall.
- **Rollback:** safe — both binaries are stateless over Redis/Kafka; the dispatcher resumes from last committed offsets, the gateway re-accepts connections (clients reconnect with backoff).

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

- **Runtime:** multi-threaded Tokio, async I/O over epoll/kqueue — an idle connection is a parked future, not a thread. `realtime-gateway` runs the accept loop + per-connection mailboxes + the heartbeat reaper; `realtime-dispatcher` runs the fan-out consumers. Global tracing/OTel subscriber installed before serve; W3C trace-context propagated across the Kafka boundary.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| Open connections / node | capacity + autoscale signal | approaching node ceiling ⇒ scale |
| Per-connection memory (RSS / conns) | the C10M cost ceiling | `> SLO` ⇒ investigate leak / shedding |
| Event→device latency p99 | live responsiveness | `> SLO` ⇒ investigate registry / node-hop |
| Send-queue shed rate (`RTM-5001`) | slow-consumer pressure | sustained spike ⇒ investigate clients / network |
| Heartbeat reap rate (`RTM-5002`) | half-open churn | abnormal spike ⇒ investigate network / LB |
| Reconnect rate | herd / churn on deploy | spike off-deploy ⇒ investigate node loss |
| DLQ produce rate (`*.dlq`) | poison / retry-exhausted ingestion | any sustained rate ⇒ page |

---

## 🛠️ Local Development &nbsp;·&nbsp; CORE

```bash
cargo build -p realtime && cargo clippy -p realtime --all-targets
cargo test  -p realtime                                    # fast, infra-free unit run
docker compose up -d redis kafka                           # repo-root compose (Phase 6)
cargo test  -p realtime --features integration-realtime    # live suite (Phase 6)
```

---

## 🚨 Troubleshooting & Runbook &nbsp;·&nbsp; CORE

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. Messages arrive only on app reopen, not live.**
Root cause: Redis registry/pub-sub degraded, or dispatcher lag — live fan-out stalled. Mitigation: check Redis health and consumer-group lag; the message is durable in `chat`/`notification`, so clients catch up on reconnect — no loss; live delivery recovers when the fabric does.

**2. Node memory climbing toward OOM while CPU is near idle.**
Root cause: connection accumulation or slow-consumer buffering. Mitigation: confirm the per-connection send-queue cap and shedding are active; check the shed-rate metric; autoscale on connections + memory, not CPU — a full idle node is ~0% CPU.

**3. Every deploy triggers an auth/handshake spike.**
Root cause: reconnect thundering-herd — clients reconnecting without jitter on drain. Mitigation: confirm jittered backoff in the drain control frame and client SDK; stagger node rollouts.

**4. Some users never receive live events; FDs/registry slots leak.**
Root cause: half-open connections the kernel still holds. Mitigation: confirm the heartbeat reaper interval/timeout; check the reap-rate metric; reaping frees the FD + registry slot and flips presence offline.

**5. A client can see another user's stream.**
Root cause (critical): a channel-scope authorization gap. Mitigation: this must be impossible — subscriptions are authorized against the pinned identity (`RTM-3001`); treat any occurrence as a security incident and audit the subscribe path.

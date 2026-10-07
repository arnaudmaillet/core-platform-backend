# `media` — Move pixels and frames at hyperscale **without ever putting a byte on the mesh**

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | `<team>` · `<#slack-channel>` |
> | **On-call / escalation** | `<oncall-rotation>` → `<escalation-policy>` |
> | **Tier** | TIER-1 (mixed posture: fail-**open** delivery plane · fail-**closed** compliance gate) |
> | **Deployable** | `crates/apps/media-server` (library crate: `crates/services/media`) |
> | **Datastores** | Object storage (S3 / MinIO — canonical bytes) · Postgres db `media` (metadata SoR) · Redis Cluster (delivery cache + ticket reservations) |
> | **Async** | publishes `media.v1.events` · consumes object-store finalize notifications, `moderation.v1.events`, `account.v1.events`, `post.v1.events`, `profile.v1.events` (Kafka) |
> | **Upstream callers** | gateway/BFF, `post`, `profile` |
> | **Downstream deps** | Object storage, CDN, `moderation` (Screen), Postgres, Redis, Kafka |
> | **SLO** | `<99.9%>` control-plane avail · `<p99 ResolveDelivery < N ms>` · processing lag tracked, not SLA'd |

---

## 🎯 Overview & Service Role

`media` is the **media control plane** for the platform: it owns media *assets and
their processing lifecycle* — the asset state machine, the derivative/rendition
catalog, the storage-key scheme, the CDN delivery policy, and the compliance holds
that gate both delivery and deletion.

The hard problem it solves is **moving binary at hyperscale without melting the
synchronous mesh** — a naive "POST your video to the API" design routes terabytes
through gRPC and Kafka, couples every upload to a transcode, and blocks `CreatePost`
on processing. The resolving pattern is a strict **control-plane / data-plane
split**: bytes flow client ⇄ object storage ⇄ CDN on a pre-signed, direct path the
service *authorizes* but never *carries*; the mesh moves only tickets, metadata,
and URLs.

**Core objectives:** (1) **no bytes on the mesh** — ever; (2) the publish path
never waits on processing (upload-first, reference-not-bytes, resolve
progressively); (3) CSAM-class media is gated **fail-closed** before it can go
public, and a legal hold overrides GDPR erasure.

| Plane | Interface | Sync? | Posture |
|---|---|---|---|
| **A — Upload brokerage** | `IssueUploadTicket` → pre-signed PUT (bytes go direct to object store) | gRPC, narrow | fail-closed on policy |
| **B — Transformation** | Kafka: finalize → validate → screen → derive → publish | async | lag is an SLO |
| **C — Delivery resolution** | `ResolveDelivery` / `BatchResolveDelivery` → CDN / signed URLs | gRPC, hot read | fail-**open** (placeholder) |

---

## 📐 Architecture & Concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS command/query
buses, **object storage** as the canonical byte store, **Postgres** as the asset
metadata SoR, **Redis** for the hot delivery cache + ticket reservations, Kafka for
the async pipeline and lifecycle events.

```
            ┌─────────── control plane (gRPC, no bytes) ───────────┐
 client ───▶│ IssueUploadTicket → asset(Pending) + pre-signed PUT  │
            └──────────────────────────┬──────────────────────────┘
                                        │  (bytes NEVER traverse here)
   bytes ══ PUT ═════════════▶ [ Object Storage ] ◀═══ origin pull ═══ [ CDN ]
                                        │                                  ▲
              finalize (S3 event→Kafka  │  or CommitUpload RPC)            │ signed/immutable URL
                                        ▼                                  │
   Plane B:  validate → Screen(moderation, fail-closed) → derive ──▶ renditions
                                        │  emit media.v1.events (AssetReady …)
                                        ▼
   Plane C:  ResolveDelivery(asset_id) ─────────────────────────────▶ CDN URL
```

**Content-addressed immutability (the key mechanism).** Public derivative keys are
`/{kind}/{content_hash}/{rendition}.{ext}` with `Cache-Control: immutable`. An edit
is a *new asset* = a *new hash* = a *new URL*, so **cache invalidation is a
takedown-only operation** — never an edit-path concern. Private media uses
short-lived signed URLs minted per-request *after* the edge authorizes the viewer.

**Takedowns reach the origin.** Because a public URL never expires, a quarantine
cannot rely on `ResolveDelivery` alone (a URL handed out before keeps working). It is
saved first (no new URL), then **every object of the asset's trees** (`{kind}/{hash}/…`:
renditions, a video's whole HLS output) and its original upload **move under
`quarantine/`** — a prefix the CDN's origin access is not granted — and the CDN is
purged last. A restore moves them back *before* reinstating the asset. Each step is
idempotent, so a redelivered event redoes only what is left. Identical bytes share
keys, so **a takedown is about the content**: every asset holding the same bytes is
quarantined with it (each saved and announced), and a reversal restores them all —
unless one of them is under a **legal hold** (CSAM evidence), in which case nothing is
restored. Each asset records the moderation **enforcements** covering it; a reversal
lifts only its own, and the bytes come back only once no copy is still covered by
another enforcement (two decisions on two copies: overturning one keeps the content
down). A **re-upload of taken-down bytes is quarantined on arrival** (enforcements and
legal hold carried over), before any rendition is written back to the shared public
keys. A delete erases public and quarantined copies alike, except objects another
asset with the same bytes still holds (the legal hold still blocks a delete first).

> **Invariants** (and where enforced): no message carries a `bytes` payload
> (proto review + `media-api` contract rule); asset state transitions are guarded
> in the domain; optimistic lock on the asset row (`ConcurrentModification`);
> object storage is canonical for bytes, Postgres for truth-about-bytes; a legal
> hold blocks hard-delete even on owner GDPR erasure.

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Control-plane availability (non-5xx / non-`UNAVAILABLE`) | `<99.9%>` | 30d rolling | `<metric>` |
| `ResolveDelivery` p99 (Plane C, hot read) | `< <N> ms` | 1h | `<metric>` |
| `IssueUploadTicket` p99 (Plane A) | `< <N> ms` | 1h | `<metric>` |
| Processing lag (upload → `AssetReady`) | `< <N> s` p99 | live | `<consumer-group> lag` (SLO, not SLA — publish never waits) |
| Durability | no acked asset metadata lost | — | Postgres commit; object-store 11-nines |

**Error budget:** `<0.1% / 30d ≈ 43m>`. **On burn:** `<freeze rollout | page>`.

---

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

**Downstream — what `media` needs to function:**

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Object storage | canonical bytes; pre-sign target | uploads + delivery-origin fail | **Hard** for upload; delivery rides CDN cache |
| Postgres | asset metadata SoR | metadata writes/reads fail | **Hard** — `UNAVAILABLE` on control plane |
| Redis | delivery cache, ticket reservations | cache-miss path | **Soft** — resolves from Postgres |
| CDN | edge delivery | cold edge / origin pressure | **Soft** — origin pull, slower |
| `moderation` (Screen) | pre-publish CSAM gate | gate unavailable | **Fail-closed** for CSAM-class (asset held, not published) |
| Kafka | finalize + lifecycle events | pipeline stalls | **Soft** — uploads queue, processing lags |

**Upstream — who depends on `media` (your blast radius if YOU fail):**

| Caller | Uses | User-visible impact if `media` is down |
|---|---|---|
| gateway/BFF | `ResolveDelivery` | media renders as placeholder/blurhash; feed still loads |
| `post` / `profile` | asset reference + `media.v1.events` | posts publish (reference Pending); media resolves late |

> **Critical path?** **No for core writes** — `post`/`profile` reference an
> `asset_id` and never block on media. **Yes for media render** — delivery
> resolution is on the read path, but fails open to a placeholder.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### gRPC — `media.v1.MediaService` *(Phase 1 — control plane, zero byte fields)*

```protobuf
service MediaService {
  // Plane A — upload brokerage (returns a pre-signed URL; bytes go direct to store)
  rpc IssueUploadTicket (IssueUploadTicketRequest) returns (IssueUploadTicketResponse);
  rpc CommitUpload      (CommitUploadRequest)      returns (CommitUploadResponse);
  rpc AbortUpload       (AbortUploadRequest)       returns (AbortUploadResponse);
  // Asset metadata
  rpc GetAsset          (GetAssetRequest)          returns (GetAssetResponse);
  rpc DeleteAsset       (DeleteAssetRequest)       returns (DeleteAssetResponse);
  rpc ListAssetsByOwner (ListAssetsByOwnerRequest) returns (ListAssetsByOwnerResponse); // mesh only
  // Plane C — delivery resolution (hot read; CDN / signed URLs)
  rpc ResolveDelivery      (ResolveDeliveryRequest)      returns (ResolveDeliveryResponse);
  rpc BatchResolveDelivery (BatchResolveDeliveryRequest) returns (BatchResolveDeliveryResponse);
  // Ops
  rpc Reprocess         (ReprocessRequest)         returns (ReprocessResponse);
}
```

**An account's assets (#653).** `ListAssetsByOwner(owner_id, limit, page_token, url_ttl_secs)` is
**mesh only** (never on the edge): the GDPR data export lists every asset an account owns (not deleted),
by id (index `(owner_id, id)`, migration 0002), each with a signed download of its **original**, valid
`url_ttl_secs` (60 s – 7 days, S3's cap; signed against the public endpoint). An asset that is not
deliverable (processing, failed, quarantined) comes without a URL — a quarantined file is never handed
out.

> **Wire / contract rule:** **no `media.v1` message carries a `bytes` payload.**
> Uploads are brokered via pre-signed object-store URLs; delivery is brokered via
> CDN / signed URLs. Enums are fully prefixed with an `_UNSPECIFIED = 0` sentinel.

**Boundary invariants:** `CommitUpload` on a missing object → `FAILED_PRECONDITION`
(`MED-1005`); resolve of a quarantined asset → `PERMISSION_DENIED` (`MED-7001`,
451); delete under legal hold → `PERMISSION_DENIED` (`MED-7003`).

### Error contract

Every fault implements `error::AppError` with a stable `MED-XXXX` code, mapped to
gRPC `Status` / HTTP by the shared `error` crate:

| Range | Class |
|---|---|
| `MED-1xxx` | upload brokerage / ticket (Plane A) |
| `MED-2xxx` | asset metadata SoR (+ concurrency) |
| `MED-3xxx` | rendition / transformation pipeline (Plane B) |
| `MED-4xxx` | object-store adapter — the byte plane (retryable infra) |
| `MED-5xxx` | CDN / delivery / signing (Plane C) |
| `MED-6xxx` | content validation / probe (magic-byte, decode-bomb, malware) |
| `MED-7xxx` | compliance / Screen (Quarantined / LegalHold = 451; ScreenUnavailable = 503 fail-closed) |
| `MED-8xxx` | inbound event decode / source mapping |
| `MED-9xxx` | cross-cutting (domain/parse, event I/O) |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

> Kafka topics are an API. A schema change here breaks consumers exactly like a proto change.

**Publishes** (`media.v1.events`, serde structs, keyed by `asset_id`):

| Topic | Trigger | Key | Consumers |
|---|---|---|---|
| `media.v1.events` → `AssetUploaded` | finalize accepted | `asset_id` | `moderation` (screen), internal pipeline |
| `media.v1.events` → `AssetVariantReady` | a rendition completes | `asset_id` | BFF (progressive render) |
| `media.v1.events` → `AssetReady` | all renditions done | `asset_id` | `post`, `profile`, `search`, `timeline` |
| `media.v1.events` → `AssetFailed` / `AssetQuarantined` / `AssetDeleted` | terminal states | `asset_id` | callers, GC |

**Consumes:**

| Topic | Consumer group | Purpose | On poison/exhaustion |
|---|---|---|---|
| object-store finalize (bridged) | `media-finalize-consumer` | upload finalize (source of truth over `CommitUpload`) | DLQ |
| `moderation.v1.events` | `media-moderation-consumer` | quarantine / restore (revoke / re-enable delivery) | DLQ |
| `account.v1.events` | `media-account-consumer` | `account_deleted` → every asset of the account is deleted like an owner's delete (bytes, renditions, quarantined copies, staging, tombstone, `AssetDeleted`, cache, CDN); legal-held assets are kept and logged (GDPR Art. 17, #777; idempotent) | DLQ |
| `post.v1.events` / `profile.v1.events` | `media-binding-consumer` | mark assets bound; orphan GC of abandoned uploads | DLQ |

> **Runtime contract (mandatory):** all consumers run under `run_consumer` —
> manual commit after a terminal outcome, bounded retry with backoff + jitter, DLQ
> on exhaustion/poison. Idempotency: deterministic `asset_id` + content-addressed
> rendition keys (a re-derived rendition writes the same key).

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Failure | Symptom | Service behavior | Operator action |
|---|---|---|---|
| Object storage down | uploads + origin fail | upload hard-fails; delivery rides CDN cache | check store / IAM; uploads retry |
| Postgres down | control plane errors | `UNAVAILABLE` on metadata ops | failover / restore |
| Redis cold/evicted | resolve latency rise | rebuild from Postgres, safe | usually none |
| CDN cold / origin pressure | first-byte latency | origin pull from store | warm / check edge config |
| `moderation` Screen unavailable | CSAM-class held | **fail-closed**: asset not published | check moderation; hard timeout bounds wait |
| Transcode worker backlog | `AssetReady` lag | publish unaffected; placeholder shown | scale workers; check DLQ |

**Backpressure & limits:** per-kind size ceilings + MIME allowlist enforced in the
signed upload policy; decode-bomb / dimension caps at validate; Screen hard timeout
(Phase 7) so a moderation outage can't wedge uploads; processing isolated to a
separate worker role so transcode CPU can't degrade Plane A/C latency.

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
media = { path = "crates/services/media" }
```

Library-only. Implements [`service_runtime::Service`](../../platform/service-runtime/README.md)
as `media::service::MediaService` (Phase 5) — `build` wires the object-store /
Postgres / Redis adapters and spawns the finalize, moderation, and binding
consumers; `register` adds the gRPC services; `health_probes` exposes liveness.
Telemetry, config + hot-reload, ingress rate-limiting, health, and graceful
shutdown are owned by the runtime.

### Bootstrap (`crates/apps/media-server`)

```rust
use media::service::MediaService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("MEDIA_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50063".to_owned()).parse()?;
    service_runtime::serve::<MediaService>(addr).await
}
```

> **Build status:** complete through Phase 7 — error namespace, `media.v1` proto
> (8 control-plane RPCs, no byte fields), domain + application, the S3/Postgres/
> Redis/CDN/image adapters, the server + worker consumers, and a live MinIO +
> Postgres + Redis integration suite. All control-plane RPCs are byte-free.
>
> **Authorization (deployment requirement):** `media` self-authorizes nothing. The
> edge/gateway (via `auth-context`) authenticates the caller and supplies the
> `owner_id` on `IssueUploadTicket` / `DeleteAsset` / `AbortUpload`; ownership is
> defense-checked in-handler (a non-owner delete returns `NOT_FOUND`, leaking
> nothing). Viewer authorization for private media happens at the edge **before**
> `ResolveDelivery` mints a signed URL. Expose mutating RPCs only behind that gate.

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `media`-specific variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `MEDIA_GRPC_ADDR` | No | `0.0.0.0:50063` | gRPC control-plane bind address |
| `MEDIA_OBJECT_STORE_ENDPOINT` | No | `http://localhost:9000` | S3/MinIO endpoint URL |
| `MEDIA_OBJECT_STORE_REGION` | No | `us-east-1` | S3 region |
| `MEDIA_OBJECT_STORE_BUCKET` | No | `media` | canonical bytes bucket |
| `MEDIA_S3_ACCESS_KEY` / `MEDIA_S3_SECRET_KEY` | **Yes (prod)** | `minioadmin` | object-store credentials |
| `MEDIA_PRESIGN_TTL_SECS` | No | `900` | server-side signed-URL validity |
| `MEDIA_OBJECT_STORE_TIMEOUT_MS` | No | `10000` | hard timeout on every object-store HTTP call |
| `MEDIA_CDN_BASE_URL` | No | `…:9000/media` | public, content-addressed delivery origin |
| `MEDIA_CLOUDFRONT_DISTRIBUTION_ID` | No | — | takedowns (quarantine / delete) purge the asset's renditions from this CloudFront distribution (`CreateInvalidation`, SigV4 with the `MEDIA_S3_*` keys, whose IAM user needs `cloudfront:CreateInvalidation`); a failed purge fails the takedown, which is retried. Unset: takedowns only log (local runs) |
| `MEDIA_UPLOAD_TICKET_TTL_SECS` | No | `900` | pre-signed upload validity window |
| `MEDIA_SIGNED_URL_TTL_SECS` | No | `300` | private (signed) delivery URL validity |
| `MEDIA_DEDUP_ENABLED` | No | `false` | content-hash dedup (off until refcount-purge is hardened) |
| `MEDIA_SCREEN_GRPC_ENDPOINT` | No | `http://localhost:50061` | moderation Screen gate endpoint |
| `MEDIA_SCREEN_TIMEOUT_MS` | No | `200` | fail-closed Screen hard timeout |

### Inherited infrastructure variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_* / REDIS_HOSTS / KAFKA_BROKERS` | **Yes** | — | metadata SoR, hot cache, async pipeline |

> Full connection/timeout/reconnect tuning lives in the relevant shared storage/transport crates.

### Compile-time features
- `integration-media` — gates the live MinIO + Postgres + Redis suite (Phase 6).
- `build.rs` (Phase 1, in `media-api`) compiles `contracts/proto/media/v1/*` and emits the reflection descriptor set.

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

- **Migrations:** `crates/services/media/migrations/*.sql` against Postgres db
  `media` (Phase 4). Apply **before** rolling the new binary. Expand-then-contract.
- **Object-store / CDN:** bucket lifecycle (hot derivatives → IA/Glacier cold
  masters) and CDN cache policy are declared infra, not app cron. Object-lock /
  WORM bucket for legal holds.
- **Rollout:** rolling; risky pipeline changes gated by config + `Reprocess`.
- **Rollback:** binary is safe to roll back (metadata schema forward-compatible
  with N-1). **Stateful gotcha:** the content-addressed key scheme and the
  rendition-ladder slugs must **never** change meaning after data exists — version
  them, don't mutate.

**Security review (Phase 7, manual pass): clean.** No bytes ever cross gRPC/Kafka
(verified — generated `media.v1` has zero `bytes` fields); no raw bytes or PII in
logs (only operational fields: `event_type`, `asset_id`, object keys); no
`unwrap`/`expect`/`panic` on request or pipeline paths; the content probe never
trusts the client's declared type; signed URLs are short-lived; a legal hold blocks
hard-delete (CSAM/NCMEC preservation overrides GDPR erasure). One fleet-consistent,
gateway-enforced finding (not a code fix): no per-RPC caller authorization — see the
Authorization note above. Object-store calls are bounded by a hard timeout
(`MEDIA_OBJECT_STORE_TIMEOUT_MS`) and the Screen gate by `MEDIA_SCREEN_TIMEOUT_MS`,
so neither a stuck store nor a stuck moderation gate can wedge a worker.

**Deferred (documented, not built):** video transcoding (images-first v1 — a
`Transcoder` sibling port + ABR ladder is the fast-follow); a real malware-scan
sidecar (the `MalwareScanner` port ships a pass-through stub); the orphan-GC
consumer (unbound-after-TTL reaping of abandoned uploads); WebP/AVIF rendition
encoding (v1 emits JPEG); and
the dedup refcount-aware GDPR purge (dedup ships behind a default-off flag until
that path has live coverage; an account's erasure already keeps an object another
asset still uses).

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

- **Runtime:** multi-threaded Tokio. API server (light, Plane A/C) is deployed as a
  **separate role** from the processing workers (heavy, Plane B) so transcode load
  can't degrade control-plane latency. W3C trace-context propagated across Kafka.

| Signal | Why it matters | Suggested alert |
|---|---|---|
| `ResolveDelivery` p99 | hot read on the render path | `> SLO ⇒ investigate` |
| processing lag (upload→Ready) | progressive-render UX | `sustained > N s ⇒ scale workers` |
| Screen fail-closed rate | CSAM gate health | `spike ⇒ page` |
| DLQ produce rate | poison / retry-exhausted | any sustained rate ⇒ page |

---

## 🛠️ Local Development &nbsp;·&nbsp; CORE

```bash
cargo build -p media && cargo clippy -p media --all-targets
cargo test  -p media                                   # fast, infra-free unit run
cargo test  -p media --features integration-media      # live MinIO + Postgres + Redis (Phase 6)
```

---

## 🚨 Troubleshooting & Runbook &nbsp;·&nbsp; CORE

> Format: **symptom → root cause → mitigation.** One entry per real incident class.

**1. `CommitUpload` returns `FAILED_PRECONDITION` (`MED-1005`).**
Root cause: the client called commit before the bytes finished landing in the
object store (or the PUT failed). Mitigation: the client retries the direct PUT;
the S3 finalize event is the authoritative trigger and will converge regardless.

**2. Media renders as a placeholder forever.**
Root cause: the asset is stuck in `Processing` — transcode worker backlog or a
poison message in the pipeline. Mitigation: check `media-finalize-consumer` lag and
the DLQ; scale workers; `Reprocess` the asset if a rendition was dropped.

**3. Delete returns 451 (`MED-7003`).**
Root cause: a legal hold is active (e.g. CSAM evidence preservation) — hard-delete
is intentionally blocked, overriding GDPR erasure. Mitigation: this is correct
behavior; escalate to trust & safety / legal, do not force-delete.

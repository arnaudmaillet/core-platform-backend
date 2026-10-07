# `media` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Media — asset lifecycle control plane + delivery brokerage |
> | **Subdomain class** | **Supporting** — necessary media handling; bespoke pipeline, but not the product's value origin |
> | **System of …** | **Record** for asset *metadata/lifecycle* (the bytes live in object storage, not here) |
> | **Aggregate root(s)** | `Asset` (`domain`) |
> | **Tier** | **TIER-1** |
> | **Failure posture** | **Mixed** — *fail-open* delivery resolution; *fail-closed* CSAM `Screen` before an asset goes ready |
> | **Upstream contexts** | clients (upload tickets); `moderation` (Screen + takedown) |
> | **Downstream contexts** | `post`, `profile`, `search` — via **Published Language** (`media.v1.events`) |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `media` is the authority for **the media asset lifecycle**: it answers
**"is this asset uploaded, screened, transformed, and safe to deliver — and where from?"**

**The hard problem.** Handling large binaries without ever putting bytes on gRPC/Kafka: a
three-plane design — (A) an upload broker issuing **pre-signed direct-to-object-store** tickets, (B)
an async Kafka transform pipeline, (C) a delivery-resolution/CDN brokerage — with a **fail-closed
CSAM Screen** gating readiness.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Move bytes through gRPC/Kafka → uploads go direct to object storage via pre-signed URLs.
- ❌ Decide moderation policy → `moderation` decides; media enforces Screen/takedown.
- ❌ Own where the asset is *used* → `post` / `profile` reference it.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Asset | A media asset and its lifecycle | `Asset`, `AssetId`, `AssetState` |
| Upload ticket | A pre-signed direct-to-store upload grant | `UploadTicket`, `ReserveParams`, `UploadConstraints` |
| Rendition | A derived variant (size/format) of an asset | `Rendition`, `RenditionKind` |
| Content hash | The dedup/identity hash of the bytes | `ContentHash` |
| Storage key | The object-store location | `StorageKey` |
| Blurhash / dimensions | Perceptual placeholder + size metadata | `Blurhash`, `Dimensions` |
| Delivery visibility | Whether/how an asset may be served | `DeliveryVisibility` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Asset` | aggregate root | The asset lifecycle state machine |
| `UploadTicket` / `ReserveParams` / `UploadConstraints` | VO | A bounded, pre-signed upload grant |
| `Rendition` / `RenditionKind` | VO/enum | Derived variants |
| `ContentHash` / `StorageKey` / `MimeType` / `Dimensions` / `Blurhash` | VO | Byte identity + placement + metadata |
| `MediaKind` / `AssetState` / `DeliveryVisibility` | enum | Closed kind/state/delivery vocabularies |

**Asset lifecycle:**

```
reserved --(upload+finalize)--> uploaded --(Screen: fail-closed)--> ready --(variant)--> variant_ready
   │                                  │                                │
   │                                  └--(Screen fail)--> quarantined  └--(takedown)--> deleted --(restore)--> ready
   └--(timeout)--> failed
```

> **Legal transitions only.** An asset cannot reach `ready` without passing the CSAM `Screen`; a
> takedown quarantines/deletes; restore is explicit.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Asset metadata/lifecycle — **Postgres** (asset doc, JSONB) + **Redis** (cache). The **bytes** live in **S3/MinIO** (object store), referenced by `StorageKey` — media owns the control plane, not a bytes-in-database copy.

**The "do-not-write" list:** media never decides moderation policy and never owns where an asset is
embedded (`post`/`profile` reference it).

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Bytes never traverse gRPC/Kafka — only pre-signed direct-to-store | infrastructure | `MED-1xxx` |
| I2 | An asset reaches `ready` only after a passing CSAM `Screen` (fail-closed) | application | `MED-7xxx` |
| I3 | A moderation takedown quarantines/deletes the asset | application (consumer) | `MED-6xxx` |
| I4 | Delivery resolution fails open (degrade, never block) | application | `MED-5xxx` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Plane A — upload broker.** Client requests an `UploadTicket` → media reserves an `Asset` + issues
a pre-signed URL → client uploads **direct to object storage** → finalize.

**Plane B — transform (async).** On `media.v1.events` (uploaded) the processor probes the image,
runs the **fail-closed CSAM Screen** (gRPC to `moderation`, hard timeout), generates renditions +
blurhash, and transitions the asset to `ready` (or `quarantined`).

**Plane C — delivery (fail-open).** Resolve an asset → CDN URL (CloudFront brokerage). A
`moderation` takedown consumer quarantines/deletes on demand.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| clients | upstream | OHS | pre-signed upload tickets | uploads break |
| `moderation` | upstream | Customer/Supplier (sync) + ACL | `Screen` gRPC + takedown consumer | readiness gating / takedowns break |
| `post` / `profile` / `search` | downstream | Published Language | `media.v1.events` | embeds/indexing break |

> **Anti-Corruption Layer:** the object-store + CDN adapters isolate the byte plane; the moderation
> takedown decode maps foreign events to asset transitions.

---

## 8. Domain Events (semantics, not wire)

| Event (`media.v1.events`) | Means | Emitted when | Who reacts |
|---|---|---|---|
| `asset_uploaded` | bytes landed in the store | finalize | the transform pipeline (Plane B) |
| `asset_ready` / `asset_variant_ready` | the asset (or a variant) is safe to deliver | Screen passes / rendition done | `post`, `profile`, `search` |
| `asset_quarantined` / `asset_deleted` / `asset_restored` | safety/lifecycle transitions | Screen fail / takedown / restore | embeds, delivery |
| `asset_failed` | processing failed | timeout/error | upload UX |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Byte-free control plane — pre-signed direct-to-store uploads, bytes never on gRPC/Kafka | [`ADR-0011`](../../../../docs/adr/0011-media-byte-free-control-plane.md) | Accepted |
| Fail-open delivery / fail-closed CSAM Screen split | [`ADR-0011`](../../../../docs/adr/0011-media-byte-free-control-plane.md) | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Supporting — necessary media plumbing; bespoke pipeline.
- **Volatility:** medium — new media kinds/renditions are additive.
- **Known modeling debt:** images-first; video transcoding deferred.
- **Deferred capabilities:** real malware sidecar, orphan-GC consumer, WebP/AVIF, dedup-refcount GDPR purge. An account's media are erased with the account (`account_deleted`, #777; legal holds kept).

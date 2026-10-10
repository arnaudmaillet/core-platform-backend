# `post` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Posts — the content lifecycle |
> | **Subdomain class** | **Core** — published content is the platform's primary substance |
> | **System of …** | **Record** for posts and their lifecycle |
> | **Aggregate root(s)** | `Post` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Failure posture** | **Fail-closed on writes** — a published post must persist |
> | **Upstream contexts** | clients (author); `profile` (author identity); `moderation` (gating); `media` (attachments) |
> | **Downstream contexts** | `timeline`, `geo-discovery`, `search`, `counter`, `realtime` — via **Published Language** (`post.v1.events`) |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `post` is the authority for **content**: it answers
**"what was published by whom, in what state, with what media — and is it still live?"**

**The hard problem.** Owning a high-write content store cheaply and serving it by author, while
being the fan-out source the whole read side depends on — a two-table ScyllaDB layout
(`post.posts` by id + `post.posts_by_profile` by author) with `post.v1.events` as the published
language.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Build feeds/timelines → `timeline` consumes `post.v1.events`.
- ❌ Index for search/discovery → `search` / `geo-discovery` consume events.
- ❌ Own media bytes → references `media` attachments; counts → `counter`.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Post | A unit of published content | `Post`, `PostId`, `PostKind`, `PostStatus` |
| Caption | The post's text | `Caption` |
| Location | Optional client-supplied post coordinates (WGS-84) | `GeoPoint` |
| Media attachment | A reference to a `media` asset + its CDN URL | `MediaAttachment`, `CdnUrl` |
| Audio reference | Attached audio track | `AudioReference`, `AudioId`, `AudioKind` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Post` | aggregate root | The content lifecycle state machine |
| `Caption` / `MediaAttachment` / `AudioReference` / `GeoPoint` | VO | Content validity + media/audio refs + valid lat/lng |
| `PostKind` / `PostStatus` | enum | Closed kind/status vocabularies (proto maps kind/status +1) |

**Lifecycle:**

```
published --(update)--> published' --(delete)--> deleted   |   (moderation gate may block/remove)
```

> **Legal transitions only.** Proto enums map domain tinyint +1 (no UNSPECIFIED sentinel); a delete
> emits `post.deleted` for downstream teardown.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Posts — **ScyllaDB** two-table (`post.posts` by id, `post.posts_by_profile` by author). No other service writes them.

**This context holds copies it does NOT own:**

| Copied data | Owned by | Kept fresh via | Staleness tolerance |
|---|---|---|---|
| Author profile snapshot fields | `profile` | `profile.v1.events` (DLQ `profile.v1.events.dlq`) | eventually consistent |
| Moderation state | `moderation` | `moderation.v1.events` | eventually consistent |

**The "do-not-write" list:** post never builds feeds, indexes, or counts — it emits the events those derive from.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | A post references a valid author | domain | `PST-1xxx` |
| I2 | Both tables stay consistent (id + by-profile) | domain/application | `PST-1xxx` |
| I3 | A lifecycle change emits the matching `post.v1.events` | domain (after-save) | — |
| I4 | Proto kind/status mapping is +1 with no UNSPECIFIED | infrastructure (codec) | — |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Publish / update / delete.** Authorized command → write both Scylla tables → publish
`PostPublished` / `PostUpdated` / `PostDeleted` on `post.v1.events` (and `post.deleted`, the one legacy
topic left, #487). Downstream `timeline` fans out, `search`/`geo-discovery` index, `notification`
notifies mentions, `realtime` broadcasts.

**Denormalization.** Consume `profile.v1.events` to keep author snapshot fields fresh; consume
`moderation.v1.events` to reflect enforcement.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| `profile` | upstream | ACL | `profile.v1.events` | author snapshots go stale |
| `moderation` | upstream | ACL | `moderation.v1.events` | enforcement reflection breaks |
| `media` | upstream | Customer/Supplier | `MediaAttachment` references | broken media in posts |
| `timeline`/`search`/`geo-discovery`/`counter`/`realtime` | downstream | Published Language (OHS) | `post.v1.events` | the entire read/discovery side breaks |

> **Anti-Corruption Layer:** the `profile`/`moderation` event consumers translate foreign wire
> shapes into post's denormalized fields.

---

## 8. Domain Events (semantics, not wire)

| Event (`post.v1.events`) | Means | Emitted when | Who reacts |
|---|---|---|---|
| `PostPublished` | new content went live (carries `caption`, `thumbnail_url`, optional `lat`/`lng` for `geo`) | publish commits | `timeline` (fan-out), `search`/`geo` (index), `notification` (mentions), `realtime` |
| `PostUpdated` | content was edited | update commits | `search` (re-index) |
| `post.deleted` | content was removed | delete commits | `timeline`/`search`/`geo` (teardown) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Two-table ScyllaDB layout (by id + by author) with `post.v1.events` as published language | [`ADR-0013`](../../../../docs/adr/0013-post-two-table-scylla-with-published-language.md) | Accepted |
| Post→geo payload enrichment: `PostPublished` carries caption + thumbnail + optional location (client-supplied at `CreatePost`); locationless posts are not geo-indexed | _resolved — see geo-discovery §6_ | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Core — content is the primary substance of the platform.
- **Volatility:** medium — post kinds and attachments evolve.
- **Known modeling debt:** location is captured only at `CreatePost` (no dedicated "set location" command yet); edits don't re-emit it.
- **Deferred capabilities:** richer media/audio; scheduled posts; edit history.

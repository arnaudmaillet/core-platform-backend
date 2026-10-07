# Event Catalog (semantic)

> Populated from each producer's Domain Card §8 (`crates/services/<svc>/docs/DOMAIN.md`). Records the
> **business meaning** of every domain event — *what does it mean that this happened, and who
> reacts?* It does **not** restate the wire/proto schema (owned by each producer's contract).

## Source of truth & maintenance

This catalog has two halves:

- **Topic wiring** (which service produces/consumes which topic) is **generated** from the
  event-topology registry (`crates/contracts/event-topology`) into the block below — it is the
  authority on *which* edges exist and cannot drift (a golden test + `tools/event-catalog/sync.sh`
  enforce it). Don't hand-edit it; change the registry and regenerate.
- **Event semantics** (what each event *means*, when it fires, who reacts and *why*) are authored by
  humans in the per-domain sections that follow.

Cross-reference each edge in [`CONTEXT_MAP.md`](./CONTEXT_MAP.md), and the per-event detail in each
producer's `DOMAIN.md §8`.

## Topic wiring (generated)

<!-- BEGIN GENERATED: topic-wiring · source crates/contracts/event-topology · do not edit by hand -->
> ⚙️ Generated from the event-topology registry (`crates/contracts/event-topology`). Do not edit by hand — change the registry and run `cargo run -p event-topology --bin gen-event-catalog` (or `tools/event-catalog/sync.sh --write`). The *meaning* of each event is authored in the semantic sections below.

### Produced topics → consumers

| Topic | Producer | Consumers |
|---|---|---|
| `account.v1.events` | `account` | `audit`, `auth`, `profile`, `media` |
| `profile.v1.events` | `profile` | `search`, `post`, `social-graph`, `geo-discovery`, `timeline`, `media`, `comment`, `chat` |
| `notification.v1.events` | `notification` | `realtime` |
| `post.published` | `post` | `notification`, `geo-discovery` |
| `post.updated` | `post` | — *(orphan — see below)* |
| `post.deleted` | `post` | `timeline`, `geo-discovery` |
| `post.v1.events` | `post` | `timeline`, `search`, `realtime` |
| `comment.created` | `comment` | `notification`, `engagement` |
| `comment.deleted` | `comment` | `engagement` |
| `engagement.reactions` | `engagement` | `counter`, `notification`, `timeline`, `engagement` |
| `social-graph.followed` | `social-graph` | `notification`, `timeline` |
| `social-graph.unfollowed` | `social-graph` | `timeline` |
| `social-graph.blocked` | `social-graph` | — *(orphan — see below)* |
| `social-graph.author_tier_changed` | `social-graph` | `profile` |
| `social-graph.follow_requested` | `social-graph` | `notification` |
| `chat.conversation.created` | `chat` | `chat` |
| `chat.conversation.published` | `chat` | — *(orphan — see below)* |
| `chat.conversation.unpublished` | `chat` | `chat` |
| `chat.member.joined` | `chat` | `chat` |
| `chat.member.left` | `chat` | `chat` |
| `chat.message.sent` | `chat` | `chat` |
| `chat.message.push` | `chat` | `notification` |
| `counter.v1.popularity` | `counter` | `realtime`, `geo-discovery`, `timeline` |
| `moderation.v1.events` | `moderation` | `audit`, `search`, `media`, `post`, `geo-discovery`, `timeline`, `notification` |
| `auth.v1.events` | `auth` | `audit` |
| `media.v1.events` | `media` | `media` |

### Deferred — consumed, producer intentionally not in-repo

| Topic | Consumer(s) | Why |
|---|---|---|
| `audit.v1.events` | `audit` | Generic privileged-record ingest lane. Domain producers emit their own topics (account/auth/moderation .v1.events) which audit consumes directly; this lane is fed by the sync gRPC RecordPrivileged path and future generic producers. |
| `moderation.reports` | `moderation` | External user-report intake — produced by the client/edge, not a fleet service. |
| `moderation.signals` | `moderation` | External ML-classifier signals — produced off-fleet. |
| `view.v1.events` | `counter` | Upstream view telemetry producer not yet built (counter-analytics blueprint deferral). |
| `impression.v1.events` | `counter` | Upstream impression telemetry producer not yet built (counter deferral). |
| `click.v1.events` | `counter` | Upstream click telemetry producer not yet built (counter deferral). |
| `social-graph.follows` | `counter` | Counter wants a single combined follow stream; social-graph emits the split past-tense social-graph.followed/.unfollowed instead. Combined producer is deferred — TRACKED NAMING MISMATCH, not just a missing emitter. |

### Orphan producers — produced, no in-repo consumer

| Topic | Producer | Why |
|---|---|---|
| `post.updated` | `post` | No stream consumer — search/timeline/realtime act on post.v1.events PostUpdated; the legacy per-type topic is emitted for completeness. |
| `social-graph.blocked` | `social-graph` | Block is enforced on the gRPC read path; no stream consumer yet. |
| `chat.conversation.published` | `chat` | Chat delivery-plane headroom. |

<!-- END GENERATED: topic-wiring -->

## Identity & Account — `account.v1.events` (producer: `account`)

| Event | Means (past-tense business fact) | Emitted when | Consumers & why |
|---|---|---|---|
| `account_created` / `email_changed` / `email_verified` / `phone_changed` | a PII-bearing account lifecycle fact occurred | the matching command commits | `audit` (PII sealed in crypto-shred envelope), `profile` (persona) |
| `password_changed` / `mfa_enrolled` / `mfa_revoked` | a security/credential fact (no PII) | credential change | `audit` (Authentication category) |
| `activated` / `deactivated` / `suspended` / `deleted` / `kyc_status_changed` | an identity-lifecycle transition (`deleted`: anonymized by the GDPR janitor, or an admin delete) | lifecycle change | `audit` (Identity; `deleted` → **crypto-shreds the subject**, closing the Art. 17 loop), `profile` |
| `role_assigned` / `role_revoked` | an authorization grant changed | role grant/revoke | `audit` (Authorization) |
| `gdpr_deletion_requested` | the right to erasure (Art. 17) was invoked; erasure scheduled 30 days out (an active account is deactivated meanwhile) | user/DPO request | `audit` (DataErasure evidence) |
| `gdpr_deletion_cancelled` | a pending erasure was withdrawn within its grace period | the holder signs back in / `CancelGdprDeletion` | `audit` (DataErasure evidence) |
| `gdpr_data_export_requested` | the right to access/portability was invoked | user/DPO request | export fulfilment (downstream) |
| `date_of_birth_set` | the holder recorded a date of birth (≥ the minimum age; the date itself stays in account) | `CreateAccount` with one / `SetDateOfBirth` | none (the age bracket travels in the edge token's `age` claim) |
| `consents_updated` | the holder gave or withdrew consents (Art. 7), effective changes only, with the policy version | `UpdateConsents` changes something | `audit` (Consent — the tamper-evident copy of `account_consent_history`) |

## Authentication — `auth.v1.events` (producer: `auth`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `session_issued` | an authenticated session was established | login / token issuance | `audit` (Authentication) |
| `session_revoked` | a session was invalidated | logout / revoke / generation bump | `audit` (Authentication) |
| `subject_linked` | an IdP subject was bound to an account | account-link flow | internal |

## Profile — `profile.v1.events` (producer: `profile`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `profile_created` / `profile_updated` | the public persona was created/edited | command commits | `post`/`search` (snapshots, indexing) |
| `handle_changed` | the @handle changed | handle claim | `search` (re-index), embeds |
| `profile_verified` | the verification badge changed | verification | `search`, embeds |
| `tier_changed` | the author tier changed | tier recompute (from `social-graph`) | `geo-discovery` (weight), `timeline` (push/pull) |
| `profile_hidden` / `profile_restored` / `profile_deleted` | a visibility/lifecycle transition | owner or moderation action | read models (teardown/restore); `social-graph` (audience projection: hidden) |
| `profile_visibility_changed` | the owner made the profile private or public | `SetVisibility` | `social-graph` (audience projection: private → content for followers only, via `CheckAccess`) |
| `profile_interaction_settings_changed` | the owner changed who may comment / mention / message (everyone, followers, mutuals, no one), downloads and like counts | `SetInteractionSettings`, or a 13–17 holder's profile creation (teen defaults) | `social-graph` (projection read by `CheckInteraction`, which comment calls before writing) |
| `profile_location_settings_changed` | the owner turned ghost mode on/off, or changed location precision (precise / city), its audience (`audience`: everyone / followers / mutuals, #657) or the new-posts preference (`on_new_posts`) | `SetLocationSettings`, or a 13–17 holder's profile creation (teen default: ghost, mutuals, no location on new posts) | `geo-discovery` (projection applied to every map surface: a ghost's posts leave others' maps; city level shows them only at the coarse band, at the cell centre) |

## Content — `post.v1.events` (producer: `post`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `post.published` | new content went live | publish commits | `timeline` (fan-out), `search`/`geo-discovery` (index), `counter`, `realtime` (broadcast) |
| `post.updated` | content was edited | update commits | `search`/`geo-discovery` (re-index) |
| `post.deleted` | content was removed | delete commits | `timeline`/`search`/`geo-discovery` (teardown) |

## Comments — `comment.created` / `comment.deleted` (producer: `comment`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `comment.created` | a comment was posted on a post | create commits | `notification` (notify author), `counter`/`engagement` (count++) |
| `comment.deleted` | a comment was tombstoned or purged | delete commits | `counter`/`engagement` (count--), feeds |

## Engagement — `engagement.*` (producer: `engagement`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `engagement.reactions` (`ReactionUpserted`/`Removed`) | a reaction edge was set/cleared | react/unreact commits | `notification`, `counter` |
| `engagement.score_updated` | the weighted engagement score changed | score recompute | `geo-discovery` (virality), `counter` |
| `engagement.post_reactions` / `engagement.post_interaction_counters` | per-post reaction/interaction rollups | aggregation | downstream consumers |

## Magnitudes — `counter.v1.popularity` (producer: `counter`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `counter.v1.popularity` | an entity's popularity magnitude changed | a window flush updates a popularity score | `search` (ranking), `realtime` (live broadcast) |

## Trust & Safety — `moderation.v1.events` (producer: `moderation`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `decision_recorded` | an authoritative integrity ruling was made — carries *who decided* + *why* (DSA SoR) | a decision is recorded (auto-screen / human review / appeal reversal) | `audit` (seals rationale in crypto-shred envelope) |
| `enforcement_applied` / `enforcement_reversed` | a consequence was applied/lifted against an actor (versioned) | enforcement commits | `timeline`, `chat`, `account` (Plane-B denorm); `post` (holds the restriction its reads apply: `remove_content` → author-only); `search`, `media` (visibility / takedown); `geo-discovery` (map suppression: `remove_content` / `visibility_limit` hide a post, a reversal restores it); `audit` |
| `case_opened` / `case_resolved` | a review unit opened/closed | ingestion threshold / reviewer action | Plane-B consumers |
| `appeal_resolved` | an appeal was decided (upheld / overturned; `by_reporter`; `profile_ids` = the appellant account's profiles) | appeal resolution | `notification` (tells each of `profile_ids` of the outcome, #744) |

> Keyed by `actor_id` for per-actor ordering. `decision_recorded` is the compliance-evidence variant
> (offender-centric consumers ignore it; `audit` consumes it + `enforcement_applied`).

## Conversations — `chat.*` (producer: `chat`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `chat.conversation.created` / `chat.conversation.published` / `chat.conversation.unpublished` | conversation lifecycle facts | create / publish / unpublish | `VisibilityWorker` (audience-plane teardown); `InboxWorker` (created: the owner's inbox entry, #656) |
| `chat.member.joined` / `chat.member.left` | membership changed | join/leave | `InboxWorker` (the member's inbox entry in / out, #656) |
| `chat.message.sent` | a message was committed to the log; `withheld` (shown to its sender only) and `request` (a message request's one message) since #656 | send commits | `InboxWorker` (each member's inbox entry up front — a withheld message moves only its sender's); chat's own live plane (**not** consumed by `realtime` — Separate Ways). Pushes go through `chat.message.push` below, not this topic |
| `chat.message.push` | a message's push (#654): its sender, a 100-character text preview (empty for media), and the `recipients` — the members it reaches in their inbox (not its sender, not a request's recipient, not someone blocking the sender) who have not muted the conversation. System messages are never pushed. Kept 24 h (`RETENTION`): it carries message text | `InboxWorker` once the inbox entries are written (at least once) | `notification` (`ChatPushWorker`: sends it once per message — Redis claim on `message_id` — to each recipient's iOS devices, under their `messages` preference, pause and quiet hours) |

## Media — `media.v1.events` (producer: `media`)

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `asset_uploaded` | bytes landed in the object store | finalize | the transform pipeline (Plane B) |
| `asset_ready` / `asset_variant_ready` | the asset (or a variant) is safe to deliver | CSAM Screen passes / rendition done | `post`, `profile`, `search` |
| `asset_quarantined` / `asset_deleted` / `asset_restored` | a safety/lifecycle transition | Screen fail / takedown / restore | embeds, delivery |
| `asset_failed` | processing failed | timeout/error | upload UX |

## Social Graph — relation events (producer: `social-graph`)

> The split, past-tense topics below **are** produced and consumed (see the wiring block). Only the
> *combined* `social-graph.follows` stream `counter` would prefer is deferred (a tracked naming
> mismatch); `counter` reconciles via gRPC meanwhile.

| Event | Means | Emitted when | Consumers & why |
|---|---|---|---|
| `ProfileFollowed` / `ProfileUnfollowed` | a follow edge was created/removed | follow/unfollow commits | `timeline`/`counter` (consume **via gRPC today**; the `social-graph.follows` stream is deferred) |
| `FollowRequested` / `FollowRequestWithdrawn` | a follow request to a private profile was made / is gone without becoming a follow (cancelled, declined, cut by a block, moot once the profile is public) — both on `social-graph.follow_requested`, keyed `actor:target`, so a withdrawal is never read before its request | request / cancel / decline / block / follow of a now-public profile | `notification` (tells the owner; a withdrawal retracts that notice and its unread count) |
| `ProfileBlocked` / `ProfileUnblocked` | a block edge changed (severs follows) | block/unblock commits | feeds |
| `AuthorTierChanged` | the author's tier changed | follower count crosses a threshold | `profile` (owns + re-emits as `tier_changed`) |

## Terminal sinks — publish nothing of record

`audit`, `search`, `timeline`, `geo-discovery`, `realtime` consume the above and assert no durable
business facts outward. `notification`'s `NotificationCreated`/`Read` are internal feed state, not a
System-of-Record stream.

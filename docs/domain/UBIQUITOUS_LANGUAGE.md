# Ubiquitous Language (cross-context)

> Populated from the 17 per-service Domain Cards (`crates/services/<svc>/docs/DOMAIN.md` §2). Holds
> **only** terms used by more than one bounded context. Terms that live inside a single context stay
> in that service's `DOMAIN.md §2`, not here.

## Why a cross-context glossary

The same word often means different things in different contexts ("subject", "visibility", "score"),
and a few words must mean *exactly the same thing everywhere* (identifiers, tier, popularity). This
file pins down the second category and flags the first, so an event or RPC consumed across a
boundary is not silently misread.

## Rules

- **Code symbol is mandatory.** A term with no `crate::Type` / proto / topic is aspirational, not
  ubiquitous — leave it out until it has one. (Pure architectural *posture* vocabulary is listed
  separately at the bottom, by reference, since it has no single symbol.)
- **Contracts stay in English.** Per the [translation standard](../i18n/TRANSLATION.md), identifiers,
  error codes, topics, env vars, and type names are language-invariant.
- **Note divergence.** When a word means different things in different contexts, give it a row per
  context and say so explicitly.

## Shared terms (one meaning everywhere)

| Term | Meaning | Code symbol / contract | Owning context |
|---|---|---|---|
| Profile id | The public-persona identifier (a profile / content author) | `ProfileId` — aliased `AuthorId` in `geo-discovery` / `timeline` | `profile` |
| Account id | The account-of-record identifier (the private identity) | `AccountId` | `account` |
| Post id | The content (post) identifier | `PostId` | `post` |
| Author tier | The follower-count-derived tier of an author | `AuthorTier` | `social-graph` computes → `profile` owns + emits (`tier_changed`) |
| Popularity score | The published popularity magnitude of an entity | `PopularityScore` (topic `counter.v1.popularity`) | `counter` |
| Stream sequence | The per-stream monotonic token a client dedupes / re-syncs from | `StreamSeq` / `SequenceState` | `realtime` |
| Consumer runtime | The mandatory Kafka consumer runner: manual commit after a terminal outcome, bounded retry + jitter, DLQ on poison/exhaustion | `run_consumer` | `transport` (shared kernel) |
| Deterministic event id | A content-addressed `UUIDv5` enabling idempotent dedup on at-least-once redelivery | UUIDv5 id convention | `notification`, `moderation`, `audit` |
| Monotonic per-subject version | A counter bumped per subject to order / revoke (revocation family; enforcement ordering) | `Generation` (`auth`) · `EnforcementVersion` (`moderation`) | shared pattern |

## Overloaded terms (different meaning per context)

| Term | Context | Meaning here | Code symbol |
|---|---|---|---|
| **Subject** | `audit` | A pseudonymous **data subject** — the person whose PII is sealed in the chain | pseudonym + `PiiEnvelope` |
| **Subject** | `moderation` | The **moderated target** — entity type + id + actor + surface | `SubjectRef` |
| **Subject** | `notification` | The **thing a notification is about** | `SubjectId` / `SubjectKind` |
| **Visibility** | `chat` | Member-plane vs Audience-plane (the Shadowing Pattern) | `Visibility` |
| **Visibility** | `profile` | Dual-axis: owner choice **and** moderation masking | `ProfileVisibility` |
| **Visibility** | `search` | The authority (owner / moderation) honoured at query time | `VisibilityAuthority` |
| **Visibility** | `media` | Whether / how an asset may be served | `DeliveryVisibility` |
| **Like** | `wallet` / `engagement` | One point an account stakes on a post or comment (#665); the like count is the sum | `Stake`, `LikeTarget` |
| **Score** | `geo-discovery` | Virality ranking weight for a map card | `ViralityScore` |
| **Score** | `counter` | Published popularity magnitude | `PopularityScore` |
| **Event** | `audit` | An immutable **recorded compliance fact** (the thing stored) | `AuditEvent` / `AuditRecord` |
| **Event** | fleet-wide | A **published domain fact** on Kafka (the thing emitted) | `DomainEvent` |
| **Entity kind** | `counter` | The kind of entity being **counted** | `EntityKind` (counter) |
| **Entity kind** | `search` | The kind of **indexed document** (post / profile / hashtag) | `EntityKind` (search) |

> **Watch-out:** the overloaded terms above are the most common cross-boundary misreads. When an
> event or RPC crosses contexts, resolve the term against the **producing** context's meaning.

## Cross-cutting posture vocabulary (by reference)

These are pervasive *architectural* terms, not domain types — they have no single code symbol, so
they live by reference rather than in the tables above:

- **SoR / SoReference / System-of-Connection / Evidence** — a context's authority class; defined per
  service in each `DOMAIN.md` Domain Card ("System of …").
- **Fail-open / fail-closed** — a context's failure posture; defined per service in each Domain Card
  and summarized in `CONTEXT_MAP.md`.
- **OHS / Published Language / ACL / Conformist / Customer-Supplier / Separate Ways / Shared Kernel**
  — the DDD coupling vocabulary; defined once in [`CONTEXT_MAP.md`](./CONTEXT_MAP.md#relationship-patterns-the-vocabulary).

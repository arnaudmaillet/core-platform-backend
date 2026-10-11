# Architecture Decision Records (ADRs)

> An **ADR** captures one architectural decision, the forces behind it, and the alternatives
> rejected — the *why* that code and diagrams cannot express and that otherwise evaporates into
> Slack threads and tribal memory.

## Conventions

- **Format:** MADR-lean — see [`0000-template.md`](./0000-template.md).
- **Naming:** `00NN-<kebab-title>.md`, number zero-padded to 4 digits, monotonically increasing.
- **Immutability:** an Accepted ADR is never edited to change its decision. To reverse it, add a
  **new** ADR that supersedes it and flip the old one's status to `Superseded by ADR-00MM`. The
  preserved trail of superseded decisions is the point.
- **Linkage:** every ADR is referenced from the affected service's `DOMAIN.md §9` (and, for
  cross-context decisions, from `docs/domain/CONTEXT_MAP.md`). Never inline the rationale in those
  docs — link the ADR so the *why* lives in exactly one place.

## Status lifecycle

```
Proposed --> Accepted --> Superseded by ADR-00MM
                      \--> Deprecated
```

## Index

| ADR | Title | Status | Context(s) |
|---|---|---|---|
| [0000](./0000-template.md) | _Template (not a decision)_ | — | — |
| [0001](./0001-audit-is-a-separate-evidence-plane.md) | Audit is a separate tamper-evident evidence plane, not a log aggregator | Accepted | audit |
| [0002](./0002-moderation-decision-enforcement-sor-with-fail-closed-screen.md) | Moderation is the decision/enforcement SoR with a narrow fail-closed Screen gate | Accepted | moderation |
| [0003](./0003-realtime-is-a-fail-open-system-of-connection.md) | Realtime is a fail-open System-of-Connection, never a record store | Accepted | realtime |
| [0004](./0004-account-is-the-single-identity-sor.md) | Account is the single identity SoR and owns an outbound event lane | Accepted | account |
| [0005](./0005-auth-federated-idp-with-platform-edge-tokens.md) | Auth federates an IdP and issues platform ES256 edge tokens with generation-based revocation | Accepted (superseded in part by 0018) | auth |
| [0006](./0006-chat-shadowing-pattern-member-vs-audience-plane.md) | Chat uses the Shadowing Pattern — separate Member and Audience visibility planes | Accepted | chat |
| [0007](./0007-comment-flat-thread-two-table-tombstone-purge.md) | Comment uses a nil-UUID flat thread, two-table Scylla, tombstone-vs-purge deletion | Accepted | comment |
| [0008](./0008-counter-magnitudes-are-a-reconcilable-soref.md) | Counter is a reconcilable SoReference for magnitudes, distinct from edge state | Accepted | counter |
| [0009](./0009-engagement-redis-primary-lua-atomic-with-kafka-write-behind.md) | Engagement is Redis-primary (Lua-atomic) with Kafka write-behind durability | Accepted | engagement |
| [0010](./0010-geo-discovery-h3-grid-dual-layer-redis-topk.md) | Geo-discovery is an H3 grid_disk + dual-layer Redis Top-K spatial read-model | Accepted | geo-discovery |
| [0011](./0011-media-byte-free-control-plane.md) | Media is a byte-free control plane (fail-open delivery / fail-closed CSAM Screen) | Accepted | media |
| [0012](./0012-notification-write-collapse-fanout-claim-gated-counter.md) | Notification uses write-collapse fan-out + a claim-gated idempotent unread counter | Accepted | notification |
| [0013](./0013-post-two-table-scylla-with-published-language.md) | Post uses a two-table Scylla layout and is the published-language fan-out source | Accepted | post |
| [0014](./0014-profile-public-persona-with-dual-axis-visibility.md) | Profile is the public persona over the account SoR with dual-axis visibility | Accepted | profile |
| [0015](./0015-search-opensearch-single-store-external-versioning.md) | Search is an OpenSearch read-model with external versioning and a fail-open read path | Accepted | search |
| [0016](./0016-social-graph-four-table-scylla-logged-batch.md) | Social-graph uses a 4-table Scylla schema with logged-batch atomic dual-writes | Accepted | social-graph |
| [0017](./0017-timeline-hybrid-push-pull-fanout.md) | Timeline uses a hybrid push/pull fan-out | Accepted | timeline |
| [0018](./0018-passwordless-identity-passkeys-for-staff.md) | Passwordless identity: Keycloak retired, passkeys for staff | Accepted | auth; account |

<!-- Add one row per ADR as it lands. -->

## Backlog

Every service now has a keystone ADR (0001–0017). The next candidates are the **finer-grained locked
choices** behind them, to be recorded as their own ADRs when next revisited — e.g. audit's
crypto-shred RtbF mechanism, its dual-lane fail-open/fail-closed split, and the hash-chain + WORM +
external-witness immutability scheme; auth's session-`Generation` rotation; profile/social-graph's
author-tier producer flow.

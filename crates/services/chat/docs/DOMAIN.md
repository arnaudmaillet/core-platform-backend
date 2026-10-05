# `chat` — Domain & Functional Contract

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Conversations & Messaging |
> | **Subdomain class** | **Core** — direct-messaging is a primary product surface; the member/audience shadowing model is bespoke |
> | **System of …** | **Record** for conversations, membership, and messages |
> | **Aggregate root(s)** | `Conversation`, `Message` (`domain`) |
> | **Tier** | **TIER-0** |
> | **Failure posture** | **Fail-closed on writes** (a sent message must persist); live delivery is best-effort over its own plane |
> | **Upstream contexts** | end-user clients (send/read); `moderation` (content gating) |
> | **Downstream contexts** | consumers of `chat.*` events; runs its **own** live plane (coexists with `realtime`) |
> | **Decision log** | _none yet — see [`docs/adr/`](../../../../docs/adr/README.md)_ |

---

## 1. Business Capability & Non-Goals

**Capability.** `chat` is the authority for **conversations and messages**: it answers
**"what was said, in which conversation, by whom, and who is allowed to see it?"**

**The hard problem.** Serving both *members* (who read/write) and a broader *audience* (who may
view a published conversation) without conflating the two visibility planes — the **Shadowing
Pattern** (a Member plane vs an Audience plane) — and storing a high-volume message log
cost-effectively via bucketed `(conversation_id, bucket)` partitions.

**Non-goals — what this context deliberately does NOT do:**
- ❌ Be the generic live-delivery plane → `realtime` is that; chat runs its own live plane today (coexist-first).
- ❌ Classify/decide on content → `moderation` decides; chat enforces gating.
- ❌ Store media bytes → `media` owns those.

---

## 2. Ubiquitous Language

| Term | Meaning in this context | Code symbol |
|---|---|---|
| Conversation | A messaging thread with a membership and a policy | `Conversation`, `ConversationId`, `ConversationKind` |
| Message | A single message in a conversation log | `Message`, `MessageId`, `MessageContent` |
| Participant | A member of a conversation, with a role | `Participant`, `Role` |
| Conversation policy | The rules governing the conversation | `ConversationPolicy` |
| Visibility | Member-plane vs audience-plane visibility (the shadowing) | `Visibility` |
| Content type | The kind of message content | `ContentType` |

---

## 3. Domain Model

| Element | Kind | Invariant boundary it guards |
|---|---|---|
| `Conversation` | aggregate root | Membership + policy + publish/visibility state |
| `Message` | aggregate root | Message integrity within a conversation bucket |
| `Participant` / `Role` | VO/enum | Who may read/write and at what authority |
| `ConversationPolicy` | VO | The conversation's governing rules |
| `Visibility` / `ConversationKind` / `ContentType` | enum | Closed plane/kind/content vocabularies |

**Conversation lifecycle:**

```
created --(publish)--> published --(unpublish)--> unpublished (audience plane torn down)
   │  member join/leave                                   │
   └──────────────── message.sent (bucketed log) ─────────┘
```

> **Legal transitions only.** Membership is checked at the gRPC boundary; an unpublish tears down
> the audience plane via the `VisibilityWorker`; only members write.

---

## 4. Data Ownership & Boundaries

**This context is the source of truth for:**
- Conversations, membership, and the message log — **ScyllaDB** (bucketed `(conversation_id, bucket)` log). No other service writes these.
- Live routing/presence — **Redis** sharded pub/sub (`RedisSubscriber`), derived/ephemeral.

**The "do-not-write" list:** chat doesn't own media bytes (references `media`), doesn't decide
moderation, and doesn't write any other service's state.

---

## 5. Invariants & Business Rules

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | Only members may write/read the member plane | application + gRPC boundary | `CHT-1007`/`PERMISSION_DENIED` (public); `CHT-1009`/`NOT_FOUND` (private) |
| I2 | Member plane and audience plane are kept distinct (shadowing) | domain | `CHT-2xxx` |
| I3 | Unpublish tears down the audience plane completely | application (`VisibilityWorker`) | `CHT-4xxx` |
| I4 | Messages are appended to the correct conversation bucket | domain | `CHT-9xxx` |

---

## 6. Workflows & Orchestration

> Inline until a corrected C4 is regenerated from `docs/domain/`.

**Send message.** Member-authorized send → append to the `(conversation_id, bucket)` Scylla log →
publish `chat.message.sent` and push live over chat's own Redis sharded pub/sub plane (dual gRPC
streaming to connected clients).

**Publish / unpublish (shadowing).** Publishing a conversation opens the audience plane;
unpublishing triggers the `VisibilityWorker` teardown, consuming `chat.conversation.unpublished`
(DLQ `chat.conversation.unpublished.dlq`) to dismantle audience-plane state.

**Membership.** Join/leave emit `chat.member.joined` / `chat.member.left`. A public conversation is
open-join; a private one admits only a profile holding a pending **invitation** (issued by an
owner/admin, consumed by the join, 7-day TTL). A non-invited joiner is refused as if the conversation
did not exist (`CHT-1009`, rendered like `CHT-1001`). The same concealment applies to every other RPC
an outsider aims at a private conversation, so none of them can probe for its existence.

---

## 7. Context Relationships (Context-Map slice)

| Neighbour context | Direction | Pattern | Mechanism | What breaks if they change |
|---|---|---|---|---|
| clients | upstream | OHS | gRPC + dual streaming | client messaging breaks |
| `moderation` | upstream | Customer/Supplier | content gating | gated content path breaks |
| `realtime` | peer | Separate Ways (coexist) | — (not consumed; chat owns its live plane) | future consolidation deferred |
| event consumers | downstream | Published Language | `chat.*` topics | downstream reactions break |

> **Anti-Corruption Layer:** the Redis subscriber + visibility worker isolate the live-plane
> mechanics from the domain.

---

## 8. Domain Events (semantics, not wire)

| Event | Means | Emitted when | Who reacts |
|---|---|---|---|
| `chat.conversation.created` / `chat.conversation.published` / `chat.conversation.unpublished` | conversation lifecycle facts | create / publish / unpublish | `VisibilityWorker`, downstream consumers |
| `chat.member.joined` / `chat.member.left` | membership change | join/leave | downstream consumers |
| `chat.message.sent` | a message was committed to the log | send commits | (chat live plane; **not** consumed by `realtime` yet) |

---

## 9. Decisions & Rationale

| Decision | ADR | Status |
|---|---|---|
| Shadowing Pattern — distinct Member vs Audience visibility planes | [`ADR-0006`](../../../../docs/adr/0006-chat-shadowing-pattern-member-vs-audience-plane.md) | Accepted |
| Coexist-first with `realtime` (chat keeps its own live plane for now) | _see ADR-0003 consequences_ | Accepted |

---

## 10. Subdomain Classification & Evolution

- **Classification:** Core — messaging is a primary product surface.
- **Volatility:** medium — conversation kinds and visibility rules evolve with product.
- **Known modeling debt:** chat's own live plane duplicates `realtime` (the consolidation seam is deliberately open).
- **Deferred capabilities:** consolidation onto `realtime`; richer media-in-chat; reactions.

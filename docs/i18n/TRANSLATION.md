# Service documentation — translation standard

This repo keeps **English as the single canonical source** for every service README.
Translations (e.g. `README.fr.md`) are **derived artifacts**: regenerated from the English
source, never edited independently of it, and mechanically checked for drift.

A stale or mistranslated technical doc is worse than none — it lies to the on-call engineer.
This standard exists to make drift **impossible to introduce silently**.

---

## 1. Canonical / derived

- `README.md` (English) is canonical and is what GitHub renders by default.
- `README.fr.md` (and any future `README.<lang>.md`) is a **translation of a specific revision**
  of its sibling `README.md`, recorded by content hash (§3).
- On **any** discrepancy, the English version governs. Every translation says so in its banner.

---

## 2. The Do-Not-Translate (DNT) invariant

A README encodes **contracts**, and contracts are language-invariant. The following appear
**verbatim, in English**, in every translation — translating them corrupts a contract:

| Category | Examples (must stay verbatim) |
|---|---|
| Error codes | `CHT-4001`, `PRF-1002`, `GEO-2001` |
| gRPC / HTTP status names | `FAILED_PRECONDITION`, `UNAVAILABLE`, `ABORTED` |
| Environment variables | `CHAT_MESSAGE_BUCKET_HOURS`, `KAFKA_BROKERS`, `REDIS_HOSTS` |
| Kafka topics & consumer groups | `chat.conversation.unpublished`, `chat-visibility-consumer` |
| Proto / package / RPC / service names | `chat.v1.ChatService`, `StreamConversation`, `CommandResponse` |
| Keyspace / table / column names | `messages_by_conversation`, `posts_by_profile`, `created_at_ms` |
| Redis key patterns | `{conv:<id>}`, `sg:geo:tile:{h3}:{res}` |
| Rust / type / trait / crate names | `service_runtime::Service`, `MessageRepository`, `run_consumer` |
| CLI / shell commands | `cargo build -p chat`, `cqlsh -f …` |
| Tier labels & metric names | `TIER-0`, `geo_discovery_tile_query_duration_ms` |
| Code blocks (proto / rust / bash / cql) | translated **only** in `# comments`, never in code |

> Rule of thumb: **if a machine reads it, freeze it. If only a human reads it, translate it.**

---

## 3. Provenance frontmatter (the drift anchor)

Every translation begins with YAML frontmatter binding it to the exact source revision, followed
by a human-readable banner:

```markdown
---
i18n:
  source: ./README.md
  source_sha256: <sha256 of README.md at translation time>
  translated_at: <YYYY-MM-DD>
  status: complete        # complete | stale
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.
```

- `source_sha256` is the SHA-256 of the **entire** sibling `README.md` at the moment of translation.
- `status: complete` asserts the translation matches that hash.
- `status: stale` is the **only** sanctioned way to acknowledge known drift (see §5).

---

## 4. Scope — what gets translated

| Section | Translate? |
|---|---|
| Tagline, Overview, Architecture prose, Invariants notes | ✅ prose |
| SLO / Blast Radius / Failure Modes — **description cells** | ✅ prose |
| Deployment, Telemetry intro, Local Dev intro, Troubleshooting | ✅ prose |
| Service Card — **row labels** | ✅ (values are identifiers → frozen) |
| Public Interfaces (proto / Rust ports) | ❌ frozen |
| Error-code tables | ❌ frozen |
| Configuration env-var tables | ❌ frozen |
| Events & Async Contract tables | ❌ frozen |
| All code / bash / cql blocks | ❌ frozen (except `# comments`) |

Reference tables that are dense with identifiers stay English: a French engineer reads
`CHAT_MAX_PAGE_SIZE` exactly as an English one does, and freezing them shrinks the drift surface.

---

## 5. Workflow

**Translating / updating:**
1. Translate from the current `README.md`, honoring the DNT invariant (§2) and scope (§4).
2. Keep the **section structure identical** to the English file (same headings, same order) so
   reviewers can diff section-by-section.
3. Stamp the provenance hash:
   ```bash
   bash tools/i18n/i18n-drift.sh stamp crates/services/<svc>/README.fr.md
   ```
4. A French-speaking engineer reviews. Never machine-translate without the DNT list + human review.

**When you change an English README but can't retranslate now:**
- Set the translation's `status: stale`. This keeps CI green **and** surfaces a ⚠️ banner to readers.
  It is an *acknowledged-debt* valve — drift is visible, never silent.

**CI gate** (`.github/workflows/i18n-drift.yml`, called by `ci.yml` and reported in its `ci-ok` check — #828): on any PR touching a README, recomputes each
`README.md` hash and compares it to the sibling `README.fr.md`'s `source_sha256`:
- match → ✅ pass
- mismatch + `status: complete` → ❌ **fail** ("update the translation or mark it stale")
- mismatch + `status: stale` → ⚠️ pass with warning

---

## 6. Adding a new language

Copy the pattern with `README.<lang>.md` (e.g. `README.es.md`). The drift script discovers any
`README.*.md` sibling automatically; no per-language config needed.

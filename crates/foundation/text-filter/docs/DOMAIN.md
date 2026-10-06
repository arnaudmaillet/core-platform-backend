# `text-filter` — Domain & Functional Contract

> One matching rule for a holder's content filters: *"does this text contain something its recipient
> chose not to see?"*

> **Domain Card**
>
> | | |
> |---|---|
> | **Shared capability** | Matching a text against a holder's hidden words and the offensive-term list |
> | **Layer** | `foundation` — pure, no IO |
> | **Subdomain class** | **Supporting** — the safety settings of comments (#660) and message requests (#810) |
> | **Primary abstraction(s)** | `ContentFilter`, `TermList` |
> | **Footprint** | pure (no IO, no state; `TermList::from_file` reads a file once, at startup) |
> | **Failure posture** | N/A — callers decide what an unreadable filter means (both fail closed) |
> | **Depends on** | `serde` |
> | **Consumed by** | `comment`, `chat` |
> | **Decision log** | extracted from `comment` (#728) when `chat` needed the same rule (#810) |

---

## 1. Technical Capability & Non-Goals

**Capability.** Given a holder's `ContentFilter` and the offensive `TermList`, say whether a text is
caught. One rule for every surface, so a word hidden from comments is hidden from message requests the
same way.

**Non-goals:** storing the filters, projecting profile events, choosing what a caught text becomes, and
supplying the offensive list — all the services'.

## 2. Ubiquitous Language

| Term | Meaning |
|---|---|
| **Hidden words** | Words, phrases or emoji a holder hides (≤ 200, each ≤ 64 chars, set at profile). |
| **Offensive filter** | The holder's switch for the operator's offensive-term list; on by default. |
| **Caught** | A text the filter hides: it contains a hidden word, or an offensive term with the filter on. |

## 3. Invariants

- Matching is case-insensitive.
- A term with a letter or digit matches whole words (a phrase: those words in a row); a term with
  neither (an emoji) matches anywhere.
- An empty filter and an empty list catch nothing; the default filter keeps the offensive filter on.

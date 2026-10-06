# `text-filter` — A holder's hidden words and the offensive-term list, matched against user text

> **Crate Card**
>
> | | |
> |---|---|
> | **Role** | `foundation` — one matching rule for the content filters a holder sets |
> | **Package** | `text-filter` (dir: `crates/foundation/text-filter`) |
> | **Consumed by** | `comment` (the comments on a holder's posts, #660), `chat` (the message requests a holder gets, #810) |
> | **Depends on** | `serde` (the filter is stored and sent as-is) |
> | **Stability** | stable contract |
> | **Feature flags** | none |
> | **Owner** | `<TODO: team>` · `<TODO: #slack-channel>` |

---

## 🎯 Overview & role

A holder hides words from what others write to them — comments on their posts, message requests — and
keeps an offensive-term filter on or off (profile `ProfileCommentFiltersChanged`). `text-filter` is the
one place that decides whether a text is caught, so a comment and a message request are judged the same
way.

- `ContentFilter { hidden_words, filter_offensive }` — the holder's filter; the default (nothing set) has
  no hidden words and the offensive filter **on**. `hides(text, &offensive)`.
- `TermList` — a list of terms (the offensive list): trimmed, lowercased, `#` lines and blanks dropped,
  de-duplicated; `TermList::from_file(path)` reads one term per line.

## 📐 Matching

- Case-insensitive.
- A term with a letter or a digit matches **whole words**: the text and the term are split on
  non-alphanumerics, so `spoiler` catches "Big SPOILER!" but not "spoilers"; a phrase (`the end`) matches
  those words in a row.
- A term with neither (an emoji) matches anywhere.

## 🔒 Non-goals

- Storing or projecting the filters: each service keeps its own projection (`comment.comment_filters`,
  `chat.message_filters`).
- Deciding *what happens* to a caught text (hidden from others, moved to hidden requests): the services'.
- Shipping the offensive list: an operator-provided file each service reads
  (`COMMENT_OFFENSIVE_TERMS_FILE`, `CHAT_OFFENSIVE_TERMS_FILE`).

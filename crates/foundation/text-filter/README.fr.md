---
i18n:
  source: ./README.md
  source_sha256: c7fba746a6997e591b41cf4ea7e905d82e4b02d24c3a2841e66833b1c23ebd92
  translated_at: 2026-10-06
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, signatures, identifiants) sont volontairement laissés en anglais.

# `text-filter` — Les mots masqués d'un titulaire et la liste des termes injurieux, appliqués aux textes des utilisateurs

> **Fiche du crate**
>
> | | |
> |---|---|
> | **Rôle** | `foundation` — une seule règle de correspondance pour les filtres de contenu d'un titulaire |
> | **Paquet** | `text-filter` (dossier : `crates/foundation/text-filter`) |
> | **Consommé par** | `comment` (les commentaires sur les posts d'un titulaire, #660), `chat` (les demandes de messages qu'il reçoit, #810) |
> | **Dépend de** | `serde` (le filtre est stocké et transmis tel quel) |
> | **Stabilité** | contrat stable |
> | **Feature flags** | aucun |
> | **Responsable** | `<TODO: team>` · `<TODO: #slack-channel>` |

---

## 🎯 Vue d'ensemble & rôle

Un titulaire masque des mots dans ce que les autres lui écrivent — commentaires sur ses posts, demandes de
messages — et active ou non un filtre de termes injurieux (profile `ProfileCommentFiltersChanged`).
`text-filter` est le seul endroit qui décide si un texte est intercepté, si bien qu'un commentaire et une
demande de message sont jugés de la même façon.

- `ContentFilter { hidden_words, filter_offensive }` — le filtre du titulaire ; par défaut (rien de
  réglé), aucun mot masqué et le filtre injurieux **actif**. `hides(text, &offensive)`.
- `TermList` — une liste de termes (la liste injurieuse) : nettoyés, en minuscules, lignes `#` et vides
  écartées, sans doublons ; `TermList::from_file(path)` lit un terme par ligne.

## 📐 Correspondance

- Insensible à la casse.
- Un terme qui contient une lettre ou un chiffre correspond à des **mots entiers** : le texte et le terme
  sont découpés sur les caractères non alphanumériques, si bien que `spoiler` intercepte « Big SPOILER! »
  mais pas « spoilers » ; une expression (`the end`) correspond à ces mots à la suite.
- Un terme sans aucun des deux (un emoji) correspond n'importe où.

## 🔒 Hors périmètre

- Stocker ou projeter les filtres : chaque service garde sa propre projection (`comment.comment_filters`,
  `chat.message_filters`).
- Décider *ce qu'il advient* d'un texte intercepté (masqué pour les autres, déplacé dans les demandes
  masquées) : c'est aux services.
- Fournir la liste injurieuse : un fichier fourni par l'exploitation que chaque service lit
  (`COMMENT_OFFENSIVE_TERMS_FILE`, `CHAT_OFFENSIVE_TERMS_FILE`).

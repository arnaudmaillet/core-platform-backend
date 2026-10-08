---
i18n:
  source: ./UBIQUITOUS_LANGUAGE.md
  source_sha256: 13bc9b0832935f6ead969976437bb241367eb7a55042a78f370c9d444ec69624
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`UBIQUITOUS_LANGUAGE.md`](./UBIQUITOUS_LANGUAGE.md) fait foi.
> En cas de divergence, l'anglais prime. Les termes du langage omniprésent, les noms de types et les
> identifiants restent en anglais (ce sont les mots réellement employés dans le code et les contrats).

# Langage omniprésent (inter-contexte)

> Rempli depuis le §2 des 17 Domain Cards par service (`crates/services/<svc>/docs/DOMAIN.md`).
> Contient **uniquement** les termes employés par plus d'un bounded context. Les termes propres à un
> seul contexte restent dans le `DOMAIN.md §2` de ce service.

## Pourquoi un glossaire inter-contexte

Le même mot signifie souvent des choses différentes selon le contexte (« Subject », « Visibility »,
« Score »), et quelques mots doivent signifier *exactement la même chose partout* (identifiants,
tier, popularité). Ce fichier fige la seconde catégorie et signale la première, pour qu'un événement
ou un RPC consommé au-delà d'une frontière ne soit pas silencieusement mal interprété.

## Règles

- **Le symbole de code est obligatoire.** Un terme sans `crate::Type` / proto / topic est une
  aspiration, pas un langage omniprésent — l'omettre tant qu'il n'en a pas. (Le vocabulaire de
  *posture* purement architectural est listé séparément en bas, par référence, car il n'a pas de
  symbole unique.)
- **Les contrats restent en anglais.** Selon le [standard de traduction](../i18n/TRANSLATION.md), les
  identifiants, codes d'erreur, topics, variables d'environnement et noms de types sont invariants de
  langue.
- **Signaler la divergence.** Quand un mot signifie des choses différentes selon le contexte, lui
  donner une ligne par contexte et le dire explicitement.

## Termes partagés (un seul sens partout)

| Terme | Sens | Symbole de code / contrat | Contexte propriétaire |
|---|---|---|---|
| Profile id | L'identifiant de persona public (un profil / auteur de contenu) | `ProfileId` — aliasé `AuthorId` dans `geo-discovery` / `timeline` | `profile` |
| Account id | L'identifiant de compte de référence (l'identité privée) | `AccountId` | `account` |
| Post id | L'identifiant de contenu (post) | `PostId` | `post` |
| Author tier | Le tier d'un auteur dérivé du nombre de followers | `AuthorTier` | `social-graph` calcule → `profile` possède + émet (`tier_changed`) |
| Popularity score | La magnitude de popularité publiée d'une entité | `PopularityScore` (topic `counter.v1.popularity`) | `counter` |
| Stream sequence | Le token monotone par flux qu'un client déduplique / re-synchronise | `StreamSeq` / `SequenceState` | `realtime` |
| Consumer runtime | Le runner de consommateur Kafka obligatoire : commit manuel après un résultat terminal, retry borné + jitter, DLQ sur poison/épuisement | `run_consumer` | `transport` (shared kernel) |
| Deterministic event id | Un `UUIDv5` adressé-par-contenu permettant la déduplication idempotente en redelivery at-least-once | convention d'id UUIDv5 | `notification`, `moderation`, `audit` |
| Monotonic per-subject version | Un compteur incrémenté par sujet pour ordonner / révoquer (famille de révocation ; ordonnancement d'enforcement) | `Generation` (`auth`) · `EnforcementVersion` (`moderation`) | pattern partagé |

## Termes surchargés (sens différent selon le contexte)

| Terme | Contexte | Sens ici | Symbole de code |
|---|---|---|---|
| **Subject** | `audit` | Un **sujet de données** pseudonymisé — la personne dont la PII est scellée dans la chaîne | pseudonyme + `PiiEnvelope` |
| **Subject** | `moderation` | La **cible de modération** — type d'entité + id + acteur + surface | `SubjectRef` |
| **Subject** | `notification` | Le **sujet d'une notification** | `SubjectId` / `SubjectKind` |
| **Visibility** | `chat` | Plan Membre vs plan Audience (le Shadowing Pattern) | `Visibility` |
| **Visibility** | `profile` | Bi-axiale : choix du propriétaire **et** masquage par modération | `ProfileVisibility` |
| **Visibility** | `search` | L'autorité (propriétaire / modération) honorée au moment de la requête | `VisibilityAuthority` |
| **Visibility** | `media` | Si / comment un asset peut être servi | `DeliveryVisibility` |
| **Like** | `wallet` / `engagement` | Un point qu'un compte mise sur un post ou un commentaire (#665) ; le compteur de likes en est la somme | `Stake`, `LikeTarget` |
| **Score** | `geo-discovery` | Pondération de classement par viralité pour une map card | `ViralityScore` |
| **Score** | `counter` | Magnitude de popularité publiée | `PopularityScore` |
| **Event** | `audit` | Un **fait de conformité enregistré** immuable (la chose stockée) | `AuditEvent` / `AuditRecord` |
| **Event** | toute la flotte | Un **fait de domaine publié** sur Kafka (la chose émise) | `DomainEvent` |
| **Entity kind** | `counter` | Le type d'entité **comptée** | `EntityKind` (counter) |
| **Entity kind** | `search` | Le type de **document indexé** (post / profile / hashtag) | `EntityKind` (search) |

> **Attention :** les termes surchargés ci-dessus sont les erreurs de lecture inter-frontières les
> plus fréquentes. Quand un événement ou un RPC traverse des contextes, résoudre le terme selon le
> sens du contexte **producteur**.

## Vocabulaire de posture transverse (par référence)

Ce sont des termes *architecturaux* omniprésents, pas des types de domaine — ils n'ont pas de symbole
de code unique, donc ils vivent par référence plutôt que dans les tables ci-dessus :

- **SoR / SoReference / System-of-Connection / Evidence** — la classe d'autorité d'un contexte ;
  définie par service dans la Domain Card de chaque `DOMAIN.md` (« System of … »).
- **Fail-open / fail-closed** — la posture de défaillance d'un contexte ; définie par service dans
  chaque Domain Card et résumée dans `CONTEXT_MAP.md`.
- **OHS / Published Language / ACL / Conformist / Customer-Supplier / Separate Ways / Shared Kernel**
  — le vocabulaire de couplage DDD ; défini une fois dans
  [`CONTEXT_MAP.md`](./CONTEXT_MAP.fr.md#patterns-de-relation-le-vocabulaire).

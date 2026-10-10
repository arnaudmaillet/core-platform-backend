---
i18n:
  source: ./DOMAIN.md
  source_sha256: 449ed9020b19528c45882a83250b93ea52d09a4024ab27852bbc948610cf4b4b
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `timeline` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Timeline — le read-model du fil d'accueil |
> | **Classe de sous-domaine** | **Supporting** — une projection de fil dérivée ; ne possède aucun contenu source |
> | **System of …** | **Reference (SoRef)** — un fil par-utilisateur matérialisé, reconstructible depuis l'amont |
> | **Racine(s) d'agrégat** | `FeedEntry` (projection), adressé par `FeedCursor` |
> | **Tier** | **TIER-1** |
> | **Posture de défaillance** | **Fail-open** — un fil dégradé retourne moins/des entrées plus périmées, jamais une erreur |
> | **Contextes amont** | `post` (contenu), `social-graph` (graphe de followers) — via événements + gRPC |
> | **Contextes aval** | clients (lecture du fil) ; ne publie rien de référence |
> | **Journal de décisions** | [`ADR-0017`](../../../../docs/adr/0017-timeline-hybrid-push-pull-fanout.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `timeline` est l'autorité pour **le fil d'accueil** : il répond à
**« que doit voir cet utilisateur dans son fil en ce moment, et dans quel ordre ? »**

**Le problème difficile.** La génération de fil à l'échelle sans le coût fan-out-on-write des
célébrités ni le coût fan-out-on-read de tout le monde — un modèle **hybride push/pull** : matérialiser
les fils pour les auteurs normaux (push), tirer pour les auteurs haut-tier au moment de la lecture,
fusionnés via un Lua `ZREVRANGEBYSCORE`.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Posséder les posts → `post` est le SoR ; timeline détient des références de fil.
- ❌ Posséder le graphe de followers → lit `social-graph` (gRPC) pour le fan-out.
- ❌ Classer par magnitudes de popularité → ce signal vient de `counter` (là où câblé).

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Feed entry | Un item matérialisé dans le fil d'un utilisateur | `FeedEntry` |
| Feed cursor | La position de pagination dans un fil | `FeedCursor` |
| Fan-out mode | Push (matérialiser) vs pull (au moment de la lecture) par auteur | `FanOutMode` |
| Author tier | Le tier qui décide push vs pull | `AuthorTier` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `FeedEntry` | projection (agrégat) | L'identité d'un item de fil + son score d'ordonnancement |
| `FeedCursor` | VO | Position de pagination stable |
| `FanOutMode` | enum | Décision push vs pull par auteur |
| `AuthorTier` | enum | Le tier pilotant la décision hybride |

> **Invariant.** Les entrées de fil sont ordonnées par score (Lua `ZREVRANGEBYSCORE` via eval) ; les
> membres sont encodés de façon compacte ; `from_uuid` est infaillible. Les auteurs haut-tier sont
> tirés au moment de la lecture, pas matérialisés, pour borner le coût de fan-out.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité (de *référence*) pour :**
- Le fil par-utilisateur matérialisé — **Redis** (ZSETs de fil) + **ScyllaDB** (matérialisation durable). Reconstructible depuis `post` + `social-graph`.

**Ce contexte détient des copies dérivées qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Contenu/refs de post | `post` | `post.v1.events` / `post.deleted` | cohérence à terme |
| Graphe de followers | `social-graph` | lectures gRPC de l'ensemble des followers | au moment de la lecture |
| Tier d'auteur | `profile` (émet) | consommation du changement de tier | cohérence à terme |

**La liste « ne-pas-écrire » :** timeline n'écrit jamais les posts ni le graphe — il les projette en fils.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Fan-out hybride — auteurs normaux poussés, haut-tier tirés à la lecture | domaine/application | `TML-1xxx` |
| I2 | Ordonnancement du fil par score via Lua `ZREVRANGEBYSCORE` | infrastructure (Lua) | `TML-1xxx` |
| I3 | Les lectures échouent ouvertes (dégradent, jamais d'erreur) | application | `TML-1xxx` |
| I4 | Un post supprimé est retiré des fils | application (consumer) | `TML-1xxx` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Fan-out à la publication (push).** Consommer `PostPublished` (`post.v1.events`) → lire l'ensemble des followers de
l'auteur depuis `social-graph` (gRPC) → pour les auteurs tier-normal, matérialiser l'entrée dans le
ZSET de fil de chaque follower. Les auteurs haut-tier sont ignorés ici (tirés à la lecture).

**Lecture (fusion hybride).** Une lecture de fil fusionne les entrées push matérialisées avec un pull
au moment de la lecture des followees haut-tier de l'utilisateur, ordonnés par score via Lua
`ZREVRANGEBYSCORE`, paginés par `FeedCursor`. Fail-open sur un backend dégradé.

**Démantèlement.** Consommer `post.deleted` → retirer l'entrée des fils affectés.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `post` | amont | ACL | `post.v1.events` / `post.deleted` | la fraîcheur/le démantèlement du fil casse |
| `social-graph` | amont | Customer/Supplier (gRPC) | lectures de l'ensemble des followers | le fan-out casse |
| `profile` | amont | ACL | `tier_changed` | la décision push/pull devient périmée |
| clients | aval | OHS | RPC de lecture du fil | le fil d'accueil casse |

> **Anti-Corruption Layer :** le consumer d'événements `post` traduit le cycle de vie des posts en mutations de fil.

---

## 8. Événements de Domaine (sémantique, pas wire)

> Ne publie **rien de référence** — c'est un read-model. Il consomme `post` (et lit `social-graph`) ;
> leurs sens sont possédés par ces contextes.

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Fan-out hybride push/pull (matérialiser les auteurs normaux, tirer le haut-tier à la lecture) | [`ADR-0017`](../../../../docs/adr/0017-timeline-hybrid-push-pull-fanout.md) | Accepté |
| Compatibilité ascendante pour le fan-out piloté par tier d'auteur (livré #469) | _ouvert — initiative author-tier_ | Cadré |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — une projection de fil dérivée de `post` + `social-graph`.
- **Volatilité :** moyenne — le classement et le seuil push/pull évoluent.
- **Dette de modélisation connue :** réglage de performance du fan-out (TD-4) ; le côté producteur du tier d'auteur pas encore complet.
- **Capacités différées :** classement pondéré par popularité depuis `counter` ; classement personnalisé/ML.

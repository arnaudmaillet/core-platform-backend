---
i18n:
  source: ./DOMAIN.md
  source_sha256: 0091372e49d1d4a9d3fcabedb57be892bace57e04c8969540327d89693dedf69
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `social-graph` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Social Graph — relations follower/following et block |
> | **Classe de sous-domaine** | **Core** — le graphe social est le réseau lui-même |
> | **System of …** | **Record** pour les relations (follows, blocks) et le tier d'auteur dérivé |
> | **Racine(s) d'agrégat** | `Relation` (avec `FollowEdge` / `BlockEdge`) |
> | **Tier** | **TIER-1** |
> | **Posture de défaillance** | **Fail-closed en écriture** — un changement de relation doit être atomique + durable |
> | **Contextes amont** | clients (follow/block) ; `profile` (identité) |
> | **Contextes aval** | `timeline` (fan-out, lectures gRPC), `counter` (comptes de followers), `profile` (tier) — via gRPC + événements |
> | **Journal de décisions** | [`ADR-0016`](../../../../docs/adr/0016-social-graph-four-table-scylla-logged-batch.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `social-graph` est l'autorité pour **les relations** : il répond à
**« qui suit qui, qui a bloqué qui, et quel tier est cet auteur ? »**

**Le problème difficile.** Maintenir un graphe de relations à fort fan-out avec des index inverses
cohérents et des lectures de hot-set — un schéma ScyllaDB à 4 tables avec mise en cache des
relations chaudes en Redis Set, atomicité logged-batch pour la double-écriture, et des seuils de tier
dérivés du nombre de followers.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Construire des timelines → `timeline` lit le graphe (gRPC) et fan-out.
- ❌ Posséder la *présentation* du tier d'auteur → `social-graph` le calcule ; `profile` le possède + émet.
- ❌ Posséder les profils → `profile`.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Relation | Une arête dirigée entre deux profils | `Relation`, `RelationKind`, `RelationStatus` |
| Follow / block edge | Les deux types de relation | `FollowEdge`, `BlockEdge` |
| Relation context | Les métadonnées entourant une relation | `RelationContext` |
| Author tier | Le tier dérivé du nombre de followers | `AuthorTier`, `TierThresholds`, `AuthorTierChanged` |
| Severed follows | Les follows retirés quand un block est appliqué | `SeveredFollows` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Relation` | racine d'agrégat | Cohérence d'arête à travers les index avant + inverse |
| `FollowEdge` / `BlockEdge` | VO | Les deux types de relation dirigée |
| `RelationStatus` / `RelationKind` | enum | Vocabulaires de relation fermés |
| `AuthorTier` / `TierThresholds` | VO | Dérivation du tier depuis le nombre de followers |
| `SeveredFollows` | VO | Les follows qu'un block démantèle |

**Transitions de relation :**

```
(none) --(follow)--> following --(unfollow)--> (none)
(none) --(suivre un profil privé)--> requested --(le propriétaire approuve)--> following
                                    requested --(refus / annulation / blocage)--> (none)
(none) --(block)--> blocked (sectionne les follows existants des deux côtés)
```

> **Transitions légales uniquement.** Un block sectionne les follows existants (`SeveredFollows`) ;
> les index avant et inverse sont écrits atomiquement (logged batch) ; un nombre de followers
> franchissant un seuil change l'`AuthorTier`.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Les relations (follows, blocks) et leurs index inverses — **ScyllaDB** (4 tables) + **Redis** (Sets de relations chaudes). Aucun autre service n'écrit les relations.

**Ce contexte détient des copies qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Existence de profil | `profile` | `profile.v1.events` | cohérence à terme |

**La liste « ne-pas-écrire » :** social-graph ne construit jamais de feeds et n'écrit jamais la
présentation de profil (il calcule le tier ; `profile` le possède + émet).

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Les index de relation avant + inverse sont écrits atomiquement | application (logged batch) | `SGR-1xxx` |
| I2 | Un block sectionne les follows existants des deux directions | domaine | `SGR-1xxx` |
| I3 | Le tier d'auteur est dérivé du nombre de followers franchissant `TierThresholds` | domaine | — |
| I4 | Les lectures de relations chaudes sont servies depuis les Redis Sets, reconstructibles depuis Scylla | infrastructure | `SGR-1xxx` |
| I5 | Suivre un profil privé crée une demande en attente, jamais un follow : aucun accès, absent des listes, tant que le propriétaire n'a pas approuvé (un seul batch journalisé : lignes de demande retirées, lignes de follow ajoutées) ; un blocage supprime les demandes dans les deux sens | domaine + application | `SGR-1005` / `SGR-1006` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Follow / unfollow / block.** Muter l'agrégat `Relation` → écriture logged-batch atomique des lignes
Scylla avant + inverse + mise à jour du hot-set Redis. Un block émet `SeveredFollows`.

**Calcul du tier.** Un changement de nombre de followers franchissant une frontière `TierThresholds`
produit `AuthorTierChanged`, alimentant le flux profile→tier (initiative author-tier ; côté
producteur cadré).

**Lectures.** `timeline` lit l'ensemble des followers via gRPC pour le fan-out ; `counter` réconcilie
les comptes de followers via gRPC (le stream Kafka `social-graph.follows` est un producteur différé).

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `profile` | amont | ACL | `profile.v1.events` | validité des relations vs profils inconnus |
| `timeline` | aval | Customer/Supplier (gRPC) | lectures de l'ensemble des followers pour le fan-out | le fan-out du fil casse |
| `counter` | aval | Customer/Supplier (gRPC) | réconciliation du nombre de followers | les magnitudes de followers dérivent |
| `profile` | aval | Published Language | flux de changement de tier | l'émission du tier d'auteur casse |

> **Anti-Corruption Layer :** le consumer d'événements `profile` garde la validité des relations
> alignée avec l'existence des profils.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `ProfileFollowed` / `ProfileUnfollowed` | une arête de follow a été créée/retirée | follow/unfollow commite | timeline/counter (consommateurs ; câblage du stream de follows différé) |
| `ProfileBlocked` / `ProfileUnblocked` | une arête de block a changé (sectionne les follows) | block/unblock commite | fils |
| `AuthorTierChanged` | le tier de l'auteur a changé | le nombre de followers franchit un seuil | `profile` (possède + ré-émet) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Schéma ScyllaDB 4 tables + Redis hot Sets + atomicité logged-batch pour la double-écriture | [`ADR-0016`](../../../../docs/adr/0016-social-graph-four-table-scylla-logged-batch.md) | Accepté |
| Tier d'auteur : social-graph calcule → profile possède + émet | _ouvert — initiative author-tier_ | Cadré |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Core — le graphe de relations est le réseau social.
- **Volatilité :** faible-à-moyenne — les types de relation sont stables ; la politique de tier peut se régler.
- **Dette de modélisation connue :** réconciliation des orphelins (TD-6) ; le producteur Kafka `social-graph.follows` est différé (counter consomme via gRPC pour l'instant).
- **Capacités différées :** traversées de recommandation type NebulaGraph ; requêtes mutuelles/second-degré.

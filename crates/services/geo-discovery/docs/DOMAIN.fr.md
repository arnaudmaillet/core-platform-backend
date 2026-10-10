---
i18n:
  source: ./DOMAIN.md
  source_sha256: b46e8c3635c963f067ae86ba9236a099db7c137b2811ba81ffaee0d49cc8fb1a
  translated_at: 2026-10-10
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `geo-discovery` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Geo Discovery — découverte spatiale de posts sur une carte |
> | **Classe de sous-domaine** | **Supporting** — un read-model spatial dérivé ; surface produit distinctive mais ne possède aucune vérité |
> | **System of …** | **Reference (SoRef)** — un index spatial requêtable sur les posts, reconstructible depuis l'amont |
> | **Racine(s) d'agrégat** | `MapPostCard` (projection Focus) + `RadarPin` (projection Radar), clé par `H3Index` |
> | **Tier** | **TIER-1** |
> | **Posture de défaillance** | **Fail-open** — un index dégradé retourne moins/des pins plus périmés, jamais une erreur |
> | **Contextes amont** | `post` (posts publiés avec localisation), `engagement` (viralité), `profile`/`social-graph` (tier d'auteur) — via **ACL** |
> | **Contextes aval** | clients (requêtes de viewport carte) ; ne publie rien de référence |
> | **Journal de décisions** | [`ADR-0010`](../../../../docs/adr/0010-geo-discovery-h3-grid-dual-layer-redis-topk.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `geo-discovery` est l'autorité pour **la découverte spatiale** : il répond à
**« quels sont les posts les plus pertinents visibles dans ce viewport de carte en ce moment ? »**

**Le problème difficile.** Servir des requêtes de viewport à latence interactive sur une population de
posts en changement constant — un **viewport H3 `grid_disk`** mappé vers une structure Redis
double-couche (ZSET + cardinalité), avec des scripts Lua Top-K / XX / prune et une rétention TTL pour
que l'index s'auto-élague.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Posséder les posts → `post` est le SoR ; geo détient une projection spatiale.
- ❌ Calculer les scores d'engagement → les consomme depuis `engagement`.
- ❌ Posséder le tier d'auteur → consomme `profile.tier_changed`.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Radar pin | Le marqueur de carte léger (id + coordonnées + miniature) pour le chemin panoramique | `RadarPin` |
| Map post card | Le résumé projeté, rendable sur carte, d'un post pour le chemin focus | `MapPostCard` |
| H3 index / resolution | L'id de cellule spatiale hexagonale et sa résolution | `H3Index`, `H3Resolution` |
| Geo coordinate | Un point lat/lng | `GeoCoordinate` |
| Virality score | Le poids de classement dérivé de l'engagement | `ViralityScore` |
| Author tier | Le tier de l'auteur (affecte classement/visibilité) | `AuthorTier` |
| Retention TTL | Combien de temps une carte reste dans l'index spatial | `RetentionTtl` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `RadarPin` | projection (agrégat) | Le marqueur de carte léger pour le chemin panoramique |
| `MapPostCard` | projection (agrégat) | Le résumé de post hydraté pour le chemin focus |
| `H3Index` / `H3Resolution` | VO | Identité de cellule spatiale + granularité de zoom |
| `GeoCoordinate` | VO | lat/lng valides à la construction |
| `ViralityScore` / `AuthorTier` | VO/enum | Entrées de classement |
| `RetentionTtl` | VO | Durée de vie auto-élaguante |

> **Invariant.** Une carte vit dans exactement la/les cellule(s) H3 de sa coordonnée ; le classement
> dans une cellule est Top-K par viralité, élagué et TTL'd.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité (de *référence*) pour :**
- L'index spatial — **Redis** (double-couche ZSET + cardinalité) + **ScyllaDB** (`map_post_cards`). Reconstructible depuis les événements amont.

**Ce contexte détient des copies dérivées qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Contenu/localisation du post | `post` | `post.v1.events` / `post.deleted` | cohérence à terme |
| Viralité | `engagement` | `engagement.score_updated` | cohérence à terme |
| Tier d'auteur | `profile` | `profile.tier_changed` | cohérence à terme |

**La liste « ne-pas-écrire » :** geo ne mute jamais les posts, scores ou tiers — il les indexe.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Une carte est indexée dans la bonne cellule H3 pour sa coordonnée | domaine | `GEO-1xxx` |
| I2 | Dans une cellule, les résultats sont Top-K par viralité, élagués + TTL'd | domaine (Lua) | `GEO-2xxx` |
| I3 | Les requêtes de viewport échouent ouvertes (dégradent, jamais d'erreur) | application | `GEO-1xxx` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Maintenance de l'index.** Consommer `PostPublished` (ajouter carte), `post.deleted` (retirer),
`engagement.score_updated` (re-classer), `profile.tier_changed` (re-pondérer) → mettre à jour le ZSET
Redis double-couche via Lua Top-K/XX/prune ; le TTL gère la rétention.

**Requête de viewport (Radar).** Un viewport de carte → `H3 grid_disk` des cellules couvrantes →
fusionner Top-K par cellule → retourner des `RadarPin` légers (id + coordonnées + miniature) depuis
Redis seul. Un index dégradé retourne moins/des pins plus périmés (fail-open).

**Focus de pin (Focus).** Au tap, un lot de `post_id` → `GetGeoTimeline` → `MapPostCard` entièrement
hydratées (légende, métadonnées auteur, palier) depuis Redis avec repli ScyllaDB — la lecture à froid que
le chemin Radar évite délibérément.

> **Contrat de payload (résolu).** `PostPublished` porte désormais `lat`/`lng`, `caption` et
> `thumbnail_url` (localisation fournie par le client au `CreatePost`) ; les posts sans localisation ne
> sont simplement pas géo-indexés. `author_handle` / `author_avatar_url` sont réservés sur la carte et
> complétés depuis `profile.v1.events` (jointure séparée).

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `post` | amont | ACL | `post.v1.events` / `post.deleted` | les cartes cessent d'apparaître/de se purger |
| `engagement` | amont | ACL | `engagement.score_updated` | le classement devient périmé |
| `profile` | amont | ACL | `profile.tier_changed` | la pondération par tier casse |
| clients | aval | OHS | requête gRPC de viewport | la découverte sur carte casse |

> **Anti-Corruption Layer :** les consommateurs traduisent chaque événement amont en mises à jour de `MapPostCard`.

---

## 8. Événements de Domaine (sémantique, pas wire)

> Ne publie **rien de référence** — c'est un read-model. Il consomme les faits de `post` /
> `engagement` / `profile` ; leurs sens sont possédés par ces contextes.

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Viewport H3 grid_disk + index spatial Top-K Redis double-couche (ZSET+cardinalité) | [`ADR-0010`](../../../../docs/adr/0010-geo-discovery-h3-grid-dual-layer-redis-topk.md) | Accepté |
| Séparation de lecture Radar/Focus : `RadarPin` léger (panoramique Redis seul) vs `MapPostCard` hydratée (`GetGeoTimeline` au tap) | _inline — ce changement_ | Accepté |
| Enrichissement de payload post→geo : `PostPublished` porte lat/lng + caption + miniature (localisation fournie par le client au `CreatePost`) | _résolu — ce changement_ | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — une projection spatiale distinctive mais dérivée.
- **Volatilité :** moyenne — les entrées de classement évoluent.
- **Dette de modélisation connue :** `author_handle` / `author_avatar_url` attendent la jointure
  `profile.v1.events` (réservés sur la carte, vides jusque-là).
- **Capacités différées :** requêtes spatiales plus riches ; clustering ; heatmaps.

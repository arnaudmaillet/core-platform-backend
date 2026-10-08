---
i18n:
  source: ./DOMAIN.md
  source_sha256: 6f7342254ed096b27a9e4722be94c0905836b7e502d9b6fcdced176aba3a1d92
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `counter` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Counter / Analytics — les magnitudes (« combien ? ») |
> | **Classe de sous-domaine** | **Supporting** — un plan de mesure dérivé ; précieux mais pas l'origine de la valeur produit |
> | **System of …** | **Reference (SoRef)** pour comptes/cardinalités/tendances — exact-mais-réconciliable, jamais l'état d'arête (« qui ? ») |
> | **Racine(s) d'agrégat** | `Metric` + `WindowAggregator` (`domain`) |
> | **Tier** | **TIER-1** |
> | **Posture de défaillance** | **Fail-open** — une lecture dégrade vers une magnitude périmée/approximative, jamais une erreur sur le hot path |
> | **Contextes amont** | producteurs view/impression/click, `wallet` (mises : likes, #665) — via **ACL** sur Kafka |
> | **Contextes aval** | `search` (PopularityScore), `realtime` (broadcast) — via **Published Language** (`counter.v1.popularity`) |
> | **Journal de décisions** | [`ADR-0008`](../../../../docs/adr/0008-counter-magnitudes-are-a-reconcilable-soref.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `counter` est l'autorité pour **les magnitudes** : il répond à
**« combien de vues / likes / followers / rang de tendance cette entité a-t-elle ? »** — jamais *qui*
a fait quoi (c'est l'état d'arête, possédé ailleurs).

**Le problème difficile.** Compter un firehose au volume d'écriture sans perdre en exactitude ni faire
fondre le hot path : ingestion pure-Kafka avec pré-agrégation fenêtrée N→1, compteurs shardés à deux
étages, un store 3-tiers (Redis hot / Postgres warm-SoRef + réconciliation / Scylla TWCS cold), HLL
pour les vues uniques et CMS pour les tendances, exact-mais-réconciliable pour likes/follows.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Détenir l'état d'arête (qui a liké/suivi qui) → `engagement` / `social-graph`.
- ❌ Être un System of Record → c'est un System of *Reference* réconciliable.
- ❌ Servir de l'analytics d'événements bruts → il sert des magnitudes agrégées.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Metric | Une quantité comptée avec un kind + une agrégation | `Metric`, `MetricKind`, `Aggregation` |
| Window | Le pivot d'idempotence — un intervalle d'agrégation borné | `WindowId`, `WindowKey`, `WindowSize`, `WindowAggregator` |
| Observation / delta | Un signal unique / le changement plié à appliquer | `Observation`, `WindowDelta` |
| Cardinality | Compte unique approximatif (HLL) | `Cardinality` |
| Popularity score | La magnitude de popularité publiée + poids | `PopularityScore`, `PopularityWeights` |
| Trending | Items classés adossés au CMS dans un scope | `TrendingItem`, `TrendingScope`, `TrendingQuery` |
| Time-series bucket | Un bucket temporel TWCS du tier cold | `TimeSeriesBucket`, `TimeGranularity` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Metric` | racine d'agrégat | `MetricKind` (exact/approx) × `Aggregation` (sum/cardinality) orthogonaux |
| `WindowAggregator` | racine d'agrégat | Pli N→1 ; `WindowId` rend les flushes idempotents |
| `CounterValue` / `CountSnapshot` / `Cardinality` | VO | Une magnitude / un instantané ponctuel / une estimation HLL |
| `PopularityScore` / `PopularityWeights` | VO | Le signal de popularité publié |
| `EntityRef` / `EntityId` / `EntityKind` | VO | Ce qui est compté |

> **`WindowId` est le pivot.** Il garde tous les effets de bord multi-tiers (le ledger de deltas),
> donc un flush rejoué ne peut double-compter.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité (de *référence*) pour :**
- Les magnitudes — **Redis** (hot) + **Postgres** (warm-SoRef + ledger de réconciliation) + **ScyllaDB** (cold TWCS séries temporelles). Réconciliable face aux SoR d'arête propriétaires.

**Ce contexte détient des copies dérivées qu'il ne possède PAS :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Vérité follower/following | `social-graph` | source de réconciliation (gRPC) | réconcilié périodiquement (dérive → `CTR-5002`) |
| Likes (mises) | `wallet` | `wallet.v1.events` | cohérence à terme |

**La liste « ne-pas-écrire » :** counter ne possède jamais le *qui* — il dérive les magnitudes et
réconcilie face aux SoR d'arête ; il supersède seulement les comptes *bruts* de vues/partages
d'engagement.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Un flush est idempotent par `WindowId` (pas de double-compte au rejeu) | domaine + CTE ledger Pg | `CTR-2xxx` |
| I2 | Les magnitudes ne revendiquent jamais une identité d'arête (« qui ») | domaine | — |
| I3 | Les lectures hot échouent ouvertes (timeout dur → périmé/approx) | application | `CTR-4xxx` |
| I4 | Les valeurs de référence se réconcilient au SoR propriétaire ; la dérive alarme | application | `CTR-5002` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Ingest → agréger → flush.** Les signaux affluent (`view.v1.events`, `impression.v1.events`,
`click.v1.events`, engagement) → `WindowAggregator` plie N→1 → `DeltaFlusher` écrit idempotemment
(gardé par le ledger `WindowId`) à travers les 3 tiers → `PopularityPublisher` émet
`counter.v1.popularity`.

**Lecture (fail-open).** Les lectures hot frappent Redis avec un `tokio::timeout` dur ; en cas de
miss/timeout, retourner une valeur périmée/approximative plutôt qu'une erreur.

**Réconcilier.** Un `Reconciler` répare périodiquement les magnitudes face au SoR propriétaire
(`set_total`/overwrite) ; la divergence lève `CTR-5002`.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| producteurs view/impression/click | amont | ACL | `*.v1.events` | les comptes cessent d'avancer |
| `wallet` | amont | ACL | `wallet.v1.events` (`stake_committed`) | les magnitudes de likes cassent |
| `social-graph` | source de réconciliation | Customer/Supplier | gRPC follower/following | la réconciliation du compte de followers casse |
| `search` | aval | Published Language | `counter.v1.popularity` | le PopularityScore de search devient périmé |
| `realtime` | aval | Published Language | `counter.v1.popularity` (broadcast) | les compteurs live s'arrêtent |

> **Anti-Corruption Layer :** la couche `decode` pure mappe chaque forme wire de signal amont vers
> `Observation`.

---

## 8. Événements de Domaine (sémantique, pas wire)

| Événement | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `counter.v1.popularity` | la magnitude de popularité d'une entité a changé | un flush de fenêtre met à jour un score de popularité | `search` (classement), `realtime` (broadcast live) |

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Les magnitudes (« combien ») sont un SoRef réconciliable séparé, distinct de l'état d'arête (« qui ») ; supersède les comptes bruts d'engagement | [`ADR-0008`](../../../../docs/adr/0008-counter-magnitudes-are-a-reconcilable-soref.md) | Accepté |
| Flush idempotent gardé par `WindowId` à travers un store 3-tiers | [`ADR-0008`](../../../../docs/adr/0008-counter-magnitudes-are-a-reconcilable-soref.md) | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Supporting — un plan de mesure/référence dérivé des SoR d'arête.
- **Volatilité :** moyenne — les nouveaux types de métrique et producteurs sont additifs.
- **Dette de modélisation connue :** la réconciliation like/share/comment attend un RPC de compte de likes (engagement).
- **Capacités différées :** producteurs amont view/impression/click ; le stream `social-graph.follows` ; producteur de shard-fan-out.

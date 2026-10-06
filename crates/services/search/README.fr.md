---
i18n:
  source: ./README.md
  source_sha256: 9bb77e9980ef6fb079e3e3b1a2f3eb7f2415cb4b84d9e6d9627119818012168e
  translated_at: 2026-10-06
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `search` — Trouver profils, posts et hashtags à l'échelle du réseau en dizaines de millisecondes, sans jamais détenir la vérité

> **Fiche service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Équipe** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **Astreinte / escalade** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — surface de découverte très visible, mais **dérivée et fail-open** : hors de tout chemin d'écriture synchrone ; une panne dégrade la découverte, elle ne bloque jamais une écriture/publication/connexion |
> | **Déployable** | `crates/apps/search-server` (crate bibliothèque : `crates/services/search`) |
> | **Stockages** | cluster OpenSearch — index `profiles`, `posts`, `hashtags` (chacun derrière des alias lecture/écriture). **Aucun** Postgres / Scylla / Redis propre |
> | **Async** | ne publie **rien** · consomme `post.v1.events`, `profile.v1.events`, `moderation.v1.events` (+ un flux hashtag dérivé) (Kafka) |
> | **Appelants amont** | gateway / BFF (le chemin de requête `Search` / `Suggest`) |
> | **Dépendances aval** | OpenSearch, Kafka. Les systèmes de référence appartiennent à `post` / `profile` — search n'appelle **aucun** service sur le chemin de requête |
> | **SLO** | `<TODO>` dispo · `Search` p99 `< <TODO ~150> ms` · latence d'ingestion `< <TODO ~5> s` |

---

## 🎯 Vue d'ensemble & rôle du service

`search` est le **modèle de lecture de découverte** de la plateforme : un index inversé dérivé et tolérant aux fautes de frappe sur les profils, posts et hashtags. Il détient l'*appariement, la tolérance aux fautes et le classement* ; il ne détient **rien d'autoritatif**. Chaque octet de son index est une copie jetable, reconstructible à tout moment depuis les services sources — c'est un système de *référence*, jamais un système de *vérité*.

Le problème difficile qu'il résout est de **servir la découverte plein-texte au volume de lecture du réseau sans coupler la découverte au chemin d'écriture ni à aucun service source**. Une conception naïve interroge les services de contenu à chaque recherche et indexe de façon synchrone à chaque écriture ; cela couple la latence de recherche à N services et taxe chaque publication. Le motif qui résout cela est la **séparation CQRS** la plus nette de la flotte : le côté commande est **100 % de consommateurs Kafka asynchrones** (il n'y a aucun RPC d'écriture), et le côté requête est une **lecture gRPC sans état** qui ne touche que le moteur.

**Objectifs fondamentaux :** (1) l'indexation est **asynchrone et hors du chemin d'écriture** — publier un post n'attend jamais search ; (2) le chemin de requête est **autonome** — aucun appel inter-service, les résultats sont des références que l'appelant hydrate ; (3) l'index est **entièrement reconstructible** depuis le système de référence + les événements (le réindexage est une opération de premier ordre) ; (4) la posture est **fail-open** — une panne dégrade vers des résultats vides/partiels, jamais un blocage amont.

| Préoccupation | Chemin | Contrat de latence | Notes |
|---|---|---|---|
| **Ingestion** | consommateurs Kafka async (`run_consumer`) | nulle (hors du chemin d'écriture) | événement → indexé en secondes ; la latence est un SLO, pas une exigence de cohérence |
| **Requête** | gRPC synchrone, moteur seul | p99 bornée | renvoie des références classées + projection d'affichage minimale ; aucun fan-out |
| **Réindexage** | hors-ligne / blue-green via alias | n/a | reconstruction depuis le système de référence ; bascule d'alias atomique, sans interruption |

---

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS là où c'est pertinent, **OpenSearch** comme unique moteur canonique, Kafka pour l'ingestion. Il n'y a pas de second stockage — search ne détient aucune vérité à persister.

```
 post-service       ── post.v1.events ──┐
 profile-service    ── profile.v1.events ──┤
                                           ├─► [run_consumer · une boucle par topic] ─► Projector ─► SearchIndex ─► OpenSearch
 moderation-service ── moderation.v1.events ──┤      (commit manuel, retry, DLQ)      (xform pure)   (port)        (alias :
 (hashtags dérivés) ── depuis post events ──┘                                                                      posts-write/read)
                                                                                                                      ▲
   gateway / BFF ──► search.v1.SearchService/Search ──────────── requête sans état (moteur seul, sans fan-out) ─────┘
                          renvoie (entity_type, id, score, snippet) + projection d'affichage minimale
```

**La correction face au désordre est poussée dans le moteur.** Chaque document d'index porte un `doc_version` monotone dérivé de la version/`updated_at` de l'entité source ; les écritures utilisent OpenSearch **`version_type=external`**, de sorte qu'un événement périmé, rejoué ou désordonné ne peut jamais écraser un document plus récent — sans verrou, sans lecture-modification-écriture dans le consommateur. C'est l'analogue, côté search, de la version d'enforcement monotone par sujet de `moderation`.

> **Invariants** (et où ils sont garantis) :
> - **Search ne détient aucune vérité.** Les résultats sont des références `(entity_type, id, score, snippet)` + une projection d'affichage minimale indexée ; l'appelant hydrate les champs vivants/autoritatifs. Test décisif : tout champ indexé doit être reconstructible en rejouant les événements ou en scannant le système de référence — domaine + projector.
> - **Le versionnement externe est le mécanisme d'idempotence.** Les événements désordonnés/redélivrés sont résolus par la garde de version du moteur, pas par un état côté consommateur — frontière infrastructure.
> - **La visibilité de modération est un signal de premier ordre.** Un `EnforcementApplied` de `moderation` (RemoveContent/VisibilityLimit) bascule `searchable=false` (document conservé, car la modération est réversible) ; `EnforcementReversed` le rétablit — couche application.
> - **Le blocage/sourdine personnel n'est PAS indexé.** Un index inversé partagé ne peut intégrer des exclusions par-spectateur ; le blocage/sourdine est un filtre par-requête appliqué en bordure — contrat de frontière.
> - **L'effacement RGPD est une purge profonde.** La purge d'un acteur exécute `delete_by_query` sur `author_id` sur tous les index, sans pierre tombale conservée (l'index est une copie de PII indexable) — frontière infrastructure.

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (non-5xx / non-`UNAVAILABLE`) | `<TODO 99.9%>` | 30j glissants | `<metric>` |
| Latence de requête p99 (`Search`) | `< <TODO 150> ms` | 1h | `<metric>` |
| Latence d'ingestion (événement → indexé) | `< <TODO 5> s` | live | latence du `<consumer-group>` |
| Débit de réindexage | `<TODO docs/s>` | par job | métrique du job de réindexage |

**Budget d'erreur :** `<TODO>`. **En cas d'épuisement :** `<gel du déploiement | page>`. Note : comme search est fail-open, l'objectif de *disponibilité* couvre la dégradation du chemin de requête, pas la correction des données — celle-ci est couverte par la latence d'ingestion + la suite de requêtes-témoins de pertinence.

---

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

**Aval — ce dont `search` a besoin pour fonctionner :**

| Dépendance | Rôle | Si en panne → | Dégradation |
|---|---|---|---|
| OpenSearch | l'index (appariement/classement/stockage) | les requêtes échouent, l'ingestion s'arrête | **Souple** — la requête renvoie vide/partiel (fail-open) ; l'ingestion reprend au dernier offset commité |
| Kafka | flux d'ingestion | l'indexation cesse d'avancer | **Souple** — l'index devient périmé, la latence croît ; aucune perte (commit manuel) |

**Amont — qui dépend de `search` (votre rayon d'impact si VOUS tombez) :**

| Appelant | Utilise | Impact visible si `search` est en panne |
|---|---|---|
| gateway / BFF | `Search` / `Suggest` | la barre de recherche dégrade vers des résultats vides ou périmés ; **aucune** écriture, publication ou connexion n'est affectée |

> **Chemin critique ?** **Non** — dérivé, asynchrone, fail-open. Search n'est jamais sur le chemin synchrone d'une écriture, d'une publication ou d'un flux d'authentification.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### gRPC — `search.v1.SearchService` *(Phase 1)*

La surface synchrone est délibérément en **lecture seule** : `Search` (fédérée, filtrable par type d'entité, paginée par curseur), `Suggest`/autocomplétion (préfixe), et `MultiSearch` (fan-out + fusion). **Il n'y a aucun RPC d'écriture/d'indexation** — l'ingestion est exclusivement Kafka — et search **ne publie aucun événement** (c'est un modèle de lecture terminal).

**Les recherches récentes (#663)** sont la seule chose que search garde *pour* un profil, afin que chaque appareil affiche la même liste :
- `RecordRecentSearch` — l'app l'appelle quand une recherche est validée ou un résultat touché, pas à chaque frappe.
- `ListRecentSearches`, `DeleteRecentSearch` (insensible à la casse), `ClearSearchHistory`.

Les quatre RPC sont exposés en périphérie, `authenticated` + `require_profile`, et chacun répond la liste telle qu'elle est : la plus récente d'abord, 50 au plus, aucune de plus de 90 jours. Une requête est rognée, ses espaces fusionnés, coupée à 100 caractères, et une recherche identique plus ancienne, quelle que soit sa casse, remonte plutôt que de se répéter.

Le stockage tient en un document par profil dans `<prefix>-recent-searches` (non indexé, jamais cherché), tenu par un upsert scripté painless. L'historique de visionnage reste sur l'appareil (aucun flux de vues n'existe).

> **Contrat de fil :** les résultats sont des références — `(entity_type, id, score, highlight/snippet)` plus les champs d'affichage minimaux indexés (handle, nom affiché, clé de miniature, `author_id`, `created_at`). Les appelants DOIVENT hydrater les champs volatils/autoritatifs (compteurs vivants, URLs média signées, état de suivi, bio courante) depuis `post`/`profile`. Search ne renvoie aucune entité autoritative.

### Ports Rust (contrat hexagonal) *(Phase 3)*

```rust
#[async_trait] pub trait SearchIndex { /* upsert(doc, version) · delete(id) · set_visibility(id, bool) · delete_by_query(author_id) · query(SearchQuery) */ }
#[async_trait] pub trait IndexAdmin { /* create-index · alias · reindex — la surface ops de Phase 7 */ }
```

### Contrat d'erreur

Chaque faute implémente `error::AppError` avec un code `SCH-XXXX` stable, mappé vers gRPC `Status` / HTTP par le crate partagé `error` :

| Plage | Classe |
|---|---|
| `SCH-1xxx` | requête / parse |
| `SCH-2xxx` | index / upsert (dont `SCH-2002` saut de version périmée) |
| `SCH-3xxx` | projection / transformation |
| `SCH-4xxx` | disponibilité du moteur (cœur fail-open ; retryable) |
| `SCH-5xxx` | réindexage / alias / migration |
| `SCH-8xxx` | décodage d'événement entrant / mapping source |
| `SCH-9xxx` | transverse (domaine/parse, consommation d'événements) |

---

## 📨 Événements & contrat asynchrone &nbsp;·&nbsp; CORE

> Les topics Kafka sont une API. Un changement de schéma dans un topic consommé casse l'indexation exactement comme un changement de proto.

**Publie :** rien. Search est un modèle de lecture terminal.

**Consomme :**

| Topic | Groupe de consommateurs | Rôle | À l'épuisement/poison |
|---|---|---|---|
| `post.v1.events` | `search-post-indexer` | indexe/met à jour/supprime les posts (contenu hydraté via `GetPost`) | DLQ `post.v1.events.dlq` |
| `profile.v1.events` | `search-profile-indexer` | indexe/met à jour/supprime les profils (contenu hydraté via `GetProfileById`) ; masquage par le propriétaire → drapeau de visibilité **owner** ; `ProfileTabSettingsChanged` → la fenêtre de posts de l'auteur | DLQ `profile.v1.events.dlq` |
| `moderation.v1.events` | `search-moderation-indexer` | bascule le drapeau de visibilité **moderation** au masquage ; rétablit à la réversion et, pour un **post**, le réhydrate (`GetPost`, version de contenu = son `updated_at`), pour qu'un post écarté pendant le retrait — ou restauré sous le retrait et jamais réannoncé (#663) — revienne. Tant que `post` le lit encore retiré/limité : `SCH-8004`, réessayé | DLQ `moderation.v1.events.dlq` |
| `<hashtag stream>` | `search-post-indexer` | maintient l'index hashtag (dérivé des événements post) | DLQ `<...>.dlq` |

> **Deux autorités de visibilité :** un document n'est recherchable que si **les deux** drapeaux l'autorisent — `searchable = moderation_searchable AND owner_searchable`. Ce sont deux champs indépendants, chacun avec sa propre garde de version, écrits par des flux différents (`moderation.v1.events` vs un événement de masquage par le propriétaire du profil). Aucune autorité ne peut surpasser l'autre : un propriétaire de profil qui rétablit sa propre visibilité ne peut pas lever un masquage de modération, et inversement. Une **troisième autorité**, `discoverable` (#661), porte le choix du profil d'être trouvable par la recherche (`by_handle_search` de `ProfileDiscoverySettingsChanged`) ; le masquage et la modération n'y touchent pas. Ses deux champs ont été ajoutés aux index existants sur place : `ensure_indices` pose les propriétés communes à chaque démarrage, une mise à jour `_mapping` additive (sans réindexation). Un drapeau absent vaut visible. Un post **soumis à une limite d'âge** (`age_gate` de la modération) est hors de la recherche pour tous : search n'a pas de niveau de contenu par lecteur, il tient donc le contenu mature à l'écart des 13–17 ans et des invités de façon prudente (RESTRICTED par défaut, comme les fils de découverte).

> **Fenêtre d'historique des posts (#664) :** un post que l'auteur ne montre plus aux visiteurs reste indexé mais sort de la recherche à `visible_until` (`must_not range visible_until <= now` ; pas de champ, pas de fenêtre). L'hydratation le prend dans le `GetPost` mesh de post (`visible_until_ms` : `created_at` + la fenêtre), si bien qu'un post sort à l'heure dite sans événement. Un changement de fenêtre (`ProfileTabSettingsChanged.post_window` : `all` / `six_months` 183 j / `one_month` 30 j / `three_days` 3 j, les mêmes jours que post) le recalcule pour tous les posts de l'auteur par un `_update_by_query` gardé par `window_version` (heure de l'événement), si bien qu'élargir la fenêtre ramène les posts sans réindexation. Un `post_window` inconnu part en DLQ pour être rejoué. Un post publié pendant que la fenêtre change peut garder l'ancienne fin jusqu'à sa prochaine modification (l'hydratation et le changement se croisent).

> **Contrat d'exécution (obligatoire) :** tous les consommateurs s'exécutent sous `run_consumer` — commit manuel après une issue terminale, retry borné avec backoff + jitter, DLQ à l'épuisement/poison, reconstruction depuis le dernier offset commité sur erreur broker. **Idempotence :** la garde de version externe du moteur (`version_type=external`) ; les suppressions sont idempotentes par nature ; une écriture de version périmée (`SCH-2002`) et un type d'événement inconnu sont repliés en `Ok` pour que l'offset soit tout de même commité. Une boucle `run_consumer` par topic source (la logique dépend du topic).

---

## 🌩️ Modes de défaillance & dégradation &nbsp;·&nbsp; OPS

| Défaillance | Symptôme | Comportement du service | Action opérateur |
|---|---|---|---|
| OpenSearch indisponible (requête) | erreurs / latence `Search` | **fail-open** — renvoie vide/partiel, jamais de 5xx sur la page | vérifier la santé du cluster ; le circuit-breaker garde le chemin de requête réactif |
| OpenSearch indisponible (ingestion) | la latence d'ingestion monte | le consommateur retente dans son budget, puis DLQ ; offset non commité → aucune perte | restaurer le cluster ; le consommateur reprend au dernier offset commité |
| Événement désordonné / rejoué | — | le versionnement externe rejette l'écriture périmée (`SCH-2002`, replié en `Ok`) | aucune (par conception) |
| Changement de mapping/analyzer requis | — | réindexage blue-green dans un nouvel index physique + bascule d'alias atomique | lancer le job de réindexage (Phase 7) |
| Index/alias manquant | `SCH-4003 IndexNotFound` | faute dure (écart de déploiement/migration), non retentée | appliquer la migration de mapping d'index avant le déploiement |

**Contre-pression & limites :** plafonds de taille de page + pagination par curseur ; un circuit-breaker / timeout dur sur le chemin de requête pour qu'un moteur lent déleste plutôt que de mettre en file ; l'ingestion est naturellement limitée par le débit du consommateur.

---

## 📦 Intégration & usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
search = { path = "crates/services/search" }
```

Bibliothèque uniquement. Implémentera [`service_runtime::Service`](../../platform/service-runtime/README.md) en tant que `search::service::SearchService` (Phase 5) — `build` câble l'adaptateur OpenSearch **et lance les consommateurs d'ingestion** (une boucle `run_consumer` supervisée par topic source), `register` ajoute les services gRPC, `health_probes` ping le moteur. Télémétrie, config + rechargement à chaud, limitation de débit en entrée, santé et arrêt gracieux sont gérés par le runtime.

### Bootstrap (`crates/apps/search-server`)

```rust
use search::service::SearchService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("SEARCH_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50062".to_owned()).parse()?;
    service_runtime::serve::<SearchService>(addr).await
}
```

> **État de build :** complet jusqu'à la Phase 7 (8 phases : scaffold → proto → domaine → application+ports → adaptateur OpenSearch+décodage → serveur+consommateurs → IT live → durcissement). La suite d'intégration live est derrière le feature `integration-search`. L'ingestion post, **profil** et modération est entièrement câblée (le contenu post + profil est hydraté via `GetPost` / `GetProfileById`).
>
> **Résultats selon le lecteur.** Pour tout appelant hors mesh, une page de résultats est filtrée par le `CheckAccess` de social-graph sur ses auteurs (un appel groupé, découpé selon les plafonds de la RPC) : un résultat **post** ne reste que si son auteur est `VISIBLE` pour le lecteur (pas un auteur privé qu'il ne suit pas, pas de blocage dans un sens ou l'autre, pas masqué) ; un résultat **profil** reste sauf s'il est `HIDDEN` (un profil privé reste trouvable par son en-tête) ; les hashtags restent toujours. Les suggestions suivent la règle des profils ; les suggestions de posts sont retirées pour les clients (elles ne nomment pas d'auteur). Si le contrôle échoue, search reste disponible (fail-open) mais ne montre rien de non vérifié : résultats profils et posts retirés, hashtags gardés, `degraded = true`. Une page peut revenir plus courte ; le curseur ne change pas. `exclude_author_ids` reste une exclusion supplémentaire fournie par l'appelant (les exclusions personnelles ne sont jamais indexées).

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables spécifiques à `search` *(remplies par phase)*

| Variable | Requise | Défaut | Description |
|---|---|---|---|
| `SEARCH_GRPC_ADDR` | Non | `0.0.0.0:50062` | adresse d'écoute gRPC |
| `SEARCH_OPENSEARCH_URL` | Non | `http://localhost:9200` | endpoint OpenSearch |
| `SEARCH_INDEX_PREFIX` | Non | `search` | espace de noms index/alias (`<prefix>-profiles`, `-posts`, `-hashtags`) |
| `SEARCH_OPENSEARCH_USER` / `SEARCH_OPENSEARCH_PASSWORD` | Non | — | basic auth optionnelle (définir les deux) |
| `SEARCH_QUERY_TIMEOUT_MS` | Non | `800` | timeout dur par requête moteur ; à l'expiration la requête échoue **ouvert** (dégradée) |
| `SEARCH_POST_GRPC_ENDPOINT` | Non | `http://localhost:50056` | endpoint `post` pour l'hydrateur de contenu d'ingestion |
| `SEARCH_HYDRATE_RPC_TIMEOUT_MS` | Non | `5000` | deadline par requête des RPC d'hydratation — un appel suspendu bloquerait sinon la partition du consumer |
| `SEARCH_HYDRATE_CONNECT_TIMEOUT_MS` | Non | `2000` | deadline de connexion à l'ouverture des canaux `post` / `profile` |
| `SEARCH_SOCIAL_GRAPH_GRPC_ENDPOINT` | **Oui** (prod) | `http://localhost:50053` | endpoint `social-graph` du filtre d'audience du chemin de requête (`CheckAccess`) ; injoignable → les résultats clients se réduisent aux hashtags |
| `SEARCH_AUDIENCE_RPC_TIMEOUT_MS` / `SEARCH_AUDIENCE_CONNECT_TIMEOUT_MS` | Non | `500` / `500` | délais de cet appel (il s'exécute dans la requête de l'utilisateur) |

### Variables d'infrastructure héritées

| Variable | Requise | Défaut | Description |
|---|---|---|---|
| `KAFKA_BROKERS` | **Oui** | — | brokers du flux d'ingestion |

> La sémantique moteur est canonique sur **OpenSearch** (mono-nœud en dev/CI pour la parité, cluster en prod). Un adaptateur Meilisearch peut exister pour la vélocité locale mais n'est **pas** une cible de correction de pertinence.

### Fonctionnalités à la compilation
- `integration-search` — active la suite d'intégration live (OpenSearch mono-nœud) et la suite de pertinence par requêtes-témoins.
- `build.rs` compile `contracts/proto/search/v1/*.proto` et émet le descripteur de réflexion.

---

## 🚀 Déploiement, migrations & rollback &nbsp;·&nbsp; OPS

- **Les mappings/analyzers d'index sont des migrations.** Artefacts versionnés (`MAPPING_VERSION`) créés au démarrage par `IndexAdmin::ensure_indices`, appliqués **avant** que le nouveau binaire ne serve — l'analogue search des migrations SQL/CQL.
- **Le réindexage est de premier ordre** (`application::reindex::Reindexer`). Un changement de mapping/analyzer est un **basculement blue-green via alias** : créer un nouvel index physique, repointer l'alias d'**écriture** (les écritures live + le backfill atterrissent sur le nouvel index), remplir depuis le système de référence, puis repointer l'alias de **lecture** en dernier. Sans interruption — et le versionnement externe garantit qu'un document de backfill ne peut jamais écraser une écriture live plus récente.
- **Reconstruction depuis la vérité.** Comme la rétention Kafka est finie, l'index est reconstruit par un **backfill bi-source** (`BackfillSource`) qui scanne le système de référence `post`/`profile`, pas par un « rejeu depuis le début ». *(Le `BackfillSource` concret adossé à gRPC est un suivi différé — il nécessite les services live et, pour les profils, une capacité de scan/événements amont.)*
- **Rollback :** sûr — le binaire est sans état ; les alias d'index permettent de repointer instantanément vers l'index physique précédent.
- **Pièges d'état :** la config analyzer/tokenizer est de fait un schéma ; la changer impose un réindexage, jamais une édition en place.

---

## 📈 Télémétrie, performance & métriques &nbsp;·&nbsp; CORE

- **Runtime :** Tokio multi-threadé (consommateurs d'ingestion + handlers de requête). Souscripteur tracing/OTel global installé avant `serve` ; trace-context W3C propagé à travers la frontière Kafka.

| Signal | Pourquoi c'est important | Alerte suggérée |
|---|---|---|
| Latence d'ingestion (par groupe de consommateurs) | fraîcheur de l'index | `> SLO` soutenu ⇒ page |
| Latence p99 `Search` | réactivité de la découverte | `> SLO` ⇒ investiguer le moteur |
| Débit de production DLQ (`*.dlq`) | ingestion poison / retry épuisé | tout débit soutenu ⇒ page |
| Taux de réussite des requêtes-témoins | régressions de classement | tout échec ⇒ bloquer la release |

---

## 🛠️ Développement local &nbsp;·&nbsp; CORE

```bash
cargo build -p search && cargo clippy -p search --all-targets
cargo test  -p search                                  # run unitaire rapide, sans infra
docker compose up -d opensearch kafka                  # compose à la racine (Phase 6)
cargo test  -p search --features integration-search    # suite live (OpenSearch mono-nœud)
```

---

## 🚨 Dépannage & runbook &nbsp;·&nbsp; CORE

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réelle.

**1. La recherche renvoie des résultats vides/partiels.**
Cause racine : OpenSearch indisponible ou dégradé — search échoue **ouvert** par conception plutôt que d'erreurer la page. Mitigation : vérifier la santé du cluster ; le chemin de requête reste réactif via circuit-breaker ; les résultats reviennent quand le moteur revient.

**2. Le contenu nouveau/édité n'est pas indexé.**
Cause racine : latence d'ingestion ou consommateur bloqué. Mitigation : vérifier la latence du groupe de consommateurs et les topics `*.dlq` ; une erreur broker/moteur retient l'offset (aucune perte), donc le consommateur rattrape une fois la dépendance rétablie.

**3. Du contenu modéré/banni apparaît encore dans la recherche.**
Cause racine : le consommateur `moderation.v1.events` est en retard ou la bascule `searchable` a été mise en DLQ. Mitigation : vérifier la latence du consommateur d'événements de modération + la DLQ ; retraiter l'événement d'enforcement.

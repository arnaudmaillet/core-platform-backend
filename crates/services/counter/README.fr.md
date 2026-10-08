---
i18n:
  source: ./README.md
  source_sha256: ff76dc31c3105b3a1bbc7e7d4c5564089ad9cb0982e73da442e6de4669196c4e
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `counter` — Compter chaque vue, like, partage et abonnement à l'échelle du firehose, servir les totaux en sous-milliseconde, et ne détenir aucune vérité

> **Fiche service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Équipe** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **Astreinte / escalade** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-1** — surface d'engagement très visible, mais **dérivée et fail-open** : hors de tout chemin d'écriture synchrone ; une panne dégrade les compteurs en « périmé mais servi », elle ne bloque jamais un like/abonnement/publication |
> | **Déployable** | **deux** binaires — `crates/apps/counter-server` (chemin de lecture) **et** `crates/apps/counter-worker` (agrégateur de flux). Crate bibliothèque : `crates/services/counter` |
> | **Stockages** | **Redis** (compteurs chauds · HLL · CMS) · **Postgres** (totaux matérialisés tièdes + registre de réconciliation) · **ScyllaDB TWCS** (séries temporelles historiques froides). Ne détient aucune entité |
> | **Async** | publie `counter.v1.popularity` (signal de classement grossier) · consomme `view.v1.events`, `impression.v1.events`, `click.v1.events`, `wallet.v1.events` (likes, #665), les événements d'abonnement de social-graph (Kafka) |
> | **Appelants amont** | gateway / BFF, `timeline`, `search` (hydratation des compteurs + classement) |
> | **Dépendances aval** | Redis, Postgres, Scylla, Kafka. Le système de référence reste dans `post` / `profile` / `media` / `engagement` / `social-graph` — counter n'appelle **aucun** service sur le chemin de lecture |
> | **SLO** | `<TODO>` dispo · `BatchGetCounters` p99 `< <TODO ~5> ms` · latence d'ingestion `< <TODO ~10> s` |

---

## 🎯 Vue d'ensemble & rôle du service

`counter` est l'**agrégateur de compteurs temps réel et le système de référence analytique** de la plateforme : il absorbe le firehose d'engagement/vues, sert des magnitudes grossières au reste de la flotte avec une latence sous-milliseconde, et ne détient **aucune** entité. Chaque compteur est une matérialisation dérivée d'une vérité qui vit dans les services propriétaires — reconstructible, à tout instant, en rejouant leurs flux d'événements. C'est un système de *référence*, jamais un système d'enregistrement.

Le problème difficile qu'il résout est d'**absorber des millions d'engagements concurrents par seconde sans faire fondre une ligne transactionnelle**. Une conception naïve fait un `UPDATE … SET count = count + 1` par événement — transformant un post viral en une seule partition ScyllaDB brûlante ou une seule ligne Postgres verrouillée, où le débit s'effondre à l'inverse de la latence d'un seul verrou. Le motif résolvant est un **entonnoir d'amplification d'écriture** : ingestion Kafka shardée → pré-agrégation fenêtrée en mémoire qui collapse N événements en un seul delta → compteurs chauds Redis pipelinés → write-behind batché et idempotent. **Aucune écriture par-événement ne touche jamais un store durable.**

**Objectifs clés :** (1) l'ingestion est **asynchrone et hors du chemin d'écriture** — liker un post n'attend jamais `counter` ; (2) le chemin de lecture est **autonome et sous-milliseconde** — un simple multi-get Redis, aucun appel inter-service, aucun scan de table analytique en ligne ; (3) les compteurs sont **entièrement reconstructibles** depuis le système de référence + les événements (la réconciliation est de première classe) ; (4) la posture est **fail-open** — une panne de compteur dégrade en « périmé mais servi », jamais un blocage amont.

| Préoccupation | Chemin | Contrat de latence | Notes |
|---|---|---|---|
| **Ingestion** | consommateurs Kafka async (`run_consumer`) dans `counter-worker` | aucun (hors chemin d'écriture) | firehose → delta fenêtré → Redis en secondes ; la latence est un SLO, pas une exigence de cohérence |
| **Lecture** | gRPC synchrone, Redis seul (cache-aside vers Postgres en cas de miss) | p99 sous-ms | renvoie des magnitudes pour des références d'entités ; pas de fan-out |
| **Signal de classement** | `counter.v1.popularity` async (grossier, boucle lente) | aucun | `search` / `timeline` le consomment ; jamais un appel synchrone |

---

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS là où c'est pertinent, un **store à trois tiers** (Redis chaud / Postgres tiède / Scylla froid), Kafka pour l'ingestion. Le choix structurel déterminant est **deux déployables** : le serveur de lecture et le worker de flux partagent une crate de domaine mais aucun processus, déploiement, ni domaine de panne.

```
 télémétrie edge/BFF  ── view.v1.events ──┐
                      ── impression.v1.events ──┤
                      ── click.v1.events ──┤      ┌─────────────── counter-worker ───────────────┐
 wallet-service       ── wallet.v1.events ─────┤  │ [run_consumer · par topic]                    │
 social-graph         ── événements d'abonnement ──┘  │  → pré-agrégation fenêtrée (N événements → 1 Δ)│
                                                  ├─►│  → Redis (HINCRBY / PFADD / CMS, ré-agg shard) │
 (clés shardées étalent les entités chaudes)      │  │  → write-behind idempotent (clé de fenêtre)    │
                                                  │  │  → boucle de réconciliation → publie popularité│
                                                  │  └────────┬───────────────┬──────────────┬────────┘
                                                  │           ▼               ▼              ▼
                                                  │      Redis (chaud)  Postgres (tiède  Scylla TWCS
                                                  │                     totaux+registre)  (série froide)
                                                  │           ▲
   gateway/BFF/timeline/search ──► counter.v1.CounterService/BatchGetCounters ── lecture Redis seul ─┘
                                       renvoie des magnitudes pour (entity_type, id) · cache-aside en miss
   search / timeline ◄── counter.v1.popularity (signal de classement grossier, boucle lente)
```

**L'amplification d'écriture est éjectée avant la durabilité.** Les millions de `+1` d'un post viral sont étalés sur N partitions par une **clé shardée** (`entity_id:{0..N}`), repliés par chaque worker en un seul delta en mémoire par **fenêtre glissante**, pipelinés dans Redis, et seulement *ensuite* flushés — batchés et idempotents sur `(entity, metric, window_id)` — vers Postgres/Scylla. Un crash de worker et une re-livraison Kafka ré-appliquent la *même* fenêtre sans double comptage.

> **Invariants** (et où ils sont garantis) :
> - **Counter ne détient aucune source de vérité.** Les lectures renvoient des magnitudes pour une *référence* d'entité ; l'appelant hydrate l'entité depuis son système de référence. Test décisif : chaque compteur doit être reconstructible en rejouant les événements ou en scannant le système de référence — domaine + réconciliation.
> - **Il répond « combien ? », jamais « qui ? »/« lesquels ? ».** L'état d'arête par-acteur (qui a liké, qui suit) appartient à `engagement` / `social-graph`. Dès qu'une question requiert une identité ou un ensemble, il délègue — contrat de frontière.
> - **Aucune écriture durable par-événement.** Chaque écriture vers Postgres/Scylla est un agrégat de fenêtre ; le flush durable est idempotent sur `(entity, metric, window_id)` — frontière infrastructure.
> - **Exact vs probabiliste par classe de métrique.** Likes/partages/abonnés/commentaires sont *exacts-mais-réconciliables* (une fenêtre sur un ensemble qu'un autre système de référence possède) ; vues/impressions/spectateurs-uniques/portée sont *probabilistes par conception* — total via compteurs shardés (double comptage toléré), uniques via **HyperLogLog**, tendances via **Count-Min Sketch** — domaine.
> - **Lecture et classement sont des mécanismes de livraison séparés.** Le pull sous-ms (`BatchGetCounters`) et le push grossier (`counter.v1.popularity`) ne partagent jamais un chemin, donc le fan-out de classement ne taxe jamais le tier de lecture — application.

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (non-5xx / non-`UNAVAILABLE`) | `<TODO 99.9%>` | 30j glissants | `<metric>` |
| Latence de lecture p99 (`BatchGetCounters`) | `< <TODO 5> ms` | 1h | `<metric>` |
| Latence d'ingestion (événement → compté) | `< <TODO 10> s` | live | lag du `<consumer-group>` |
| Dérive de compteur (approximatif vs réconcilié) | `< <TODO 0.5%>` | par cycle de réconciliation | métrique de réconciliation |

**Budget d'erreur :** `<TODO>`. **En cas de burn :** `<gel des déploiements | page>`. Note : `counter` étant fail-open, l'objectif de *disponibilité* couvre la dégradation du chemin de lecture (périmé mais servi), pas l'exactitude — l'exactitude est couverte par la latence d'ingestion + le SLI de dérive de réconciliation.

---

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

**Aval — ce dont `counter` a besoin pour fonctionner :**

| Dépendance | Rôle | Si en panne → | Dégradation |
|---|---|---|---|
| Redis | compteurs chauds / HLL / CMS (chemin lecture + écriture) | lectures dégradées, écritures chaudes bloquées | **Souple** — les lectures retombent sur le dernier total Postgres flushé (périmé mais servi) ; le worker bufferise/réessaie dans le budget |
| Postgres | totaux matérialisés tièdes + registre de réconciliation | miss cache-aside + flush bloqué | **Souple** — les lectures chaudes servent toujours depuis Redis ; le flush durable réessaie (aucune perte, offsets non commités) |
| Scylla (TWCS) | séries temporelles historiques froides | `GetTimeSeries` dégradé | **Souple** — les compteurs live intacts ; seule l'analytique historique est impactée |
| Kafka | ingestion + publication de popularité | le comptage cesse d'avancer | **Souple** — les compteurs se périment, le lag croît ; aucune perte (commit manuel) |

**Amont — qui dépend de `counter` (votre rayon d'impact si VOUS tombez) :**

| Appelant | Utilise | Impact visible si `counter` est en panne |
|---|---|---|
| gateway / BFF | `BatchGetCounters` | les compteurs d'engagement s'affichent périmés ou absents ; **aucune** écriture, like, abonnement ou publication n'est affecté |
| `timeline` / `search` | `counter.v1.popularity` | le classement retombe sur son dernier snapshot grossier ; la découverte fonctionne toujours |

> **Chemin critique ?** **Non** — dérivé, async, fail-open. `counter` n'est jamais sur le chemin synchrone d'une écriture, d'un like, d'un abonnement, d'une publication ou d'un flux d'auth.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### gRPC — `counter.v1.CounterService` *(Phase 1)*

La surface synchrone est délibérément **en lecture seule** : `BatchGetCounters` (magnitudes pour un lot de références d'entités + masque de métriques — le chemin chaud d'hydratation de feed), `GetTrending` (top-K pour une portée, servi depuis CMS + un tas borné), et `GetTimeSeries` (buckets historiques — le seul RPC autorisé à toucher le tier froid Scylla, explicitement *non* sous-ms et hors du chemin de feed). **Il n'y a aucun RPC d'écriture/incrément** — l'ingestion est Kafka uniquement.

**Compteurs de likes masqués (#809).** Quand l'auteur d'un post masque ses compteurs de likes, sa valeur
`LIKE` est retirée de `BatchGetCounters` pour tout autre que l'auteur (un des profils de l'appelant,
d'après le jeton) — invités compris — et un `GetTrending` `LIKE` retire le post (les rangs se resserrent,
si bien que sa place ne le trahit pas non plus). Les autres métriques ne changent pas ; le mesh lit tout.
À qui est le post et le réglage de l'auteur viennent de post (`BatchGetLikeVisibility`, en cache 60 s par
instance) ; quand post ne peut pas répondre, le `LIKE` de chaque post est retenu et la lecture est marquée
`degraded` — jamais une erreur. Sans `COUNTER_POST_GRPC_ENDPOINT`, rien n'est retenu (un avertissement au
démarrage).

> **Contrat de fil :** les résultats sont des magnitudes attachées à une référence — `(entity_type, id, metric, value)` plus la provenance approximatif-vs-exact. Les appelants DOIVENT hydrater l'entité elle-même (corps du post, profil, URL média) depuis son système de référence. `counter` ne renvoie aucune entité autoritative ni aucune appartenance par-acteur.

### Ports Rust (contrat hexagonal) *(Phase 3)*

```rust
#[async_trait] pub trait CounterStore    { /* incr_window · read_batch · pfadd/pfcount · cms_topk — le tier chaud Redis */ }
#[async_trait] pub trait CounterLedger   { /* upsert_window(idempotent) · read_total · reconcile — le tier tiède Postgres */ }
#[async_trait] pub trait TimeSeriesStore { /* append_bucket · range — le tier froid Scylla */ }
#[async_trait] pub trait SignalPublisher { /* publie la popularité grossière — counter.v1.popularity */ }
```

### Contrat d'erreur

Chaque faute implémente `error::AppError` avec un code `CTR-XXXX` stable, mappé vers gRPC `Status` / HTTP par la crate partagée `error` :

| Plage | Classe |
|---|---|
| `CTR-1xxx` | lecture / requête |
| `CTR-2xxx` | agrégation / fenêtre |
| `CTR-3xxx` | flush / write-behind (réessayable) |
| `CTR-4xxx` | disponibilité du store (cœur fail-open ; réessayable) |
| `CTR-5xxx` | réconciliation / dérive |
| `CTR-8xxx` | décodage d'événement entrant / mapping de source |
| `CTR-9xxx` | transverse (domaine/parse, consommation d'événements) |

---

## 📨 Événements & contrat async &nbsp;·&nbsp; CORE

> Les topics Kafka sont une API. Un changement de schéma sur un topic consommé casse le comptage exactement comme un changement de proto.

**Publie :**

| Topic | Clé | Rôle |
|---|---|---|
| `counter.v1.popularity` | id d'entité | snapshot grossier et périodique de popularité/tendance pour le classement ; consommé par `search` (`PopularityScore`) et `timeline`. Jamais un compteur par-événement |

**Consomme :**

| Topic | Groupe de consommateurs | Rôle | En cas de poison/épuisement |
|---|---|---|---|
| `view.v1.events` | `counter-view-aggregator` | agrège les vues (total via compteur shardé, uniques via HLL) | DLQ `view.v1.events.dlq` |
| `impression.v1.events` | `counter-impression-aggregator` | agrège impressions / portée | DLQ `impression.v1.events.dlq` |
| `click.v1.events` | `counter-click-aggregator` | agrège clics / entrées de CTR | DLQ `click.v1.events.dlq` |
| `wallet.v1.events` | `counter-stake-aggregator` | les likes sont des points (#665) : chaque `stake_committed` ajoute ses `points` à la magnitude `like` du post ou du commentaire (autres événements du wallet ignorés). Approximatif comme toute somme ici (une mise relivrée compte deux fois, un crash perd une fenêtre) et entrée de popularité : le compteur exact de likes est celui d'engagement | DLQ `wallet.v1.events.dlq` |
| `<événements d'abonnement social-graph>` | `counter-follow-aggregator` | agrège les compteurs d'abonnés / abonnements | DLQ `<...>.dlq` |

> **Contrat de runtime (obligatoire) :** tous les consommateurs tournent sous `run_consumer` — commit manuel après un résultat terminal, réessai borné avec backoff + jitter, DLQ à l'épuisement/poison, reconstruction depuis le dernier offset commité en cas d'erreur broker. **Idempotence :** le flush durable est clé sur `(entity, metric, window_id)`, donc un événement re-livré ré-applique la même fenêtre sans double comptage ; un événement non-mappé/inconnu (`CTR-8002`) est replié en `Ok` pour que l'offset commite ; les métriques approximatives tolèrent le double comptage at-least-once par conception.

---

## 🌩️ Modes de défaillance & dégradation &nbsp;·&nbsp; OPS

| Défaillance | Symptôme | Comportement du service | Action opérateur |
|---|---|---|---|
| Redis indisponible (lecture) | latence / erreurs `BatchGetCounters` | **fail-open** — retombe sur le dernier total Postgres flushé (périmé), ne renvoie jamais 5xx au feed | vérifier la santé de Redis ; les lectures récupèrent quand le tier chaud revient |
| Redis indisponible (ingestion) | la latence d'ingestion monte | le worker bufferise/réessaie dans le budget, puis DLQ ; offset non commité → aucune perte | restaurer Redis ; les consommateurs reprennent au dernier offset commité |
| Flush Postgres en échec | le flush réessaie, lag sur le total durable | les compteurs chauds intacts ; le total durable prend du retard | restaurer Postgres ; le re-flush idempotent rattrape |
| Entité chaude (virale) | une entité domine une partition | la clé shardée (`entity_id:{0..N}`) + ré-agrégation à deux étages étale la charge | aucune (par conception) ; augmenter le nombre de shards si besoin |
| Dérive de réconciliation > tolérance | `CTR-5002 DriftThresholdExceeded` | le compteur approximatif corrigé contre la vérité rejouée du système de référence | investiguer le flux source ; la boucle de réconciliation auto-guérit les métriques exactes |
| Kafka indisponible | les compteurs cessent d'avancer | les consommateurs au ralenti ; aucune perte (commit manuel) | restaurer les brokers ; les compteurs rattrapent |

**Contre-pression & limites :** l'agrégation fenêtrée est le délestage primaire (N→1) ; plafonds de taille de lot par requête sur `BatchGetCounters` ; un timeout dur sur le chemin de lecture pour qu'un Redis lent déleste plutôt que de mettre en file ; l'ingestion est naturellement limitée par le débit des consommateurs.

---

## 📦 Intégration & usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
counter = { path = "crates/services/counter" }
```

Bibliothèque uniquement. Implémentera [`service_runtime::Service`](../../platform/service-runtime/README.md) **deux fois** (Phase 5) : `counter::service::CounterReadService` (le binaire `counter-server` — `build` câble l'adaptateur de lecture Redis, `register` ajoute le service gRPC, `health_probes` ping Redis) et `counter::service::CounterWorkerService` (le binaire `counter-worker` — `build` câble les trois adaptateurs de store + le publieur Kafka et **spawn les consommateurs d'agrégation supervisés**, sans ingress gRPC). La télémétrie, la config + hot-reload, la santé et l'arrêt gracieux sont gérés par le runtime.

### Bootstrap (`crates/apps/counter-server`)

```rust
use counter::service::CounterReadService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("COUNTER_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50064".to_owned()).parse()?;
    service_runtime::serve::<CounterReadService>(addr).await
}
```

> **État de build :** complet jusqu'à la Phase 7 (les 8 phases : scaffold → proto → domaine → application+ports → adaptateurs → câblage serveur+worker → IT live → durcissement). La suite d'intégration live est protégée par `integration-counter` et exerce les vrais tiers Redis + Postgres + Scylla. **La boucle de réconciliation est câblée :** le worker exécute un balayage supervisé qui pagine les paires réconciliables depuis le registre et guérit la dérive des compteurs exacts contre `social-graph` (le système de référence autoritaire) via `GetRelationStatus`. *Follower/following* sont réconciliés aujourd'hui ; la réconciliation *like/share/comment* reste différée — `engagement` expose des scores de réaction pondérés (pas un compte de réactions), et ses compteurs share/comment sont les approximatifs que ce service supersède. Suivis restants : une cadence de popularité autonome, le producteur concret de fan-out par shard, et un hook de drain à l'arrêt gracieux.
>
> **Autorisation (exigence de déploiement) :** `counter` ne s'auto-autorise en rien. Les RPC de lecture sont des magnitudes agrégées exposées à l'appelant ; contrôler l'accès au gateway / `auth-context` avant exposition. Les compteurs ne portent aucune identité par-acteur, donc ne fuitent aucune appartenance.

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables spécifiques à `counter` *(remplies par phase)*

| Variable | Requis | Défaut | Description |
|---|---|---|---|
| `COUNTER_GRPC_ADDR` | Non | `0.0.0.0:50064` | adresse d'écoute gRPC du serveur de lecture |
| `COUNTER_WORKER_GRPC_ADDR` | Non | `0.0.0.0:50065` | adresse santé/réflexion du worker (aucun RPC métier) |
| `COUNTER_AGGREGATION_WINDOW_MS` | Non | `5000` | fenêtre glissante de pré-agrégation (le collapse N→1) |
| `COUNTER_FLUSH_INTERVAL_MS` | Non | `=fenêtre` | fréquence de drain+flush des fenêtres fermées par le worker |
| `COUNTER_SHARD_COUNT` | Non | `16` | shards de clé pour entités chaudes (`entity_id:{0..N}`) |
| `COUNTER_READ_TIMEOUT_MS` | Non | `50` | timeout dur de lecture chaude par requête ; à expiration la lecture échoue **open** (total ledger périmé) |
| `COUNTER_POPULARITY_INTERVAL_S` | Non | `60` | cadence du signal de popularité (réservé ; actuellement couplé au flush) |
| `COUNTER_RECONCILE_INTERVAL_S` | Non | `3600` | cadence du balayage de réconciliation (correction de dérive follower/following) |
| `COUNTER_DRIFT_TOLERANCE` | Non | `5` | dérive absolue tolérée avant correction d'un compteur exact par la réconciliation |
| `COUNTER_SOCIAL_GRAPH_GRPC_ENDPOINT` | Non | `http://localhost:50053` | endpoint `social-graph` — comptes follower/following autoritaires pour la réconciliation |
| `COUNTER_SOCIAL_GRAPH_RPC_TIMEOUT_MS` | Non | `5000` | deadline par requête des RPC `social-graph` — un appel suspendu bloquerait sinon la boucle de réconciliation |
| `COUNTER_SOCIAL_GRAPH_CONNECT_TIMEOUT_MS` | Non | `2000` | deadline de connexion à l'ouverture du canal `social-graph` |
| `COUNTER_POST_GRPC_ENDPOINT` | Non | non défini | adresse mesh de `post` (p. ex. `http://post:50056`), serveur de lecture seulement : les compteurs de likes masqués sont retenus (#809). Non défini → retenus pour personne |
| `COUNTER_POST_RPC_TIMEOUT_MS` · `COUNTER_POST_CONNECT_TIMEOUT_MS` | Non | `500` · `1000` | délais de cet appel |

### Variables d'infrastructure héritées

| Variable | Requis | Défaut | Description |
|---|---|---|---|
| `REDIS_URL` | **Oui** | — | tier chaud des compteurs |
| `DATABASE_URL` | **Oui** | — | registre tiède (Postgres) |
| `SCYLLA_NODES` | **Oui** | — | séries temporelles froides (Scylla) |
| `KAFKA_BROKERS` | **Oui** | — | ingestion + publication de popularité |

### Fonctionnalités à la compilation
- `integration-counter` — active la suite d'intégration live, adossée à des conteneurs (Redis + Postgres + Scylla réels — le tiering chaud/tiède/froid complet).
- `build.rs` (Phase 1, dans `counter-api`) compile `contracts/proto/counter/v1/*.proto` et émet le descriptor set de réflexion.

---

## 🚀 Déploiement, migrations & rollback &nbsp;·&nbsp; OPS

- **Deux déployables, scalés indépendamment.** `counter-server` scale avec le QPS de lecture de la flotte ; `counter-worker` scale avec le volume du firehose d'ingestion. Ils sont publiés ensemble (même image/tag) mais déployés et autoscalés séparément.
- **Les migrations de schéma** (tables de registre Postgres + tables de séries temporelles Scylla TWCS) appartiennent à `crates/apps/migrator`, appliquées avant que le nouveau binaire ne serve.
- **Reconstruire depuis la vérité.** La rétention Kafka étant finie, les compteurs exacts sont réparés par la **boucle de réconciliation** qui scanne/rejoue le système de référence propriétaire (réactions `engagement`, abonnements `social-graph`), pas par un « replay depuis le début ». Les compteurs approximatifs (vues) sont acceptés comme approximatifs.
- **Rollback :** sûr — les deux binaires sont sans état au-dessus de leurs stores ; le worker reprend aux derniers offsets commités, le serveur est en lecture pure.
- **Pièges avec état :** changer `COUNTER_AGGREGATION_WINDOW_MS` ou `COUNTER_SHARD_COUNT` en vol affecte les fenêtres en cours — drainer ou accepter un pic de lag transitoire ; les clés d'idempotence durables rendent l'opération sûre, pas transparente.

---

## 📈 Télémétrie, performance & métriques &nbsp;·&nbsp; CORE

- **Runtime :** Tokio multi-thread. `counter-worker` exécute les consommateurs d'agrégation + l'ordonnanceur de flush + la boucle de réconciliation ; `counter-server` exécute les handlers de lecture. Subscriber tracing/OTel global installé avant serve ; trace-context W3C propagé à travers la frontière Kafka.

| Signal | Pourquoi c'est important | Alerte suggérée |
|---|---|---|
| Latence d'ingestion (par groupe de consommateurs) | fraîcheur des compteurs live | `> SLO` soutenu ⇒ page |
| Latence p99 `BatchGetCounters` | réactivité de l'hydratation de feed | `> SLO` ⇒ investiguer Redis |
| Échec / lag du flush durable | divergence du tier tiède, risque de replay | soutenu ⇒ page |
| Dérive de réconciliation | divergence approximatif-vs-vérité | `> SLO` ⇒ investiguer le flux source |
| Taux de production DLQ (`*.dlq`) | ingestion empoisonnée / réessais épuisés | tout taux soutenu ⇒ page |

---

## 🛠️ Développement local &nbsp;·&nbsp; CORE

```bash
cargo build -p counter && cargo clippy -p counter --all-targets
cargo test  -p counter                                    # run unitaire rapide, sans infra
docker compose up -d redis postgres scylla kafka          # compose à la racine (Phase 6)
cargo test  -p counter --features integration-counter     # suite live (tiers chaud/tiède/froid)
```

---

## 🚨 Dépannage & runbook &nbsp;·&nbsp; CORE

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réel.

**1. Les compteurs s'affichent périmés ou à zéro.**
Cause racine : Redis dégradé — les lectures échouent **open** vers le dernier total Postgres flushé plutôt que d'erreur le feed. Mitigation : vérifier la santé de Redis ; les compteurs live récupèrent quand le tier chaud revient ; le total durable borne la péremption du fallback.

**2. Un nouvel engagement n'est pas reflété dans les compteurs.**
Cause racine : lag d'ingestion ou un consommateur bloqué. Mitigation : vérifier le lag du groupe de consommateurs et les topics `*.dlq` ; une erreur broker/store retient l'offset (aucune perte), donc le worker rattrape une fois la dépendance rétablie.

**3. Un compteur semble faux / a dérivé.**
Cause racine : double comptage at-least-once sur une métrique approximative, ou un événement exact manqué. Mitigation : les métriques approximatives sont acceptées dans la tolérance ; les métriques exactes auto-guérissent au prochain cycle de réconciliation — vérifier la métrique de dérive et, si `CTR-5002` a déclenché, le flux source.

**4. Une entité virale crée un point chaud sur une partition.**
Cause racine : nombre de shards trop bas pour le débit de l'entité. Mitigation : augmenter `COUNTER_SHARD_COUNT` ; la conception de compteur shardé à deux étages étale l'entité sur plus de partitions/workers et ré-agrège dans Redis.

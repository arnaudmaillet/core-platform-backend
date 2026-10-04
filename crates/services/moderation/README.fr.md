---
i18n:
  source: ./README.md
  source_sha256: 0086d641ff44a8867fa2fedb04037a84dde049727cea25b73af43ca855306426
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `moderation` — Décider, appliquer et prouver les actions d'intégrité sans taxer chaque écriture du réseau

> **Fiche service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Équipe** | `<TODO: team>` · `<TODO: #slack-channel>` |
> | **Astreinte / escalade** | `<TODO: oncall-rotation>` → `<TODO: escalation-policy>` |
> | **Tier** | **TIER-0** — système de référence légal/conformité ; la porte `Screen` est dans le chemin de publication synchrone pour les catégories de préjudice catastrophique |
> | **Déployable** | `crates/apps/moderation-server` (crate bibliothèque : `crates/services/moderation`) |
> | **Datastores** | Postgres db `moderation` (SoR décision/dossier) · ScyllaDB keyspace `moderation` (historique signaux/preuves) · Redis Cluster (projection d'application + corpus de hash Screen) |
> | **Asynchrone** | publie `moderation.v1.events` · consomme `post.v1.events`, `comment.*`, contenu chat, `moderation.reports`, `moderation.signals` (Kafka) |
> | **Appelants amont** | `media`, `post` (porte Screen) ; console d'opérations interne (API dossier/file/appel) |
> | **Dépendances aval** | `account` (gRPC — exécution des suspensions), services de classification (signaux), Postgres, ScyllaDB, Redis, Kafka |
> | **SLO** | `<TODO>` dispo · `Screen` p99 `< <TODO> ms` · délai de revue async `< <TODO> s` |

---

## 🎯 Vue d'ensemble & rôle du service

`moderation` est le microservice **confiance, sécurité & conformité** : il détient la *décision d'intégrité de référence* — quelle action a été prise contre quelle entité, sous quelle version de politique, avec quelles preuves — et il en est la source autoritaire et auditable pour le reste de la flotte et pour les régulateurs.

Le problème difficile qu'il résout est de **modérer au volume d'écriture du réseau sans devenir un goulot de latence global**. Une conception naïve appelle une RPC de modération à chaque post, message et upload ; cela taxe chaque écriture et couple la disponibilité du contenu à une panne d'intégrité. Le motif qui résout cela est une **séparation en trois plans** : le chemin lourd de classification/revue est découplé du chemin chaud de décision.

**Objectifs fondamentaux :** (1) le contenu est revu **a posteriori et de façon asynchrone** par défaut — zéro latence ajoutée sur le chemin d'écriture ; (2) le chemin chaud de *lecture* lit un **état d'application dénormalisé**, jamais une RPC de modération par élément ; (3) seule une porte `Screen` synchrone, étroite et **fail-closed** garde les catégories de préjudice catastrophique ; (4) chaque application est un enregistrement **immuable et auditable** suffisant pour les obligations DSA / NCMEC / forces de l'ordre.

| Plan | Chemin | Contrat de latence | Usage |
|---|---|---|---|
| **A — Ingestion** | consommateurs Kafka async | aucune (hors chemin d'écriture) | ~99 % du contenu : publication optimiste, revue a posteriori |
| **B — État d'application** | événements + projection Redis | local / O(1) | « cet acteur est-il restreint / ce contenu masqué » sur le chemin chaud de lecture |
| **C — Porte Screen** | gRPC synchrone | borné, timeout strict | CSAM / NCII / TVEC uniquement — recherche de hash déterministe, fail-closed |

---

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), CQRS là où c'est pertinent, une **séparation en trois stores**, Kafka pour l'ingestion et les événements d'application.

```
                         signalements           signaux classifieurs
                              │                        │
  post/comment/chat/media     ▼                        ▼
  *.created  ─────────►  moderation.reports     moderation.signals
       │                      │                        │
       ▼                      ▼                        ▼
  ┌──────────── Plan A : consommateurs d'ingestion (run_consumer) ─────────────┐
  │  vérifs déterministes peu coûteuses (blocklist · hash connu · historique)   │
  │  → fan-out vers classifieurs async → ouverture de Case au-delà d'un seuil   │
  └───────────────┬───────────────────────────────────────────────┬───────────┘
                  ▼                                                 ▼
        moteur d'application graduée                        Scylla : historique
        (PenaltyLedger · version de politique)              signaux / preuves (TWCS)
                  │
                  ▼
        Postgres SoR : cases · decisions(WORM) · appeals · penalty_ledger · policy_versions
                  │
   ┌──────────────┼───────────────────────────────────────────────┐
   ▼              ▼                                                 ▼
 moderation.v1.events            projection Redis                Plan C : Screen
 (dénorm Plan B :            mod:enf:{actor:<id>}  ◄── lecture     (corpus de hash / bloom,
  timeline/chat/account)     vérifs synchrones O(1)  chaude        porte fail-closed)
```

**Cohérence de l'application.** Chaque `EnforcementAction` porte une **version monotone par sujet**, de sorte qu'une annulation ne peut jamais devancer une ré-application (la même discipline de génération que `auth` utilise pour les sessions). Les événements d'application sont **clés par `actor_id`** pour un ordonnancement par acteur.

> **Invariants** (et où ils sont appliqués) :
> - **Les décisions sont append-only** (`decisions` est le registre de preuves légal ; une annulation est une *nouvelle* décision, jamais une mutation) — domaine + Postgres.
> - **Screen est déterministe et sans inférence** — uniquement recherches hash/blocklist ; le ML ne tourne jamais en ligne (frontière infrastructure).
> - **Politique d'échec par catégorie** — CSAM/NCII/TVEC échouent **fermé** (bloquer en cas d'incertitude ou de panne de la porte) ; spam/limite échouent **ouvert** (reste en ligne, revu async) — couche application.
> - **Idempotence de domaine** — Cases clés par UUIDv5 déterministe de l'identité du sujet ; la redélivrance est réelle (standard consommateur).

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité `Screen` (Plan C) | `<TODO 99.95%>` | 30j glissants | `<metric>` |
| Latence `Screen` p99 | `< <TODO> ms` (recherche hash uniquement) | 1h | `<metric>` |
| Délai de revue d'ingestion (Plan A) | `< <TODO> s` | live | lag `moderation-ingestion-consumer` |
| Propagation d'application (décision → Plan B visible) | `< <TODO> s` | 1h | `<metric>` |
| Durabilité du registre de décisions | aucune décision acquittée perdue | — | commit synchrone Postgres |

**Budget d'erreur :** `<TODO>`. **En cas de consommation :** `<TODO: gel du déploiement | page>`. **Règle spéciale :** une panne soutenue de `Screen`/`HashCorpus` est un événement **fail-closed** — les uploads en catégories catastrophiques sont bloqués, pas dégradés ; page immédiate.

---

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

**Aval — ce dont `moderation` a besoin pour fonctionner :**

| Dépendance | Rôle | Si en panne → | Dégradation |
|---|---|---|---|
| Postgres | SoR décision/dossier | écritures au registre échouent | **Dure** — `UNAVAILABLE` pour les RPC dossier/décision/appel |
| ScyllaDB | historique signaux/preuves | écritures d'historique échouent | **Douce** — décisions toujours enregistrées ; historique rattrapé |
| Redis | projection d'application + corpus Screen | lectures Plan B/C échouent | **Dure pour Plan C** (fail-closed), **Douce pour Plan B** (les consommateurs re-dénormalisent) |
| Kafka | événements d'ingestion + application | pas d'ingestion / pas de dénorm | **Douce** — le backlog se vide à la reprise ; contenu déjà en ligne |
| `account` (gRPC) | exécution suspension/ban | actions de cycle de vie inapplicables | **Douce** — décision enregistrée, application réessayée |
| services de classification | signaux ML | pas de signaux ML | **Douce** — le moteur tourne sur règles déterministes uniquement |

**Amont — qui dépend de `moderation` (rayon d'impact en cas de panne) :**

| Appelant | Utilise | Impact visible si `moderation` est en panne |
|---|---|---|
| `media`, `post` | `Screen` (Plan C) | uploads de catégories catastrophiques **bloqués** (fail-closed) ; contenu normal inchangé |
| `timeline`, `chat`, `account` | `moderation.v1.events` (Plan B) | propagation d'application retardée ; état déjà appliqué inchangé |
| console d'opérations | API dossier/file/appel | les modérateurs ne peuvent ni trier ni décider ; le backlog croît |

> **Chemin critique ?** **Partiellement** — seule la porte `Screen` (Plan C) est synchrone (et uniquement pour `media`/`post`, uniquement pour les catégories catastrophiques). Tout le reste est async/dérivé.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### gRPC — `moderation.v1.ModerationService` *(le contrat arrive en Phase 1)*

```protobuf
// Plan C — la porte de pré-publication étroite et fail-closed (media/post uniquement).
rpc Screen (ScreenRequest) returns (ScreenResponse);

// Signalements clients (edge member_or_guest — DSA art. 16, invités compris).
rpc SubmitReport (..) returns (..);

// Console d'opérations — cycle de vie dossier / file / appel.
rpc OpenCase   (..) returns (..);   rpc AssignCase (..) returns (..);
rpc DecideCase (..) returns (..);   rpc ListQueue  (..) returns (..);
rpc FileAppeal (..) returns (..);   rpc ResolveAppeal (..) returns (..);

// Conformité / back-office.
rpc GetStatementOfReasons (..) returns (..);   // export DSA SoR
rpc GetEnforcementState   (..) returns (..);   // interne ; DÉCONSEILLÉ sur le chemin chaud
```

> **Règle de contrat / wire :** la surface n'expose que des types d'intégrité normalisés — `SubjectRef` (entity_type + entity_id + actor_id + surface), catégorie de politique, type d'action, identifiants de dossier/appel — jamais de champs spécifiques aux classifieurs ou internes au contenu.
>
> **Règle du chemin chaud :** la flotte lit l'application via le **Plan B** (événements + projection Redis), **pas** `GetEnforcementState`. La RPC n'existe que pour le back-office/lectures froides.
>
> **Signalements clients (`SubmitReport`).** Tout le monde peut signaler (DSA art. 16) : la RPC est ouverte à toute
> session client, membre **ou invité** (edge `member_or_guest`). L'**auteur du signalement** est le jeton vérifié (le
> compte d'un membre, ou `guest:<id>`), jamais un champ de requête ; un appel mesh n'a pas d'auteur et est refusé
> (`MOD-6007`). Le client n'envoie que le contenu (`POST` / `COMMENT` / `PROFILE` + id) : le **compte** qu'une
> décision sanctionnerait est résolu côté serveur (post / commentaire → profil auteur → compte propriétaire, via le
> mesh), de sorte qu'un auteur de signalement ne peut jamais diriger une sanction vers quelqu'un d'autre. Quota par
> auteur : `MODERATION_REPORTS_PER_HOUR` / `_PER_DAY` (20 / 100) dans Redis, **fail-open** si Redis est indisponible
> (signaler est un droit ; la file l'absorbe). Le signalement alimente ensuite le dossier du sujet comme un
> signalement du pipeline ; celui d'un invité est un signal plus faible (`guest_report`, 0,3 contre 0,5). `OpenCase`
> reste mesh uniquement (c'est celui de la console de modération).
>
> **Autorisation (exigence de déploiement) :** les RPC d'opérations mutatives (`DecideCase`, `AssignCase`, `OpenCase`, `ResolveAppeal`) sont **privilégiées** — elles bannissent/suspendent/suppriment. Le service n'autorise pas lui-même l'appelant ; les RPC mutatives **doivent** être restreintes à des principaux modérateurs authentifiés en périphérie (autorisation gateway / contrôle de permission `auth-context`, ex. `moderation:decide`) avant exposition. `Screen` et `FileAppeal` sont orientées appelant ; le reste est réservé aux modérateurs.

### Ports Rust (contrat hexagonal) *(Phase 3)*

`SignalSource` · `Case/Decision/Penalty/Appeal Repository` · `EnforcementProjection` (Redis) · `HashCorpus` (Screen) · `ClassifierGateway` · `AccountDirectory` (gRPC) · `EventPublisher` — AFIT, sans `async_trait`, fakes en mémoire pour la couche application.

### Contrat d'erreur

Chaque faute implémente `error::AppError` avec un code stable `MOD-XXXX` (voir [`src/error.rs`](src/error.rs)) :

| Plage | Classe |
|---|---|
| `MOD-1xxx` | cycle de vie dossier |
| `MOD-2xxx` | registre de décisions (append-only) |
| `MOD-3xxx` | action d'application |
| `MOD-4xxx` | pénalité / strikes / politique |
| `MOD-5xxx` | appel |
| `MOD-6xxx` | réception des signalements |
| `MOD-7xxx` | porte Screen / corpus de hash (Plan C) |
| `MOD-8xxx` | dépendances d'intégrité externes (classifieurs / annuaire de comptes) |
| `MOD-9xxx` | transversal (domaine/parse · concurrence · publication d'événement) |

`DB-*` / `SDB-*` / `RDS-*` / `VAL-*` sont délégués depuis les crates de stockage et de validation.

---

## 📨 Événements & contrat asynchrone &nbsp;·&nbsp; CORE

> Les topics Kafka sont une API. Un changement de schéma ici casse les consommateurs exactement comme un changement de proto.

**Publie :**

| Topic | Déclencheur | Clé | Consommateurs |
|---|---|---|---|
| `moderation.v1.events` | application appliquée/annulée, dossier ouvert/résolu, appel résolu | `actor_id` | `timeline`, `chat`, `account` (dénorm Plan B) |
| `moderation.v1.events` · `decision_recorded` | une décision est enregistrée (screen automatisé, revue humaine, annulation d'appel) | `actor_id` | `audit` (preuve de conformité) |

> L'événement **`decision_recorded`** est l'enregistrement de preuve de conformité dédié que le plan `audit` consomme — contrairement aux événements Plan-B centrés sur le contrevenant ci-dessus, il porte *qui a décidé* (l'autorité) et *pourquoi* (le motif / énoncé de motifs DSA), issu du registre `Decision` immuable. Le motif est scellé dans une enveloppe crypto-effaçable par `audit` à l'ingestion ; par convention il est référentiel-aux-politiques, non citateur-de-contenu. Les autres consommateurs ignorent ce variant.

**Consomme :**

| Topic | Groupe de consommateurs | Rôle | En cas de poison/épuisement |
|---|---|---|---|
| `post.v1.events` / `comment.*` / contenu chat | `moderation-ingestion-consumer` | Plan A : construire le sujet, screener à bas coût, ouvrir des dossiers | DLQ `<topic>.dlq` |
| `moderation.reports` | `moderation-report-consumer` | signalements d'abus (dédup → dossier) | DLQ `moderation.reports.dlq` |
| `moderation.signals` | `moderation-signal-consumer` | verdicts classifieurs → moteur gradué | DLQ `moderation.signals.dlq` |

> **Contrat d'exécution (obligatoire) :** tous les consommateurs tournent sous `run_consumer` — commit manuel après un résultat terminal, retry borné avec backoff + jitter, DLQ à l'épuisement/poison, reconstruction depuis le dernier offset commité en cas d'erreur broker. **Idempotence :** Cases clés par UUIDv5 déterministe de l'identité du sujet ; les sauts intentionnels (bloqué, auto-cible, dédup) se replient en `Ok` pour commiter au lieu d'inonder la DLQ.

---

## 🌩️ Modes de défaillance & dégradation &nbsp;·&nbsp; OPS

| Défaillance | Symptôme | Comportement du service | Action opérateur |
|---|---|---|---|
| Redis / corpus de hash en panne | `Screen` retourne `MOD-7002/7003` | **Fail-closed** pour CSAM/NCII/TVEC → les appelants bloquent l'upload | Page ; restaurer le corpus ; les uploads reprennent |
| Postgres en panne | RPC dossier/décision `UNAVAILABLE` | Écritures du registre échouent ; **aucune décision silencieuse** | Page ; bascule ; le backlog d'ingestion se vide ensuite |
| Lag Kafka (ingestion) | revue retardée | contenu déjà en ligne (optimiste) ; dossiers ouverts en retard | Vérifier broker / classifieur ; le lag se résorbe |
| `account` gRPC en panne | suspensions non appliquées | décision enregistrée ; application réessayée | Vérifier `account` ; les retries convergent |
| classifieur en panne | moins de signaux | le moteur tourne sur **règles déterministes uniquement** | Doux ; investiguer le classifieur |

**Contre-pression & limites :** la porte `Screen` a un **timeout strict + disjoncteur** (Phase 7) pour qu'une panne de modération ne bloque pas `media`/`post` ; l'ingestion régule via le lag des consommateurs, pas en jetant des messages ; la profondeur de file est le signal de charge pour la capacité de revue humaine.

---

## 📦 Intégration & usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
moderation = { path = "crates/services/moderation" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md) sous `moderation::service::ModerationService` *(Phase 5)* — `build` câble les adaptateurs et auto-lance les consommateurs ingestion/report/signal, `register` ajoute les services gRPC, `health_probes` expose la vivacité sur Postgres + Scylla + Redis. Télémétrie, config + rechargement à chaud, limitation de débit en entrée, santé et arrêt gracieux sont gérés par le runtime.

### Amorçage (`crates/apps/moderation-server`) *(forme de la Phase 5)*

```rust
use moderation::service::ModerationService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("MODERATION_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50061".to_owned()).parse()?;
    service_runtime::serve::<ModerationService>(addr).await
}
```

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables spécifiques à `moderation` *(croissent par phase)*

| Variable | Requis | Défaut | Description |
|---|---|---|---|
| `MODERATION_GRPC_ADDR` | Non | `0.0.0.0:50061` | adresse de bind gRPC |
| `MODERATION_ACCOUNT_GRPC_ENDPOINT` | Non | `http://localhost:50059` | endpoint du service `account` (exécution des suspensions) |
| `MODERATION_POST_GRPC_ENDPOINT` · `MODERATION_COMMENT_GRPC_ENDPOINT` · `MODERATION_PROFILE_GRPC_ENDPOINT` | **Oui** (prod) | `:50056` · `:50057` · `:50052` sur localhost | résolution du sujet d'un signalement (à qui appartient le contenu signalé) |
| `MODERATION_CONTENT_RPC_TIMEOUT_MS` · `MODERATION_CONTENT_CONNECT_TIMEOUT_MS` | Non | `1000` · `1000` | délais de ces appels |
| `MODERATION_REPORTS_PER_HOUR` · `MODERATION_REPORTS_PER_DAY` | Non | `20` · `100` | quota de signalements client par auteur (membre ou invité) |
| `MODERATION_ACCOUNT_RPC_TIMEOUT_MS` | Non | `2000` | deadline par requête des RPC `account` (tonic n'a pas de timeout par défaut) |
| `MODERATION_ACCOUNT_CONNECT_TIMEOUT_MS` | Non | `2000` | deadline de connexion à l'ouverture du canal `account` |
| `MODERATION_SCREEN_TIMEOUT_MS` | Non | `200` | timeout strict de la porte Plan C ; à l'expiration la porte retourne `MOD-7002` et l'appelant échoue fermé pour les catégories catastrophiques |

### Variables d'infrastructure héritées

| Variable | Requis | Défaut | Description |
|---|---|---|---|
| `POSTGRES_*` | **Oui** | — | connexion SoR décision/dossier |
| `SCYLLA_*` | **Oui** | — | connexion historique signaux/preuves |
| `REDIS_HOSTS` | **Oui** | — | projection d'application + corpus Screen |
| `KAFKA_BROKERS` | **Oui** | — | événements ingestion + application |

> Le réglage complet connexion/timeout/reconnexion vit dans les crates de stockage/transport partagées.

### Fonctionnalités à la compilation
- `integration-moderation` *(Phase 6)* — active la suite d'intégration adossée à des conteneurs.
- `build.rs` compile `moderation.v1` et émet le jeu de descripteurs de réflexion *(Phase 1)*.

---

## 🚀 Déploiement, migrations & rollback &nbsp;·&nbsp; OPS

- **Migrations :** `crates/services/moderation/migrations/*.{sql,cql}` (SoR Postgres + historique Scylla) *(Phase 4)*. À appliquer **avant** de déployer le nouveau binaire, via le conteneur init `migrator`.
- **Déploiement :** rolling ; les changements risqués du moteur de politique sont gardés par une **version de politique** épinglée (une décision enregistre la version sous laquelle elle a été prise, donc les déploiements sont auditables et réversibles).
- **Rollback :** le rollback du binaire est sûr ; le registre de décisions est append-only et compatible en avant. **Ne jamais** muter rétroactivement une décision — enregistrer une annulation.
- **Pièges avec état :** le **schéma de hash Screen** et la **monotonie de version par sujet** ne doivent jamais changer une fois des données présentes.

---

## 📈 Télémétrie, performance & métriques &nbsp;·&nbsp; CORE

- **Runtime :** Tokio multi-thread (consommateurs ingestion/report/signal + gRPC). Souscripteur tracing/OTel global installé avant `serve` ; contexte de trace W3C propagé à travers la frontière Kafka.

| Signal | Pourquoi c'est important | Alerte suggérée |
|---|---|---|
| `Screen` p99 + taux d'erreur | santé de la porte préjudice catastrophique | dépassement ⇒ page (fail-closed bloque les uploads) |
| lag consommateur d'ingestion | latence de revue a posteriori | croissance soutenue ⇒ investiguer |
| lag de propagation d'application | fraîcheur de la dénorm Plan B | dépassement ⇒ investiguer |
| taux de production DLQ (`*.dlq`) | poison / retry épuisé | tout taux soutenu ⇒ page |
| erreurs d'écriture de décision | durabilité du registre | toute occurrence ⇒ page |

---

## 🛠️ Développement local &nbsp;·&nbsp; CORE

```bash
cargo build  -p moderation && cargo clippy -p moderation --all-targets
cargo test   -p moderation
# Phase 6+ : suite d'intégration live (lance Postgres + Scylla + Redis + Kafka)
# cargo test -p moderation --features integration-moderation
```

> **Statut de build :** complet jusqu'à la Phase 7 — contrat proto, domaine, application + ports, adaptateurs d'infrastructure (Postgres/Scylla/Redis/Kafka), câblage runtime + consommateurs d'ingestion auto-lancés, suite d'intégration live adossée à des conteneurs, et durcissement (le timeout strict du `Screen`). Les erreurs `MOD-XXXX`, les tests unitaires et la suite `integration-moderation` sont tous verts. Les métadonnées d'organisation (équipe, astreinte, chiffres SLO) et l'autorisation au niveau gateway sont des `<TODO>` de déploiement.

---

## 🚨 Dépannage & runbook &nbsp;·&nbsp; CORE

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réelle.

**1. Uploads `media`/`post` échouant avec `MOD-7002`/`MOD-7003`.**
Cause racine : la porte `Screen` ou le corpus de hash (Redis) est indisponible ; pour les catégories de préjudice catastrophique, la politique de l'appelant est **fail-closed**, donc les uploads sont bloqués par conception. Mitigation : restaurer le corpus Redis ; vérifier `MODERATION_SCREEN_TIMEOUT_MS` ; les uploads reprennent automatiquement une fois la porte saine.

**2. Un contenu qui devrait être actionné est encore visible.**
Cause racine : lag de dénormalisation Plan B (événement d'application pas encore consommé par `timeline`/`chat`) **ou** le backlog d'ingestion n'a pas encore atteint le sujet (le Plan A est a posteriori). Mitigation : vérifier le lag des consommateurs de `moderation.v1.events` en aval et le lag du consommateur d'ingestion ; confirmer que l'`EnforcementAction` existe dans Postgres et sa clé de projection Redis `mod:enf:{actor:<id>}`.

**3. Le taux de production `*.dlq` grimpe.**
Cause racine : signaux/signalements poison ou une dépendance épuisée. Mitigation : inspecter les en-têtes `x-dlq-*` ; si panne de dépendance, corriger et rejouer depuis la DLQ ; si réellement poison, laisser parqué et trier.

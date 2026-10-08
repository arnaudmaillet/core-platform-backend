---
i18n:
  source: ./README.md
  source_sha256: 03bb53776b15296935ab5bcfbd8055e097aaf44791d06ba2a303604676fc2478
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants, noms de types) sont volontairement laissés en anglais.

# Documentation Domaine & Fonctionnelle

> **Objet.** Ce répertoire contient la documentation fonctionnelle **inter-contexte** de la
> plateforme — les faits qu'*aucun service ne peut maintenir vrais à lui seul*. La documentation
> domaine par service vit **avec son crate**, dans `crates/services/<svc>/docs/DOMAIN.md` ; ce
> répertoire ne contient que ce qui traverse les contextes et indexe le reste.

## La règle de séparation

> **Documenter chaque fait là où il peut devenir faux.** Si exactement un bounded context peut
> rendre une affirmation fausse, elle vit dans le `DOMAIN.md` de ce crate. Si deux contextes ou
> plus doivent s'accorder pour qu'elle soit vraie, elle vit ici.

| Vit **avec le crate** (`crates/services/<svc>/docs/DOMAIN.md`) | Vit **ici** (`docs/domain/`) |
|---|---|
| Objet du bounded context, non-objectifs | La **context map** (relations entre contextes) |
| Agrégats, invariants, machines à états | Le **langage omniprésent** inter-contexte |
| Propriété des données (ce dont ce contexte est SoR) | Le **catalogue d'événements** sémantique |
| Glossaire par contexte, workflows | Cet index |

## Vérité de référence (ground truth)

La source de vérité sur *ce qui existe* est **le code**, jamais un diagramme :
- les crates — `crates/services/*`, `crates/apps/*` ;
- leurs contrats proto `*.v1` ;
- la **garde de registre de topologie d'événements** (vérifiée par machine, donc fiable).

Toute la documentation ici en *dérive*. Le modèle C4 pré-flotte hérité a été **supprimé** ; un modèle
C4 corrigé a été *régénéré à partir* des Domain Cards + `CONTEXT_MAP.md` dans
[`docs/architecture/`](../architecture/README.md).

## Contenu

| Fichier | Ce qu'il contient | Statut |
|---|---|---|
| [`CONTEXT_MAP.md`](./CONTEXT_MAP.md) | Context map DDD couvrant les 17 contextes, avec les patterns de relation (ACL / Conformist / Published Language / OHS / Customer-Supplier / Separate Ways) | ✅ rempli |
| [`UBIQUITOUS_LANGUAGE.md`](./UBIQUITOUS_LANGUAGE.md) | Termes partagés par plus d'un contexte — partagés (un seul sens) vs surchargés (sens par contexte) ; les termes propres à un contexte restent dans chaque `DOMAIN.md` | ✅ rempli |
| [`EVENT_CATALOG.md`](./EVENT_CATALOG.md) | Sens sémantique de chaque événement de domaine (rédigé depuis les Domain Cards ; colonnes topic/producteur/consommateur à réconcilier avec la garde de topologie) | ✅ rempli |

## Documents domaine par service (`DOMAIN.md` par crate)

Les 17 services. La profondeur dépend du tier : TIER-0/1 reçoivent les sections DEEP complètes ;
TIER-2 conserve CORE plus des sections DEEP réduites à une ligne.

| Service | Bounded context | `DOMAIN.md` | Statut |
|---|---|---|---|
| `account` | Account / Identity SoR | [`crates/services/account/docs/DOMAIN.md`](../../crates/services/account/docs/DOMAIN.md) | ✅ |
| `audit` | Preuve de conformité infalsifiable | [`crates/services/audit/docs/DOMAIN.md`](../../crates/services/audit/docs/DOMAIN.md) | ✅ |
| `auth` | Authentification / session / courtier IdP | [`crates/services/auth/docs/DOMAIN.md`](../../crates/services/auth/docs/DOMAIN.md) | ✅ |
| `chat` | Conversations & messagerie | [`crates/services/chat/docs/DOMAIN.md`](../../crates/services/chat/docs/DOMAIN.md) | ✅ |
| `comment` | Fils de commentaires | [`crates/services/comment/docs/DOMAIN.md`](../../crates/services/comment/docs/DOMAIN.md) | ✅ |
| `counter` | Compteurs / SoReference analytique | [`crates/services/counter/docs/DOMAIN.md`](../../crates/services/counter/docs/DOMAIN.md) | ✅ |
| `engagement` | Likes (points) & compteurs d'interaction | [`crates/services/engagement/docs/DOMAIN.md`](../../crates/services/engagement/docs/DOMAIN.md) | ✅ |
| `geo-discovery` | Découverte géo-spatiale | [`crates/services/geo-discovery/docs/DOMAIN.md`](../../crates/services/geo-discovery/docs/DOMAIN.md) | ✅ |
| `media` | Plan de contrôle média | [`crates/services/media/docs/DOMAIN.md`](../../crates/services/media/docs/DOMAIN.md) | ✅ |
| `moderation` | Confiance, sécurité & intégrité | [`crates/services/moderation/docs/DOMAIN.md`](../../crates/services/moderation/docs/DOMAIN.md) | ✅ |
| `notification` | Fil d'activité de notifications | [`crates/services/notification/docs/DOMAIN.md`](../../crates/services/notification/docs/DOMAIN.md) | ✅ |
| `post` | Contenu / publications | [`crates/services/post/docs/DOMAIN.md`](../../crates/services/post/docs/DOMAIN.md) | ✅ |
| `profile` | Personas publics | [`crates/services/profile/docs/DOMAIN.md`](../../crates/services/profile/docs/DOMAIN.md) | ✅ |
| `realtime` | Plan de livraison temps réel / connexion | [`crates/services/realtime/docs/DOMAIN.md`](../../crates/services/realtime/docs/DOMAIN.md) | ✅ |
| `search` | Read-model de découverte | [`crates/services/search/docs/DOMAIN.md`](../../crates/services/search/docs/DOMAIN.md) | ✅ |
| `social-graph` | Relations followers / following | [`crates/services/social-graph/docs/DOMAIN.md`](../../crates/services/social-graph/docs/DOMAIN.md) | ✅ |
| `timeline` | Fan-out de timeline | [`crates/services/timeline/docs/DOMAIN.md`](../../crates/services/timeline/docs/DOMAIN.md) | ✅ |

## Contrats de bibliothèque d'infrastructure partagée (`foundation/` + `platform/`)

Les 14 bibliothèques transversales que chaque service compose. Ce ne sont **pas** des bounded contexts —
elles ne possèdent aucune donnée métier et n'émettent aucun événement de domaine ; elles possèdent une
**capacité technique** (un mécanisme, un contrat, une séquence de boot). Leur `DOMAIN.md` suit donc la
**variante bibliothèque** ([`docs/templates/DOMAIN.lib.template.md`](../templates/DOMAIN.lib.template.md)) :
même squelette à 10 sections et tiering CORE/DEEP, mais la section « propriété des données » devient une
frontière de *direction de dépendance / pureté* et la section « événements de domaine » se réduit aux
signaux émis (`tracing`/métriques/aucun).

**`foundation/`** — feuilles pures et contrats quasi-racine (aucune IO sauf indication) :

| Crate | Capacité partagée | `DOMAIN.md` | Statut |
|---|---|---|---|
| `error` | Contrat d'erreur distribuée (trait · sévérité · forme wire) | [`crates/foundation/error/docs/DOMAIN.md`](../../crates/foundation/error/docs/DOMAIN.md) | ✅ |
| `health` | Contrat de probe liveness/readiness (feuille du graphe) | [`crates/foundation/health/docs/DOMAIN.md`](../../crates/foundation/health/docs/DOMAIN.md) | ✅ |
| `infra-config` | Config externalisée & hot-reload fail-closed | [`crates/foundation/infra-config/docs/DOMAIN.md`](../../crates/foundation/infra-config/docs/DOMAIN.md) | ✅ |
| `resilience` | Tolérance aux pannes en sortie (circuit breaker · retry · timeout) | [`crates/foundation/resilience/docs/DOMAIN.md`](../../crates/foundation/resilience/docs/DOMAIN.md) | ✅ |
| `traffic` | Rate limiting en entrée (mécanisme GCRA pur) | [`crates/foundation/traffic/docs/DOMAIN.md`](../../crates/foundation/traffic/docs/DOMAIN.md) | ✅ |
| `validate-core` | Abstraction de validation zéro-dépendance (Separated Interface) | [`crates/foundation/validate-core/docs/DOMAIN.md`](../../crates/foundation/validate-core/docs/DOMAIN.md) | ✅ |

**`platform/`** — les couches dispatch applicatif, transport, sécurité, et runtime :

| Crate | Capacité partagée | `DOMAIN.md` | Statut |
|---|---|---|---|
| `auth-context` | Vérification JWT en entrée + identité task-local | [`crates/platform/auth-context/docs/DOMAIN.md`](../../crates/platform/auth-context/docs/DOMAIN.md) | ✅ |
| `cqrs` | Bus Command/Query in-process + pipeline de middleware | [`crates/platform/cqrs/docs/DOMAIN.md`](../../crates/platform/cqrs/docs/DOMAIN.md) | ✅ |
| `service-runtime` | Bootstrap unifié de la flotte (le trait `Service` + `serve`) | [`crates/platform/service-runtime/docs/DOMAIN.md`](../../crates/platform/service-runtime/docs/DOMAIN.md) | ✅ |
| `telemetry` | Bootstrap d'observabilité en un appel (logs · traces · métriques) | [`crates/platform/telemetry/docs/DOMAIN.md`](../../crates/platform/telemetry/docs/DOMAIN.md) | ✅ |
| `test-support` | Échafaudage de tests d'intégration (dev-only) | [`crates/platform/test-support/docs/DOMAIN.md`](../../crates/platform/test-support/docs/DOMAIN.md) | ✅ |
| `traffic-redis` | Backend distribué Redis-lease pour `traffic` | [`crates/platform/traffic-redis/docs/DOMAIN.md`](../../crates/platform/traffic-redis/docs/DOMAIN.md) | ✅ |
| `transport` | gRPC + Kafka avec propagation de trace + `run_consumer` | [`crates/platform/transport/docs/DOMAIN.md`](../../crates/platform/transport/docs/DOMAIN.md) | ✅ |
| `validation` | Middleware de validation d'entrée CQRS + codes `VAL-xxxx` | [`crates/platform/validation/docs/DOMAIN.md`](../../crates/platform/validation/docs/DOMAIN.md) | ✅ |

## Rédaction

- Modèle service : [`docs/templates/DOMAIN.template.md`](../templates/DOMAIN.template.md) — copier vers
  `crates/services/<svc>/docs/DOMAIN.md` et le remplir.
- Modèle bibliothèque partagée : [`docs/templates/DOMAIN.lib.template.md`](../templates/DOMAIN.lib.template.md)
  — pour `crates/foundation/*` et `crates/platform/*` ; même squelette, sections orientées bibliothèque.
- Décisions : consigner la justification dans des ADR immuables sous [`docs/adr/`](../adr/README.md)
  et les relier depuis `DOMAIN.md §9` — ne jamais intégrer le *pourquoi* en ligne.
- i18n : l'anglais est canonique ; un miroir `DOMAIN.fr.md` suit le
  [standard de traduction](../i18n/TRANSLATION.md). La garde de dérive (`tools/i18n/i18n-drift.sh`)
  couvre **tout** `<name>.<lang>.md`, donc `DOMAIN.fr.md`, `CONTEXT_MAP.fr.md`, etc. sont vérifiés
  comme les README.

> 🇬🇧 Source anglaise : [`README.md`](./README.md).

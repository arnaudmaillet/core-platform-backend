---
i18n:
  source: ./DOMAIN.md
  source_sha256: c384061b042434769b3233101a0880915f0fb52ba269f91faa33dd9d4349ec2e
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `test-support` — Contrat de Domaine & Fonctionnel

> L'épine dorsale des tests d'intégration : containers, migrations, et l'await anti-flake. Il répond à *« qu'est-ce qui est identique à travers chaque suite live de service, pour que chaque suite ne porte que ses propres scénarios ? »*

> **Domain Card**
>
> | | |
> |---|---|
> | **Capacité partagée** | Échafaudage de tests d'intégration agnostique au backend : orchestration de containers, runners de migration, et la primitive de synchronisation `await_until` |
> | **Couche** | `platform` — **dev-only** ; jamais lié dans un binaire de service |
> | **Classe de sous-domaine** | **Supporting** — infrastructure de test ; le levier est la cohérence + la discipline anti-flake |
> | **Abstraction(s) primaire(s)** | `containers::*` + `migrate::*` + `await_until` (`test_support`) |
> | **Empreinte** | dev-only — une `[dev-dependency]` ; boot des containers Docker ; nécessite un daemon Docker en marche |
> | **Posture en cas d'échec** | N/A — échafaudage de test ; la correction = aucun flake, pas la résilience runtime |
> | **Dépend de** | `testcontainers(-modules)`, `rdkafka`, `tokio`, `scylla(-storage)`, `sqlx`, `tracing` |
> | **Consommé par** | la suite live de chaque service (`tests/<svc>_it/`), comme une `[dev-dependency]` |
> | **Journal des décisions** | aucun — justification dans [`README §Architecture`](../README.md) |

---

## 1. Capacité Technique & Non-Objectifs &nbsp;·&nbsp; CORE

**Capacité.** `test-support` fait autorité dans la flotte pour l'**infrastructure de test live** : il répond à
**« comment chaque service boote-t-il ses vrais backends (Scylla/Redis/Kafka/Postgres/MinIO), applique les
migrations une fois, et synchronise les assertions sans dormir ? »** — pour que la suite de chaque service ne
porte que son graphe de composition root et ses scénarios.

**Le problème difficile.** Les suites d'intégration live sont flaky et lentes quand chacune réinvente le boot de
container, l'assignation de port, l'application de migration, et (pire) des `sleep` fixes en course avec des
containers lents. `test-support` extrait les cinq piliers de la suite gold-standard `chat` pour que les parties
*identiques* soient écrites une fois et que la discipline anti-flake (`await_until`, jamais `sleep`) soit imposée
par la primitive partagée.

**Non-objectifs — ce que ce crate ne fait délibérément PAS :**
- ❌ Être lié dans un binaire de service → il est dev-only ; cela tirerait l'échafaudage de test en production.
- ❌ Posséder la logique de test / les scénarios → ceux-ci vivent dans le `tests/<svc>_it/` de chaque service.
- ❌ Fournir l'isolation par teardown → l'isolation est par namespacing (clés UUID fraîches), une discipline par harness.

---

## 2. Langage Omniprésent &nbsp;·&nbsp; CORE

| Terme | Sens dans ce crate | Symbole de code |
|---|---|---|
| Ready entry point | Boot le container (une fois) + applique les migrations (une fois), retourne l'endpoint | `scylla_ready`, `postgres_ready` |
| Migration runner | Application DDL idempotente avec adaptation mono-nœud | `scylla_apply`, `postgres_apply` |
| The await primitive | Sonder l'état observable contre une deadline — jamais `sleep` | `await_until` |
| Namespacing | Clés UUID fraîches par scénario pour l'isolation parallèle | (discipline dans chaque harness) |

---

## 3. Modèle Public & Surface de Contrat &nbsp;·&nbsp; CORE

| Élément | Nature | Frontière de contrat / invariant gardée |
|---|---|---|
| `containers::*_ready` | boot+migrate paresseux | Backé par `OnceCell` ; un ensemble de containers par binaire de test ; ports OS-mappés |
| `containers::ensure_topics` | helper Kafka | Pré-création explicite de topics (pas de races d'auto-create) |
| `migrate::*_apply` | runner idempotent | ScyllaDB `SimpleStrategy RF=1` / Postgres SQL brut, appliqué une fois |
| `await_until(label, deadline, probe)` | primitive de sync | LA règle anti-flake — sonder, jamais sleep fixe |

---

## 4. Propriété & Frontières Architecturales &nbsp;·&nbsp; CORE

**Ce crate possède :**
- Les parties identiques à travers chaque suite live de service : orchestration de containers, runners de
  migration, et la primitive de synchronisation.

**Ce crate ne possède délibérément PAS / ne doit PAS lier :**

| Préoccupation | Vit dans | Pourquoi l'arête pointe ainsi |
|---|---|---|
| Les scénarios + le graphe de composition root | le `tests/<svc>_it/` de chaque service | Spécifique au service ; pas identique |
| La discipline de namespacing | chaque harness | Le crate fournit l'infra partagée ; l'isolation est une règle par scénario |
| Tout chemin de code de production | — | Il est dev-only ; jamais sous `[dependencies]` |

**La liste « do-not-depend-on » :** il ne doit jamais apparaître sous les `[dependencies]` d'un service — seulement
`[dev-dependencies]`. Le lier dans un binaire tire `testcontainers`/`rdkafka` en production.

---

## 5. Invariants & Règles de Contrat &nbsp;·&nbsp; CORE

| # | Invariant | Appliqué à | En cas de violation |
|---|---|---|---|
| I1 | Dev-only — jamais lié dans un binaire de service | placement de dépendance (`[dev-dependencies]`) | l'échafaudage de test fuit en prod |
| I2 | Un ensemble de containers par binaire de test ; les backends bootent paresseusement une fois | `OnceCell` | conflits de port / boots gaspillés |
| I3 | Chaque endpoint utilise le port hôte mappé assigné par l'OS | `containers` | collisions de port entre suites parallèles |
| I4 | Migrations appliquées exactement une fois (adaptation mono-nœud) | `OnceCell` + runners | erreurs de réplication sur un nœud |
| I5 | Zéro sleep fixe — la synchronisation est `await_until` uniquement | la primitive (discipline) | CI flaky |

---

## 6. Flot de Contrôle & Cycle de Vie &nbsp;·&nbsp; DEEP

**Par binaire de test.** Un scénario appelle p.ex. `containers::scylla_ready("chat", "migrations")` : le backend
boote paresseusement via un `OnceCell` (partagé par chaque scénario du binaire), les migrations s'appliquent une
fois (adaptation mono-nœud), et l'endpoint OS-mappé est retourné. Le harness pilote ensuite le `App::build` du
service contre cet endpoint, lance un scénario (frappant des clés UUID fraîches pour l'isolation), et asserte
avec `await_until(label, deadline, probe)` — sondant l'état observable jusqu'à vrai ou la deadline, jamais en dormant.

---

## 7. Couplage de Crate (tranche du graphe de dépendances) &nbsp;·&nbsp; DEEP

| Crate voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `scylla-storage` / `sqlx` | amont | Conformist | runners de migration | le setup de schéma des tests live |
| `testcontainers(-modules)` | amont | Conformist | boot de container + ports mappés | toute l'orchestration |
| la suite live de chaque service | aval | Published Contract | `*_ready` + `await_until` | chaque suite d'intégration |

> **Seam de stabilité :** `await_until` et les points d'entrée `*_ready` sont le contrat partagé sur lequel
> chaque suite se construit ; le placement dev-only est lui-même une règle architecturale imposée.

---

## 8. Signaux Émis & Effets de Bord &nbsp;·&nbsp; DEEP

N/A en tant que signal de production — il n'émet que du `tracing` de temps-de-test. Effets de bord (temps-de-test) :
démarre des containers Docker (labellisés `core-platform.test-support.pid=<PID>`, supprimés quand le process de test se termine ou est interrompu), applique des migrations, crée des topics Kafka. Rien de cela n'atteint un binaire déployé.

---

## 9. Décisions & Justification &nbsp;·&nbsp; DEEP

| Décision | Où consignée | Statut |
|---|---|---|
| Extraire les cinq piliers identiques du gold standard `chat` | [`README §Architecture`](../README.md) | Accepted |
| `await_until` comme primitive de synchronisation unique (anti-flake) | [`README §Architecture`](../README.md) | Accepted |
| Isolation par namespacing, pas teardown (suites parallèles sur containers partagés) | [`README §Architecture`](../README.md) | Accepted |
| Dev-only — jamais une dépendance de production | [`README §Architecture`](../README.md) | Accepted |

---

## 10. Classification & Évolution &nbsp;·&nbsp; DEEP

- **Classification :** Supporting — infrastructure de test ; le levier est l'uniformité + la règle anti-flake imposée.
- **Stabilité :** contrat stable — les cinq piliers sont stabilisés.
- **Volatilité :** faible — les nouveaux backends sont ajoutés comme de nouvelles features `testcontainers-modules` + un point d'entrée `*_ready`.
- **Capacités différées :** aucune ; les nouveaux modules de backend sont additifs.

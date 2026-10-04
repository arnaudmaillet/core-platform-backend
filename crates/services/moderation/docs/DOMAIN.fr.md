---
i18n:
  source: ./DOMAIN.md
  source_sha256: 984f760630736fbdeceb678c79fa3035dedd397cd57457861f26e162f7971351
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `moderation` — Contrat de Domaine & Fonctionnel

> **Domain Card**
>
> | | |
> |---|---|
> | **Bounded Context** | Confiance, Sécurité & Intégrité — la *décision d'intégrité de référence* |
> | **Classe de sous-domaine** | **Core** — pour un réseau UGC, l'enforcement d'intégrité est différenciant et sur-mesure ; la sécurité *fait* partie de la valeur produit |
> | **System of …** | **Record** pour « quelle action a été prise contre quelle entité, sous quelle version de politique, avec quelle preuve » |
> | **Racine(s) d'agrégat** | `Case`, `Decision`, `EnforcementAction`, `Appeal`, `PenaltyLedger` (`domain::aggregate`) |
> | **Tier** | **TIER-0** |
> | **Posture de défaillance** | **Split par catégorie** — le `Screen` CSAM/NCII/TVEC échoue **fermé** ; tout le reste est async/fail-open |
> | **Contextes amont** | `post`, `comment`, `chat`, `media` (contenu + Screen) ; services de classifieurs (signaux) ; utilisateurs (signalements) — via **ACL** sur Kafka/gRPC |
> | **Contextes aval** | `timeline`, `chat`, `account` (dénorm enforcement Plane-B) ; `audit` (preuve `decision_recorded`) ; `account` (exécution de suspension gRPC) — via **Open-Host Service / Published Language** |
> | **Journal de décisions** | [`ADR-0002`](../../../../docs/adr/0002-moderation-decision-enforcement-sor-with-fail-closed-screen.md) |

---

## 1. Capacité Métier & Non-Objectifs

**Capacité.** `moderation` est l'autorité pour **les décisions d'intégrité et l'enforcement** : il
répond à **« cet acteur est-il restreint / ce contenu est-il actionné, par quelle autorité, et
peut-on le prouver à un régulateur ? »**

**Le problème difficile.** Modérer au *volume d'écriture* du réseau sans devenir un goulot de latence
global. Un design naïf appelle un RPC de modération à chaque post/message/upload, taxant chaque
écriture et couplant la disponibilité du contenu à une panne d'intégrité. Le pattern résolvant est un
**split à trois plans** qui découple la voie lourde de classification/revue de la voie chaude de
décision.

**Non-objectifs — ce que ce contexte ne fait délibérément PAS :**
- ❌ Classer le contenu par ML en ligne → les classifieurs sont des *producteurs* de signaux amont ; `Screen` est un lookup hash/blocklist sans inférence.
- ❌ Stocker le contenu → il référence le contenu par `SubjectRef`, ne détient jamais les octets (`media`/`post` les possèdent).
- ❌ Être l'UI de revue → la console d'ops est un appelant séparé de l'API case/appeal.
- ❌ Servir l'enforcement sur le hot read path via RPC → la flotte lit le **Plane B** (événements + projection Redis), pas `GetEnforcementState`.

---

## 2. Langage Omniprésent

| Terme | Sens dans ce contexte | Symbole de code |
|---|---|---|
| Subject | La cible normalisée de modération : type d'entité + id + acteur + surface | `SubjectRef` |
| Case | Une unité de revue ouverte sur un sujet quand des signaux franchissent un seuil | `Case` |
| Decision | Une décision append-only, auditable (l'entrée du registre de preuve légale) | `Decision`, `DecisionAuthor` |
| Enforcement action | La conséquence appliquée, versionnée monotone par sujet | `EnforcementAction`, `EnforcementVersion` |
| Strike / penalty ledger | L'historique d'enforcement gradué qui escalade les conséquences | `Strike`, `PenaltyLedger`, `PenaltyPolicy` |
| Appeal | Le recours d'un sujet contre une décision ; la résolution est une *nouvelle* décision | `Appeal`, `AppealStatus` |
| Signal / report | Un verdict de classifieur / un signalement d'abus utilisateur alimentant le Plane A | `Signal`, `Report` |
| Screen | La porte étroite, synchrone, fail-closed de pré-publication (Plane C) | (RPC `Screen`) |
| Policy category / version | La politique sous laquelle une décision a été prise ; épinglée pour l'auditabilité | `PolicyCategory`, `PolicyVersion` |

---

## 3. Modèle de Domaine

| Élément | Type | Frontière d'invariant gardée |
|---|---|---|
| `Case` | racine d'agrégat | Un sujet en revue ; clé par UUIDv5 déterministe de l'identité du sujet (ouverture idempotente) |
| `Decision` | racine d'agrégat | Décision append-only ; une réversion est une *nouvelle* décision, jamais une mutation |
| `EnforcementAction` | racine d'agrégat | Porte une `EnforcementVersion` monotone par-sujet — une réversion ne peut devancer une ré-application |
| `Appeal` | racine d'agrégat | Cycle de vie du recours ; la résolution émet une nouvelle `Decision` |
| `PenaltyLedger` | racine d'agrégat | État d'escalade gradué par acteur |
| `SubjectRef` / `EntityType` / `ActorId` | VO | La cible d'intégrité normalisée — jamais de champs vendeur ou internes au contenu |
| `PolicyCategory` / `PolicyVersion` / `Confidence` | VO | La base de politique de la décision et sa certitude |

**Cycle de vie (case → enforcement) :**

```
signals/report --(threshold)--> Case opened --(review/decide)--> Decision recorded --> EnforcementAction applied (v+1)
                                                     ▲                                          │
                                                 Appeal filed ◄───────────── (reversal = new Decision) ──┘
```

> **Transitions légales uniquement.** Les décisions ne sont jamais rétro-mutées (registre
> append-only) ; une réversion est une nouvelle `Decision`. L'`EnforcementVersion` et le schéma de
> hash `Screen` ne doivent jamais changer après l'existence de données.

---

## 4. Propriété des Données & Frontières

**Ce contexte est la source de vérité pour :**
- Cases, decisions (WORM), appeals, penalty ledger, policy versions, et l'enregistrement de chaque auteur de ses signalements (`reports`) — **Postgres** db `moderation`. Le registre de décisions est append-only.
- Historique de signaux / preuves — **ScyllaDB** keyspace `moderation` (TWCS).
- La projection d'enforcement + le corpus de hash Screen — **Redis** (dérivé/reconstructible).

**Ce contexte détient des copies qu'il ne possède PAS (read-model / dénormalisation) :**

| Donnée copiée | Possédée par | Maintenue fraîche via | Tolérance d'obsolescence |
|---|---|---|---|
| Existence/métadonnées de contenu pour construire un `SubjectRef` | `post`, `comment`, `chat`, `media` | `post.v1.events`, `comment.*`, contenu chat (Plane A) | post-hoc (publication optimiste) |
| Verdicts de classifieur | services de classifieurs | `moderation.signals` | best-effort |

**La liste « ne-pas-écrire » :** moderation ne mute jamais le contenu (il le référence) ; la
suspension de compte est *exécutée* par `account` via gRPC — moderation enregistre la décision et
demande l'exécution, il ne possède pas l'état du compte.

---

## 5. Invariants & Règles Métier

| # | Invariant | Imposé à | En cas de violation |
|---|---|---|---|
| I1 | Les décisions sont append-only — une réversion est une nouvelle décision | domaine + Postgres | `MOD-2xxx` |
| I2 | `Screen` est déterministe et sans inférence (hash/blocklist seulement ; ML jamais en ligne) | frontière infrastructure | — |
| I3 | Politique de défaillance par catégorie — CSAM/NCII/TVEC échouent **fermé** ; spam/borderline échouent **ouvert** | application | `MOD-7002`/`MOD-7003` |
| I4 | La version `EnforcementAction` est monotone par sujet (la réversion ne peut devancer la ré-application) | domaine | concurrence `MOD-9xxx` |
| I5 | Les Cases sont idempotents — clé par UUIDv5 déterministe de l'identité du sujet | domaine (consumer) | la dédup se replie en `Ok` |
| I6 | Les RPC d'ops mutants sont privilégiés-relecteur — pas auto-autorisés | frontière de déploiement (auth-context) | `PERMISSION_DENIED` |

---

## 6. Workflows & Orchestration

> En ligne jusqu'à ce qu'un C4 corrigé soit régénéré depuis `docs/domain/`.

**Plane A — ingestion async (hors du write path, fail-open).**
1. Le contenu `*.created` / les signalements / les signaux de classifieurs arrivent sur Kafka.
2. `moderation-ingestion-consumer` exécute des vérifications déterministes bon marché (blocklist, hash known-bad, historique d'acteur).
3. Fan-out vers les classifieurs async ; ouvre un `Case` quand les signaux franchissent un seuil.
- **Idempotence :** Cases clé par UUIDv5 du sujet ; les skips intentionnels (block-gated, self-target, dédup) se replient en `Ok`.

**Décision → enforcement (gradué).** Une `Decision` est enregistrée sous une `PolicyVersion`
épinglée ; le moteur de pénalités escalade via le `PenaltyLedger` ; une `EnforcementAction` est
appliquée à la version *v+1* et projetée vers Redis + émise sur `moderation.v1.events`.

**Plane B — état d'enforcement (lecture chaude, O(1)).** La flotte lit `mod:enf:{actor:<id>}` depuis
la projection Redis reconstruite par les consumers — jamais un RPC par-item.

**Plane C — porte Screen (sync, fail-closed).** `media`/`post` appellent `Screen` en pré-publication
pour les catégories catastrophiques. Un timeout dur (`MODERATION_SCREEN_TIMEOUT_MS`, défaut 200ms) +
disjoncteur le bornent ; à l'échéance/panne la porte retourne `MOD-7002` et l'appelant bloque
l'upload.

**Appel.** Un sujet dépose un recours ; la résolution enregistre une nouvelle `Decision`
(éventuellement une réversion) et une réversion d'enforcement correspondante.

---

## 7. Relations de Contexte (extrait de Context-Map)

| Contexte voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `post`/`comment`/`chat`/`media` | amont | ACL | `*.events` de contenu → `SubjectRef` (Plane A) | un changement de schéma de contenu casse la construction du sujet |
| services de classifieurs | amont | ACL | `moderation.signals` | perte des signaux ML → le moteur dégrade vers les règles déterministes |
| `media`/`post` | aval | OHS (sync) | RPC `Screen` | un changement du contrat `Screen` casse la porte de publication |
| `timeline`/`chat`/`account` | aval | Published Language | `moderation.v1.events` (Plane B) | la dénorm d'enforcement casse |
| `audit` | aval | Published Language | `moderation.v1.events` · `decision_recorded` | la piste de preuve de conformité casse |
| `account` | aval | Customer/Supplier (gRPC) | exécution de suspension/bannissement | les actions de cycle de vie ne peuvent s'appliquer (décision quand même enregistrée, retryée) |

> **Anti-Corruption Layer :** les consumers d'ingestion Plane-A traduisent chaque forme wire de
> contenu/signal vers les types domaine normalisés `SubjectRef` / `Signal` — les champs vendeur et
> internes au contenu ne fuient jamais dans le modèle.

---

## 8. Événements de Domaine (sémantique, pas wire)

> Sens seulement ; le schéma wire est possédé par le proto/README. Consolidation dans `docs/domain/EVENT_CATALOG.md`.

| Événement (sur `moderation.v1.events`) | Signifie | Émis quand | Qui réagit |
|---|---|---|---|
| `decision_recorded` | une décision d'intégrité faisant autorité a été prise — porte *qui a décidé* et *pourquoi* (statement-of-reasons DSA) | une décision est enregistrée (auto-screen, revue humaine, réversion d'appel) | `audit` — scelle la justification en enveloppe crypto-shreddable |
| `enforcement_applied` / `enforcement_reversed` | une conséquence a été appliquée / levée contre un acteur (versionnée) | l'action d'enforcement commite | `timeline`, `chat`, `account` — dénorm Plane-B |
| `case_opened` / `case_resolved` | une unité de revue a été ouverte / fermée | seuil d'ingestion / action du relecteur | consommateurs Plane-B |
| `appeal_resolved` | un appel a été tranché | résolution d'appel | consommateurs Plane-B |

> Tous les événements sont clé par `actor_id` pour l'ordonnancement par acteur. `decision_recorded`
> est la variante de preuve de conformité ; les consommateurs offender-centric l'ignorent, `audit`
> consomme seulement celui-ci + `enforcement_applied`.

---

## 9. Décisions & Justification

| Décision | ADR | Statut |
|---|---|---|
| Moderation est le SoR décision/enforcement avec une porte Screen étroite fail-closed (split à trois plans) | [`ADR-0002`](../../../../docs/adr/0002-moderation-decision-enforcement-sor-with-fail-closed-screen.md) | Accepté |
| `decision_recorded` comme événement de preuve face à audit (vs mapper les événements offender-centric) | _voir contexte ADR-0001_ | Accepté |

---

## 10. Classification de Sous-domaine & Évolution

- **Classification :** Core — la confiance & sécurité est différenciante pour une plateforme UGC ; le design à trois plans et l'enforcement gradué sont sur-mesure.
- **Volatilité :** moyenne — les catégories de politique, le moteur de pénalités et l'intégration de classifieurs évoluent avec le produit et la réglementation ; le *registre* et la discipline de version sont stables.
- **Dette de modélisation connue :** l'autorisation de gateway pour les RPC d'ops mutants est un `<TODO>` de déploiement (le service ne s'auto-autorise pas).
- **Capacités différées :** reporting de transparence DSA plus riche ; profondeur d'ingestion de contenu `chat` ; réputation d'acteur inter-surfaces.

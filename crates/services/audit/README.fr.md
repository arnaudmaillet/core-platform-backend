---
i18n:
  source: ./README.md
  source_sha256: 8ce5443647855324bb8178b5979608bf1a509bc3dcbc078fc6880c09f6feb787
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `audit` — Enregistrer qui a fait quoi, à qui, quand et sous quelle autorité — une fois, immuablement, pour toujours — et prouver que rien n'a jamais été altéré

> **Fiche de service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Propriétaire** | `<TODO: équipe>` · `<TODO: #canal-slack>` |
> | **Astreinte / escalade** | `<TODO: rotation-astreinte>` → `<TODO: politique-escalade>` |
> | **Niveau** | **TIER-0** — plan de preuve à criticité légale/réglementaire. **Posture hybride :** *fail-open côté producteurs* (la disponibilité de l'audit ne peut jamais dégrader le maillage métier) mais *fail-closed sur la durabilité et sur la voie synchrone « bris de glace »* (les actions les plus dangereuses sont refusées si elles ne peuvent être enregistrées de façon prouvable au préalable) |
> | **Déployable** | **deux** binaires — `crates/apps/audit-server` (plan lecture/enregistrement : lectures gRPC + RPC synchrone `RecordPrivileged`) **et** `crates/apps/audit-worker` (plan ingestion/vérification : voie `run_consumer` + boucles vérification/ancrage/rétention/effacement). Crate bibliothèque : `crates/services/audit` |
> | **Écouteurs** | **gRPC** interne uniquement — `:50068` (server : lectures + `RecordPrivileged`) · `:50069` (worker : health/reflection). **Aucun écouteur public/client** — les clients ne parlent jamais à l'audit |
> | **Stockages** | **Postgres** en append-only (registre canonique chaîné par hash, `UPDATE`/`DELETE` révoqués) + archive WORM **S3/MinIO Object Lock** (long terme, mode compliance) + signataire **KMS/HSM** & coffre des DEK par sujet (un domaine de confiance distinct) |
> | **Asynchrone** | **consomme** `audit.v1.events` (le topic d'événements de conformité de toute la flotte) + les flux de décisions de `moderation` / `auth` / `account` (Kafka). **Ne publie** rien de référence (l'audit est un puits terminal) |
> | **Appelants amont** | services de la flotte émettant des événements de conformité (async) + un ensemble restreint d'appelants d'actions privilégiées (`RecordPrivileged` synchrone) ; outillage DPO / audit interne / régulateur (lecture/export) |
> | **Dépendances aval** | Postgres, magasin Object-Lock, KMS/HSM, un ancrage/témoin externe (horodatage RFC 3161 et/ou un bucket WORM sur un compte séparé), Kafka. La correspondance identité↔pseudonyme reste dans `account` — l'audit ne la détient jamais |
> | **SLO** | `<TODO>` durabilité d'ingestion (zéro perte pour les événements en périmètre) · `<TODO>` `RecordPrivileged` p99 · `<TODO>` lecture p99 · cadence de vérification d'intégrité `<TODO>` |

---

## 🎯 Aperçu & rôle du service

`audit` est le **plan de preuve de conformité inviolable** de la plateforme : le System of Record append-only chaîné par hash qui répond à *« qui a fait quoi, à qui, quand, sous quelle autorité et avec quel résultat »* pour chaque événement pertinent en matière de sécurité, de vie privée et de réglementation dans la flotte.

Ce n'est emphatiquement **pas** de la « journalisation, mais en sérieux ». La télémétrie applicative (traces, métriques, logs de debug) et une piste de conformité sont deux substances différentes qui se ressemblent seulement à l'écran, et les confondre est une erreur de catégorie qu'un système à grande échelle sanctionne. La **télémétrie** est best-effort, mutable, échantillonnée, à rétention cyclique, et lue par chaque ingénieur — correct pour l'observabilité, fatal pour la preuve. Une **piste d'audit** doit être sans perte pour les événements en périmètre, append-only, inviolable, prouvablement *complète*, conservée des années, à accès contrôlé (et ses propres lectures auditées), et effaçable au niveau du *champ* sans détruire l'enregistrement. Dès qu'un identifiant PII réel atterrit dans un index Loki, vous avez créé une copie incontrôlée de données personnelles sans primitive d'effacement par champ — et une demande GDPR Art. 17 contredit alors le devoir de responsabilité de l'Art. 5(2). Un tableau de bord ne peut pas non plus répondre à la seule question qu'un régulateur ou un auditeur SOC2 pose réellement : *pouvez-vous prouver que cet enregistrement est complet et inaltéré, et que vous le détecteriez s'il ne l'était pas ?* C'est ce que ce service existe pour fournir.

**Objectifs principaux :** (1) un **registre append-only sans perte** d'événements de conformité ; (2) **l'inviolabilité même face à un opérateur hostile** — un DBA compromis ou un admin malveillant ne peut pas réécrire ou tronquer l'historique sans être détecté ; (3) réconcilier le **paradoxe effacement GDPR ⇄ rétention d'audit** via le crypto-effacement, pour que les PII puissent être irréversiblement détruites pendant que l'enregistrement et sa preuve survivent ; (4) **ne jamais devenir un goulot d'étranglement ni un SPOF** — les producteurs sont découplés derrière Kafka et ne bloquent jamais sur l'audit ; (5) une surface de lecture/export **restreinte et à accès contrôlé** pour DPO / audit interne / régulateurs, dont chaque usage est lui-même un événement d'audit.

| Préoccupation | Chemin | Posture | Notes |
|---|---|---|---|
| **Ingestion en masse** | Kafka async (`audit.v1.events`, `run_consumer`) | fail-open au producteur / zéro perte via le log | ~99 % du trafic ; un pic d'écriture devient du *retard* de consommateur, jamais du backpressure producteur |
| **Actions privilégiées** | gRPC synchrone `RecordPrivileged` sur `:50068` | **fail-closed** | bris de glace / legal hold / changements de consentement — *refusés* tant qu'ils ne sont pas enregistrés de façon prouvable |
| **Lecture / export** | gRPC synchrone `Query` / `Export` / `VerifyIntegrity` | accès contrôlé, lui-même audité | DPO / audit interne / régulateur ; besoin d'en connaître + séparation des tâches |

---

## 📐 Architecture & concepts

Hexagonal / DDD (`domain` → `application` → `infrastructure`), Kafka pour l'ingestion en masse, un registre Postgres append-only comme magasin canonique, une archive WORM Object-Lock pour l'immuabilité long terme, et KMS/HSM pour la signature + la garde des clés par sujet. Le choix structurel déterminant est **deux déployables** — un server lecture/enregistrement et un worker ingestion/vérification — qui partagent une crate de domaine mais aucun processus, déploiement ou domaine de panne.

```
    services de la flotte (moderation · auth · account · actions privilégiées partout)
        │ émission async (fire-and-forget)        │ synchrone, à enregistrer d'abord
        ▼                                         ▼
   ┌──────────┐                          ┌──────────────────┐
   │  Kafka   │  audit.v1.events         │  audit-server    │  RecordPrivileged
   │ (tampon) │  + flux de décisions     │  :50068 (gRPC)   │  (FAIL-CLOSED)
   └────┬─────┘                          │  Query/Export/   │  + lectures Query/Export
        │ run_consumer                   │  VerifyIntegrity │  (accès contrôlé,
        ▼                                └────────┬─────────┘   elles-mêmes auditées)
   ┌─────────────────────────────────────────────┴───────────┐
   │  audit-worker  — dédup → CHAÎNAGE → persiste → archive    │
   │  + boucles vérif. / ancrage-checkpoint / rétention / shred│
   └───┬─────────────────────┬──────────────────────┬─────────┘
       ▼                     ▼                       ▼
  ┌──────────┐        ┌──────────────┐        ┌──────────────┐
  │ Postgres │ chaîne │  Object Lock │ archive│  KMS / HSM   │ signe les checkpoints
  │ registre │ de     │ (S3/MinIO)   │ WORM   │  + coffre DEK│ + DEK par sujet
  │ INSERT   │ hash   │ mode         │        │ (domaine de  │ (crypto-effacement)
  │  seul    │        │ compliance   │        │  confiance   │
  └──────────┘        └──────────────┘        │  distinct)   │
                                              └──────┬───────┘
                                                     ▼
                                       ancrage / témoin externe
                                  (TSA RFC 3161 · bucket WORM compte séparé)
```

**L'immuabilité est une défense en profondeur à trois couches.** (A) **Chaîne de hash** — chaque enregistrement porte `H(prev_hash ‖ canonical(payload) ‖ sequence_no)` dans une chaîne append-only par partition ; toute mutation, réorganisation ou suppression casse chaque hash en aval, et le `sequence_no` monotone par partition rend triviale la détection de troncature/trou. (B) **WORM d'infrastructure** — le registre Postgres a `UPDATE`/`DELETE` révoqués au niveau du rôle (le service ne peut qu'`INSERT`), et l'archive long terme utilise Object Lock en *mode compliance*, où même le compte root ne peut supprimer avant l'expiration de la rétention. (C) **Checkpoints signés et ancrés à l'externe** — le worker signe périodiquement une racine de Merkle sur toutes les têtes de partition avec une clé détenue par un principal KMS/HSM *séparé*, puis l'ancre à un témoin indépendant ; un vérificateur autonome recalcule la chaîne et compare. Pour falsifier l'historique sans être détecté, un attaquant devrait contrôler simultanément quatre domaines de confiance distincts.

> **Invariants** (et où ils sont appliqués) :
> - **Les PII n'entrent jamais en clair dans la chaîne.** Un sujet est référencé par un pseudonyme opaque ; les PII inévitables vont dans une **enveloppe crypto-effaçable** par sujet (la chaîne hache le *chiffré*) — domaine + infrastructure.
> - **L'effacement est une destruction de clé, pas une suppression d'enregistrement.** Une demande GDPR Art. 17 détruit la DEK du sujet ; les PII deviennent définitivement indéchiffrables tandis que l'enregistrement, sa séquence, son hash et les métadonnées de conformité non-PII restent intacts et vérifiables — application.
> - **La rétention légale prime sur l'effacement.** Un sujet sous legal hold actif (GDPR Art. 17(3)) n'est pas effacé ; un shred sélectif par champ conserve l'enregistrement de décision pseudonymisé — domaine.
> - **Les actions les plus dangereuses échouent en mode fermé.** `RecordPrivileged` refuse l'action tant que la durabilité n'est pas confirmée — application.
> - **Les lectures sont aussi des preuves.** Chaque query/export est elle-même enregistrée ; l'accès est besoin-d'en-connaître avec séparation des tâches — application.
> - **L'audit est un puits terminal.** Il enregistre les décisions que d'autres services prennent ; il n'en prend aucune et ne publie rien de référence — domaine.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### gRPC interne — `audit.v1` *(Phase 1)*

Il n'y a **aucune interface côté client.** La surface interne est un service gRPC `audit.v1` restreint sur `:50068` : le `RecordPrivileged` **synchrone, fail-closed** (utilisé uniquement pour la classe à-enregistrer-avant-d'autoriser), et les lectures `Query` / `Export` / `VerifyIntegrity` à accès contrôlé pour DPO / audit interne / régulateurs. Le worker (`:50069`) ne sert que health/reflection. *(Le proto arrive en Phase 1 — pas encore de code.)*

### Ports Rust (contrat hexagonal) *(Phase 3)*

```rust
#[async_trait] pub trait LedgerStore      { /* insert chaîné par hash append-only par partition + lecture */ }
#[async_trait] pub trait WormArchive      { /* écriture/lecture Object-Lock (mode compliance) long terme */ }
#[async_trait] pub trait KeyVault         { /* signe les checkpoints · génère/détruit la DEK par sujet (crypto-shred) */ }
#[async_trait] pub trait CheckpointAnchor { /* publie/vérifie la racine de Merkle signée contre le témoin externe */ }
#[async_trait] pub trait EventSource      { /* les flux Kafka amont de conformité + de décisions */ }
```

### Contrat d'erreur

Chaque faute implémente `error::AppError` avec un code `AUD-XXXX` stable, mappé vers gRPC `Status` / HTTP par la crate partagée `error` :

| Plage | Classe |
|---|---|
| `AUD-1xxx` | intake d'événement / validation de contrat |
| `AUD-2xxx` | **intégrité du registre** (chaîne de hash, trous de séquence, divergence checkpoint/témoin — alarmer, jamais réessayer) |
| `AUD-3xxx` | autorisation de lecture d'audit (la surface de lecture privilégiée ; elle-même auditée) |
| `AUD-4xxx` | disponibilité du plan de stockage (le cœur de durabilité ; réessayable ; la voie synchrone échoue fermée sur `AUD-4004`) |
| `AUD-5xxx` | crypto-effacement / cycle de vie des clés (le pattern d'effacement GDPR) |
| `AUD-6xxx` | rétention / legal hold |
| `AUD-8xxx` | surface d'ingestion asynchrone (`run_consumer`) |
| `AUD-9xxx` | transverse (domaine/parsing) |

---

## 📨 Événements & contrat asynchrone &nbsp;·&nbsp; CORE

> Les topics Kafka sont une API. Un changement de schéma dans un topic consommé casse la piste d'audit exactement comme un changement de proto.

**Publie :** rien de référence. L'audit est un puits de preuve terminal. (Un signal d'alarme d'intégrité sur falsification/trou/divergence-témoin est opérationnel, pas un flux System-of-Record.)

**Consomme** *(Phase 4)* :

| Topic | Groupe de consommateurs | But | Sur poison/épuisement |
|---|---|---|---|
| `audit.v1.events` | `audit-ingest` | le firehose d'événements de conformité de toute la flotte → dédup → chaîne → persiste → archive. Produit aujourd'hui par `media` : chaque consultation par le staff d'un document privé (`data_access`, `media.private_document.viewed`, #837) | DLQ `audit.v1.events.dlq` |
| `moderation.v1.events` ✅ câblé | `audit-moderation` | `decision_recorded` (l'autorité + le motif DSA — scellé dans une enveloppe crypto-effaçable à l'ingestion) et `enforcement_applied` ; les autres variants sont un skip inoffensif | DLQ `moderation.v1.events.dlq` |
| `auth.v1.events` ✅ câblé | `audit-auth` | `session_issued` / `session_revoked` (le cycle de vie d'authentification — métadonnées structurées, sans PII, sans scellement) ; les autres variants sont un skip inoffensif | DLQ `auth.v1.events.dlq` |
| `account.v1.events` ✅ câblé | `audit-account` | toute la surface account — `account_created` / `email_changed` / `email_verified` / `phone_changed` porteurs de PII (scellée dans une enveloppe crypto-effaçable), sécurité (`password_changed`, `mfa_*` → Authentication), cycle de vie d'identité (`activated`/`deactivated`/`suspended`/`deleted`, `kyc_status_changed` → **Identity**), autorisation (`role_*` → Authorization), les événements GDPR (`gdpr_deletion_requested` / `gdpr_deletion_cancelled` → DataErasure, `gdpr_data_export_requested` → DataExport) et `consents_updated` → **Consent** (preuve art. 7(1)) — et `deleted` (l'effacement lui-même, après le délai de grâce de 30 jours) **crypto-efface aussi le sujet** (Art. 17, boucle bouclée) | DLQ `account.v1.events.dlq` |

> **Contrat runtime (obligatoire) :** tous les consommateurs tournent sous `run_consumer` — commit manuel uniquement après que l'événement est persisté de façon durable *et* chaîné, retry borné avec backoff + jitter, DLQ sur poison/épuisement. **Aucun offset commité n'avance jamais au-delà d'un événement non persisté → zéro perte.** **Idempotence :** les événements portent un id UUIDv5 déterministe ; une redélivrance est dédupliquée (`AUD-1004`, replié dans `Ok`), donc chaque événement logique apparaît exactement une fois dans la chaîne. Un événement sans rien d'enregistrable (`AUD-8002`) est un skip inoffensif replié dans `Ok`. Les chaînes par partition gardent le chemin d'écriture parallèle (pas de sérialisation globale) ; une racine de Merkle globale périodique recoud les têtes de partition.

---

## 🌩️ Modes de défaillance & dégradation &nbsp;·&nbsp; OPS

| Défaillance | Symptôme | Comportement du service | Action opérateur |
|---|---|---|---|
| Registre Postgres KO | l'ingestion cale | **fail-open au producteur** — les événements tamponnent dans Kafka ; offsets non commités → pas de perte (`AUD-4001`) | restaurer Postgres ; le consommateur vide le backlog |
| Archive Object-Lock KO | l'archivage prend du retard | le registre reste canonique ; l'archive rattrape (`AUD-4002`) | restaurer l'archive ; réconcilier |
| KMS/HSM / coffre DEK KO | impossible de signer / effacer | le chaînage continue ; checkpoint + shred différés (`AUD-4003`) | restaurer le coffre ; reprendre les boucles ancrage/shred |
| Témoin externe KO | checkpoints non témoignés | chaîne intacte ; ancrage différé (`AUD-2005`) | restaurer le témoin ; ré-ancrer |
| **`RecordPrivileged` ne peut confirmer la durabilité** | action privilégiée bloquée | **fail-closed** — action refusée (`AUD-4004`) ; l'appelant réessaie/abandonne | confirmer la santé du stockage ; le refus est correct |
| Mismatch de hash / trou de séquence | le vérificateur alerte | **alarmer, ne pas réessayer** (`AUD-2001`/`AUD-2002`) — signal de falsification/troncature | **traiter comme un incident de sécurité** ; isoler, enquêter |
| Checkpoint ≠ témoin | le vérificateur alerte | **alarmer** (`AUD-2004`) — signal de falsification au niveau opérateur | **incident de sécurité** ; forensique inter-domaines |
| Pic d'écriture (50×) | le retard consommateur monte | Kafka l'absorbe ; l'audit vide à son rythme ; pas de perte | scaler `audit-worker` ; surveiller le lag |
| Effacement vs legal hold | shred refusé | la rétention légale gagne (`AUD-5002`) | confirmer le hold ; shred sélectif par champ si applicable |

**Backpressure & limites :** le log Kafka durable est le tampon (les producteurs ne bloquent jamais) ; chaînage parallèle par partition ; timeouts durs sur les appels registre/archive/coffre/témoin ; le délai de commit durable de la voie synchrone (`AUD-4004`).

---

## 📦 Intégration & usage &nbsp;·&nbsp; CORE

```toml
[dependencies]
audit = { path = "crates/services/audit" }
```

Bibliothèque uniquement. Implémente [`service_runtime::Service`](../../platform/service-runtime/README.md) **deux fois** (Phase 5) : `audit::AuditServerService` (le binaire `audit-server` — les lectures gRPC + le RPC synchrone fail-closed `RecordPrivileged`, plus les sondes de santé backend) et `audit::AuditWorkerService` (le binaire `audit-worker` — la voie d'ingestion `run_consumer` supervisée + la boucle d'ancrage-checkpoint, aucun RPC de domaine ; les boucles crypto-shred et expiration-rétention attendent leurs sources d'entrée — voir *Différé*). La télémétrie, la config + le hot-reload, la santé et l'arrêt gracieux sont la responsabilité du runtime.

> **État de build :** **terminé jusqu'à la Phase 7.** Les huit phases sont livrées : l'espace `AUD-XXXX` + deux binaires (P0), le contrat `audit.v1` (P1), le domaine pur d'inviolabilité — chaîne de hash, crypto-effacement, rétention/holds, checkpoint de Merkle (P2), la couche application + six ports + le commit partagé fail-open/fail-closed (P3), les adaptateurs d'infrastructure — registre Postgres append-only, archive WORM Object-Lock, coffre de clés, ancrage, ingestion `run_consumer` + le mapping proto/JSON (P4), le découpage en deux binaires avec le gRPC `AuditService` (P5), et la suite live + migrations (P6). **78 tests unitaires** plus une suite live `integration-audit` (vrais Postgres + MinIO Object Lock) couvrent le domaine, les mappings codec/decode, les six handlers, et de bout en bout sur les vrais adaptateurs : le roundtrip append→chaîne→archive→vérif, la **détection de falsification en place** (un `UPDATE` malveillant capté en `AUD-2001`), le **crypto-effacement** qui efface les PII tandis que la chaîne vérifie toujours, le round-trip du checkpoint, et la redélivrance idempotente. Durcissement + simulations de panne (P7) : la voie synchrone est enveloppée dans un délai dur de commit durable, exercé par une simulation prouvant qu'**une action bris-de-glace est refusée (`AUD-4004`) quand l'audit ne peut confirmer la durabilité** — et rien n'est enregistré. Revue de sécurité : aucun token, PII, payload ou chiffré n'est jamais journalisé (les sujets sont des pseudonymes ; l'enveloppe PII opaque n'est jamais journalisée), et aucun panic faillible dans un chemin chaud. **Durcissement KMS/témoin (issues #482/#483) :** la garde de la KEK et la signature des checkpoints passent à KMS (un client KMS SigV4 fait main, épinglé aux vecteurs de la suite de tests AWS SigV4), et la racine de Merkle signée est ancrée à un témoin WORM indépendant — clôturant la menace au niveau opérateur. **116 tests unitaires** + une suite live de **13 scénarios** (vrais Postgres + MinIO), dont le crypto-effacement contre le chiffreur adossé à KMS et une **double-falsification adversariale** (ligne du registre + pointeur Postgres) que `verify_global` capte toujours via la racine signée ancrée à l'externe.
>
> **Clos (anciennement différé) :**
> - **Garde de la KEK → KMS** *(issue #482)* — les DEK par sujet sont enrobées/désenrobées par KMS sous un principal que le rôle BD du registre ne peut endosser (`KmsSubjectCipher` derrière le port `SubjectCipher`). Aucune KEK brute ne vit jamais dans l'environnement ou la mémoire d'audit ; `subject_keys` est inchangé (le blob stocké est le chiffré KMS), et le crypto-effacement reste « supprimer la ligne de DEK enrobée ». Le `AesGcmSubjectCipher` à KEK d'environnement demeure le repli local/dev, choisi par config.
> - **Checkpoints signés + témoin externe** *(issue #483)* — la racine de Merkle est signée dans le domaine de confiance KMS et ancrée à un témoin WORM indépendant (`WitnessCheckpointAnchor`) ; `verify_global` valide la signature et réconcilie les têtes vivantes contre la racine **ancrée à l'externe**, donc un opérateur qui réécrit *à la fois* une ligne du registre **et** le pointeur Postgres `checkpoint_anchors` est tout de même pris (`AUD-2004`). Le pointeur Postgres ne subsiste qu'en index de convenance. Le `PgCheckpointAnchor` non signé demeure le repli local/dev. Couvert par un test d'intégration adversarial (double-falsification registre + pointeur, vrai témoin MinIO).
>
> **Différé (explicite, pas des lacunes) :**
> - **Le *provisionnement* KMS/témoin** reste un engagement IAM / structure organisationnelle — le code est en place derrière les ports, mais l'intégrité ne vaut que par la séparation entre le principal du registre, le principal KMS de signature/chiffrement et le témoin WORM compte-séparé. Le choix d'un horodateur RFC 3161 vs un bucket Object-Lock sur un second compte, et la politique de rotation des clés, sont des décisions ops ; local/dev + CI tournent sur les replis KEK-d'environnement + HMAC (ou LocalStack KMS).
> - **L'adoption par les producteurs** — **`moderation`, `auth` et `account` sont tous câblés.** les `decision_recorded` + `enforcement_applied` de moderation (motif scellé), les `session_issued` + `session_revoked` de auth (sans PII), et les `account_created` / `email_changed` de account (PII scellée) + les deux événements GDPR sont consommés et chaînés. Un `deleted` de account (l'effacement une fois son délai de grâce écoulé ; une *demande* de suppression est seulement enregistrée, puisqu'elle peut encore être annulée) **crypto-efface le sujet**, donc toute sa PII scellée à travers les flux devient illisible tandis que la chaîne vérifie toujours — la boucle d'effacement Art. 17, bouclée de bout en bout.
> - **Le consommateur de crypto-effacement** (nécessite une source de demandes d'effacement) et **le balayage d'expiration-rétention** (nécessite des politiques de rétention résolues) — les handlers existent et sont testés ; seules les boucles worker qui les pilotent attendent leurs sources.
> - **L'auto-audit des lectures** — enregistrer chaque requête/export autorisé comme événement `DATA_ACCESS` propre. L'authentification de l'appelant + l'autorisation par RPC (`AUD-3001`/`AUD-3002`/`AUD-3004`/`AUD-3005`) sont **câblées** : chaque RPC vérifie le token edge ES256 via `auth-context` et exige sa permission `audit:*`, deny-all si non configuré (`AUDIT_JWKS_URL` ci-dessous). En attendant les événements `DATA_ACCESS`, le principal autorisé + le RPC sont tracés comme piste d'accès intérimaire.
> - **La pagination** au-delà d'une page bornée ; **l'ancrage blockchain** (excessif — RFC 3161 + WORM compte-séparé suffit) ; **le streaming SIEM temps réel** ; **la génération automatisée de rapports de transparence DSA** ; **la réplication inter-région du registre**.

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables spécifiques à `audit` *(remplies par phase)*

| Variable | Requise | Défaut | Description |
|---|---|---|---|
| `AUDIT_SERVER_GRPC_ADDR` | Non | `0.0.0.0:50068` | server : adresse gRPC lectures + `RecordPrivileged` |
| `AUDIT_WORKER_GRPC_ADDR` | Non | `0.0.0.0:50069` | worker : adresse health/reflection (aucun RPC de domaine) |
| `AUDIT_KEK_BASE64` | Non (dev/repli) | clé dev | base64 de la KEK d'environnement de 32 octets enrobant les DEK par sujet — le **repli local/dev** `AesGcmSubjectCipher`. Supplanté en production par `AUDIT_KMS_*` (KMS détient la garde) ; une clé dev fixe est dérivée si absente |
| `AUDIT_KMS_ENDPOINT` | **Oui (prod)** | — | URL de l'endpoint KMS. **Sa présence active** la garde KMS de la KEK (#482) + la signature KMS des checkpoints (#483). AWS KMS ou LocalStack ; absent ⇒ replis KEK-d'environnement + HMAC |
| `AUDIT_KMS_REGION` / `AUDIT_KMS_ACCESS_KEY` / `AUDIT_KMS_SECRET_KEY` | Non | `us-east-1` / `AWS_*` | région SigV4 KMS + identifiants (replient sur les `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` standards) |
| `AUDIT_KMS_DEK_KEY_ID` / `AUDIT_KMS_SIGNING_KEY_ID` | Non | `alias/audit-dek` / `alias/audit-checkpoint` | la clé symétrique d'enrobage des DEK (#482) et la clé asymétrique de signature des checkpoints (#483) ; cette dernière sous un principal que le rôle BD du registre ne peut endosser |
| `AUDIT_KMS_SIGNING_ALGORITHM` | Non | `ECDSA_SHA_256` | `SigningAlgorithm` KMS pour la signature du checkpoint |
| `AUDIT_WITNESS_ENDPOINT` | **Oui (prod)** | — | endpoint du bucket WORM compte-séparé (S3 Object Lock). **Sa présence active** l'ancrage au témoin externe (#483) ; absent ⇒ `PgCheckpointAnchor` Postgres-seul (dev). `AUDIT_WITNESS_{REGION,BUCKET,ACCESS_KEY,SECRET_KEY}` le configurent |
| `AUDIT_CHECKPOINT_SIGNING_KEY_BASE64` | Non (dev) | clé dev | base64 de la clé HMAC de signature des checkpoints utilisée **uniquement** quand KMS est absent (voie checkpoint-signé dev/CI) |
| `AUDIT_JWKS_URL` | **Oui (prod)** | — | endpoint JWKS vérifiant les tokens edge ES256 sur la surface privilégiée. **Sa présence active** le caller gate `auth-context` ; absent ⇒ **deny-all** (`AUD-3004`) — la surface ne s'ouvre jamais par omission. Chaque RPC exige sa permission `audit:*` : `audit:record` / `audit:read` / `audit:export` / `audit:verify` |
| `AUDIT_TOKEN_ISSUER` / `AUDIT_TOKEN_AUDIENCE` | Non (recommandé) | — | claims `iss` / `aud` attendus du token edge (les valeurs que `auth` frappe) |
| `AUDIT_JWKS_TIMEOUT_MS` | Non | `10000` | timeout par requête du fetch JWKS |
| `<config registre / archive>` | **Oui** | — | registre Postgres append-only + archive WORM Object-Lock |
| `<KAFKA_BROKERS>` | **Oui** *(worker)* | — | ingestion amont de conformité + décisions |

### Features à la compilation
- `integration-audit` *(Phase 6)* — active la suite d'intégration adossée à des conteneurs (vrais Postgres + MinIO Object Lock + Kafka) : le chemin append→chaîne→archive→vérif, la détection de falsification et le cycle de vie du crypto-effacement.

---

## 🚀 Déploiement, migrations & rollback &nbsp;·&nbsp; OPS

- **Deux déployables, scalés indépendamment.** `audit-server` scale avec le QPS lecture/enregistrement ; `audit-worker` scale avec le débit d'ingestion. Publiés ensemble (même image/tag), déroulés et scalés séparément.
- **Le stockage est append-only + WORM.** Les migrations sont *additives uniquement* — le rôle du registre détient `INSERT` mais pas `UPDATE`/`DELETE` ; l'archive est Object-Lock mode-compliance. Une migration qui tenterait de muter l'historique doit être impossible par construction.
- **La garde des clés est un domaine de confiance distinct.** La clé de signature et les DEK par sujet vivent dans KMS/HSM sous un principal distinct du rôle base de données — provisionnés hors bande, jamais co-localisés avec le registre.
- **Rollback :** sûr pour les binaires (sans état au-dessus de Postgres/Kafka ; le worker reprend depuis les derniers offsets commités). Les *données* sont par conception irréversibles — c'est tout l'intérêt.

---

## 🛠️ Développement local &nbsp;·&nbsp; CORE

```bash
cargo build -p audit && cargo clippy -p audit --all-targets
cargo test  -p audit                                    # run unitaire rapide, sans infra
docker compose up -d postgres minio kafka               # compose racine du repo (Phase 6)
cargo test  -p audit --features integration-audit       # suite live (Phase 6)
```

---

## 🚨 Dépannage & runbook &nbsp;·&nbsp; OPS

> Format : **symptôme → cause racine → mitigation.** Une entrée par classe d'incident réelle.

**1. Une action privilégiée / bris de glace est refusée.**
Cause racine : la voie synchrone n'a pas pu confirmer le commit durable+chaîné dans le délai (`AUD-4004`) — par conception elle échoue *fermée*. Mitigation : vérifier la santé registre/KMS ; le refus est correct — l'action ne doit pas se dérouler non enregistrée. Résoudre la faute de stockage, puis réessayer.

**2. Le vérificateur a levé un mismatch de hash ou un trou de séquence.**
Cause racine (critique) : un enregistrement a été altéré ou la queue tronquée (`AUD-2001`/`AUD-2002`) — l'opérateur de stockage est supposé potentiellement hostile. Mitigation : **traiter comme un incident de sécurité.** Ne pas « réparer » la chaîne ; isoler, snapshot, et réconcilier contre les checkpoints ancrés à l'externe pour borner la fenêtre de falsification.

**3. Un checkpoint signé est en désaccord avec le témoin externe.**
Cause racine (critique) : falsification au niveau opérateur au-delà du rôle base de données (`AUD-2004`). Mitigation : **incident de sécurité** ; forensique inter-domaines-de-confiance — le témoin est indépendant précisément pour que ce soit détectable.

**4. Une demande d'effacement GDPR ne prend pas effet.**
Cause racine : le sujet est sous legal hold actif ; la rétention légale (Art. 17(3)) prime sur l'effacement (`AUD-5002`). Mitigation : confirmer le hold ; appliquer un shred sélectif par champ (détruire l'enveloppe PII de contact, conserver l'enregistrement de décision pseudonymisé) si licite.

**5. Le retard consommateur grimpe pendant un pic de trafic.**
Cause racine : le débit d'ingestion dépasse la capacité du worker — attendu ; Kafka l'absorbe sans perte. Mitigation : scaler `audit-worker` ; le lag se vide. Les producteurs ne sont pas affectés (ils ne bloquent jamais sur l'audit).

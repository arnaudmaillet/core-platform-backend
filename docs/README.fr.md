---
i18n:
  source: ./README.md
  source_sha256: dae6d224db32d8b2b4de7232dc2f90a60cd785064883668885e26266ea7c7c8f
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, noms de topics, signatures, identifiants) sont volontairement
> laissés en anglais.

# `core-platform` — Documentation

**Point d'entrée et taxonomie de toute la suite documentaire.** Commencez ici, puis
suivez le routeur ci-dessous vers le guide correspondant à votre rôle et à votre
tâche. Chaque document est rédigé par rapport à l'état actuel de `develop` et
renvoie aux autres ; rien ici n'est hypothétique sauf mention explicite **DEFERRED**
(reporté) ou **STUB** (ébauche).

> **Séparation des repos (2026-10-04) :** les guides de la couche plateforme
> (infrastructure, GitOps, runbooks, graphe d'appels des NetworkPolicies) vivent
> désormais dans
> [`core-platform-infra`](https://github.com/arnaudmaillet/core-platform-infra/tree/develop/docs) ;
> les liens ci-dessous y pointent. La documentation de la couche applicative reste dans ce repo.

---

## Les deux audiences

Cette plateforme sert deux lecteurs distincts. La plupart des confusions viennent de
la lecture d'un document destiné à l'autre, aussi chaque guide indique son audience
en tête.

| Vous êtes… | Vous vous souciez de… | Commencez par |
|---|---|---|
| **Développeur d'application** | livrer un service, ses contrats gRPC/Kafka, sa config et ses secrets, comprendre pourquoi un pod est en `CrashLoopBackOff` | [Rédaction de service](#la-frontière-plateforme--application) → le `README.md` du service sous `crates/services/<svc>/` |
| **Ingénieur DevOps / plateforme** | provisionner AWS, la cascade GitOps, l'autoscaling, la plomberie des secrets, monter ou démonter un environnement | [Guide maître d'infrastructure](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md) → les approfondissements opérationnels ci-dessous |

Si vous ne lisez qu'un seul document, lisez d'abord celui vers lequel votre rôle
pointe — il renvoie ensuite vers tout le reste dont vous avez besoin.

---

## La frontière plateforme / application

Le modèle mental le plus important. La **couche plateforme** est tout ce qui existe
pour qu'un service puisse s'exécuter ; la **couche application** est le service
lui-même. Le contrat entre les deux est délibérément étroit, et les deux côtés sont
documentés séparément afin qu'aucune équipe n'ait à lire les rouages de l'autre.

```
                         ┌───────────────────────────────────────────┐
   APPLICATION LAYER     │  crates/services/<svc>  +  crates/apps/*   │
   (owned by service     │  domain → application(ports) → infra       │
    authors)             │  gRPC *-api contracts · Kafka event-topology│
                         └───────────────────────────────────────────┘
   ── contract surface ──────────────────────────────────────────────────
     • reads config from env (<SVC>_GRPC_ADDR, backend endpoints)
     • reads secrets from mounted k8s Secrets (never from AWS directly)
     • declares tier: (fail-open / fail-closed) as a pod label
     • ships as one image per binary via deploy/Dockerfile
   ── contract surface ──────────────────────────────────────────────────
                         ┌───────────────────────────────────────────┐
   PLATFORM LAYER        │  EKS · Karpenter · Terragrunt · ArgoCD     │
   (owned by DevOps)     │  MSK · ElastiCache · OpenSearch · S3 · KMS │
                         │  ESO/ClusterSecretStore · gp3 storage plane│
                         └───────────────────────────────────────────┘
```

**Règle empirique pour savoir où vit un changement :**

- Modifier *ce que fait un service* (logique, ses ports, ses topics) → couche application.
- Modifier *comment un service est ordonnancé, mis à l'échelle, atteint ou alimenté en secrets* → couche plateforme.
- Un changement qui nécessite les deux (p. ex. un nouveau datastore managé pour un
  nouveau service) traverse la frontière : provisionner dans Terragrunt, câbler le
  secret via ESO, puis le service le consomme en tant qu'env — trois modifications
  distinctes et ordonnées (voir le [guide de topologie des secrets](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/secrets-eso.md)).

---

## Carte de la documentation

### Couche plateforme — infrastructure & opérations

| Document | Ce à quoi il répond | Audience principale |
|---|---|---|
| **[Guide maître d'infrastructure](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md)** | Toute la plateforme en un seul endroit : archétypes, tiers, mise à l'échelle, état AWS, frontières de sécurité, ordre de bootstrap. **La référence canonique.** | DevOps |
| **[Opérations GitOps & ArgoCD](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/gitops-argocd.md)** | La cascade App-of-AppSets, les sync waves, le CMP envsubst, self-heal/drift, et les opérations ArgoCD au quotidien avec commandes exactes et modes de défaillance. | DevOps |
| **[Référence des unités Terragrunt](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/terragrunt-units.md)** | Chaque unité de l'arbre live, ses entrées/sorties/dépendances, le DAG d'apply, et les invocations `plan`/`apply`/`destroy` par unité. | DevOps |
| **[Topologie des secrets (ESO / ClusterSecretStore)](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/secrets-eso.md)** | Comment une valeur voyage de Terraform → Secrets Manager → ExternalSecret → env du pod, la distinction machine-généré vs seedé, et comment ajouter un nouveau secret. | DevOps + auteurs de services |

### Cycle de vie — runbooks

| Runbook | Quand y recourir |
|---|---|
| **[Cycle de vie d'un environnement](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/environment-lifecycle.md)** | La boucle complète d'un environnement jetable : **preflight → provisionnement → validation → démontage gracieux → reconstruction**, avec les contraintes d'ordonnancement qui la rendent sûre. |
| **[Reconstruction du staging jetable](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/staging-disposable-rebuild.md)** | Les pièges spécifiques Secrets-Manager/KMS liés à l'état de suppression qui bloquent un cycle `destroy → apply`. |
| **[Déploiement de la remédiation d'audit](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/audit-remediation-rollout.md)** | Le déploiement, sensible à l'ordre d'apply, du plan de conformité/audit TIER-0. |

### Couche application — rédaction de service

| Document | Ce à quoi il répond |
|---|---|
| **`crates/services/<svc>/README.md`** | Contrat par service, tier, ports, backends, espace de noms des codes d'erreur. |
| **[Catalogue d'événements](domain/EVENT_CATALOG.md)** | Qui produit/consomme chaque topic Kafka (généré depuis le registre `event-topology`). |
| **[Carte de contexte du domaine](domain/)** | Contextes délimités et langage ubiquitaire. |
| **[Décisions d'architecture](adr/)** | Les ADR derrière la forme actuelle. |
| **[Standard de README de service](templates/)** | La structure de README obligatoire que suit chaque service. |

### Standards transverses

- **[Sécurité](https://github.com/arnaudmaillet/core-platform-infra/tree/develop/docs/security)** — graphe d'appel des NetworkPolicy, frontières de contrôle TIER-0.
- **[i18n](i18n/)** — l'anglais est canonique ; les co-traductions `*.fr.md` enregistrent
  le SHA-256 de leur source et sont contrôlées en CI (`tools/i18n/i18n-drift.sh`).

---

## Conventions utilisées dans toute la documentation

- **DEFERRED** — délibérément pas encore construit ; une dépendance externe/organisationnelle
  (KMS/HSM, Keycloak, témoin WORM cross-account) ou une décision produit à trancher.
- **STUB** — échafaudé mais non câblé à un backend live (p. ex. `prod`).
- Les invocations en ligne de commande sont exactes et copiables-collables ; elles
  supposent une exécution depuis la racine du dépôt sauf `cd` explicite.
- « The fleet » (la flotte) = les ~17 services livrés en images une-par-binaire.
- Le focus environnemental est **`staging`** (le chemin GitOps live) ; les écarts
  `dev` et `prod` sont signalés en ligne.

> Nouveau sur la plateforme ? Lisez le [guide maître](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md) de
> bout en bout une fois, puis gardez le [guide GitOps](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/gitops-argocd.md)
> et le [runbook de cycle de vie](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/environment-lifecycle.md) ouverts comme
> références de travail.

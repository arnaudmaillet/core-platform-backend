# `core-platform` — Documentation

**Entry point and taxonomy for the whole documentation suite.** Start here, then
follow the router below to the guide that matches your role and task. Every
document is authored against the current `develop` state and cross-links the
others; nothing here is aspirational unless it is explicitly marked **DEFERRED**
or **STUB**.

> **Repo split (2026-10-04):** the platform-layer guides (infrastructure, GitOps,
> runbooks, NetworkPolicy call graph) now live in
> [`core-platform-infra`](https://github.com/arnaudmaillet/core-platform-infra/tree/develop/docs);
> the links below point there. Application-layer docs stay in this repo.

---

## The two audiences

This platform serves two distinct readers. Most confusion comes from reading a
document written for the other one, so each guide states its audience up top.

| You are a… | You care about… | Start with |
|---|---|---|
| **Application developer** | shipping a service, its gRPC/Kafka contracts, its config and secrets, why a pod is `CrashLoopBackOff` | [Service authoring](#application-layer) → the service's own `README.md` under `crates/services/<svc>/` |
| **DevOps / platform engineer** | provisioning AWS, the GitOps cascade, scaling, secret plumbing, standing an environment up or tearing it down | [Infrastructure master guide](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md) → the operational deep-dives below |

If you only read one document, read the one your role points at first — it links
onward to everything else you need.

---

## The platform / application boundary

The single most important mental model. The **platform layer** is everything that
exists so a service can run; the **application layer** is the service itself. The
contract between them is deliberately narrow, and both sides are documented
separately so neither team has to read the other's internals.

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

**Rule of thumb for where a change lives:**

- Changing *what a service does* (logic, its ports, its topics) → application layer.
- Changing *how a service is scheduled, scaled, reached, or fed secrets* → platform layer.
- A change that needs both (e.g. a new managed datastore for a new service) crosses
  the boundary: provision in Terragrunt, wire the secret through ESO, then the
  service consumes it as env — three separate, ordered edits (see the
  [secret topology guide](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/secrets-eso.md)).

---

## Documentation map

### Platform layer — infrastructure & operations

| Document | What it answers | Primary audience |
|---|---|---|
| **[Infrastructure master guide](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md)** | The whole platform in one place: archetypes, tiers, scaling, AWS state, security boundaries, bootstrap order. **The canonical reference.** | DevOps |
| **[GitOps & ArgoCD operations](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/gitops-argocd.md)** | The App-of-AppSets cascade, sync waves, the envsubst CMP, self-heal/drift, and day-2 ArgoCD operations with exact commands and failure modes. | DevOps |
| **[Terragrunt units reference](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/terragrunt-units.md)** | Every unit in the live tree, its inputs/outputs/dependencies, the apply DAG, and per-unit `plan`/`apply`/`destroy` invocations. | DevOps |
| **[Secret topology (ESO / ClusterSecretStore)](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/secrets-eso.md)** | How a value travels from Terraform → Secrets Manager → ExternalSecret → pod env, the machine-generated vs seeded split, and how to add a new secret. | DevOps + service authors |

### Lifecycle — runbooks

| Runbook | When you reach for it |
|---|---|
| **[Environment lifecycle](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/environment-lifecycle.md)** | The full disposable-environment loop: **preflight → provision → validate → graceful teardown → rebuild**, with the ordering constraints that make it safe. |
| **[Disposable staging rebuild](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/staging-disposable-rebuild.md)** | The narrow Secrets-Manager/KMS deletion-state gotchas that block a `destroy → apply` cycle. |
| **[Audit remediation rollout](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/audit-remediation-rollout.md)** | The apply-order-sensitive rollout of the TIER-0 audit/compliance plane. |

### Application layer — service authoring

| Document | What it answers |
|---|---|
| **`crates/services/<svc>/README.md`** | Per-service contract, tier, ports, backends, error-code namespace. |
| **[Event catalog](domain/EVENT_CATALOG.md)** | Who produces/consumes every Kafka topic (generated from the `event-topology` registry). |
| **[Domain context map](domain/)** | Bounded contexts and ubiquitous language. |
| **[Architecture decisions](adr/)** | The ADRs behind the current shape. |
| **[Service README standard](templates/)** | The mandatory README structure every service follows. |

### Cross-cutting standards

- **[Security](https://github.com/arnaudmaillet/core-platform-infra/tree/develop/docs/security)** — NetworkPolicy call graph, TIER-0 control boundaries.
- **[i18n](i18n/)** — English is canonical; `*.fr.md` co-translations record the
  SHA-256 of their source and are gated in CI (`tools/i18n/i18n-drift.sh`).

---

## Conventions used across all docs

- **DEFERRED** — deliberately not built yet; an external/organizational dependency
  (KMS/HSM, Keycloak, cross-account WORM witness) or a tracked product decision.
- **STUB** — scaffolded but not wired to a live backend (e.g. `prod`).
- Exact command-line invocations are copy-pasteable and assume you run them from
  the repo root unless a `cd` is shown.
- "The fleet" = the ~17 services shipped as per-binary images.
- Environment focus is **`staging`** (the live GitOps path); `dev` and `prod`
  deltas are called out inline.

> New to the platform? Read the [master guide](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/README.md) end to
> end once, then keep the [GitOps guide](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/infrastructure/gitops-argocd.md) and
> [environment lifecycle runbook](https://github.com/arnaudmaillet/core-platform-infra/blob/develop/docs/runbooks/environment-lifecycle.md) open as
> working references.

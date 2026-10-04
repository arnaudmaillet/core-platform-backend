# Core Platform

## Introduction

**Core Platform** is a high-performance backend architecture designed for massive horizontal scalability. This project implements the technical foundation of a modern social network based on a distributed microservices architecture, leveraging data **sharding** and an **Event-Driven** approach.

The architecture is strictly structured according to **Domain-Driven Design (DDD)** and **Clean Architecture** (Ports & Adapters) principles to ensure optimal maintainability and testability.

## Technical Architecture

The system is built on four fundamental pillars:

- **API Gateway (BFF):** A unified GraphQL layer (`graphql-bff`) that orchestrates calls to underlying microservices.
- **Microservices:** Autonomous services (Account, Profile, Social, Post) communicating via gRPC (Tonic).
- **Data Consistency:** Implementation of the **Transactional Outbox** pattern (via `outbox-producer` workers) to guarantee reliable inter-service events via Kafka.
- **Polyglot Persistence:**
  - **PostgreSQL (`infra-sqlx`):** Transactional data.
  - **ScyllaDB (`infra-scylla`):** High-volume data (posts, social graph).
  - **Redis (`infra-fred`):** Distributed caching and fast state management.

## Tech Stack

- **Languages:** Rust (Edition 2024, Tokio stack).
- **Communication:** gRPC (Tonic), Protocol Buffers, GraphQL (Async-graphql).
- **Infrastructure:**
  - **Cloud:** AWS (EKS).
  - **IaC:** Terraform, Terragrunt.
  - **CD/GitOps:** ArgoCD.
- **Messaging:** Apache Kafka (rdkafka).

## System Architecture & Documentation

The platform's documentation is layered by concern, each layer derived from the code as ground truth:

- **What each service does (domain / functional):** [`docs/domain/`](docs/domain/README.md) for
  cross-context maps, and each service's `crates/services/<svc>/docs/DOMAIN.md` for its bounded
  context, invariants, and data ownership.
- **How the platform deploys & operates:** [`core-platform-infra`](https://github.com/arnaudmaillet/core-platform-infra) —
  Terraform/Terragrunt (AWS EKS, VPC, managed services), ArgoCD, Kustomize, runbooks.
- **How to build & run each service:** the per-crate `README.md`, following
  [`docs/templates/SERVICE_README.template.md`](docs/templates/SERVICE_README.template.md).
- **System architecture (C4 Model):** [`docs/architecture/`](docs/architecture/README.md) —
  a Structurizr workspace **regenerated from** `docs/domain/CONTEXT_MAP.md` as a derived artifact.

> [!NOTE]
> The C4 model in [`docs/architecture/`](docs/architecture/README.md) is regenerated from the
> functional documentation. The previous, pre-fleet Structurizr diagrams (which described an
> architecture that never shipped) have been **removed**.

## Development & Build

### Prerequisites

- Rust (Edition 2024)
- Docker & Docker Compose
- Protoc (Protobuf Compiler)

### Build

The workspace builds with standard Cargo:

```bash
# Build the entire workspace
cargo build

# Run tests (unit and integration via testcontainers)
cargo test
```

### Local Infrastructure

To start the necessary dependencies (Postgres, Redis, ScyllaDB, Kafka) for local development, use the compose files under `local-dev/`:

```bash
docker compose -f local-dev/docker-compose.yml -f local-dev/docker-compose.db.yml up -d
```

## Repository Structure

The workspace lives under `crates/`, organised by role:

- **`crates/services/<svc>/`** — domain microservices (DDD + CQRS), **library-only**. Each exposes a composition root (`App::build`) and implements `service_runtime::Service` (`crate::service::<Svc>Service`).
- **`crates/apps/<svc>-server/`** — thin deployable binaries (one per service). Each `main` is a `service_runtime::serve::<…>(addr)` one-liner.
- **`crates/platform/service-runtime/`** — the unified fleet bootstrap: telemetry, `infrastructure.toml` load + hot-reload, ingress trace + rate-limit layers, dynamic gRPC health, and graceful shutdown. See its [README](crates/platform/service-runtime/README.md).
- **`crates/shared/`** — reusable building blocks: `error`, `cqrs`, `auth-context`, `validation`/`validate-core`, the externalized-config stack (`infra-config`, `resilience`, `traffic`, `telemetry`), `transport` (gRPC + Kafka), and storage clients under `crates/shared/storage/{postgres,scylla,redis}`.
- **Infrastructure & GitOps** — moved to [`core-platform-infra`](https://github.com/arnaudmaillet/core-platform-infra) (2026-10-04); this repo's fleet CI pins its images there.

## Roadmap

- [ ] Schema-migration runner (`apps/migrator`).
- [ ] Implementation of Sharding at the SQLx layer.
- [x] Distributed monitoring with OpenTelemetry — live log-filter and trace-sampling dials via the `[telemetry]` config section.

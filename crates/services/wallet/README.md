# `wallet` — An in-app economy ledger that never mints a point twice and never guesses a balance

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | platform team |
> | **Tier** | TIER-0 (fail-closed) |
> | **Deployable** | `crates/apps/wallet-server` (library crate: `crates/services/wallet`) |
> | **Datastores** | Postgres (own CNPG cluster, tables `wallets`, `wallet_transactions`) |
> | **Async** | consumes `account.v1.events` (group `wallet-account-events`) · publishes nothing yet |
> | **Upstream callers** | the app (client edge `:9443`) |
> | **Downstream deps** | Postgres, Kafka |
> | **SLO** | 99.9% avail · p99 read < 50 ms · p99 claim < 100 ms |

---

## 🎯 Overview & Service Role

`wallet` is the **economy ledger** (#665): it owns an account's two in-app currencies and every
movement of them.

| Currency | Shown as | Earned by | Spent on |
|---|---|---|---|
| **Points** | likes (the heart) | the hourly claim | stakes on posts and comments (later PR) |
| **Gems** | the diamond | the starter gift; settled stakes (later) | country unlocks, the ×100 stake pack (later PR) |

Nothing is sold for real money: no StoreKit, no receipts, no restore. Neither currency converts to
the other, to money, or to another user. One wallet per **account** (all its profiles).

The hard problem is **mobile retries and concurrent devices against a balance**: a claim tapped on
two phones, or retried over a flaky link, must credit once; a balance must always equal the sum of
its ledger. The service locks the wallet row for every write, writes the balance and its ledger row
in one transaction, and makes every credit idempotent per `(account, idempotency_key)`.

**Core objectives:** never credit twice · never show a balance the ledger does not back · the
server's clock decides every claim.

---

## 📐 Architecture & Concepts

```
app ──gRPC :9443 (edge, require_account)──► WalletServiceHandler
                                              │
                                              ▼
                                   Wallets (use cases) ── domain::Wallet (claim rules, pure)
                                              │
                                              ▼ WalletStore port
                                   PgWalletStore ── BEGIN; INSERT wallets ON CONFLICT DO NOTHING
                                                    (+ starter gift row); SELECT … FOR UPDATE;
                                                    UPDATE wallets + INSERT wallet_transactions; COMMIT
account.v1.events ──► account consumer (run_consumer) ──► Wallets::erase
```

**The ledger.** `wallet_transactions` is append-only: `currency`, signed `delta`, the
`balance_after` it produced, `kind`, `idempotency_key`, `created_at`. `UNIQUE (account_id,
idempotency_key)` is the idempotency mechanism: a replayed claim key answers its first award.
Client keys are 8–64 `[A-Za-z0-9_-]`; the service's own start with `sys:` (e.g.
`sys:starter-gems`), which no client key can.

**The hourly claim** (same rules as the app's mock):

| Rule | Value |
|---|---|
| Interval | one claim per hour |
| Base | 25 points |
| Streak | +10 % per consecutive UTC day after the first, up to ×2 (rounded half away from zero) |
| Daily cap | 200 points per UTC day; the last claim is clamped to what is left |
| Capped day | the next claim opens at 00:00 UTC |
| Streak shown | the chain while alive (a claim today or yesterday), else 0 |

**The starter gift.** A wallet opens on first use with 100 gems (`STARTER_GIFT`), once.

> **Invariants** (and where enforced): balances ≥ 0 (`CHECK`); each balance = Σ its ledger deltas
> (same transaction, row lock; IT-checked); one credit per key (`UNIQUE`); one claim per interval
> (row lock + domain rule); the caller's own wallet only (`edge::require_account`).

---

## 📊 Service Level Objectives (SLO) &nbsp;·&nbsp; OPS

| SLI | Objective | Window | Measured by |
|---|---|---|---|
| Availability (non-`UNAVAILABLE`/`INTERNAL`) | 99.9% | 30d rolling | gRPC server metrics |
| `GetWallet` p99 | < 50 ms | 1h | `wallet.open` span |
| `ClaimReward` p99 | < 100 ms | 1h | `wallet.claim` span |
| Erasure lag | < 60 s | live | `wallet-account-events` lag |
| Durability | no committed movement lost | — | Postgres synchronous commit |

**Error budget:** 0.1% / 30d ≈ 43 min. **On burn:** freeze rollouts.

---

## 🔗 Dependencies & Blast Radius &nbsp;·&nbsp; OPS

| Dependency | Purpose | If down → | Degradation |
|---|---|---|---|
| Postgres | the ledger | every RPC fails | **Hard** — `UNAVAILABLE` (fail-closed) |
| Kafka | account erasure | erasures wait | **Soft** — consumed on recovery |

| Caller | Uses | Impact if `wallet` is down |
|---|---|---|
| the app | `GetWallet`, `ClaimReward`, `ListWalletTransactions` | balance and claim unavailable; the rest of the app works |

> **Critical path?** No — the feed, posts and chat do not call it.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### gRPC — `wallet.v1.WalletService`

```protobuf
service WalletService {
  rpc GetWallet (GetWalletRequest) returns (Wallet);
  rpc ClaimReward (ClaimRewardRequest) returns (ClaimRewardResponse);
  rpc ListWalletTransactions (ListWalletTransactionsRequest) returns (ListWalletTransactionsResponse);
}
```

All three are on the edge (`authenticated`), bound to the caller's `account_id`
(`edge::require_account`; another account ⇒ `PERMISSION_DENIED`).

- `ClaimReward` answers `CLAIMED`, `TOO_EARLY` or `DAILY_CAP_REACHED` **in-band** (not errors),
  with the wallet after the call; `next_claim_at` says when the next claim opens.
- `ListWalletTransactions`: newest first, both currencies or one; 50 per page by default, 100 at
  most. An unknown `TransactionKind` (from a newer server) reads as `UNSPECIFIED`.

### Error contract (`x-error-code` metadata)

| Code | Meaning | gRPC |
|---|---|---|
| `WAL-5001` | an unreadable ledger row (fail closed) | `INTERNAL` |
| `WAL-9001` | invalid account id | `INVALID_ARGUMENT` |
| `WAL-9002` | invalid idempotency key | `INVALID_ARGUMENT` |
| `WAL-9003` | invalid page token | `INVALID_ARGUMENT` |
| `DB-*` | storage (delegated) | per error |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

| Topic | Direction | Event | Effect |
|---|---|---|---|
| `account.v1.events` | consumed (`wallet-account-events`, `run_consumer`) | `account_deleted` | the wallet and its history are erased (GDPR Art. 17); other events skipped; a bad id is dead-lettered |

Nothing is published yet.

---

## 🌩️ Failure Modes & Degradation &nbsp;·&nbsp; OPS

| Symptom | Root cause | Mitigation |
|---|---|---|
| `UNAVAILABLE` on every RPC | Postgres down | restore Postgres; nothing is guessed meanwhile |
| `WAL-5001` | a row the service cannot read (a newer schema rolled back) | roll forward; never patch balances by hand without a ledger row |
| erasure lag grows | Kafka or Postgres down | recovers on its own; DLQ holds poison events |

---

## 📦 Integration & Usage &nbsp;·&nbsp; CORE

```rust
let app = wallet::app::App::build(pool, wallet::config::WalletConfig::from_env());
let view = app.wallets.get(&account_id, chrono::Utc::now()).await?;
```

### Bootstrap (`crates/apps/wallet-server`)

`WALLET_GRPC_ADDR` (default `0.0.0.0:50072`) → `service_runtime::serve::<WalletService>`.

---

## ⚙️ Configuration & Runtime Environment &nbsp;·&nbsp; CORE

### `wallet`-specific variables

| Variable | Default | Meaning |
|---|---|---|
| `WALLET_GRPC_ADDR` | `0.0.0.0:50072` | mesh listener |
| `WALLET_CLAIM_INTERVAL_SECS` | `3600` | one claim per interval |
| `WALLET_CLAIM_BASE_POINTS` | `25` | an un-streaked claim |
| `WALLET_DAILY_CLAIM_CAP` | `200` | points claimable per UTC day |
| `WALLET_STARTER_GEMS` | `100` | gems a wallet opens with |

An unparsable or negative value keeps the default.

### Inherited infrastructure variables

`DATABASE_URL` / `PG_*` (Postgres), `KAFKA_*` (the consumer), `GRPC_EDGE_ADDR` and the edge token
settings (client edge), OTel.

### Compile-time features

`integration-wallet` — the live Postgres suite (`cargo test -p wallet --features integration-wallet`).

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

Migrations: `migrations/0001_create_wallet_tables.sql`, applied by `migrator wallet` (init
container) before the binary. Infra (ECR repo, manifests, `wallet-postgres`, ingress route,
NetworkPolicy): core-platform-infra#41 — the binary joins `FLEET_BINS` once its ECR repo exists.
Rollback: the binary is stateless; the schema is additive.

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

Spans `wallet.open`, `wallet.claim`, `wallet.history`, `wallet.erase` (with the shard). One row
lock per write; the history reads `(account_id, created_at DESC, id DESC)`.

---

## 🧪 Local Development & Testing &nbsp;·&nbsp; CORE

```bash
cargo test -p wallet                                   # unit: claim rules, ledger, handler, edge
cargo test -p wallet --features integration-wallet     # Postgres: concurrency, idempotency, reconciliation
```

---

## 🧭 Roadmap (#665)

1. **This crate:** the ledger, the hourly claim, the starter gems, the history, erasure.
2. Gem spends (country unlocks, the ×100 stake pack); gems blocked for minors server-side.
3. Point stakes on posts and comments (anti-abuse caps, e.g. 1,000 points per hour).
4. Stake settlement (gems earned).

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
> | **Upstream callers** | the app (client edge `:9443`) · geo-discovery (mesh `SpendGems`, country unlocks) |
> | **Downstream deps** | Postgres, Kafka |
> | **SLO** | 99.9% avail · p99 read < 50 ms · p99 claim < 100 ms |

---

## 🎯 Overview & Service Role

`wallet` is the **economy ledger** (#665): it owns an account's two in-app currencies and every
movement of them.

| Currency | Shown as | Earned by | Spent on |
|---|---|---|---|
| **Points** | likes (the heart) | the hourly claim | stakes on posts and comments (later PR) |
| **Gems** | the diamond | the starter gift; settled stakes (later) | the ×100 stake pack; country unlocks (geo-discovery) |

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
idempotency_key)` is the idempotency mechanism: a replayed key answers its first result. A
caller's key is 8–64 `[A-Za-z0-9_-]`, stored **scoped to its operation** (`claim:<key>`,
`pack:<key>`, `spend:<key>`), so a key used for a claim can never pass as a paid pack; the
service's own start with `sys:` (e.g. `sys:starter-gems`). No caller key holds a `:`.

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

**Spending gems** (adults only: a token under 18, or without an age, gets `WAL-3001`):

| Spend | Rule |
|---|---|
| ×100 stake pack (`BuyStakePack`, edge) | 3 shots of 100 of the buyer's **own** points, 50 gems; one pack at a time (`PACK_STILL_ACTIVE` while shots are left, nothing charged); shots never expire |
| Country unlock (`SpendGems`, mesh) | geo-discovery prices the country, checks the spender is an adult, and asks for the gems with a key of its own; `ref_id` = the country code |

Gems never buy points, money, or anything for another user.

> **Invariants** (and where enforced): balances ≥ 0 (`CHECK`); each balance = Σ its ledger deltas
> (same transaction, row lock; IT-checked); one movement per scoped key (`UNIQUE`); one claim per
> interval and one pack at a time (row lock + domain rule); the caller's own wallet only
> (`edge::require_account`); no gem spend under 18 (the token's `age` claim, fail-closed).

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
| the app | `GetWallet`, `ClaimReward`, `ListWalletTransactions`, `BuyStakePack` | balance, claim and pack unavailable; the rest of the app works |
| geo-discovery | `SpendGems` | country unlocks refused (fail-closed); the map works |

> **Critical path?** No — the feed, posts and chat do not call it.

---

## 🔌 Public Interfaces & API Contract &nbsp;·&nbsp; CORE

### gRPC — `wallet.v1.WalletService`

```protobuf
service WalletService {
  rpc GetWallet (GetWalletRequest) returns (Wallet);
  rpc ClaimReward (ClaimRewardRequest) returns (ClaimRewardResponse);
  rpc ListWalletTransactions (ListWalletTransactionsRequest) returns (ListWalletTransactionsResponse);
  rpc BuyStakePack (BuyStakePackRequest) returns (BuyStakePackResponse);
  rpc SpendGems (SpendGemsRequest) returns (SpendGemsResponse);   // mesh only
}
```

All but `SpendGems` are on the edge (`authenticated`), bound to the caller's `account_id`
(`edge::require_account`; another account ⇒ `PERMISSION_DENIED`). `SpendGems` is mesh only
(geo-discovery): `kind` = `COUNTRY_UNLOCK`, `amount` > 0, `ref_id` ≤ 64 characters.

- `Wallet.gem_spending_restricted` (edge only) tells the app to hide gem spends; the pack's terms
  are echoed (`stake_pack_price`, `stake_pack_shots`, `points_per_shot`).
- `BuyStakePack` answers `BOUGHT`, `PACK_STILL_ACTIVE` or `INSUFFICIENT_GEMS` in-band;
  `SpendGems` answers `SPENT` or `INSUFFICIENT_GEMS`.

- `ClaimReward` answers `CLAIMED`, `TOO_EARLY` or `DAILY_CAP_REACHED` **in-band** (not errors),
  with the wallet after the call; `next_claim_at` says when the next claim opens.
- `ListWalletTransactions`: newest first, both currencies or one; 50 per page by default, 100 at
  most. An unknown `TransactionKind` (from a newer server) reads as `UNSPECIFIED`.

### Error contract (`x-error-code` metadata)

| Code | Meaning | gRPC |
|---|---|---|
| `WAL-3001` | gems cannot be spent (under 18, or age unknown) | `PERMISSION_DENIED` |
| `WAL-5001` | an unreadable ledger row (fail closed) | `INTERNAL` |
| `WAL-9001` | invalid account id | `INVALID_ARGUMENT` |
| `WAL-9002` | invalid idempotency key | `INVALID_ARGUMENT` |
| `WAL-9003` | invalid page token | `INVALID_ARGUMENT` |
| `WAL-9004` | invalid gem spend (amount, kind, ref) | `INVALID_ARGUMENT` |
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
| `WALLET_STAKE_PACK_SHOTS` | `3` | shots in a ×100 pack |
| `WALLET_STAKE_PACK_PRICE` | `50` | a pack's price in gems |
| `WALLET_POINTS_PER_SHOT` | `100` | points one shot stakes |

An unparsable or negative value keeps the default.

### Inherited infrastructure variables

`DATABASE_URL` / `PG_*` (Postgres), `KAFKA_*` (the consumer), `GRPC_EDGE_ADDR` and the edge token
settings (client edge), OTel.

### Compile-time features

`integration-wallet` — the live Postgres suite (`cargo test -p wallet --features integration-wallet`).

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

Migrations: `migrations/0001_create_wallet_tables.sql`, `0002_transaction_ref.sql` (`ref_id`), applied by `migrator wallet` (init
container) before the binary. Infra (ECR repo, manifests, `wallet-postgres`, ingress route,
NetworkPolicy): core-platform-infra#41 — the binary joins `FLEET_BINS` once its ECR repo exists.
Rollback: the binary is stateless; the schema is additive.

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

Spans `wallet.open`, `wallet.claim`, `wallet.buy_stake_pack`, `wallet.spend_gems`, `wallet.history`,
`wallet.erase` (with the shard). One row
lock per write; the history reads `(account_id, created_at DESC, id DESC)`.

---

## 🧪 Local Development & Testing &nbsp;·&nbsp; CORE

```bash
cargo test -p wallet                                   # unit: claim rules, ledger, handler, edge
cargo test -p wallet --features integration-wallet     # Postgres: concurrency, idempotency, reconciliation
```

---

## 🧭 Roadmap (#665)

1. The ledger, the hourly claim, the starter gems, the history, erasure (#853).
2. Gem spends: the ×100 stake pack and `SpendGems`, no gem spend under 18 (this part);
   country unlocks, standings and the map filter in geo-discovery.
3. Point stakes on posts and comments (anti-abuse caps, e.g. 1,000 points per hour).
4. Stake settlement (gems earned).

# `wallet` — An in-app economy ledger that never mints a point twice and never guesses a balance

> **Service Card** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Owner** | platform team |
> | **Tier** | TIER-0 (fail-closed) |
> | **Deployable** | `crates/apps/wallet-server` (library crate: `crates/services/wallet`) |
> | **Datastores** | Postgres (own CNPG cluster, tables `wallets`, `wallet_transactions`) |
> | **Async** | consumes `account.v1.events` (group `wallet-account-events`) and `auth.v1.events` (group `wallet-auth-events`: the welcome gift) · publishes `wallet.v1.events` (likes staked) |
> | **Upstream callers** | the app (client edge `:9443`) · geo-discovery (mesh `SpendGems`, country unlocks) · account (mesh `ExportWallet` / `ListStakePositions` / `ListWalletTransactions`, the GDPR export) |
> | **Downstream deps** | Postgres, Kafka, post and comment (mesh: what likes land on), social-graph (mesh: may the reader see it), engagement (mesh: `GetLikePositions`, stake settlement) |
> | **SLO** | 99.9% avail · p99 read < 50 ms · p99 claim < 100 ms |

---

## 🎯 Overview & Service Role

`wallet` is the **economy ledger** (#665): it owns an account's two in-app currencies and every
movement of them.

| Currency | Shown as | Earned by | Spent on |
|---|---|---|---|
| **Points** | likes (the heart) | the hourly claim | likes on posts and comments (`Stake`) |
| **Gems** | the diamond | the starter gift; settled stakes (later) | stake packs; catalogue items (themes, icons); country unlocks (geo-discovery) |

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
| Stake packs (`BuyStakePack`, edge; #941) | shots of 100 of the buyer's **own** points; on sale by default `shots_3` (3 shots, 50 gems — the default pack, for a request naming none), `shots_10` (10, 150) and `shots_25` (25, 325); all or nothing, one pack at a time (`PACK_STILL_ACTIVE` while shots are left, nothing charged); shots never expire; a pack not on sale is `WAL-9005`; the ledger row's `ref_id` is the pack id and `Wallet.stake_pack_id` the loaded pack |
| Country unlock / 24 h visa (`SpendGems`, mesh; #910) | geo-discovery prices the country, checks the spender is an adult, and asks for the gems with a key of its own (`COUNTRY_UNLOCK` or `COUNTRY_VISA`); `ref_id` = the country code |
| Profile slot (`SpendGems`, mesh; #916) | profile prices the slot, checks the buyer is an adult, and asks for the gems keyed `profile-slot-<n>` (`PROFILE_SLOT`); `ref_id` = the slot number |
| Paid handle change (`SpendGems`, mesh; #911) | profile prices the change (the first is free), checks the payer is an adult, and asks for the gems keyed `handle-change-<n>` once the handle is claimed (`HANDLE_CHANGE`); `ref_id` = the profile |
| Catalogue item (`PurchaseItem`, edge; #892) | see the catalogue below |

**The gem catalogue (#892).** What gems buy, as server policy (`WALLET_CATALOG`), never influence,
ranking or visibility (charter V5.3 §35). An item id is `<family>.<name>`; the families are
`theme` (app accent themes, #946), `icon` (alternate app icons) and `frame` (avatar frames, #913,
equipped per profile by profile's `EquipAvatarFrame`, which reads `ListOwnedItems` over the mesh). On
sale by default: `theme.pink` (free), `theme.violet` (50), `theme.red` (50), `frame.gold`, `frame.neon`
and `frame.aurora` (60 each); no icon until the app bundles one.
- `ListCatalog` lists the items and prices; the app bundles each item's art keyed by its id and hides
  an id it does not know.
- `PurchaseItem` is idempotent per key, at `expected_price` (`PRICE_CHANGED` otherwise), and answers
  in-band: `PURCHASED`, `ALREADY_OWNED` (or free; nothing charged), `INSUFFICIENT_GEMS`,
  `PRICE_CHANGED`, `UNKNOWN_ITEM`. A purchase writes one ledger row of its family's kind (`THEME`,
  `APP_ICON`, `AVATAR_FRAME`; `ref_id` = the item id) and an `owned_items` row, in one transaction with the wallet
  row locked, so raced purchases charge once.
- The **account** owns an item; a free one is owned by every account and has no row.
  `ListOwnedItems` gives free ones first, then the bought ones, oldest first, kept when no longer on
  sale. Which theme or icon is applied is a device setting: the server neither stores nor serves it.
- The GDPR export carries the bought items (`ExportWalletResponse.owned_items`); erasure deletes them.

Gems never buy points, money, or anything for another user.

**Likes are points** (#665 part 3). The app batches a post's taps on the device (1 point each) and
commits the batch with `Stake` when the viewer moves on, the app goes to the background, or 10 s after
the last tap — then retries it with the same key until answered. At commit the server rechecks
everything, in this order:

| Check | Outcome |
|---|---|
| The batch's first tap is older than 24 h | `EXPIRED` (nothing spent; the app gives the points back on screen) |
| The post or comment is gone, removed or unpublished (post / comment over the mesh) | `TARGET_NOT_STAKEABLE` |
| It is the caller's own (its author is one of the token's `pids`) | `OWN_CONTENT` |
| The reader may not see it (social-graph `CheckAccess` from the token's `pids` to its author is not `VISIBLE`: a block, a private author not followed, a hidden profile) — no like, so no "liked" channel to its author | `TARGET_NOT_STAKEABLE` |
| A **shot**: no pack / no room / room for less than 100 / fewer than 100 points / not 100 left in the hour | `NO_STAKE_SHOTS` / `TARGET_CAP_REACHED` / `SHOT_DOES_NOT_FIT` / `INSUFFICIENT_BALANCE` / `RATE_LIMITED` |
| A **plain batch**: clamped to the target's room (**250** points per account and target, ever), the balance and the hour's room (**1,000** points per rolling hour) | `STAKED` with `spent` ≤ asked, or the binding limit when nothing fits |

A stake moves the points, the account's total on the target (`stakes`, migration 0003) and its
ledger row (`STAKE`, `ref_id` = `post:<id>`) in one transaction, the wallet row locked: concurrent
batches never pass the room. It is **final** (no unlike). Its announcement, `StakeCommitted`, is
written to **`wallet_outbox` in the same transaction** (migration 0004): never a stake without its
event. It is published right after the commit when the broker answers, else by the **drainer**
(every `WALLET_OUTBOX_DRAIN_SECS`, all shards, oldest first) — at least once; the consumers dedup on
`stake_key`. A row is **leased** to one replica at a time (`claimed_until`): the writer holds it 30 s
while it publishes, and the drainers claim the rest with `FOR UPDATE SKIP LOCKED` for a minute, so
replicas share the backlog instead of all publishing it. A replayed batch announces nothing new. `first` marks the account's first batch on the
target (the "X liked your post" notice goes out once). Published events are pruned after 7 days.
Without `KAFKA_BROKERS` the service **does not start** (unless `WALLET_ALLOW_LOG_PUBLISHER=true`,
local runs): a misconfigured env never drops likes silently.

**Stake settlement (#665 part 4, shadow mode).** A **position** — an account's stakes on one post or
comment, its `stakes` row — settles **once, a day after its first stake** (`WALLET_SETTLEMENT_DELAY_SECS`).
The **settler** (every `WALLET_SETTLEMENT_SECS`, all shards) leases due positions like the outbox
(`settle_claimed_until`, `FOR UPDATE SKIP LOCKED`, 5 min), asks engagement what each target came to
(`GetLikePositions`: the target's count just before the account's first like, and now), and records a
row in **`settlements`** (migration 0005) with the position's points, the counts, its **earliness**
(the share of everyone else's points that came after it) and a **pre-score** = earliness ×
√(points / 250) — model `shadow-v0`, marking the position settled in the same transaction. **No gems
are minted**: the economy charter's phase 2 (dev/economy in the iOS repo), scores recorded for
calibration first. Points staked after a position settled are not settled again (v0). An account
engagement cannot answer for stays leased and is retried after the lease. Without
`WALLET_ENGAGEMENT_GRPC_ENDPOINT` nothing settles. Settlements go with the account on erasure.

**The daily curator envelope (shadow mode).** Once a UTC day is over (10 min after midnight, so its
last settlements land), the positions settled that day share a fixed pool —
`WALLET_CURATOR_ENVELOPE_DAILY`, 1000 gems (economy charter §27–§29):
1. **outcome** — 1 when the position's target ended the day in the top `WALLET_OUTCOME_TOP_PERCENT`
   (15 %) of the day's targets by like count, else 0: popular alone pays nothing outside the top,
   and early is what scores within it;
2. **score** = outcome × pre-score; each position's raw share = pool × score / Σ scores;
3. **caps** — one target's positions together ≤ `WALLET_ENVELOPE_TARGET_CAP_BPS` (2 % = 20 gems),
   one account ≤ `WALLET_CURATOR_DAILY_CAP` (40 gems) a day; what the caps hold back stays
   unallocated (the reserve);
4. the whole gems (floored) are written as **`provisional_gems`** on each settlement, with its
   `outcome`, `score` and `envelope_day`, and the day's summary in **`envelope_days`** (migration
   0006; on the nil UUID's shard). A day is leased to one replica; recomputing it gives the same
   shares. **Still nothing minted.**

**Calibration report (#893, the Go / No-Go checks of charter §40–§41).** An operator runs
`wallet-server calibration-report [YYYY-MM-DD]` inside a wallet pod (default: yesterday, UTC). It reads
every shard's settlements counted in that day's envelope, writes nothing, and prints JSON:
- **economy:** `pool`, `allocated`, `rewarded_accounts`, and the share of the allocated gems the top
  account / top 10 / top 1 % got (`top1_share`, `top10_share`, `top1pct_share`; concentration);
- **curation:** the top rate by **earliness** quartile, earliest first, then `unknown`. A flat line means
  early stakes carry no information. Also the top rate by **stake size** (1, 2–5, 6–25, 26–100, 101–250
  points) and `single_point_share`: mostly one-point positions mean the stake is a like under another name;
- **#868's tie check:** `top_targets` against `expected_top_targets` (`top_percent` of the day's targets,
  rounded up) and their ratio `top_inflation`. Well above 1 means ties at the threshold diluted the top,
  and the tie-break of #868 is due.

Comments, cohorts and Sybil signals join the report with their features (#889, #891). The app measures
the UX criteria of §41.

> **Invariants** (and where enforced): balances ≥ 0 (`CHECK`); each balance = Σ its ledger deltas
> (same transaction, row lock; IT-checked); one movement per scoped key (`UNIQUE`); one claim per
> interval and one pack at a time (row lock + domain rule); the caller's own wallet only
> (`edge::require_account`; a like's profile: `edge::require_profile`); no gem spend under 18 (the
> token's `age` claim, fail-closed); a like stays within its target's and its hour's room (row lock),
> never on one's own content.

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
| the app | `GetWallet`, `ClaimReward`, `ListWalletTransactions`, `BuyStakePack`, `Stake`, `ListMyStakes`, `ListCatalog`, `PurchaseItem`, `ListOwnedItems` | balance, claim, pack, likes, the stakes list and the Shop unavailable (the app keeps the default theme); the rest of the app works |
| geo-discovery | `SpendGems` | country unlocks refused (fail-closed); the map works |
| profile | `SpendGems` | profile slots cannot be bought (fail-closed); profiles work |
| account | `ExportWallet`, `ListStakePositions`, `ListWalletTransactions` | GDPR exports retried next pass |
| post, comment, social-graph (downstream) | `GetPost`, `GetComment`, `CheckAccess` | likes refused (`WAL-6001`, retried by the app) |
| engagement (downstream) | `GetLikePositions` | settlements wait (retried after the lease) |

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
  rpc Stake (StakeRequest) returns (StakeResponse);               // a batch of likes
  rpc SpendGems (SpendGemsRequest) returns (SpendGemsResponse);   // mesh only
  rpc ExportWallet (ExportWalletRequest) returns (ExportWalletResponse);                 // mesh only
  rpc ListStakePositions (ListStakePositionsRequest) returns (ListStakePositionsResponse); // mesh only
  rpc ListMyStakes      (ListMyStakesRequest)      returns (ListMyStakesResponse);      // edge: the caller's account
  rpc ListCatalog       (ListCatalogRequest)       returns (ListCatalogResponse);       // edge: any member
  rpc PurchaseItem      (PurchaseItemRequest)      returns (PurchaseItemResponse);      // edge: the caller's account
  rpc ListOwnedItems    (ListOwnedItemsRequest)    returns (ListOwnedItemsResponse);    // edge: the caller's account
}
```

All but `SpendGems` are on the edge (`authenticated`), bound to the caller's `account_id`
(`edge::require_account`; another account ⇒ `PERMISSION_DENIED`). `SpendGems` is mesh only
(geo-discovery): `kind` = `COUNTRY_UNLOCK`, `amount` > 0, `ref_id` ≤ 64 characters.
`ExportWallet` (the wallet read **without opening one**; absent when the account never had one) and
`ListStakePositions` (each stake position with its settlement and envelope share, paged by target,
token `kind:id`, ≤ 500 a page) are mesh only too: account's GDPR export (#653, #665).
`ListMyStakes` (#886) is the app's view of the same positions, on the edge for the caller's account:
newest first stake first, filtered `ACTIVE` / `SETTLED` (or both), ≤ 100 a page (50 by default), token
`<first_at µs>:<kind>:<id>`. Each stake has its points, `settles_at` (first stake +
`WALLET_SETTLEMENT_DELAY_SECS`), `settled_at` once settled, and a `StakeVerdict`: `PENDING` until the
day's envelope is computed, then `REWARDED` / `NOT_REWARDED`. **No gems while settlement is shadow
mode** (product decision 2026-10-11): the app never shows gems the balance does not hold.
**Welcome gift (#827, #943).** A guest's device that becomes a member is credited a flat 50 likes, however
long it was a guest (owner decision 2026-10-14: the 25-a-day growth is gone). When the device becomes a
member (sign-up or sign-in carrying the guest's refresh token), auth publishes `guest_upgraded`. That
event carries the account and a SHA-256 of the device (its App Attest key, else its device id — never the
raw id). The wallet then credits the gift as points (`WELCOME_GIFT`), **once per device, whoever signs
in**:
- **Once per device:** the device is claimed on the nil UUID's shard first (`welcome_gifts`, migration
  0009), so another account on that device gets nothing.
- **Once per account:** an account that already claimed another device gets nothing more, even if the cap
  held its first gift to 0.
- **Idempotent:** the credit uses the service's key `sys:welcome-gift`, so a redelivery answers it.
- **Cap:** the credit is clamped under `points_cap`.
- **Erasure:** erasing the account unlinks the device but keeps it welcomed.
- **Knob:** `WALLET_WELCOME_GIFT`.

**Conviction (#888, charter §6–§9).** `StakeRequest.conviction` (C1–C4; `UNSPECIFIED` = a quick stake)
is how sure the curator is that the target ends the day in the top: a prediction recorded and
**frozen**, scored later (#889), with **no effect** on the stake.
- **Storage:** each batch's level goes on its ledger row (`wallet_transactions.conviction`). The
  position's first batch's goes on the position (`stakes.first_conviction`, never updated) and in the
  export (`StakePosition.first_conviction`). A quick stake records NULL, so its default level is left
  to the scoring.
- **Probabilities:** `Wallet.conviction_probabilities` gives the probability each level stands for
  (`WALLET_CONVICTION_PROBABILITIES`, default 0.25 / 0.40 / 0.60 / 0.80).
**Mesh callers are checked (#852, `MESH_CALLER_GATE`):** `SpendGems` takes **geo-discovery** and **profile** (#916) only, and
the end user's edge token it forwards (`authorization`) must name the account's owner, an adult; the
export reads (and `ListWalletTransactions` over the mesh) take **account** only — the caller's
ServiceAccount token, `x-mesh-token`. `off` until the infra provides the tokens (core-platform-infra#62).

- `Wallet.gem_spending_restricted` (edge only) tells the app to hide gem spends; the packs on sale
  are echoed (`stake_packs`, the default first; `stake_pack_price` / `stake_pack_shots` are the
  default pack's, for older clients; `points_per_shot`), with the loaded one (`stake_pack_id`).
- `BuyStakePack` answers `BOUGHT`, `PACK_STILL_ACTIVE` or `INSUFFICIENT_GEMS` in-band;
  `SpendGems` answers `SPENT` or `INSUFFICIENT_GEMS`; `PurchaseItem` answers a `PurchaseOutcome`
  (the catalogue above).

- `ClaimReward` answers `CLAIMED`, `TOO_EARLY`, `DAILY_CAP_REACHED` or `BALANCE_FULL` **in-band**
  (not errors), with the wallet after the call; `next_claim_at` says when the next claim opens.
  The points balance is capped (#887, charter §4.1 `A_max`): a claim is clamped to the room under
  `points_cap` (echoed in `Wallet`), and a wallet at the cap answers `BALANCE_FULL`, with
  `claim_available` false and no `next_claim_at`: spending points opens it again, no wait does.
  Gems are not capped. Call velocity (about 30 `ClaimReward` an hour per account,
  `RESOURCE_EXHAUSTED`) is the client edge's `per_caller` traffic profile, set in the infra
  repo's `infrastructure.toml`, not in this service.
- `ListWalletTransactions`: newest first, both currencies or one; 50 per page by default, 100 at
  most. An unknown `TransactionKind` (from a newer server) reads as `UNSPECIFIED`.

### Error contract (`x-error-code` metadata)

| Code | Meaning | gRPC |
|---|---|---|
| `WAL-3001` | gems cannot be spent (under 18, or age unknown) | `PERMISSION_DENIED` |
| `WAL-5001` | an unreadable ledger row (fail closed) | `INTERNAL` |
| `WAL-6001` | post, comment or social-graph unavailable: the like is retried | `UNAVAILABLE` |
| `WAL-6002` | a publish failed (internal: the outbox keeps the event; never returned by `Stake`) | `UNAVAILABLE` |
| `WAL-9001` | invalid account id | `INVALID_ARGUMENT` |
| `WAL-9002` | invalid idempotency key | `INVALID_ARGUMENT` |
| `WAL-9003` | invalid page token | `INVALID_ARGUMENT` |
| `WAL-9004` | invalid gem spend (amount, kind, ref) | `INVALID_ARGUMENT` |
| `WAL-9005` | stake pack not on sale | `INVALID_ARGUMENT` |
| `DB-*` | storage (delegated) | per error |

---

## 📨 Events & Async Contract &nbsp;·&nbsp; CORE

| Topic | Direction | Event | Effect |
|---|---|---|---|
| `auth.v1.events` | consumed (`wallet-auth-events`, `run_consumer`) | `guest_upgraded` | the welcome gift, once per device (#827); other events skipped; a malformed one is dead-lettered |
| `account.v1.events` | consumed (`wallet-account-events`, `run_consumer`) | `account_deleted` | the wallet, its history and its stakes are erased (GDPR Art. 17); other events skipped; a bad id is dead-lettered |
| `wallet.v1.events` | published (key: `<target_kind>:<target_id>`) | `stake_committed` | `{account_id, profile_id, target_kind, target_id, author_profile_id, points, total, first, stake_key, staked_at}` — likes landed; through the outbox, at least once (dedup on `stake_key`). No consumer yet: the like counts move to it in the next parts |

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
let app = wallet::app::App::build(pool, wallet::config::WalletConfig::from_env(), None); // + (targets, publisher) for likes
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
| `WALLET_POINTS_CAP` | `2000` | the points balance a claim never takes past (`BALANCE_FULL` at it); 0 keeps the default |
| `WALLET_STARTER_GEMS` | `100` | gems a wallet opens with |
| `WALLET_CATALOG` | `theme.pink:0,theme.violet:50,theme.red:50,frame.gold:60,frame.neon:60,frame.aurora:60` | the gem catalogue (#892): `item id:gems`, display order, ids `<family>.<name>` of a known family (`theme`, `icon`) and distinct; 0 = free, owned by every account; a bad list keeps the default |
| `WALLET_STAKE_PACKS` | `3:50,10:150,25:325` | the stake packs on sale (#941): `shots:gems`, the default pack first, distinct shots (a pack's id is `shots_<shots>`); a bad list keeps the default |
| `WALLET_POINTS_PER_SHOT` | `100` | points one shot stakes |
| `WALLET_STAKE_TARGET_CAP` | `250` | points one account may put on one target, ever |
| `WALLET_STAKE_HOURLY_CAP` | `1000` | points one account may stake per rolling hour |
| `WALLET_STAKE_MAX_BATCH_AGE_SECS` | `86400` | how old a batch's first tap may be |
| `WALLET_POST_GRPC_ENDPOINT` · `WALLET_COMMENT_GRPC_ENDPOINT` | `http://localhost:50056` · `:50057` | post's and comment's mesh addresses (what likes land on) |
| `WALLET_SOCIAL_GRAPH_GRPC_ENDPOINT` | `http://localhost:50053` | social-graph's mesh address (may the reader see the content) |
| `WALLET_OUTBOX_DRAIN_SECS` | `5` | how often the outbox drainer runs |
| `WALLET_ENGAGEMENT_GRPC_ENDPOINT` | unset | engagement's mesh address (e.g. `http://engagement:50058`): stake settlement. Unset → nothing settles |
| `WALLET_SETTLEMENT_DELAY_SECS` | `86400` | how long after its first stake a position settles |
| `WALLET_SETTLEMENT_SECS` · `WALLET_SETTLEMENT_BATCH` | `60` · `500` | how often the settler runs, and how many positions a pass |
| `WALLET_CURATOR_ENVELOPE_DAILY` | `1000` | gems the curators share per day (provisional, shadow mode) |
| `WALLET_CURATOR_DAILY_CAP` | `40` | the most one account receives from a day's envelope |
| `WALLET_ENVELOPE_TARGET_CAP_BPS` | `200` | the most one target's positions receive together, in basis points of the pool (≤ 10000) |
| `WALLET_OUTCOME_TOP_PERCENT` | `15` | a target scores when it ends the day in this top share of the day's targets (1–100) |
| `WALLET_WELCOME_GIFT` | `50` | the welcome gift (#827, #943): the likes a guest's device gets when it becomes a member |
| `WALLET_CONVICTION_PROBABILITIES` | `0.25,0.40,0.60,0.80` | the probability each conviction level C1–C4 stands for (#888): four ascending values in (0, 1); anything else keeps the default |
| `WALLET_ENVELOPE_SECS` | `600` | how often the envelope checks for a finished day |
| `WALLET_ALLOW_LOG_PUBLISHER` | unset | `true`: start without `KAFKA_BROKERS`, logging events (local only) |

An unparsable or negative value keeps the default.

### Inherited infrastructure variables

`DATABASE_URL` / `PG_*` (Postgres), `KAFKA_*` (the consumer), `GRPC_EDGE_ADDR` and the edge token
settings (client edge), OTel.

### Compile-time features

`integration-wallet` — the live Postgres suite (`cargo test -p wallet --features integration-wallet`).

---

## 🚀 Deployment, Migrations & Rollback &nbsp;·&nbsp; OPS

Migrations: `migrations/0001_create_wallet_tables.sql`, `0002_transaction_ref.sql` (`ref_id`),
`0003_create_stakes.sql` (`stakes`, the hour's index), `0004_create_outbox.sql` (`wallet_outbox`),
`0005_create_settlements.sql` (`settlements`; `stakes.settled_at` / `settle_claimed_until`),
`0006_create_envelope_days.sql` (`envelope_days`; the settlements' shares), `0007_stakes_by_account_recent.sql`
(`ListMyStakes`), `0008_stake_conviction.sql` (`wallet_transactions.conviction`, `stakes.first_conviction`, #888),
`0009_welcome_gifts.sql` (`welcome_gifts`, the devices welcomed, #827), `0010_stake_pack_id.sql`
(`wallets.stake_pack_id`, the loaded pack, #941), `0011_owned_items.sql` (`owned_items`, the catalogue items
bought, #892),
applied by `migrator wallet` (init
container) before the binary. Infra (ECR repo, manifests, `wallet-postgres`, ingress route,
NetworkPolicy): core-platform-infra#41 — the binary joins `FLEET_BINS` once its ECR repo exists.
Rollback: the binary is stateless; the schema is additive.

---

## 📈 Telemetry, Performance & Metrics &nbsp;·&nbsp; CORE

Spans `wallet.open`, `wallet.claim`, `wallet.buy_stake_pack`, `wallet.purchase_item`, `wallet.spend_gems`, `wallet.stake`,
`wallet.staked_on`, `wallet.history`, `wallet.erase` (with the shard). One row
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
2. Gem spends: the stake packs and `SpendGems`, no gem spend under 18 (this part);
   country unlocks, standings and the map filter in geo-discovery.
3. Likes are points: `Stake` (this part); then the like counts, notifications, the country ladder and
   the interest tags move to `StakeCommitted`; the reactions are gone, and the GDPR export reads
   an account's likes.
4. Stake settlement (gems earned), shadow mode first: engagement keeps each liker's arrival (S1), the
   settler scores positions without minting (S2), then the daily envelope shares provisional gems
   (S3, this part); minting and the creator pool after calibration.

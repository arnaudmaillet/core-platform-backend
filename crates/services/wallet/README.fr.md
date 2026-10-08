---
i18n:
  source: ./README.md
  source_sha256: d94f58b8f040e4f4106f99e45dd8bb13b94faaa10b3ff22f3f59774e187ede13
  translated_at: 2026-10-08
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`README.md`](./README.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, variables
> d'environnement, topics Kafka, identifiants) sont volontairement laissés en anglais.

# `wallet` — Un registre d'économie in-app qui ne crédite jamais deux fois et ne devine jamais un solde

> **Fiche service** &nbsp;·&nbsp; CORE
>
> | | |
> |---|---|
> | **Propriétaire** | équipe plateforme |
> | **Tier** | TIER-0 (fail-closed) |
> | **Déployable** | `crates/apps/wallet-server` (crate bibliothèque : `crates/services/wallet`) |
> | **Stockage** | Postgres (cluster CNPG dédié, tables `wallets`, `wallet_transactions`) |
> | **Asynchrone** | consomme `account.v1.events` (groupe `wallet-account-events`) · ne publie rien pour l'instant |
> | **Appelants** | l'app (edge client `:9443`) · geo-discovery (mesh `SpendGems`, déblocage de pays) |
> | **Dépendances** | Postgres, Kafka |
> | **SLO** | 99,9 % de dispo · p99 lecture < 50 ms · p99 réclamation < 100 ms |

---

## 🎯 Rôle du service

`wallet` est le **registre de l'économie** (#665) : il possède les deux monnaies in-app d'un
compte et chacun de leurs mouvements.

| Monnaie | Affichée comme | Gagnée par | Dépensée pour |
|---|---|---|---|
| **Points** | likes (le cœur) | la réclamation horaire | les mises sur posts et commentaires (PR suivante) |
| **Gems** | le diamant | le don de départ ; les mises réglées (plus tard) | le pack de mises ×100 ; le déblocage de pays (geo-discovery) |

Rien n'est vendu contre de l'argent réel : ni StoreKit, ni reçus, ni restauration. Aucune monnaie
ne se convertit en l'autre, en argent ou vers un autre utilisateur. Un portefeuille par **compte**
(tous ses profils).

Le problème difficile : **les relances mobiles et les appareils concurrents face à un solde**. Une
réclamation tapée sur deux téléphones, ou rejouée sur un réseau instable, ne doit créditer qu'une
fois ; un solde doit toujours égaler la somme de son registre. Le service verrouille la ligne du
portefeuille à chaque écriture, écrit le solde et sa ligne de registre dans une même transaction,
et rend chaque crédit idempotent par `(account, idempotency_key)`.

**Objectifs :** jamais deux crédits · jamais un solde que le registre ne justifie pas · l'horloge
du serveur décide de chaque réclamation.

---

## 📐 Architecture & concepts

```
app ──gRPC :9443 (edge, require_account)──► WalletServiceHandler
                                              │
                                              ▼
                                   Wallets (cas d'usage) ── domain::Wallet (règles, pures)
                                              │
                                              ▼ port WalletStore
                                   PgWalletStore ── BEGIN; INSERT wallets ON CONFLICT DO NOTHING
                                                    (+ ligne du don de départ); SELECT … FOR UPDATE;
                                                    UPDATE wallets + INSERT wallet_transactions; COMMIT
account.v1.events ──► consommateur account (run_consumer) ──► Wallets::erase
```

**Le registre.** `wallet_transactions` est en ajout seul : `currency`, `delta` signé, le
`balance_after` produit, `kind`, `idempotency_key`, `created_at`. `UNIQUE (account_id,
idempotency_key)` est le mécanisme d'idempotence : une clé rejouée renvoie son premier résultat.
La clé d'un appelant fait 8 à 64 caractères `[A-Za-z0-9_-]` et est stockée **rattachée à son
opération** (`claim:<clé>`, `pack:<clé>`, `spend:<clé>`) : une clé de réclamation ne peut jamais
passer pour un pack payé ; celles du service commencent par `sys:` (ex. `sys:starter-gems`).
Aucune clé d'appelant ne contient de `:`.

**La réclamation horaire** (mêmes règles que la maquette de l'app) :

| Règle | Valeur |
|---|---|
| Intervalle | une réclamation par heure |
| Base | 25 points |
| Série | +10 % par jour UTC consécutif après le premier, jusqu'à ×2 (arrondi au plus proche, 0,5 vers le haut) |
| Plafond quotidien | 200 points par jour UTC ; la dernière réclamation est ramenée à ce qui reste |
| Jour plafonné | la réclamation suivante ouvre à 00:00 UTC |
| Série affichée | la chaîne tant qu'elle vit (une réclamation aujourd'hui ou hier), sinon 0 |

**Le don de départ.** Un portefeuille s'ouvre au premier usage avec 100 gems (`STARTER_GIFT`), une
seule fois.

**Dépenser des gems** (adultes seulement : un jeton de moins de 18 ans, ou sans âge, reçoit
`WAL-3001`) :

| Dépense | Règle |
|---|---|
| Pack de mises ×100 (`BuyStakePack`, edge) | 3 tirs de 100 points **de l'acheteur**, 50 gems ; un pack à la fois (`PACK_STILL_ACTIVE` tant qu'il reste des tirs, rien n'est débité) ; les tirs n'expirent jamais |
| Déblocage de pays (`SpendGems`, mesh) | geo-discovery fixe le prix, vérifie que l'acheteur est adulte et demande les gems avec sa propre clé ; `ref_id` = le code du pays |

Les gems n'achètent jamais de points, d'argent, ni rien pour un autre utilisateur.

> **Invariants** (et où ils sont tenus) : soldes ≥ 0 (`CHECK`) ; chaque solde = Σ des deltas de son
> registre (même transaction, verrou de ligne ; vérifié en test d'intégration) ; un mouvement par
> clé rattachée (`UNIQUE`) ; une réclamation par intervalle et un pack à la fois (verrou + règle du
> domaine) ; seulement le portefeuille de l'appelant (`edge::require_account`) ; aucune dépense de
> gems avant 18 ans (claim `age` du jeton, fail-closed).

---

## 📊 Objectifs de niveau de service (SLO) &nbsp;·&nbsp; OPS

| SLI | Objectif | Fenêtre | Mesuré par |
|---|---|---|---|
| Disponibilité (hors `UNAVAILABLE`/`INTERNAL`) | 99,9 % | 30 j glissants | métriques serveur gRPC |
| `GetWallet` p99 | < 50 ms | 1 h | span `wallet.open` |
| `ClaimReward` p99 | < 100 ms | 1 h | span `wallet.claim` |
| Retard d'effacement | < 60 s | direct | retard de `wallet-account-events` |
| Durabilité | aucun mouvement validé perdu | — | commit synchrone Postgres |

**Budget d'erreur :** 0,1 % / 30 j ≈ 43 min. **En cas de dépassement :** gel des déploiements.

---

## 🔗 Dépendances & rayon d'impact &nbsp;·&nbsp; OPS

| Dépendance | Rôle | Si en panne → | Dégradation |
|---|---|---|---|
| Postgres | le registre | tous les RPC échouent | **Dure** — `UNAVAILABLE` (fail-closed) |
| Kafka | effacement des comptes | les effacements attendent | **Douce** — consommés au rétablissement |

| Appelant | Utilise | Impact si `wallet` tombe |
|---|---|---|
| l'app | `GetWallet`, `ClaimReward`, `ListWalletTransactions`, `BuyStakePack` | solde, réclamation et pack indisponibles ; le reste de l'app fonctionne |
| geo-discovery | `SpendGems` | déblocages de pays refusés (fail-closed) ; la carte fonctionne |

> **Chemin critique ?** Non — le fil, les posts et le chat ne l'appellent pas.

---

## 🔌 Interfaces publiques & contrat d'API &nbsp;·&nbsp; CORE

### gRPC — `wallet.v1.WalletService`

```protobuf
service WalletService {
  rpc GetWallet (GetWalletRequest) returns (Wallet);
  rpc ClaimReward (ClaimRewardRequest) returns (ClaimRewardResponse);
  rpc ListWalletTransactions (ListWalletTransactionsRequest) returns (ListWalletTransactionsResponse);
  rpc BuyStakePack (BuyStakePackRequest) returns (BuyStakePackResponse);
  rpc SpendGems (SpendGemsRequest) returns (SpendGemsResponse);   // mesh seulement
}
```

Tous sauf `SpendGems` sont sur l'edge (`authenticated`), liés au `account_id` de l'appelant
(`edge::require_account` ; un autre compte ⇒ `PERMISSION_DENIED`). `SpendGems` est réservé au mesh
(geo-discovery) : `kind` = `COUNTRY_UNLOCK`, `amount` > 0, `ref_id` ≤ 64 caractères.

- `Wallet.gem_spending_restricted` (edge seulement) dit à l'app de masquer les dépenses de gems ;
  les conditions du pack sont renvoyées (`stake_pack_price`, `stake_pack_shots`, `points_per_shot`).
- `BuyStakePack` répond `BOUGHT`, `PACK_STILL_ACTIVE` ou `INSUFFICIENT_GEMS` dans la réponse ;
  `SpendGems` répond `SPENT` ou `INSUFFICIENT_GEMS`.

- `ClaimReward` répond `CLAIMED`, `TOO_EARLY` ou `DAILY_CAP_REACHED` **dans la réponse** (pas
  d'erreur), avec le portefeuille après l'appel ; `next_claim_at` dit quand la prochaine ouvre.
- `ListWalletTransactions` : du plus récent au plus ancien, les deux monnaies ou une seule ; 50 par
  page par défaut, 100 au plus. Un `TransactionKind` inconnu (d'un serveur plus récent) se lit
  `UNSPECIFIED`.

### Contrat d'erreur (métadonnée `x-error-code`)

| Code | Sens | gRPC |
|---|---|---|
| `WAL-3001` | les gems ne peuvent pas être dépensés (moins de 18 ans, ou âge inconnu) | `PERMISSION_DENIED` |
| `WAL-5001` | une ligne du registre illisible (fail closed) | `INTERNAL` |
| `WAL-9001` | identifiant de compte invalide | `INVALID_ARGUMENT` |
| `WAL-9002` | clé d'idempotence invalide | `INVALID_ARGUMENT` |
| `WAL-9003` | jeton de page invalide | `INVALID_ARGUMENT` |
| `WAL-9004` | dépense de gems invalide (montant, type, référence) | `INVALID_ARGUMENT` |
| `DB-*` | stockage (délégué) | selon l'erreur |

---

## 📨 Événements & contrat asynchrone &nbsp;·&nbsp; CORE

| Topic | Sens | Événement | Effet |
|---|---|---|---|
| `account.v1.events` | consommé (`wallet-account-events`, `run_consumer`) | `account_deleted` | le portefeuille et son historique sont effacés (RGPD art. 17) ; les autres événements sont ignorés ; un identifiant invalide part en DLQ |

Rien n'est publié pour l'instant.

---

## 🌩️ Modes de panne & dégradation &nbsp;·&nbsp; OPS

| Symptôme | Cause | Remède |
|---|---|---|
| `UNAVAILABLE` sur tous les RPC | Postgres en panne | rétablir Postgres ; rien n'est deviné entre-temps |
| `WAL-5001` | une ligne que le service ne sait pas lire (un schéma plus récent annulé) | redéployer vers l'avant ; ne jamais corriger un solde à la main sans ligne de registre |
| le retard d'effacement grandit | Kafka ou Postgres en panne | se résorbe seul ; la DLQ garde les événements empoisonnés |

---

## 📦 Intégration & usage &nbsp;·&nbsp; CORE

```rust
let app = wallet::app::App::build(pool, wallet::config::WalletConfig::from_env());
let view = app.wallets.get(&account_id, chrono::Utc::now()).await?;
```

### Démarrage (`crates/apps/wallet-server`)

`WALLET_GRPC_ADDR` (défaut `0.0.0.0:50072`) → `service_runtime::serve::<WalletService>`.

---

## ⚙️ Configuration & environnement d'exécution &nbsp;·&nbsp; CORE

### Variables propres à `wallet`

| Variable | Défaut | Sens |
|---|---|---|
| `WALLET_GRPC_ADDR` | `0.0.0.0:50072` | écoute mesh |
| `WALLET_CLAIM_INTERVAL_SECS` | `3600` | une réclamation par intervalle |
| `WALLET_CLAIM_BASE_POINTS` | `25` | une réclamation sans série |
| `WALLET_DAILY_CLAIM_CAP` | `200` | points réclamables par jour UTC |
| `WALLET_STARTER_GEMS` | `100` | gems à l'ouverture d'un portefeuille |
| `WALLET_STAKE_PACK_SHOTS` | `3` | tirs dans un pack ×100 |
| `WALLET_STAKE_PACK_PRICE` | `50` | prix d'un pack en gems |
| `WALLET_POINTS_PER_SHOT` | `100` | points misés par tir |

Une valeur illisible ou négative garde le défaut.

### Variables d'infrastructure héritées

`DATABASE_URL` / `PG_*` (Postgres), `KAFKA_*` (le consommateur), `GRPC_EDGE_ADDR` et les réglages
du jeton edge (edge client), OTel.

### Features de compilation

`integration-wallet` — la suite Postgres réelle (`cargo test -p wallet --features integration-wallet`).

---

## 🚀 Déploiement, migrations & retour arrière &nbsp;·&nbsp; OPS

Migrations : `migrations/0001_create_wallet_tables.sql`, `0002_transaction_ref.sql` (`ref_id`), appliquées par `migrator wallet` (init
container) avant le binaire. Infra (dépôt ECR, manifests, `wallet-postgres`, route d'ingress,
NetworkPolicy) : core-platform-infra#41 — le binaire rejoint `FLEET_BINS` dès que son dépôt ECR
existe. Retour arrière : le binaire est sans état ; le schéma est additif.

---

## 📈 Télémétrie, performance & métriques &nbsp;·&nbsp; CORE

Spans `wallet.open`, `wallet.claim`, `wallet.buy_stake_pack`, `wallet.spend_gems`, `wallet.history`,
`wallet.erase` (avec le shard). Un verrou de
ligne par écriture ; l'historique lit `(account_id, created_at DESC, id DESC)`.

---

## 🧪 Développement local & tests &nbsp;·&nbsp; CORE

```bash
cargo test -p wallet                                   # unitaires : règles, registre, handler, edge
cargo test -p wallet --features integration-wallet     # Postgres : concurrence, idempotence, réconciliation
```

---

## 🧭 Feuille de route (#665)

1. Le registre, la réclamation horaire, les gems de départ, l'historique, l'effacement (#853).
2. Dépenses de gems : le pack ×100 et `SpendGems`, aucune dépense de gems avant 18 ans (cette
   partie) ; déblocage de pays, classement et filtre de la carte dans geo-discovery.
3. Mises de points sur posts et commentaires (plafonds anti-abus, ex. 1 000 points par heure).
4. Règlement des mises (gems gagnés).

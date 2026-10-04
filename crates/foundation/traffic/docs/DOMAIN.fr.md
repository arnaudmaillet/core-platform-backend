---
i18n:
  source: ./DOMAIN.md
  source_sha256: 67f9c8b6ba6b97017e3d06dc5ea48b1917bdace090b821b38749a4ef999c254a
  translated_at: 2026-10-04
  status: complete
---
> 🇫🇷 Traduction française — la version **anglaise** [`DOMAIN.md`](./DOMAIN.md) fait foi.
> En cas de divergence, l'anglais prime. Les contrats (codes d'erreur, topics, variables
> d'environnement, noms de types, identifiants d'ADR) restent en anglais.

# `traffic` — Contrat de Domaine & Fonctionnel

> Rate limiting côté serveur : le mécanisme pur qui répond à *« ce caller peut-il passer maintenant ? »* — le miroir en entrée de `resilience`.

> **Domain Card**
>
> | | |
> |---|---|
> | **Capacité partagée** | Rate limiting en entrée — la décision d'admission par réplica pour la charge entrante |
> | **Couche** | `foundation` — une feuille pure ; ne dépend que de `arc-swap`, `async-trait`, `governor` |
> | **Classe de sous-domaine** | **Generic** — un limiteur GCRA de commodité ; la différenciation est dans *l'emplacement*, pas l'algorithme |
> | **Abstraction(s) primaire(s)** | `TrafficProfile` + `QuotaBackend` (`traffic::profile`, `traffic::backend`) |
> | **Empreinte** | pure (aucune IO, aucun spawn) ; la feature `serde` désactivée par défaut la garde sans derive |
> | **Posture en cas d'échec** | **fail-open** — le limiteur ne fait qu'*ajouter* un `Throttle` ; il ne peut pas faire échouer une requête par erreur |
> | **Dépend de** | `arc-swap`, `async-trait`, `governor`, `serde` (optionnel) |
> | **Consommé par** | `transport` (extraction de clé + mapping `RESOURCE_EXHAUSTED`), `infra-config` (parse `[traffic]`), `traffic-redis` (implémente `QuotaBackend`) |
> | **Journal des décisions** | aucun — justification dans [`README §Architecture`](../README.md) |

---

## 1. Capacité Technique & Non-Objectifs &nbsp;·&nbsp; CORE

**Capacité.** `traffic` fait autorité dans la flotte pour la **décision d'admission en entrée** : étant
donné une clé, il répond à **« ce caller est-il dans son budget pour la fenêtre courante, ou doit-il être
throttlé ? »**

**Le problème difficile.** Un limiteur n'est utile qu'au bord du transport, mais coupler l'*algorithme* à
`tonic`/`http` le rendrait intestable et forcerait chaque consommateur à hériter d'une stack web. `traffic`
sépare la décision (pure, ici) de l'extraction-et-mapping (couplée au transport, dans `transport`), pour que
le même mécanisme serve tout futur transport sans changement.

**Non-objectifs — ce que ce crate ne fait délibérément PAS :**
- ❌ Extraire une clé d'une requête / mapper `Throttle` → `RESOURCE_EXHAUSTED` → relève de `transport`.
- ❌ Parser ou valider la section de config `[traffic]` → relève de `infra-config`.
- ❌ Coordonner un budget global à la flotte entre réplicas → relève de `traffic-redis` (le `QuotaBackend`).
- ❌ Protéger un *caller* d'un downstream lent (sortie) → c'est `resilience`, le crate miroir.

---

## 2. Langage Omniprésent &nbsp;·&nbsp; CORE

| Terme | Sens dans ce crate | Symbole de code |
|---|---|---|
| Profile | Un limiteur nommé de classe-de-service résolu depuis la config | `TrafficProfile`, `TrafficProfileSpec` |
| Decision | Le verdict du chemin chaud pour une clé | `TrafficDecision::{Allow, Throttle}` |
| Mode | La localité d'état du limiteur | `Mode::{Local, Distributed}` |
| Scope | La dimension de clé (par méthode / par caller / par IP) | `Scope` |
| Window | La période sur laquelle `rps` est compté (`per_secs`, 1 s par défaut) : `rps = 30, per_secs = 3600` vaut 30 par heure | `TrafficConfig::per_secs` |
| Quota / backend | Le seam « louer N jetons » pour le mode distribué | `Quota`, `QuotaBackend`, `QuotaError` |
| Enforce vs shadow | Si un `Throttle` rejette vraiment ou ne fait que compter | `TrafficProfile::enforce` |

---

## 3. Modèle Public & Surface de Contrat &nbsp;·&nbsp; CORE

| Élément | Nature | Frontière de contrat / invariant gardée |
|---|---|---|
| `TrafficProfile` | handle runtime | Tient le limiteur GCRA derrière `ArcSwap` ; `check(key)` est le chemin chaud, `apply`/`prune` le mutent |
| `TrafficDecision` | type valeur | Exactement deux issues — `Allow` ou `Throttle { retry_after }` ; jamais une erreur |
| `Mode` | enum | `Local` est appliqué ; `Distributed` est *parsé-mais-rejeté* jusqu'à l'arrivée du backend |
| `QuotaBackend` | trait (seam) | Le contrat atomique « louer des jetons » qu'un backend distribué doit honorer |

**Cycle de vie du Mode.**

```
config parse --(Mode::Local)--> appliqué (governor par réplica)
config parse --(Mode::Distributed)--> REJETÉ par la validation infra-config (Step 1)
```

> Seul `Mode::Local` est atteignable en production aujourd'hui. `Distributed` est modélisé pour la
> compatibilité ascendante et rejeté à la validation de config jusqu'au câblage de `traffic-redis` (Step 2).

---

## 4. Propriété & Frontières Architecturales &nbsp;·&nbsp; CORE

**Ce crate possède :**
- Le mécanisme de limiteur, les *types* de config, et `check(key) -> TrafficDecision`. La comptabilité GCRA
  et l'état par clé vivent ici et nulle part ailleurs.

**Ce crate ne possède délibérément PAS / ne doit PAS lier :**

| Préoccupation | Vit dans | Pourquoi l'arête pointe ainsi |
|---|---|---|
| `tonic` / `http` / extraction de clé | `transport` | Garde le mécanisme agnostique au transport et testable unitairement |
| Parsing TOML / validation / bindings | `infra-config` | Frontière de pureté — un crate pur ne lie ni `notify`/`toml` |
| Leasing de jetons inter-réplicas | `traffic-redis` | L'état distribué est un `QuotaBackend` injecté, pas intégré |

**La liste « do-not-depend-on » :** jamais `tonic`, `http`, `notify`, `toml`, ni un client Redis. La feature
`serde` est la *seule* surface optionnelle, désactivée par défaut pour que le cœur ne lie aucun code derive.

---

## 5. Invariants & Règles de Contrat &nbsp;·&nbsp; CORE

| # | Invariant | Appliqué à | En cas de violation |
|---|---|---|---|
| I1 | `check(key)` ne retourne jamais d'erreur — seulement `Allow`/`Throttle` | système de types (`TrafficDecision` n'a pas de variante erreur) | inatteignable |
| I2 | L'état du limiteur par clé doit être borné | runtime — l'appelant lance `prune()` sur un timer | croissance mémoire non bornée |
| I3 | `Mode::Distributed` n'est pas appliqué tant qu'aucun backend n'est câblé | validation `infra-config` | config rejetée au boot |
| I4 | Les swaps de config sont lock-free et ne réinitialisent jamais les compteurs vivants | `ArcSwap` dans `apply` | — |

---

## 6. Flot de Contrôle & Cycle de Vie &nbsp;·&nbsp; DEEP

**Chemin chaud — `check(key)`.** Un lookup/update de cellule GCRA contre l'état `governor` par clé, retournant
`Allow` ou `Throttle { retry_after }`. Aucune allocation sur le chemin commun, aucun réseau en mode `Local`.

**Swap de config — `apply(spec)`.** Piloté par le hot-reload de `infra-config` : `ArcSwap::store` échange le
spec du profil sans verrou. L'état vivant du limiteur (cellules, timers) survit au swap intact.

**Bornage mémoire — `prune()`.** L'état GCRA par clé accumule une entrée par clé distincte (non borné pour le
scope `per_caller`). Le consommateur (la boucle de prune de `service-runtime`) appelle `prune()` à une cadence
pour évincer les clés inactives ; `key_count()` dimensionne la cadence.

---

## 7. Couplage de Crate (tranche du graphe de dépendances) &nbsp;·&nbsp; DEEP

| Crate voisin | Direction | Pattern | Mécanisme | Ce qui casse s'il change |
|---|---|---|---|---|
| `transport` | aval | Published Contract | `check` / `TrafficDecision` | le limiting en entrée de chaque serveur gRPC |
| `infra-config` | aval | Conformist (`serde`) | types wire `TrafficProfileSpec` | le parsing/validation de `[traffic]` |
| `traffic-redis` | aval | Separated Interface | trait `QuotaBackend` | l'application du mode distribué |
| `resilience` | frère (miroir) | — | partage la forme catalog+bindings, direction opposée | symétrie du modèle mental |

> **Seam de stabilité :** `TrafficDecision` et `QuotaBackend` sont une API publique — un changement est
> cassant pour `transport` et `traffic-redis` respectivement.

---

## 8. Signaux Émis & Effets de Bord &nbsp;·&nbsp; DEEP

N/A — mécanisme pur. Il n'émet aucun événement `tracing` ni métrique propre ; la métrique de throttle
(`infra_traffic_throttled_total{status}`) est enregistrée par `transport` là où la décision est appliquée.

---

## 9. Décisions & Justification &nbsp;·&nbsp; DEEP

| Décision | Où consignée | Statut |
|---|---|---|
| Séparer le limiteur pur du glue transport (miroir de `resilience`) | [`README §Architecture`](../README.md) | Accepted |
| `Local` appliqué maintenant, `Distributed` parsé-mais-rejeté jusqu'au Step 2 | [`README §Architecture`](../README.md) | Accepted |

---

## 10. Classification & Évolution &nbsp;·&nbsp; DEEP

- **Classification :** Generic — un limiteur GCRA de commodité ; le levier est le layering, pas le calcul.
- **Stabilité :** en évolution — le mode `Distributed` arrive au Step 2 (le seam `QuotaBackend` existe déjà).
- **Volatilité :** faible — `Allow`/`Throttle` et `check(key)` sont stabilisés ; la croissance est additive
  (nouveaux scopes).
- **Capacités différées :** l'application distribuée globale à la flotte via `traffic-redis` (Step 2), déjà
  modélisée par `Mode::Distributed` + `QuotaBackend`.

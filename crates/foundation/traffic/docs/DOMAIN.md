# `traffic` — Domain & Functional Contract

> Server-side rate limiting: the pure mechanism that answers *"may this caller pass right now?"* — the ingress mirror of `resilience`.

> **Domain Card**
>
> | | |
> |---|---|
> | **Shared capability** | Ingress rate limiting — the per-replica admission decision for inbound load |
> | **Layer** | `foundation` — a pure leaf; depends only on `arc-swap`, `async-trait`, `governor` |
> | **Subdomain class** | **Generic** — a commodity GCRA limiter; differentiation is in *where* it sits, not the algorithm |
> | **Primary abstraction(s)** | `TrafficProfile` + `QuotaBackend` (`traffic::profile`, `traffic::backend`) |
> | **Footprint** | pure (no IO, no spawn); `serde` feature off by default keeps it derive-free |
> | **Failure posture** | **fail-open** — the limiter only ever *adds* a `Throttle`; it cannot fail a request by erroring |
> | **Depends on** | `arc-swap`, `async-trait`, `governor`, `serde` (optional) |
> | **Consumed by** | `transport` (key extraction + `RESOURCE_EXHAUSTED` mapping), `infra-config` (parses `[traffic]`), `traffic-redis` (implements `QuotaBackend`) |
> | **Decision log** | none — rationale in [`README §Architecture`](../README.md) |

---

## 1. Technical Capability & Non-Goals &nbsp;·&nbsp; CORE

**Capability.** `traffic` is the fleet's authority for the **ingress admission decision**: given a key,
it answers **"is this caller within budget for the current window, or must it be throttled?"**

**The hard problem.** A limiter is only useful at the transport edge, but coupling the *algorithm* to
`tonic`/`http` would make it untestable and force every consumer to inherit a web stack. `traffic`
splits the decision (pure, here) from the extraction-and-mapping (transport-coupled, in `transport`),
so the same mechanism serves any future transport unchanged.

**Non-goals — what this crate deliberately does NOT do:**
- ❌ Extract a key from a request / map `Throttle` → `RESOURCE_EXHAUSTED` → owned by `transport`.
- ❌ Parse or validate the `[traffic]` config section → owned by `infra-config`.
- ❌ Coordinate a fleet-global budget across replicas → owned by `traffic-redis` (the `QuotaBackend`).
- ❌ Protect a *caller* from a slow downstream (egress) → that is `resilience`, the mirror crate.

---

## 2. Ubiquitous Language &nbsp;·&nbsp; CORE

| Term | Meaning in this crate | Code symbol |
|---|---|---|
| Profile | A named class-of-service limiter resolved from config | `TrafficProfile`, `TrafficProfileSpec` |
| Decision | The hot-path verdict for one key | `TrafficDecision::{Allow, Throttle}` |
| Mode | State locality of the limiter | `Mode::{Local, Distributed}` |
| Scope | The keying dimension (per-method / per-caller / per-IP) | `Scope` |
| Window | The period `rps` is counted over (`per_secs`, default 1 s): `rps = 30, per_secs = 3600` is 30 an hour | `TrafficConfig::per_secs` |
| Quota / backend | The "lease N tokens" seam for distributed mode | `Quota`, `QuotaBackend`, `QuotaError` |
| Enforce vs shadow | Whether a `Throttle` actually rejects or only counts | `TrafficProfile::enforce` |

---

## 3. Public Model & Contract Surface &nbsp;·&nbsp; CORE

| Element | Kind | Contract / invariant boundary it guards |
|---|---|---|
| `TrafficProfile` | runtime handle | Holds the GCRA limiter behind `ArcSwap`; `check(key)` is the hot path, `apply`/`prune` mutate it |
| `TrafficDecision` | value type | Exactly two outcomes — `Allow` or `Throttle { retry_after }`; never an error |
| `Mode` | enum | `Local` is enforced; `Distributed` is *parsed-but-rejected* until the backend ships |
| `QuotaBackend` | trait (seam) | The atomic "lease tokens" contract a distributed backend must honour |

**Mode lifecycle.**

```
config parse --(Mode::Local)--> enforced (per-replica governor)
config parse --(Mode::Distributed)--> REJECTED by infra-config validation (Step 1)
```

> Only `Mode::Local` is reachable in production today. `Distributed` is modelled for
> forward-compatibility and rejected at config-validation time until `traffic-redis` is wired (Step 2).

---

## 4. Ownership & Architectural Boundaries &nbsp;·&nbsp; CORE

**This crate owns:**
- The limiter mechanism, the config *types*, and `check(key) -> TrafficDecision`. The GCRA accounting
  and per-key state live here and nowhere else.

**This crate deliberately does NOT own / must NOT link:**

| Concern | Lives in | Why the edge points that way |
|---|---|---|
| `tonic` / `http` / key extraction | `transport` | Keeps the mechanism transport-agnostic and unit-testable |
| TOML parsing / validation / bindings | `infra-config` | Purity boundary — a pure crate links no `notify`/`toml` |
| Cross-replica token leasing | `traffic-redis` | Distributed state is an injected `QuotaBackend`, not a built-in |

**The "do-not-depend-on" list:** never `tonic`, `http`, `notify`, `toml`, or a Redis client. The
`serde` feature is the *only* optional surface, and it is off by default so the core links no derive code.

---

## 5. Invariants & Contract Rules &nbsp;·&nbsp; CORE

| # | Invariant | Enforced at | On violation |
|---|---|---|---|
| I1 | `check(key)` never returns an error — only `Allow`/`Throttle` | type system (`TrafficDecision` has no error variant) | unreachable |
| I2 | Per-key limiter state must be bounded | runtime — caller runs `prune()` on a timer | unbounded memory growth |
| I3 | `Mode::Distributed` is not enforced until a backend is wired | `infra-config` validation | config rejected at boot |
| I4 | Config swaps are lock-free and never reset live counters | `ArcSwap` in `apply` | — |

---

## 6. Control Flow & Lifecycle &nbsp;·&nbsp; DEEP

**Hot path — `check(key)`.** One GCRA cell lookup/update against the per-key `governor` state, returning
`Allow` or `Throttle { retry_after }`. No allocation on the common path, no network in `Local` mode.

**Config swap — `apply(spec)`.** Driven by `infra-config` hot-reload: `ArcSwap::store` swaps the profile
spec lock-free. Live limiter state (cells, timers) survives the swap untouched.

**Memory bounding — `prune()`.** Per-key GCRA state accumulates one entry per distinct key (unbounded for
`per_caller` scope). The consumer (`service-runtime`'s prune loop) calls `prune()` on a cadence to evict
idle keys; `key_count()` sizes the cadence.

---

## 7. Crate Coupling (dependency-graph slice) &nbsp;·&nbsp; DEEP

| Neighbour crate | Direction | Pattern | Mechanism | What breaks if it changes |
|---|---|---|---|---|
| `transport` | downstream | Published Contract | `check` / `TrafficDecision` | ingress limiting on every gRPC server |
| `infra-config` | downstream | Conformist (`serde`) | `TrafficProfileSpec` wire types | `[traffic]` parsing/validation |
| `traffic-redis` | downstream | Separated Interface | `QuotaBackend` trait | distributed-mode enforcement |
| `resilience` | sibling (mirror) | — | shares the catalog+bindings shape, opposite direction | mental-model symmetry |

> **Stability seam:** `TrafficDecision` and `QuotaBackend` are public API — a change is a breaking change
> for `transport` and `traffic-redis` respectively.

---

## 8. Emitted Signals & Side-Effects &nbsp;·&nbsp; DEEP

N/A — pure mechanism. It emits no `tracing` events and no metrics of its own; the throttle metric
(`infra_traffic_throttled_total{status}`) is recorded by `transport` where the decision is applied.

---

## 9. Decisions & Rationale &nbsp;·&nbsp; DEEP

| Decision | Where recorded | Status |
|---|---|---|
| Split the pure limiter from the transport glue (mirror of `resilience`) | [`README §Architecture`](../README.md) | Accepted |
| `Local` enforced now, `Distributed` parsed-but-rejected until Step 2 | [`README §Architecture`](../README.md) | Accepted |

---

## 10. Classification & Evolution &nbsp;·&nbsp; DEEP

- **Classification:** Generic — a commodity GCRA limiter; the leverage is the layering, not the math.
- **Stability:** evolving — `Distributed` mode lands in Step 2 (the `QuotaBackend` seam already exists).
- **Volatility:** low — `Allow`/`Throttle` and `check(key)` are settled; growth is additive (new scopes).
- **Deferred capabilities:** fleet-global distributed enforcement via `traffic-redis` (Step 2), already
  modelled by `Mode::Distributed` + `QuotaBackend`.

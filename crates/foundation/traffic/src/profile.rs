//! Named traffic profiles — the bridge between externalized config and the keyed limiter.
//!
//! Two representations, mirroring the `resilience` split:
//!
//! * [`TrafficProfileSpec`] — the **wire** form, `serde`-friendly, parsed from the
//!   `[traffic]` section by `infra-config`.
//! * [`TrafficProfile`] — the **runtime** form holding the `governor` keyed limiter plus a
//!   hot-swappable [`ArcSwap`] config handle. Cloneable and cheap to pass around; clones
//!   share the same handles, so applying an update through any clone reconfigures them all.
//!
//! ## Hot-reload semantics (note the difference from circuit-breaker)
//!
//! `governor` bakes the quota into the limiter at construction, so a quota change can't be
//! swapped in place — [`apply`](TrafficProfile::apply) rebuilds the keyed limiter when
//! `rps`/`burst` change, which resets live per-key buckets. The reset is bounded (at most
//! one burst-worth of extra admissions, fleet-wide, on a deliberate config push) and only
//! happens when the quota actually changes — a reload that leaves this profile's rate
//! untouched (e.g. a different section changing) never resets it.

use std::num::NonZeroU32;
use std::sync::Arc;

use arc_swap::ArcSwap;
use governor::{
    clock::{Clock, DefaultClock},
    DefaultKeyedRateLimiter, Quota, RateLimiter,
};

use crate::config::{BackendError, Mode, Scope, TrafficConfig, TrafficDecision};

/// Deserializable, fleet-facing description of one traffic class-of-service.
///
/// ```toml
/// [traffic.profiles.write-tight]
/// rps = 50
/// burst = 10
/// scope = "per_caller"
///
/// [traffic.profiles.guest-start]
/// rps = 30
/// per_secs = 3600   # 30 an hour
/// burst = 10
/// scope = "per_ip"
/// ```
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TrafficProfileSpec {
    pub rps: u32,
    /// The window `rps` is counted over, in seconds; absent = 1 (per second).
    #[cfg_attr(feature = "serde", serde(default))]
    pub per_secs: Option<u32>,
    pub burst: u32,
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope: Scope,
    #[cfg_attr(feature = "serde", serde(default))]
    pub mode: Mode,
    /// Whether throttle decisions are enforced; `false` is shadow mode. Defaults to `true`
    /// (a profile is enforced unless explicitly shadowed — fail-closed default).
    #[cfg_attr(feature = "serde", serde(default = "default_enforce"))]
    pub enforce: bool,
    #[cfg_attr(feature = "serde", serde(default))]
    pub lease_ms: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub on_backend_error: Option<BackendError>,
}

/// Default for the `enforce` field when absent from config: enforce (not shadow).
#[cfg(feature = "serde")]
fn default_enforce() -> bool {
    true
}

impl TrafficProfileSpec {
    fn to_config(&self) -> TrafficConfig {
        TrafficConfig {
            rps: self.rps,
            per_secs: self.per_secs.unwrap_or(1),
            burst: self.burst,
            scope: self.scope,
            mode: self.mode,
            enforce: self.enforce,
            lease_ms: self.lease_ms,
            on_backend_error: self.on_backend_error,
        }
    }

    /// Lowers the wire spec into a live [`TrafficProfile`], building the keyed limiter and
    /// allocating the shared config handle. Call once per profile at boot.
    pub fn resolve(&self) -> TrafficProfile {
        let config = self.to_config();
        TrafficProfile {
            limiter: Arc::new(ArcSwap::from_pointee(build_limiter(&config))),
            config: Arc::new(ArcSwap::from_pointee(config)),
            clock: DefaultClock::default(),
        }
    }
}

/// Clamps to a non-zero quota component; validation guarantees `> 0`, this is belt-and-braces.
fn nz(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap_or(NonZeroU32::MIN)
}

fn build_limiter(config: &TrafficConfig) -> DefaultKeyedRateLimiter<String> {
    // One cell replenishes every `per_secs / rps` seconds.
    let window = std::time::Duration::from_secs(u64::from(config.per_secs.max(1)));
    let period = (window / nz(config.rps).get()).max(std::time::Duration::from_nanos(1));
    let quota = Quota::with_period(period)
        .unwrap_or_else(|| Quota::per_second(nz(config.rps)))
        .allow_burst(nz(config.burst));
    RateLimiter::keyed(quota)
}

/// Live, layer-facing profile holding the keyed limiter and a hot-reload config handle.
#[derive(Clone)]
pub struct TrafficProfile {
    config: Arc<ArcSwap<TrafficConfig>>,
    limiter: Arc<ArcSwap<DefaultKeyedRateLimiter<String>>>,
    clock: DefaultClock,
}

impl TrafficProfile {
    /// The key dimension to bucket on — read by the layer to pick its key extractor.
    pub fn scope(&self) -> Scope {
        self.config.load().scope
    }

    /// The state-locality mode.
    pub fn mode(&self) -> Mode {
        self.config.load().mode
    }

    /// Whether throttle decisions should be acted on (`false` = shadow mode). Read by the
    /// layer per request, so a hot-reload promotes shadow → enforce with no restart.
    pub fn enforce(&self) -> bool {
        self.config.load().enforce
    }

    /// A snapshot of the current runtime config.
    pub fn config(&self) -> TrafficConfig {
        (**self.config.load()).clone()
    }

    /// Charges one cell against `key` and returns the admission decision. Lock-free; reads
    /// the current limiter snapshot, so a hot-reload is observed by the next call.
    pub fn check(&self, key: &str) -> TrafficDecision {
        match self.limiter.load().check_key(&key.to_owned()) {
            Ok(_) => TrafficDecision::Allow,
            Err(not_until) => TrafficDecision::Throttle {
                retry_after: not_until.wait_time_from(self.clock.now()),
            },
        }
    }

    /// Drops keys that are no longer rate-limiting (idle). Call periodically to bound memory
    /// for unbounded keyspaces (`per_caller`); a no-op-cheap sweep otherwise.
    pub fn prune(&self) {
        self.limiter.load().retain_recent();
    }

    /// Number of currently-tracked keys — for cardinality/memory metrics.
    pub fn key_count(&self) -> usize {
        self.limiter.load().len()
    }

    /// Applies a freshly-loaded spec to the live handles (the hot-reload entry point).
    ///
    /// Rebuilds the keyed limiter **only** when the quota (`rps`/`burst`) changed — see the
    /// module note on why a quota change resets buckets. Other fields swap without a reset.
    pub fn apply(&self, spec: &TrafficProfileSpec) {
        let next = spec.to_config();
        let current = self.config.load();
        let quota_changed =
            next.rps != current.rps || next.per_secs != current.per_secs || next.burst != current.burst;

        if quota_changed {
            self.limiter.store(Arc::new(build_limiter(&next)));
        }
        self.config.store(Arc::new(next));
    }

    /// Helper for callers that only care whether distributed mode is requested (Step 2).
    pub fn is_distributed(&self) -> bool {
        matches!(self.mode(), Mode::Distributed)
    }

    /// Helper exposing the backend-failure policy (Step 2); `None` outside distributed mode.
    pub fn on_backend_error(&self) -> Option<BackendError> {
        self.config.load().on_backend_error
    }

    /// The fleet-global [`Quota`](crate::Quota) to enforce when this profile is distributed.
    /// `lease_ms` falls back to [`DEFAULT_LEASE_MS`](crate::DEFAULT_LEASE_MS) if unset.
    pub fn quota(&self) -> crate::backend::Quota {
        let config = self.config.load();
        crate::backend::Quota {
            rps: config.rps,
            burst: config.burst,
            lease_ms: config.lease_ms.unwrap_or(crate::backend::DEFAULT_LEASE_MS),
        }
    }
}

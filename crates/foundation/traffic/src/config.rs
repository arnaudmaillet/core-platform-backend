//! Runtime traffic values, the keying/mode enums, and the limiter decision type.

use std::time::Duration;

/// Key dimension a profile limits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Scope {
    /// One bucket per gRPC method — fleet-coarse, identity-free, enforceable pre-auth.
    #[default]
    PerMethod,
    /// One bucket per authenticated caller per method. Requires an upstream layer to have
    /// established the principal; falls back to method-level keying when none is present.
    PerCaller,
    /// One bucket per client IP address per method — for anonymous methods (no principal
    /// to key on), e.g. starting a guest session. The address is the one the trusted
    /// proxy (the ALB) appended to `X-Forwarded-For`, else the peer address; falls back to
    /// method-level keying when neither is known. Mobile carriers put many users behind
    /// one address (CGNAT): size these limits generously.
    PerIp,
}

/// Where the limiter's counter state lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Mode {
    /// In-process, per-replica. The only mode enforced today.
    #[default]
    Local,
    /// Redis-coordinated global lease. Parsed for forward-compatibility but not yet
    /// enforced — `infra-config` validation rejects it until Step 2 ships the backend.
    Distributed,
}

/// What a distributed profile does when its coordination backend is unreachable.
/// Parsed now so adding the distributed backend needs no schema migration; inert until then.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum BackendError {
    /// Degrade to the always-on local limiter (availability over precision).
    #[default]
    FailOpen,
    /// Reject (precision/safety over availability) — for hard abuse/billing quotas.
    FailClosed,
}

/// Resolved, runtime traffic values for one profile. Cheap to clone and compare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrafficConfig {
    /// Sustained admit rate: requests per [`per_secs`](Self::per_secs) seconds (per second
    /// by default). Always `> 0` once validated.
    pub rps: u32,
    /// The window `rps` is counted over, in seconds (`1` = per second). Lets a profile
    /// express rates below one per second (`rps = 30, per_secs = 3600` = 30 an hour).
    /// Always `>= 1` once validated.
    pub per_secs: u32,
    /// Bucket capacity — the largest instantaneous burst admitted. Always `>= 1`.
    pub burst: u32,
    /// Key dimension.
    pub scope: Scope,
    /// State-locality mode.
    pub mode: Mode,
    /// Whether throttle decisions are *acted on*. `true` (default) rejects; `false` is
    /// **shadow mode** — the cell is still charged and a would-throttle is observable, but
    /// the request is admitted. Hot-reloadable, so a pilot promotes shadow → enforce (or
    /// rolls back) by editing the ConfigMap, with no redeploy.
    pub enforce: bool,
    /// Distributed-only (Step 2): replica↔backend lease sync cadence, milliseconds.
    pub lease_ms: Option<u64>,
    /// Distributed-only (Step 2): backend-failure policy.
    pub on_backend_error: Option<BackendError>,
}

/// Outcome of a limiter check on the hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrafficDecision {
    /// Admit the request.
    Allow,
    /// Shed the request; `retry_after` is the soonest a retry under this key could succeed.
    Throttle { retry_after: Duration },
}

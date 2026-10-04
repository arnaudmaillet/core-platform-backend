//! The `[traffic]` section: externalized, hot-reloadable ingress rate-limit profiles.
//!
//! The server-side mirror of `[resilience]`: same catalog+bindings shape, but bindings map
//! an inbound gRPC **method** (`/post.PostService/CreatePost`) to a rate-limit profile.
//!
//! ```toml
//! [traffic]
//! default_profile = "standard"
//!
//! [traffic.profiles.standard]
//! rps   = 1000
//! burst = 200
//! scope = "per_method"
//!
//! [traffic.profiles.write-tight]
//! rps   = 50
//! burst = 10
//! scope = "per_caller"
//!
//! [traffic.bindings]
//! "/post.PostService/CreatePost" = "write-tight"
//!
//! # The client edge (optional): clients, not the mesh.
//! edge_default_profile = "client"
//!
//! [traffic.profiles.client]
//! rps   = 50
//! burst = 100
//! scope = "per_caller"
//!
//! [traffic.edge_bindings]
//! "/auth.v1.AuthService/StartGuestSession" = "guest-start"   # a per_ip profile
//! ```
//!
//! # Mesh vs client edge
//!
//! A service serves two listeners off the same routes: the **mesh** one (in-cluster
//! callers, no principal) and, when enabled, the **client edge** one (token-authenticated
//! clients behind the ALB). `bindings` / `default_profile` govern the mesh. The edge uses
//! `edge_bindings`, then `edge_default_profile`, falling back to the mesh resolution when
//! those are absent — so a section without them behaves exactly as before. Keeping them
//! apart matters: a `per_caller` profile on the mesh has no principal to key on and
//! collapses every in-cluster caller of a method into one bucket.

use std::collections::HashMap;

use serde::Deserialize;
use tracing::warn;
use traffic::{Mode, TrafficProfile, TrafficProfileSpec};

use crate::{
    catalog::{validate_bindings, Catalog},
    error::ConfigError,
};

/// TOML header for this section, used in catalog/validation error text.
const SECTION: &str = "traffic";

fn default_traffic_profile() -> String {
    "standard".to_string()
}

/// The `[traffic]` section: a rate-limit-profile catalog plus per-method bindings.
#[derive(Debug, Clone, Deserialize)]
pub struct TrafficSection {
    /// Catalog of named rate-limit profiles (`"standard"`, `"write-tight"`, …).
    pub profiles: HashMap<String, TrafficProfileSpec>,

    /// Maps an inbound gRPC method path to a profile name.
    #[serde(default)]
    pub bindings: HashMap<String, String>,

    /// Profile applied to any method without an explicit binding — so installing the layer
    /// means *every* method is limited (unbound ≠ unlimited; fail-closed by default).
    #[serde(default = "default_traffic_profile")]
    pub default_profile: String,

    /// Client-edge listener: method → profile. Unbound edge methods use
    /// `edge_default_profile`, else the mesh resolution.
    #[serde(default)]
    pub edge_bindings: HashMap<String, String>,

    /// Client-edge listener: profile for methods absent from `edge_bindings`. When set, the
    /// mesh `bindings` do not apply on the edge. `None` = edge methods absent from
    /// `edge_bindings` resolve as on the mesh (`bindings`, then `default_profile`).
    #[serde(default)]
    pub edge_default_profile: Option<String>,
}

impl TrafficSection {
    /// Enforces invariants the type system can't: references resolve, quotas are positive,
    /// and (Step 1) distributed mode is rejected rather than silently under-enforced.
    /// Run before resolving and before every hot-swap (fail-closed).
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_bindings(SECTION, &self.profiles, &self.bindings, &self.default_profile)?;
        let edge_default = self.edge_default_profile.as_deref().unwrap_or(&self.default_profile);
        validate_bindings(SECTION, &self.profiles, &self.edge_bindings, edge_default)?;

        for (name, spec) in &self.profiles {
            if spec.rps == 0 {
                return Err(ConfigError::validation(format!(
                    "[traffic] profile '{name}': rps must be > 0"
                )));
            }
            if spec.per_secs == Some(0) {
                return Err(ConfigError::validation(format!(
                    "[traffic] profile '{name}': per_secs must be >= 1"
                )));
            }
            // The distributed backend budgets per second; a longer window is local-only.
            if matches!(spec.mode, Mode::Distributed) && spec.per_secs.is_some_and(|s| s > 1) {
                return Err(ConfigError::validation(format!(
                    "[traffic] profile '{name}': per_secs > 1 is only supported with mode = \"local\""
                )));
            }
            if spec.burst == 0 {
                return Err(ConfigError::validation(format!(
                    "[traffic] profile '{name}': burst must be >= 1"
                )));
            }
            // Distributed mode requires a lease window; without it the backend can't size
            // its per-window global budget. (The backend itself lives in `traffic-redis`
            // and is wired into the transport layer by the serving binary.)
            if matches!(spec.mode, Mode::Distributed) && !matches!(spec.lease_ms, Some(ms) if ms > 0) {
                return Err(ConfigError::validation(format!(
                    "[traffic] profile '{name}': mode = \"distributed\" requires lease_ms > 0"
                )));
            }
        }

        Ok(())
    }
}

/// The boot-time-resolved set of live rate-limit profiles plus the binding table.
pub struct TrafficRegistry {
    catalog: Catalog<TrafficProfile>,
    /// The client-edge resolution; `None` = same as the mesh. Shares the catalog's live
    /// profiles (clones share handles), so hot-reload reaches both.
    edge: Option<Catalog<TrafficProfile>>,
}

impl TrafficRegistry {
    /// Validates and resolves a `[traffic]` section into live profiles (each owning a
    /// `governor` keyed limiter).
    pub fn from_section(section: TrafficSection) -> Result<Self, ConfigError> {
        section.validate()?;

        let profiles: HashMap<String, TrafficProfile> = section
            .profiles
            .iter()
            .map(|(name, spec)| (name.clone(), spec.resolve()))
            .collect();

        let edge = if section.edge_bindings.is_empty() && section.edge_default_profile.is_none() {
            None
        } else {
            // An edge method bound in neither table keeps its mesh binding, then the
            // edge default (or the mesh default).
            let mut bindings = section.bindings.clone();
            if section.edge_default_profile.is_some() {
                bindings.clear();
            }
            bindings.extend(section.edge_bindings.clone());
            let default = section.edge_default_profile.clone().unwrap_or_else(|| section.default_profile.clone());
            Some(Catalog::new(profiles.clone(), bindings, default))
        };

        Ok(Self {
            catalog: Catalog::new(profiles, section.bindings, section.default_profile),
            edge,
        })
    }

    /// Returns the live profile bound to a gRPC `method`, falling back to the default.
    pub fn profile_for(&self, method: &str) -> TrafficProfile {
        self.catalog.profile_for(method)
    }

    /// Resolves `method` to `(effective profile name, was explicitly bound, profile)`.
    ///
    /// The layer uses this so it can label metrics by profile name and bound-ness — the
    /// name is borrowed, so the hot path allocates nothing until a label is actually built.
    pub fn resolve(&self, method: &str) -> (&str, bool, TrafficProfile) {
        self.catalog.resolve(method)
    }

    /// [`resolve`](Self::resolve) for the **client edge** listener: `edge_bindings`, then
    /// `edge_default_profile`; the mesh resolution when the section configures neither.
    pub fn resolve_edge(&self, method: &str) -> (&str, bool, TrafficProfile) {
        self.edge.as_ref().unwrap_or(&self.catalog).resolve(method)
    }

    /// Looks up a profile by catalog name.
    pub fn profile(&self, name: &str) -> Option<TrafficProfile> {
        self.catalog.profile(name)
    }

    /// Drops idle keys across every profile. The serving binary should call this on an
    /// interval (e.g. once a minute) to bound memory for unbounded keyspaces (`per_caller`);
    /// it is cheap for bounded ones (`per_method`).
    pub fn prune_all(&self) {
        for (_, profile) in self.catalog.iter() {
            profile.prune();
        }
    }

    /// Total number of keys currently tracked across all profiles — for a cardinality gauge.
    pub fn tracked_keys(&self) -> usize {
        self.catalog.iter().map(|(_, profile)| profile.key_count()).sum()
    }

    /// Hot-applies a freshly-parsed `[traffic]` section to the live handles.
    ///
    /// Validates first and bails before any mutation on failure. Only profiles known at
    /// boot are updated; a quota change rebuilds that profile's limiter (see
    /// [`traffic::TrafficProfile::apply`]).
    pub fn apply(&self, section: TrafficSection) -> Result<(), ConfigError> {
        section.validate()?;

        for (name, profile) in self.catalog.iter() {
            match section.profiles.get(name) {
                Some(spec) => profile.apply(spec),
                None => warn!(
                    section = SECTION,
                    profile = %name,
                    "profile absent from reloaded config — keeping previous values (topology change requires restart)"
                ),
            }
        }

        Ok(())
    }
}

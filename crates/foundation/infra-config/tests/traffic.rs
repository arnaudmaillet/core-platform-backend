//! Traffic section: catalog resolution, scope/quota parsing, fail-closed validation.

use infra_config::{InfrastructureConfig, TrafficRegistry};
use traffic::{Scope, TrafficDecision};

const SAMPLE: &str = r#"
[resilience]
default_profile = "standard"
[resilience.profiles.standard]
timeout = { duration_ms = 10000 }
circuit_breaker = { failure_threshold = 5, success_threshold = 2, open_duration_ms = 30000, half_open_max_calls = 1 }
retry = { max_attempts = 3, backoff = { kind = "exponential", base_ms = 50, max_ms = 10000, jitter = "full" } }

[traffic]
default_profile = "standard"
[traffic.profiles.standard]
rps = 1000
burst = 200
scope = "per_method"
[traffic.profiles.write-tight]
rps = 1
burst = 1
scope = "per_caller"
[traffic.bindings]
"/post.PostService/CreatePost" = "write-tight"
"#;

fn traffic_registry(toml: &str) -> TrafficRegistry {
    let cfg = InfrastructureConfig::from_toml(toml).unwrap();
    TrafficRegistry::from_section(cfg.traffic.expect("[traffic] present")).unwrap()
}

#[test]
fn resolves_bindings_scopes_and_quotas() {
    let registry = traffic_registry(SAMPLE);

    let tight = registry.profile_for("/post.PostService/CreatePost");
    assert_eq!(tight.scope(), Scope::PerCaller);
    assert_eq!(tight.config().rps, 1);

    // unbound method -> default ("standard")
    let std = registry.profile_for("/some.Unbound/Method");
    assert_eq!(std.scope(), Scope::PerMethod);
    assert_eq!(std.config().rps, 1000);
}

#[test]
fn enforces_limit_through_resolved_profile() {
    let registry = traffic_registry(SAMPLE);
    let tight = registry.profile_for("/post.PostService/CreatePost");

    // burst = 1 → first cell allowed, second shed.
    assert_eq!(tight.check("acct-1"), TrafficDecision::Allow);
    assert!(matches!(tight.check("acct-1"), TrafficDecision::Throttle { .. }));
    // different caller key has its own bucket.
    assert_eq!(tight.check("acct-2"), TrafficDecision::Allow);
}

#[test]
fn hot_reload_widens_quota() {
    let registry = traffic_registry(SAMPLE);
    let tight = registry.profile_for("/post.PostService/CreatePost");
    assert_eq!(tight.check("k"), TrafficDecision::Allow);
    assert!(matches!(tight.check("k"), TrafficDecision::Throttle { .. }));

    let widened = SAMPLE
        .replace("rps = 1\nburst = 1", "rps = 1000\nburst = 1000");
    let cfg = InfrastructureConfig::from_toml(&widened).unwrap();
    registry.apply(cfg.traffic.unwrap()).unwrap();

    assert_eq!(tight.check("k"), TrafficDecision::Allow);
}

#[test]
fn rejects_zero_rps() {
    let bad = SAMPLE.replace("rps = 1000", "rps = 0");
    let cfg = InfrastructureConfig::from_toml(&bad).unwrap();
    let err = TrafficRegistry::from_section(cfg.traffic.unwrap()).err().expect("expected error");
    assert!(err.to_string().contains("rps must be > 0"), "got: {err}");
}

#[test]
fn distributed_requires_lease_ms() {
    // distributed without lease_ms → rejected.
    let no_lease = SAMPLE.replace(
        "scope = \"per_method\"",
        "scope = \"per_method\"\nmode = \"distributed\"",
    );
    let cfg = InfrastructureConfig::from_toml(&no_lease).unwrap();
    let err = TrafficRegistry::from_section(cfg.traffic.unwrap()).err().expect("expected error");
    assert!(err.to_string().contains("requires lease_ms > 0"), "got: {err}");

    // distributed with lease_ms → accepted.
    let with_lease = SAMPLE.replace(
        "scope = \"per_method\"",
        "scope = \"per_method\"\nmode = \"distributed\"\nlease_ms = 200",
    );
    let cfg = InfrastructureConfig::from_toml(&with_lease).unwrap();
    assert!(TrafficRegistry::from_section(cfg.traffic.unwrap()).is_ok());
}

#[test]
fn rejects_binding_to_unknown_profile() {
    let bad = SAMPLE.replace(r#"= "write-tight""#, r#"= "nope""#);
    let cfg = InfrastructureConfig::from_toml(&bad).unwrap();
    let err = TrafficRegistry::from_section(cfg.traffic.unwrap()).err().expect("expected error");
    assert!(err.to_string().contains("unknown profile 'nope'"), "got: {err}");
}

const EDGE: &str = r#"
[resilience]
default_profile = "standard"
[resilience.profiles.standard]
timeout = { duration_ms = 10000 }
circuit_breaker = { failure_threshold = 5, success_threshold = 2, open_duration_ms = 30000, half_open_max_calls = 1 }
retry = { max_attempts = 3, backoff = { kind = "exponential", base_ms = 50, max_ms = 10000, jitter = "full" } }

[traffic]
default_profile = "standard"
edge_default_profile = "client"
[traffic.profiles.standard]
rps = 1000
burst = 200
scope = "per_method"
[traffic.profiles.client]
rps = 50
burst = 100
scope = "per_caller"
[traffic.profiles.guest-start]
rps = 30
per_secs = 3600
burst = 10
scope = "per_ip"
[traffic.bindings]
"/post.PostService/CreatePost" = "standard"
[traffic.edge_bindings]
"/auth.v1.AuthService/StartGuestSession" = "guest-start"
"#;

#[test]
fn the_edge_resolves_its_own_bindings_and_default_the_mesh_is_unchanged() {
    let registry = traffic_registry(EDGE);

    // Mesh: as before.
    let (name, bound, _) = registry.resolve("/auth.v1.AuthService/StartGuestSession");
    assert_eq!((name, bound), ("standard", false));
    assert_eq!(registry.profile_for("/social_graph.v1.SocialGraphService/CheckAccess").scope(), Scope::PerMethod);

    // Edge: its bindings, then its default; mesh bindings do not leak in.
    let (name, bound, start) = registry.resolve_edge("/auth.v1.AuthService/StartGuestSession");
    assert_eq!((name, bound), ("guest-start", true));
    assert_eq!(start.scope(), Scope::PerIp);
    assert_eq!(start.config().per_secs, 3600);
    let (name, _, client) = registry.resolve_edge("/post.PostService/CreatePost");
    assert_eq!(name, "client");
    assert_eq!(client.scope(), Scope::PerCaller);
}

#[test]
fn without_edge_settings_the_edge_resolves_like_the_mesh() {
    let registry = traffic_registry(SAMPLE);
    let (name, bound, _) = registry.resolve_edge("/post.PostService/CreatePost");
    assert_eq!((name, bound), ("write-tight", true));
    let (name, bound, _) = registry.resolve_edge("/some.Unbound/Method");
    assert_eq!((name, bound), ("standard", false));
}

#[test]
fn edge_references_and_windows_are_validated() {
    for (from, to) in [
        ("edge_default_profile = \"client\"", "edge_default_profile = \"nope\""),
        ("= \"guest-start\"", "= \"nope\""),
        ("per_secs = 3600", "per_secs = 0"),
        ("per_secs = 3600", "per_secs = 3600\nmode = \"distributed\"\nlease_ms = 200"),
    ] {
        let toml = EDGE.replace(from, to);
        let cfg = InfrastructureConfig::from_toml(&toml);
        let section = cfg.ok().and_then(|c| c.traffic);
        assert!(
            section.is_none_or(|s| TrafficRegistry::from_section(s).is_err()),
            "should be rejected: {to}"
        );
    }
}

//! Behavioural tests for the pure limiter: admit/shed, per-key isolation, hot-reload.

use traffic::{Scope, TrafficDecision, TrafficProfileSpec};

fn spec(rps: u32, burst: u32) -> TrafficProfileSpec {
    TrafficProfileSpec {
        rps,
        per_secs: None,
        burst,
        scope: Scope::PerMethod,
        mode: traffic::Mode::Local,
        enforce: true,
        lease_ms: None,
        on_backend_error: None,
    }
}

#[test]
fn admits_up_to_burst_then_sheds() {
    // burst = 2 → two immediate cells admitted, the third shed.
    let profile = spec(1, 2).resolve();
    assert_eq!(profile.check("/svc/M"), TrafficDecision::Allow);
    assert_eq!(profile.check("/svc/M"), TrafficDecision::Allow);
    match profile.check("/svc/M") {
        TrafficDecision::Throttle { retry_after } => assert!(retry_after.as_millis() > 0),
        TrafficDecision::Allow => panic!("expected throttle after burst exhausted"),
    }
}

#[test]
fn keys_are_isolated() {
    let profile = spec(1, 1).resolve();
    // Each distinct key has its own bucket.
    assert_eq!(profile.check("key-a"), TrafficDecision::Allow);
    assert_eq!(profile.check("key-b"), TrafficDecision::Allow);
    // Re-hitting an exhausted key sheds.
    assert!(matches!(profile.check("key-a"), TrafficDecision::Throttle { .. }));
    assert_eq!(profile.key_count(), 2);
}

#[test]
fn widening_quota_hot_reload_admits_again() {
    let profile = spec(1, 1).resolve();
    assert_eq!(profile.check("k"), TrafficDecision::Allow);
    assert!(matches!(profile.check("k"), TrafficDecision::Throttle { .. }));

    // SRE widens the quota; the rebuild resets buckets and admits immediately.
    profile.apply(&spec(1000, 1000));
    assert_eq!(profile.check("k"), TrafficDecision::Allow);
}

#[test]
fn non_quota_reload_preserves_buckets() {
    let profile = spec(1, 1).resolve();
    assert_eq!(profile.check("k"), TrafficDecision::Allow);
    assert!(matches!(profile.check("k"), TrafficDecision::Throttle { .. }));

    // Same rps/burst, only scope flips → no limiter rebuild, exhausted bucket survives.
    let mut same_quota = spec(1, 1);
    same_quota.scope = Scope::PerCaller;
    profile.apply(&same_quota);

    assert_eq!(profile.scope(), Scope::PerCaller);
    assert!(matches!(profile.check("k"), TrafficDecision::Throttle { .. }));
}

#[test]
fn a_window_longer_than_a_second_expresses_slow_rates() {
    // 2 an hour, burst 2: two admitted, the third must wait ~30 minutes.
    let mut slow = spec(2, 2);
    slow.per_secs = Some(3600);
    slow.scope = Scope::PerIp;
    let profile = slow.resolve();
    assert_eq!(profile.scope(), Scope::PerIp);
    assert_eq!(profile.config().per_secs, 3600);
    assert_eq!(profile.check("ip-1"), TrafficDecision::Allow);
    assert_eq!(profile.check("ip-1"), TrafficDecision::Allow);
    match profile.check("ip-1") {
        TrafficDecision::Throttle { retry_after } => assert!(retry_after.as_secs() > 1_700, "{retry_after:?}"),
        TrafficDecision::Allow => panic!("expected throttle"),
    }
    assert_eq!(profile.check("ip-2"), TrafficDecision::Allow, "another address has its own bucket");
}

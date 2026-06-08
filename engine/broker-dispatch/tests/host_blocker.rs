use broker_dispatch::{HostBlocker, HostBlockerConfig};
use std::time::Duration;

#[test]
fn blocks_after_transport_failures_and_clears_on_success() {
    let blocker = HostBlocker::new(HostBlockerConfig {
        failures_before_block: 2,
        initial_cooldown_ms: 50,
        max_cooldown_ms: 50,
        multiplier: 1.0,
    });
    let url = "https://dead.example.com/hook";

    blocker.record_transport_failure(url);
    assert!(!blocker.is_blocked(url));

    blocker.record_transport_failure(url);
    assert!(blocker.is_blocked(url));

    std::thread::sleep(Duration::from_millis(60));
    assert!(!blocker.is_blocked(url));

    blocker.record_transport_failure(url);
    blocker.record_transport_failure(url);
    assert!(blocker.is_blocked(url));

    blocker.record_success(url);
    assert!(!blocker.is_blocked(url));
}

#[test]
fn admin_unblock_clears_host() {
    let blocker = HostBlocker::new(HostBlockerConfig {
        failures_before_block: 1,
        initial_cooldown_ms: 60_000,
        max_cooldown_ms: 60_000,
        multiplier: 1.0,
    });
    let url = "http://slow.example:8080/path";
    blocker.record_transport_failure(url);
    assert!(blocker.is_blocked(url));
    assert!(blocker.unblock(url));
    assert!(!blocker.is_blocked(url));
}

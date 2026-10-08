//! A held call must not outlive the test that held it.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::CallKind;
use kabudachi_core::protocol::ids::ShardName;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_testkit::FaultingAuthority;

/// A node's authority client runs each call on a blocking thread, and a
/// runtime waits for its blocking threads when it shuts down. So a test that
/// ends, or panics, with one of its node's calls held must let that call go,
/// or the test never ends.
#[test]
fn a_held_call_returns_once_no_handle_outside_held_calls_is_left() {
    let authority = FaultingAuthority::new(RealClock::new(), Duration::from_millis(1_000));
    let kept = authority.clone();
    // As a node's client holds it: one handle, shared by the calls it makes.
    let client = Arc::new(authority.clone());
    authority.hold_next(CallKind::ReadRecoveryEpoch);
    let (returned, call_returned) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = client.read_shard(&ShardName::new("shard-1"));
        let _ = returned.send(());
    });
    while !authority.is_holding(CallKind::ReadRecoveryEpoch) {
        std::thread::sleep(StdDuration::from_millis(1));
    }

    drop(authority);
    assert!(
        kept.is_holding(CallKind::ReadRecoveryEpoch),
        "a handle the test still has could release the call, so it stays held"
    );

    drop(kept);
    call_returned
        .recv_timeout(StdDuration::from_secs(10))
        .expect("with no handle left that could release it, the held call returns");
}

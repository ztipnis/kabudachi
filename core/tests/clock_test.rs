mod support;

use kabudachi_core::time::{Clock, Duration, Instant};
use support::clock::FakeClock;

#[test]
fn fresh_fake_clock_reports_initial_instant() {
    let clock = FakeClock::new();
    let now = clock.now();
    assert_eq!(now, Instant::at(0));
}

#[test]
fn advancing_by_duration_moves_now_forward() {
    let clock = FakeClock::new();
    let before = clock.now();

    clock.advance(Duration::from_ticks(42));
    let after = clock.now();

    assert_eq!(after, before + Duration::from_ticks(42));
}

#[test]
fn advancing_by_zero_duration_is_noop() {
    let clock = FakeClock::new();
    let before = clock.now();

    clock.advance(Duration::from_ticks(0));
    let after = clock.now();

    assert_eq!(before, after);
}

#[test]
fn consecutive_advances_compose_correctly() {
    let clock = FakeClock::new();
    let initial = clock.now();

    // Advance by A then B
    clock.advance(Duration::from_ticks(10));
    clock.advance(Duration::from_ticks(20));
    let after_consecutive = clock.now();

    // Create a new clock and advance by A+B
    let clock2 = FakeClock::at(initial);
    clock2.advance(Duration::from_ticks(30));
    let after_direct = clock2.now();

    assert_eq!(after_consecutive, after_direct);
}

#[test]
fn now_never_moves_backward() {
    let clock = FakeClock::new();
    let mut prev = clock.now();

    for i in 0..5 {
        clock.advance(Duration::from_ticks(i));
        let current = clock.now();
        assert!(current >= prev, "Time moved backward!");
        prev = current;
    }
}


use kabudachi_core::time::{Clock, Duration, Instant};
use crate::support::clock::FakeClock;

#[test]
fn wall_clock_millis_is_settable_independently_of_the_monotonic_clock() {
    let clock = FakeClock::new();

    clock.advance(Duration::from_ticks(50));
    clock.set_wall_clock_millis(12_345);

    assert_eq!(
        clock.now(),
        Instant::at(50),
        "setting the wall clock must not move the monotonic clock"
    );
    assert_eq!(clock.wall_clock_millis(), 12_345);
}

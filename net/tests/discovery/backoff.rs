//! The wait between discoveries that found nothing: longer each time up to a
//! ceiling, and gone once one finds work.

use std::time::Duration;

use kabudachi_net::discovery::IdleBackoff;

#[test]
fn discovery_that_finds_nothing_waits_longer_each_time_and_finding_work_resets_it() {
    let mut backoff = IdleBackoff::default();

    let waits: Vec<Duration> = (0..10).map(|_| backoff.after(0)).collect();

    assert_eq!(waits[0], IdleBackoff::MIN);
    assert!(waits.windows(2).all(|pair| pair[1] >= pair[0]), "{waits:?}");
    assert_eq!(*waits.last().unwrap(), IdleBackoff::MAX, "it stops growing at the ceiling");
    assert_eq!(backoff.after(1), Duration::ZERO, "finding work means no wait");
    assert_eq!(backoff.after(0), IdleBackoff::MIN, "and the next empty round starts over");
}

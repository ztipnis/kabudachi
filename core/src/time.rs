//! Kabudachi's own time types, so `core` never has to depend on `tokio` or
//! any other async runtime's clock. One tick is one millisecond: every
//! `Instant`/`Duration` value here, and every `Duration::from_millis`/
//! `from_secs` constructor, agrees on that unit.

/// The time sources the state machine reads: a monotonic reading (`now`) for
/// every timer, plus a wall-clock reading (`wall_clock_millis`) for moments
/// that leave the node.
pub trait Clock {
    fn now(&self) -> Instant;

    /// Milliseconds since the Unix epoch, read from the wall clock.
    /// Used to break ties between competing roll calls and to stamp the
    /// wall-clock times a Task record carries off the node. It is not
    /// monotonic (an NTP sync or a manual correction moves it either way), so
    /// it must never measure elapsed time or drive a timeout.
    fn wall_clock_millis(&self) -> u64;
}

/// A monotonic point in time as a tick count; it advances only when the
/// simulation advances it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(u64);

impl Instant {
    pub const fn at(ticks: u64) -> Self {
        Instant(ticks)
    }

    pub fn as_ticks(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Duration(u64);

/// A moment by a wall clock, in milliseconds since the Unix epoch. Unlike
/// [`Instant`] it can be compared across workers, but only as far as their
/// clocks agree, so it is never used to measure a timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WallTime(u64);

impl WallTime {
    pub const fn from_unix_millis(millis: u64) -> Self {
        WallTime(millis)
    }

    pub fn as_unix_millis(&self) -> u64 {
        self.0
    }

    /// `clock`'s wall-clock reading now.
    pub fn now(clock: &impl Clock) -> Self {
        WallTime(clock.wall_clock_millis())
    }
}

impl Duration {
    pub const fn from_ticks(ticks: u64) -> Self {
        Duration(ticks)
    }

    /// One tick is one millisecond, so this is a direct wrap: it exists for
    /// callers who think in milliseconds, not because the value changes
    /// shape.
    pub const fn from_millis(millis: u64) -> Self {
        Duration(millis)
    }

    /// Saturates at `u64::MAX` ticks instead of overflowing when `secs *
    /// 1_000` would not fit in a `u64`, consistent with `Instant`/`Duration`
    /// arithmetic elsewhere in this module.
    pub const fn from_secs(secs: u64) -> Self {
        Duration(secs.saturating_mul(1_000))
    }

    pub fn as_ticks(&self) -> u64 {
        self.0
    }
}

/// Saturates at zero instead of panicking when `self` is earlier than
/// `other`.
impl std::ops::Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, other: Instant) -> Duration {
        Duration(self.0.saturating_sub(other.0))
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, other: Duration) -> Instant {
        Instant(self.0.saturating_add(other.0))
    }
}

/// The scheduler's clock in real time: one tick per millisecond since this
/// clock was created. It only counts forward, whatever the system clock does.
#[derive(Debug, Clone, Copy)]
pub struct RealClock {
    origin: std::time::Instant,
}

impl RealClock {
    pub fn new() -> Self {
        RealClock {
            origin: std::time::Instant::now(),
        }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now(&self) -> Instant {
        let millis = self.origin.elapsed().as_millis();
        Instant::at(u64::try_from(millis).unwrap_or(u64::MAX))
    }

    fn wall_clock_millis(&self) -> u64 {
        let since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::ZERO);
        u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn from_secs_converts_to_milliseconds() {
        assert_eq!(Duration::from_secs(2), Duration::from_ticks(2_000));
    }

    #[test]
    fn from_secs_saturates_on_overflow() {
        assert_eq!(
            Duration::from_secs(u64::MAX),
            Duration::from_ticks(u64::MAX)
        );
    }

    // Bracketed by std's own monotonic clock rather than a fixed bound, so a
    // slow or descheduled test thread cannot make it fail.
    #[test]
    fn a_new_real_clock_starts_near_zero() {
        let before = std::time::Instant::now();
        let clock = RealClock::new();

        let reading = clock.now().as_ticks();

        let since_before = before.elapsed().as_millis() as u64;
        assert!(
            reading <= since_before,
            "a new clock read {reading} ticks, but only {since_before} ms had passed"
        );
    }

    // The upper bound is std's own measurement of the same span (plus one
    // tick, as each reading rounds down), not a fixed limit, so an overslept
    // `thread::sleep` or a descheduled thread cannot make it fail.
    #[test]
    fn one_tick_passes_per_millisecond() {
        let clock = RealClock::new();
        let outer = std::time::Instant::now();
        let start = clock.now();

        std::thread::sleep(std::time::Duration::from_millis(30));

        let elapsed = (clock.now() - start).as_ticks();
        let outer_millis = outer.elapsed().as_millis() as u64;
        assert!(elapsed >= 30, "elapsed {elapsed} ticks over a 30 ms sleep");
        assert!(
            elapsed <= outer_millis + 1,
            "elapsed {elapsed} ticks while std measured {outer_millis} ms"
        );
    }

    #[test]
    fn real_clocks_wall_clock_reading_is_system_time_in_milliseconds() {
        let clock = RealClock::new();
        let system_millis = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after the Unix epoch")
                .as_millis() as u64
        };

        let before = system_millis();
        let actual = clock.wall_clock_millis();
        let after = system_millis();

        assert!(
            (before..=after).contains(&actual),
            "wall clock reading {actual} should lie between {before} and {after}"
        );
    }

    #[test]
    fn subtraction_saturates_backward() {
        let earlier = Instant::at(10);
        let later = Instant::at(15);
        let duration = earlier - later;
        assert_eq!(duration, Duration::from_ticks(0));
    }

    #[test]
    fn addition_saturates_overflow() {
        let near_max = Instant::at(u64::MAX - 10);
        let large_duration = Duration::from_ticks(100);
        let result = near_max + large_duration;
        assert_eq!(result, Instant::at(u64::MAX));
    }
}

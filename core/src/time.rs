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

    /// The instant on the monotonic clock, which reads `now` while the wall
    /// clock reads `wall_now`, at which `delay` counted from this moment
    /// ends. Time already gone by the wall clock is taken off the delay; a
    /// wall clock that reads before this moment has taken none off.
    pub fn deadline(self, delay: Duration, now: Instant, wall_now: WallTime) -> Instant {
        let elapsed = Duration(wall_now.0.saturating_sub(self.0));
        now + Duration(delay.0.saturating_sub(elapsed.0))
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

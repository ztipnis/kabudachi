/// A monotonic time source, so the state machine never depends on wall-clock
/// time.
pub trait Clock {
    fn now(&self) -> Instant;
}

/// A monotonic point in time as a tick count; it advances only when the
/// simulation advances it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(u64);

impl Instant {
    pub fn at(ticks: u64) -> Self {
        Instant(ticks)
    }

    pub fn as_ticks(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Duration(u64);

impl Duration {
    pub fn from_ticks(ticks: u64) -> Self {
        Duration(ticks)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_addition_works() {
        let start = Instant::at(10);
        let duration = Duration::from_ticks(5);
        let result = start + duration;
        assert_eq!(result, Instant::at(15));
    }

    #[test]
    fn instant_subtraction_works() {
        let later = Instant::at(15);
        let earlier = Instant::at(10);
        let duration = later - earlier;
        assert_eq!(duration, Duration::from_ticks(5));
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

    #[test]
    fn instant_ordering() {
        let a = Instant::at(5);
        let b = Instant::at(10);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, Instant::at(5));
    }

    #[test]
    fn duration_ordering() {
        let a = Duration::from_ticks(5);
        let b = Duration::from_ticks(10);
        assert!(a < b);
        assert!(b > a);
    }
}

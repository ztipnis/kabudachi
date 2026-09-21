use std::time::Instant as StdInstant;

use kabudachi_core::time::{Clock, Instant};

/// The scheduler's clock in real time: one tick per millisecond since this
/// clock was created. It only counts forward, whatever the system clock does.
#[derive(Debug, Clone, Copy)]
pub struct RealClock {
    origin: StdInstant,
}

impl RealClock {
    pub fn new() -> Self {
        RealClock {
            origin: StdInstant::now(),
        }
    }
}

impl Clock for RealClock {
    fn now(&self) -> Instant {
        let millis = self.origin.elapsed().as_millis();
        Instant::at(u64::try_from(millis).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_clock_starts_near_zero() {
        assert!(RealClock::new().now().as_ticks() < 1_000);
    }

    #[test]
    fn one_tick_passes_per_millisecond() {
        let clock = RealClock::new();
        let start = clock.now();

        std::thread::sleep(std::time::Duration::from_millis(30));

        let elapsed = (clock.now() - start).as_ticks();
        assert!((30..1_000).contains(&elapsed), "elapsed {elapsed} ticks");
    }

    #[test]
    fn time_never_goes_backwards() {
        let clock = RealClock::new();
        let mut last = clock.now();

        for _ in 0..1_000 {
            let now = clock.now();
            assert!(now >= last);
            last = now;
        }
    }
}

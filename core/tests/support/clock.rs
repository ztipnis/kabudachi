use kabudachi_core::time::{Clock, Duration, Instant};
use std::cell::Cell;
use std::rc::Rc;

/// A simulated clock. `Clone` shares state, so a clone can be handed to a node
/// while the test keeps advancing the same clock.
pub struct FakeClock {
    now: Rc<Cell<Instant>>,
    /// Kept separate from `now`: real wall clocks can disagree or drift
    /// across workers even while their monotonic clocks agree, so a test
    /// must be able to move this independently.
    wall_clock_millis: Rc<Cell<u64>>,
}

impl Clone for FakeClock {
    fn clone(&self) -> Self {
        Self {
            now: Rc::clone(&self.now),
            wall_clock_millis: Rc::clone(&self.wall_clock_millis),
        }
    }
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(Instant::at(0))),
            wall_clock_millis: Rc::new(Cell::new(0)),
        }
    }

    pub fn at(instant: Instant) -> Self {
        Self {
            now: Rc::new(Cell::new(instant)),
            wall_clock_millis: Rc::new(Cell::new(0)),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let current = self.now.get();
        self.now.set(current + duration);
    }

    /// Sets the wall-clock reading directly, without touching `now()`.
    pub fn set_wall_clock_millis(&self, millis: u64) {
        self.wall_clock_millis.set(millis);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        self.now.get()
    }

    fn wall_clock_millis(&self) -> u64 {
        self.wall_clock_millis.get()
    }
}

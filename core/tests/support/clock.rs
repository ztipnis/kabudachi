use kabudachi_core::time::{Clock, Duration, Instant};
use std::cell::Cell;
use std::rc::Rc;

/// A simulated clock. `Clone` shares state, so a clone can be handed to a node
/// while the test keeps advancing the same clock.
pub struct FakeClock {
    now: Rc<Cell<Instant>>,
}

impl Clone for FakeClock {
    fn clone(&self) -> Self {
        Self {
            now: Rc::clone(&self.now),
        }
    }
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(Instant::at(0))),
        }
    }

    pub fn at(instant: Instant) -> Self {
        Self {
            now: Rc::new(Cell::new(instant)),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let current = self.now.get();
        self.now.set(current + duration);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        self.now.get()
    }
}

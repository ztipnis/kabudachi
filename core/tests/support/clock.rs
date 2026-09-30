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
    /// How far each `now()` moves the clock on after it answers; zero by
    /// default. Shared by clones, like `now`.
    step_on_read: Rc<Cell<Duration>>,
}

impl Clone for FakeClock {
    fn clone(&self) -> Self {
        Self {
            now: Rc::clone(&self.now),
            wall_clock_millis: Rc::clone(&self.wall_clock_millis),
            step_on_read: Rc::clone(&self.step_on_read),
        }
    }
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(Instant::at(0))),
            wall_clock_millis: Rc::new(Cell::new(0)),
            step_on_read: Rc::new(Cell::new(Duration::from_ticks(0))),
        }
    }

    pub fn at(instant: Instant) -> Self {
        Self {
            now: Rc::new(Cell::new(instant)),
            wall_clock_millis: Rc::new(Cell::new(0)),
            step_on_read: Rc::new(Cell::new(Duration::from_ticks(0))),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let current = self.now.get();
        self.now.set(current + duration);
    }

    /// From now on every `now()` returns the current instant and then moves
    /// the clock on by `step`, as a real clock read twice across a tick does.
    pub fn advance_on_every_read(&self, step: Duration) {
        self.step_on_read.set(step);
    }

    /// Sets the wall-clock reading directly, without touching `now()`.
    pub fn set_wall_clock_millis(&self, millis: u64) {
        self.wall_clock_millis.set(millis);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        let read = self.now.get();
        self.now.set(read + self.step_on_read.get());
        read
    }

    fn wall_clock_millis(&self) -> u64 {
        self.wall_clock_millis.get()
    }
}

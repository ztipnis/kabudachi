//! The result backend contract, run against the adapter `core` ships, and how
//! values and composites are split into entries and read back.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kabudachi_core::time::{Clock, Duration, Instant};

mod checks;
mod in_memory;

/// A wall clock that moves only when a test passes time, and that a backend
/// may share across threads, as a result backend must be.
#[derive(Clone, Default)]
struct WallClock {
    millis: Arc<AtomicU64>,
}

impl Clock for WallClock {
    fn now(&self) -> Instant {
        Instant::at(self.millis.load(Ordering::SeqCst))
    }

    fn wall_clock_millis(&self) -> u64 {
        self.millis.load(Ordering::SeqCst)
    }
}

impl kabudachi_testkit::PassTime for WallClock {
    fn pass(&self, duration: Duration) {
        self.millis.fetch_add(duration.as_ticks(), Ordering::SeqCst);
    }
}

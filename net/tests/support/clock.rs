//! A `core` clock that follows tokio's, for tests that pause tokio's time.

use kabudachi_core::time::{Clock, Instant};

/// A [`Clock`] that reads tokio's clock, so paused tokio time pauses it too.
#[derive(Debug, Clone, Copy)]
pub struct TokioClock {
    origin: tokio::time::Instant,
}

impl TokioClock {
    pub fn new() -> Self {
        TokioClock {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::at(u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX))
    }

    // Only breaks ties between roll calls; a monotonic reading is fine here.
    fn wall_clock_millis(&self) -> u64 {
        self.now().as_ticks()
    }
}

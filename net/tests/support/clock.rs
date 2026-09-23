//! A real, millisecond-granularity `Clock` for `net`'s own tests.
//!
//! This is a deliberate ~15-line duplicate of `bindings::clock::RealClock`,
//! not a dev-dependency on `kabudachi-bindings`: `bindings` depends on the
//! whole of `pyo3` (plus `tokio`) as a crate, and pulling that into `net`'s
//! *test* dependency graph for a clock type that isn't itself pyo3-aware
//! would be a real architectural and build-time cost (Python headers/linking
//! becoming a `net`-test requirement) for reusing 15 lines. It would also
//! invert the intended layering: `bindings` is documented as the sole PyO3
//! FFI boundary sitting *above* `net`/`core`, so `net` depending on it — even
//! only in tests — runs against that grain. See task-C3-report.md for the
//! full write-up of this decision.

use std::time::Instant as StdInstant;

use kabudachi_core::time::{Clock, Instant};

/// One tick per millisecond since this clock was created.
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
}

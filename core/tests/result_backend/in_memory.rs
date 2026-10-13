use kabudachi_core::result_backend::InMemoryResultBackend;
use kabudachi_core::time::WallTime;
use kabudachi_testkit::{BackendAdapter, check_result_backend_contract};

use super::WallClock;

struct InMemory {
    clock: WallClock,
}

impl BackendAdapter for InMemory {
    type Backend = InMemoryResultBackend<WallClock>;

    fn fresh(&self) -> Self::Backend {
        InMemoryResultBackend::new(self.clock.clone())
    }

    fn now(&self) -> WallTime {
        WallTime::now(&self.clock)
    }
}

#[test]
fn the_in_memory_backend_keeps_the_result_backend_contract() {
    let clock = WallClock::default();
    check_result_backend_contract(
        &InMemory {
            clock: clock.clone(),
        },
        &clock,
    );
}

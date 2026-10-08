use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::time::Duration;
use kabudachi_testkit::{AuthorityAdapter, check_authority_contract};

use crate::support::clock::FakeClock;

/// In-memory authorities on one simulated clock.
struct InMemory {
    clock: FakeClock,
    ttl: Duration,
}

impl AuthorityAdapter for InMemory {
    type Authority = InMemoryAuthority<FakeClock>;

    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn fresh(&self) -> Self::Authority {
        InMemoryAuthority::new(self.clock.clone(), self.ttl)
    }

    fn flush(&self, authority: &Self::Authority) {
        authority.flush();
    }

    /// An in-memory authority never fails on its own; what makes its calls
    /// fail while it is down is fault injection, which the contract does
    /// not cover.
    fn go_down(&self, _authority: &Self::Authority) {}

    fn come_back(&self, authority: &Self::Authority) {
        authority.back_from_outage();
    }
}

#[test]
fn the_in_memory_authority_keeps_the_coordination_authority_contract() {
    let clock = FakeClock::new();
    let adapter = InMemory {
        clock: clock.clone(),
        ttl: Duration::from_millis(1_000),
    };
    check_authority_contract(&adapter, &clock);
}

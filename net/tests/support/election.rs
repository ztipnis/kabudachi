//! Election helpers shared by the `net/tests/<area>/` crates.

use kabudachi_core::election::Step;
use kabudachi_core::time::Clock;

/// A step that asks for nothing and is due now: the first step of a node
/// started as a founder or inside a known configuration, and what a node
/// driven before hands `run_driver` when it is driven again.
pub fn due_now(clock: &impl Clock) -> Step {
    Step {
        outputs: Vec::new(),
        next_deadline: Some(clock.now()),
    }
}

/// Calls `build` until `clock` reads the same tick before and after the
/// call, and returns what that call built. A node starts its leader-contact
/// timer on the tick it is built, so the nodes one such call builds all
/// start theirs on the same tick.
///
/// A node counts its leader contact as fresh for exactly one suspicion
/// timeout, and suspects no sooner. Built on one tick, every node's contact
/// has gone stale by the time any node's roll call can reach it, so the
/// first roll call is answered and the test's election settles at once.
/// Built a tick apart, the first call could reach a node whose contact is
/// still fresh, which refuses it; the election would still settle, but
/// only once a later call is retried a roll-call deadline and a suspicion
/// timeout on. The gossip roll call, unlike the ring roll call it replaced,
/// does not need the winner to suspect first.
///
/// Should two nodes suspect together, the tie-break ranks their calls the
/// same way on both, the worse initiator answers the better call and
/// abandons its own, and the better one wins. Which one wins depends on the
/// jitter, the wall clock and the `WorkerId`s, so a test that does not fix
/// them must not name the winner.
///
/// Equal suspicion timeouts (or a follower's longer than its leader's) are
/// the safe way round: the leader's lease runs out before a follower could
/// suspect it (see `ElectionTimings::suspect_timeout`).
pub fn built_on_one_tick<C: Clock, T>(clock: &C, mut build: impl FnMut() -> T) -> T {
    loop {
        let before = clock.now();
        let built = build();
        if clock.now() == before {
            return built;
        }
    }
}

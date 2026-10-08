//! A leader leaves office exactly when its lease ends: a node whose clock ran
//! past the end of its lease between two ticks never acts on its next input
//! as leader. The node checks its lease once, in `step`, before any input.

use std::collections::BTreeSet;

use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

use super::scenario_records::{STEP, elected_among};

#[test]
fn a_leader_whose_lease_ends_between_ticks_is_no_leader_on_its_next_input() {
    let (mut cluster, leader) = elected_among(3);
    assert_eq!(cluster.states()[&leader], WorkerState::Leader);
    assert!(cluster.scheduler_mut(&leader).is_leader());

    // The clock moves past the end of the lease with no tick and no message
    // in between.
    let lease = cluster.node(&leader).timings().lease_length();
    cluster.advance_clock_only(Duration::from_ticks(lease.as_ticks() + STEP.as_ticks()));
    assert!(!cluster.scheduler_mut(&leader).is_leader(), "the grant ended");

    let outputs = cluster.step(
        &leader,
        Input::WatchWorkers {
            silent_holders: BTreeSet::new(),
            answered: BTreeSet::new(),
        },
    );

    assert_eq!(cluster.states()[&leader], WorkerState::NoQuorum);
    assert!(
        outputs.contains(&Output::Grant(None)),
        "the node reports that it holds no grant"
    );
}

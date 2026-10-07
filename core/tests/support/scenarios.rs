//! Election helpers shared by the scenario tests. They let a scenario elect a
//! leader among chosen nodes with one roll call it picks the initiator of,
//! and let a leader that has been cut off run out its lease.

use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, Instant};

use crate::support::builders::{past_any_suspicion, timings};
use crate::support::harness::{Cluster, StepRecord};

/// Lets the clock run past every node's suspicion timeout, whatever its
/// jitter, without delivering or ticking anything, then moves each of
/// `members` to `LeaderSuspect` with a single `Tick`, stopping short of the
/// roll call a second `Tick` would start.
///
/// Every other node's timers run out too and fire on the next `advance`, so
/// this suits a cluster where nothing but `members` is still taking part.
pub fn suspect_leader_by_hand(cluster: &mut Cluster, members: &[WorkerId]) {
    cluster.advance_clock_only(past_any_suspicion(cluster.suspect_timeout().as_ticks()));
    for id in members {
        cluster.step(id, Input::Tick);
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "{id:?} must suspect its leader once its suspicion timer has run out"
        );
    }
}

/// Elects a real, quorum-granted `Leader` among `members` (all
/// `LeaderSuspect` and mutually reachable, and together a quorum of their
/// configuration) with a single roll call, started by the first of them,
/// which wins and is returned.
///
/// Only that one is ticked: into `RollCall`, then, once the others have
/// answered its call as the harness delivers the messages and the call's
/// deadline has come, into `Candidate`. The others grant it their votes
/// without being ticked. The new leader announces itself with an ack to
/// every member, which returns each from `LeaderSuspect` to `Active` in that
/// same delivery, before any of them is ticked into a roll call of its own.
/// The others then heartbeat the new leader from there on.
pub fn elect_new_leader_among(cluster: &mut Cluster, members: &[WorkerId]) -> WorkerId {
    for id in members {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "every member must start LeaderSuspect"
        );
    }

    let initiator = members[0].clone();
    cluster.step(&initiator, Input::Tick);
    assert_eq!(
        cluster.states()[&initiator],
        WorkerState::RollCall,
        "the initiator must start its own roll call"
    );

    cluster.deliver_messages();
    cluster.advance_clock_only(timings(cluster.suspect_timeout()).roll_call_deadline);
    cluster.step(&initiator, Input::Tick);
    assert_eq!(
        cluster.states()[&initiator],
        WorkerState::Candidate,
        "the initiator must stand at its roll call's deadline"
    );
    cluster.deliver_messages();

    assert_eq!(
        cluster.states()[&initiator],
        WorkerState::Leader,
        "the initiator must reach quorum-granted Leader"
    );
    initiator
}

/// Bootstraps 5 nodes and elects a real leader with heartbeats flowing. Each
/// suspects after its own jittered suspicion timeout; the first to suspect
/// (or, among nodes that suspect together, the best roll call: the lowest
/// `WorkerId`, as every node reads the same wall clock) wins, and every
/// other node answers it. Returns `(cluster, leader)` settled.
///
/// Every follower then heartbeats the leader in the same phase, so when a
/// scenario later cuts the leader off, the survivors' leader contact goes
/// stale at the same instant. Electing inside a partition would leave the
/// followers that joined after it heals a heartbeat out of phase: a survivor
/// whose contact is still fresh refuses the first roll call, and a scenario
/// that elects its next leader by hand, with a single roll call, needs that
/// call answered.
pub fn bootstrap_5_and_elect_leader(
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap(5, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);

    let leader = cluster.leader().expect("the five must elect a leader");
    for id in &ids {
        let expected = if *id == leader {
            WorkerState::Leader
        } else {
            WorkerState::Active
        };
        assert_eq!(
            cluster.states()[id],
            expected,
            "the whole 5-node cluster must settle"
        );
    }

    (cluster, leader)
}

/// Advances a settled cluster whose leader has just been cut off from enough
/// followers to lose its quorum, to where the leader has run out its
/// quorum-contact lease but no follower has yet suspected it.
///
/// A settled follower last heard from its leader at most one heartbeat
/// interval ago, and suspects it a suspicion timeout after that; the newest
/// ack a follower has confirmed is older still, and the lease runs a tenth
/// short of the suspicion timeout from it. So a suspicion timeout less one
/// heartbeat interval falls between the two.
pub fn run_out_cut_off_leaders_lease(cluster: &mut Cluster) {
    let timings = timings(cluster.suspect_timeout());
    let lease_run_out = timings.suspect_timeout.as_ticks() - timings.heartbeat_interval.as_ticks();
    cluster.advance(Duration::from_ticks(lease_run_out));
}

/// When `leader` first reported `worker` lost among `steps`.
pub fn reported_lost_at(steps: &[StepRecord], leader: &WorkerId, worker: &WorkerId) -> Instant {
    steps
        .iter()
        .find(|step| {
            step.node == *leader
                && step
                    .outputs
                    .iter()
                    .any(|output| *output == Output::WorkerLost(worker.clone()))
        })
        .map(|step| step.at)
        .unwrap_or_else(|| panic!("{leader:?} reported {worker:?} lost"))
}

/// The abort deadline `worker` last reported among `steps` taken no later
/// than `at`, `Some(None)` for a withdrawal; `None` if it reported none.
pub fn abort_deadline_at(
    steps: &[StepRecord],
    worker: &WorkerId,
    at: Instant,
) -> Option<Option<Instant>> {
    steps
        .iter()
        .filter(|step| step.node == *worker && step.at <= at)
        .flat_map(|step| &step.outputs)
        .filter_map(|output| match output {
            Output::AbortDeadline(deadline) => Some(*deadline),
            _ => None,
        })
        .next_back()
}

/// Asserts that by `replayed_at`, when a leader replays `worker`'s runs,
/// `worker` has been told to abort them no later than that.
pub fn assert_aborts_by(steps: &[StepRecord], worker: &WorkerId, replayed_at: Instant) {
    let deadline = abort_deadline_at(steps, worker, replayed_at).flatten();
    assert!(
        deadline.is_some_and(|by| by < replayed_at),
        "{worker:?} must abort before its runs are replayed at {replayed_at:?}, but its \
         deadline was {deadline:?}"
    );
}

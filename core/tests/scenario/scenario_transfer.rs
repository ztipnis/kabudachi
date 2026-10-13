//! Leadership handed over as a leader drains: the shard has a new leader
//! within about a roll call, and the old leader leaves through it once it
//! leads. A successor that never stands costs no more than a suspicion
//! timeout. A sole voter admits its pending member and hands over to it.

use std::collections::BTreeSet;

use kabudachi_core::election::{HandOffTo, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

use crate::support::harness::{Cluster, StepRecord};

/// Long against a roll call (a quarter of it): a hand-over and an
/// election after a suspicion are told apart by time alone.
const SUSPECT: Duration = Duration::from_ticks(40);
const TICK: Duration = Duration::from_ticks(1);
/// Past the default drain wait of ten suspicion timeouts.
const PATIENCE_TICKS: u64 = 40 * 30;

fn run_until(cluster: &mut Cluster, done: impl Fn(&Cluster) -> bool) -> bool {
    for _ in 0..PATIENCE_TICKS {
        if done(cluster) {
            return true;
        }
        cluster.advance(TICK);
    }
    done(cluster)
}

fn in_office(cluster: &Cluster, id: &WorkerId) -> bool {
    matches!(cluster.states()[id], WorkerState::LeaderReconciling | WorkerState::Leader)
}

/// `voters` voters and `pending` joiners, run until a leader leads a
/// committed configuration.
fn elected(voters: usize, pending: usize) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap_with_pending(voters, pending, SUSPECT);
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some_and(|leader| cluster
            .node(&leader)
            .configuration()
            .is_some_and(|configuration| !configuration.is_joint()))),
        "the voters elect a leader and commit its founding"
    );
    let leader = cluster.leader().expect("a leader");
    (cluster, leader)
}

fn transfers(steps: &[StepRecord], from: &WorkerId) -> Vec<WorkerId> {
    steps
        .iter()
        .filter(|step| step.node == *from)
        .flat_map(|step| step.outputs.iter())
        .filter_map(|output| match output {
            Output::TransferLeadership { to } => Some(to.clone()),
            _ => None,
        })
        .collect()
}

fn hand_offs(steps: &[StepRecord], from: &WorkerId) -> Vec<HandOffTo> {
    steps
        .iter()
        .filter(|step| step.node == *from)
        .flat_map(|step| step.outputs.iter())
        .filter_map(|output| match output {
            Output::HandOff(to) => Some(to.clone()),
            _ => None,
        })
        .collect()
}

fn assert_no_grant_overlap(cluster: &Cluster) {
    assert!(cluster.first_grant_overlap().is_none(), "two nodes held a valid grant at once");
}

#[test]
fn a_draining_leader_hands_over_within_about_a_roll_call_and_leaves_through_its_successor() {
    let (mut cluster, leader) = elected(3, 0);
    let followers: BTreeSet<WorkerId> =
        cluster.node_ids().into_iter().filter(|id| *id != leader).collect();
    cluster.record_steps();
    let asked_at = cluster.now();

    cluster.drain(&leader);
    assert!(run_until(&mut cluster, |cluster| followers.iter().any(|f| in_office(cluster, f))));
    let took_over_in = cluster.now() - asked_at;
    let successor = followers.iter().find(|f| in_office(&cluster, f)).cloned().expect("a successor");

    // A roll call and a vote take a quarter of a suspicion timeout each;
    // waiting out a suspicion takes a whole one at least.
    assert!(took_over_in < SUSPECT, "the successor took office {took_over_in:?} after the drain");
    assert!(run_until(&mut cluster, |cluster| cluster.states()[&leader] == WorkerState::Stopped));
    assert!(run_until(&mut cluster, |cluster| cluster.states()[&successor] == WorkerState::Leader
        && cluster.node(&successor).voters().len() == 2));
    let steps = cluster.take_steps();
    let position = |node: &WorkerId, state: WorkerState| {
        steps.iter().position(|step| {
            step.node == *node && step.outputs.contains(&Output::StateChanged(state))
        })
    };
    assert!(
        position(&successor, WorkerState::Leader) < position(&leader, WorkerState::Stopped),
        "it leaves once its successor leads, not before"
    );
    assert_eq!(transfers(&steps, &leader), vec![successor.clone()]);
    assert_eq!(hand_offs(&steps, &leader), vec![HandOffTo::Leader(successor)], "the successor places its records");
    assert_no_grant_overlap(&cluster);
}

#[test]
fn a_hand_over_to_a_successor_that_never_stands_costs_no_more_than_a_suspicion_timeout() {
    let (mut cluster, leader) = elected(5, 0);
    // A message takes two ticks, so the one that asks the successor to stand
    // can be cut off in flight.
    cluster.network().set_delay(Duration::from_ticks(2));
    cluster.record_steps();
    cluster.drain(&leader);
    let mut asked = Vec::new();
    for _ in 0..PATIENCE_TICKS {
        cluster.advance(TICK);
        asked = transfers(&cluster.take_steps(), &leader);
        cluster.record_steps();
        if !asked.is_empty() {
            break;
        }
    }
    let target = asked.pop().expect("the leader hands over once its voters crawled");
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != target)
        .collect();
    cluster.partition([target.clone()].into_iter().collect(), rest.clone());
    cluster.network().drop_in_flight_across_partition();

    let survivors: BTreeSet<WorkerId> = rest.into_iter().filter(|id| *id != leader).collect();
    let handed_at = cluster.now();
    let (mut left_in, mut replaced_in) = (None, None);
    for _ in 0..PATIENCE_TICKS {
        cluster.advance(TICK);
        let since = (cluster.now() - handed_at).as_ticks();
        if left_in.is_none() && cluster.states()[&leader] == WorkerState::Stopped {
            left_in = Some(since);
        }
        if replaced_in.is_none() && survivors.iter().any(|id| in_office(&cluster, id)) {
            replaced_in = Some(since);
        }
        if left_in.is_some() && replaced_in.is_some() {
            break;
        }
    }
    // The leader departs as it would alone once the hand-over lapses, a
    // suspicion timeout after it began. Its acks stopped refreshing their
    // contact when it began, so the others suspect it within a suspicion
    // timeout and a half, and elect a leader a roll call and a vote on.
    let suspect = SUSPECT.as_ticks();
    let left_in = left_in.expect("the leader departs");
    assert!(left_in <= suspect + 1, "it left {left_in} ticks after handing over");
    let replaced_in = replaced_in.expect("the others elect a leader");
    assert!(replaced_in < 2 * suspect, "they elected {replaced_in} ticks after it handed over");
    assert!(!in_office(&cluster, &target));
    assert_no_grant_overlap(&cluster);
}

#[test]
fn a_sole_voter_admits_its_pending_member_and_hands_over_to_it() {
    let (mut cluster, leader) = elected(1, 1);
    let joiner = cluster.node_ids().into_iter().find(|id| *id != leader).expect("a joiner");
    cluster.record_steps();

    cluster.drain(&leader);
    assert!(run_until(&mut cluster, |cluster| cluster.states()[&joiner] == WorkerState::Leader
        && cluster.node(&joiner).voters().len() == 1
        && cluster.states()[&leader] == WorkerState::Stopped));
    let steps = cluster.take_steps();
    assert_eq!(transfers(&steps, &leader), vec![joiner.clone()]);
    assert_eq!(hand_offs(&steps, &leader), vec![HandOffTo::Leader(joiner)]);
    assert_no_grant_overlap(&cluster);
}

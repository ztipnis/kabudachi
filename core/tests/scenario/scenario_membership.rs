//! Membership changes across a cluster:
//! admission batches, removals with no commit round, and elections that fall
//! in the middle of either. Every scenario runs real `WorkerNode`s through
//! the `Cluster` harness and checks that no two nodes ever held a valid
//! grant at once.


use crate::support::builders::checked;
use kabudachi_core::protocol::checked::CheckedPayload;
use std::collections::BTreeSet;

use kabudachi_core::configuration::{Admission, Configuration, Generation, Tally};
use kabudachi_core::election::Output;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use crate::support::builders::worker;
use crate::support::harness::{Cluster, StepRecord};

const SUSPECT: Duration = Duration::from_ticks(10);
const TICK: Duration = Duration::from_ticks(1);

/// How long a scenario waits for something that should happen within a few
/// heartbeats or elections.
const PATIENCE_TICKS: u64 = 60 * 10;

fn ids(labels: impl IntoIterator<Item = usize>) -> BTreeSet<WorkerId> {
    labels
        .into_iter()
        .map(|i| worker(&format!("worker-{i}")))
        .collect()
}

/// Advances `cluster` a tick at a time until `done` holds, for at most
/// `PATIENCE_TICKS`. Returns whether it came to hold.
fn run_until(cluster: &mut Cluster, done: impl Fn(&Cluster) -> bool) -> bool {
    for _ in 0..PATIENCE_TICKS {
        if done(cluster) {
            return true;
        }
        cluster.advance(TICK);
    }
    done(cluster)
}

fn configuration_of(cluster: &Cluster, id: &WorkerId) -> Configuration {
    cluster
        .node(id)
        .configuration()
        .cloned()
        .unwrap_or_else(|| panic!("{id:?} holds a configuration"))
}

/// Whether `id` counts as a voter of `configuration`, by the admission
/// generations it holds.
fn is_voter_of(cluster: &Cluster, id: &WorkerId, configuration: &Configuration) -> bool {
    let node = cluster.node(id);
    configuration.is_voter(Admission {
        current: node.admission(),
        prior: node.prior_admission(),
    })
}

/// The leaders among `group`.
fn leaders_among(cluster: &Cluster, group: &BTreeSet<WorkerId>) -> BTreeSet<WorkerId> {
    let states = cluster.states();
    group
        .iter()
        .filter(|id| states[*id] == WorkerState::Leader)
        .cloned()
        .collect()
}

/// A cluster of `voters` voters and `joiners` pending members, the joiners
/// cut off while the voters elect a leader and commit what they founded.
/// The joiners stay cut off. Returns the cluster and its leader.
fn elected_with_joiners_away(voters: usize, joiners: usize) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap_with_pending(voters, joiners, SUSPECT);
    cluster.partition(ids(0..voters), ids(voters..voters + joiners));
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some_and(
            |leader| !configuration_of(cluster, &leader).is_joint()
        )),
        "the voters elect a leader and commit its founding"
    );
    let leader = cluster.leader().expect("a leader");
    (cluster, leader)
}

/// The distinct joint configurations `leader` announced on its acks among
/// `steps`.
fn joint_configurations_acked(steps: &[StepRecord], leader: &WorkerId) -> BTreeSet<Generation> {
    steps
        .iter()
        .filter(|step| step.node == *leader)
        .flat_map(|step| &step.outputs)
        .filter_map(|output| match output {
            Output::Send { message, .. } => match checked(message.clone()).into_payload() {
                Some(CheckedPayload::HeartbeatAck(ack)) => Some(ack.configuration()),
                _ => None,
            },
            _ => None,
        })
        .filter(Configuration::is_joint)
        .map(|configuration| configuration.generation())
        .collect()
}

/// Whether `id` ever reported moving to `state` among `steps`.
fn ever_moved_to(steps: &[StepRecord], id: &WorkerId, state: WorkerState) -> bool {
    steps
        .iter()
        .any(|step| step.node == *id && step.outputs.contains(&Output::StateChanged(state)))
}

/// Whether `workers`, at the admissions they hold, are a quorum of
/// `configuration`.
fn quorum_of(cluster: &Cluster, configuration: &Configuration, workers: &[&WorkerId]) -> bool {
    let mut tally = Tally::against(configuration);
    for id in workers {
        let node = cluster.node(id);
        tally.record(
            (*id).clone(),
            Admission {
                current: node.admission(),
                prior: node.prior_admission(),
            },
        );
    }
    tally.has_quorum()
}

fn assert_no_grant_overlap(cluster: &Cluster) {
    assert_eq!(
        cluster.first_grant_overlap(),
        None,
        "no two nodes ever hold a valid grant at once"
    );
}

/// Three voters elect a leader; four joiners then arrive. The first batch
/// takes the joiner that confirmed first; the other three wait and form the
/// second. Just as the leader starts the second batch, a partition cuts the
/// leader and those three off from the two other voters and the first
/// joiner. Returns the cluster, the leader, and the two sides: the old side
/// (a majority of the configuration the batch moved from, three of four)
/// and the batch side (a majority of the batch's new side, four of seven).
fn second_batch_cut_off() -> (Cluster, WorkerId, BTreeSet<WorkerId>, BTreeSet<WorkerId>) {
    let (mut cluster, leader) = elected_with_joiners_away(3, 4);
    let joiners = ids(3..7);
    // A delay of a few ticks keeps each batch joint for several ticks, so a
    // scenario stepping a tick at a time sees the second one start.
    cluster.network().set_delay(Duration::from_ticks(2));
    cluster.heal();
    let mut batches: Vec<Generation> = Vec::new();
    for _ in 0..PATIENCE_TICKS {
        let configuration = configuration_of(&cluster, &leader);
        if configuration.is_joint() && !batches.contains(&configuration.generation()) {
            batches.push(configuration.generation());
        }
        if batches.len() == 2 {
            break;
        }
        cluster.advance(TICK);
    }
    assert_eq!(batches.len(), 2, "two batches start");
    let (first, second): (BTreeSet<WorkerId>, BTreeSet<WorkerId>) = joiners
        .into_iter()
        .partition(|joiner| cluster.node(joiner).admission().is_some());
    assert_eq!(
        (first.len(), second.len()),
        (1, 3),
        "setup: the first batch took one joiner, the second the three that waited"
    );

    let old_side: BTreeSet<WorkerId> = ids(0..3)
        .into_iter()
        .filter(|id| *id != leader)
        .chain(first)
        .collect();
    let mut batch_side = second;
    batch_side.insert(leader.clone());
    cluster.partition(old_side.clone(), batch_side.clone());
    // No delay from here: under it, a roll call's replies could never beat
    // the tests' roll-call deadline (a quarter of the suspicion timeout),
    // and no side could elect whatever the rules.
    cluster.network().set_delay(Duration::from_ticks(0));
    (cluster, leader, old_side, batch_side)
}

/// A joint configuration's quorums need a majority of both sides, and
/// generations never alias across a split. Cut off mid-batch, the leader and
/// the batch's joiners are a majority of its new side but not of its old; the
/// other side, which has adopted the batch, is a majority of its old side but
/// not of its new. The batch never commits, the old leader's lease runs out,
/// and neither side elects: counting only the new side would let the first
/// elect, only the old side the second. Healed, one leader leads everyone and
/// admits them all.
#[test]
fn a_split_mid_batch_elects_on_neither_side_and_heals_to_one_leader() {
    let (mut cluster, leader, old_side, batch_side) = second_batch_cut_off();
    cluster.record_steps();
    for _ in 0..PATIENCE_TICKS {
        cluster.advance(TICK);
    }
    let steps = cluster.take_steps();

    assert_ne!(cluster.states()[&leader], WorkerState::Leader);
    for id in old_side.iter().chain(&batch_side) {
        assert!(
            !ever_moved_to(&steps, id, WorkerState::Leader),
            "{id:?} won with a majority of one side of the batch alone"
        );
    }

    cluster.heal();
    let everyone = ids(0..7);
    let one_leader_leads_everyone = run_until(&mut cluster, |cluster| {
        let leaders = leaders_among(cluster, &everyone);
        leaders.len() == 1 && {
            let configuration = configuration_of(cluster, leaders.first().unwrap());
            !configuration.is_joint()
                && everyone
                    .iter()
                    .all(|id| is_voter_of(cluster, id, &configuration))
        }
    });
    assert!(one_leader_leads_everyone);
    assert_no_grant_overlap(&cluster);
}

/// A worker cut off while its leader changed the configuration several
/// times (two removals and a batch) calls roll calls under the old one: it
/// never wins, alone or once it returns.
#[test]
fn a_stale_initiator_several_generations_behind_cannot_win() {
    let (mut cluster, leader) = elected_with_joiners_away(5, 1);
    let followers: Vec<WorkerId> = ids(0..5).into_iter().filter(|id| *id != leader).collect();
    let stale = followers[0].clone();
    let before = configuration_of(&cluster, &stale).generation();
    cluster.record_steps();

    let mut rest = ids(0..6);
    rest.remove(&stale);
    cluster.partition(BTreeSet::from([stale.clone()]), rest);
    cluster.drain(&followers[1]);
    cluster.drain(&followers[2]);
    let changed = run_until(&mut cluster, |cluster| {
        let configuration = configuration_of(cluster, &leader);
        !configuration.is_joint() && is_voter_of(cluster, &worker("worker-5"), &configuration)
    });
    assert!(changed, "two removals and a batch");
    let behind = configuration_of(&cluster, &leader).generation();
    assert!(behind > before.next_change(behind.term()).next_change(behind.term()));
    for _ in 0..3 * SUSPECT.as_ticks() {
        cluster.advance(TICK);
    }
    cluster.heal();
    run_until(&mut cluster, |cluster| {
        cluster.states()[&stale] == WorkerState::Active
            && configuration_of(cluster, &stale).generation() >= behind
    });

    let steps = cluster.take_steps();
    assert!(
        steps.iter().any(|step| step.node == stale
            && step
                .outputs
                .contains(&Output::StateChanged(WorkerState::RollCall))),
        "it tried"
    );
    assert!(!ever_moved_to(&steps, &stale, WorkerState::Leader));
    assert_eq!(cluster.leader(), Some(leader));
    assert_no_grant_overlap(&cluster);
}

/// A leader asked to leave before its voters have crawled their routing
/// leads on, however long that takes within its drain wait; once each has
/// crawled it leaves and the survivors elect one of themselves.
#[test]
fn a_draining_leader_waits_for_its_voters_routing_crawl_and_the_survivors_elect() {
    let mut cluster = Cluster::bootstrap(3, SUSPECT);
    cluster.hold_routing_crawls();
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some_and(
            |leader| !configuration_of(cluster, &leader).is_joint()
        )),
        "the voters elect a leader and commit its founding"
    );
    let leader = cluster.leader().expect("a leader");
    let followers: Vec<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();

    cluster.drain(&leader);
    for _ in 0..3 * SUSPECT.as_ticks() {
        cluster.advance(TICK);
        assert_eq!(
            cluster.states()[&leader],
            WorkerState::Leader,
            "it left before its voters crawled"
        );
    }

    for follower in &followers {
        let _ = cluster.routing_crawled(follower);
    }
    // The drain wait lasts ten suspicion timeouts, so only the crawl reports
    // can release the leader this early: were they ignored, it would hold on
    // for most of the remaining wait.
    for _ in 0..2 * SUSPECT.as_ticks() {
        if cluster.states()[&leader] == WorkerState::Stopped {
            break;
        }
        cluster.advance(TICK);
    }
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Stopped,
        "the crawl reports release the leader long before its drain wait ends"
    );
    let survivors_elect = run_until(&mut cluster, |cluster| {
        cluster
            .leader()
            .is_some_and(|new_leader| followers.contains(&new_leader))
    });

    assert!(survivors_elect, "{:?}", cluster.states());
    assert_no_grant_overlap(&cluster);
}

/// A leader asked to drain that loses office before it may leave has not
/// withdrawn its request: once it follows the leader that replaced it, it
/// stops, and that leader keeps leading.
#[test]
fn a_drain_request_kept_across_lost_office_stops_the_node_under_the_new_leader() {
    let mut cluster = Cluster::bootstrap(3, SUSPECT);
    cluster.hold_routing_crawls();
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some_and(
            |leader| !configuration_of(cluster, &leader).is_joint()
        )),
        "the voters elect a leader and commit its founding"
    );
    let leader = cluster.leader().expect("a leader");
    let followers: BTreeSet<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();
    cluster.drain(&leader);
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Leader,
        "no crawl was reported, so it still leads"
    );

    cluster.partition([leader.clone()].into_iter().collect(), followers.clone());
    assert!(
        run_until(&mut cluster, |cluster| !leaders_among(cluster, &followers).is_empty()),
        "the followers elect a leader: {:?}",
        cluster.states()
    );
    let new_leader = leaders_among(&cluster, &followers).into_iter().next().expect("a leader");
    assert_ne!(
        cluster.states()[&leader],
        WorkerState::Stopped,
        "cut off, the old leader has no leader to tell it leaves"
    );
    cluster.heal();

    assert!(
        run_until(&mut cluster, |cluster| cluster.states()[&leader] == WorkerState::Stopped),
        "the kept request stops it once it follows the new leader: {:?}",
        cluster.states()
    );
    assert_eq!(cluster.states()[&new_leader], WorkerState::Leader);
    assert_no_grant_overlap(&cluster);
}

/// A rolling deploy: joiners arrive and old voters drain at once, the drains
/// taking the old side below a majority of its original count mid-batch.
/// Each removal re-announces the batch with shrunk counts, so the batch
/// still commits and its leader never loses its quorum.
#[test]
fn a_rolling_deploy_commits_its_batch_and_never_loses_the_quorum() {
    let (mut cluster, leader) = elected_with_joiners_away(3, 3);
    let joiners = ids(3..6);
    let draining: Vec<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();
    cluster.record_steps();

    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader)
            .is_joint()),
        "a batch starts"
    );
    for id in &draining {
        cluster.drain(id);
    }
    let committed = run_until(&mut cluster, |cluster| {
        let configuration = configuration_of(cluster, &leader);
        !configuration.is_joint()
            && joiners
                .iter()
                .all(|joiner| is_voter_of(cluster, joiner, &configuration))
    });

    let steps = cluster.take_steps();
    assert!(committed, "the batch commits with the joiners");
    assert!(
        !ever_moved_to(&steps, &leader, WorkerState::NoQuorum),
        "the leader never loses its quorum"
    );
    assert_eq!(cluster.leader(), Some(leader.clone()));
    let configuration = configuration_of(&cluster, &leader);
    for id in &draining {
        assert!(
            !is_voter_of(&cluster, id, &configuration),
            "{id:?} drained and is out"
        );
    }
    let joiners: Vec<WorkerId> = joiners.into_iter().collect();
    assert!(
        quorum_of(
            &cluster,
            &configuration,
            &[&leader, &joiners[0], &joiners[1]]
        ) && !quorum_of(&cluster, &configuration, &[&leader, &joiners[0]]),
        "N counts the leader and the three joiners alone"
    );
    assert_no_grant_overlap(&cluster);
}

/// Several workers draining at once shrink N in one generation (every
/// pending SELF_REMOVE lands in the next generation), with no
/// commit round and no joint configuration, and the leader keeps leading
/// the smaller configuration. Every message is delivered twice, so each
/// `SelfRemove` also arrives as a duplicate and still removes its sender once.
#[test]
fn a_mass_self_remove_shrinks_n_in_one_generation_with_no_commit_round() {
    let (mut cluster, leader) = elected_with_joiners_away(7, 0);
    let followers: Vec<WorkerId> = ids(0..7).into_iter().filter(|id| *id != leader).collect();
    let before = configuration_of(&cluster, &leader);
    cluster.record_steps();
    cluster.network().set_duplicate_rate(1.0);

    for id in &followers[..3] {
        cluster.drain(id);
    }
    let shrunk = run_until(&mut cluster, |cluster| {
        let configuration = configuration_of(cluster, &leader);
        followers[3..]
            .iter()
            .all(|id| configuration_of(cluster, id) == configuration)
            && configuration.generation() > before.generation()
    });

    let steps = cluster.take_steps();
    assert!(
        shrunk,
        "every remaining follower holds the shrunk configuration"
    );
    assert!(
        joint_configurations_acked(&steps, &leader).is_empty(),
        "no commit round"
    );
    let configuration = configuration_of(&cluster, &leader);
    let term = cluster.node(&leader).term();
    assert_eq!(
        configuration.generation(),
        before.generation().next_change(term),
        "one generation for the three removals"
    );
    assert!(
        quorum_of(
            &cluster,
            &configuration,
            &[&leader, &followers[3], &followers[4]]
        ) && !quorum_of(&cluster, &configuration, &[&leader, &followers[3]]),
        "N counts the four left alone"
    );
    assert_eq!(cluster.leader(), Some(leader.clone()));
    assert!(!ever_moved_to(&steps, &leader, WorkerState::NoQuorum));
    assert_no_grant_overlap(&cluster);
}

/// A rolling deploy that replaces every old voter, the leader last: the
/// old voters' removals shrink the batch's old side to the leader alone,
/// and the leader's own drain empties it. The batch then collapses to its
/// new side, its joiners, which the leader announces on its final
/// acks; they elect among themselves and admit the joiners that
/// waited.
#[test]
fn a_rolling_deploy_that_replaces_the_leader_last_collapses_the_batch() {
    let (mut cluster, leader) = elected_with_joiners_away(3, 3);
    let joiners = ids(3..6);
    let draining: Vec<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();
    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader)
            .is_joint()),
        "a batch starts"
    );
    for id in &draining {
        cluster.drain(id);
    }
    cluster.deliver_messages();
    assert!(
        configuration_of(&cluster, &leader).is_joint(),
        "setup: the old voters are out and the batch is still in flight"
    );

    // The batch is joint, so the leader is never free to leave: it leads on
    // until its drain wait runs out.
    cluster.drain(&leader);
    assert!(
        run_until(&mut cluster, |cluster| cluster.node(&leader).state()
            == WorkerState::Stopped),
        "the leader leaves once its drain wait runs out"
    );
    cluster.deliver_messages();
    let collapsed = configuration_of(&cluster, joiners.first().unwrap());
    assert!(!collapsed.is_joint(), "the batch collapsed to its new side");
    for joiner in &joiners {
        assert_eq!(
            configuration_of(&cluster, joiner),
            collapsed,
            "{joiner:?} holds it from the leader's final ack"
        );
    }
    let batch_joiners: Vec<&WorkerId> = joiners
        .iter()
        .filter(|joiner| is_voter_of(&cluster, joiner, &collapsed))
        .collect();
    assert!(!batch_joiners.is_empty());
    assert!(
        quorum_of(&cluster, &collapsed, &batch_joiners),
        "N counts the batch's joiners alone, the old voters gone"
    );
    assert!(
        run_until(&mut cluster, |cluster| {
            let leaders = leaders_among(cluster, &joiners);
            leaders.len() == 1 && {
                let configuration = configuration_of(cluster, leaders.first().unwrap());
                !configuration.is_joint()
                    && joiners
                        .iter()
                        .all(|joiner| is_voter_of(cluster, joiner, &configuration))
            }
        }),
        "the joiners elect among themselves and admit those that waited"
    );
    assert_no_grant_overlap(&cluster);
}

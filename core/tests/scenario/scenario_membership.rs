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

/// How long past a suspicion timeout a leader waits before it reports a
/// worker lost, in scenarios about losses.
const RECONNECT: Duration = Duration::from_ticks(100);

/// A suspicion timeout and a reconnect timeout, in ticks: `SUSPECT` plus
/// `RECONNECT`.
const LOSS_TICKS: u64 = 10 + 100;

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
    elected_with_joiners_away_suspecting_after(voters, joiners, SUSPECT)
}

/// `elected_with_joiners_away` for nodes that suspect a silent leader only
/// after `suspect_timeout`.
fn elected_with_joiners_away_suspecting_after(
    voters: usize,
    joiners: usize,
    suspect_timeout: Duration,
) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap_with_pending(voters, joiners, suspect_timeout);
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

/// `elected_with_joiners_away` with a reconnect timeout of `RECONNECT`, so a
/// scenario about reporting workers lost need not simulate the default 30 s.
fn elected_with_joiners_away_reconnecting_quickly(
    voters: usize,
    joiners: usize,
) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap_with_reconnect_timeout(voters, joiners, SUSPECT, RECONNECT);
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

/// A joiner that holds its promise but then falls silent for a while, its
/// last confirmation of the leader's ack older than the batch's freshness
/// bound yet within a suspicion timeout, is waited for: the leader keeps the
/// round and starts the batch at the generation it promised once the joiner
/// confirms again, rather than discarding the round and promising a later
/// generation that every joiner must confirm afresh.
#[test]
fn a_joiner_that_goes_briefly_silent_after_its_promise_is_still_admitted_at_the_promised_generation() {
    let suspect_timeout = Duration::from_ticks(40);
    let (mut cluster, leader) = elected_with_joiners_away_suspecting_after(3, 1, suspect_timeout);
    let joiner = worker("worker-3");
    cluster.network().set_delay(TICK);
    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| cluster
            .node(&joiner)
            .admission()
            .is_some()),
        "setup: the joiner holds a promise"
    );
    let promised = cluster.node(&joiner).admission().expect("a promise");
    cluster.partition(
        BTreeSet::from([joiner.clone()]),
        ids(0..3),
    );
    cluster.network().drop_in_flight_across_partition();
    for _ in 0..26 {
        cluster.advance(TICK);
    }
    cluster.heal();

    let started = run_until(&mut cluster, |cluster| {
        configuration_of(cluster, &leader).is_joint()
    });

    assert!(started, "the batch starts");
    assert_eq!(
        configuration_of(&cluster, &leader).generation(),
        promised,
        "the batch is at the promised generation"
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
    // A delay keeps each batch joint for a few ticks, so a scenario stepping a
    // tick at a time sees the second one start; a longer one leaves the
    // joiners that wait too stale to join it.
    cluster.network().set_delay(Duration::from_ticks(1));
    cluster.heal();
    let mut batches: Vec<Generation> = Vec::new();
    // The joiners promised their admission by the time the first batch
    // starts: no second round of promises starts before it commits.
    let mut first = BTreeSet::new();
    for _ in 0..PATIENCE_TICKS {
        let configuration = configuration_of(&cluster, &leader);
        if configuration.is_joint() && !batches.contains(&configuration.generation()) {
            batches.push(configuration.generation());
            if first.is_empty() {
                first = joiners
                    .iter()
                    .filter(|joiner| cluster.node(joiner).admission().is_some())
                    .cloned()
                    .collect();
            }
            cluster.heal();
        }
        if batches.len() == 2 {
            // Acks of the second batch now take longer than a heartbeat
            // interval, which the old side's phases differ by at most, so
            // every member of the old side holds the batch before the first
            // echo of it reaches the leader.
            cluster.network().set_delay(Duration::from_ticks(6));
            break;
        }
        cluster.advance(TICK);
    }
    assert_eq!(batches.len(), 2, "two batches start");
    let second: BTreeSet<WorkerId> = joiners.difference(&first).cloned().collect();
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
    // The second batch starts after the last joiner of it confirms, on the
    // heartbeat that catches the last member up with the commit and so starts
    // the promise round, and the old side has yet to hear of it: cut once it
    // has, before the echoes that would commit the batch reach the leader.
    let second_batch = configuration_of(&cluster, &leader).generation();
    assert!(
        run_until(&mut cluster, |cluster| old_side
            .iter()
            .all(|id| configuration_of(cluster, id).generation() == second_batch)),
        "setup: the old side has adopted the second batch"
    );
    cluster.partition(old_side.clone(), batch_side.clone());
    cluster.network().drop_in_flight_across_partition();
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
    // The leader admits no one before every voter has echoed its
    // configuration, the worker about to be cut off included.
    for _ in 0..3 * SUSPECT.as_ticks() {
        cluster.advance(TICK);
    }

    let mut rest = ids(0..6);
    rest.remove(&stale);
    cluster.partition(BTreeSet::from([stale.clone()]), rest);
    let admitted = run_until(&mut cluster, |cluster| {
        let configuration = configuration_of(cluster, &leader);
        !configuration.is_joint() && is_voter_of(cluster, &worker("worker-5"), &configuration)
    });
    assert!(admitted, "a batch");
    cluster.drain(&followers[1]);
    cluster.drain(&followers[2]);
    let changed = run_until(&mut cluster, |cluster| {
        configuration_of(cluster, &leader).generation()
            >= before
                .next_change(before.term())
                .next_change(before.term())
                .next_change(before.term())
                .next_change(before.term())
    });
    assert!(changed, "two removals");
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

/// Five voters elect a leader while a sixth worker waits to join, cut off
/// from the election. Two followers lose contact with the leader once the
/// election has founded its configuration, before they hear that it
/// committed, and the leader admits the waiting worker without them: they
/// are then behind the rest, which refuse their calls as stale. With the
/// leader lost, the two that kept up and the new voter are not a majority of
/// the six, so a new leader needs the two laggards: the leader must not
/// start the batch before everyone the commit counted has caught up.
#[test]
fn a_leader_lost_after_admitting_without_two_laggards_is_replaced() {
    let mut cluster = Cluster::bootstrap_with_pending(5, 1, SUSPECT);
    cluster.network().set_delay(Duration::from_ticks(2));
    cluster.partition(ids(0..5), ids(5..6));
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some_and(
            |leader| configuration_of(cluster, &leader).is_joint()
        )),
        "the voters elect a leader that founds a joint configuration"
    );
    let leader = cluster.leader().expect("a leader");
    let laggards: BTreeSet<WorkerId> = ids(0..5)
        .into_iter()
        .filter(|id| *id != leader)
        .take(2)
        .collect();
    let keeping_up: BTreeSet<WorkerId> = ids(0..6)
        .into_iter()
        .filter(|id| !laggards.contains(id))
        .collect();
    cluster.partition(keeping_up, laggards);
    // The leader admits no one while two members it counts have yet to
    // echo the commit: the waiting worker stays pending and no batch starts.
    for _ in 0..3 * SUSPECT.as_ticks() {
        cluster.advance(TICK);
    }
    assert!(
        !configuration_of(&cluster, &leader).is_joint(),
        "no batch while the laggards are behind"
    );
    assert_eq!(cluster.node(&worker("worker-5")).admission(), None);

    let rest: BTreeSet<WorkerId> = ids(0..6).into_iter().filter(|id| *id != leader).collect();
    cluster.partition(BTreeSet::from([leader.clone()]), rest.clone());
    let replaced = run_until(&mut cluster, |cluster| {
        !leaders_among(cluster, &rest).is_empty()
    });

    assert!(replaced, "no new leader: {:?}", cluster.states());
    assert_no_grant_overlap(&cluster);
}

/// Three voters elect a leader and commit what it founded; a joiner waits
/// cut off. The link from the leader to one voter then fails one way: its
/// heartbeats still reach the leader, but it never hears an ack, so it
/// confirms none. The leader reports that voter lost after the loss timeout,
/// although it keeps hearing it, and removes it like a voter that left;
/// the other voter, which acks, stays counted. Admissions, which wait for
/// every counted member to hold the configuration, then go ahead.
#[test]
fn a_voter_that_hears_no_ack_is_removed_though_its_heartbeats_arrive_and_admissions_go_ahead() {
    let (mut cluster, leader) = elected_with_joiners_away_reconnecting_quickly(3, 1);
    let joiner = worker("worker-3");
    let voters: Vec<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();
    let (mute, other) = (voters[0].clone(), voters[1].clone());
    cluster.network().block_one_way(leader.clone(), mute.clone());

    // Past the suspicion and reconnect timeouts.
    for _ in 0..LOSS_TICKS + 100 {
        cluster.advance(TICK);
    }
    assert_eq!(
        configuration_of(&cluster, &leader).voter_count(),
        Some(2),
        "the voter that confirmed no ack is removed, the one that acked is not"
    );
    assert_eq!(cluster.leader(), Some(leader.clone()), "the leader keeps its office");
    assert!(
        is_voter_of(&cluster, &other, &configuration_of(&cluster, &leader)),
        "the acking voter stays counted"
    );

    // The link is still one-way while the waiting worker is admitted.
    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| {
            let configuration = configuration_of(cluster, &leader);
            !configuration.is_joint() && is_voter_of(cluster, &joiner, &configuration)
        }),
        "the waiting worker is admitted"
    );

    // Once its link heals the removed voter rejoins as a pending member and
    // is admitted again.
    cluster.network().unblock_all();
    assert!(
        run_until(&mut cluster, |cluster| {
            let configuration = configuration_of(cluster, &leader);
            !configuration.is_joint() && is_voter_of(cluster, &mute, &configuration)
        }),
        "the removed voter is admitted again once its link heals"
    );
    assert_no_grant_overlap(&cluster);
}

/// Five voters; the links from the leader to two of them, D and E, fail one
/// way, so both confirm no ack. The leader removes one, and a third voter, C,
/// is then cut off from the leader and the fourth, B, while D and E, still
/// holding the five-voter configuration, can reach C. A second removal that
/// took E out while C, D and E, a majority of the five, could still elect
/// under the old generation would leave two leaders beside each other. The
/// leader removes the second only while a majority of the five holds the
/// generation the removal moves from, so no two nodes ever hold a valid
/// grant at once.
#[test]
fn a_second_removal_waits_until_a_majority_of_the_voters_it_leaves_behind_holds_the_change() {
    let (mut cluster, leader) = elected_with_joiners_away_reconnecting_quickly(5, 0);
    cluster.network().set_delay(Duration::from_ticks(1));
    let followers: Vec<WorkerId> = ids(0..5).into_iter().filter(|id| *id != leader).collect();
    let (b, c, d, e) = (
        followers[0].clone(),
        followers[1].clone(),
        followers[2].clone(),
        followers[3].clone(),
    );
    cluster.network().block_one_way(leader.clone(), d.clone());
    cluster.network().block_one_way(leader.clone(), e.clone());
    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader)
            .voter_count()
            .is_some_and(|voters| voters <= 4)),
        "the first of the two is removed"
    );
    cluster.partition(
        BTreeSet::from([leader.clone(), b.clone()]),
        BTreeSet::from([c.clone()]),
    );
    cluster.network().drop_in_flight_across_partition();

    for _ in 0..2 * LOSS_TICKS {
        cluster.advance(TICK);
        assert_no_grant_overlap(&cluster);
    }
}

/// Three voters admit a joiner in a batch, and the link from the leader to
/// one of the three fails one way the moment the batch commits, before that
/// voter echoes the committed generation. It must still be removed, once it
/// has confirmed no ack for a loss timeout, or no later admission could ever
/// go ahead: a second waiting worker is then admitted, and no two nodes
/// ever hold a valid grant at once.
#[test]
fn a_voter_muted_as_a_batch_commits_is_removed_and_a_waiting_worker_is_admitted() {
    let mut cluster = Cluster::bootstrap_with_reconnect_timeout(3, 2, SUSPECT, RECONNECT);
    let (joiner, waiting) = (worker("worker-3"), worker("worker-4"));
    cluster.partition(ids(0..3), ids(3..5));
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some_and(
            |leader| !configuration_of(cluster, &leader).is_joint()
        )),
        "the voters elect a leader and commit its founding"
    );
    let leader = cluster.leader().expect("a leader");
    let muted = ids(0..3).into_iter().find(|id| *id != leader).expect("a follower");
    cluster.network().set_delay(TICK);
    // The first joiner is reachable, the second waits for later.
    cluster.partition(ids(0..4), BTreeSet::from([waiting.clone()]));
    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader).is_joint()),
        "a batch starts"
    );
    assert!(
        run_until(&mut cluster, |cluster| {
            let configuration = configuration_of(cluster, &leader);
            !configuration.is_joint() && is_voter_of(cluster, &joiner, &configuration)
        }),
        "the batch commits"
    );
    cluster.network().block_one_way(leader.clone(), muted.clone());
    cluster.network().drop_in_flight_across_partition();

    for _ in 0..4 * LOSS_TICKS {
        cluster.advance(TICK);
    }
    assert_eq!(
        configuration_of(&cluster, &leader).voter_count(),
        Some(3),
        "the muted voter is removed from the four"
    );
    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| {
            let configuration = configuration_of(cluster, &leader);
            !configuration.is_joint() && is_voter_of(cluster, &waiting, &configuration)
        }),
        "the waiting worker is admitted"
    );
    assert_no_grant_overlap(&cluster);
}

/// A voter whose link from a newly elected leader fails one way before it
/// echoes anything in that office is still removed after a loss timeout: its
/// answer to the roll call said which configuration it holds, which anchors
/// the removal.
#[test]
fn a_voter_muted_just_after_an_election_is_removed() {
    let mut cluster = Cluster::bootstrap_with_reconnect_timeout(3, 0, SUSPECT, RECONNECT);
    cluster.network().set_delay(TICK);
    assert!(
        run_until(&mut cluster, |cluster| cluster.leader().is_some()),
        "the voters elect a leader"
    );
    let leader = cluster.leader().expect("a leader");
    let muted = ids(0..3).into_iter().find(|id| *id != leader).expect("a follower");
    cluster.network().block_one_way(leader.clone(), muted.clone());
    cluster.network().drop_in_flight_across_partition();

    for _ in 0..4 * LOSS_TICKS {
        cluster.advance(TICK);
    }

    assert_eq!(configuration_of(&cluster, &leader).voter_count(), Some(2));
    assert_eq!(cluster.leader(), Some(leader));
    assert_no_grant_overlap(&cluster);
}

/// A voter that dies outright is reported lost for task replay but stays in
/// the configuration: only one whose heartbeats still arrive is removed.
#[test]
fn a_voter_that_goes_silent_stays_counted() {
    let (mut cluster, leader) = elected_with_joiners_away_reconnecting_quickly(3, 0);
    let dead = ids(0..3).into_iter().find(|id| *id != leader).expect("a follower");
    cluster.stall(&dead, Duration::from_secs(1_000_000));
    cluster.record_steps();

    for _ in 0..LOSS_TICKS + 100 {
        cluster.advance(TICK);
    }

    let steps = cluster.take_steps();
    assert!(
        steps
            .iter()
            .any(|step| step.node == leader && step.outputs.contains(&Output::WorkerLost(dead.clone()))),
        "the dead voter is reported lost, for task replay"
    );
    assert_eq!(configuration_of(&cluster, &leader).voter_count(), Some(3));
    assert_eq!(cluster.leader(), Some(leader));
}

/// Three voters elect a leader and commit what it founded, and every voter
/// echoes the commit. A joiner then arrives and the leader starts a batch,
/// but one voter, cut off from the leader at that moment, never hears the
/// batch start: it still holds the commit, which the other voter and the
/// joiner, now holding the batch, refuse as stale, while it refuses their
/// calls as not the best. The leader is then lost, and a new leader needs the
/// cut-off voter on the old side and counts it on the new side only once it
/// has taken up the batch from a refusal.
#[test]
fn a_leader_lost_after_starting_a_batch_a_voter_missed_is_replaced() {
    let (mut cluster, leader) = elected_with_joiners_away(3, 1);
    let joiner = worker("worker-3");
    let voters: Vec<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();
    let (missed, other) = (voters[0].clone(), voters[1].clone());
    cluster.network().set_delay(Duration::from_ticks(1));
    // The leader admits no one before every voter has echoed the commit.
    for _ in 0..3 * SUSPECT.as_ticks() {
        cluster.advance(TICK);
    }
    let committed = configuration_of(&cluster, &missed).generation();

    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader)
            .is_joint()),
        "a batch starts"
    );
    cluster.partition(BTreeSet::from([missed.clone()]), BTreeSet::from([leader.clone()]));
    cluster.network().drop_in_flight_across_partition();
    assert!(
        run_until(&mut cluster, |cluster| {
            [&other, &joiner]
                .iter()
                .all(|id| configuration_of(cluster, id).is_joint())
        }),
        "setup: the other voter and the joiner hold the batch"
    );
    assert_eq!(
        configuration_of(&cluster, &missed).generation(),
        committed,
        "setup: the cut-off voter missed the batch start"
    );

    let rest: BTreeSet<WorkerId> = ids(0..4).into_iter().filter(|id| *id != leader).collect();
    cluster.partition(BTreeSet::from([leader.clone()]), rest.clone());
    cluster.network().drop_in_flight_across_partition();
    let replaced = run_until(&mut cluster, |cluster| {
        !leaders_among(cluster, &rest).is_empty()
    });

    assert!(replaced, "no new leader: {:?}", cluster.states());
    assert_no_grant_overlap(&cluster);
}

/// `voters` voters elect a leader and commit what they founded, with `joiners`
/// workers waiting to join, out of reach. `draining` of the voters drain,
/// which moves the leader's configuration on, and the leader admits no one
/// before every voter left has echoed it: the voter left in the leader's
/// charge stalls while the joiners come within reach, so once it catches up
/// the leader takes every joiner in one batch. The instant its configuration
/// turns joint the joiners are cut off from the voters, every message on its
/// way to them lost: no joiner is told of the batch by an ack. Then the
/// leader's lease, which needs a majority of the batch's new side, runs out.
/// Returns the cluster, once the joiners are back in reach, and the old
/// leader.
fn batch_the_joiners_never_hear_of_with_its_leader_lapsed(
    voters: usize,
    draining: usize,
    joiners: usize,
) -> (Cluster, WorkerId) {
    let (mut cluster, leader) =
        elected_with_joiners_away_suspecting_after(voters, joiners, Duration::from_ticks(40));
    cluster.network().set_delay(TICK);
    let followers: Vec<WorkerId> = ids(0..voters).into_iter().filter(|id| *id != leader).collect();
    let (leaving, staying) = followers.split_at(draining);
    for voter in staying {
        cluster.stall(voter, Duration::from_ticks(15));
    }
    for voter in leaving {
        cluster.drain(voter);
    }
    for _ in 0..3 {
        cluster.advance(TICK);
    }
    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader)
            .is_joint()),
        "a batch starts"
    );
    let staying: BTreeSet<WorkerId> = staying.iter().cloned().chain([leader.clone()]).collect();
    cluster.partition(staying.clone(), ids(voters..voters + joiners));
    cluster.network().drop_in_flight_across_partition();
    assert!(
        run_until(&mut cluster, |cluster| staying
            .iter()
            .all(|id| configuration_of(cluster, id).is_joint())),
        "setup: every voter left holds the batch"
    );
    assert!(
        run_until(&mut cluster, |cluster| cluster.states()[&leader]
            != WorkerState::Leader),
        "setup: the leader's lease runs out for want of the joiners"
    );
    cluster.heal();
    (cluster, leader)
}

/// Two voters elect a leader, which starts a batch of two joiners. No joiner
/// hears of it from an ack, and the leader loses its lease: its old side is
/// both voters and its new side needs a majority of four, which the two
/// voters are not. Every joiner must have learned its admission before the
/// batch began, or the survivors can never count it.
#[test]
fn a_leader_lost_after_starting_a_batch_no_joiner_heard_of_is_replaced() {
    let (mut cluster, _leader) = batch_the_joiners_never_hear_of_with_its_leader_lapsed(3, 1, 2);

    let replaced = run_until(&mut cluster, |cluster| cluster.leader().is_some());

    assert!(replaced, "no new leader: {:?}", cluster.states());
    assert_no_grant_overlap(&cluster);
}

/// The same for a shard's first batch: its one voter leads, and its new side
/// is itself and the joiner, so its lease needs the joiner too.
#[test]
fn a_sole_voter_lost_after_starting_the_first_batch_its_joiner_never_heard_of_is_replaced() {
    let (mut cluster, _leader) = batch_the_joiners_never_hear_of_with_its_leader_lapsed(1, 0, 1);

    let replaced = run_until(&mut cluster, |cluster| cluster.leader().is_some());

    assert!(replaced, "no new leader: {:?}", cluster.states());
    assert_no_grant_overlap(&cluster);
}

/// Three voters elect a leader and two joiners, X and Y, arrive. X is told
/// it will be admitted, then cut off before the leader hears it agree, and
/// the leader admits Y alone in a batch, which a split leaves uncommitted:
/// the leader and Y on one side, the two other voters and X on the other.
/// X's promise names no generation of that batch, so it is no voter of it,
/// and the batch's new side (the three voters and Y, needing three of four)
/// is out of the other side's reach: nobody there wins. Healed, the shard
/// elects and admits X after all.
#[test]
fn a_joiner_told_of_an_admission_the_leader_never_heard_it_take_is_no_voter_of_a_later_batch() {
    let (mut cluster, leader) = elected_with_joiners_away(3, 2);
    let (x, y) = (worker("worker-3"), worker("worker-4"));
    let others: BTreeSet<WorkerId> = ids(0..3).into_iter().filter(|id| *id != leader).collect();
    cluster.network().set_delay(TICK);
    cluster.heal();
    assert!(
        run_until(&mut cluster, |cluster| cluster
            .node(&x)
            .admission()
            .is_some()),
        "setup: X learns of its admission"
    );
    assert!(
        !configuration_of(&cluster, &leader).is_joint(),
        "setup: before any batch exists"
    );
    cluster.partition(
        BTreeSet::from([x.clone()]),
        ids(0..5).into_iter().filter(|id| *id != x).collect(),
    );
    cluster.network().drop_in_flight_across_partition();

    assert!(
        run_until(&mut cluster, |cluster| configuration_of(cluster, &leader)
            .is_joint()),
        "a batch of Y alone starts"
    );
    let batch = configuration_of(&cluster, &leader);
    assert!(
        run_until(&mut cluster, |cluster| others
            .iter()
            .all(|id| configuration_of(cluster, id) == batch)),
        "setup: the other voters hold the batch"
    );
    assert!(cluster.node(&x).admission().is_some());
    assert!(
        !is_voter_of(&cluster, &x, &batch),
        "X's admission names no generation of the batch"
    );
    let side_with_x: BTreeSet<WorkerId> = others.iter().cloned().chain([x.clone()]).collect();
    cluster.partition(
        side_with_x.clone(),
        BTreeSet::from([leader.clone(), y.clone()]),
    );
    cluster.network().drop_in_flight_across_partition();
    cluster.record_steps();
    for _ in 0..PATIENCE_TICKS {
        cluster.advance(TICK);
    }
    let steps = cluster.take_steps();
    for id in &side_with_x {
        assert!(
            !ever_moved_to(&steps, id, WorkerState::Leader),
            "{id:?} won counting X as a voter of a batch that left it out"
        );
    }

    cluster.heal();
    let everyone = ids(0..5);
    let admitted_after_all = run_until(&mut cluster, |cluster| {
        let leaders = leaders_among(cluster, &everyone);
        leaders.len() == 1 && {
            let configuration = configuration_of(cluster, leaders.first().unwrap());
            !configuration.is_joint()
                && everyone
                    .iter()
                    .all(|id| is_voter_of(cluster, id, &configuration))
        }
    });
    assert!(admitted_after_all, "{:?}", cluster.states());
    assert_no_grant_overlap(&cluster);
}

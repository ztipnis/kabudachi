//! A follower's half of leader liveness: the routing crawl its heartbeats
//! report, and the acks it ignores.

use crate::support::builders::{
    ack_message, configuration_of, epoch, g0, heartbeat, heartbeat_message, leader_ack,
    past_any_suspicion, roll_call_reply, self_remove, self_remove_message, shard, timings,
    vote_grant, vote_grant_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, close_roll_call, deliver, elect, finish_reconciling, leader_of_three,
    published_roll_calls, sent_to, tick, voter_node,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, Input, Output, WorkerNode};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{IncarnationId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    JoinResponse, LeaderHeartbeatAck, WorkerHeartbeat, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};
use std::collections::BTreeSet;

const SUSPECT_TIMEOUT: u64 = 10;

/// A node that joined through a JOIN naming `leader-1` in term 1, as a
/// pending member.
fn joined_node(clock: &FakeClock) -> TestNode {
    let node = WorkerNode::start(
        Identity {
            id: worker("joiner"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
        },
        Entry::Joining(JoinResponse {
            leader_id: Some(worker("leader-1").into()),
            leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
            shard_id: Some(shard("shard-1").into()),
        }),
        clock.clone(),
        None,
    )
    .0;
    assert!(node.is_pending_member(), "setup invariant");
    node
}

/// An ack from `leader-1` in term 1, sent at `send_token`, carrying a
/// configuration of 3 at `generation` that admits the recipient at that same
/// generation.
fn ack_admitting(generation: Generation, send_token: u64) -> LeaderHeartbeatAck {
    let configuration = Configuration::single(Single {
        generation,
        base: generation,
        voter_count: 3,
    })
    .expect("valid");
    LeaderHeartbeatAck {
        send_token,
        ..leader_ack(&worker("leader-1"), 1, &configuration, Some(generation))
    }
}

/// The heartbeat `node` sends its leader one interval from now.
fn next_heartbeat(node: &mut TestNode, clock: &FakeClock) -> WorkerHeartbeat {
    clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT)).heartbeat_interval);
    let outputs = tick(node);
    let mut heartbeats: Vec<_> = sent_to(&outputs, &worker("leader-1"))
        .into_iter()
        .map(|message| match message.payload {
            Some(election_message::Payload::Heartbeat(heartbeat)) => heartbeat,
            other => panic!("expected only heartbeats to the leader, got {other:?}"),
        })
        .collect();
    assert_eq!(heartbeats.len(), 1, "{outputs:?}");
    heartbeats.remove(0)
}

fn receive_ack(node: &mut TestNode, ack: LeaderHeartbeatAck) {
    let leader = worker("leader-1");
    deliver(node, &leader, ack_message(ack));
}

// A voter's heartbeat says it has crawled only for a crawl completed while
// it held its current admission: one from before it was admitted, or from
// before a re-admission, does not count.
#[test]
fn a_heartbeat_reports_a_routing_crawl_only_since_the_current_admission() {
    let clock = FakeClock::new();
    let mut node = joined_node(&clock);

    let _ = node.step(Input::RoutingCrawled);
    assert!(
        !next_heartbeat(&mut node, &clock).routing_crawled,
        "a pending member has nothing to report"
    );

    receive_ack(&mut node, ack_admitting(Generation::new(epoch(0), 1, 2), 1));
    assert!(
        !next_heartbeat(&mut node, &clock).routing_crawled,
        "the crawl predates the admission"
    );

    let _ = node.step(Input::RoutingCrawled);
    assert!(next_heartbeat(&mut node, &clock).routing_crawled);

    receive_ack(&mut node, ack_admitting(Generation::new(epoch(0), 1, 3), 2));
    assert!(
        !next_heartbeat(&mut node, &clock).routing_crawled,
        "re-admitted: crawl again"
    );
}

// Verified through a `Tick`: had the ack been accepted, leader contact would
// have moved and the node would not suspect its leader.
#[test]
fn an_ack_for_another_shard_or_from_other_than_the_leader_it_names_is_ignored() {
    let ack = || leader_ack(&worker("leader-1"), 1, &configuration_of(3), Some(g0()));
    let mut other_shard = ack();
    other_shard.shard_id = Some(shard("shard-2").into());
    let rows = [
        (worker("leader-1"), other_shard),
        (worker("impostor"), ack()),
    ];

    for (sender, ack) in rows {
        let clock = FakeClock::new();
        let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT_TIMEOUT);

        clock.advance(Duration::from_ticks(6));
        deliver(&mut node, &sender, ack_message(ack));
        assert_eq!(node.known_leader(), None, "{sender:?}");

        clock.advance(Duration::from_ticks(9));
        tick(&mut node);
        assert_eq!(node.state(), WorkerState::LeaderSuspect, "{sender:?}");
    }
}

// A worker may start a TaskRun only once it can name the instant by which it
// would have to abort it: once a leader holding a grant has vouched for
// hearing one of its heartbeats, or while it leads alone, with a grant no
// rival can outlast and so no leader that could replay its runs.
#[test]
fn a_worker_has_a_contact_floor_once_a_leader_vouches_for_hearing_it() {
    let clock = FakeClock::new();
    let mut node = joined_node(&clock);
    assert!(!node.has_contact_floor(), "no leader has heard the joiner yet");

    receive_ack(&mut node, ack_admitting(g0(), 1));
    assert!(
        !node.has_contact_floor(),
        "an ack that echoes no heartbeat vouches for nothing"
    );

    receive_ack(
        &mut node,
        LeaderHeartbeatAck {
            heartbeat_token: Some(clock.now().as_ticks()),
            ..ack_admitting(g0(), 2)
        },
    );
    assert!(node.has_contact_floor());

    let mut alone = voter_node(&clock, &worker("alone"), 1, SUSPECT_TIMEOUT);
    for _ in 0..1_000 {
        if matches!(alone.state(), WorkerState::LeaderReconciling | WorkerState::Leader) {
            break;
        }
        clock.advance(Duration::from_ticks(1));
        let _ = tick(&mut alone);
    }
    assert!(matches!(alone.state(), WorkerState::LeaderReconciling | WorkerState::Leader));
    assert!(alone.has_contact_floor(), "no other leader can replay a lone leader's runs");
}

// A leader echoes the instant of the heartbeat it heard. An instant later than
// the worker's clock reads was never sent by it, so such an ack vouches for
// nothing.
#[test]
fn an_ack_echoing_a_heartbeat_from_the_future_gives_a_worker_no_contact_floor() {
    let clock = FakeClock::new();
    let mut node = joined_node(&clock);

    receive_ack(
        &mut node,
        LeaderHeartbeatAck {
            heartbeat_token: Some(clock.now().as_ticks() + 1),
            ..ack_admitting(g0(), 1)
        },
    );

    assert!(!node.has_contact_floor());
}

fn run_ids(count: usize) -> Vec<TaskRunId> {
    (0..count)
        .map(|n| TaskRunId::new(format!("run-{n:03}")))
        .collect()
}

/// The ack `leader` sends `follower` in answer to a heartbeat from it.
fn ack_to(leader: &mut TestNode, follower: &WorkerId) -> LeaderHeartbeatAck {
    let outputs = deliver(leader, follower, heartbeat_message(heartbeat(follower, None)));
    let mut acks = sent_to(&outputs, follower)
        .into_iter()
        .filter_map(|message| match message.payload {
            Some(election_message::Payload::HeartbeatAck(ack)) => Some(ack),
            _ => None,
        });
    let ack = acks.next().unwrap_or_else(|| panic!("no ack in {outputs:?}"));
    assert!(acks.next().is_none(), "one ack per heartbeat");
    ack
}

fn cancelled_in(ack: &LeaderHeartbeatAck) -> Vec<TaskRunId> {
    ack.cancelled_runs.iter().cloned().map(TaskRunId::from).collect()
}

// A leader tells a worker which of its runs it cancelled in its acks, at most
// 64 at a time, rotating through them so none is starved by the others, until
// told to stop, and holds none for a node that does not lead. Past the end of
// a worker's runs the rotation wraps to the start, so every ack lists a full
// batch whenever the worker has that many runs: with 150, the third lists the
// last 22 and then the first 42.
#[test]
fn a_leader_lists_the_runs_it_cancelled_in_its_acks_in_rotation_until_told_to_stop() {
    const SUSPECT: u64 = 10;
    let clock = FakeClock::new();
    let (p1, p2) = (worker("p1"), worker("p2"));
    let mut leader = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    elect(&mut leader, &clock, SUSPECT, &[p1.clone(), p2.clone()]);
    let runs = run_ids(150);
    for run in &runs {
        leader.tell_cancelled(p1.clone(), run.clone());
    }

    let mut follower = voter_node(&clock, &p1, 3, SUSPECT);
    let mut told = BTreeSet::new();
    for _ in 0..3 {
        let ack = ack_to(&mut leader, &p1);
        let listed = cancelled_in(&ack);
        assert_eq!(listed.len(), 64, "a full batch in every ack");
        let raised: Vec<_> = deliver(&mut follower, &worker("w1"), ack_message(ack))
            .into_iter()
            .filter_map(|output| match output {
                Output::RunsCancelled(runs) => Some(runs),
                _ => None,
            })
            .collect();
        assert_eq!(raised, vec![listed.clone()], "the follower raises what the ack listed");
        told.extend(listed);
    }
    assert_eq!(told, runs.iter().cloned().collect(), "three acks reach all 150 runs");
    assert!(
        cancelled_in(&ack_to(&mut leader, &p2)).is_empty(),
        "another worker is told nothing"
    );

    leader.forget_cancelled(&p1);
    assert!(cancelled_in(&ack_to(&mut leader, &p1)).is_empty());

    // A cancel handed to a node that holds no office is dropped, not told by
    // the office it wins later.
    let mut fresh = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    fresh.tell_cancelled(p1.clone(), runs[0].clone());
    elect(&mut fresh, &clock, SUSPECT, &[p1.clone(), p2]);
    assert!(cancelled_in(&ack_to(&mut fresh, &p1)).is_empty());
}

// A worker's heartbeats say what it runs and which runs it holds from the
// moment it is told: that it runs compaction, so its leader can hand it
// compaction runs, and the digest of its runs, so its leader can see which it
// still holds.
#[test]
fn a_heartbeat_reports_what_its_sender_runs_and_holds_from_the_moment_it_is_told() {
    let clock = FakeClock::new();
    let mut node = joined_node(&clock);
    let held = Digest::blake3(b"the runs this worker holds");

    let before = next_heartbeat(&mut node, &clock);
    assert!(!before.runs_compaction, "off unless the worker says it runs compaction");
    assert!(before.active_task_runs_digest.is_empty());

    node.set_runs_compaction(true);
    node.set_active_runs_digest(held.clone());
    let after = next_heartbeat(&mut node, &clock);
    assert!(after.runs_compaction);
    assert_eq!(after.active_task_runs_digest, held.value());
}

// A worker that leaves the roster is told nothing more of its runs.
#[test]
fn a_leader_stops_listing_cancelled_runs_to_a_worker_that_left() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock, SUSPECT_TIMEOUT);
    let p1 = worker("p1");
    let runs = run_ids(1);
    leader.tell_cancelled(p1.clone(), runs[0].clone());
    assert_eq!(
        cancelled_in(&ack_to(&mut leader, &p1)),
        runs,
        "setup invariant"
    );

    deliver(
        &mut leader,
        &p1,
        self_remove_message(self_remove(&p1, "shard-1")),
    );

    assert!(cancelled_in(&ack_to(&mut leader, &p1)).is_empty());
}

// A leader that loses its office and wins it back tells only the runs it
// cancelled in the new one.
#[test]
fn a_leader_that_wins_office_back_lists_none_of_the_runs_its_old_office_was_told() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock, SUSPECT_TIMEOUT);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let runs = run_ids(1);
    leader.tell_cancelled(p1.clone(), runs[0].clone());
    assert_eq!(
        cancelled_in(&ack_to(&mut leader, &p1)),
        runs,
        "setup invariant"
    );

    // A later term's heartbeat deposes it.
    deliver(
        &mut leader,
        &p1,
        heartbeat_message(WorkerHeartbeat {
            term_seen: 2,
            ..heartbeat(&p1, None)
        }),
    );
    assert_eq!(
        leader.state(),
        WorkerState::LeaderSuspect,
        "setup invariant"
    );

    let call = (0..3)
        .find_map(|_| {
            clock.advance(past_any_suspicion(SUSPECT_TIMEOUT));
            published_roll_calls(&tick(&mut leader)).into_iter().next()
        })
        .expect("a roll call");
    let me = call.initiator_id();
    let admission = leader.admission();
    for peer in [&p1, &p2] {
        deliver(
            &mut leader,
            peer,
            roll_call_reply(&me, call.term, peer, admission),
        );
    }
    close_roll_call(&mut leader, &clock, SUSPECT_TIMEOUT);
    for peer in [&p1, &p2] {
        if leader.state() == WorkerState::Candidate {
            deliver(
                &mut leader,
                peer,
                vote_grant_message(vote_grant(me.clone(), peer.clone(), call.term)),
            );
        }
    }
    finish_reconciling(&mut leader);
    assert_eq!(leader.state(), WorkerState::Leader, "setup invariant");

    assert!(cancelled_in(&ack_to(&mut leader, &p1)).is_empty());
}

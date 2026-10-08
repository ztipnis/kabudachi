//! A follower's half of leader liveness: the routing crawl its heartbeats
//! report, and the acks it ignores.

use crate::support::builders::{
    ack_message, configuration_of, epoch, g0, heartbeat, heartbeat_message, leader_ack, shard,
    timings, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, deliver, elect, sent_to, tick, voter_node};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, Input, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{JoinResponse, LeaderHeartbeatAck, election_message};
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

/// Whether the heartbeat `node` sends its leader one interval from now says
/// it has crawled its routing.
fn next_heartbeat_reports_a_crawl(node: &mut TestNode, clock: &FakeClock) -> bool {
    clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT)).heartbeat_interval);
    let outputs = tick(node);
    let heartbeats: Vec<_> = sent_to(&outputs, &worker("leader-1"))
        .into_iter()
        .map(|message| match message.payload {
            Some(election_message::Payload::Heartbeat(heartbeat)) => heartbeat,
            other => panic!("expected only heartbeats to the leader, got {other:?}"),
        })
        .collect();
    assert_eq!(heartbeats.len(), 1, "{outputs:?}");
    heartbeats[0].routing_crawled
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
        !next_heartbeat_reports_a_crawl(&mut node, &clock),
        "a pending member has nothing to report"
    );

    receive_ack(&mut node, ack_admitting(Generation::new(epoch(0), 1, 2), 1));
    assert!(
        !next_heartbeat_reports_a_crawl(&mut node, &clock),
        "the crawl predates the admission"
    );

    let _ = node.step(Input::RoutingCrawled);
    assert!(next_heartbeat_reports_a_crawl(&mut node, &clock));

    receive_ack(&mut node, ack_admitting(Generation::new(epoch(0), 1, 3), 2));
    assert!(
        !next_heartbeat_reports_a_crawl(&mut node, &clock),
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

// A worker that runs compaction says so in every heartbeat it sends, from the
// moment it is told to, so its leader can hand it compaction runs.
#[test]
fn a_heartbeat_says_whether_its_sender_runs_compaction() {
    let clock = FakeClock::new();
    let mut node = joined_node(&clock);
    let says = |node: &mut TestNode| {
        clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT)).heartbeat_interval);
        let outputs = tick(node);
        match sent_to(&outputs, &worker("leader-1")).remove(0).payload {
            Some(election_message::Payload::Heartbeat(heartbeat)) => heartbeat.runs_compaction,
            other => panic!("expected a heartbeat to the leader, got {other:?}"),
        }
    };

    assert!(!says(&mut node), "off unless the worker says it runs compaction");
    node.set_runs_compaction(true);
    assert!(says(&mut node));
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
// told to stop, and holds none for a node that does not lead.
#[test]
fn a_leader_lists_the_runs_it_cancelled_in_its_acks_in_rotation_until_told_to_stop() {
    const SUSPECT: u64 = 10;
    let clock = FakeClock::new();
    let (p1, p2) = (worker("p1"), worker("p2"));
    let mut leader = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    elect(&mut leader, &clock, SUSPECT, &[p1.clone(), p2.clone()]);
    let runs: Vec<TaskRunId> = (0..150).map(|n| TaskRunId::new(format!("run-{n:03}"))).collect();
    for run in &runs {
        leader.tell_cancelled(p1.clone(), run.clone());
    }

    let mut follower = voter_node(&clock, &p1, 3, SUSPECT);
    let mut told = BTreeSet::new();
    for _ in 0..3 {
        let ack = ack_to(&mut leader, &p1);
        let listed = cancelled_in(&ack);
        assert!(listed.len() <= 64, "{} runs in one ack", listed.len());
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

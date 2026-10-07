//! A follower's half of leader liveness: the routing crawl its heartbeats
//! report, and the acks it ignores.

use crate::support::builders::{
    ack_message, configuration_of, g0, leader_ack, shard, timings, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, deliver, sent_to, tick, voter_node};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, Input, WorkerNode};
use kabudachi_core::protocol::ids::IncarnationId;
use kabudachi_core::protocol::messages::{JoinResponse, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

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

    receive_ack(&mut node, ack_admitting(Generation::new(0, 1, 2), 1));
    assert!(
        !next_heartbeat_reports_a_crawl(&mut node, &clock),
        "the crawl predates the admission"
    );

    let _ = node.step(Input::RoutingCrawled);
    assert!(next_heartbeat_reports_a_crawl(&mut node, &clock));

    receive_ack(&mut node, ack_admitting(Generation::new(0, 1, 3), 2));
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

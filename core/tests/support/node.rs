//! Helpers for driving `WorkerNode`s by hand and reading back what they
//! asked their driver to do.

use std::collections::{BTreeMap, VecDeque};

use kabudachi_core::election::{Entry, Identity, Input, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionMessage, ElectionReject, RollCall, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::LeadershipGrant;
use kabudachi_core::time::{Clock, Duration};

use crate::support::builders::{
    g0, heartbeat, heartbeat_message, past_any_suspicion, roll_call_reply, shard, timings,
    vote_grant, vote_grant_message, voter_of,
};
use crate::support::clock::FakeClock;

/// A node on the tests' fake clock.
pub type TestNode = WorkerNode<FakeClock>;

/// `my_id`'s node in `shard-1`: an `Active` voter, admitted at `g0`, of a
/// configuration of `voter_count` voters, suspecting its leader after
/// `suspect_timeout` ticks, with no authority.
pub fn voter_node(
    clock: &FakeClock,
    my_id: &WorkerId,
    voter_count: usize,
    suspect_timeout: u64,
) -> TestNode {
    WorkerNode::start(
        Identity {
            id: my_id.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(suspect_timeout)),
        },
        Entry::Known(voter_of(voter_count)),
        clock.clone(),
        None,
    )
    .0
}

/// The messages among `outputs`, each with its recipient, in the order the
/// node sent them.
pub fn sent(outputs: &[Output]) -> Vec<(WorkerId, ElectionMessage)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send { to, message } => Some((to.clone(), message.clone())),
            _ => None,
        })
        .collect()
}

/// The messages among `outputs` addressed to `recipient`, in order.
pub fn sent_to(outputs: &[Output], recipient: &WorkerId) -> Vec<ElectionMessage> {
    sent(outputs)
        .into_iter()
        .filter(|(to, _)| to == recipient)
        .map(|(_, message)| message)
        .collect()
}

/// The messages among `outputs` the node published, in order.
pub fn published(outputs: &[Output]) -> Vec<ElectionMessage> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Publish { message } => Some(message.clone()),
            _ => None,
        })
        .collect()
}

/// The roll calls among `outputs` the node published, in order.
pub fn published_roll_calls(outputs: &[Output]) -> Vec<RollCall> {
    published(outputs)
        .into_iter()
        .filter_map(|message| match message.payload {
            Some(election_message::Payload::RollCall(call)) => Some(call),
            _ => None,
        })
        .collect()
}

/// The refusals among `outputs` addressed to `recipient`, in order.
pub fn rejects_sent_to(outputs: &[Output], recipient: &WorkerId) -> Vec<ElectionReject> {
    sent_to(outputs, recipient)
        .into_iter()
        .filter_map(|message| match message.payload {
            Some(election_message::Payload::ElectionReject(reject)) => Some(reject),
            _ => None,
        })
        .collect()
}

/// The workers among `outputs` sent a message `is_kind` picks, in order.
pub fn recipients_of(
    outputs: &[Output],
    is_kind: impl Fn(&election_message::Payload) -> bool,
) -> Vec<WorkerId> {
    sent(outputs)
        .into_iter()
        .filter(|(_, message)| message.payload.as_ref().is_some_and(&is_kind))
        .map(|(to, _)| to)
        .collect()
}

/// The states `outputs` report the node moving into, in order.
pub fn state_changes(outputs: &[Output]) -> Vec<WorkerState> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::StateChanged(state) => Some(*state),
            _ => None,
        })
        .collect()
}

/// The leadership grants `outputs` report, `None` for a withdrawn one, in
/// order.
pub fn grants(outputs: &[Output]) -> Vec<Option<LeadershipGrant>> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Grant(grant) => Some(*grant),
            _ => None,
        })
        .collect()
}

/// Reports each of `peers` to `node` as connected, the way a driver reports
/// the connections it already holds.
pub fn connect<C>(node: &mut WorkerNode<C>, peers: &[WorkerId])
where
    C: Clock,
{
    for peer in peers {
        // Reporting a connection sends nothing and moves no timer.
        let _ = node.step(Input::PeerConnected(peer.clone()));
    }
}

/// Hands `node` `message` from `from` and returns what it asked for.
pub fn deliver<C>(
    node: &mut WorkerNode<C>,
    from: &WorkerId,
    message: ElectionMessage,
) -> Vec<Output>
where
    C: Clock,
{
    node.step(Input::Message {
        from: from.clone(),
        message,
    })
    .outputs
}

/// Feeds `node` a `Tick` and returns what it asked for.
pub fn tick<C>(node: &mut WorkerNode<C>) -> Vec<Output>
where
    C: Clock,
{
    node.step(Input::Tick).outputs
}

/// Lets `clock` run past `node`'s suspicion timeout of `suspect_timeout`
/// ticks, whatever its jitter, then ticks it from `Active` into
/// `LeaderSuspect` and on into `RollCall`, and returns what the second tick
/// asked for: the roll call it published.
pub fn start_roll_call(
    node: &mut TestNode,
    clock: &FakeClock,
    suspect_timeout: u64,
) -> Vec<Output> {
    clock.advance(past_any_suspicion(suspect_timeout));
    tick(node);
    assert_eq!(node.state(), WorkerState::LeaderSuspect, "start_roll_call");
    tick(node)
}

/// Lets `clock` run to the deadline of the roll call `node`, built with
/// `timings(suspect_timeout ticks)`, has just started, and ticks it there;
/// returns what that tick asked for.
pub fn close_roll_call(
    node: &mut TestNode,
    clock: &FakeClock,
    suspect_timeout: u64,
) -> Vec<Output> {
    clock.advance(timings(Duration::from_ticks(suspect_timeout)).roll_call_deadline);
    tick(node)
}

/// Stands `node`, an `Active` voter, as the candidate of a real roll call
/// for the term after the latest it knows of: starts its roll call (see
/// [`start_roll_call`]), hands it a reply from each of `peers`, admitted at
/// `g0`, and closes the call at its deadline (see [`close_roll_call`]),
/// which must make it the candidate. Returns its roll call.
pub fn stand_as_candidate(
    node: &mut TestNode,
    clock: &FakeClock,
    suspect_timeout: u64,
    peers: &[WorkerId],
) -> RollCall {
    let started = start_roll_call(node, clock, suspect_timeout);
    let call = published_roll_calls(&started).remove(0);
    let me = call.initiator_id();
    for peer in peers {
        deliver(
            node,
            peer,
            roll_call_reply(&me, call.term, peer, Some(g0())),
        );
    }
    close_roll_call(node, clock, suspect_timeout);
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "stand_as_candidate: the node must stand"
    );
    call
}

/// Elects `node`, an `Active` voter, over a real roll call for the term
/// after the latest it knows of: stands it as the candidate (see
/// [`stand_as_candidate`]), so each of `peers` is in the roster it wins,
/// then hands it a grant from each of them until it wins. With no `peers`
/// its own roll call elects it at its deadline. Returns what the step that
/// won asked for.
pub fn elect(
    node: &mut TestNode,
    clock: &FakeClock,
    suspect_timeout: u64,
    peers: &[WorkerId],
) -> Vec<Output> {
    if peers.is_empty() {
        start_roll_call(node, clock, suspect_timeout);
        let won = close_roll_call(node, clock, suspect_timeout);
        assert_eq!(
            node.state(),
            WorkerState::Leader,
            "elect: the node must win"
        );
        return won;
    }
    let call = stand_as_candidate(node, clock, suspect_timeout, peers);
    let me = call.initiator_id();
    let mut won = Vec::new();
    for peer in peers {
        if node.state() == WorkerState::Candidate {
            won = deliver(
                node,
                peer,
                vote_grant_message(vote_grant(me.clone(), peer.clone(), call.term)),
            );
        }
    }
    assert_eq!(
        node.state(),
        WorkerState::Leader,
        "elect: the node must win"
    );
    won
}

/// Commits the joint configuration `leader` founded when it won: hands it a
/// heartbeat from each of `followers` confirming an ack of its term sent
/// now and saying it holds that configuration. `followers` must be a
/// majority of each side, with the leader.
pub fn commit_founding(
    leader: &mut TestNode,
    clock: &FakeClock,
    followers: &[WorkerId],
) {
    let held = leader
        .configuration()
        .expect("commit_founding: the leader holds a configuration")
        .generation();
    for follower in followers {
        let mut beat = heartbeat(
            follower,
            Some(AckEcho {
                term: leader.term(),
                send_token: clock.now().as_ticks(),
            }),
        );
        beat.configuration_generation = Some(held.into());
        deliver(leader, follower, heartbeat_message(beat));
    }
    assert!(
        leader
            .configuration()
            .is_some_and(|configuration| !configuration.is_joint()),
        "commit_founding: the leader must have committed"
    );
}

/// Hands each message among `outputs`, which `from` produced, to its
/// addressee among `nodes`, then does the same for every message those
/// deliveries produce, until none is left. A published message goes to
/// every node among `nodes` but its publisher. A message to a worker not in
/// `nodes` is dropped.
pub fn deliver_all<C>(
    nodes: &mut BTreeMap<WorkerId, WorkerNode<C>>,
    from: &WorkerId,
    outputs: Vec<Output>,
) where
    C: Clock,
{
    let mut in_flight: VecDeque<(WorkerId, WorkerId, ElectionMessage)> = VecDeque::new();
    let queue = |in_flight: &mut VecDeque<_>,
                 sender: &WorkerId,
                 outputs: &[Output],
                 everyone: Vec<WorkerId>| {
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    in_flight.push_back((sender.clone(), to.clone(), message.clone()));
                }
                Output::Publish { message } => {
                    for to in everyone.iter().filter(|to| *to != sender) {
                        in_flight.push_back((sender.clone(), to.clone(), message.clone()));
                    }
                }
                _ => {}
            }
        }
    };
    queue(
        &mut in_flight,
        from,
        &outputs,
        nodes.keys().cloned().collect(),
    );
    while let Some((sender, to, message)) = in_flight.pop_front() {
        let everyone: Vec<WorkerId> = nodes.keys().cloned().collect();
        let Some(node) = nodes.get_mut(&to) else {
            continue;
        };
        let replies = deliver(node, &sender, message);
        queue(&mut in_flight, &to, &replies, everyone);
    }
}

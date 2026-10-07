//! Helpers for driving `WorkerNode`s by hand and reading back what they
//! asked their driver to do.

use kabudachi_core::election::{Entry, Identity, Input, Output, WorkerNode};
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionMessage, ElectionReject, RollCall,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::LeadershipGrant;
use kabudachi_core::time::{Clock, Duration};

use crate::support::builders::{
    checked, g0, heartbeat, heartbeat_message, message_input, past_any_suspicion, roll_call_reply, shard, timings,
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
pub fn published_roll_calls(outputs: &[Output]) -> Vec<Checked<RollCall>> {
    published(outputs)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::RollCall(call)) => Some(call),
            _ => None,
        })
        .collect()
}

/// The refusals among `outputs` addressed to `recipient`, in order.
pub fn rejects_sent_to(outputs: &[Output], recipient: &WorkerId) -> Vec<Checked<ElectionReject>> {
    sent_to(outputs, recipient)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::ElectionReject(reject)) => Some(reject),
            _ => None,
        })
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
    node.step(message_input(from, message)).outputs
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
) -> Checked<RollCall> {
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

/// Hands `node`, which has just won, `Input::Reconciled` for its office, as
/// its driver does once its scheduler has rebuilt and republished; returns
/// what that step asked for. A node tested for its election alone has no
/// tasks to reconcile.
pub fn finish_reconciling(node: &mut TestNode) -> Vec<Output> {
    let office = node
        .office_term()
        .expect("finish_reconciling: the node holds office");
    assert_eq!(
        node.state(),
        WorkerState::LeaderReconciling,
        "finish_reconciling: the node must be reconciling"
    );
    node.step(Input::Reconciled(office)).outputs
}

/// Elects `node`, an `Active` voter, over a real roll call for the term
/// after the latest it knows of: stands it as the candidate (see
/// [`stand_as_candidate`]), so each of `peers` is in the roster it wins,
/// then hands it a grant from each of them until it wins, and finishes its
/// reconciling. With no `peers` its own roll call elects it at its
/// deadline. Returns what the steps that won and reconciled asked for.
pub fn elect(
    node: &mut TestNode,
    clock: &FakeClock,
    suspect_timeout: u64,
    peers: &[WorkerId],
) -> Vec<Output> {
    if peers.is_empty() {
        start_roll_call(node, clock, suspect_timeout);
        let mut won = close_roll_call(node, clock, suspect_timeout);
        won.extend(finish_reconciling(node));
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
    won.extend(finish_reconciling(node));
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

//! Drives a `core::election::WorkerNode` against a real [`crate::messenger::Net`]:
//! nothing inside `WorkerNode` ever polls its own inbox (see
//! `core::election`'s module doc — `on_message`/`tick` are the whole surface
//! a driver calls), so something outside it has to drain `Net::poll_inbox`
//! and feed the results in, and call `tick()` on a schedule. This is that
//! something.
//!
//! This is the chunk-C3 "walking skeleton" piece: proof that `core`'s
//! existing, unmodified election state machine converges over a real
//! `Net`/libp2p swarm when driven by an ordinary async loop, not just the
//! in-memory simulator. Chunk C4 grew it with the bootstrap join protocol's
//! answering side (see [`respond_to_join_requests`]); chunk C6 grows it
//! again with the claim arbitration protocol's answering side (see
//! [`respond_to_claim_requests`]) and worker-side leader tracking (see
//! "Leader tracking" below). A later chunk (graceful shutdown, ...) is
//! expected to grow it further rather than replace it.
//!
//! ## Leader tracking (chunk C6)
//!
//! Claim arbitration needs a follower to know which peer to send
//! `REQUEST_CLAIM` to — "whoever it currently believes is leader" — but
//! `WorkerNode` exposes no such accessor (checked: `core/src/election.rs`
//! has none). Per this chunk's brief, that belief is tracked here, in `net`,
//! rather than adding new `core` surface: every inbound `ElectionMessage`
//! passes through this driver before `node.on_message` sees it (the loop
//! below), and a `LeaderHeartbeatAck` among them already carries the term's
//! `leader_id` — `core::election::WorkerNode::on_leader_ack` uses the same
//! field internally. [`run_driver`]'s `on_leader` callback fires with that
//! `WorkerId` whenever one arrives, mirroring the existing `on_tick`
//! callback's shape.
//!
//! This is a deliberate approximation, not a re-derivation of
//! `on_leader_ack`'s full gate: `on_message` only honours an ack when
//! `ack.leader_id() == from` (a message naming a different worker than its
//! own sender is dropped) — that check is replicated here, since it's
//! observable from the message alone. `on_leader_ack` additionally checks
//! the ack's shard_id, recovery_epoch and term against `WorkerNode`'s private
//! fields, which this driver cannot see without new `core` accessors (the
//! brief's "no new core surface" guidance). For this chunk's scope — a
//! single shard, non-adversarial peers — that gap is accepted rather than
//! closed; a later chunk with a real need (multiple shards sharing a
//! process, or a hostile-peer threat model) should revisit whether `core`
//! should expose this instead.

use std::collections::BTreeSet;
use std::time::Duration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::MembershipView;
use kabudachi_core::protocol::ids::{IdGenerator, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    Claim, ClaimReject, ClaimRejectReason, ClaimResponse, ElectionMessage, JoinMember,
    JoinResponse, claim_response, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{ClaimRejection, Scheduler};
use kabudachi_core::time::Clock;
use kabudachi_core::transport::PeerMessenger;

use crate::messenger::Net;

/// Runs forever: every `tick_interval`, drains `net`'s inbox into
/// `node.on_message`, calls `node.tick()`, then reports the resulting
/// `node.state()` to `on_tick` (pass `|_| {}` to ignore it). Callers stop
/// this by dropping (or aborting the task wrapping) the future it returns —
/// there is no internal exit condition, mirroring `WorkerNode` itself having
/// no concept of being "done".
///
/// `on_tick` exists because nothing else here gives an external caller any
/// way to observe `node`'s state: `run_driver` holds `node` by exclusive
/// `&mut` borrow for as long as it runs (forever, absent cancellation), so a
/// caller cannot also read `node.state()` from outside — e.g. in a
/// `tokio::select!` racing two of these against a convergence check, as
/// `net/tests/two_node_election_test.rs` does. `on_leader` (chunk C6) exists
/// for the same reason, one level down: see the module doc's "Leader
/// tracking" section.
///
/// `scheduler` (chunk C6) is driven alongside `node`: every tick,
/// [`respond_to_claim_requests`] first syncs it to `node`'s current election
/// state (`Scheduler::set_worker_state`) and then answers whatever
/// `/kabudachi/claim/1` requests are pending. Every driven node carries one,
/// whether or not it is ever leader — `Scheduler::request_claim`'s own
/// `NotLeader` rejection already handles a non-leader node correctly, so
/// there is no need for `run_driver`'s caller to decide "am I the kind of
/// node that answers claims" up front (mirrors `respond_to_join_requests`
/// not checking "am I in a position to answer" either — it just answers with
/// whatever it currently knows).
///
/// `node`'s transport is expected to be `&Net` borrowing the very `net`
/// passed here (see `impl PeerMessenger for &Net` in `crate::messenger` for
/// why a caller needs both a transport-shaped handle inside the node and its
/// own handle to poll), but nothing here enforces that — any `PeerMessenger`
/// works, as long as `net`'s inbox is the one the caller actually wants
/// drained into `node`.
#[allow(clippy::too_many_arguments)]
pub async fn run_driver<C, M, V, A, C2, I>(
    node: &mut WorkerNode<C, M, V, A>,
    net: &Net,
    tick_interval: Duration,
    on_tick: impl Fn(WorkerState),
    on_leader: impl Fn(WorkerId),
    scheduler: &mut Scheduler<C2, I>,
) -> std::convert::Infallible
where
    C: Clock,
    M: PeerMessenger,
    V: MembershipView,
    A: CoordinationAuthority,
    C2: Clock,
    I: IdGenerator,
{
    let my_id = net.local_worker_id();
    let mut interval = tokio::time::interval(tick_interval);
    // A run that falls behind (e.g. a slow CI host) coalesces to "run once,
    // now", rather than bursting through a backlog of catch-up ticks.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        for (from, msg) in net.poll_inbox(my_id.clone()) {
            if let Some(leader_id) = observed_leader(&from, &msg) {
                on_leader(leader_id);
            }
            node.on_message(from, msg);
        }
        respond_to_join_requests(node, net, &my_id);
        respond_to_claim_requests(node, scheduler, net);
        node.tick();
        on_tick(node.state());
    }
}

/// Chunk C6's leader-tracking peek: `Some(leader_id)` if `msg` is a
/// `LeaderHeartbeatAck` naming `from` as leader (the one check `on_message`
/// itself applies before honouring an ack — see the module doc's "Leader
/// tracking" section for what this deliberately does not also check).
fn observed_leader(from: &WorkerId, msg: &ElectionMessage) -> Option<WorkerId> {
    match &msg.payload {
        Some(election_message::Payload::HeartbeatAck(ack)) if ack.leader_id() == *from => {
            Some(from.clone())
        }
        _ => None,
    }
}

/// Answers every inbound `/kabudachi/join/1` request queued on `net` with
/// this node's current electorate (README §27 Phase 2, spec decision 5 step
/// (a)'s answering side).
///
/// `core::election::WorkerNode` knows the electorate but nothing about
/// network addresses, and `Net` knows addresses but nothing about the
/// electorate (see `Net::peer_addresses`'s doc) — composing a `JOIN_RESPONSE`
/// needs both, so it happens here, in the driver, rather than being pushed
/// into either side alone.
///
/// A member this node has no known address for — including itself, before
/// its first successful `listen_on` — is still included, with `multiaddr`
/// left empty, rather than blocking on discovering one. Omitting the member
/// entirely would make the response's `WorkerId` set incomplete, and
/// `WorkerNode::finish_joining` rebuilds its electorate from exactly that
/// set — so every member must be named even when there's nothing to dial
/// yet. `Net::try_join_via_seed` skips dialing an empty `multiaddr` but
/// still records the `WorkerId`.
fn respond_to_join_requests<C, M, V, A>(node: &WorkerNode<C, M, V, A>, net: &Net, my_id: &WorkerId)
where
    C: Clock,
    M: PeerMessenger,
    V: MembershipView,
    A: CoordinationAuthority,
{
    let pending = net.poll_join_requests();
    if pending.is_empty() {
        return;
    }

    let electorate: BTreeSet<WorkerId> = node.electorate();
    let known_addresses = net.peer_addresses();
    let local_addr = net.local_multiaddr();

    let members: Vec<JoinMember> = electorate
        .into_iter()
        .map(|member_id| {
            let addr = if &member_id == my_id {
                local_addr.clone()
            } else {
                known_addresses.get(&member_id).cloned()
            };
            JoinMember {
                worker_id: Some(member_id.into()),
                multiaddr: addr.map(|addr| addr.to_string()).unwrap_or_default(),
            }
        })
        .collect();

    for handle in pending {
        net.respond_join(
            handle,
            JoinResponse {
                members: members.clone(),
            },
        );
    }
}

/// Answers every inbound `/kabudachi/claim/1` request queued on `net`,
/// calling `core::scheduler::Scheduler::request_claim` for each one (README
/// §27 Phase 2, chunk C6's answering side).
///
/// Unlike [`respond_to_join_requests`], this needs *write* access to
/// `scheduler` (it decides, not just reads) and must first bring it into
/// sync with `node`'s current election state:
/// `Scheduler::request_claim`/`is_leader` gate entirely on
/// `Scheduler::set_worker_state`, which nothing calls on its own (see that
/// method's doc — `bindings/src/election.rs`'s `Publisher::publish` does the
/// same sync for the single-process case this chunk's brief points at as
/// precedent). The sync runs on every call, not only when a request is
/// actually pending, so a scheduler that currently has nothing to answer is
/// still correct the next time something arrives — a claim request and the
/// election state change that would flip the answer can arrive in either
/// order within the same tick.
fn respond_to_claim_requests<C, M, V, A, C2, I>(
    node: &WorkerNode<C, M, V, A>,
    scheduler: &mut Scheduler<C2, I>,
    net: &Net,
) where
    C: Clock,
    M: PeerMessenger,
    V: MembershipView,
    A: CoordinationAuthority,
    C2: Clock,
    I: IdGenerator,
{
    scheduler.set_worker_state(node.state());

    let pending = net.poll_claim_requests();
    if pending.is_empty() {
        return;
    }

    for handle in pending {
        let claimant = handle.from();
        let task_id = handle.task_id();
        let response = match scheduler.request_claim(&claimant, &task_id) {
            Ok(claim) => ClaimResponse {
                result: Some(claim_response::Result::Accept(Claim {
                    task: Some(claim.task),
                    task_run_id: Some(claim.task_run_id.into()),
                    attempt_number: claim.attempt_number,
                    chain: claim.chain,
                })),
            },
            Err(rejection) => ClaimResponse {
                result: Some(claim_response::Result::Reject(ClaimReject {
                    reason: claim_reject_reason(rejection) as i32,
                })),
            },
        };
        net.respond_claim(handle, response);
    }
}

/// `core::scheduler::ClaimRejection` -> wire `ClaimRejectReason`, one arm per
/// variant and no wildcard arm: if `core` ever adds an eighth
/// `ClaimRejection` case, this fails to compile instead of silently mapping
/// it to the wrong reason (the exact hazard task-C6-brief.md flags — this
/// enum's shape has changed more than once during Phase 1).
fn claim_reject_reason(rejection: ClaimRejection) -> ClaimRejectReason {
    match rejection {
        ClaimRejection::NotLeader => ClaimRejectReason::ClaimRejectNotLeader,
        ClaimRejection::TaskUnknown => ClaimRejectReason::ClaimRejectTaskUnknown,
        ClaimRejection::NotReady => ClaimRejectReason::ClaimRejectNotReady,
        ClaimRejection::AlreadySelected => ClaimRejectReason::ClaimRejectAlreadySelected,
        ClaimRejection::Finished => ClaimRejectReason::ClaimRejectFinished,
        ClaimRejection::Superseded => ClaimRejectReason::ClaimRejectSuperseded,
        ClaimRejection::KeyBusy => ClaimRejectReason::ClaimRejectKeyBusy,
    }
}

#[cfg(test)]
mod tests {
    use kabudachi_core::protocol::ids::ShardId;
    use kabudachi_core::protocol::messages::{LeaderHeartbeatAck, RollCall};

    use super::*;

    fn ack_from(leader: &WorkerId) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(
                LeaderHeartbeatAck {
                    shard_id: Some(ShardId::new("shard-1").into()),
                    leader_id: Some(leader.clone().into()),
                    recovery_epoch: 0,
                    term: 1,
                    membership_generation: 0,
                },
            )),
        }
    }

    #[test]
    fn observed_leader_returns_the_sender_of_its_own_heartbeat_ack() {
        let leader = WorkerId::new("leader-1");

        let observed = observed_leader(&leader, &ack_from(&leader));

        assert_eq!(observed, Some(leader));
    }

    #[test]
    fn observed_leader_ignores_an_ack_naming_a_different_worker_than_its_sender() {
        // Mirrors core::election::WorkerNode::on_message's own guard
        // (`ack.leader_id() == from`): a message claiming a leader other
        // than whoever actually sent it is untrustworthy, whether `core` or
        // this peek is the one looking at it.
        let sender = WorkerId::new("sender-1");
        let claimed_leader = WorkerId::new("someone-else");

        let observed = observed_leader(&sender, &ack_from(&claimed_leader));

        assert_eq!(observed, None);
    }

    #[test]
    fn observed_leader_ignores_non_heartbeat_ack_payloads() {
        let from = WorkerId::new("worker-1");
        let roll_call = ElectionMessage {
            payload: Some(election_message::Payload::RollCall(RollCall {
                roll_call_id: "roll-call-1".into(),
                shard_id: Some(ShardId::new("shard-1").into()),
                recovery_epoch: 0,
                membership_generation: 0,
                membership_digest: vec![],
                highest_term_seen: 0,
                initiator_id: Some(from.clone().into()),
                responses: vec![],
            })),
        };

        assert_eq!(observed_leader(&from, &roll_call), None);
    }

    #[test]
    fn claim_reject_reason_maps_every_claim_rejection_variant() {
        assert_eq!(
            claim_reject_reason(ClaimRejection::NotLeader),
            ClaimRejectReason::ClaimRejectNotLeader
        );
        assert_eq!(
            claim_reject_reason(ClaimRejection::TaskUnknown),
            ClaimRejectReason::ClaimRejectTaskUnknown
        );
        assert_eq!(
            claim_reject_reason(ClaimRejection::NotReady),
            ClaimRejectReason::ClaimRejectNotReady
        );
        assert_eq!(
            claim_reject_reason(ClaimRejection::AlreadySelected),
            ClaimRejectReason::ClaimRejectAlreadySelected
        );
        assert_eq!(
            claim_reject_reason(ClaimRejection::Finished),
            ClaimRejectReason::ClaimRejectFinished
        );
        assert_eq!(
            claim_reject_reason(ClaimRejection::Superseded),
            ClaimRejectReason::ClaimRejectSuperseded
        );
        assert_eq!(
            claim_reject_reason(ClaimRejection::KeyBusy),
            ClaimRejectReason::ClaimRejectKeyBusy
        );
    }
}

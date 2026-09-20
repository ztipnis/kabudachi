//! Message, ID and membership builders shared by the election tests.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use kabudachi_core::membership::{MembershipView, RingMembership};
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::{
    ElectionMessage, RollCall, RollCallObservation, SelfRemove, VoteGrant, VoteRequest,
    election_message,
};

use crate::support::clock::FakeClock;
use crate::support::network::FakeNetwork;

const SHARD: &str = "shard-1";

pub fn worker(id: &str) -> WorkerId {
    WorkerId::new(id)
}

pub fn shard(id: &str) -> ShardId {
    ShardId::new(id)
}

/// A `RollCall` for `shard-1`; epoch, generation, digest and term are zero or
/// empty because no test reads them back.
pub fn roll_call(id: &str, initiator: WorkerId, responses: Vec<RollCallObservation>) -> RollCall {
    RollCall {
        roll_call_id: id.to_string(),
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch: 0,
        membership_generation: 0,
        membership_digest: vec![],
        highest_term_seen: 0,
        initiator_id: Some(initiator.into()),
        responses,
    }
}

/// An `Active` worker's observation reporting `highest_term_seen`.
pub fn observation(worker_id: WorkerId, highest_term_seen: u64) -> RollCallObservation {
    RollCallObservation {
        worker_id: Some(worker_id.into()),
        state: generated::WorkerState::Active as i32,
        highest_term_seen,
        current_leader_seen: None,
        leader_contact_age_ticks: 0,
    }
}

pub fn roll_call_message(call: RollCall) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::RollCall(call)),
    }
}

/// A `VoteRequest` for `shard-1`; membership fields and digests are left
/// zero or empty.
pub fn vote_request(candidate: WorkerId, recovery_epoch: u64, term: u64) -> VoteRequest {
    VoteRequest {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch,
        term,
        candidate_id: Some(candidate.into()),
        membership_generation: 0,
        membership_digest: vec![],
        roll_call_digest: vec![],
    }
}

/// A `VoteGrant` for `shard-1` at recovery epoch 0.
pub fn vote_grant(candidate: WorkerId, voter: WorkerId, term: u64) -> VoteGrant {
    VoteGrant {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch: 0,
        term,
        candidate_id: Some(candidate.into()),
        voter_id: Some(voter.into()),
    }
}

/// A `FakeNetwork` sharing `clock`, with every one of `members` registered.
pub fn make_network(clock: &FakeClock, members: &[WorkerId]) -> FakeNetwork {
    let network = FakeNetwork::new(Rc::new(clock.clone()));
    for member in members {
        network.register(member.clone());
    }
    network
}

/// A shared handle onto a `RingMembership`. `WorkerNode` owns its membership
/// and exposes no accessor, so tests keep a clone of this handle to read the
/// node's membership back.
#[derive(Clone)]
pub struct SharedMembership(Rc<RefCell<RingMembership>>);

impl SharedMembership {
    pub fn new(initial_members: BTreeSet<WorkerId>) -> Self {
        Self(Rc::new(RefCell::new(RingMembership::new(initial_members))))
    }
}

impl MembershipView for SharedMembership {
    fn effective_electorate(&self) -> BTreeSet<WorkerId> {
        self.0.borrow().effective_electorate()
    }

    fn ring_successors(&self, me: WorkerId) -> Vec<WorkerId> {
        self.0.borrow().ring_successors(me)
    }

    fn ring_predecessors(&self, me: WorkerId) -> Vec<WorkerId> {
        self.0.borrow().ring_predecessors(me)
    }

    fn membership_generation(&self) -> u64 {
        self.0.borrow().membership_generation()
    }

    fn membership_digest(&self) -> [u8; 8] {
        self.0.borrow().membership_digest()
    }

    fn apply_self_remove(&mut self, msg: &SelfRemove) {
        self.0.borrow_mut().apply_self_remove(msg)
    }

    fn rebuild(&mut self, new_members: BTreeSet<WorkerId>) {
        self.0.borrow_mut().rebuild(new_members)
    }
}

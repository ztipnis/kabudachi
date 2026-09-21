//! The pieces of a shard with exactly one worker: nobody to message, no
//! external authority to consult, and a node that elects itself.

use std::collections::BTreeSet;

use crate::coordination_authority::{AuthorityError, CoordinationAuthority};
use crate::election::WorkerNode;
use crate::membership::RingMembership;
use crate::protocol::ids::{IncarnationId, ShardId, WorkerId};
use crate::protocol::messages::ElectionMessage;
use crate::time::{Clock, Duration};
use crate::transport::PeerMessenger;

/// A messenger for a shard where the worker is alone: messages go nowhere and
/// none arrive.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoPeers;

impl PeerMessenger for NoPeers {
    fn send(&self, _from: WorkerId, _to: WorkerId, _message: ElectionMessage) {}

    fn poll_inbox(&self, _me: WorkerId) -> Vec<(WorkerId, ElectionMessage)> {
        Vec::new()
    }

    fn reachable_peers(&self, _me: WorkerId) -> BTreeSet<WorkerId> {
        BTreeSet::new()
    }
}

/// An authority that can never be reached. A lone worker never needs forced
/// recovery, so it has none to ask.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAuthority;

impl CoordinationAuthority for NoAuthority {
    fn discover_workers(&self, _shard_id: &ShardId) -> Result<BTreeSet<WorkerId>, AuthorityError> {
        Err(AuthorityError::Unavailable)
    }

    fn read_recovery_epoch(&self, _shard_id: &ShardId) -> Result<u64, AuthorityError> {
        Err(AuthorityError::Unavailable)
    }

    fn force_reconfigure(
        &self,
        _shard_id: &ShardId,
        _expected_recovery_epoch: u64,
        _replacement_members: BTreeSet<WorkerId>,
    ) -> Result<u64, AuthorityError> {
        Err(AuthorityError::Unavailable)
    }
}

/// The concrete node type of a shard of one, nameable where a generic
/// `WorkerNode` cannot be (for example a Python-facing class).
pub type SingleNode<C> = WorkerNode<C, NoPeers, RingMembership, NoAuthority>;

/// A node that is the whole shard. Its majority is itself, so it becomes
/// leader on its own once time has moved: a lone worker has no peer to
/// falsely suspect, so it waits no suspicion timeout before doing so. The
/// node only acts as `clock` advances and `tick` is called.
pub fn single_node<C: Clock>(
    worker_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    clock: C,
) -> SingleNode<C> {
    let membership = RingMembership::new(BTreeSet::from([worker_id.clone()]));
    WorkerNode::new(
        worker_id,
        incarnation_id,
        shard_id,
        clock,
        NoPeers,
        membership,
        NoAuthority,
        Duration::from_ticks(0),
    )
}

//! Wiring for a worker that never talks to a real network: `bindings`'s
//! Phase-1 single-process runtime is deliberately a shard of one, with no
//! seeds to dial and no coordination authority worth consulting.
//!
//! This is `bindings`'s own replacement for the deleted
//! `core::single_node` module (chunk C5): that module's `single_node()`
//! helper hardcoded an instant (`Duration::from_ticks(0)`) suspicion
//! timeout, which is a legitimate, deliberate product choice for this
//! specific single-process runtime (prompt self-election, no network to
//! wait on) — but it is *not* the generic behavior of a one-member
//! electorate. `kabudachi_net::bootstrap::bootstrap_node` (chunk C5's
//! `net`-side bootstrap cascade) covers the generic case: a lone node that
//! reaches self-election still waits out whatever `suspect_timeout` it was
//! actually configured with, like any other node. Keeping the instant
//! variant here, scoped to this crate's one caller, keeps that distinction
//! visible instead of blurring it back into a shared, ambiguously-named
//! helper.
//!
//! `InMemoryAuthority` (from `kabudachi_core`, built for chunk C5's
//! bootstrap cascade) stands in for "no authority": a freshly constructed
//! one, with nothing ever registered against it, always answers
//! `discover_workers` with an empty set — and
//! `core::election::WorkerNode` only ever calls into its `authority` from
//! `attempt_forced_recovery`, which only runs from `WorkerState::NoQuorum`.
//! A one-member electorate driven by `NoPeers` (below) can never *reach*
//! `NoQuorum`: `WorkerNode::tick_as_leader`'s quorum of 1 is always met by
//! counting the node itself, since `NoPeers::reachable_peers` reporting zero
//! other peers is exactly what a lone worker's real state is. So for this
//! single-worker use case the authority is provably never consulted, and no
//! dedicated "unavailable" stand-in type (the deleted `NoAuthority`) is
//! needed here.

use std::collections::BTreeSet;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::time::{Clock, Duration};
use kabudachi_core::transport::PeerMessenger;

/// A messenger for a worker that is alone: messages go nowhere and none
/// arrive. Bindings-local replacement for the deleted
/// `core::single_node::NoPeers`.
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

/// The concrete node type of `bindings`'s single-process, single-worker
/// shard, nameable where a generic `WorkerNode` cannot be (for example the
/// field type `election::run_election` takes).
pub type LocalNode<C> = WorkerNode<C, NoPeers, RingMembership, InMemoryAuthority>;

/// A node that is the whole shard. Its majority is itself, so it becomes
/// leader on its own once time has moved: a lone worker has no peer to
/// falsely suspect, so it waits no suspicion timeout before doing so (see
/// the module doc for why that is specific to this single-process runtime,
/// not the generic one-member-electorate case). The node only acts as
/// `clock` advances and `tick` is called.
pub fn local_node<C: Clock>(
    worker_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    clock: C,
) -> LocalNode<C> {
    let membership = RingMembership::new(BTreeSet::from([worker_id.clone()]));
    WorkerNode::new(
        worker_id,
        incarnation_id,
        shard_id,
        clock,
        NoPeers,
        membership,
        InMemoryAuthority::new(),
        Duration::from_ticks(0),
    )
}

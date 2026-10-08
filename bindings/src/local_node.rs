//! Wiring for a worker that never talks to a real network: `bindings`'s
//! Phase-1 single-process runtime is deliberately a shard of one, with no
//! seeds to dial and no coordination authority worth consulting.
//!
//! This is `bindings`'s own replacement for the deleted `core::single_node`
//! module. The suspicion timeout comes from the caller:
//! `_native.NativeRuntime`'s `suspect_timeout_ms`, which is 0 by default. An
//! instant suspicion timeout is a legitimate, deliberate product choice for
//! this specific single-process runtime (prompt self-election, no network to
//! wait on) — but it is *not* the generic behavior of a one-voter
//! configuration. `kabudachi_net::bootstrap::bootstrap` (the `net`-side
//! bootstrap cascade) covers the generic case: a lone node that reaches
//! self-election still waits out whatever `suspect_timeout` it was actually
//! configured with, like any other node. Keeping the instant default here,
//! scoped to this crate's one caller, keeps that distinction visible instead
//! of blurring it back into a shared, ambiguously-named helper.
//!
//! This single-process runtime has no coordination authority configured, so
//! `local_node` passes `None`: the node never registers, never fences itself
//! and never needs a recovery fence to lead.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::{ElectionTimings, Entry, Identity, Step, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, ShardName, WorkerId};
use kabudachi_core::time::{Clock, Duration};

/// How often the node would heartbeat its leader as a follower. A lone node
/// never has a leader other than itself to heartbeat, so the value never
/// matters; it only has to be non-zero.
const HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// How long the node's own roll call runs before it decides on it. A lone
/// node has no other worker to hear from, so the shortest deadline there is
/// elects it soonest.
const ROLL_CALL_DEADLINE_MS: u64 = 1;

/// The concrete node type of `bindings`'s single-process, single-worker
/// shard, nameable where a generic `WorkerNode` cannot be (for example the
/// field type `election::run_election` takes). It is never told of a
/// connection and receives nothing. The only messages it sends are the roll
/// call it publishes and the election certificate a winner sends every
/// voter, here only itself, both of which `election::run_election` drops.
pub type LocalNode<C> = WorkerNode<C>;

/// A node that is the whole shard: the worker that creates it, a new
/// incarnation of `shard_name` each start, the only voter of its genesis configuration (at recovery epoch 0). Its quorum is
/// itself, so it becomes leader on its own once `suspect_timeout` has
/// passed and its roll call has run its one millisecond: a lone worker has
/// no peer to falsely suspect, so the single-process runtime passes 0 unless
/// told otherwise (see the module doc for why that is specific to this
/// runtime, not the generic one-voter case). The node only acts as `clock`
/// advances and it is stepped. Returns the node with its first step, for
/// `election::run_election` to carry out.
///
/// The shard's recovery epoch is of lineage 0: with no authority, no other
/// founding of this shard can exist to tell apart from this one.
pub fn local_node<C: Clock>(
    worker_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_name: &ShardName,
    clock: C,
    suspect_timeout: Duration,
) -> (LocalNode<C>, Step) {
    let shard_id = ShardId::mint(shard_name);
    let identity = Identity {
        id: worker_id,
        incarnation: incarnation_id,
        shard: shard_id.clone(),
        timings: ElectionTimings::new(
            suspect_timeout,
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    };
    let entry = Entry::Founding {
        shard_id,
        recovery_epoch: RecoveryEpoch::new(0, 0),
        registered_at: None,
    };
    WorkerNode::start(identity, entry, clock, None)
}

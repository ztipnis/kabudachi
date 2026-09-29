//! How a [`WorkerNode`] comes to be: [`WorkerNode::start`], the one
//! constructor, from the worker's [`Identity`] and the [`Entry`] by which it
//! enters its shard.

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::{IncarnationId, ShardId, WorkerId};
use crate::protocol::messages::JoinResponse;
use crate::time::{Clock, Instant};

use super::{AuthorityTimings, ElectionTimings, KnownConfiguration, Step, WorkerNode};

/// Who a node is: its worker, that worker's process incarnation, the shard
/// it belongs to, and the timers it runs its election on. Every worker in a
/// shard runs the same timings (see [`ElectionTimings::suspect_timeout`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub id: WorkerId,
    pub incarnation: IncarnationId,
    pub shard: ShardId,
    pub timings: ElectionTimings,
}

/// How a node enters its shard. The bootstrap cascade ends by choosing one.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// Found a new shard as its only member, at `recovery_epoch` (ADR-0001
    /// decision 1): the node starts `Active` as the only voter of the
    /// genesis configuration, admitted at the genesis generation, and leads
    /// once its suspicion timeout has passed and its own roll call, of one
    /// voter, has elected it. The epoch's lineage is the one the founder
    /// drew (see [`RecoveryEpoch::founding`]), which a node with an
    /// authority must know to recognise its own epoch there.
    ///
    /// `registered_at` is when the founder asked the authority to register
    /// it, before it took ownership of the shard; `None` with no authority.
    /// Its orphan deadline counts from then, not from when the node is
    /// built: a founder that counted from later could still lead after its
    /// registration had lapsed, and after another worker had found the
    /// shard with no one registered and re-founded it.
    Founding {
        recovery_epoch: RecoveryEpoch,
        registered_at: Option<Instant>,
    },
    /// Join the leader this pointer names, as a pending member: one no
    /// quorum counts until an election admits it, which learns the shard's
    /// configuration from its leader's first ack. The pointer must name a
    /// leader of the node's shard (see [`WorkerNode::finish_joining`]); one
    /// that names none leaves the node `Bootstrapping`.
    Joining(JoinResponse),
    /// Start `Active` inside a configuration already known, at that
    /// configuration's recovery epoch, of lineage 0.
    Known(KnownConfiguration),
}

impl<C: Clock> WorkerNode<C> {
    /// Builds the node `identity` names, entering its shard by `entry`, and
    /// returns it with its first step, which the caller carries out like any
    /// other (see [`super::carry_out`]). For a joiner that step heartbeats
    /// the leader it joined at once; otherwise it asks for nothing and is
    /// due now, so the node is ticked at once to learn its first deadline.
    ///
    /// `authority` is the coordination authority's timings, or `None` for a
    /// node with no authority, which never registers, fences itself or
    /// needs a recovery fence to lead. The node starts connected to no one:
    /// its driver reports the connections it holds as
    /// [`super::Input::PeerConnected`]. Its leader-contact timer starts now,
    /// so a new node is not immediately suspicious.
    ///
    /// # Panics
    ///
    /// Panics if `identity.timings.heartbeat_interval`,
    /// `roll_call_deadline` or `clock_drift_divisor` is zero, or if the node
    /// is not alone a quorum of its shard and twice its heartbeat interval
    /// is not shorter than its lease length: a caller bug. A lone voter
    /// never needs a lease, so it may run with any suspicion timeout, zero
    /// among them.
    pub fn start(
        identity: Identity,
        entry: Entry,
        clock: C,
        authority: Option<AuthorityTimings>,
    ) -> (Self, Step) {
        let Identity {
            id,
            incarnation,
            shard,
            timings,
        } = identity;
        match entry {
            Entry::Founding {
                recovery_epoch,
                registered_at,
            } => {
                let mut node = Self::genesis(
                    id,
                    incarnation,
                    shard,
                    clock,
                    recovery_epoch.number,
                    authority,
                    timings,
                )
                .with_recovery_lineage(recovery_epoch.lineage);
                if let Some(sent_at) = registered_at {
                    node = node.registered_at(sent_at);
                }
                let first = node.due_now();
                (node, first)
            }
            Entry::Joining(pointer) => {
                let mut node =
                    Self::bootstrapping(id, incarnation, shard, clock, authority, timings);
                let first = node.finish_joining(&pointer);
                (node, first)
            }
            Entry::Known(known) => {
                let node = Self::new(id, incarnation, shard, clock, known, authority, timings);
                let first = node.due_now();
                (node, first)
            }
        }
    }

    /// A step that asks for nothing and is due now.
    fn due_now(&self) -> Step {
        Step {
            outputs: Vec::new(),
            next_deadline: Some(self.clock.now()),
        }
    }
}

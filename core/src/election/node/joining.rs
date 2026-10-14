use crate::coordination_authority::{RecoveryEpoch, ShardRecord};
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::JoinResponse;
use crate::protocol::messages::prelude::*;
use crate::protocol::worker_state::WorkerState;
use crate::time::Clock;

use super::super::{Output, ReplyToken, StopReason, WorkerNode};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Takes the leader `pointer` names and drives `Bootstrapping ->
    /// Joining -> Active` (see [`Input::JoinAnswer`](crate::election::Input::JoinAnswer)).
    pub(super) fn join(&mut self, pointer: &JoinResponse) {
        if !self.state.can_transition_to(WorkerState::Joining) {
            return;
        }
        let Some(leader_id) = pointer.leader_id() else {
            return;
        };
        // A leader of another incarnation of the shard leads nothing this node
        // belongs to.
        if pointer.shard_id().as_ref() != Some(&self.shard_id) {
            return;
        }
        // A first join takes any pointer. After that, a pointer the floor the
        // node rejoins at does not accept leads nothing the node can return
        // to.
        let floor = self.standing.join_floor();
        if !floor.accepts(pointer) {
            return;
        }
        let named = RecoveryEpoch::new(pointer.recovery_epoch, pointer.recovery_epoch_lineage);

        self.transition_to(WorkerState::Joining);
        // A node with a floor is rejoining, and the pointer was taken from a
        // search that may lag the authority: it holds the pointer until a
        // read of the authority names its epoch. A read asked before this
        // says nothing of it.
        self.rejoin.awaited_read = None;
        if floor.epoch().is_some() {
            self.rejoin.held = Some(pointer.clone());
            return;
        }
        self.become_member_of(leader_id, pointer.term, named);
    }

    /// Becomes the pending member of `leader`, elected in `term` at
    /// `epoch`, that a validated JOIN pointer named: `Joining -> Active`.
    fn become_member_of(&mut self, leader: WorkerId, term: u64, epoch: RecoveryEpoch) {
        self.standing.joined(epoch, term);
        self.leader = Some((leader, term));
        // A freshly joined node hasn't heard from its leader yet; start the
        // suspicion clock now so it isn't judged suspect the instant it
        // ticks — the same reasoning `Self::new`'s doc gives for a freshly
        // constructed node.
        self.last_leader_contact = self.clock.now();
        // Its registration starts with its membership: until now it kept
        // none, so its lease counts from the join.
        let now = self.clock.now();
        if let Some(authority) = self.authority.as_mut() {
            authority.restart_at(now);
        }
        self.transition_to(WorkerState::Active);
    }

    /// Records `token` as the read of the authority's epoch that the next
    /// answer must name (see [`Input::AuthorityEpochAsked`](crate::election::Input::AuthorityEpochAsked)).
    pub(super) fn note_epoch_read_asked(&mut self, token: ReplyToken) {
        if matches!(
            self.state,
            WorkerState::Bootstrapping | WorkerState::Joining
        ) {
            self.rejoin.awaited_read = Some(token);
        }
    }

    /// Applies the answer `held` to the read `token` when it is the latest
    /// asked (see [`Input::AuthorityEpochRead`](crate::election::Input::AuthorityEpochRead)): a bootstrapping node
    /// refreshes its floor, and one validating a pointer becomes a member or
    /// drops the pointer.
    pub(super) fn on_epoch_read(&mut self, token: ReplyToken, held: ShardRecord) {
        if self.rejoin.awaited_read != Some(token) {
            return;
        }
        self.rejoin.awaited_read = None;
        if held.shard_id != self.shard_id {
            self.abandon_shard();
            return;
        }
        let held = held.recovery_epoch;
        match self.state {
            WorkerState::Bootstrapping => {
                let mut floor = self.standing.join_floor();
                let before = floor;
                floor.refresh(held);
                if floor != before {
                    self.standing.rejoin_at(held);
                }
            }
            WorkerState::Joining => {
                let Some(pointer) = self.rejoin.held.take() else {
                    return;
                };
                let named =
                    RecoveryEpoch::new(pointer.recovery_epoch, pointer.recovery_epoch_lineage);
                match pointer.leader_id() {
                    Some(leader) if named == held => {
                        self.become_member_of(leader, pointer.term, named);
                    }
                    _ => {
                        self.standing.rejoin_at(held);
                        self.transition_to(WorkerState::Bootstrapping);
                    }
                }
            }
            _ => {}
        }
    }

    /// Leaves the shard this node held for the one the authority holds at
    /// `epoch`, which it cannot resume or recover into: discards everything
    /// it knew of its own and goes back to `Bootstrapping`, for its driver to
    /// join it again. The epoch it rejoins is a
    /// floor: a JOIN pointer to a leader left on an older one (see
    /// [`EpochOrder::Stale`](crate::election::EpochOrder::Stale)) must not take it back there.
    pub(super) fn rejoin_at(&mut self, epoch: RecoveryEpoch) {
        self.forget_election_state();
        self.standing.rejoin_at(epoch);
        if let Some(authority) = self.authority.as_mut() {
            authority.rejoined();
        }
        self.leader = None;
        self.transition_to(WorkerState::Bootstrapping);
    }

    /// The node found the shard gone: no record under its name, or a record
    /// of another incarnation of it. The shard is abandoned, and this node
    /// stops for good,
    /// raising an alert. A restart re-enters the bootstrap cascade.
    pub(super) fn abandon_shard(&mut self) {
        self.stop_reason = Some(StopReason::Abandoned);
        self.transition_to(WorkerState::Stopped);
        self.outputs.push(Output::ShardAbandoned);
    }

    /// Forgets the election state this node held under its recovery epoch,
    /// as it leaves that epoch behind. What its standing forgets goes with
    /// the transition that moves it (see [`ShardStanding::accept_ack`](crate::election::standing::ShardStanding::accept_ack) and
    /// [`ShardStanding::rejoin_at`](crate::election::standing::ShardStanding::rejoin_at)).
    pub(super) fn forget_election_state(&mut self) {
        if let Some(authority) = self.authority.as_mut() {
            authority.epoch_left();
        }
        self.newest_accepted_ack = None;
        self.round.forget();
        self.drop_recovery();
        self.office = None;
    }
}

use crate::protocol::worker_state::WorkerState;
use crate::time::Clock;

use super::super::authority_standing::{AuthorityVerdict, AuthorityView};
use super::super::{AuthorityReply, Output, WorkerNode};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Asks the authority for what this node's standing has come due for
    /// (see [`AuthorityStanding::calls_due`](crate::election::authority_standing::AuthorityStanding::calls_due)).
    pub(super) fn ask_authority_if_due(&mut self) {
        let now = self.clock.now();
        let Some(authority) = self.authority.as_mut() else {
            return;
        };
        let view = AuthorityView {
            me: &self.my_id,
            shard: &self.shard_id,
            state: self.state,
            term: self.term,
            own_epoch: self.standing.epoch(),
        };
        self.outputs
            .extend(authority.calls_due(&view, now).into_iter().map(Output::Authority));
    }

    /// Handles what the authority answered to a call this node asked for.
    pub(super) fn on_authority_reply(&mut self, reply: AuthorityReply) {
        let now = self.clock.now();
        let Some(authority) = self.authority.as_mut() else {
            return;
        };
        let view = AuthorityView {
            me: &self.my_id,
            shard: &self.shard_id,
            state: self.state,
            term: self.term,
            own_epoch: self.standing.epoch(),
        };
        let verdicts = authority.on_reply(reply, &view, now);
        self.apply_authority(verdicts);
    }

    /// Carries out, in order, what this node's authority standing decided.
    fn apply_authority(&mut self, verdicts: Vec<AuthorityVerdict>) {
        for verdict in verdicts {
            match verdict {
                AuthorityVerdict::Ask(call) => self.outputs.push(Output::Authority(call)),
                AuthorityVerdict::Resume => {
                    self.lease.resumed();
                    self.last_leader_contact = self.clock.now();
                    self.transition_to(WorkerState::Active);
                }
                AuthorityVerdict::RejoinAt(epoch) => self.rejoin_at(epoch),
                AuthorityVerdict::StandAt {
                    epoch,
                    term,
                    roster,
                } => {
                    self.newest_accepted_ack = None;
                    if let Some(authority) = self.authority.as_mut() {
                        authority.epoch_left();
                    }
                    self.standing
                        .recovered_to(epoch, term, Some(&roster), &self.my_id);
                    self.term = term;
                    self.transition_to(WorkerState::Candidate);
                }
                AuthorityVerdict::Lead(roster) => self.take_office(roster),
                AuthorityVerdict::Abandon => self.abandon_shard(),
                AuthorityVerdict::LoseQuorum => self.lose_quorum(),
                AuthorityVerdict::SuspectAgain => self.suspect_again(),
            }
        }
    }

    /// Gives up the authority path this node runs, if any.
    pub(super) fn drop_recovery(&mut self) {
        if let Some(authority) = self.authority.as_mut() {
            authority.drop_recovery();
        }
    }

    /// Fences this node if its registration has
    /// lapsed, on its own clock, in a state that takes part in elections:
    /// it drops any roll call, vote or recovery it runs and any grant it
    /// holds, and asks its executor to abort its TaskRuns within the
    /// reconnect timeout, less drift. It keeps its configuration and
    /// admission generation. Returns whether it did.
    pub(super) fn orphan_if_unregistered(&mut self) -> bool {
        let now = self.clock.now();
        let lapsed = self
            .authority
            .as_ref()
            .is_some_and(|authority| !authority.is_registered(now));
        let takes_part = matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::Candidate
                | WorkerState::LeaderReconciling
                | WorkerState::Leader
                | WorkerState::NoQuorum
        );
        if !lapsed || !takes_part {
            return false;
        }
        self.round.stop();
        self.drop_recovery();
        self.transition_to(WorkerState::Fenced);
        self.lease.orphaned(now);
        true
    }
}

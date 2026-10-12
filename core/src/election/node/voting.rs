use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration};
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::{RollCall, VoteGrant, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::time::Clock;

use super::super::authority_standing::AuthorityStanding;
use super::super::election_round::{ElectionRound, Verdict, View};
use super::super::{Output, WorkerNode, led_or_followed};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Whether this node's state takes part in elections: only `Active`,
    /// `LeaderSuspect`, `RollCall` and `NoQuorum` answer roll calls and
    /// grant votes, a pending member among them.
    pub(super) fn takes_part_in_elections(&self) -> bool {
        matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::NoQuorum
        )
    }

    /// Has this node's election round decide, through `decide`, with a view
    /// of this node as it is now, and carries out what it decided (see
    /// [`Self::apply`]).
    pub(super) fn decide(&mut self, decide: impl FnOnce(&mut ElectionRound, &View<'_>) -> Vec<Verdict>) {
        let admission = self.counted_admission();
        let takes_part = self.takes_part_in_elections();
        let leader_contact_is_fresh = self.current_leader_still_valid();
        let view = View {
            me: &self.my_id,
            shard: &self.shard_id,
            recovery_epoch: self.standing.epoch(),
            highest_term_seen: self.standing.highest_term_seen(),
            configuration: led_or_followed(&self.office, &self.standing),
            admission,
            takes_part,
            roll_call_is_census: self
                .authority
                .as_ref()
                .is_some_and(AuthorityStanding::roll_call_is_census),
            leader_contact_is_fresh,
            roll_call_deadline: self.timings.roll_call_deadline,
            suspect_timeout: self.timings.suspect_timeout,
        };
        let verdicts = decide(&mut self.round, &view);
        self.apply(verdicts);
    }

    /// Carries out, in order, what this node's election round decided:
    /// sends and publishes its messages, and moves this node's state, term
    /// and highest term seen as each verdict says.
    fn apply(&mut self, verdicts: Vec<Verdict>) {
        use election_message::Payload;
        for verdict in verdicts {
            match verdict {
                Verdict::Publish(call) => self.publish_roll_call(call),
                Verdict::Answer { initiator, reply } => {
                    self.send(initiator, Payload::RollCallReply(reply));
                }
                Verdict::Reject {
                    to,
                    term,
                    reason,
                    name_leader,
                } => self.send_reject(to, term, reason, name_leader),
                Verdict::Stand { term } => {
                    self.transition_to(WorkerState::Candidate);
                    self.term = term;
                }
                Verdict::AskVotes { voters, request } => {
                    for voter in voters {
                        self.send(voter, Payload::VoteRequest(request.clone()));
                    }
                }
                Verdict::Grant { candidate, grant } => self.grant_vote(candidate, grant),
                Verdict::Certify {
                    respondent,
                    certificate,
                } => self.send(respondent, Payload::ElectionCertificate(certificate)),
                Verdict::Won { term, roster } => {
                    self.term = term;
                    self.take_office(roster);
                }
                Verdict::NoQuorum {
                    term,
                    configuration,
                    respondents,
                } => self.close_short(term, configuration, respondents),
                Verdict::SuspectAgain => self.suspect_again(),
            }
            self.debug_assert_round_matches_state();
        }
    }

    /// Starts this node's roll call `call`: drops any recovery, moves to
    /// `RollCall` and publishes it.
    fn publish_roll_call(&mut self, call: RollCall) {
        self.drop_recovery();
        self.transition_to(WorkerState::RollCall);
        self.publish(election_message::Payload::RollCall(call));
    }

    /// Grants `candidate` this vote, having raised the highest term seen to
    /// its term, which makes an initiator contesting an earlier term step down.
    fn grant_vote(&mut self, candidate: WorkerId, grant: VoteGrant) {
        self.standing.saw_term(grant.term);
        self.step_down_if_outpaced();
        self.send(candidate, election_message::Payload::VoteGrant(grant));
    }

    /// This node's roll call for `term` under `configuration` closed short
    /// with `respondents`: it goes `NoQuorum` and, with an authority, takes
    /// the authority path from that census, which ends where the roll call's
    /// retry would have come.
    fn close_short(
        &mut self,
        term: u64,
        configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
    ) {
        self.lose_quorum();
        let now = self.clock.now();
        let retry_at = self.round.roll_call_due(now);
        if let Some(authority) = self.authority.as_mut() {
            let call = authority.begin_recovery(term, configuration, respondents, now, retry_at);
            self.outputs.push(Output::Authority(call));
        }
    }

    /// The round's roll call and vote stand in for the `RollCall` and
    /// `Candidate` states' checks (see `ElectionRound`), so they move together.
    fn debug_assert_round_matches_state(&self) {
        debug_assert_eq!(
            self.round.roll_call_term().is_some(),
            self.state == WorkerState::RollCall,
            "a roll call runs exactly while the node is RollCall"
        );
        debug_assert!(
            !self.round.is_standing() || self.state == WorkerState::Candidate,
            "a vote runs only while the node is Candidate"
        );
    }

    /// The term this node holds or contests: its roll call's while
    /// `RollCall`, its candidacy's while `Candidate` and its leadership's
    /// while `Leader`. `None` in every other state.
    pub(super) fn term_in_play(&self) -> Option<u64> {
        match self.state {
            WorkerState::RollCall => self.round.roll_call_term(),
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader => {
                Some(self.term)
            }
            _ => None,
        }
    }

    /// Steps down once this node has seen a term
    /// later than the one it holds or contests: another worker is electing,
    /// or has elected, a leader for it. An ack from that term's leader
    /// returns the node to `Active` (see [`Self::on_leader_ack`]); anything
    /// else leaves it suspecting its leader again (see
    /// [`Self::suspect_again`]).
    pub(super) fn step_down_if_outpaced(&mut self) {
        if self
            .term_in_play()
            .is_some_and(|term| self.standing.highest_term_seen() > term)
        {
            self.suspect_again();
        }
    }

    /// Gives up the roll call or candidacy this node runs, if any, and moves
    /// to `LeaderSuspect`, to start its next roll call only after a fresh
    /// suspicion timeout, jittered for the latest term it knows of: the
    /// wait gives the election that outpaced it time to finish, and the
    /// jitter keeps rivals that failed together from retrying together.
    pub(super) fn suspect_again(&mut self) {
        self.retry_after_a_fresh_suspicion_timeout();
        self.transition_to(WorkerState::LeaderSuspect);
    }

    pub(super) fn lose_quorum(&mut self) {
        self.retry_after_a_fresh_suspicion_timeout();
        self.transition_to(WorkerState::NoQuorum);
    }

    /// Gives up the roll call, candidacy or recovery this node runs, if any,
    /// and puts its next roll call a fresh suspicion timeout away.
    fn retry_after_a_fresh_suspicion_timeout(&mut self) {
        let retry_at = self.clock.now() + self.jittered_suspect_timeout();
        self.round.retry_at(retry_at);
        self.drop_recovery();
    }
}

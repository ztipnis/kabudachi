//! The roll call this node starts and the candidacy it stands in once that
//! call finds a returning quorum.

use super::roll_call::RollCallRound;
use super::vote_round::VoteRound;
use super::{ElectionRound, Verdict, View};
use crate::configuration::{Admission, Roster};
use crate::coordination_authority::RecoveryEpoch;
use crate::election::standing::{EpochOrder, order};
use crate::protocol::checked::Checked;
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{ElectionCertificate, RollCallReply, VoteGrant};
use crate::time::Instant;

impl ElectionRound {
    /// Starts a roll call for the term after
    /// the latest this node knows of, under its configuration, stamped with
    /// `timestamp_millis` from its wall clock; the node is its first
    /// respondent. The call collects replies until a roll-call deadline
    /// after `now` (see [`Self::on_deadline`]), even one a single voter
    /// wins. A node with no configuration starts none.
    pub(crate) fn begin_roll_call(
        &mut self,
        view: &View,
        timestamp_millis: u64,
        now: Instant,
    ) -> Vec<Verdict> {
        let Some(configuration) = view.configuration else {
            return Vec::new();
        };
        let term = self.next_term(view.highest_term_seen);
        self.hold.clear();
        let round = RollCallRound::start(
            term,
            configuration.clone(),
            timestamp_millis,
            view.me.clone(),
            view.admission,
            now + self.roll_call_span(view),
        );
        self.ballot.record_own_roll_call(term, round.rank().clone());
        let call = round.call(view.shard);
        self.roll_call = Some(round);
        vec![Verdict::Publish(call)]
    }

    /// Records a reply to this node's roll call from `responder`. Ignored
    /// unless it answers that call (same shard, term and initiator) and the
    /// call is not abandoned. While the call runs it counts the respondent
    /// at its deadline (see [`Self::on_deadline`]); once the node stands as
    /// the call's candidate, a new respondent is asked for its vote.
    pub(crate) fn on_roll_call_reply(
        &mut self,
        view: &View,
        responder: WorkerId,
        reply: &Checked<RollCallReply>,
    ) -> Vec<Verdict> {
        if reply.shard_id() != *view.shard || reply.initiator_id() != *view.me {
            return Vec::new();
        }
        let admission = answered_admission(reply);
        if let Some(round) = self.roll_call.as_mut() {
            if round.term() == reply.term {
                round.record(responder, admission, reply.configuration_generation());
            }
            return Vec::new();
        }
        if let Some(vote) = self.vote.as_mut()
            && vote.term() == reply.term
            && vote.record_respondent(responder.clone(), admission, reply.configuration_generation())
        {
            return vec![Verdict::AskVotes {
                voters: vec![responder],
                request: vote.request(view.shard, view.epoch()),
            }];
        }
        Vec::new()
    }

    /// Decides on the roll call or candidacy in progress once its deadline
    /// has come by `now`; before that, or with neither, does nothing.
    ///
    /// A roll call whose returning voters
    /// are a quorum stands the node as its candidate (see
    /// [`Self::stand`]); one short of that leaves it `NoQuorum`. A call it
    /// abandoned for a better one, whose leader has not acked it by now, or
    /// a call for a term it has already seen a vote or leader in, leaves it
    /// suspecting its leader again, as does a candidacy not won by its
    /// deadline.
    pub(crate) fn on_deadline(&mut self, view: &View, now: Instant) -> Vec<Verdict> {
        if let Some(round) = self
            .roll_call
            .as_ref()
            .filter(|round| now >= round.deadline())
        {
            if round.is_abandoned() || round.term() <= view.highest_term_seen {
                return vec![Verdict::SuspectAgain];
            }
            if view.roll_call_is_census || !round.has_returning_quorum() {
                let verdict = Verdict::NoQuorum {
                    term: round.term(),
                    configuration: round.configuration().clone(),
                    respondents: round.respondents().clone(),
                };
                self.no_quorum_streak = self.no_quorum_streak.saturating_add(1);
                return vec![verdict];
            }
            return self.stand(view, now);
        }
        if self
            .vote
            .as_ref()
            .is_some_and(|vote| now >= vote.deadline())
        {
            return vec![Verdict::SuspectAgain];
        }
        Vec::new()
    }

    /// Records `voter`'s vote for this node's candidacy, then checks whether
    /// it has won. Ignored unless the node stands as a candidate, the grant
    /// is for its candidacy (same term, shard and recovery epoch, addressed
    /// to this node), and the voter answered its roll call.
    pub(crate) fn on_vote_grant(
        &mut self,
        view: &View,
        voter: WorkerId,
        grant: &Checked<VoteGrant>,
    ) -> Vec<Verdict> {
        let Some(vote) = self.vote.as_mut() else {
            return Vec::new();
        };
        if !is_grant_for(view, vote.term(), grant) {
            return Vec::new();
        }
        vote.record_grant(voter);
        self.win_if_quorum(view)
    }

    /// Stands as the candidate of this node's roll call: grants itself its
    /// own vote and asks every other respondent for theirs, which must come
    /// in within a roll-call deadline of `now`. A single-voter
    /// configuration is won there and then.
    fn stand(&mut self, view: &View, now: Instant) -> Vec<Verdict> {
        let Some(round) = self.roll_call.take() else {
            return Vec::new();
        };
        let vote = VoteRound::stand(round, now + self.roll_call_span(view));
        let term = vote.term();
        // Its own vote does not raise the highest term seen: a candidacy
        // that lapses unwon leaves no term anyone else voted in, and the
        // leader that outlasted it must still be one this node can follow.
        // The ballot records
        // the vote, so this node answers and grants nothing for this term
        // or an earlier one (see `Self::voter`), and a win raises the term
        // seen as the node takes office.
        self.ballot.record_own_grant(term, view.me);
        let mut verdicts = vec![
            Verdict::Stand { term },
            Verdict::AskVotes {
                voters: vote.voters_to_ask(),
                request: vote.request(view.shard, view.epoch()),
            },
        ];
        self.vote = Some(vote);
        verdicts.extend(self.win_if_quorum(view));
        verdicts
    }

    /// Once this candidacy has won (see the `vote_round` module), founds
    /// what its roll call's respondents found (see
    /// [`Roster::after_election`]): under a single configuration, a joint
    /// one whose new side is the respondents, each admitted at its new
    /// generation, and whose old side is the configuration the roll call ran
    /// under; under a joint configuration not yet committed, that one
    /// re-stamped at a generation of this term and re-based there, each
    /// respondent its new side counted re-admitted at it. It certifies
    /// that configuration, with the respondent's admission generations
    /// there, to every other respondent, and then wins.
    fn win_if_quorum(&mut self, view: &View) -> Vec<Verdict> {
        let Some(vote) = self.vote.take_if(|vote| vote.has_won()) else {
            return Vec::new();
        };
        let census = vote.census();
        let mut roster = Roster::after_election(
            view.epoch(),
            vote.term(),
            census.configuration(),
            census.respondents(),
        );
        roster.seed_held_generations(census.held_generations());
        let mut verdicts: Vec<Verdict> = census
            .respondents()
            .keys()
            .filter(|respondent| *respondent != view.me)
            .map(|respondent| Verdict::Certify {
                respondent: respondent.clone(),
                certificate: certificate_for(view, vote.term(), &roster, respondent),
            })
            .collect();
        self.end_no_quorum_streak();
        verdicts.push(Verdict::Won {
            term: vote.term(),
            roster,
        });
        verdicts
    }
}

/// The admission generations `reply` answered a roll call with.
fn answered_admission(reply: &Checked<RollCallReply>) -> Admission {
    Admission {
        current: reply.admission(),
        prior: reply.prior_admission(),
    }
}

/// Whether `grant` is a vote for this node's candidacy for `term`: for that
/// term, this node's shard and recovery epoch, and addressed to this node.
fn is_grant_for(view: &View, term: u64, grant: &Checked<VoteGrant>) -> bool {
    let epoch = RecoveryEpoch::new(grant.recovery_epoch, grant.recovery_epoch_lineage);
    let own_epoch = view.recovery_epoch.map(|own| order(own, epoch)) == Some(EpochOrder::Mine);
    let addressed = grant.shard_id() == *view.shard && grant.candidate_id() == *view.me;
    grant.term == term && own_epoch && addressed
}

/// The certificate of what this node's win in `term` founded, for `respondent`.
fn certificate_for(
    view: &View,
    term: u64,
    roster: &Roster,
    respondent: &WorkerId,
) -> ElectionCertificate {
    ElectionCertificate {
        shard_id: Some(view.shard.clone().into()),
        recovery_epoch: view.epoch_number(),
        recovery_epoch_lineage: view.epoch().lineage,
        term,
        leader_id: Some(view.me.clone().into()),
        configuration: Some(roster.configuration().into()),
        recipient_admission: roster.admission_of(respondent).map(Into::into),
        recipient_prior_admission: roster.prior_admission_of(respondent).map(Into::into),
    }
}

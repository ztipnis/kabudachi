//! The election a worker takes part in (ADR-0001 decisions 3 to 8): the
//! roll call it starts once it suspects its leader, the roll calls and vote
//! requests of other workers it answers or refuses, the candidacy it stands
//! in once its roll call finds a returning quorum, and the rule by which
//! that candidacy wins. [`ElectionRound`] decides each of these and returns
//! what to do as [`Verdict`]s; `WorkerNode` turns them into messages, state
//! changes and leadership. The roll call (see the `roll_call` module), the
//! vote (see the `vote_round` module) and this node's history as a voter
//! (see the `ballot` module) are private to it.
//!
//! What moved and why. The round owns the fields only the election reads
//! and writes: the ballot, the roll call and vote in progress, when this
//! node may next start a roll call, and until when answering another's
//! holds its own back. What it reads of the node, the node hands it as a
//! [`View`]: its own and its shard's ids, its recovery epoch, the highest
//! term it has seen, its configuration and its admission generations,
//! whether its state takes part in elections, whether its leader contact
//! is fresh, and the roll-call deadline; the time and the wall clock come
//! as arguments. Every change to the node's own fields becomes a verdict:
//! its state and term (`Stand`, `SuspectAgain`, `NoQuorum`, `Won`), its
//! highest term seen (`Grant`), the forced recovery it drops as it calls
//! (`Publish`) or takes up as its call falls short (`NoQuorum`), and every
//! message it sends. A refusal names the node's leader, highest term seen
//! and configuration, which the node attaches as it sends one (`Reject`).

mod ballot;
mod roll_call;
mod vote_round;

use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration, Roster};
use crate::election::standing::{EpochOrder, order_numbers};
use crate::protocol::checked::Checked;
use crate::protocol::ids::{ShardId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    ElectionCertificate, ElectionRejectReason, RollCall, RollCallReply, VoteGrant, VoteRequest,
};
use crate::time::{Duration, Instant};

use ballot::{Ballot, RollCallVerdict, VoteVerdict, Voter};
use roll_call::{CallRank, RollCallRound};
use vote_round::VoteRound;

/// What the round reads of the node deciding, as of the input it handles.
pub(crate) struct View<'a> {
    pub(crate) me: &'a WorkerId,
    pub(crate) shard: &'a ShardId,
    pub(crate) recovery_epoch: u64,
    pub(crate) highest_term_seen: u64,
    /// `None` for a joiner that has accepted no leader ack yet: it starts
    /// no roll call.
    pub(crate) configuration: Option<&'a Configuration>,
    /// The admission generations the node answers a roll call with, and
    /// counts itself by in its own.
    pub(crate) admission: Admission,
    /// Whether the node's state takes part in elections at all.
    pub(crate) takes_part: bool,
    /// Whether the node heard from its leader within its suspicion timeout.
    pub(crate) leader_contact_is_fresh: bool,
    /// The configured base roll-call deadline.
    pub(crate) roll_call_deadline: Duration,
    /// The suspicion timeout, which caps how far backoff widens that
    /// deadline (see [`ElectionRound::roll_call_span`]).
    pub(crate) suspect_timeout: Duration,
}

/// What the node does with what the round decided, in order.
#[derive(Debug)]
pub(crate) enum Verdict {
    /// The node started this roll call: move to `RollCall`, dropping any
    /// forced recovery, and publish it to the shard.
    Publish(RollCall),
    /// Answer `initiator`'s roll call with `reply`.
    Answer {
        initiator: WorkerId,
        reply: RollCallReply,
    },
    /// Refuse `to`'s roll call or vote request for `term`, saying why;
    /// `name_leader` asks the node to name the leader it follows.
    Reject {
        to: WorkerId,
        term: u64,
        reason: ElectionRejectReason,
        name_leader: bool,
    },
    /// The roll call found its returning quorum: move to `Candidate` for
    /// `term`.
    Stand { term: u64 },
    /// Ask each of `voters` for its vote with `request`.
    AskVotes {
        voters: Vec<WorkerId>,
        request: VoteRequest,
    },
    /// Grant `candidate` this vote, having first raised the highest term
    /// seen to its term.
    Grant {
        candidate: WorkerId,
        grant: VoteGrant,
    },
    /// Certify to `respondent` what the won election founded.
    Certify {
        respondent: WorkerId,
        certificate: ElectionCertificate,
    },
    /// The candidacy for `term` won: lead `roster` in that term.
    Won { term: u64, roster: Roster },
    /// The roll call for `term` under `configuration` closed short of a
    /// returning quorum, with these respondents: go `NoQuorum`, and, with
    /// an authority, take the authority path from this census.
    NoQuorum {
        term: u64,
        configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
    },
    /// The roll call or candidacy ended with no one elected: suspect the
    /// leader again, to call again after a fresh suspicion timeout.
    SuspectAgain,
}

/// This node's part in its shard's elections: its history as a voter, the
/// roll call or candidacy it runs, and when it may start its next roll
/// call.
#[derive(Debug, Clone)]
pub(crate) struct ElectionRound {
    /// The roll calls this node answered and the votes it granted.
    ballot: Ballot,
    /// The roll call this node started last, while it collects replies to
    /// it. `Some` only while the node is `RollCall`.
    roll_call: Option<RollCallRound>,
    /// The vote this node runs while it stands as the candidate. `Some`
    /// only while the node is `Candidate` through a roll call of its own.
    vote: Option<VoteRound>,
    /// Until when this node starts no roll call of its own: two base
    /// roll-call deadlines after it last answered another worker's roll
    /// call.
    suppressed_until: Instant,
    /// The earliest instant at which this node, while `LeaderSuspect` or
    /// `NoQuorum`, may start its next roll call: when it began suspecting
    /// its leader, or, after it gave up a term it held or contested or lost
    /// its quorum, a fresh suspicion timeout later.
    next_roll_call_at: Instant,
    /// How many roll calls of this node's own in a row closed `NoQuorum`,
    /// each widening the next one's deadline (see [`Self::roll_call_span`]).
    /// Ends when the node wins, follows a leader or leaves its epoch.
    no_quorum_streak: u32,
}

impl ElectionRound {
    pub(crate) fn new(now: Instant) -> Self {
        ElectionRound {
            ballot: Ballot::default(),
            roll_call: None,
            vote: None,
            suppressed_until: now,
            next_roll_call_at: now,
            no_quorum_streak: 0,
        }
    }

    /// How long this node's next roll call, and the vote that follows it,
    /// run: the base deadline doubled once per roll call of its own in a row
    /// that closed `NoQuorum`, up to the suspicion timeout (never below the
    /// base).
    ///
    /// A reply that misses a deadline is dropped with its term, and each
    /// retry is a new term, so with a fixed deadline shorter than the
    /// shard's round trip, as on a starved host, no call would ever count a
    /// reply: a livelock instead of a late election. Widening bounds that:
    /// once the span passes the round trip the call succeeds. The suspicion
    /// timeout caps it because a node that waits longer for replies than it
    /// takes to suspect its leader is no better off, and
    /// `roll_call_deadline` is documented to stay below it. A node that
    /// retries after a fresh suspicion timeout loses no safety to a long
    /// call: the span moves no lease, only how long this node collects
    /// replies. Answering another's call still holds this node back for two
    /// base deadlines only, so a widened caller can be contested early,
    /// which costs extra calls, never safety.
    fn roll_call_span(&self, view: &View) -> Duration {
        let base = view.roll_call_deadline.as_ticks();
        let cap = view.suspect_timeout.as_ticks().max(base);
        let widened = base
            .checked_shl(self.no_quorum_streak)
            .filter(|widened| widened >> self.no_quorum_streak == base)
            .unwrap_or(u64::MAX);
        Duration::from_ticks(widened.min(cap))
    }

    /// The node won, follows a leader or left its recovery epoch: its roll
    /// calls start again at the base deadline.
    pub(crate) fn end_no_quorum_streak(&mut self) {
        self.no_quorum_streak = 0;
    }

    /// When a `LeaderSuspect` or `NoQuorum` node may start a roll call, as
    /// of `now`: at the instant its suspicion began or its retry falls due,
    /// unless it answered another worker's roll call less than two
    /// roll-call deadlines ago (ADR-0001 decision 5): that worker is being
    /// elected, by its census and then its vote, so the node waits until
    /// both could have ended.
    pub(crate) fn roll_call_due(&self, now: Instant) -> Instant {
        self.next_roll_call_at.max(self.suppressed_until).max(now)
    }

    /// The node began suspecting its leader at `at`: it may call from then.
    pub(crate) fn may_call_from(&mut self, at: Instant) {
        self.next_roll_call_at = at;
    }

    /// Gives up the roll call or candidacy in progress, if any, and puts
    /// the next roll call off until `at`.
    pub(crate) fn retry_at(&mut self, at: Instant) {
        self.stop();
        self.next_roll_call_at = at;
    }

    /// Gives up the roll call or candidacy in progress, if any.
    pub(crate) fn stop(&mut self) {
        self.roll_call = None;
        self.vote = None;
    }

    /// Gives up the roll call or candidacy in progress and every answer and
    /// vote given, as the node leaves its recovery epoch behind.
    pub(crate) fn forget(&mut self) {
        self.stop();
        self.end_no_quorum_streak();
        self.ballot = Ballot::default();
    }

    /// The deadline of the roll call or candidacy in progress: at it,
    /// [`Self::on_deadline`] decides on it.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.roll_call
            .as_ref()
            .map(RollCallRound::deadline)
            .or_else(|| self.vote.as_ref().map(VoteRound::deadline))
    }

    /// Whether the node stands as the candidate of a roll call of its own.
    pub(crate) fn is_standing(&self) -> bool {
        self.vote.is_some()
    }

    /// The term of the roll call in progress, if any.
    pub(crate) fn roll_call_term(&self) -> Option<u64> {
        self.roll_call.as_ref().map(RollCallRound::term)
    }

    /// The workers that have answered the roll call in progress, the node
    /// itself included: none once it gave the call up for a better one.
    pub(crate) fn respondents(&self) -> impl Iterator<Item = &WorkerId> {
        self.roll_call
            .as_ref()
            .filter(|round| !round.is_abandoned())
            .into_iter()
            .flat_map(|round| round.respondents().keys())
    }

    /// The candidate this node granted its vote in `term`, if any.
    pub(crate) fn granted_in(&self, term: u64) -> Option<&WorkerId> {
        self.ballot.granted_in(term)
    }

    /// The latest term this node knows of: `highest_term_seen`, or that of
    /// the latest roll call it accepted, its own included, if later.
    pub(crate) fn latest_term(&self, highest_term_seen: u64) -> u64 {
        highest_term_seen.max(self.ballot.highest_roll_call_term().unwrap_or(0))
    }

    /// Starts a roll call (ADR-0001 decisions 4 and 6) for the term after
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

    /// Decides on a roll call published by `initiator` (see the `ballot`
    /// module for the rules): answers it with the node's admission
    /// generations, refuses it with the reason, or passes over a repeat of
    /// a call it answered. A call for another shard, or its own, is
    /// dropped. A call it answers is electing someone, so the node starts
    /// no roll call of its own until two base roll-call deadlines after
    /// `now`: a repeat of that call, passed over, does not push that back.
    /// That bounds a base-width call only; an initiator whose calls have
    /// widened (see [`Self::roll_call_span`]) can be contested early.
    ///
    /// An initiator that answers a better call for its own term abandons
    /// its own call for it: it stays `RollCall` as that call's respondent,
    /// and never stands as its own call's candidate.
    pub(crate) fn on_roll_call(
        &mut self,
        view: &View,
        initiator: WorkerId,
        call: &Checked<RollCall>,
        now: Instant,
    ) -> Vec<Verdict> {
        if call.shard_id() != *view.shard || initiator == *view.me {
            return Vec::new();
        }
        let voter = self.voter(view);
        let verdict = self.ballot.on_roll_call(
            &voter,
            call.term,
            call.configuration().generation(),
            CallRank::of(call),
        );
        match verdict {
            RollCallVerdict::Answer => {
                // A call of base width closes within a roll-call deadline of
                // this answer, and its candidate's vote within another: a
                // call of this node's own before then would only contest
                // the next term against the worker it is helping elect. A
                // widened call can outlast this, costing the contest a
                // retry, never safety.
                let census_and_vote =
                    Duration::from_ticks(view.roll_call_deadline.as_ticks().saturating_mul(2));
                self.suppressed_until = self.suppressed_until.max(now + census_and_vote);
                if let Some(own) = self.roll_call.as_mut()
                    && own.term() == call.term
                {
                    own.abandon();
                }
                let reply = RollCallReply {
                    shard_id: Some(view.shard.clone().into()),
                    term: call.term,
                    initiator_id: Some(initiator.clone().into()),
                    responder_id: Some(view.me.clone().into()),
                    responder_address: String::new(),
                    admission: view.admission.current.map(Into::into),
                    prior_admission: view.admission.prior.map(Into::into),
                };
                vec![Verdict::Answer { initiator, reply }]
            }
            RollCallVerdict::Reject(reason) => {
                // A caller left on an earlier recovery epoch learns who leads
                // the current one, whose ack then moves it on.
                let name_leader = reason == ElectionRejectReason::LeaderStillValid
                    || order_numbers(
                        view.recovery_epoch,
                        call.configuration().generation().recovery_epoch(),
                    ) == EpochOrder::Stale;
                vec![Verdict::Reject {
                    to: initiator,
                    term: call.term,
                    reason,
                    name_leader,
                }]
            }
            RollCallVerdict::Pass | RollCallVerdict::Drop => Vec::new(),
        }
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
                round.record(responder, admission);
            }
            return Vec::new();
        }
        if let Some(vote) = self.vote.as_mut()
            && vote.term() == reply.term
            && vote.record_respondent(responder.clone(), admission)
        {
            return vec![Verdict::AskVotes {
                voters: vec![responder],
                request: vote.request(view.shard, view.recovery_epoch),
            }];
        }
        Vec::new()
    }

    /// Decides on the roll call or candidacy in progress once its deadline
    /// has come by `now`; before that, or with neither, does nothing.
    ///
    /// A roll call (ADR-0001 decisions 13 and 15) whose returning voters
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
            if !round.has_returning_quorum() {
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

    /// Decides on `candidate`'s request for this node's vote (README §12.6
    /// `on_vote_request`; see the `ballot` module for the rules). A request
    /// for another shard is ignored; every refusal is answered with the
    /// reason.
    pub(crate) fn on_vote_request(
        &mut self,
        view: &View,
        candidate: WorkerId,
        req: &Checked<VoteRequest>,
    ) -> Vec<Verdict> {
        if req.shard_id() != *view.shard {
            return Vec::new();
        }
        let voter = self.voter(view);
        let verdict = self.ballot.on_vote_request(
            &voter,
            req.recovery_epoch,
            req.term,
            req.roll_call_generation(),
            &candidate,
        );
        match verdict {
            VoteVerdict::Grant => {
                let grant = VoteGrant {
                    shard_id: Some(view.shard.clone().into()),
                    recovery_epoch: view.recovery_epoch,
                    term: req.term,
                    candidate_id: Some(candidate.clone().into()),
                    voter_id: Some(view.me.clone().into()),
                };
                vec![Verdict::Grant { candidate, grant }]
            }
            VoteVerdict::Reject(reason) => vec![Verdict::Reject {
                to: candidate,
                term: req.term,
                reason,
                name_leader: reason == ElectionRejectReason::LeaderStillValid,
            }],
        }
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
        if grant.term != vote.term()
            || grant.shard_id() != *view.shard
            || order_numbers(view.recovery_epoch, grant.recovery_epoch) != EpochOrder::Mine
            || grant.candidate_id() != *view.me
        {
            return Vec::new();
        }
        vote.record_grant(voter);
        self.win_if_quorum(view)
    }

    /// What the ballot needs to know of the node to decide on a roll call
    /// or a vote request.
    fn voter(&self, view: &View) -> Voter {
        Voter {
            takes_part: view.takes_part,
            recovery_epoch: view.recovery_epoch,
            // Its own candidacies count here though they raise no term
            // seen: it votes in no term at or below one it stood in.
            highest_term_seen: view
                .highest_term_seen
                .max(self.ballot.highest_granted_term().unwrap_or(0)),
            configuration_generation: view.configuration.map(Configuration::generation),
            leader_contact_is_fresh: view.leader_contact_is_fresh,
        }
    }

    /// The term this node's next roll call contests: the one after the
    /// latest it knows of. A roll call that failed has taken its term, so
    /// the next one contests a later term, where no answer or vote given to
    /// the failed call stands in its way.
    ///
    /// # Panics
    ///
    /// If the latest term is already `u64::MAX`. Decode
    /// ([`crate::protocol::checked::decode`]) refuses that term from any
    /// peer, so reaching it needs a peer bug that names `u64::MAX - 1`, plus
    /// one roll call of this node's own. Wrapping instead would contest term
    /// 0, below every term already seen, which is worse than a panic.
    fn next_term(&self, highest_term_seen: u64) -> u64 {
        self.latest_term(highest_term_seen)
            .checked_add(1)
            .expect("a term overflowed u64::MAX")
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
        // leader that outlasted it must still be one this node can follow
        // (ADR-0001 decision 14 as amended 2026-09-28). The ballot records
        // the vote, so this node answers and grants nothing for this term
        // or an earlier one (see `Self::voter`), and a win raises the term
        // seen as the node takes office.
        self.ballot.record_own_grant(term, view.me);
        let mut verdicts = vec![
            Verdict::Stand { term },
            Verdict::AskVotes {
                voters: vote.voters_to_ask(),
                request: vote.request(view.shard, view.recovery_epoch),
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
    /// there, to every other respondent (README §12.6), and then wins.
    fn win_if_quorum(&mut self, view: &View) -> Vec<Verdict> {
        let Some(vote) = self.vote.take_if(|vote| vote.has_won()) else {
            return Vec::new();
        };
        let census = vote.census();
        let roster = Roster::after_election(
            view.recovery_epoch,
            vote.term(),
            census.configuration(),
            census.respondents(),
        );
        let mut verdicts: Vec<Verdict> = census
            .respondents()
            .keys()
            .filter(|respondent| *respondent != view.me)
            .map(|respondent| Verdict::Certify {
                respondent: respondent.clone(),
                certificate: ElectionCertificate {
                    shard_id: Some(view.shard.clone().into()),
                    recovery_epoch: view.recovery_epoch,
                    term: vote.term(),
                    leader_id: Some(view.me.clone().into()),
                    configuration: Some(roster.configuration().into()),
                    recipient_admission: roster.admission_of(respondent).map(Into::into),
                    recipient_prior_admission: roster
                        .prior_admission_of(respondent)
                        .map(Into::into),
                },
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

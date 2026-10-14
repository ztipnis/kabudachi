//! The election a worker takes part in: the
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
//! node may next start a roll call, and the roll call of another it answered
//! that holds its own back until it resolves. What it reads of the node, the
//! node hands it as a [`View`]: its own and its shard's ids, its recovery
//! epoch, the highest term it has seen, its configuration and its admission
//! generations, whether its state takes part in elections, whether its
//! leader contact is fresh, and the roll-call deadline; the time and the
//! wall clock come as arguments. Every change to the node's own fields
//! becomes a verdict: its state and term (`Stand`, `SuspectAgain`,
//! `NoQuorum`, `Won`), its highest term seen (`Grant`), the forced recovery
//! it drops as it calls (`Publish`) or takes up as its call falls short
//! (`NoQuorum`), and every message it sends. A refusal names the node's
//! leader, highest term seen and configuration, which the node attaches as
//! it sends one (`Reject`).

mod answering;
mod ballot;
mod candidacy;
mod hold;
mod roll_call;
mod vote_round;

use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration, Roster};
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::{ShardId, WorkerId};
use crate::protocol::messages::{
    ElectionCertificate, ElectionRejectReason, RollCall, RollCallReply, VoteGrant, VoteRequest,
};
use crate::time::{Duration, Instant};

use ballot::Ballot;
use hold::RollCallHold;
use roll_call::RollCallRound;
use vote_round::VoteRound;

/// What the round reads of the node deciding, as of the input it handles.
pub(crate) struct View<'a> {
    pub(crate) me: &'a WorkerId,
    pub(crate) shard: &'a ShardId,
    /// `None` before the node first joins a shard.
    pub(crate) recovery_epoch: Option<RecoveryEpoch>,
    pub(crate) highest_term_seen: u64,
    /// `None` for a joiner that has accepted no leader ack yet: it starts
    /// no roll call.
    pub(crate) configuration: Option<&'a Configuration>,
    /// The admission generations the node answers a roll call with, and
    /// counts itself by in its own.
    pub(crate) admission: Admission,
    /// Whether the node's state takes part in elections at all.
    pub(crate) takes_part: bool,
    /// Whether the node's roll call is a census only: the authority holds a
    /// later epoch of the node's lineage than its own, so the roll call,
    /// whatever it returns, never stands the node as a candidate.
    pub(crate) roll_call_is_census: bool,
    /// Whether the node heard from its leader within its suspicion timeout.
    pub(crate) leader_contact_is_fresh: bool,
    /// The configured base roll-call deadline.
    pub(crate) roll_call_deadline: Duration,
    /// The suspicion timeout, which caps how far backoff widens that
    /// deadline (see [`ElectionRound::roll_call_span`]).
    pub(crate) suspect_timeout: Duration,
}

impl View<'_> {
    /// The node's recovery epoch, epoch 0 of lineage 0 before it first joins.
    fn epoch(&self) -> RecoveryEpoch {
        self.recovery_epoch.unwrap_or(RecoveryEpoch::new(0, 0))
    }

    fn epoch_number(&self) -> u64 {
        self.epoch().number
    }
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
    /// The latest roll call of another worker this node answered, while
    /// it holds this node's own roll calls back.
    hold: RollCallHold,
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
            hold: RollCallHold::default(),
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
    /// replies. Answering another's call holds this node back until that
    /// call resolves, or at most two base deadlines and a suspicion timeout:
    /// one such window per answer, at most two per episode plus two base
    /// deadlines. So a widened caller can be contested early, which costs
    /// extra calls, never safety.
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
    /// unless it answered another worker's roll call that has not resolved:
    /// that worker may be winning its election, so a call of this node's
    /// would only contest the next term against it. The node waits for the
    /// leader's ack of that call's term, or, if none comes, until the
    /// call's census, vote and a suspicion timeout for the caller to go
    /// quiet could all have ended. Answers made while a hold is live, from
    /// any caller, do not push it past two such windows from the first,
    /// except that each answer holds the node for at least two base roll-call
    /// deadlines, unless it comes after the episode's end.
    pub(crate) fn roll_call_due(&self, now: Instant) -> Instant {
        let released = self.hold.release_at().unwrap_or(now);
        self.next_roll_call_at.max(released).max(now)
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
        self.hold.clear();
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

    /// The candidate this node granted its vote in `term`, if any.
    pub(crate) fn granted_in(&self, term: u64) -> Option<&WorkerId> {
        self.ballot.granted_in(term)
    }

    /// The latest term this node knows of: `highest_term_seen`, or that of
    /// the latest roll call it accepted, its own included, or refused as
    /// counted against an older configuration than its own, if later.
    pub(crate) fn latest_term(&self, highest_term_seen: u64) -> u64 {
        highest_term_seen
            .max(self.ballot.highest_roll_call_term().unwrap_or(0))
            .max(self.ballot.stale_call_term().unwrap_or(0))
    }

    /// The node now follows a leader of `term`: the roll calls it answered or
    /// made for later terms, short of those it voted in, are forgotten (see
    /// `Ballot::forget_calls_above`), and the roll call it answered no longer
    /// holds the node's own roll calls back: one of an earlier term has
    /// resolved, and one of a later term was disproved by the live leader.
    /// Following a leader also pays a call the node was owed after an
    /// episode's cap.
    pub(crate) fn follow_leader_of(&mut self, term: u64) {
        self.ballot.forget_calls_above(term);
        self.hold.clear();
    }

    /// The term this node's next roll call contests: the one after the
    /// latest it knows of. A roll call that failed has taken its term, so
    /// the next one contests a later term, where no answer or vote given to
    /// the failed call stands in its way; a call, or an answer to another's
    /// call, that a leader's ack outlived has not (see
    /// [`Self::follow_leader_of`]).
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
}

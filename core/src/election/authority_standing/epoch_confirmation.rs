//! What a member's latest read of the authority's recovery epoch says of its
//! own: whether it may stand for election.
//!
//! A member of a shard with an authority elects only at the epoch the
//! authority holds. So it asks the authority for its epoch once it suspects
//! its leader, and stands only while the answer to its latest read is its own
//! epoch, number and lineage, or the authority holds none (flushed), when the
//! plain rule applies and it stands at its own. An answer naming a later epoch
//! of its own lineage, which a swap whose reply was lost leaves with no
//! leader, lets it stand at its own as well, but only to roll a call that
//! gathers respondents: however many answer, its authority path then counts
//! them against the live registrations and swaps from the later epoch, and it
//! never leads at its own epoch, which the authority refuses a fence for. If
//! that census is refused, again and again, because other live workers
//! outnumber its respondents, a node still behind the authority rejoins at the
//! authority's epoch, which also takes it out of the count that kept the path
//! from swapping; a few refusals it stands through, for workers that called
//! their rolls apart find one another on the next. An answer naming any other
//! epoch sends it to rejoin there. A read that failed confirms nothing, and is
//! asked again a renewal interval later. A read still unanswered a TTL after
//! it was asked is asked again, for the call may have been lost on its way,
//! and the answer to any read asked since the node began suspecting its leader
//! counts: a slow authority is waited for, not raced. The confirmation does
//! not expire by a clock: it ends when the node leaves the epoch or the
//! membership, or calls a roll call, which spends it, so a node whose attempt
//! failed reads the authority again before the next. A candidate and a
//! follower keep it unspent, so a follower that suspects its leader again may
//! stand on it until its epoch changes.

use crate::coordination_authority::{AuthorityError, RecoveryEpoch};
use crate::election::authority::ReplyToken;
use crate::election::standing::{EpochOrder, order};
use crate::time::{Duration, Instant};

/// What an answer to the latest read confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Confirmed {
    /// The authority held this epoch.
    Epoch(RecoveryEpoch),
    /// The authority held none.
    Empty,
    /// The authority held a later epoch of the node's own lineage, which the
    /// node may stand beside, but never lead at.
    Behind,
}

/// How many census refusals in a row a node stands through before it rejoins
/// the later epoch it stands beside: workers that rolled their calls apart
/// each draw fewer respondents than the live count, and must get another call
/// to find one another before the node gives up on a leaderless epoch.
const REFUSALS_BEFORE_REJOINING: u32 = 3;

/// What the node does with an answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Answer {
    /// Not the answer to the latest read, or a failed one: nothing changes.
    Ignored,
    /// The answer confirmed the node's epoch, found the authority empty, or
    /// named a later epoch of the node's lineage while the node has been
    /// refused fewer than `REFUSALS_BEFORE_REJOINING` times in a census.
    Confirmed,
    /// The authority holds another epoch, or a later one of the node's
    /// lineage after the node was refused `REFUSALS_BEFORE_REJOINING` times
    /// in a census: rejoin there.
    RejoinAt(RecoveryEpoch),
}

/// A member's reads of the authority's epoch, and what the latest answer
/// confirmed.
#[derive(Debug, Default)]
pub(super) struct EpochConfirmation {
    /// The tokens of the reads asked and not answered; an answer to any other
    /// is dropped. An answer takes them all: it is newer than every read
    /// asked before it.
    awaited: Vec<ReplyToken>,
    /// When the next read may be asked: a TTL after one was asked, or a
    /// renewal interval after one failed; `None` before any was.
    next_read_at: Option<Instant>,
    confirmed: Option<Confirmed>,
    /// How many times the node's authority path found the shard's live
    /// registrations outnumber the respondents its roll call drew, since it last
    /// left an epoch, stopped suspecting its leader, or read the authority at
    /// its own epoch or empty.
    refusals: u32,
    /// Whether the roll call the node called last is a census only.
    census: bool,
}

impl EpochConfirmation {
    /// Whether a member at `own` may stand: its latest read confirmed that
    /// epoch, found the authority empty, or found a later epoch of the node's
    /// lineage (`Behind`), which it may stand beside only to roll a census.
    pub(super) fn permits(&self, own: RecoveryEpoch) -> bool {
        match self.confirmed {
            Some(Confirmed::Epoch(held)) => held == own,
            Some(Confirmed::Empty | Confirmed::Behind) => true,
            None => false,
        }
    }

    /// Whether the node's roll call in flight only gathers respondents for
    /// the authority path: it was called standing beside a later epoch.
    pub(super) fn is_census(&self) -> bool {
        self.census
    }

    /// The node called a roll call: it spends its confirmation, and the call
    /// is a census if the confirmation was of a later epoch.
    pub(super) fn spend_on_roll_call(&mut self) {
        let census = self.confirmed == Some(Confirmed::Behind);
        let refusals = self.refusals;
        self.clear();
        self.census = census;
        self.refusals = refusals;
    }

    /// The roll call is over.
    pub(super) fn end_census(&mut self) {
        self.census = false;
    }

    /// Whether the node asks for a read at `now`: it holds no confirmation,
    /// and none was asked within the last interval.
    pub(super) fn read_due(&self, now: Instant) -> bool {
        self.confirmed.is_none() && self.next_read_at.is_none_or(|at| now >= at)
    }

    /// When the next read is due, for a node that has none confirmed. `None`
    /// while it holds one, or before it has asked any.
    pub(super) fn next_read_at(&self) -> Option<Instant> {
        self.confirmed.is_none().then_some(self.next_read_at).flatten()
    }

    /// A read named `token` was asked at `now`; if it is still unanswered
    /// `lost_after` later, the next is asked, beside it.
    pub(super) fn read_asked(&mut self, token: ReplyToken, now: Instant, lost_after: Duration) {
        self.awaited.push(token);
        self.next_read_at = Some(now + lost_after);
    }

    /// Whether `token` names a read asked and not yet answered.
    pub(super) fn is_awaiting(&self, token: ReplyToken) -> bool {
        self.awaited.contains(&token)
    }

    /// Takes the answer `result` to the read named `token`, for a node at
    /// `own`, at `now`. The first answer to any awaited read counts, once; a
    /// failed one is followed by the next read `interval` later.
    pub(super) fn answered(
        &mut self,
        token: ReplyToken,
        result: Result<Option<RecoveryEpoch>, AuthorityError>,
        own: RecoveryEpoch,
        now: Instant,
        interval: Duration,
    ) -> Answer {
        if !self.is_awaiting(token) {
            return Answer::Ignored;
        }
        self.awaited.clear();
        match result {
            Err(_) => {
                self.next_read_at = Some(now + interval);
                Answer::Ignored
            }
            Ok(None) => {
                // Recovering an emptied authority after a lost swap needs a
                // catastrophic-reset procedure that is not implemented yet:
                // an empty answer says nothing of an epoch such a swap may
                // have made, so the plain rule stands this node at its own.
                self.confirmed = Some(Confirmed::Empty);
                self.refusals = 0;
                Answer::Confirmed
            }
            Ok(Some(held)) if order(&own, held.into()) == EpochOrder::Mine => {
                // Refusals of roll calls at an epoch the authority holds say
                // nothing of a leaderless later one.
                self.confirmed = Some(Confirmed::Epoch(held));
                self.refusals = 0;
                Answer::Confirmed
            }
            Ok(Some(held))
                if self.refusals < REFUSALS_BEFORE_REJOINING
                    && held.lineage == own.lineage
                    && order(&own, held.into()) == EpochOrder::Later =>
            {
                self.confirmed = Some(Confirmed::Behind);
                Answer::Confirmed
            }
            Ok(Some(held)) => Answer::RejoinAt(held),
        }
    }

    /// The node's authority path was refused for lack of a majority of the
    /// live registrations. Refusals of ordinary authority paths, not only of
    /// census reads, count too, so a node can rejoin after fewer than
    /// `REFUSALS_BEFORE_REJOINING` census refusals.
    pub(super) fn outvoted(&mut self) {
        self.refusals = self.refusals.saturating_add(1);
    }

    /// Gives up the read in flight and its pacing, keeping the confirmation:
    /// the node is no longer suspecting its leader.
    pub(super) fn stop_reading(&mut self) {
        self.awaited.clear();
        self.next_read_at = None;
        self.refusals = 0;
        self.census = false;
    }

    /// Forgets everything: the node left the epoch, or its membership.
    pub(super) fn clear(&mut self) {
        *self = EpochConfirmation::default();
    }
}

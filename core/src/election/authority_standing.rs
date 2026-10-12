//! A node's standing with its coordination authority: its registration and
//! fence timers, the forced recovery it runs, how it reconnects once fenced,
//! and the one reply it awaits.
//!
//! [`AuthorityStanding`] owns the fields only the authority path reads and
//! writes: the lease (see the `authority_lease` module), the recovery in
//! progress (see the `forced_recovery` module), the token of the reply the
//! node waits on, and the mint every authority call's token comes from. What
//! it reads of the node, the node hands it as an [`AuthorityView`]; the time
//! comes as an argument. Every change to the node's own fields becomes an
//! [`AuthorityVerdict`], which `WorkerNode` maps to a transition, as it does
//! for `ElectionRound`'s verdicts. The standing numbers, and awaits where it
//! must, every call it asks, so the node never builds an authority call.

mod authority_lease;
mod due_calls;
mod epoch_confirmation;
mod fence;
mod forced_recovery;
mod recovery_path;
mod replies;

use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration, Roster};
use crate::coordination_authority::RecoveryEpoch;
use crate::election::authority::{
    AuthorityCall, AuthorityRequest, AuthorityTimings, Issuer, ReplyToken,
    ReplyTokens,
};
use crate::protocol::ids::{ShardId, WorkerId};
use crate::protocol::worker_state::WorkerState;
use crate::time::Instant;

use authority_lease::AuthorityLease;
use epoch_confirmation::EpochConfirmation;
use forced_recovery::ForcedRecovery;

/// A node's standing with its coordination authority.
pub(crate) struct AuthorityStanding {
    lease: AuthorityLease,
    /// The authority-path attempt in progress, from the census of the roll
    /// call that fell short, while `NoQuorum` or, waiting out the fence,
    /// `Candidate`.
    recovery: Option<ForcedRecovery>,
    /// When the node gives up `recovery` if it is still running, so a call of
    /// it the authority never answers does not park the node `NoQuorum`.
    recovery_gives_up_at: Option<Instant>,
    /// When the swap of `recovery` was asked, while it waits on its reply.
    swap_asked_at: Option<Instant>,
    /// The token of the one authority read or swap this node now waits on:
    /// its forced recovery's current step, or, while `Fenced`, its read of
    /// the recovery epoch. Replies arrive whenever the driver gets them,
    /// possibly out of order, so a reply to any earlier call is stale and
    /// ignored.
    awaited: Option<ReplyToken>,
    /// What this node's latest read of the authority's epoch confirmed, while
    /// it suspects its leader: whether it may stand for election.
    confirmation: EpochConfirmation,
    /// While in office, a fence reply found no record under the name and this
    /// node's republish has not landed: it publishes no hint until it has.
    record_lost: bool,
    /// Took office, and publishes its hint at its next `calls_due`.
    hint_now: bool,
    /// `ReplyTokens::new(Issuer::Node)`, made once in `starting_at`. The
    /// standing is never rebuilt (`join` and `registered_at` call
    /// `restart_at`), so the node never repeats a number.
    tokens: ReplyTokens,
}

/// What the standing reads of the node, as of the reply or step it handles.
pub(crate) struct AuthorityView<'a> {
    pub(crate) me: &'a WorkerId,
    /// The incarnation of the shard the node belongs to.
    pub(crate) shard: &'a ShardId,
    pub(crate) state: WorkerState,
    /// The node's current term.
    pub(crate) term: u64,
    /// The node's recovery epoch; `None` before its first join.
    pub(crate) own_epoch: Option<RecoveryEpoch>,
}

impl AuthorityView<'_> {
    /// Whether the node holds office, reconciling or leading.
    fn in_office(&self) -> bool {
        matches!(
            self.state,
            WorkerState::LeaderReconciling | WorkerState::Leader
        )
    }
}

/// What the node does with a reply, in order.
#[derive(Debug)]
pub(crate) enum AuthorityVerdict {
    /// Ask the authority this (already numbered, and awaited if it must be).
    Ask(AuthorityCall),
    /// Fenced, it finds its own epoch: resume `Active`.
    Resume,
    /// Leave for the shard the authority holds at this epoch.
    RejoinAt(RecoveryEpoch),
    /// The recovery swapped the epoch: stand as `Candidate` for `term` at
    /// `epoch`, leading `roster` once the fence comes.
    StandAt {
        epoch: RecoveryEpoch,
        term: u64,
        roster: Roster,
    },
    /// The fence came: lead the recovered roster.
    Lead(Roster),
    /// The shard is gone: the authority holds no record of it, or a record of
    /// another incarnation. The shard is abandoned.
    Abandon,
    LoseQuorum,
    SuspectAgain,
}

impl AuthorityStanding {
    /// The standing of a node that starts at `now` (see
    /// `AuthorityLease::starting_at`).
    pub(crate) fn starting_at(
        timings: AuthorityTimings,
        drift_divisor: u64,
        now: Instant,
    ) -> Self {
        AuthorityStanding {
            lease: AuthorityLease::starting_at(timings, drift_divisor, now),
            recovery: None,
            recovery_gives_up_at: None,
            swap_asked_at: None,
            awaited: None,
            confirmation: EpochConfirmation::default(),
            record_lost: false,
            hint_now: false,
            tokens: ReplyTokens::new(Issuer::Node),
        }
    }

    /// Starts the lease over at `now`: for a node that has just joined its
    /// shard, or whose registration was asked for before it was built.
    pub(crate) fn restart_at(&mut self, now: Instant) {
        self.lease.restart_at(now);
    }

    /// Whether a member at `own` may stand for election: its latest read of
    /// the authority's epoch confirmed `own`, or found the authority empty.
    pub(crate) fn permits_standing(&self, own: RecoveryEpoch) -> bool {
        self.confirmation.permits(own)
    }

    /// Whether the node's roll call in flight is a census of respondents for
    /// the authority path, not an election: the node called it standing beside
    /// a later epoch of its own lineage.
    pub(crate) fn roll_call_is_census(&self) -> bool {
        self.confirmation.is_census()
    }

    /// The node left the epoch it was confirmed at: its confirmation, and any
    /// read it waits on, are void.
    pub(crate) fn epoch_left(&mut self) {
        self.confirmation.clear();
    }


    pub(crate) fn is_registered(&self, now: Instant) -> bool {
        self.lease.is_registered(now)
    }

    /// Until when the node's fence lets it act; `None` while it holds none.
    pub(crate) fn fence_valid_until(&self) -> Option<Instant> {
        self.lease.fence_valid_until()
    }

    /// The recovery epoch of the office this node took, fence granted or not,
    /// for as long as it leads.
    pub(crate) fn office_epoch(&self) -> Option<RecoveryEpoch> {
        self.lease.office_epoch()
    }

    /// The node moved to `next`: outside `Candidate`, `LeaderReconciling`
    /// and `Leader` it gives up any fence.
    ///
    /// Only a node that is a member of its shard keeps what its latest read
    /// of the authority's epoch confirmed. A node that suspects its leader
    /// keeps it, and so does a candidate or a follower, which no longer waits
    /// on a read: a follower that suspects again may stand on it until its
    /// epoch changes. A roll call spends it, so an attempt that fails and
    /// calls again needs a read of its own.
    pub(crate) fn state_changed(&mut self, next: WorkerState) {
        if !matches!(
            next,
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader
        ) {
            self.lease.drop_fence();
        }
        if !matches!(next, WorkerState::LeaderReconciling | WorkerState::Leader) {
            self.record_lost = false;
            self.hint_now = false;
        }
        match next {
            WorkerState::LeaderSuspect | WorkerState::NoQuorum => self.confirmation.end_census(),
            WorkerState::RollCall => self.confirmation.spend_on_roll_call(),
            WorkerState::Candidate | WorkerState::Active => self.confirmation.stop_reading(),
            _ => self.confirmation.clear(),
        }
    }

    /// The node took office at `now`, leading `epoch`: it needs a fence unless
    /// it holds one, and leads `epoch` from now on.
    pub(crate) fn took_office(&mut self, epoch: Option<RecoveryEpoch>, now: Instant) {
        self.lease.took_office(epoch, now);
        self.hint_now = true;
    }

    /// A roll call of `term` under `configuration` fell short with these
    /// `respondents`: starts the authority path, which it gives up at
    /// `give_up_at` if it has not finished, and returns its first call, the
    /// read of the shard's live registrations.
    pub(crate) fn begin_recovery(
        &mut self,
        term: u64,
        configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
        now: Instant,
        give_up_at: Instant,
    ) -> AuthorityCall {
        self.recovery = Some(ForcedRecovery::start(term, configuration, respondents));
        self.recovery_gives_up_at = Some(give_up_at);
        self.ask_awaited(AuthorityRequest::ReadLiveRegistrations, now)
    }

    /// Gives up the authority path in progress, if any.
    pub(crate) fn drop_recovery(&mut self) {
        self.recovery = None;
    }

    /// The node left its shard for another: it awaits nothing and runs no
    /// recovery.
    pub(crate) fn rejoined(&mut self) {
        self.awaited = None;
        self.recovery = None;
    }


    /// Asks for `request` at `now`, with a fresh token.
    fn ask(&mut self, request: AuthorityRequest, now: Instant) -> AuthorityCall {
        AuthorityCall::new(request, &mut self.tokens, now)
    }

    /// Asks for `request` as the one read or swap this node now waits on
    /// (see `awaited`).
    fn ask_awaited(&mut self, request: AuthorityRequest, now: Instant) -> AuthorityCall {
        let call = self.ask(request, now);
        self.awaited = Some(call.token);
        call
    }

    /// Whether `token` is the one this node waits on, and if it is, stops
    /// waiting. Whole-token equality: a reply of another issuer, kind or
    /// number never empties the slot.
    fn take_awaited(&mut self, token: ReplyToken) -> bool {
        self.awaited.take_if(|awaited| *awaited == token).is_some()
    }
}

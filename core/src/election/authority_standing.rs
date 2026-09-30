//! A node's standing with its coordination authority (ADR-0001 decisions 11
//! and 12): its registration and fence timers, the forced recovery it runs,
//! how it reconnects once fenced, and the one reply it awaits.
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
mod forced_recovery;

use std::collections::BTreeMap;

use crate::configuration::{Admission, Configuration, Roster};
use crate::coordination_authority::{AuthorityError, LiveRegistrations, RecoveryEpoch};
use crate::election::authority::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, Issuer, ReplyToken,
    ReplyTokens,
};
use crate::election::standing::{EpochOrder, order};
use crate::protocol::ids::WorkerId;
use crate::protocol::worker_state::WorkerState;
use crate::time::{Duration, Instant};

use authority_lease::{AuthorityLease, Reconnect};
use forced_recovery::{ForcedRecovery, Next, cannot_recover_from};

/// A node's standing with its coordination authority.
pub(crate) struct AuthorityStanding {
    lease: AuthorityLease,
    /// The authority-path attempt in progress, from the census of the roll
    /// call that fell short, while `NoQuorum` or, waiting out the fence,
    /// `Candidate`.
    recovery: Option<ForcedRecovery>,
    /// The token of the one authority read or swap this node now waits on:
    /// its forced recovery's current step, or, while `Fenced`, its read of
    /// the recovery epoch. Replies arrive whenever the driver gets them,
    /// possibly out of order, so a reply to any earlier call is stale and
    /// ignored.
    awaited: Option<ReplyToken>,
    /// `ReplyTokens::new(Issuer::Node)`, made once in `starting_at`. The
    /// standing is never rebuilt (`join` and `registered_at` call
    /// `restart_at`), so the node never repeats a number.
    tokens: ReplyTokens,
}

/// What the standing reads of the node, as of the reply or step it handles.
pub(crate) struct AuthorityView<'a> {
    pub(crate) me: &'a WorkerId,
    pub(crate) state: WorkerState,
    /// The node's recovery epoch; `None` before its first join.
    pub(crate) own_epoch: Option<RecoveryEpoch>,
}

/// What the node does with a reply, in order.
#[derive(Debug)]
pub(crate) enum AuthorityVerdict {
    /// Ask the authority this (already numbered, and awaited if it must be).
    Ask(AuthorityCall),
    /// Fenced, it finds its own epoch: resume `Active`.
    Resume,
    /// Leave for the shard the authority holds at this epoch (decision 12).
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
    /// The recovery epoch is gone: the shard is abandoned.
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
            awaited: None,
            tokens: ReplyTokens::new(Issuer::Node),
        }
    }

    /// Starts the lease over at `now`: for a node that has just joined its
    /// shard, or whose registration was asked for before it was built.
    pub(crate) fn restart_at(&mut self, now: Instant) {
        self.lease.restart_at(now);
    }

    /// Whether a node in `state` keeps a registration with its authority: a
    /// fenced node keeps registering, to reconnect.
    fn registers_in(state: WorkerState) -> bool {
        !matches!(
            state,
            WorkerState::Bootstrapping
                | WorkerState::Joining
                | WorkerState::Draining
                | WorkerState::Stopped
        )
    }

    /// The registration and fence renewals this node's lease has come due
    /// for at `now`, for a node in `view.state`: a registration, from every
    /// state that keeps one, and, while it needs one, its recovery fence.
    pub(crate) fn calls_due(&mut self, view: &AuthorityView, now: Instant) -> Vec<AuthorityCall> {
        let register = Self::registers_in(view.state) && self.lease.registration_due(now);
        if register {
            self.lease.registration_asked(now);
        }
        // A node that has joined no shard has no epoch to hold a fence at,
        // and never needs one.
        let fence = view.own_epoch.filter(|_| self.lease.fence_due(now));
        if fence.is_some() {
            self.lease.fence_asked(now);
        }
        let mut calls = Vec::new();
        if register {
            calls.push(self.ask(AuthorityRequest::Register, now));
        }
        if let Some(recovery_epoch) = fence {
            calls.push(self.ask(AuthorityRequest::AcquireFence { recovery_epoch }, now));
        }
        calls
    }

    /// The earliest instant at which the lease wants something done for a
    /// node in `state`: a registration or fence attempt, and, until it is
    /// `Fenced`, fencing itself. `None` in a state that keeps no
    /// registration.
    pub(crate) fn next_deadline(&self, state: WorkerState) -> Option<Instant> {
        Self::registers_in(state).then(|| self.lease.next_deadline(state == WorkerState::Fenced))
    }

    pub(crate) fn is_registered(&self, now: Instant) -> bool {
        self.lease.is_registered(now)
    }

    /// Until when the node's fence lets it act; `None` while it holds none.
    pub(crate) fn fence_valid_until(&self) -> Option<Instant> {
        self.lease.fence_valid_until()
    }

    /// The node moved to `next`: outside `Candidate`, `LeaderReconciling`
    /// and `Leader` it gives up any fence.
    pub(crate) fn state_changed(&mut self, next: WorkerState) {
        if !matches!(
            next,
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader
        ) {
            self.lease.drop_fence();
        }
    }

    /// The node took office at `now`: it needs a fence unless it holds one.
    pub(crate) fn took_office(&mut self, now: Instant) {
        if self.lease.fence_valid_until().is_none() {
            self.lease.need_fence(now);
        }
    }

    /// A roll call of `term` under `configuration` fell short with these
    /// `respondents`: starts the authority path and returns its first call,
    /// the read of the shard's live registrations.
    pub(crate) fn begin_recovery(
        &mut self,
        term: u64,
        configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
        now: Instant,
    ) -> AuthorityCall {
        self.recovery = Some(ForcedRecovery::start(term, configuration, respondents));
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

    /// Handles what the authority answered to a call this node asked for,
    /// as of `now`, and returns what the node does about it, in order.
    pub(crate) fn on_reply(
        &mut self,
        reply: AuthorityReply,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        match reply {
            AuthorityReply::Registered {
                sent_at, result, ..
            } => {
                let Ok(granted) = result else {
                    return Vec::new();
                };
                self.lease.registered(sent_at, granted);
                // A fenced node that can register again reads the epoch to
                // learn whether it may resume (ADR-0001 decision 12).
                if view.state == WorkerState::Fenced {
                    vec![AuthorityVerdict::Ask(
                        self.ask_awaited(AuthorityRequest::ReadRecoveryEpoch, now),
                    )]
                } else {
                    Vec::new()
                }
            }
            AuthorityReply::LiveRegistrations { token, result, .. } => {
                if self.take_awaited(token) {
                    self.on_live_registrations(result, view, now)
                } else {
                    Vec::new()
                }
            }
            AuthorityReply::RecoveryEpoch { token, result, .. } => {
                if !self.take_awaited(token) {
                    Vec::new()
                } else if view.state == WorkerState::Fenced {
                    match result {
                        Ok(epoch) => self.reconnect(epoch, view, now),
                        Err(_) => Vec::new(),
                    }
                } else {
                    self.on_recovery_epoch(result, view, now)
                }
            }
            AuthorityReply::RecoveryEpochSwapped {
                token,
                expected,
                new,
                result,
                ..
            } => {
                let awaited = self.take_awaited(token);
                self.on_recovery_epoch_swapped(expected, new, awaited, result, view, now)
            }
            AuthorityReply::Fence {
                recovery_epoch,
                sent_at,
                result,
                ..
            } => self.on_fence(recovery_epoch, sent_at, result, view, now),
        }
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

    /// A fenced node that can reach its authority again, which reports the
    /// shard's recovery epoch as `authority_epoch`, resumes, rejoins or stays
    /// fenced (see [`Reconnect`]). Rejoining discards everything it knew of
    /// the shard, for the node to join again.
    fn reconnect(
        &mut self,
        authority_epoch: Option<RecoveryEpoch>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if !self.lease.is_registered(now) {
            return Vec::new();
        }
        match Reconnect::decide(view.own_epoch, authority_epoch) {
            Reconnect::Resume => vec![AuthorityVerdict::Resume],
            Reconnect::Rejoin(epoch) => vec![AuthorityVerdict::RejoinAt(epoch)],
            Reconnect::StayFenced => Vec::new(),
        }
    }

    fn on_live_registrations(
        &mut self,
        result: Result<LiveRegistrations, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if view.state != WorkerState::NoQuorum {
            return Vec::new();
        }
        let Some(recovery) = self.recovery.as_mut() else {
            return Vec::new();
        };
        let next = match result {
            // A node the authority does not list as live has no standing
            // to recover the shard on the authority's count.
            Ok(live) if live.addresses().contains_key(view.me) => {
                recovery.on_live_registrations(&live)
            }
            _ => Next::GiveUp,
        };
        self.follow_recovery(next, now)
    }

    fn on_recovery_epoch(
        &mut self,
        result: Result<Option<RecoveryEpoch>, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if view.state != WorkerState::NoQuorum {
            return Vec::new();
        }
        let Some(recovery) = self.recovery.as_mut() else {
            return Vec::new();
        };
        let next = match result {
            Ok(epoch) => recovery.on_recovery_epoch(epoch, view.own_epoch),
            Err(_) => Next::GiveUp,
        };
        self.follow_recovery(next, now)
    }

    /// A compare-and-swap of the recovery epoch came back: either this
    /// node's authority path, or a leader republishing its epoch after the
    /// authority lost it (README §15.3), which then asks for its fence
    /// again at once.
    fn on_recovery_epoch_swapped(
        &mut self,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        awaited: bool,
        result: Result<(), AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if view.state == WorkerState::NoQuorum
            && let Some(recovery) = self.recovery.as_mut()
        {
            if !awaited {
                return Vec::new();
            }
            let next = recovery.on_swapped(expected, new, result.is_ok());
            return self.follow_recovery(next, now);
        }
        // The leader's republish (README §15.3): once the epoch is back, by
        // this swap or another worker's, it asks for its fence at once.
        // Otherwise it asks when the fence is next due, so an authority that
        // keeps failing is not asked again within the same instant.
        let republished = match &result {
            Ok(()) => true,
            Err(AuthorityError::EpochConflict { current }) => {
                current.is_some_and(|current| order(&new, current.into()) == EpochOrder::Mine)
            }
            Err(_) => false,
        };
        if view.state == WorkerState::Leader
            && expected.is_none()
            && view.own_epoch.map(|own| order(&own, new.into())) == Some(EpochOrder::Mine)
            && republished
        {
            self.lease.retry_fence_at(now);
        }
        Vec::new()
    }

    /// Carries a recovery on as `next` says.
    fn follow_recovery(&mut self, next: Next, now: Instant) -> Vec<AuthorityVerdict> {
        match next {
            Next::ReadEpoch => vec![AuthorityVerdict::Ask(
                self.ask_awaited(AuthorityRequest::ReadRecoveryEpoch, now),
            )],
            Next::Swap { from, to } => vec![AuthorityVerdict::Ask(self.ask_awaited(
                AuthorityRequest::SwapRecoveryEpoch {
                    expected: Some(from),
                    new: to,
                },
                now,
            ))],
            Next::AwaitFence { epoch } => self.stand_through_authority(epoch, now),
            Next::Abandon => {
                self.recovery = None;
                vec![AuthorityVerdict::Abandon]
            }
            Next::Rejoin(epoch) => vec![AuthorityVerdict::RejoinAt(epoch)],
            Next::GiveUp => {
                self.recovery = None;
                Vec::new()
            }
        }
    }

    /// The authority path swapped the recovery epoch to `epoch`: the node
    /// adopts it, and the configuration its recovery founds there, stands as
    /// `Candidate` for its roll call's term, and asks for the fence, which
    /// it must hold before it leads (ADR-0001 decision 11.4).
    fn stand_through_authority(&mut self, epoch: RecoveryEpoch, now: Instant) -> Vec<AuthorityVerdict> {
        let Some(recovery) = self.recovery.as_ref() else {
            return Vec::new();
        };
        let Some(roster) = recovery.founded_roster() else {
            return Vec::new();
        };
        let term = recovery.term();
        self.lease.need_fence(now);
        vec![AuthorityVerdict::StandAt {
            epoch,
            term,
            roster,
        }]
    }

    /// Handles the authority's answer to this node's request for the
    /// recovery fence at `epoch`, asked at `sent_at`. Only a `Leader`, or a
    /// `Candidate` waiting out the fence after its authority path, holds or
    /// seeks one; an answer for another epoch than its own is stale.
    ///
    /// - Granted: the fence lets it act until a TTL, less drift, after it
    ///   asked. A waiting candidate now leads.
    /// - Held by another worker: it asks again once that fence has run out.
    /// - The epoch is missing (the authority lost its data): a leader
    ///   republishes it (README §15.3) and asks again; a waiting candidate
    ///   gives up, its swap lost with the data.
    /// - The epoch has moved on: the shard was recovered without it. A
    ///   leader steps down, and a waiting candidate gives up; either
    ///   adopts the new epoch from its leader's ack.
    /// - Unavailable: it asks again at its next renewal.
    fn on_fence(
        &mut self,
        epoch: RecoveryEpoch,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        let seeking = match view.state {
            WorkerState::Leader => true,
            WorkerState::Candidate => self
                .recovery
                .as_ref()
                .is_some_and(ForcedRecovery::is_awaiting_fence),
            _ => false,
        };
        let own_epoch_is_it = view.own_epoch.map(|own| order(&own, epoch.into())) == Some(EpochOrder::Mine);
        if !seeking || !own_epoch_is_it {
            return Vec::new();
        }
        match result {
            Ok(granted) => {
                self.lease.fence_acquired(sent_at, granted);
                if view.state == WorkerState::Candidate {
                    self.lead_recovered()
                } else {
                    Vec::new()
                }
            }
            Err(AuthorityError::FenceHeld { remaining }) => {
                self.lease
                    .retry_fence_at(now + remaining + Duration::from_ticks(1));
                Vec::new()
            }
            Err(AuthorityError::EpochConflict { current: None })
                if view.state == WorkerState::Leader =>
            {
                vec![AuthorityVerdict::Ask(self.ask(
                    AuthorityRequest::SwapRecoveryEpoch {
                        expected: None,
                        new: epoch,
                    },
                    now,
                ))]
            }
            // An epoch this node cannot recover from: it rejoins the shard
            // at it, as a reconnecting fenced node and a `NoQuorum` node's
            // recovery do, rather than win again and meet it again.
            Err(AuthorityError::EpochConflict {
                current: Some(held),
            }) if cannot_recover_from(view.own_epoch, held) => {
                vec![AuthorityVerdict::LoseQuorum, AuthorityVerdict::RejoinAt(held)]
            }
            Err(AuthorityError::EpochConflict { .. }) => {
                if view.state == WorkerState::Leader {
                    vec![AuthorityVerdict::SuspectAgain]
                } else {
                    vec![AuthorityVerdict::LoseQuorum]
                }
            }
            Err(AuthorityError::Unavailable) => Vec::new(),
        }
    }

    /// The candidate holds the fence its authority path waited for: it leads
    /// the configuration that path founded, a roster of its counted
    /// respondents, each admitted at the founded generation.
    fn lead_recovered(&mut self) -> Vec<AuthorityVerdict> {
        match self
            .recovery
            .take()
            .and_then(|recovery| recovery.founded_roster())
        {
            Some(roster) => vec![AuthorityVerdict::Lead(roster)],
            None => Vec::new(),
        }
    }
}

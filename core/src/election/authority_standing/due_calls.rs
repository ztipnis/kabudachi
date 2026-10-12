use crate::coordination_authority::RecoveryEpoch;
use crate::election::authority::{AuthorityCall, AuthorityRequest};
use crate::protocol::worker_state::WorkerState;
use crate::time::Instant;

use super::{AuthorityStanding, AuthorityView};

impl AuthorityStanding {
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
        self.give_up_overdue_recovery(view.state, now);
        let register = self.ask_registration_if_due(view.state, now);
        let fence = self.ask_fence_if_due(view.own_epoch, now);
        let mut calls = Vec::new();
        if register {
            calls.push(self.ask(AuthorityRequest::Register, now));
        }
        if let Some(recovery_epoch) = fence {
            calls.push(self.ask(AuthorityRequest::AcquireFence { recovery_epoch }, now));
        }
        if let Some(recovery_epoch) = self.hint_due(view, register) {
            calls.push(self.ask(
                AuthorityRequest::PublishLeaderHint {
                    recovery_epoch,
                    term: view.term,
                },
                now,
            ));
        }
        calls.extend(self.confirmation_read_due(view, now));
        calls
    }

    /// Drops the authority path, and the reply it awaits, once it has run past
    /// its give-up instant.
    fn give_up_overdue_recovery(&mut self, state: WorkerState, now: Instant) {
        if self.recovery_overdue(state, now) {
            self.recovery = None;
            self.awaited = None;
        }
    }

    /// Whether a registration has come due for a node in `state`; marks it
    /// asked.
    fn ask_registration_if_due(&mut self, state: WorkerState, now: Instant) -> bool {
        let register = Self::registers_in(state) && self.lease.registration_due(now);
        if register {
            self.lease.registration_asked(now);
        }
        register
    }

    /// The epoch to ask a fence at, if one has come due; marks it asked.
    fn ask_fence_if_due(
        &mut self,
        own_epoch: Option<RecoveryEpoch>,
        now: Instant,
    ) -> Option<RecoveryEpoch> {
        // A node that has joined no shard has no epoch to hold a fence at,
        // and never needs one.
        let fence = own_epoch.filter(|_| self.lease.fence_due(now));
        if fence.is_some() {
            self.lease.fence_asked(now);
        }
        fence
    }

    /// The epoch to publish a leader hint at, if one is due.
    fn hint_due(&mut self, view: &AuthorityView, register: bool) -> Option<RecoveryEpoch> {
        // A leader says where it can be reached on the registration's schedule,
        // so a hint lasts while the leader does, unless the record under the
        // name is not yet its own again.
        let may_hint = view.in_office() && !self.record_lost;
        let due = may_hint && (register || std::mem::take(&mut self.hint_now));
        view.own_epoch.filter(|_| due)
    }

    /// The read confirming this member's epoch, if one has come due.
    fn confirmation_read_due(
        &mut self,
        view: &AuthorityView,
        now: Instant,
    ) -> Option<AuthorityCall> {
        let due = view.own_epoch.is_some()
            && self.may_ask_for_confirmation(view.state)
            && self.confirmation.read_due(now);
        if !due {
            return None;
        }
        let call = self.ask(AuthorityRequest::ReadRecoveryEpoch, now);
        self.confirmation
            .read_asked(call.token, now, self.lease.call_timeout());
        Some(call)
    }

    /// When the authority path of a node in `state` is given up, if it is
    /// running. Once its swap is asked, the path outlives the usual give-up
    /// instant: the swap may have landed, and only its reply says whether this
    /// node holds the new epoch or lost the race for it. A reply that has not
    /// come a call timeout after the swap was asked is counted lost, for it may
    /// never come; the epoch the swap may have made is then found by the
    /// node's next read, which leaves it standing beside that epoch (see
    /// [`EpochConfirmation`](super::epoch_confirmation::EpochConfirmation)) to recover it.
    fn recovery_gives_up_in(&self, state: WorkerState) -> Option<Instant> {
        if state != WorkerState::NoQuorum {
            return None;
        }
        match self.recovery.as_ref() {
            None => None,
            Some(recovery) if recovery.is_swapping() => self
                .swap_asked_at
                .map(|asked| asked + self.lease.call_timeout()),
            Some(_) => self.recovery_gives_up_at,
        }
    }

    /// Whether the authority path of a node in `state` has run past the
    /// instant it is given up at.
    fn recovery_overdue(&self, state: WorkerState, now: Instant) -> bool {
        self.recovery_gives_up_in(state).is_some_and(|at| now >= at)
    }

    /// Whether a node in `state` has no leader it follows and may stand for
    /// election, so it must know the authority's epoch first.
    pub(super) fn suspects_its_leader(state: WorkerState) -> bool {
        matches!(state, WorkerState::LeaderSuspect | WorkerState::NoQuorum)
    }

    /// Whether a node in `state` asks for a read to confirm its epoch: it
    /// suspects its leader, and runs no authority path. That path reads the
    /// epoch itself and rejoins or swaps by what it finds, and a read of the
    /// node's own beside it would send the node off before the path could.
    fn may_ask_for_confirmation(&self, state: WorkerState) -> bool {
        Self::suspects_its_leader(state) && self.recovery.is_none()
    }

    /// The earliest instant at which the lease wants something done for a
    /// node in `state`: a registration or fence attempt, and, until it is
    /// `Fenced`, fencing itself. `None` in a state that keeps no
    /// registration.
    pub(crate) fn next_deadline(&self, state: WorkerState) -> Option<Instant> {
        let lease = Self::registers_in(state)
            .then(|| self.lease.next_deadline(state == WorkerState::Fenced))?;
        // A node that suspects its leader also asks again for the read that
        // has not confirmed its epoch, and gives up an authority path that
        // does not finish.
        let read = self
            .may_ask_for_confirmation(state)
            .then(|| self.confirmation.next_read_at())
            .flatten();
        let give_up = self.recovery_gives_up_in(state);
        Some([read, give_up].into_iter().flatten().fold(lease, Instant::min))
    }
}

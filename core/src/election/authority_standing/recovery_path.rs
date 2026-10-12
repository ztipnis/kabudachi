use crate::coordination_authority::{AuthorityError, LiveRegistrations, RecoveryEpoch};
use crate::election::authority::AuthorityRequest;
use crate::election::standing::{EpochOrder, order};
use crate::protocol::worker_state::WorkerState;
use crate::time::Instant;

use super::forced_recovery::Next;
use super::{AuthorityStanding, AuthorityVerdict, AuthorityView};

impl AuthorityStanding {
    pub(super) fn on_live_registrations(
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

    pub(super) fn on_recovery_epoch(
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
    /// authority lost it, which then asks for its fence
    /// again at once.
    pub(super) fn on_recovery_epoch_swapped(
        &mut self,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        awaited: bool,
        result: Result<(), AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if view.state == WorkerState::NoQuorum && self.recovery.is_some() {
            return self.on_recovery_swap(expected, new, awaited, &result, now);
        }
        self.on_republish(expected, new, &result, view, now)
    }

    /// The reply to the swap of this node's authority path, if it is the one
    /// awaited.
    fn on_recovery_swap(
        &mut self,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        awaited: bool,
        result: &Result<(), AuthorityError>,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        let Some(recovery) = self.recovery.as_mut().filter(|_| awaited) else {
            return Vec::new();
        };
        let next = recovery.on_swapped(expected, new, result);
        self.follow_recovery(next, now)
    }

    fn on_republish(
        &mut self,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        result: &Result<(), AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        // The leader's republish: once the epoch is back, by
        // this swap or another worker's, it asks for its fence at once.
        // Otherwise it asks when the fence is next due, so an authority that
        // keeps failing is not asked again within the same instant.
        if !(republished(new, result) && republishes_own_epoch(view, expected, new)) {
            return Vec::new();
        }
        self.lease.retry_fence_at(now);
        // The record is this node's again: its hint follows at once.
        self.record_lost = false;
        vec![AuthorityVerdict::Ask(self.ask(
            AuthorityRequest::PublishLeaderHint {
                recovery_epoch: new,
                term: view.term,
            },
            now,
        ))]
    }

    /// Carries a recovery on as `next` says.
    fn follow_recovery(&mut self, next: Next, now: Instant) -> Vec<AuthorityVerdict> {
        match next {
            Next::ReadEpoch => vec![AuthorityVerdict::Ask(
                self.ask_awaited(AuthorityRequest::ReadRecoveryEpoch, now),
            )],
            Next::Swap { from, to } => {
                self.swap_asked_at = Some(now);
                vec![AuthorityVerdict::Ask(self.ask_awaited(
                    AuthorityRequest::SwapRecoveryEpoch {
                        expected: Some(from),
                        new: to,
                    },
                    now,
                ))]
            }
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
            Next::Outvoted => {
                self.recovery = None;
                self.confirmation.outvoted();
                Vec::new()
            }
        }
    }

    /// The authority path swapped the recovery epoch to `epoch`: the node
    /// adopts it, and the configuration its recovery founds there, stands as
    /// `Candidate` for its roll call's term, and asks for the fence, which
    /// it must hold before it leads.
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
}

/// Whether a swap that came back `result` left `new` as the authority's epoch.
fn republished(new: RecoveryEpoch, result: &Result<(), AuthorityError>) -> bool {
    match result {
        Ok(()) => true,
        Err(AuthorityError::ShardConflict { current }) => current
            .as_ref()
            .is_some_and(|current| order(new, current.recovery_epoch) == EpochOrder::Mine),
        Err(_) => false,
    }
}

/// Whether a leader in office republished its own epoch `new` over a missing
/// record.
fn republishes_own_epoch(
    view: &AuthorityView,
    expected: Option<RecoveryEpoch>,
    new: RecoveryEpoch,
) -> bool {
    let own = view.own_epoch.map(|own| order(own, new)) == Some(EpochOrder::Mine);
    view.in_office() && expected.is_none() && own
}

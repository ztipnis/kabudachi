use crate::coordination_authority::{AuthorityError, RecoveryEpoch, ShardRecord};
use crate::election::authority::{AuthorityReply, AuthorityRequest, ReplyToken};
use crate::protocol::worker_state::WorkerState;
use crate::time::{Duration, Instant};

use super::authority_lease::Reconnect;
use super::epoch_confirmation::Answer;
use super::{AuthorityStanding, AuthorityVerdict, AuthorityView};

impl AuthorityStanding {
    /// Handles what the authority answered to a call this node asked for,
    /// as of `now`, and returns what the node does about it, in order.
    pub(crate) fn on_reply(
        &mut self,
        reply: AuthorityReply,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if Self::finds_another_incarnation(&reply, view) {
            // The authority holds another incarnation of the shard under its
            // name: this one is gone, whatever epoch either is at.
            self.recovery = None;
            self.awaited = None;
            return vec![AuthorityVerdict::Abandon];
        }
        match reply {
            AuthorityReply::Registered {
                sent_at, result, ..
            } => self.on_registered(sent_at, result, view, now),
            AuthorityReply::LiveRegistrations { token, result, .. } => {
                if self.take_awaited(token) {
                    self.on_live_registrations(result, view, now)
                } else {
                    Vec::new()
                }
            }
            AuthorityReply::RecoveryEpoch { token, result, .. } => {
                self.on_epoch_reply(token, result, view, now)
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
            AuthorityReply::LeaderHint { .. } | AuthorityReply::LeaderHintPublished { .. } => {
                Vec::new()
            }
        }
    }

    /// Whether `reply` shows another incarnation of the shard under its name,
    /// to a node that reads records.
    fn finds_another_incarnation(reply: &AuthorityReply, view: &AuthorityView) -> bool {
        Self::reads_records_in(view.state)
            && reply
                .held_record()
                .is_some_and(|held| held.shard_id != *view.shard)
    }

    fn on_registered(
        &mut self,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        let Ok(granted) = result else {
            return Vec::new();
        };
        self.lease.registered(sent_at, granted);
        // A fenced node that can register again reads the epoch to
        // learn whether it may resume.
        if view.state == WorkerState::Fenced {
            vec![AuthorityVerdict::Ask(
                self.ask_awaited(AuthorityRequest::ReadRecoveryEpoch, now),
            )]
        } else {
            Vec::new()
        }
    }

    fn on_epoch_reply(
        &mut self,
        token: ReplyToken,
        result: Result<Option<ShardRecord>, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        let result = result.map(|held| held.map(|held| held.recovery_epoch));
        if self.confirmation.is_awaiting(token) {
            return self.on_member_read(token, result, view, now);
        }
        if !self.take_awaited(token) {
            return Vec::new();
        }
        if view.state != WorkerState::Fenced {
            return self.on_recovery_epoch(result, view, now);
        }
        match result {
            Ok(epoch) => self.reconnect(epoch, view, now),
            Err(_) => Vec::new(),
        }
    }

    /// Whether a node in `state` reads the authority's record, so that it
    /// may find another incarnation of its shard there.
    fn reads_records_in(state: WorkerState) -> bool {
        matches!(
            state,
            WorkerState::LeaderSuspect
                | WorkerState::NoQuorum
                | WorkerState::Candidate
                | WorkerState::LeaderReconciling
                | WorkerState::Leader
                | WorkerState::Fenced
        )
    }

    /// The answer to a read of the authority's epoch that a member asked for
    /// before it may stand (see [`EpochConfirmation`](super::epoch_confirmation::EpochConfirmation)). A node that no longer
    /// suspects its leader drops it.
    fn on_member_read(
        &mut self,
        token: ReplyToken,
        result: Result<Option<RecoveryEpoch>, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        let (true, Some(own)) = (Self::suspects_its_leader(view.state), view.own_epoch) else {
            self.confirmation.stop_reading();
            return Vec::new();
        };
        let interval = self.lease.renewal_interval();
        match self
            .confirmation
            .answered(token, result, own, now, interval)
        {
            Answer::RejoinAt(epoch) => vec![AuthorityVerdict::RejoinAt(epoch)],
            Answer::Confirmed | Answer::Ignored => Vec::new(),
        }
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
}

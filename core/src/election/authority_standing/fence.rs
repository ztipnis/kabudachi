use crate::coordination_authority::{AuthorityError, RecoveryEpoch, ShardRecord};
use crate::election::authority::AuthorityRequest;
use crate::election::standing::{EpochOrder, order};
use crate::protocol::worker_state::WorkerState;
use crate::time::{Duration, Instant};

use super::forced_recovery::{ForcedRecovery, cannot_recover_from};
use super::{AuthorityStanding, AuthorityVerdict, AuthorityView};

impl AuthorityStanding {
    /// Handles the authority's answer to this node's request for the
    /// recovery fence at `epoch`, asked at `sent_at`. Only a `Leader`, or a
    /// `Candidate` waiting out the fence after its authority path, holds or
    /// seeks one; an answer for another epoch than its own is stale.
    ///
    /// - Granted: the fence lets it act until a TTL, less drift, after it
    ///   asked. A waiting candidate now leads.
    /// - Held by another worker: it asks again once that fence has run out.
    /// - The record is missing (the authority lost its data): a leader
    ///   republishes it and asks again; a waiting candidate
    ///   gives up, its swap lost with the data.
    /// - The epoch has moved on: the shard was recovered without it. A
    ///   leader steps down, and a waiting candidate gives up; either
    ///   adopts the new epoch from its leader's ack.
    /// - Unavailable: it asks again at its next renewal.
    pub(super) fn on_fence(
        &mut self,
        epoch: RecoveryEpoch,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        if !self.seeks_fence_at(epoch, view) {
            return Vec::new();
        }
        match result {
            Ok(granted) => self.on_fence_granted(epoch, sent_at, granted, view, now),
            Err(refused) => self.on_fence_refused(epoch, refused, view, now),
        }
    }

    /// Whether a node in `view.state` seeks the recovery fence at `epoch`, its
    /// own.
    fn seeks_fence_at(&self, epoch: RecoveryEpoch, view: &AuthorityView) -> bool {
        let seeking = match view.state {
            WorkerState::LeaderReconciling | WorkerState::Leader => true,
            WorkerState::Candidate => self
                .recovery
                .as_ref()
                .is_some_and(ForcedRecovery::is_awaiting_fence),
            _ => false,
        };
        let own_epoch_is_it =
            view.own_epoch.map(|own| order(own, epoch)) == Some(EpochOrder::Mine);
        seeking && own_epoch_is_it
    }

    fn on_fence_granted(
        &mut self,
        epoch: RecoveryEpoch,
        sent_at: Instant,
        granted: Duration,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        self.lease.fence_acquired(epoch, sent_at, granted);
        if view.state == WorkerState::Candidate {
            self.lead_recovered()
        } else if std::mem::take(&mut self.record_lost) {
            // A fence needs the record: it is this node's again, though
            // the reply to its republish never said so. Its hint
            // follows at once.
            vec![AuthorityVerdict::Ask(self.ask(
                AuthorityRequest::PublishLeaderHint {
                    recovery_epoch: epoch,
                    term: view.term,
                },
                now,
            ))]
        } else {
            Vec::new()
        }
    }

    fn on_fence_refused(
        &mut self,
        epoch: RecoveryEpoch,
        refused: AuthorityError,
        view: &AuthorityView,
        now: Instant,
    ) -> Vec<AuthorityVerdict> {
        match refused {
            AuthorityError::FenceHeld { remaining } => {
                self.lease
                    .retry_fence_at(now + remaining + Duration::from_ticks(1));
                Vec::new()
            }
            AuthorityError::ShardConflict { current: None } if view.in_office() => {
                self.record_lost = true;
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
            AuthorityError::ShardConflict {
                current:
                    Some(ShardRecord {
                        recovery_epoch: held,
                        ..
                    }),
            } if cannot_recover_from(view.own_epoch, held) => {
                vec![AuthorityVerdict::LoseQuorum, AuthorityVerdict::RejoinAt(held)]
            }
            AuthorityError::ShardConflict { .. } => {
                if view.in_office() {
                    vec![AuthorityVerdict::SuspectAgain]
                } else {
                    vec![AuthorityVerdict::LoseQuorum]
                }
            }
            AuthorityError::Unavailable => Vec::new(),
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

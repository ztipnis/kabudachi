//! A leader's quorum-contact lease (ADR-0001 decision 16): how long it may
//! go on acting on the strength of the acks a quorum of its configuration
//! has confirmed receiving.
//!
//! A leader answers each follower heartbeat with an ack, and a heartbeat
//! confirms the newest ack its sender accepted. The quorum-contact time is
//! the newest instant such that the leader and the roster members that
//! confirmed an ack sent at or after it are a quorum of the leader's
//! configuration. The lease ends a lease length after that: a suspicion
//! timeout less a share for clock drift (see
//! `ElectionTimings::lease_length`). No follower that received one of those
//! acks can have suspected the leader before then, and a follower whose leader
//! contact is fresh grants no vote, so no other leader can be elected while
//! the lease lasts.
//!
//! A leader of a joint configuration, which its election founded or
//! re-stamped, needs a quorum of both its sides: the old side is the
//! configuration the founding moved from, so the lease shares a majority
//! with any election still counted against that one, until the leader
//! commits the new side alone. Each member counts at the admissions the
//! roster holds, which the leader's latest change re-based, whether or not
//! the ack carrying them has reached it: a confirmation shows that the
//! member's leader contact is fresh, which is what keeps it from granting
//! a rival its vote.

use std::collections::BTreeMap;

use crate::configuration::{Roster, Tally};
use crate::protocol::ids::WorkerId;
use crate::scheduler::LeaseEnd;
use crate::time::{Duration, Instant};

/// The acks a leader's followers have confirmed since it won.
#[derive(Debug, Clone)]
pub(crate) struct QuorumContactLease {
    won_at: Instant,
    /// Per worker, the send instant of the newest of this leader's acks
    /// that worker has confirmed receiving.
    confirmed_acks: BTreeMap<WorkerId, Instant>,
}

impl QuorumContactLease {
    /// The lease of a leader that won at `won_at`, with no ack confirmed
    /// yet.
    pub(crate) fn starting_at(won_at: Instant) -> Self {
        QuorumContactLease {
            won_at,
            confirmed_acks: BTreeMap::new(),
        }
    }

    /// Records that `worker` received an ack sent at `sent_at`. Of one
    /// worker's confirmations the newest is kept.
    pub(crate) fn confirm(&mut self, worker: WorkerId, sent_at: Instant) {
        let newest = self.confirmed_acks.entry(worker).or_insert(sent_at);
        *newest = (*newest).max(sent_at);
    }

    /// When the lease of `leader`, leading `roster`, ends: unbounded when
    /// the leader alone is a quorum of its configuration, and `None` while
    /// no quorum has confirmed one of its acks.
    ///
    /// The leader counts itself at its own admission generations, and each
    /// other member at the ones the roster holds (see
    /// [`Roster::counted_admission_of`]). Confirmations from a worker the
    /// roster does not hold as a member never count, nor do those of a
    /// member that is no voter in the configuration.
    pub(crate) fn end(
        &self,
        leader: &WorkerId,
        roster: &Roster,
        lease_length: Duration,
    ) -> Option<LeaseEnd> {
        let mut tally = Tally::against(roster.configuration());
        tally.record(leader.clone(), roster.counted_admission_of(leader));
        if tally.has_quorum() {
            return Some(LeaseEnd::Unbounded);
        }

        let mut confirmations: Vec<(Instant, &WorkerId)> = roster
            .members()
            .keys()
            .filter(|member| *member != leader)
            .filter_map(|member| self.confirmed_acks.get(member).map(|at| (*at, member)))
            .collect();
        // Newest first: feeding members in this order, the confirmation that
        // first completes a quorum is the quorum-contact time.
        confirmations.sort_unstable_by(|a, b| b.cmp(a));
        for (confirmed_at, member) in confirmations {
            tally.record(member.clone(), roster.counted_admission_of(member));
            if tally.has_quorum() {
                return Some(LeaseEnd::At(confirmed_at + lease_length));
            }
        }
        None
    }

    /// Those of `workers` an admission batch may take and still leave
    /// `leader`, leading `roster`, a lease of its own (ADR-0001 decision 9):
    /// each has confirmed an ack of this leader sent no earlier than the
    /// threshold, `recent_since` or, while the lease is bounded, the
    /// quorum-contact time if that is earlier.
    ///
    /// Whom a batch admits never bears on the lease's safety: the lease of
    /// the configuration a batch founds counts only confirmations of its own
    /// voters, whoever they are (the TLA+ model's batches admit any joiners).
    /// The threshold is for liveness, so that a batch leaves the leader a
    /// lease worth having: the batch's old side is unchanged, and on its new
    /// side a majority of the old voters confirmed at or after the
    /// quorum-contact time, so with every joiner confirmed at or after the
    /// threshold, the new lease ends no sooner than the earlier of the old
    /// end and a lease length after `recent_since`.
    ///
    /// Not the quorum-contact time alone: the confirmation that commits a
    /// batch moves it to about now, and joiners that confirmed while the
    /// batch was in flight, heartbeating out of phase with the voters, would
    /// wait round after round. Not any confirmation within a lease length
    /// either: one held back by a stall of the leader's own driver would
    /// bound an unbounded lease (the leader alone a quorum) almost at once.
    /// No lease, no batch.
    pub(crate) fn admissible<'w>(
        &self,
        workers: impl IntoIterator<Item = &'w WorkerId>,
        leader: &WorkerId,
        roster: &Roster,
        lease_length: Duration,
        recent_since: Instant,
    ) -> Vec<&'w WorkerId> {
        let threshold = match self.end(leader, roster, lease_length) {
            None => return Vec::new(),
            Some(LeaseEnd::Unbounded) => recent_since,
            Some(LeaseEnd::At(end)) => {
                let quorum_contact =
                    Instant::at(end.as_ticks().saturating_sub(lease_length.as_ticks()));
                quorum_contact.min(recent_since)
            }
        };
        workers
            .into_iter()
            .filter(|worker| {
                self.confirmed_acks
                    .get(*worker)
                    .is_some_and(|confirmed_at| *confirmed_at >= threshold)
            })
            .collect()
    }

    /// When the leader goes `NoQuorum` unless more confirmations arrive: its
    /// lease end or, while it has no lease yet, one lease length after its
    /// win. `None` while its lease is unbounded.
    pub(crate) fn no_quorum_at(
        &self,
        leader: &WorkerId,
        roster: &Roster,
        lease_length: Duration,
    ) -> Option<Instant> {
        match self.end(leader, roster, lease_length) {
            Some(LeaseEnd::Unbounded) => None,
            None => Some(self.won_at + lease_length),
            Some(LeaseEnd::At(end)) => Some(end),
        }
    }
}

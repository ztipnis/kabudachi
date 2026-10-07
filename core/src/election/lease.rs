//! A leader's grant and every worker's abort deadline: how long a leader may
//! act, and by when a worker that may have lost its leader's ear must have
//! aborted its TaskRuns. [`Lease`] answers both from the acks a leader's
//! followers confirmed, the leader contact a follower had, and when a worker
//! fenced itself, and reports each change of either once, as a [`LeaseChange`]
//! `WorkerNode` turns into an output. The quorum-contact lease (see the
//! `quorum_contact_lease` module) is private to it.
//!
//! What moved and why. The lease owns the fields only these two answers
//! read: the leader's confirmed acks, the contact floor, the orphan abort
//! deadline, and the grant and abort deadline last reported. What it reads
//! of the node, the node hands it: while it leads, its [`Office`] (its id,
//! roster, term, recovery epoch and the end of its recovery fence); its
//! election timings and reconnect timeout; the time. Its last leader
//! contact stays with the node, which suspects its leader by it; the lease
//! learns of that contact only through the ack that proves it
//! ([`Lease::acked`]).

pub(super) mod quorum_contact_lease;

use crate::configuration::Roster;
use crate::protocol::ids::WorkerId;
use crate::scheduler::{LeadershipGrant, LeaseEnd};
use crate::time::{Duration, Instant};

use super::{ElectionTimings, earliest};
use quorum_contact_lease::QuorumContactLease;

/// What the lease reads of a node that leads, as of the step it reports.
pub(crate) struct Office<'a> {
    pub(crate) me: &'a WorkerId,
    pub(crate) roster: &'a Roster,
    pub(crate) term: u64,
    pub(crate) recovery_epoch: u64,
    /// When the leader's recovery fence ends: `Unbounded` for a node with no
    /// authority, which needs none. A leader that needs a fence and holds
    /// none has no office.
    pub(crate) fence_end: LeaseEnd,
}

/// A change the node must report, in order (see `Output::Grant` and
/// `Output::AbortDeadline`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseChange {
    Grant(Option<LeadershipGrant>),
    AbortDeadline(Option<Instant>),
}

/// This node's lease as a leader, and its abort deadline as a worker.
#[derive(Debug, Clone)]
pub(crate) struct Lease {
    /// The acks confirmed since this node last won. Meaningful only while
    /// it leads.
    quorum: QuorumContactLease,
    /// The latest instant, on this node's clock, before which every leader
    /// that may yet replay this worker's TaskRuns had heard from it or had
    /// not yet won; `None` until any has.
    contact_floor: Option<Instant>,
    /// Since it last fenced itself, orphaned: by when it must have aborted
    /// its TaskRuns. Cleared once it resumes, or once a leader acks it
    /// after it rejoined.
    orphan_abort_by: Option<Instant>,
    /// The grant this node last reported, so it reports each change once.
    reported_grant: Option<LeadershipGrant>,
    /// The abort deadline this node last reported, so it reports each
    /// change once.
    reported_abort_deadline: Option<Instant>,
}

impl Lease {
    /// The lease of a node that has neither led nor heard from a leader.
    pub(crate) fn new(now: Instant) -> Self {
        Lease {
            quorum: QuorumContactLease::starting_at(now),
            contact_floor: None,
            orphan_abort_by: None,
            reported_grant: None,
            reported_abort_deadline: None,
        }
    }

    /// The node won at `now`: its quorum contact starts afresh, with no ack
    /// confirmed yet.
    pub(crate) fn won(&mut self, now: Instant) {
        self.quorum = QuorumContactLease::starting_at(now);
    }

    /// While leading: `worker` received an ack sent at `sent_at`.
    pub(crate) fn confirm(&mut self, worker: WorkerId, sent_at: Instant) {
        self.quorum.confirm(worker, sent_at);
    }

    /// The node accepted a leader ack, which vouched for when that leader
    /// heard this node's heartbeat if it names `heard_at`. It is a member
    /// again, no longer orphaned.
    pub(crate) fn acked(&mut self, heard_at: Option<Instant>) {
        self.contact_floor = self.contact_floor.max(heard_at);
        self.orphan_abort_by = None;
    }

    /// The node fenced itself at `now`: it must abort within a reconnect
    /// timeout, less drift.
    pub(crate) fn orphaned(&mut self, now: Instant, timings: &ElectionTimings) {
        self.orphan_abort_by = Some(now + timings.less_drift(timings.reconnect_timeout));
    }

    /// The node resumed from its fence: it is no longer orphaned.
    pub(crate) fn resumed(&mut self) {
        self.orphan_abort_by = None;
    }

    /// The node stopped leading at `now`: it holds no grant from here on.
    /// No rival can have won before the grant it last reported ended, so
    /// its contact floor rises to that end, or to `now` if the grant ends
    /// later. The node reports the withdrawal itself.
    pub(crate) fn withdraw_grant(&mut self, now: Instant) {
        self.raise_contact_floor_to_grant(now);
        self.reported_grant = None;
    }

    /// Whether the node leads `office` with a grant that has not ended at
    /// `now`.
    pub(crate) fn holds_grant_at(
        &self,
        office: Option<&Office>,
        timings: &ElectionTimings,
        now: Instant,
    ) -> bool {
        self.grant(office, timings)
            .is_some_and(|grant| match grant.valid_until {
                LeaseEnd::Unbounded => true,
                LeaseEnd::At(end) => end > now,
            })
    }

    /// What changed since the node last reported, now that it holds `grant`
    /// (see [`Self::grant`]): its grant, then its abort deadline, which a
    /// changed grant can move. `lost_after` is how long a leader goes
    /// without hearing from a worker before it reports that worker lost and
    /// replays its TaskRuns; the node hands over the same span its own
    /// leader side counts, so the abort deadline, drift taken off it, always
    /// falls before any such replay.
    pub(crate) fn report(
        &mut self,
        grant: Option<LeadershipGrant>,
        timings: &ElectionTimings,
        lost_after: Duration,
        now: Instant,
    ) -> Vec<LeaseChange> {
        let mut changes = Vec::new();
        if grant != self.reported_grant {
            self.raise_contact_floor_to_grant(now);
            self.reported_grant = grant;
            changes.push(LeaseChange::Grant(grant));
        }
        let deadline = self.abort_deadline(timings, lost_after, now);
        if deadline != self.reported_abort_deadline {
            self.reported_abort_deadline = deadline;
            changes.push(LeaseChange::AbortDeadline(deadline));
        }
        changes
    }

    /// When the node's contact floor goes stale, while that is still ahead
    /// of `now`: a step then may report a new abort deadline.
    pub(crate) fn next_deadline(&self, timings: &ElectionTimings, now: Instant) -> Option<Instant> {
        self.leader_contact_stale_at(timings)
            .filter(|stale_at| *stale_at > now)
    }

    /// While leading: when the leader goes `NoQuorum` unless more
    /// confirmations arrive (see [`QuorumContactLease::no_quorum_at`]).
    pub(crate) fn no_quorum_at(
        &self,
        leader: &WorkerId,
        roster: &Roster,
        timings: &ElectionTimings,
    ) -> Option<Instant> {
        self.quorum
            .no_quorum_at(leader, roster, timings.lease_length())
    }

    /// While leading: those of `workers` an admission batch may take and
    /// still leave the leader a lease of its own (see
    /// [`QuorumContactLease::admissible`]).
    pub(crate) fn admissible<'w>(
        &self,
        workers: impl IntoIterator<Item = &'w WorkerId>,
        leader: &WorkerId,
        roster: &Roster,
        timings: &ElectionTimings,
        recent_since: Instant,
    ) -> Vec<&'w WorkerId> {
        self.quorum.admissible(
            workers,
            leader,
            roster,
            timings.lease_length(),
            recent_since,
        )
    }

    /// The grant `office` gives: `None` without one, or while no quorum has
    /// confirmed an ack. It ends at the earlier of the fence and the
    /// quorum-contact lease. Read afresh for every report,
    /// never kept, so it follows every move of either end.
    pub(crate) fn grant(
        &self,
        office: Option<&Office>,
        timings: &ElectionTimings,
    ) -> Option<LeadershipGrant> {
        let office = office?;
        let quorum_contact_end =
            self.quorum
                .end(office.me, office.roster, timings.lease_length())?;
        let valid_until = match (quorum_contact_end, office.fence_end) {
            (LeaseEnd::Unbounded, fence_end) => fence_end,
            (quorum_end, LeaseEnd::Unbounded) => quorum_end,
            (LeaseEnd::At(quorum_end), LeaseEnd::At(fence_end)) => {
                LeaseEnd::At(quorum_end.min(fence_end))
            }
        };
        Some(LeadershipGrant {
            term: office.term,
            recovery_epoch: office.recovery_epoch,
            valid_until,
        })
    }

    /// No rival leader can win before the grant this node last reported
    /// ends: as that grant is replaced or withdrawn, raises its contact
    /// floor to its end, or to `now` if it ends later.
    fn raise_contact_floor_to_grant(&mut self, now: Instant) {
        let Some(grant) = self.reported_grant else {
            return;
        };
        let floor = match grant.valid_until {
            LeaseEnd::Unbounded => now,
            LeaseEnd::At(end) => end.min(now),
        };
        self.contact_floor = self.contact_floor.max(Some(floor));
    }

    /// This node's contact floor, counting the end of the grant it holds,
    /// which no rival leader can precede. `None` while that grant is
    /// unbounded, since no rival can win at all, and before any leader has
    /// heard this node.
    fn effective_contact_floor(&self) -> Option<Instant> {
        match self.reported_grant.map(|grant| grant.valid_until) {
            Some(LeaseEnd::Unbounded) => None,
            Some(LeaseEnd::At(end)) => Some(self.contact_floor.map_or(end, |floor| floor.max(end))),
            None => self.contact_floor,
        }
    }

    /// When this node goes a suspicion timeout, less drift, past its contact
    /// floor: from then on a leader may already count it silent, and it
    /// must be ready to abort.
    fn leader_contact_stale_at(&self, timings: &ElectionTimings) -> Option<Instant> {
        self.effective_contact_floor()
            .map(|floor| floor + timings.lease_length())
    }

    /// The instant by which this node must have aborted its TaskRuns, if
    /// any: `lost_after`, less drift, past its contact floor, once that
    /// floor is stale, or the deadline it took when it fenced itself,
    /// whichever is earlier.
    fn abort_deadline(
        &self,
        timings: &ElectionTimings,
        lost_after: Duration,
        now: Instant,
    ) -> Option<Instant> {
        let out_of_contact = self
            .leader_contact_stale_at(timings)
            .filter(|stale_at| *stale_at <= now)
            .and(self.effective_contact_floor())
            .map(|floor| floor + timings.less_drift(lost_after));
        earliest(out_of_contact, self.orphan_abort_by)
    }
}

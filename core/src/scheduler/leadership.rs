//! Whether a scheduler may decide anything: the leadership grant its
//! worker's election gave it, the lease that bounds the grant, and the
//! versions the records it publishes carry.

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::IdGenerator;
use crate::task_record::{RecordVersion, VersionOrder};
use crate::time::{Clock, Duration, Instant};

use super::{Observer, ReportRejection, Scheduler};

/// What lets a scheduler act as its shard's leader: its worker's election
/// says the worker leads `term` at `recovery_epoch`, and may act until
/// `valid_until`.
///
/// The election hands it over (see `election::carry_out`), and the
/// scheduler holds it until the election reports a change. The scheduler
/// checks `valid_until` against its own clock on every leader-only call, so
/// a grant that has run out stops it acting even before the election says
/// anything more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipGrant {
    pub term: u64,
    /// The recovery epoch the worker leads in, lineage included: what a Task
    /// record this leader writes is versioned by.
    pub recovery_epoch: RecoveryEpoch,
    pub valid_until: LeaseEnd,
    /// The shard's default reconnect timeout (`ElectionTimings::reconnect_timeout`),
    /// for runs of tasks submitted without their own: what their claims tell
    /// their workers, and how long after a worker falls silent they may be
    /// replayed.
    pub reconnect_timeout: Duration,
}

/// When a leader's lease ends, and with it the time it may act as leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseEnd {
    /// Nothing ends it, as for a leader that alone is a majority of its
    /// electorate.
    Unbounded,
    /// It ends at this instant of the election's clock, which the scheduler
    /// must read too: the lease is over from this instant on.
    At(Instant),
}

/// Whether a scheduler leads now, and the version of the next record it
/// publishes.
#[derive(Default)]
pub(super) struct Leadership {
    /// The leadership grant its worker's election last gave it, if any.
    grant: Option<LeadershipGrant>,
    /// Whether the scheduler led at the last call that checked, which
    /// `next_deadline` reads for the lease end.
    noticed_leading: bool,
    /// The epoch and term this scheduler last published in, and the revision
    /// it publishes next there. A grant of a newer epoch or term starts it at
    /// 0, and one of an older epoch or term publishes nothing; a grant of the
    /// same term handed back after a lapse continues it, so no version is ever
    /// published twice with different contents.
    revisions: Option<(RecoveryEpoch, u64, u64)>,
}

impl Leadership {
    pub(super) fn grant(&self) -> Option<LeadershipGrant> {
        self.grant
    }

    pub(super) fn hold(&mut self, grant: Option<LeadershipGrant>) {
        self.grant = grant;
    }

    /// Whether it leads by `clock`: it holds a grant whose lease has not
    /// ended and that is not older than the epoch and term it already
    /// published in. The clock is read only for a bounded lease.
    fn leads(&self, clock: &impl Clock) -> bool {
        let Some(grant) = self.grant else {
            return false;
        };
        let leased = match grant.valid_until {
            LeaseEnd::Unbounded => true,
            LeaseEnd::At(end) => clock.now() < end,
        };
        leased && self.may_publish_under(grant.recovery_epoch, grant.term)
    }

    /// Like [`Self::leads`], and remembers the answer as the one last noticed.
    fn check(&mut self, clock: &impl Clock) -> bool {
        let leading = self.leads(clock);
        self.noticed_leading = leading;
        leading
    }

    /// The instant the held grant's lease ends, or `None` for no grant or an
    /// unbounded lease. It is still the lease's end after that instant.
    fn lease_end(&self) -> Option<Instant> {
        match self.grant?.valid_until {
            LeaseEnd::Unbounded => None,
            LeaseEnd::At(end) => Some(end),
        }
    }

    /// The lease's end while the scheduler last found itself leading. A
    /// lapse is found by a call that reads the clock, so the end stays in
    /// the answer until one has: a caller that asks "is it due yet?" must
    /// still get to `catch_up`. That is why this reads what was noticed, not
    /// `leads`, which is false from the end's instant on.
    pub(super) fn noticed_lease_end(&self) -> Option<Instant> {
        self.lease_end().filter(|_| self.noticed_leading)
    }

    /// The shard's default reconnect timeout, which the grant carries. Asked
    /// only while leading, so a grant is held.
    pub(super) fn shard_reconnect_timeout(&self) -> Duration {
        self.grant
            .expect("only a leader resolves a run's reconnect timeout, and a leader holds a grant")
            .reconnect_timeout
    }

    /// Whether the epoch and term are not older than the ones already
    /// published in: under an older one, a count restarted at 0 would publish
    /// versions that were already published with other contents, so a
    /// scheduler holding such a grant does not lead.
    pub(super) fn may_publish_under(&self, recovery_epoch: RecoveryEpoch, grant_term: u64) -> bool {
        let Some((epoch, term, next)) = self.revisions else {
            return true;
        };
        if epoch == recovery_epoch && term == grant_term {
            return true;
        }
        let published = RecordVersion {
            recovery_epoch: epoch,
            leader_term: term,
            revision: next.saturating_sub(1),
        };
        let offered = RecordVersion {
            recovery_epoch,
            leader_term: grant_term,
            revision: 0,
        };
        published.order(&offered) == VersionOrder::Newer
    }

    pub(super) fn next_version(
        &mut self,
        recovery_epoch: RecoveryEpoch,
        term: u64,
    ) -> RecordVersion {
        let revision = match &mut self.revisions {
            Some((epoch, current, next)) if *epoch == recovery_epoch && *current == term => {
                let revision = *next;
                *next += 1;
                revision
            }
            slot => {
                *slot = Some((recovery_epoch, term, 1));
                0
            }
        };
        RecordVersion {
            recovery_epoch,
            leader_term: term,
            revision,
        }
    }
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Takes the leadership grant its worker's election reports, or `None`
    /// once the worker does not lead. The scheduler keeps it until the next
    /// call, but acts on it only until its lease ends.
    ///
    /// While its node reconciles, a grant takes effect only once
    /// [`Self::reconcile`] has rebuilt for the same office; any other is not
    /// held, since leading before the rebuild would decide on nothing. The
    /// grant that ends the reconciliation loses the runs of every worker
    /// reported silent meanwhile whose reconnect timeouts have passed (see
    /// [`Self::note_silence`]).
    pub fn set_leadership_grant(&mut self, grant: Option<LeadershipGrant>) {
        let takes_effect = self.reconciliation.takes_grant(grant.as_ref());
        self.leadership.hold(grant.filter(|_| takes_effect));
        if self.check_leader() {
            self.lose_overdue();
        }
        self.compact_every_due_key();
        self.end_call();
    }

    /// Whether this scheduler leads, the only one that decides anything: it
    /// holds a grant whose lease has not ended by its own clock and that is
    /// not older than the epoch and term it already published in.
    pub fn is_leader(&self) -> bool {
        self.leadership.leads(&self.clock)
    }

    /// The instant the lease of the grant this scheduler holds ends, or
    /// `None` for no grant or an unbounded lease. It is still the lease's
    /// end after that instant has passed.
    pub fn lease_end(&self) -> Option<Instant> {
        self.leadership.lease_end()
    }

    /// Like `is_leader`, and remembers the answer as the one last noticed.
    pub(super) fn check_leader(&mut self) -> bool {
        self.leadership.check(&self.clock)
    }

    pub(super) fn require_leader(&mut self) -> Result<(), ReportRejection> {
        if self.check_leader() {
            Ok(())
        } else {
            Err(ReportRejection::NotLeader)
        }
    }
}

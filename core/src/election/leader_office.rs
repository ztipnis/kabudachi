//! What a node holds only while it leads: the
//! roster it leads, the removals it has accepted and not yet applied, and
//! when it last heard from each worker it has not reported lost.
//!
//! While the node leads, the office is the only holder of the configuration
//! and admissions it leads: the node's own standing takes them back when
//! leadership ends (see [`LeaderOffice::hand_back`]). Every operation
//! applies the accepted removals first, so nothing reads or changes the
//! configuration while a removal waits.
//!
//! Until a removal is applied the leader counts the departing worker as it
//! did, its last confirmation included, which can hold its lease a little
//! longer than the shrunk configuration's would. That is safe: a departed
//! worker has stopped, grants no vote to anyone, and its leader contact was
//! fresh when it confirmed (the TLA+ model keeps a stopped worker's
//! confirmation until the lease runs past it). The same holds of a removal
//! the leader simply announces late.

mod admission;
mod liveness;
mod removal_guard;
mod reports;
mod roster_reads;

use std::collections::BTreeSet;

use crate::configuration::{Configuration, Generation, Roster};
use crate::protocol::ids::WorkerId;
use crate::queues::Subscription;
use crate::time::Instant;

use super::ElectionTimings;
use super::lease::Lease;
use super::standing::ShardStanding;
use liveness::Liveness;
use removal_guard::RemovalGuard;
use reports::MemberReports;

/// What the leader's node lends the office for one operation.
pub(crate) struct Duties<'a> {
    pub(crate) me: &'a WorkerId,
    pub(crate) lease: &'a Lease,
    pub(crate) timings: &'a ElectionTimings,
    pub(crate) now: Instant,
}

/// What a heartbeat told the office.
pub(crate) enum Heard {
    /// The sender is alive.
    Alive,
    /// The sender confirmed one of this leader's acks of its term (the node
    /// has recorded that with its lease), and holds the configuration of
    /// generation `held`, if it said which. The ack it confirmed was sent at
    /// `sent_at`.
    Confirmed {
        held: Option<Generation>,
        sent_at: Instant,
    },
}

/// What a worker's heartbeat says of it, beyond its liveness and the ack it
/// confirms.
pub(crate) struct MemberReport {
    /// Whether it has completed a routing crawl since its admission.
    pub(crate) routing_crawled: bool,
    /// The admission generation it holds, a promise included.
    pub(crate) admission: Option<Generation>,
    /// Whether it runs compaction, which the leader reads to know which
    /// workers can run the compaction it assigns.
    pub(crate) runs_compaction: bool,
    /// The places it has free to run tasks in.
    pub(crate) available_capacity: u32,
    /// The queues it takes work from, which the leader reads to claim only
    /// those for it.
    pub(crate) queues: Subscription,
}

/// What one ack carries of the configuration the office leads. A joiner the
/// office promised admission in a batch it has yet to start gets that
/// generation as its admission: a promise, later than the configuration the
/// ack carries.
pub(crate) struct AckContent {
    pub(crate) configuration: Configuration,
    pub(crate) recipient_admission: Option<Generation>,
    pub(crate) recipient_prior_admission: Option<Generation>,
}

impl AckContent {
    fn of(roster: &Roster, peer: &WorkerId) -> Self {
        AckContent {
            configuration: roster.configuration().clone(),
            recipient_admission: roster
                .promised_admission_of(peer)
                .or_else(|| roster.admission_of(peer)),
            recipient_prior_admission: roster.prior_admission_of(peer),
        }
    }
}

/// A draining leader's final announcement: its roster after its own removal
/// and every one pending.
pub(crate) struct Departure {
    roster: Roster,
}

impl Departure {
    /// What the final ack to `peer` carries.
    pub(crate) fn ack_for(&self, peer: &WorkerId) -> AckContent {
        AckContent::of(&self.roster, peer)
    }
}

/// The office of a node that leads: see the module docs.
pub(crate) struct LeaderOffice {
    roster: Roster,
    term: u64,
    /// When it last heard from, and last had an ack confirmed by, each
    /// worker, and which it has reported silent or lost.
    liveness: Liveness,
    /// What keeps the removal of voters that confirm no ack safe (see
    /// [`Self::remove_one_unconfirming`]).
    removal: RemovalGuard,
    /// The workers whose SELF_REMOVE it has accepted since it last changed
    /// its configuration, to take out together (see
    /// [`Self::apply_pending_removals`]).
    pending_removals: BTreeSet<WorkerId>,
    /// What each worker's latest heartbeat said of it.
    reports: MemberReports,
}

impl LeaderOffice {
    /// Takes office over `roster` in `term`: hears every other member and
    /// pending joiner at `duties.now`, and commits at once if the leader
    /// alone is a majority of each side. Joiners that waited out a change
    /// are admitted from the first heartbeat on: the node does not lead yet,
    /// so nothing admits at the win.
    pub(crate) fn take(roster: Roster, term: u64, duties: &Duties) -> Self {
        let liveness = Liveness::hearing(
            roster
                .members()
                .keys()
                .chain(roster.pending())
                .filter(|worker| *worker != duties.me)
                .cloned(),
            duties.now,
        );
        let mut office = LeaderOffice {
            roster,
            term,
            liveness,
            removal: RemovalGuard::default(),
            pending_removals: BTreeSet::new(),
            reports: MemberReports::default(),
        };
        office.commit_when_due(duties.me);
        // Seeded only now: `commit_when_due` notes the voters before the guard
        // knows the roll call's generation, so the first note keeps only the
        // current generation.
        if let Some((generation, sides)) = office.roster.called_under() {
            office.removal.seed(generation, sides);
        }
        office.note_voters();
        office
    }

    pub(crate) fn roster(&self) -> &Roster {
        &self.roster
    }

    pub(crate) fn configuration(&self) -> &Configuration {
        self.roster.configuration()
    }

    pub(crate) fn admission_of(&self, worker: &WorkerId) -> Option<Generation> {
        self.roster.admission_of(worker)
    }

    pub(crate) fn prior_admission_of(&self, worker: &WorkerId) -> Option<Generation> {
        self.roster.prior_admission_of(worker)
    }

    /// A worker's heartbeat: heard now. A confirmation records the
    /// generation it holds and commits when due. `report.admission` is the
    /// one the sender holds, which confirms a promise made to it. The sender
    /// joins as pending if unknown, admission advances when due, and removals
    /// apply. A crawl report (`report.routing_crawled`) counts only when
    /// `report.admission` is the one the roster counts the sender by now, so a
    /// delayed heartbeat from before a re-admission does not count.
    pub(crate) fn take_heartbeat(
        &mut self,
        from: WorkerId,
        heard: Heard,
        report: MemberReport,
        duties: &Duties,
    ) {
        // Removals first, so what follows sees who is a member now.
        self.apply_pending_removals();
        let is_member = self.roster.members().contains_key(&from);
        self.liveness.heard(&from, &heard, is_member, duties);
        let counted_by = self.roster.admission_of(&from);
        self.reports.record(&from, &report, counted_by);
        if let Some(admission) = report.admission {
            self.roster.record_held_admission(&from, admission);
        }
        if let Heard::Confirmed { held, .. } = heard {
            if let Some(held) = held {
                self.roster.record_held_generation(&from, held);
            }
            self.commit_if_confirmed(duties);
        }
        self.roster.add_pending(from);
        self.admit_waiting_joiners(duties);
        self.apply_pending_removals();
    }

    /// A SELF_REMOVE the node accepted (its term guard stays with the
    /// node): `departing` is taken out with every other one accepted, in
    /// the next operation.
    pub(crate) fn take_removal(&mut self, departing: WorkerId) {
        self.pending_removals.insert(departing);
    }

    /// What an ack to `peer` carries, after applying removals.
    pub(crate) fn ack_for(&mut self, peer: &WorkerId) -> AckContent {
        self.apply_pending_removals();
        AckContent::of(&self.roster, peer)
    }

    /// The acks that announce this leader to every one of `peers`.
    pub(crate) fn announce(&mut self, peers: &BTreeSet<WorkerId>) -> Vec<(WorkerId, AckContent)> {
        self.apply_pending_removals();
        peers
            .iter()
            .map(|peer| (peer.clone(), AckContent::of(&self.roster, peer)))
            .collect()
    }

    /// Whether the lease has run out even for the configuration without the
    /// departed. Removals apply only when it has run out first: the departed
    /// no longer confirm anything, so the configuration without them may
    /// still have its quorum.
    pub(crate) fn has_lost_quorum(&mut self, duties: &Duties) -> bool {
        if !self.lease_has_run_out(duties) {
            return false;
        }
        self.apply_pending_removals();
        self.lease_has_run_out(duties)
    }

    fn lease_has_run_out(&self, duties: &Duties) -> bool {
        duties
            .lease
            .no_quorum_at(duties.me, &self.roster, duties.timings)
            .is_some_and(|at| duties.now >= at)
    }

    /// Notes the voters of the current generation, and forgets those of
    /// generations nothing points at any more.
    fn note_voters(&mut self) {
        self.removal.note_voters(&self.roster);
    }

    /// Drops what is kept about workers that are no longer members.
    fn forget_non_members(&mut self) {
        self.liveness.forget_non_members(self.roster.members());
    }

    /// Leadership ended: `standing` takes on the configuration the roster
    /// leads as it stands, and `me`'s admissions there. Removals still
    /// pending are not applied: nothing announced them, and applying them
    /// would mint a generation nobody heard of.
    pub(crate) fn hand_back(self, standing: &mut ShardStanding, me: &WorkerId) {
        standing.take_on_roster(&self.roster, me);
    }

    /// A draining leader leaves: its own removal joins the pending ones, all
    /// apply, `standing` takes the result, and the announcement comes back
    /// unless the removal changed nothing (see [`Roster::remove_all`]: a
    /// removal that would leave no voter announces nothing).
    pub(crate) fn depart(
        mut self,
        me: &WorkerId,
        standing: &mut ShardStanding,
    ) -> Option<Departure> {
        let before = self.roster.configuration().generation();
        self.pending_removals.insert(me.clone());
        self.apply_pending_removals();
        standing.take_on_roster(&self.roster, me);
        (self.roster.configuration().generation() != before).then_some(Departure {
            roster: self.roster,
        })
    }

    /// Takes every worker whose SELF_REMOVE was accepted since the last
    /// change out of the roster together (every pending SELF_REMOVE
    /// in the next generation; see [`Roster::remove_all`]):
    /// a voter leaves a configuration shrunk at the next generation,
    /// re-based there with every remaining voter re-admitted, the leader
    /// included; during a founding or a batch the joint configuration is
    /// re-announced with shrunk counts. A pending joiner, or a member that
    /// is no voter, is only forgotten.
    fn apply_pending_removals(&mut self) {
        if self.pending_removals.is_empty() {
            return;
        }
        let departing = std::mem::take(&mut self.pending_removals);
        self.roster.remove_all(&departing, self.term);
        self.forget_non_members();
        self.note_voters();
    }
}

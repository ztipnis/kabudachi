//! What a node holds only while it leads (ADR-0001 decisions 8 to 10): the
//! roster it leads, the removals it has accepted and not yet applied, and
//! when it last heard from each worker it has not reported lost.
//!
//! While the node leads, the office is the only holder of the configuration
//! and admissions it leads: the node's own standing takes them back when
//! leadership ends (see [`LeaderOffice::hand_back`]). Every operation
//! applies the accepted removals first, so nothing reads or changes the
//! configuration while a removal waits (ADR-0001 decision 10).
//!
//! Until a removal is applied the leader counts the departing worker as it
//! did, its last confirmation included, which can hold its lease a little
//! longer than the shrunk configuration's would. That is safe: a departed
//! worker has stopped, grants no vote to anyone, and its leader contact was
//! fresh when it confirmed (the TLA+ model keeps a stopped worker's
//! confirmation until the lease runs past it). The same holds of a removal
//! the leader simply announces late.

use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::{Configuration, Generation, Roster};
use crate::protocol::ids::WorkerId;
use crate::time::{Duration, Instant};

use super::ElectionTimings;
use super::lease::Lease;
use super::standing::ShardStanding;

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
    /// generation `held`, if it said which.
    Confirmed { held: Option<Generation> },
}

/// What one ack carries of the configuration the office leads.
pub(crate) struct AckContent {
    pub(crate) configuration: Configuration,
    pub(crate) recipient_admission: Option<Generation>,
    pub(crate) recipient_prior_admission: Option<Generation>,
}

impl AckContent {
    fn of(roster: &Roster, peer: &WorkerId) -> Self {
        AckContent {
            configuration: roster.configuration().clone(),
            recipient_admission: roster.admission_of(peer),
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
    /// When it last heard from each worker it has not yet reported lost.
    last_heard: BTreeMap<WorkerId, Instant>,
    /// The workers whose SELF_REMOVE it has accepted since it last changed
    /// its configuration, to take out together (see
    /// [`Self::apply_pending_removals`]).
    pending_removals: BTreeSet<WorkerId>,
}

impl LeaderOffice {
    /// Takes office over `roster` in `term`: hears every other member and
    /// pending joiner at `duties.now`, and commits at once if the leader
    /// alone is a majority of each side. Joiners that waited out a change
    /// are admitted from the first heartbeat on: the node does not lead yet,
    /// so nothing admits at the win.
    pub(crate) fn take(roster: Roster, term: u64, duties: &Duties) -> Self {
        let last_heard = roster
            .members()
            .keys()
            .chain(roster.pending())
            .filter(|worker| *worker != duties.me)
            .map(|worker| (worker.clone(), duties.now))
            .collect();
        let mut office = LeaderOffice {
            roster,
            term,
            last_heard,
            pending_removals: BTreeSet::new(),
        };
        office.commit_when_due(duties.me);
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
    /// generation it holds and commits when due. The sender joins as pending
    /// if unknown, a batch starts when due, and removals apply.
    pub(crate) fn take_heartbeat(&mut self, from: WorkerId, heard: Heard, duties: &Duties) {
        self.last_heard.insert(from.clone(), duties.now);
        if let Heard::Confirmed { held } = heard {
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

    /// When the office next has a worker to report lost: `lost_after` after
    /// it last heard from the one it heard from longest ago.
    pub(crate) fn next_lost_at(&self, lost_after: Duration) -> Option<Instant> {
        self.last_heard
            .values()
            .min()
            .map(|heard| *heard + lost_after)
    }

    /// The workers not heard from for `lost_after` by `now`, each reported
    /// once.
    pub(crate) fn lost_by(&mut self, now: Instant, lost_after: Duration) -> Vec<WorkerId> {
        let lost: Vec<WorkerId> = self
            .last_heard
            .iter()
            .filter(|(_, heard)| now >= **heard + lost_after)
            .map(|(worker, _)| worker.clone())
            .collect();
        for worker in &lost {
            self.last_heard.remove(worker);
        }
        lost
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
    /// change out of the roster together (ADR-0001 decision 10: every
    /// pending SELF_REMOVE in the next generation; see [`Roster::remove_all`]):
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
    }

    /// Commits the joint configuration this office leads once a majority of
    /// each side holds it (see [`Roster::commit_if_confirmed`]): it then
    /// leads the new side alone, with each member's admission generation
    /// there, its own included, and no prior admission generation any more.
    /// Whether it committed.
    fn commit_when_due(&mut self, me: &WorkerId) -> bool {
        self.apply_pending_removals();
        self.roster.commit_if_confirmed(me, self.term)
    }

    /// [`Self::commit_when_due`], then the admission of joiners that waited
    /// out the change, if it committed.
    fn commit_if_confirmed(&mut self, duties: &Duties) {
        if self.commit_when_due(duties.me) {
            self.admit_waiting_joiners(duties);
        }
    }

    /// Starts an admission batch (ADR-0001 decision 9, see
    /// [`Roster::begin_batch`]) of every worker waiting to join that has
    /// confirmed one of this leader's acks recently enough to leave it a
    /// lease worth having (see [`Lease::admissible`]): sent within the last
    /// two heartbeat intervals, or no earlier than the lease's quorum-contact
    /// time. A worker that drained after its last confirmation, its
    /// SELF_REMOVE not yet here, may be taken too: the same as one that is
    /// admitted and then drains, which the removal handles in turn. The
    /// leader itself, if its configuration does not count it, joins too.
    /// Nothing starts while the configuration is joint: joiners wait for the
    /// commit, which calls this again.
    fn admit_waiting_joiners(&mut self, duties: &Duties) {
        self.apply_pending_removals();
        if self.roster.configuration().is_joint() {
            return;
        }
        // A joiner heartbeats every heartbeat interval, echoing the ack that
        // answered its previous heartbeat: two intervals cover that ack's
        // age, and network delays within one.
        let recent_since = Instant::at(
            duties
                .now
                .as_ticks()
                .saturating_sub(2 * duties.timings.heartbeat_interval.as_ticks()),
        );
        let roster = &self.roster;
        let mut waiting: BTreeSet<WorkerId> = duties
            .lease
            .admissible(
                roster
                    .pending()
                    .iter()
                    .chain(roster.members().keys())
                    .filter(|worker| roster.is_admissible(worker)),
                duties.me,
                roster,
                duties.timings,
                recent_since,
            )
            .into_iter()
            .cloned()
            .collect();
        if roster.is_admissible(duties.me) {
            waiting.insert(duties.me.clone());
        }
        self.roster.begin_batch(&waiting, self.term);
    }
}

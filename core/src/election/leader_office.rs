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

use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::{Configuration, Generation, Roster, Side, Tally};
use crate::protocol::ids::WorkerId;
use crate::reconcile::Answered;
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
    /// generation `held`, if it said which. The ack it confirmed was sent at
    /// `sent_at`.
    Confirmed {
        held: Option<Generation>,
        sent_at: Instant,
    },
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
    /// When it last heard from each worker it has not yet reported lost.
    last_heard: BTreeMap<WorkerId, Instant>,
    /// When each counted member last confirmed a recent ack of this leader;
    /// a member not in it counts from when it was first heard.
    confirmed_at: BTreeMap<WorkerId, Instant>,
    /// The counted members reported lost for confirming no ack while their
    /// heartbeats kept arriving, to take out one at a time (see
    /// [`Self::remove_one_unconfirming`]).
    unconfirming: BTreeSet<WorkerId>,
    /// The workers reported lost and not heard since: one that confirmed no
    /// ack stays here until it confirms one, its heartbeats notwithstanding.
    lost: BTreeSet<WorkerId>,
    /// The workers reported silent and not heard since, each with the
    /// instant from which the reconnect timeouts of the runs it holds count
    /// (see [`Self::silence_changes`]).
    reported_silence: BTreeMap<WorkerId, Instant>,
    /// The voters taken out for confirming no ack, each with the generation
    /// it last echoed before (see [`Self::may_remove_one_unconfirming`]).
    removed: Vec<(WorkerId, Option<Generation>)>,
    /// The voters of each generation this office may still need to reason
    /// about: the current one, those a member last echoed, and those a
    /// record of `removed` points at.
    voters_at: BTreeMap<Generation, Vec<Side>>,
    /// The workers whose SELF_REMOVE it has accepted since it last changed
    /// its configuration, to take out together (see
    /// [`Self::apply_pending_removals`]).
    pending_removals: BTreeSet<WorkerId>,
    /// The workers whose latest heartbeat reported a routing crawl, with the
    /// admission each crawled at.
    crawled: BTreeMap<WorkerId, Generation>,
    /// The workers whose latest heartbeat said they run compaction.
    compaction_runners: BTreeSet<WorkerId>,
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
            confirmed_at: BTreeMap::new(),
            unconfirming: BTreeSet::new(),
            lost: BTreeSet::new(),
            reported_silence: BTreeMap::new(),
            removed: Vec::new(),
            voters_at: BTreeMap::new(),
            pending_removals: BTreeSet::new(),
            crawled: BTreeMap::new(),
            compaction_runners: BTreeSet::new(),
        };
        office.commit_when_due(duties.me);
        if let Some((generation, sides)) = office.roster.called_under() {
            office.voters_at.insert(generation, sides);
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
    /// generation it holds and commits when due. `admission` is the one the
    /// sender holds, which confirms a promise made to it. The sender joins as
    /// pending if unknown, admission advances when due, and removals apply. A
    /// crawl report (`routing_crawled`) counts only when `admission` is the
    /// one the roster counts the sender by now, so a delayed heartbeat from
    /// before a re-admission does not count.
    pub(crate) fn take_heartbeat(
        &mut self,
        from: WorkerId,
        heard: Heard,
        routing_crawled: bool,
        admission: Option<Generation>,
        runs_compaction: bool,
        duties: &Duties,
    ) {
        // Removals first, so what follows sees who is a member now.
        self.apply_pending_removals();
        self.last_heard.insert(from.clone(), duties.now);
        // A counted member confirms when its heartbeat echoes an ack sent
        // within a suspicion timeout, as `confirming_lately` reads a promised
        // joiner's. One that keeps heartbeating but confirms none, a one-way
        // link say, is removed once it has gone a loss timeout without (see
        // [`Self::lost_by`]).
        if self.roster.members().contains_key(&from) {
            self.confirmed_at.entry(from.clone()).or_insert(duties.now);
            if let Heard::Confirmed { sent_at, .. } = heard
                && sent_at.as_ticks() + duties.timings.suspect_timeout.as_ticks()
                    >= duties.now.as_ticks()
            {
                self.confirmed_at.insert(from.clone(), duties.now);
                self.unconfirming.remove(&from);
            }
        }
        if !self.unconfirming.contains(&from) {
            self.lost.remove(&from);
        }
        if runs_compaction {
            self.compaction_runners.insert(from.clone());
        } else {
            self.compaction_runners.remove(&from);
        }
        match admission.filter(|_| routing_crawled) {
            Some(admission) if Some(admission) == self.roster.admission_of(&from) => {
                self.crawled.insert(from.clone(), admission);
            }
            _ => {
                self.crawled.remove(&from);
            }
        }
        if let Some(admission) = admission {
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

    /// Whether every voter of the committed configuration other than `me`
    /// is known to the roster and has reported a routing crawl at the
    /// admission the roster counts it by now. Never while the configuration
    /// is joint.
    pub(crate) fn remaining_voters_have_crawled(&mut self, me: &WorkerId) -> bool {
        self.apply_pending_removals();
        let configuration = self.roster.configuration();
        let Some(voter_count) = configuration.voter_count() else {
            return false;
        };
        let others: Vec<&WorkerId> = self
            .roster
            .members()
            .keys()
            .filter(|worker| {
                *worker != me && configuration.is_voter(self.roster.counted_admission_of(worker))
            })
            .collect();
        let leader_votes =
            usize::from(configuration.is_voter(self.roster.counted_admission_of(me)));
        others.len() + leader_votes == voter_count
            && others.iter().all(|worker| {
                self.crawled
                    .get(*worker)
                    .is_some_and(|crawled_at| Some(*crawled_at) == self.roster.admission_of(worker))
            })
    }

    /// The members whose latest heartbeat said they run compaction.
    pub(crate) fn compaction_runners(&self) -> BTreeSet<WorkerId> {
        let members = self.roster.members();
        self.compaction_runners
            .iter()
            .filter(|worker| members.contains_key(*worker) || self.roster.pending().contains(*worker))
            .cloned()
            .collect()
    }

    /// The voters of the committed configuration this office knows by id,
    /// `me` included when it votes. A voter the roster does not know is not
    /// named.
    pub(crate) fn voter_ids(&self, me: &WorkerId) -> Vec<WorkerId> {
        let configuration = self.roster.configuration();
        self.roster
            .members()
            .keys()
            .chain(std::iter::once(me))
            .filter(|worker| configuration.is_voter(self.roster.counted_admission_of(worker)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect()
    }

    /// The voters of the committed configuration records may be placed on:
    /// those of [`Self::voter_ids`] that this office has not reported lost
    /// since it last heard them (`me` is never lost).
    pub(crate) fn placeable_voter_ids(&self, me: &WorkerId) -> Vec<WorkerId> {
        self.voter_ids(me)
            .into_iter()
            .filter(|voter| voter == me || !self.lost.contains(voter))
            .collect()
    }

    /// Whether the roster holds `worker` as a voter of the committed
    /// configuration or as a pending member. Removals pending are not applied
    /// first: a worker that has just asked to leave may be counted once more,
    /// and nothing its claim decides outlives its removal.
    pub(crate) fn is_voter_or_pending(&self, worker: &WorkerId) -> bool {
        self.roster.pending().contains(worker)
            || self
                .roster
                .configuration()
                .is_voter(self.roster.counted_admission_of(worker))
    }

    /// Whom a reconciliation asks: the voters of the committed configuration
    /// (either side of a joint one), `me` among them, then the pending
    /// members. Read from the roster alone.
    pub(crate) fn reconcilees(&self, me: &WorkerId) -> Vec<WorkerId> {
        let mut asked = self.voter_ids(me);
        asked.extend(
            self.roster
                .pending()
                .iter()
                .filter(|worker| !asked.contains(*worker))
                .cloned()
                .collect::<Vec<_>>(),
        );
        asked
    }

    /// What `answered` amounts to among the voters: all of them (of both
    /// sides of a joint configuration), a quorum, or short of one. Pending
    /// members and workers the roster does not hold count for nothing; `me`
    /// counts as the lease counts it, whether or not the roster lists it.
    pub(crate) fn voters_answered(
        &self,
        me: &WorkerId,
        answered: &BTreeSet<WorkerId>,
    ) -> Answered {
        let mut tally = Tally::against(self.roster.configuration());
        for worker in answered
            .iter()
            .filter(|worker| *worker == me || self.roster.members().contains_key(*worker))
        {
            tally.record(worker.clone(), self.roster.counted_admission_of(worker));
        }
        if tally.is_unanimous() {
            Answered::All
        } else if tally.has_quorum() {
            Answered::Quorum
        } else {
            Answered::Short
        }
    }

    /// Whether the roster holds `worker`, admitted or pending. Removals
    /// pending are not applied first, as for [`Self::is_voter_or_pending`].
    pub(crate) fn is_member(&self, worker: &WorkerId) -> bool {
        self.roster.members().contains_key(worker) || self.roster.pending().contains(worker)
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

    /// Starts counting from `now` toward reporting each of `workers` but `me`
    /// lost, unless it already counts one: a worker heard from keeps its own
    /// time.
    pub(crate) fn watch(&mut self, workers: BTreeSet<WorkerId>, me: &WorkerId, now: Instant) {
        for worker in workers.into_iter().filter(|worker| worker != me) {
            self.last_heard.entry(worker).or_insert(now);
        }
    }

    /// `workers` answered this office's rebuild at `now`: each is heard, as
    /// by a heartbeat, so it counts as silent again only from now, and a
    /// silence reported for it is forgotten. A counted member that still
    /// confirms no ack is silent by its last confirmation, as for a
    /// heartbeat, and is reported silent again at once.
    pub(crate) fn heard_answers(&mut self, workers: BTreeSet<WorkerId>, me: &WorkerId, now: Instant) {
        for worker in workers.into_iter().filter(|worker| worker != me) {
            self.last_heard.insert(worker.clone(), now);
            if !self.unconfirming.contains(&worker) {
                self.lost.remove(&worker);
            }
            self.reported_silence.remove(&worker);
        }
    }

    /// When the office next has a worker to report lost: `lost_after`
    /// after it last heard from each, or, for a counted member whose
    /// heartbeats still arrive (heard within `still_heard` of `now`), after
    /// it last confirmed an ack if that comes first.
    pub(crate) fn next_lost_at(
        &self,
        now: Instant,
        lost_after: Duration,
        still_heard: Duration,
    ) -> Option<Instant> {
        self.last_heard
            .iter()
            .map(|(worker, heard)| {
                let silent = *heard + lost_after;
                match self.confirmed_at.get(worker) {
                    Some(confirmed)
                        if self.roster.members().contains_key(worker)
                            && now < *heard + still_heard =>
                    {
                        silent.min(*confirmed + lost_after)
                    }
                    _ => silent,
                }
            })
            .min()
    }

    /// When `worker`'s runs' reconnect timeouts start to count: a suspicion
    /// timeout after it was last heard or, for a counted member whose
    /// heartbeats still arrive (one heard within `suspect_timeout` of `now`)
    /// but which confirms no ack, after it last confirmed one, if that is
    /// earlier, as [`Self::lost_by`] counts. `None` for a worker not tracked.
    fn silent_from(&self, worker: &WorkerId, now: Instant, suspect_timeout: Duration) -> Option<Instant> {
        let heard = *self.last_heard.get(worker)?;
        let since = match self.confirmed_at.get(worker) {
            Some(confirmed)
                if self.roster.members().contains_key(worker) && now < heard + suspect_timeout =>
            {
                heard.min(*confirmed)
            }
            _ => heard,
        };
        Some(since + suspect_timeout)
    }

    /// The silences to report at `now`, each once: a worker newly silent,
    /// with the instant its runs' reconnect timeouts count from (see
    /// [`Self::silent_from`]), and `None` for one reported silent and heard
    /// since. A worker reported lost and not heard since keeps its silence,
    /// as does a lost member that still confirms no ack: nothing has ended
    /// either silence. A silence once reported keeps its instant until it
    /// ends, so a replay it times never moves earlier.
    pub(crate) fn silence_changes(
        &mut self,
        now: Instant,
        suspect_timeout: Duration,
    ) -> Vec<(WorkerId, Option<Instant>)> {
        let mut changes = Vec::new();
        for worker in self.last_heard.keys().filter(|worker| !self.lost.contains(*worker)) {
            let silent = self
                .silent_from(worker, now, suspect_timeout)
                .filter(|from| *from <= now);
            match (silent, self.reported_silence.contains_key(worker)) {
                (Some(from), false) => changes.push((worker.clone(), Some(from))),
                (None, true) => changes.push((worker.clone(), None)),
                _ => {}
            }
        }
        for (worker, from) in &changes {
            match from {
                Some(from) => {
                    self.reported_silence.insert(worker.clone(), *from);
                }
                None => {
                    self.reported_silence.remove(worker);
                }
            }
        }
        changes
    }

    /// When the next worker not yet reported silent falls silent, while
    /// that is ahead of `now`.
    pub(crate) fn next_silent_at(&self, now: Instant, suspect_timeout: Duration) -> Option<Instant> {
        self.last_heard
            .keys()
            .filter(|worker| !self.lost.contains(*worker) && !self.reported_silence.contains_key(*worker))
            .filter_map(|worker| self.silent_from(worker, now, suspect_timeout))
            .filter(|from| *from > now)
            .min()
    }

    /// The workers not heard from for `lost_after` by `now`, each reported
    /// once. A counted member whose heartbeats still arrive (one heard
    /// within `still_heard`) but which confirmed no recent ack for
    /// `lost_after` is reported lost too, and queued for removal (see
    /// [`Self::remove_one_unconfirming`]) so admissions stop waiting on it.
    /// A silent member is only reported.
    pub(crate) fn lost_by(
        &mut self,
        now: Instant,
        lost_after: Duration,
        still_heard: Duration,
    ) -> Vec<WorkerId> {
        let mut lost: Vec<WorkerId> = self
            .last_heard
            .iter()
            .filter(|(_, heard)| now >= **heard + lost_after)
            .map(|(worker, _)| worker.clone())
            .collect();
        let queued_before = self.unconfirming.clone();
        let unconfirming: Vec<WorkerId> = self
            .last_heard
            .iter()
            .filter(|(worker, heard)| {
                self.roster.members().contains_key(*worker)
                    && now < **heard + still_heard
                    && self
                        .confirmed_at
                        .get(*worker)
                        .is_some_and(|confirmed| now >= *confirmed + lost_after)
            })
            .map(|(worker, _)| worker.clone())
            .collect();
        self.unconfirming.extend(unconfirming.iter().cloned());
        lost.extend(unconfirming);
        for worker in &lost {
            self.last_heard.remove(worker);
            self.confirmed_at.remove(worker);
            self.lost.insert(worker.clone());
        }
        // A voter already reported for confirming no ack is reported again
        // only after it confirmed one in between, which unqueues it.
        lost.retain(|worker| !queued_before.contains(worker));
        lost
    }

    /// Takes one queued unconfirming member out of the configuration if it
    /// may (see [`Self::may_remove_one_unconfirming`]). One voter per
    /// change, so each moves from n voters to n - 1 and the majorities of the
    /// two share a voter. Whether it removed one: the shrunk generation is
    /// then unheld, which holds the next removal and every admission back
    /// until the remaining voters echo it.
    fn remove_one_unconfirming(&mut self, duties: &Duties) -> bool {
        self.drop_settled_records(duties.me);
        if !self.may_remove_one_unconfirming(duties.me) {
            return false;
        }
        while let Some(worker) = self.unconfirming.pop_first() {
            if self.roster.members().contains_key(&worker) {
                let last_echoed = self.roster.held_generation_of(&worker);
                self.roster.remove_lost(&worker, self.term);
                self.removed.push((worker, last_echoed));
                self.forget_non_members();
                self.note_voters();
                return true;
            }
        }
        false
    }

    /// Whether a queued unconfirming member may be taken out now: the
    /// configuration is committed, and the voters that hold it, the leader
    /// among them, outside the queue, are a majority of the voters of every
    /// configuration a muted or removed voter may still stand at: the one
    /// each queued voter last echoed, and the one each voter removed
    /// before last echoed. Any majority of such a configuration then holds a
    /// voter that, holding a later generation, refuses its call as stale, so
    /// those voters, the removed ones among them, cannot elect a second
    /// leader. A voter whose echoed generation is unknown, as any is until it
    /// echoes in this office, blocks removal.
    fn may_remove_one_unconfirming(&self, me: &WorkerId) -> bool {
        if self.unconfirming.is_empty() || self.roster.configuration().is_joint() {
            return false;
        }
        let counted = self.roster.counted_voters();
        let mut anchors: BTreeSet<Option<Generation>> = self
            .unconfirming
            .iter()
            .filter(|worker| counted.contains(*worker))
            .map(|worker| self.roster.held_generation_of(worker))
            .collect();
        anchors.extend(
            self.removed
                .iter()
                .filter(|(worker, _)| !counted.contains(worker))
                .map(|(_, anchor)| *anchor),
        );
        let current = self.roster.configuration().generation();
        let holders: BTreeSet<&WorkerId> = counted
            .iter()
            .filter(|worker| {
                *worker == me
                    || self.roster.held_generation_of(worker) == Some(current)
            })
            .filter(|worker| !self.unconfirming.contains(*worker))
            .collect();
        anchors.into_iter().all(|anchor| {
            let Some(voters) = anchor.and_then(|generation| self.voters_at.get(&generation))
            else {
                return false;
            };
            voters.iter().all(|(known, total)| {
                let holding = known.iter().filter(|voter| holders.contains(voter)).count();
                2 * holding > *total
            })
        })
    }

    /// Forgets the voters removed that can no longer elect behind the
    /// leader's back: one readmitted, or one a majority of whose anchor
    /// configuration now holds a later generation, which is permanent. The
    /// leader holds the current generation, later than any anchor but it.
    fn drop_settled_records(&mut self, me: &WorkerId) {
        let counted = self.roster.counted_voters();
        let current = self.roster.configuration().generation();
        let roster = &self.roster;
        let voters_at = &self.voters_at;
        self.removed.retain(|(worker, anchor)| {
            if counted.contains(worker) {
                return false;
            }
            let Some(anchor) = anchor else { return true };
            let Some(voters) = voters_at.get(anchor) else {
                return true;
            };
            // Settled once a majority of every side holds a later generation.
            !voters.iter().all(|(known, total)| {
                let later = known
                    .iter()
                    .filter(|voter| {
                        counted.contains(*voter)
                            && if *voter == me {
                                current > *anchor
                            } else {
                                roster
                                    .held_generation_of(voter)
                                    .is_some_and(|held| held > *anchor)
                            }
                    })
                    .count();
                2 * later > *total
            })
        });
    }

    /// Notes the voters of the current generation, and forgets those of
    /// generations nothing points at any more.
    fn note_voters(&mut self) {
        let current = self.roster.configuration().generation();
        if !self.voters_at.contains_key(&current) {
            self.voters_at.insert(current, self.roster.sides());
        }
        let roster = &self.roster;
        let removed = &self.removed;
        self.voters_at.retain(|generation, _| {
            *generation == current
                || removed.iter().any(|(_, anchor)| *anchor == Some(*generation))
                || roster
                    .members()
                    .keys()
                    .any(|member| roster.held_generation_of(member) == Some(*generation))
        });
    }

    /// Drops what is kept about workers that are no longer members.
    fn forget_non_members(&mut self) {
        let members = self.roster.members();
        self.confirmed_at.retain(|worker, _| members.contains_key(worker));
        self.unconfirming.retain(|worker| members.contains_key(worker));
        self.lost.retain(|worker| members.contains_key(worker));
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

    /// Commits the joint configuration this office leads once a majority of
    /// each side holds it (see [`Roster::commit_if_confirmed`]): it then
    /// leads the new side alone, with each member's admission generation
    /// there, its own included, and no prior admission generation any more.
    /// Whether it committed.
    fn commit_when_due(&mut self, me: &WorkerId) -> bool {
        self.apply_pending_removals();
        let committed = self.roster.commit_if_confirmed(me, self.term);
        self.note_voters();
        committed
    }

    /// [`Self::commit_when_due`], then the admission of joiners that waited
    /// out the change, if it committed.
    fn commit_if_confirmed(&mut self, duties: &Duties) {
        if self.commit_when_due(duties.me) {
            self.admit_waiting_joiners(duties);
        }
    }

    /// Advances the admission of workers waiting to join, which takes two
    /// phases: the leader first promises each worker that has confirmed one
    /// of its acks recently enough to leave it a lease worth having (see
    /// [`Lease::admissible`]; sent within the last two heartbeat intervals,
    /// or no earlier than the lease's quorum-contact time) its admission at a
    /// generation of its term (see [`Roster::promise_admission`]); and once
    /// every one has said it holds the promise, starts the batch (see
    /// [`Roster::begin_batch`]) that admits exactly those workers there. So
    /// each joiner holds its admission, which a quorum counts at the batch's
    /// generation, before the batch exists, and a leader lost before any
    /// joiner heard of the batch leaves survivors that can still count them.
    /// A round with a worker that has yet to confirm its promise and has not
    /// confirmed an ack for a suspicion timeout, or whose workers are no
    /// longer all admissible when every one holds its promise, is replaced by
    /// a new round, since a batch must admit exactly those promised. A worker that drained after its last
    /// confirmation, its SELF_REMOVE not yet here, may be promised: the same
    /// as one that is admitted and then drains, which the removal handles in
    /// turn. The leader itself, if its configuration does not count it, joins
    /// the batch too. Nothing starts while the configuration is joint:
    /// joiners wait for the commit, which calls this again.
    ///
    /// Nor does anything start until every member the configuration counts
    /// has said it holds it. A member that missed the acks after a commit
    /// and is left a generation behind by the next batch is refused by those
    /// that hold the newer one, and if the leader is then lost it may be
    /// needed for a quorum nobody can reach. A member that goes silent
    /// therefore blocks admissions until it is replaced by other means. One
    /// whose heartbeats keep arriving but which confirms none of this
    /// leader's acks blocks them only until a suspicion timeout and a
    /// reconnect timeout have passed since it last confirmed one: the leader
    /// then removes it, one voter per configuration change, once every other
    /// voter holds the committed configuration (see
    /// [`Self::remove_one_unconfirming`]). It may rejoin as a pending joiner
    /// once its link heals.
    fn admit_waiting_joiners(&mut self, duties: &Duties) {
        self.advance_admission(duties);
        self.note_voters();
    }

    /// The body of [`Self::admit_waiting_joiners`].
    fn advance_admission(&mut self, duties: &Duties) {
        self.apply_pending_removals();
        if self.remove_one_unconfirming(duties) {
            return;
        }
        if self.roster.configuration().is_joint() || !self.roster.is_held_by_every_voter(duties.me)
        {
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
        let waiting: BTreeSet<WorkerId> = duties
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
        let leader = roster.is_admissible(duties.me).then_some(duties.me);
        // A round stands while every promised worker that has yet to confirm
        // its promise still confirms the leader's acks, if not recently
        // enough for a batch then within a suspicion timeout: a worker that
        // heartbeats on but confirms nothing new would hold every later
        // joiner back for good, so a new round replaces the round. Hearing a
        // heartbeat is no such confirmation, which an echo of an old ack
        // gives as well as a fresh one.
        let promised: Vec<(&WorkerId, bool)> = roster.promised_workers().collect();
        let confirming_lately: BTreeSet<&WorkerId> = duties
            .lease
            .admissible(
                promised.iter().map(|(worker, _)| *worker),
                duties.me,
                roster,
                duties.timings,
                Instant::at(
                    duties
                        .now
                        .as_ticks()
                        .saturating_sub(duties.timings.suspect_timeout.as_ticks()),
                ),
            )
            .into_iter()
            .collect();
        if !promised.is_empty() {
            let confirming = |worker: &WorkerId| {
                waiting.contains(worker) || confirming_lately.contains(worker)
            };
            if roster.is_every_promise_held() {
                // The batch's new side must not cost the leader its lease, so
                // each worker it takes, one that confirmed its promise before
                // included, must still be one a lease is worth leaving it
                // with. A promised worker not waiting at this moment but
                // still confirming is between two confirmations: the next
                // heartbeat brings it back, whereas a new round would make
                // every joiner confirm again.
                if promised.iter().all(|(worker, _)| waiting.contains(*worker)) {
                    self.roster.begin_batch(leader, self.term);
                    return;
                }
                if promised.iter().all(|(worker, _)| confirming(worker)) {
                    return;
                }
            } else if promised
                .iter()
                .all(|(worker, held)| *held || confirming(worker))
            {
                return;
            }
        }
        if !self.roster.promise_admission(&waiting, self.term) {
            self.roster.begin_batch(leader, self.term);
        }
    }
}

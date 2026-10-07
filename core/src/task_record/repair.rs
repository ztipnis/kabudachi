//! A leader's repair of where its records are held.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::generated::{self, TaskRecord};
use crate::protocol::ids::{TaskId, WorkerId};
use crate::task_record::gate::{PlacedWrite, PriorPlacement, Write, WriteOutcome};
use crate::task_record::version::{RecordVersion, VersionOrder};
use crate::time::{Duration, Instant};

/// The most writes a repair (and the writes already under way) keep in
/// flight.
const IN_FLIGHT: usize = 64;

/// What a leader keeps to repair the placement of its records. Every
/// revision it writes is noted ([`Self::written`]), so it knows where each
/// record went and which placements the next revision must reach as well
/// (see [`PlacedWrite`]). When the voters it can place records on change, or a
/// write is refused, it asks for the records concerned to be published again
/// ([`Self::check`]), a bounded number at a time, so they are written where
/// they belong now. It keeps nothing past an office: a leader that stops
/// leading forgets it all, and the next one learns where records were held
/// from what it finds ([`Self::found`]) and from the writes it makes.
#[derive(Debug)]
pub struct Repair {
    retry_after: Duration,
    /// Where each record's newest write went, and in which version.
    written: BTreeMap<TaskId, Written>,
    /// Writes issued whose outcome has not arrived.
    in_flight: Vec<Write>,
    /// The voters records could be placed on at the last check.
    seen: Option<Vec<WorkerId>>,
    /// Records whose placement changed, not yet published again.
    pending: BTreeSet<TaskId>,
    /// Records whose joint write was stored, to be published once more on
    /// their placement alone. Kept while the scheduler does not lead.
    ending_move: BTreeSet<TaskId>,
    /// Records whose newest write was refused, to publish again at the
    /// instant.
    retries: BTreeMap<TaskId, Instant>,
    /// Whether the last check found something to wake for: the scheduler
    /// leads and there is room for a write.
    can_wake: bool,
}

#[derive(Debug)]
struct Written {
    version: RecordVersion,
    holders: BTreeSet<WorkerId>,
    /// Whether the newest revision was written jointly with earlier placements
    /// and has not been stored yet: once it is, the record is written once
    /// more on its placement alone, which ends the move.
    moving: bool,
    /// The placements some revision of the record may still be known by, each
    /// with the newest version written to it: every one an earlier revision
    /// was written to, until a revision stored at all of them is stored at
    /// the next.
    chain: Vec<(RecordVersion, BTreeSet<WorkerId>)>,
}

impl Written {
    /// Notes `holders` as a placement `version` was written to.
    fn reach(&mut self, version: RecordVersion, holders: &BTreeSet<WorkerId>) {
        match self.chain.iter_mut().find(|(_, placement)| placement == holders) {
            Some((newest, _)) => *newest = version,
            None => self.chain.push((version, holders.clone())),
        }
    }
}

impl Repair {
    /// A repair that publishes a record whose write was refused again
    /// `retry_after` later.
    pub fn new(retry_after: Duration) -> Self {
        Repair {
            retry_after,
            written: BTreeMap::new(),
            in_flight: Vec::new(),
            seen: None,
            pending: BTreeSet::new(),
            ending_move: BTreeSet::new(),
            retries: BTreeMap::new(),
            can_wake: false,
        }
    }

    /// `records`, which a rebuild or a late answer gave this leader, were held
    /// as their placements say. A record this leader has written since keeps
    /// its own entry.
    pub fn found(&mut self, records: &[TaskRecord]) {
        for record in records {
            let Ok((task, version)) = crate::task_record::store::identify(record) else {
                continue;
            };
            let holders: BTreeSet<WorkerId> =
                record.placement.iter().cloned().map(WorkerId::from).collect();
            if holders.is_empty() {
                continue;
            }
            // The placements the revision was written to jointly are still
            // ones the record may be known by.
            let carried: Vec<BTreeSet<WorkerId>> = record
                .prior_placements
                .iter()
                .map(|prior| prior.holders.iter().cloned().map(WorkerId::from).collect())
                .collect();
            let known = self
                .written
                .get(&task)
                .is_some_and(|written| written.version.order(&version) != VersionOrder::Newer);
            if !known {
                self.written.insert(
                    task,
                    Written {
                        version,
                        holders: holders.clone(),
                        moving: false,
                        chain: std::iter::once(holders)
                            .chain(carried)
                            .map(|placement| (version, placement))
                            .collect(),
                    },
                );
            }
        }
    }

    /// `write`, its record placed on the holders it names, is being written
    /// now: it learns the other placements it must reach (see
    /// [`PlacedWrite::prior`]), those earlier revisions of the record were
    /// written to that it does not name. `is_member` says whether a holder is
    /// still in the configuration: a placement that has lost holders is
    /// reached at all of those that remain, if they are fewer than a majority.
    ///
    /// # Panics
    /// If the record lacks its version, its task or its task id.
    pub fn written(&mut self, write: &mut PlacedWrite, is_member: impl Fn(&WorkerId) -> bool) {
        let issued = Write::of(&write.record);
        let holders: BTreeSet<WorkerId> = write.holders().into_iter().collect();
        if holders.is_empty() {
            return;
        }
        let before = self.written.get(&issued.task_id);
        write.prior = before
            .map(|written| {
                written
                    .chain
                    .iter()
                    .filter(|(_, placement)| *placement != holders)
                    .map(|(_, placement)| {
                        PriorPlacement::new(placement.iter().cloned().collect(), &is_member)
                    })
                    .collect()
            })
            .unwrap_or_default();
        write.record.prior_placements = write
            .prior
            .iter()
            .map(|prior| generated::PlacementHolders {
                holders: prior.holders.iter().cloned().map(Into::into).collect(),
            })
            .collect();
        if let Some(before) = self.written.get_mut(&issued.task_id)
            && before.version == issued.version
        {
            // The same revision placed anew while its write is under way:
            // nothing writes it to the new holders, so the old ones keep
            // theirs until a revision written after this stores.
            before.holders.extend(holders.iter().cloned());
            before.moving |= !write.prior.is_empty();
            before.reach(issued.version, &holders);
            return;
        }
        let mut written = self.written.remove(&issued.task_id).unwrap_or(Written {
            version: issued.version,
            holders: BTreeSet::new(),
            moving: false,
            chain: Vec::new(),
        });
        written.version = issued.version;
        written.moving = !write.prior.is_empty();
        written.holders = holders.clone();
        written.reach(issued.version, &holders);
        self.in_flight.push(issued.clone());
        self.written.insert(issued.task_id, written);
    }

    /// `outcome` of a write arrived at `now`. A stored write was stored at a
    /// majority of every placement its revision had to reach, so the
    /// placements of earlier revisions no longer need reaching. A refused
    /// one is, if it is its record's newest, published again after the retry
    /// delay: a record some revision of which was not stored is not certain
    /// to be held anywhere.
    pub fn settled(&mut self, outcome: &WriteOutcome, now: Instant) {
        let newest = self
            .written
            .get(&outcome.write.task_id)
            .is_some_and(|written| written.version == outcome.write.version);
        if let Some(at) = self.in_flight.iter().position(|write| *write == outcome.write) {
            self.in_flight.remove(at);
        }
        if outcome.stored {
            if newest {
                self.retries.remove(&outcome.write.task_id);
            }
            if let Some(written) = self.written.get_mut(&outcome.write.task_id) {
                let stored = outcome.write.version;
                written
                    .chain
                    .retain(|(version, _)| version.order(&stored) != VersionOrder::Newer);
                if newest && std::mem::take(&mut written.moving) {
                    self.ending_move.insert(outcome.write.task_id.clone());
                }
            }
            return;
        }
        if newest {
            self.retries
                .insert(outcome.write.task_id.clone(), now + self.retry_after);
        }
    }

    /// The records to publish again now. `in_office` says whether the node
    /// holds office, and `leading` whether its scheduler leads, which a
    /// leader that is still reconciling does not; `placeable` is the voters it
    /// could place a record on, `holds` says whether it still holds a task,
    /// and `place` where a record would be held now, if it can be placed. A
    /// leader whose voters changed finds every record held elsewhere than it
    /// would be now. Refused writes due again come first, then the moved
    /// records, in all no more than leave room under the number of writes in
    /// flight. Nothing is published, and nothing noted is forgotten, until
    /// the scheduler leads; a node that holds no office forgets it all.
    #[allow(clippy::too_many_arguments)]
    pub fn check(
        &mut self,
        in_office: bool,
        leading: bool,
        placeable: &[WorkerId],
        holds: impl Fn(&TaskId) -> bool,
        place: impl Fn(&TaskId) -> Option<Vec<WorkerId>>,
        now: Instant,
    ) -> Vec<TaskId> {
        self.can_wake = false;
        if !in_office {
            *self = Repair::new(self.retry_after);
            return Vec::new();
        }
        if !leading {
            // Nothing can be published: a leader that leads again finds the
            // records held elsewhere than they belong by looking, and keeps
            // the refusals still owed a republish, but a due retry must not
            // wake its driver meanwhile.
            self.seen = None;
            self.pending.clear();
            return Vec::new();
        }
        self.written.retain(|task, _| holds(task));
        self.pending.extend(std::mem::take(&mut self.ending_move));
        self.pending.retain(|task| holds(task));
        self.retries.retain(|task, _| holds(task));
        let mut voters = placeable.to_vec();
        voters.sort();
        if self.seen.as_ref() != Some(&voters) {
            for (task, written) in &self.written {
                let moved = place(task).is_some_and(|holders| {
                    holders.into_iter().collect::<BTreeSet<_>>() != written.holders
                });
                if moved {
                    self.pending.insert(task.clone());
                }
            }
            self.seen = Some(voters);
        }
        let room = IN_FLIGHT.saturating_sub(self.in_flight.len());
        let due: Vec<TaskId> = self
            .retries
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(task, _)| task.clone())
            .collect();
        let batch: Vec<TaskId> = due
            .into_iter()
            .chain(self.pending.iter().cloned())
            .take(room)
            .collect();
        for task in &batch {
            self.retries.remove(task);
            self.pending.remove(task);
        }
        self.can_wake = room > batch.len();
        batch
    }

    /// When the earliest refused write is due to be published again, as the
    /// last check found it: never while the scheduler does not lead, and never
    /// while writes in flight leave no room (an outcome wakes the driver then,
    /// where an instant already past would spin it).
    pub fn wake_at(&self) -> Option<Instant> {
        self.can_wake.then(|| self.retries.values().copied().min()).flatten()
    }
}

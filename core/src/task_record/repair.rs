//! A leader's repair of where its records are held.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{TaskId, WorkerId};
use crate::task_record::gate::{Write, WriteOutcome};
use crate::task_record::version::RecordVersion;
use crate::time::{Duration, Instant};

/// The most writes a repair (and the writes already under way) keep in
/// flight.
const IN_FLIGHT: usize = 64;

/// A revision stored where it now belongs, and the holders that no longer
/// hold the record: each is sent `record` so it drops its stale copy.
#[derive(Debug, Clone, PartialEq)]
pub struct Retirement {
    pub record: TaskRecord,
    pub former: Vec<WorkerId>,
}

/// What a leader keeps to repair the placement of its records. Every
/// revision it writes is noted ([`Self::written`]), so it knows where each
/// record went. When the voters it can place records on change, or a write is
/// refused, it asks for the records concerned to be published again
/// ([`Self::check`]), a bounded number at a time, so they are written where
/// they belong now; and once such a write is stored, it names the holders
/// the record left ([`Self::settled`]), which hold a copy nothing else would
/// ever update. It keeps nothing past an office: a leader that stops
/// leading forgets it all, and the next one learns where records went from
/// the writes it makes.
#[derive(Debug)]
pub struct Repair {
    retry_after: Duration,
    /// Where each record's newest write went, and in which version.
    written: BTreeMap<TaskId, Written>,
    /// The copies to retire once the write that moved the record is stored.
    leaving: Vec<(Write, Retirement)>,
    /// Writes issued whose outcome has not arrived.
    in_flight: Vec<Write>,
    /// The voters records could be placed on at the last check.
    seen: Option<Vec<WorkerId>>,
    /// Records whose placement changed, not yet published again.
    pending: BTreeSet<TaskId>,
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
    /// Holders the record left whose copy has not been retired yet: a
    /// write that was refused retired nothing, and its successor still owes it.
    owed: BTreeSet<WorkerId>,
}

impl Repair {
    /// A repair that publishes a record whose write was refused again
    /// `retry_after` later.
    pub fn new(retry_after: Duration) -> Self {
        Repair {
            retry_after,
            written: BTreeMap::new(),
            leaving: Vec::new(),
            in_flight: Vec::new(),
            seen: None,
            pending: BTreeSet::new(),
            retries: BTreeMap::new(),
            can_wake: false,
        }
    }

    /// `record`, placed on the holders it names, is being written now.
    ///
    /// # Panics
    /// If `record` lacks its version, its task or its task id.
    pub fn written(&mut self, record: &TaskRecord) {
        let write = Write::of(record);
        let holders: BTreeSet<WorkerId> =
            record.placement.iter().cloned().map(WorkerId::from).collect();
        if holders.is_empty() {
            return;
        }
        if let Some(before) = self.written.get_mut(&write.task_id)
            && before.version == write.version
        {
            // The same revision placed anew while its write is under way:
            // nothing writes it to the new holders, so the old ones keep
            // theirs until a revision written after this stores.
            before.holders.extend(holders);
            return;
        }
        let owed: BTreeSet<WorkerId> = self
            .written
            .get(&write.task_id)
            .map(|before| {
                before
                    .owed
                    .union(&before.holders)
                    .filter(|holder| !holders.contains(*holder))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if !owed.is_empty() {
            self.leaving.push((
                write.clone(),
                Retirement {
                    record: record.clone(),
                    former: owed.iter().cloned().collect(),
                },
            ));
        }
        self.in_flight.push(write.clone());
        self.written.insert(
            write.task_id,
            Written {
                version: write.version,
                holders,
                owed,
            },
        );
    }

    /// `outcome` of a write arrived at `now`. A stored write that moved its
    /// record away from holders returns the copies to retire. A refused one
    /// is, if it is its record's newest, published again after the retry
    /// delay: a record some revision of which was not stored is not certain
    /// to be held anywhere.
    pub fn settled(&mut self, outcome: &WriteOutcome, now: Instant) -> Option<Retirement> {
        let newest = self
            .written
            .get(&outcome.write.task_id)
            .is_some_and(|written| written.version == outcome.write.version);
        if let Some(at) = self.in_flight.iter().position(|write| *write == outcome.write) {
            self.in_flight.remove(at);
        }
        let retirement = self
            .leaving
            .iter()
            .position(|(write, _)| *write == outcome.write)
            .map(|at| self.leaving.remove(at).1);
        if outcome.stored {
            if newest {
                self.retries.remove(&outcome.write.task_id);
            }
            if let (Some(retirement), Some(written)) =
                (&retirement, self.written.get_mut(&outcome.write.task_id))
            {
                written.owed.retain(|holder| !retirement.former.contains(holder));
            }
            return retirement;
        }
        if newest {
            self.retries
                .insert(outcome.write.task_id.clone(), now + self.retry_after);
        }
        None
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

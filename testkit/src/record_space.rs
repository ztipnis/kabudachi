//! The shard's Task records as a simulation holds them: no network, no
//! clock of its own, every outcome a function of the calls made.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::task_record::{Origin, VersionedRecords, Write};
use kabudachi_core::time::{Duration, Instant};

/// The shard's Task records as the simulator's nodes hold them: each node
/// has its own store; a write lands at once on every placement holder the
/// writer can reach that is up, and its acknowledgement reaches the writer
/// `ack_delay` later, counting the holders that stored it. Holders can be
/// taken down and the space partitioned, like the simulated network.
#[derive(Clone, Default)]
pub struct RecordSpace(Rc<RefCell<Space>>);

/// One write's outcome, for its writer.
pub struct SpaceWrite {
    pub writer: WorkerId,
    pub write: Write,
    /// Whether a quorum of the placement stored it.
    pub stored: bool,
}

#[derive(Default)]
struct Space {
    ack_delay: Option<Duration>,
    stores: BTreeMap<WorkerId, VersionedRecords>,
    down: BTreeSet<WorkerId>,
    cut: Option<(BTreeSet<WorkerId>, BTreeSet<WorkerId>)>,
    acknowledgements: Vec<(Instant, SpaceWrite)>,
    /// Writers whose writes stay in flight, and the writes held so far.
    holding: BTreeSet<WorkerId>,
    in_flight: Vec<(WorkerId, TaskRecord, usize)>,
}

impl Space {
    /// Lands `record` on every placement holder that is up (and, unless the
    /// write was already on its way, that `writer` can reach), and queues the
    /// writer's acknowledgement.
    fn deliver(
        &mut self,
        writer: &WorkerId,
        record: TaskRecord,
        quorum: usize,
        now: Instant,
        on_its_way: bool,
    ) {
        let write = Write::of(&record);
        let mut stored = 0;
        for holder in record.placement.iter().cloned().map(WorkerId::from) {
            let reached = if on_its_way {
                !self.down.contains(&holder)
            } else {
                self.reaches(writer, &holder)
            };
            if !reached {
                continue;
            }
            // The holders' own store decides: it refuses an older version and a
            // different record at the same version, and a refused put is no
            // acknowledgement.
            if self.store_of(&holder).put(record.clone(), now).is_ok() {
                stored += 1;
            }
        }
        let due = now + self.ack_delay.unwrap_or(Duration::from_ticks(0));
        self.acknowledgements.push((
            due,
            SpaceWrite {
                writer: writer.clone(),
                write,
                stored: stored >= quorum,
            },
        ));
    }

    /// `holder`'s store, which names `holder` as its holder.
    fn store_of(&mut self, holder: &WorkerId) -> &mut VersionedRecords {
        self.stores
            .entry(holder.clone())
            .or_insert_with(|| VersionedRecords::default().held_by(holder.clone()))
    }

    fn reaches(&self, from: &WorkerId, to: &WorkerId) -> bool {
        if self.down.contains(to) {
            return false;
        }
        match &self.cut {
            Some((a, b)) if from != to => {
                !(a.contains(from) && b.contains(to) || b.contains(from) && a.contains(to))
            }
            _ => true,
        }
    }
}

impl RecordSpace {
    /// The `factor` voters a record of `task` is placed on (a stable order
    /// of their ids mixed with the task's, not kad's metric) and its
    /// majority quorum. The order is the same in every run.
    pub fn placement(
        task: &TaskId,
        voters: &[WorkerId],
        factor: usize,
    ) -> (Vec<WorkerId>, usize) {
        let mut ranked: Vec<(u64, &WorkerId)> = voters
            .iter()
            .map(|voter| (mixed(voter.as_str(), task.as_str()), voter))
            .collect();
        ranked.sort();
        let holders: Vec<WorkerId> = ranked
            .into_iter()
            .take(factor)
            .map(|(_, voter)| voter.clone())
            .collect();
        let quorum = holders.len() / 2 + 1;
        (holders, quorum)
    }

    /// Writes `record` (its `placement` filled) for `writer` at `now`. A
    /// writer whose writes are held (see `hold_writes_from`) lands nothing and
    /// is acknowledged by no one.
    ///
    /// # Panics
    /// If `record` lacks its version, its task or its task id.
    pub fn write(&self, writer: &WorkerId, record: TaskRecord, quorum: usize, now: Instant) {
        let mut space = self.0.borrow_mut();
        if space.holding.contains(writer) {
            space.in_flight.push((writer.clone(), record, quorum));
            return;
        }
        space.deliver(writer, record, quorum, now, false);
    }

    /// From now on `writer`'s writes stay in flight: they land on no holder
    /// and are acknowledged by none until released.
    pub fn hold_writes_from(&self, writer: &WorkerId) {
        self.0.borrow_mut().holding.insert(writer.clone());
    }

    /// Delivers every write held from `writer`, now, to each holder that is
    /// up, as if it had been delayed on the way (a cut made since does not
    /// stop it); holders store or refuse it by version, as always. Later
    /// writes of `writer` are no longer held.
    pub fn release_writes_from(&self, writer: &WorkerId, now: Instant) {
        let mut space = self.0.borrow_mut();
        space.holding.remove(writer);
        let (released, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut space.in_flight)
            .into_iter()
            .partition(|(from, _, _)| from == writer);
        space.in_flight = kept;
        for (writer, record, quorum) in released {
            space.deliver(&writer, record, quorum, now, true);
        }
    }

    /// `from` hands `record`, a copy it held, to each of `holders` it can
    /// reach, which keep it as a drained worker's copy; says whether at least
    /// `quorum` of them stored it. Nothing is held back and no acknowledgement
    /// is delayed: the drain waits for the outcome.
    pub fn hand_off(
        &self,
        from: &WorkerId,
        record: &TaskRecord,
        holders: &[WorkerId],
        quorum: usize,
        now: Instant,
    ) -> bool {
        let mut space = self.0.borrow_mut();
        let mut stored = 0;
        for holder in holders {
            if space.reaches(from, holder)
                && space
                    .store_of(holder)
                    .put_from(record.clone(), Origin::HandOff, now)
                    .is_ok()
            {
                stored += 1;
            }
        }
        stored >= quorum
    }

    /// `writer`, the leader, sends `record`, a revision placed elsewhere, to
    /// each of `former` it can reach, which drop their stale copies.
    pub fn retire(&self, writer: &WorkerId, record: &TaskRecord, former: &[WorkerId], now: Instant) {
        let mut space = self.0.borrow_mut();
        for holder in former {
            if space.reaches(writer, holder) {
                let _ = space.store_of(holder).put(record.clone(), now);
            }
        }
    }

    /// The acknowledgements due by `now`, in the order their writes were made.
    pub fn take_due(&self, now: Instant) -> Vec<SpaceWrite> {
        let mut space = self.0.borrow_mut();
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut space.acknowledgements)
            .into_iter()
            .partition(|(at, _)| *at <= now);
        space.acknowledgements = later;
        due.into_iter().map(|(_, write)| write).collect()
    }

    /// When the earliest acknowledgement still to come falls due.
    pub fn next_due(&self) -> Option<Instant> {
        self.0
            .borrow()
            .acknowledgements
            .iter()
            .map(|(at, _)| *at)
            .min()
    }

    /// How long a write's acknowledgement takes to reach its writer, for
    /// the writes made from now on.
    pub fn set_ack_delay(&self, delay: Duration) {
        self.0.borrow_mut().ack_delay = Some(delay);
    }

    /// A holder that is down stores nothing until it is up again.
    pub fn set_up(&self, holder: &WorkerId, up: bool) {
        let mut space = self.0.borrow_mut();
        if up {
            space.down.remove(holder);
        } else {
            space.down.insert(holder.clone());
        }
    }

    /// Cuts writes between the two groups, replacing any earlier cut.
    pub fn partition(&self, a: &BTreeSet<WorkerId>, b: &BTreeSet<WorkerId>) {
        self.0.borrow_mut().cut = Some((a.clone(), b.clone()));
    }

    pub fn heal(&self) {
        self.0.borrow_mut().cut = None;
    }

    /// Whether `from` can reach `holder`: it is up and no cut separates them.
    pub fn can_reach(&self, from: &WorkerId, holder: &WorkerId) -> bool {
        self.0.borrow().reaches(from, holder)
    }

    /// Every record `holder` holds, the newest revision of each task.
    pub fn held_records(&self, holder: &WorkerId) -> Vec<TaskRecord> {
        self.0
            .borrow()
            .stores
            .get(holder)
            .map(|store| store.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// What `holder` holds of `task`.
    pub fn held_by(&self, holder: &WorkerId, task: &TaskId) -> Option<TaskRecord> {
        self.0
            .borrow()
            .stores
            .get(holder)
            .and_then(|store| store.get(task))
            .cloned()
    }
}

/// FNV-1a over the voter's id, a separator and the task's id, finalized: fixed
/// by construction, unlike a randomly keyed hasher.
fn mixed(voter: &str, task: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in voter.bytes().chain([0xff]).chain(task.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // FNV's last bytes barely move the high bits, which order the voters: a
    // finalizer lets the task's id, its last bytes, change the order.
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash
}

//! The shard's Task records as a simulation holds them: no network, no
//! clock of its own, every outcome a function of the calls made.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::task_record::{VersionOrder, Write, identify};
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
    stores: BTreeMap<WorkerId, BTreeMap<TaskId, TaskRecord>>,
    down: BTreeSet<WorkerId>,
    cut: Option<(BTreeSet<WorkerId>, BTreeSet<WorkerId>)>,
    acknowledgements: Vec<(Instant, SpaceWrite)>,
}

impl Space {
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

    /// Writes `record` (its `placement` filled) for `writer` at `now`.
    ///
    /// # Panics
    /// If `record` lacks its version, its task or its task id.
    pub fn write(&self, writer: &WorkerId, record: TaskRecord, quorum: usize, now: Instant) {
        let write = Write::of(&record);
        let mut space = self.0.borrow_mut();
        let mut stored = 0;
        for holder in record.placement.iter().cloned().map(WorkerId::from) {
            if !space.reaches(writer, &holder) {
                continue;
            }
            let (task, version) =
                identify(&record).expect("the scheduler builds every record with its task and version");
            let held = space.stores.entry(holder).or_default();
            // The real store refuses an older version and a different record
            // at the same version; a refused put is no acknowledgement.
            let accepted = held.get(&task).is_none_or(|have| {
                let (_, have_version) =
                    identify(have).expect("a stored record names its task and version");
                match have_version.order(&version) {
                    VersionOrder::Newer => true,
                    VersionOrder::Same => *have == record,
                    VersionOrder::Older => false,
                }
            });
            if accepted {
                stored += 1;
                held.insert(task, record.clone());
            }
        }
        let due = now + space.ack_delay.unwrap_or(Duration::from_ticks(0));
        space.acknowledgements.push((
            due,
            SpaceWrite {
                writer: writer.clone(),
                write,
                stored: stored >= quorum,
            },
        ));
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

/// FNV-1a over the voter's id, a separator and the task's id: fixed by
/// construction, unlike a randomly keyed hasher.
fn mixed(voter: &str, task: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in voter.bytes().chain([0xff]).chain(task.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use kabudachi_core::protocol::generated::{RecordVersion, Task};

    use super::*;

    #[test]
    fn a_placement_is_the_same_in_every_run() {
        let voters: Vec<WorkerId> = (0..5).map(|i| WorkerId::new(format!("worker-{i}"))).collect();

        let (holders, quorum) = RecordSpace::placement(&TaskId::new("task-1"), &voters, 3);

        assert_eq!(quorum, 2);
        assert_eq!(
            holders,
            ["worker-1", "worker-3", "worker-0"].map(WorkerId::new),
            "the order is fixed by the ids, not by the process"
        );
    }

    fn revision(task: &TaskId, revision: u64, placement: &[WorkerId]) -> TaskRecord {
        TaskRecord {
            version: Some(RecordVersion {
                revision,
                ..Default::default()
            }),
            task: Some(Task {
                task_id: Some(task.clone().into()),
                ..Default::default()
            }),
            placement: placement.iter().cloned().map(Into::into).collect(),
            ..Default::default()
        }
    }

    fn held_revision(space: &RecordSpace, holder: &WorkerId, task: &TaskId) -> Option<u64> {
        space
            .held_by(holder, task)
            .map(|record| record.version.unwrap().revision)
    }

    #[test]
    fn writes_through_a_cut_and_a_down_holder_miss_their_quorum_and_never_replace_a_newer_revision() {
        let [a, b, c] = ["a", "b", "c"].map(WorkerId::new);
        let holders = [a.clone(), b.clone(), c.clone()];
        let task = TaskId::new("task-1");
        let space = RecordSpace::default();
        let now = Instant::at(0);

        // `a` is cut off from `b` and `c` is down: only `a` stores revision 1.
        space.partition(&BTreeSet::from([a.clone()]), &BTreeSet::from([b.clone()]));
        space.set_up(&c, false);
        space.write(&a, revision(&task, 1, &holders), 2, now);
        assert_eq!(held_revision(&space, &a, &task), Some(1));
        assert_eq!(held_revision(&space, &b, &task), None);
        assert_eq!(held_revision(&space, &c, &task), None);

        // Healed, revision 3 reaches everyone; the older revision 2 written
        // after it is refused everywhere, so it neither replaces nor counts
        // as stored.
        space.heal();
        space.set_up(&c, true);
        space.write(&a, revision(&task, 3, &holders), 2, now);
        space.write(&a, revision(&task, 2, &holders), 2, now);
        for holder in &holders {
            assert_eq!(held_revision(&space, holder, &task), Some(3), "{holder:?}");
        }

        let outcomes: Vec<(u64, bool)> = space
            .take_due(now)
            .into_iter()
            .map(|outcome| (outcome.write.version.revision, outcome.stored))
            .collect();
        assert_eq!(outcomes, [(1, false), (3, true), (2, false)]);
        assert!(space.take_due(now).is_empty(), "each outcome is taken once");
    }
}

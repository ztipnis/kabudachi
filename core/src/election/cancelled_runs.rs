//! The runs a leader cancelled on each worker, listed in its acks to that
//! worker a batch at a time.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use crate::protocol::ids::{TaskRunId, WorkerId};

/// The most cancelled runs one ack lists; the rest follow in later acks.
const MAX_CANCELLED_RUNS_PER_ACK: usize = 64;

/// The runs a leader cancelled on one worker, and how far its acks have
/// listed them: each ack lists the next batch after the last run listed,
/// wrapping round, so every run goes out within a few acks however many there
/// are.
#[derive(Default)]
struct CancelledRuns {
    runs: BTreeSet<TaskRunId>,
    last_listed: Option<TaskRunId>,
}

impl CancelledRuns {
    /// The next batch for an ack, advancing the cursor past it.
    fn next_batch(&mut self) -> Vec<TaskRunId> {
        let after = match &self.last_listed {
            Some(last) => (Bound::Excluded(last.clone()), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };
        let wrapped = self
            .last_listed
            .iter()
            .flat_map(|last| self.runs.range(..=last));
        let batch: Vec<TaskRunId> = self
            .runs
            .range(after)
            .chain(wrapped)
            .take(MAX_CANCELLED_RUNS_PER_ACK)
            .cloned()
            .collect();
        if let Some(last) = batch.last() {
            self.last_listed = Some(last.clone());
        }
        batch
    }
}

/// While it holds office: per worker, the runs that worker holds which this
/// leader cancelled and stored, to list in its acks to it.
#[derive(Default)]
pub(super) struct CancelledRunsLedger {
    by_worker: BTreeMap<WorkerId, CancelledRuns>,
}

impl CancelledRunsLedger {
    /// `worker` holds `run`, stored as cancelled.
    pub(super) fn tell(&mut self, worker: WorkerId, run: TaskRunId) {
        self.by_worker.entry(worker).or_default().runs.insert(run);
    }

    /// `worker` holds none of the runs it was told of: its acks stop listing them.
    pub(super) fn forget(&mut self, worker: &WorkerId) {
        self.by_worker.remove(worker);
    }

    /// The next batch to list in an ack to `worker`, advancing its cursor.
    pub(super) fn next_batch_for(&mut self, worker: &WorkerId) -> Vec<TaskRunId> {
        self.by_worker
            .get_mut(worker)
            .map(CancelledRuns::next_batch)
            .unwrap_or_default()
    }

    /// A new office tells only what it cancelled itself.
    pub(super) fn clear(&mut self) {
        self.by_worker.clear();
    }
}

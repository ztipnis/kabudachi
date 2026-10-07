use std::collections::{BTreeMap, BTreeSet};

use super::{Answered, HeldKey, Rebuild, ReconcileTerm, ReportPage, ReportedRun, WorkerRuns};
use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{TaskId, TaskRunId, WorkerId};
use crate::task_record::{RecordVersion, VersionOrder, identify};
use crate::time::{Duration, Instant};

/// Where a worker's next page starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cursor {
    AfterRun(TaskRunId),
    AfterKey(TaskId),
}

/// The pages of one worker's answer received so far.
#[derive(Debug, Clone, Default)]
struct Staged {
    runs: Vec<ReportedRun>,
}

/// One leader's reconciliation: whom it asked, what each answered, the full
/// records it holds, and what it may conclude from them.
#[derive(Debug, Clone)]
pub struct ReconcileRound {
    term: ReconcileTerm,
    started: Instant,
    grace: Duration,
    asked: BTreeSet<WorkerId>,
    answered: BTreeSet<WorkerId>,
    staged: BTreeMap<WorkerId, Staged>,
    /// Answers complete but not yet handed over by `take_settled`.
    unhanded: BTreeMap<WorkerId, WorkerRuns>,
    /// Every distinct version of every task reported, with its placement.
    reported: BTreeMap<TaskId, Vec<HeldKey>>,
    /// Every run id reported for a task, by the runs themselves.
    reported_runs: BTreeMap<TaskId, BTreeSet<TaskRunId>>,
    records: BTreeMap<TaskId, TaskRecord>,
    /// The version of each record already handed over.
    handed: BTreeMap<TaskId, RecordVersion>,
    handed_first: bool,
}

/// Adds `run` to `runs`, replacing any earlier copy with the same run id.
fn keep_newest(runs: &mut Vec<ReportedRun>, run: ReportedRun) {
    match runs
        .iter_mut()
        .find(|known| known.claim.task_run_id == run.claim.task_run_id)
    {
        Some(known) => *known = run,
        None => runs.push(run),
    }
}

impl ReconcileRound {
    /// A round for `term`, asking `reconcilees` (the leader's voters and
    /// pending members, itself included), started at `now`. It may stop
    /// early only once `grace` (one suspicion timeout) has passed.
    pub fn new(
        term: ReconcileTerm,
        reconcilees: impl IntoIterator<Item = WorkerId>,
        now: Instant,
        grace: Duration,
    ) -> Self {
        ReconcileRound {
            term,
            started: now,
            grace,
            asked: reconcilees.into_iter().collect(),
            answered: BTreeSet::new(),
            staged: BTreeMap::new(),
            unhanded: BTreeMap::new(),
            reported: BTreeMap::new(),
            reported_runs: BTreeMap::new(),
            records: BTreeMap::new(),
            handed: BTreeMap::new(),
            handed_first: false,
        }
    }

    pub fn term(&self) -> ReconcileTerm {
        self.term
    }

    /// Asks `worker` too: a worker that became pending after the round began.
    pub fn ask_also(&mut self, worker: WorkerId) {
        self.asked.insert(worker);
    }

    /// Those asked that have not answered in full.
    pub fn unanswered(&self) -> Vec<WorkerId> {
        self.asked.difference(&self.answered).cloned().collect()
    }

    /// Those that answered in full.
    pub fn answered(&self) -> BTreeSet<WorkerId> {
        self.answered.clone()
    }

    /// Takes one page of `from`'s answer to a question asked at `asked_at`,
    /// and says where its next page starts, or `None` once it has answered in
    /// full. A page from a worker not asked is ignored.
    pub fn page(&mut self, from: &WorkerId, page: ReportPage, asked_at: Instant) -> Option<Cursor> {
        if !self.asked.contains(from) {
            return None;
        }
        // A page that is neither the last nor carries anything to continue from
        // is malformed: drop what was staged and leave the worker unanswered,
        // so that it is asked again from the start.
        if !page.last && page.keys.is_empty() && page.runs.is_empty() {
            self.staged.remove(from);
            return None;
        }
        let cursor = if page.last {
            None
        } else if let Some(key) = page.keys.last() {
            Some(Cursor::AfterKey(key.task_id.clone()))
        } else {
            page.runs
                .last()
                .map(|run| Cursor::AfterRun(run.claim.task_run_id.clone()))
        };
        for key in page.keys {
            let versions = self.reported.entry(key.task_id.clone()).or_default();
            if !versions.iter().any(|known| known.version == key.version) {
                versions.push(key);
            }
        }
        for run in &page.runs {
            if let Some(task_id) = run.claim.task.task_id.as_ref() {
                self.reported_runs
                    .entry(TaskId::new(task_id.value.clone()))
                    .or_default()
                    .insert(run.claim.task_run_id.clone());
            }
        }
        // Runs are kept once, by run id; a run sent again replaces the earlier
        // copy, since the worker's newer word on it is the one that counts.
        let staged = self.staged.entry(from.clone()).or_default();
        for run in page.runs {
            keep_newest(&mut staged.runs, run);
        }
        // A page with nothing to continue from ends the answer, whatever it
        // says: there is no cursor to ask for the next one with.
        if cursor.is_some() {
            return cursor;
        }
        let staged = self.staged.remove(from).unwrap_or_default();
        self.answered.insert(from.clone());
        // A complete answer repeated before the earlier one was handed over
        // replaces it: it is the worker's newer word on everything it holds,
        // so a run it lacks is one the worker no longer has.
        self.unhanded.insert(
            from.clone(),
            WorkerRuns {
                runs: staged.runs,
                asked_at: Some(asked_at),
            },
        );
        None
    }

    /// Whether the leader may stop collecting: every voter answered, or, once
    /// the grace has passed, a quorum of them. `answered` is what the leader's
    /// configuration makes of `self.answered()`.
    pub fn may_finish(&self, answered: Answered, now: Instant) -> bool {
        match answered {
            Answered::All => true,
            Answered::Quorum => now >= self.grace_ends_at(),
            Answered::Short => false,
        }
    }

    /// When the grace ends.
    pub fn grace_ends_at(&self) -> Instant {
        self.started + self.grace
    }

    /// The tasks whose newest reported version this round has no full record
    /// of: the leader fetches them. A version whose every holder left the
    /// configuration (`is_member` false) and whose record was not found does
    /// not count: the newest version that was found is the one then.
    pub fn missing_records(&self, is_member: impl Fn(&WorkerId) -> bool) -> Vec<TaskId> {
        self.reported
            .keys()
            .filter(|task| {
                self.newest_reachable(task, &is_member)
                    .is_some_and(|newest| !self.holds_as_new(task, newest.version))
            })
            .cloned()
            .collect()
    }

    /// A full record, from the leader's own store or a lookup. Kept if its
    /// task was reported and it is newer than what the round holds of it. It
    /// may be older than the newest version reported: when every holder of
    /// that one has left, this is the newest that can be found.
    pub fn fetched(&mut self, record: TaskRecord) {
        let Ok((task_id, version)) = identify(&record) else {
            return;
        };
        if !self.reported.contains_key(&task_id) {
            return;
        }
        let held = self
            .records
            .get(&task_id)
            .and_then(|held| identify(held).ok());
        if held.is_some_and(|(_, held)| held.order(&version) == VersionOrder::Older) {
            return;
        }
        self.records.insert(task_id, record);
    }

    /// Everything not yet handed over: on the first call the whole rebuild
    /// (certain records, uncertain tasks, every answer so far with no
    /// `asked_at`); on later calls the records that have become certain since,
    /// every task still uncertain, and the answers that arrived since, each
    /// with when it was asked. `is_member` says whether the leader's roster
    /// still holds a worker.
    pub fn take_settled(&mut self, is_member: impl Fn(&WorkerId) -> bool) -> Rebuild {
        // A record already handed over stays handed over. If a newer version of
        // that task is reported later, the task is uncertain until the newer
        // record is fetched, and then that record is handed over again.
        let mut rebuild = Rebuild::default();
        for task in self.reported.keys() {
            if self.is_certain(task, &is_member) {
                let record = &self.records[task];
                let version = self
                    .record_version(task)
                    .expect("a held record is identified");
                let new = self
                    .handed
                    .get(task)
                    .is_none_or(|handed| handed.order(&version) == VersionOrder::Newer);
                if new {
                    rebuild.records.push(record.clone());
                }
            } else {
                rebuild.uncertain.insert(task.clone(), self.runs_of(task));
                // A task's key never changes, so any reported version names it.
                if let Some(key) = self.reported[task]
                    .iter()
                    .find_map(|held| held.coalescing.clone())
                {
                    rebuild.uncertain_keys.insert(task.clone(), key);
                }
            }
        }
        for record in &rebuild.records {
            if let Ok((task, version)) = identify(record) {
                self.handed.insert(task, version);
            }
        }
        rebuild.reports = std::mem::take(&mut self.unhanded);
        if !self.handed_first {
            self.handed_first = true;
            for runs in rebuild.reports.values_mut() {
                runs.asked_at = None;
            }
        }
        rebuild
    }

    /// Takes back what [`Self::take_settled`] handed over and its caller could
    /// not use: the next call hands it over again, the answers that arrived
    /// meanwhile superseding the ones given back.
    pub fn give_back(&mut self, learnt: Rebuild) {
        for record in &learnt.records {
            if let Ok((task, version)) = identify(record)
                && self.handed.get(&task) == Some(&version)
            {
                self.handed.remove(&task);
            }
        }
        for (worker, runs) in learnt.reports {
            self.unhanded.entry(worker).or_insert(runs);
        }
    }

    /// Whether nothing remains to learn: every worker asked has answered or
    /// left (`is_member` false), and no task is uncertain.
    pub fn is_complete(&self, is_member: impl Fn(&WorkerId) -> bool) -> bool {
        self.asked
            .iter()
            .all(|worker| self.answered.contains(worker) || !is_member(worker))
            && self
                .reported
                .keys()
                .all(|task| self.is_certain(task, &is_member))
    }

    /// The newest version reported for `task` that is not lost: one whose
    /// holders have all left the configuration and whose record the round
    /// does not hold is skipped, for the next newest. If every version is
    /// lost, the newest, which then stays unknown.
    fn newest_reachable(
        &self,
        task: &TaskId,
        is_member: impl Fn(&WorkerId) -> bool,
    ) -> Option<&HeldKey> {
        let mut versions: Vec<&HeldKey> = self.reported.get(task)?.iter().collect();
        versions.sort_by(|a, b| match a.version.order(&b.version) {
            // `order` says where `b` stands against `a`.
            VersionOrder::Newer => std::cmp::Ordering::Less,
            VersionOrder::Older => std::cmp::Ordering::Greater,
            VersionOrder::Same => std::cmp::Ordering::Equal,
        });
        let lost = |key: &HeldKey| {
            !self.holds_as_new(task, key.version)
                && !key.placement.is_empty()
                && !key.placement.iter().any(&is_member)
        };
        versions
            .iter()
            .rev()
            .find(|key| !lost(key))
            .or_else(|| versions.last())
            .copied()
    }

    fn record_version(&self, task: &TaskId) -> Option<RecordVersion> {
        identify(self.records.get(task)?)
            .ok()
            .map(|(_, version)| version)
    }

    fn holds_as_new(&self, task: &TaskId, newest: RecordVersion) -> bool {
        self.record_version(task)
            .is_some_and(|held| held.order(&newest) != VersionOrder::Newer)
    }

    fn runs_of(&self, task: &TaskId) -> BTreeSet<TaskRunId> {
        let named = self.reported[task]
            .iter()
            .filter_map(|key| key.latest_run.clone());
        let claimed = self.reported_runs.get(task).into_iter().flatten().cloned();
        named.chain(claimed).collect()
    }

    /// Whether the newest record reported for a task is known for certain. Its
    /// placement `p` was written at a majority `w = p/2 + 1`, so any `p - w + 1`
    /// of its holders include one that stored it: once that many have answered,
    /// no newer revision can be hiding among the silent ones. Failing that, a
    /// task is also known once every holder still in the configuration has
    /// answered: then a newer revision could only be on holders that have left.
    ///
    /// That holds because holders only move forward. A revision is stored at a
    /// majority of the placement `p_j` of the revision before it, as well as of
    /// its own (it is written jointly when the placement moves), and a holder
    /// that a later revision leaves out keeps a stub of it, which it reports
    /// like a record. So whichever revision `p_j` belongs to, a reader that
    /// hears more than `p_j - w` of its holders meets one that holds, or holds
    /// the stub of, any later revision, and reports it as newer.
    ///
    /// Losing every holder of one write (more than `p - w` of them: one, with
    /// the default replication factor of three) can lose that revision. That is
    /// the tolerance of a majority write, accepted here.
    fn is_certain(&self, task: &TaskId, is_member: impl Fn(&WorkerId) -> bool) -> bool {
        let Some(newest) = self.newest_reachable(task, &is_member) else {
            return false;
        };
        if !self.holds_as_new(task, newest.version) {
            return false;
        }
        let placement = &newest.placement;
        if placement.is_empty() {
            return true;
        }
        let quorum = placement.len() / 2 + 1;
        let heard = placement
            .iter()
            .filter(|holder| self.answered.contains(*holder))
            .count();
        heard > placement.len() - quorum
            || placement
                .iter()
                .filter(|holder| is_member(holder))
                .all(|holder| self.answered.contains(holder))
    }
}

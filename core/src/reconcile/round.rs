use std::collections::{BTreeMap, BTreeSet};

use super::{Answered, HeldKey, Rebuild, ReconcileTerm, ReportPage, ReportedRun, WorkerRuns};
use crate::protocol::ids::{TaskId, TaskRunId, WorkerId};
use crate::task_record::{RecordKey, RecordVersion, ShardRecord, VersionOrder};
use crate::time::{Duration, Instant};

/// Where a worker's next page starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cursor {
    AfterRun(TaskRunId),
    AfterKey(RecordKey),
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
    /// Every distinct version of every record reported, with its placement.
    reported: BTreeMap<RecordKey, Vec<HeldKey>>,
    /// Every run id reported for a task, by the runs themselves.
    reported_runs: BTreeMap<TaskId, BTreeSet<TaskRunId>>,
    records: BTreeMap<RecordKey, ShardRecord>,
    /// The version of each record already handed over.
    handed: BTreeMap<RecordKey, RecordVersion>,
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

/// Whether `page` is neither the last nor carries anything to continue from.
fn is_malformed(page: &ReportPage) -> bool {
    !page.last && page.keys.is_empty() && page.runs.is_empty()
}

/// Where the page after `page` starts: after its last key, or else its last
/// run. `None` once `page` is the last.
fn cursor_after(page: &ReportPage) -> Option<Cursor> {
    if page.last {
        return None;
    }
    if let Some(key) = page.keys.last() {
        return Some(Cursor::AfterKey(key.key.clone()));
    }
    page.runs
        .last()
        .map(|run| Cursor::AfterRun(run.claim.task_run_id.clone()))
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
        if is_malformed(&page) {
            self.staged.remove(from);
            return None;
        }
        let cursor = cursor_after(&page);
        self.note_versions(page.keys);
        self.note_runs(&page.runs);
        self.stage(from, page.runs);
        // A page with nothing to continue from ends the answer, whatever it
        // says: there is no cursor to ask for the next one with.
        if cursor.is_none() {
            self.finish_answer(from, asked_at);
        }
        cursor
    }

    /// Keeps every version of a record `keys` reports that is not known yet,
    /// with its placement.
    fn note_versions(&mut self, keys: Vec<HeldKey>) {
        for key in keys {
            let versions = self.reported.entry(key.key.clone()).or_default();
            if !versions.iter().any(|known| known.version == key.version) {
                versions.push(key);
            }
        }
    }

    /// Notes the id of every run in `runs` under its task.
    fn note_runs(&mut self, runs: &[ReportedRun]) {
        for run in runs {
            if let Some(task_id) = run.claim.task.task_id.as_ref() {
                self.reported_runs
                    .entry(TaskId::new(task_id.value.clone()))
                    .or_default()
                    .insert(run.claim.task_run_id.clone());
            }
        }
    }

    /// Stages `runs` as part of `from`'s answer. Runs are kept once, by run id;
    /// a run sent again replaces the earlier copy, since the worker's newer
    /// word on it is the one that counts.
    fn stage(&mut self, from: &WorkerId, runs: Vec<ReportedRun>) {
        let staged = self.staged.entry(from.clone()).or_default();
        for run in runs {
            keep_newest(&mut staged.runs, run);
        }
    }

    /// `from` has answered in full: what it staged becomes its answer, to be
    /// handed over with when it was asked. A complete answer repeated before
    /// the earlier one was handed over replaces it: it is the worker's newer
    /// word on everything it holds, so a run it lacks is one it no longer has.
    fn finish_answer(&mut self, from: &WorkerId, asked_at: Instant) {
        let staged = self.staged.remove(from).unwrap_or_default();
        self.answered.insert(from.clone());
        self.unhanded.insert(
            from.clone(),
            WorkerRuns {
                runs: staged.runs,
                asked_at: Some(asked_at),
            },
        );
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

    /// The records whose newest reported version this round has no full copy
    /// of: the leader fetches them. A version whose every holder left the
    /// configuration (`is_member` false) and whose record was not found does
    /// not count: the newest version that was found is the one then.
    pub fn missing_records(&self, is_member: impl Fn(&WorkerId) -> bool) -> Vec<RecordKey> {
        self.reported
            .keys()
            .filter(|key| {
                self.newest_reachable(key, &is_member)
                    .is_some_and(|newest| !self.holds_as_new(key, newest.version))
            })
            .cloned()
            .collect()
    }

    /// A full record, from the leader's own store or a lookup. Kept if it
    /// was reported and it is newer than what the round holds of it. It may
    /// be older than the newest version reported: when every holder of that
    /// one has left, this is the newest that can be found.
    pub fn fetched(&mut self, record: impl Into<ShardRecord>) {
        let record = record.into();
        let Ok((key, version)) = record.identify() else {
            return;
        };
        if !self.reported.contains_key(&key) {
            return;
        }
        if self
            .record_version(&key)
            .is_some_and(|held| held.order(&version) == VersionOrder::Older)
        {
            return;
        }
        self.records.insert(key, record);
    }

    /// Everything not yet handed over: on the first call the whole rebuild
    /// (certain records, uncertain tasks and composites, every answer so far
    /// with no `asked_at`); on later calls the records that have become
    /// certain since, everything still uncertain, and the answers that arrived since, each
    /// with when it was asked. `is_member` says whether the leader's roster
    /// still holds a worker.
    pub fn take_settled(&mut self, is_member: impl Fn(&WorkerId) -> bool) -> Rebuild {
        // A record already handed over stays handed over. If a newer version of
        // that task is reported later, the task is uncertain until the newer
        // record is fetched, and then that record is handed over again.
        let mut rebuild = Rebuild::default();
        let mut handing = Vec::new();
        for key in self.reported.keys() {
            if self.is_certain(key, &is_member) {
                handing.extend(self.record_not_handed(key));
            } else {
                self.note_uncertain(key, &mut rebuild);
            }
        }
        for record in handing {
            self.hand_over(record, &mut rebuild);
        }
        rebuild.reports = self.take_answers();
        rebuild
    }

    /// Takes back what [`Self::take_settled`] handed over and its caller could
    /// not use: the next call hands it over again, the answers that arrived
    /// meanwhile superseding the ones given back.
    pub fn give_back(&mut self, learnt: Rebuild) {
        let given_back = learnt
            .records
            .into_iter()
            .map(ShardRecord::Task)
            .chain(learnt.composites.into_iter().map(ShardRecord::Composite))
            .chain(learnt.pages.into_iter().map(ShardRecord::Page));
        for record in given_back {
            if let Ok((key, version)) = record.identify()
                && self.handed.get(&key) == Some(&version)
            {
                self.handed.remove(&key);
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
                .all(|key| self.is_certain(key, &is_member))
    }
}

impl ReconcileRound {
    /// The record under `key`, which the round holds as the newest, unless
    /// that version or a newer one was handed over already.
    fn record_not_handed(&self, key: &RecordKey) -> Option<ShardRecord> {
        let version = self
            .record_version(key)
            .expect("a held record is identified");
        self.handed
            .get(key)
            .is_none_or(|handed| handed.order(&version) == VersionOrder::Newer)
            .then(|| self.records[key].clone())
    }

    /// Notes what `key` names as uncertain in `rebuild`: a task with every
    /// run reported for it and its coalescing key, if it has one; for a
    /// composite's record or page, the composite.
    fn note_uncertain(&self, key: &RecordKey, rebuild: &mut Rebuild) {
        match key {
            RecordKey::Task(task) => {
                rebuild.uncertain.insert(task.clone(), self.runs_of(task));
                // A task's key never changes, so any reported version names it.
                if let Some(coalescing) = self.reported[key]
                    .iter()
                    .find_map(|held| held.coalescing.clone())
                {
                    rebuild.uncertain_keys.insert(task.clone(), coalescing);
                }
            }
            RecordKey::Composite(id) | RecordKey::Page(id, _) => {
                rebuild.uncertain_composites.insert(id.clone());
            }
        }
    }

    /// Puts `record` in the part of `rebuild` its kind belongs to, and notes
    /// its version as handed over.
    fn hand_over(&mut self, record: ShardRecord, rebuild: &mut Rebuild) {
        if let Ok((key, version)) = record.identify() {
            self.handed.insert(key, version);
        }
        match record {
            ShardRecord::Task(record) => rebuild.records.push(record),
            ShardRecord::Composite(record) => rebuild.composites.push(record),
            ShardRecord::Page(record) => rebuild.pages.push(record),
        }
    }

    /// The complete answers not yet handed over. The first hand-over is the
    /// rebuild, which takes every answer so far as one: none carries when it
    /// was asked.
    fn take_answers(&mut self) -> BTreeMap<WorkerId, WorkerRuns> {
        let mut answers = std::mem::take(&mut self.unhanded);
        if !self.handed_first {
            self.handed_first = true;
            for runs in answers.values_mut() {
                runs.asked_at = None;
            }
        }
        answers
    }

    /// The newest version reported for `task` that is not lost: one whose
    /// holders have all left the configuration and whose record the round
    /// does not hold is skipped, for the next newest. If every version is
    /// lost, the newest, which then stays unknown.
    fn newest_reachable(
        &self,
        key: &RecordKey,
        is_member: impl Fn(&WorkerId) -> bool,
    ) -> Option<&HeldKey> {
        let mut versions: Vec<&HeldKey> = self.reported.get(key)?.iter().collect();
        versions.sort_by(|a, b| match a.version.order(&b.version) {
            // `order` says where `b` stands against `a`.
            VersionOrder::Newer => std::cmp::Ordering::Less,
            VersionOrder::Older => std::cmp::Ordering::Greater,
            VersionOrder::Same => std::cmp::Ordering::Equal,
        });
        let lost = |held: &HeldKey| {
            !self.holds_as_new(key, held.version)
                && !held.placement.is_empty()
                && !held.placement.iter().any(&is_member)
        };
        versions
            .iter()
            .rev()
            .find(|held| !lost(held))
            .or_else(|| versions.last())
            .copied()
    }

    fn record_version(&self, key: &RecordKey) -> Option<RecordVersion> {
        self.records
            .get(key)?
            .identify()
            .ok()
            .map(|(_, version)| version)
    }

    fn holds_as_new(&self, key: &RecordKey, newest: RecordVersion) -> bool {
        self.record_version(key)
            .is_some_and(|held| held.order(&newest) != VersionOrder::Newer)
    }

    fn runs_of(&self, task: &TaskId) -> BTreeSet<TaskRunId> {
        let named = self.reported[&RecordKey::Task(task.clone())]
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
    fn is_certain(&self, key: &RecordKey, is_member: impl Fn(&WorkerId) -> bool) -> bool {
        let Some(newest) = self.newest_reachable(key, &is_member) else {
            return false;
        };
        if !self.holds_as_new(key, newest.version) {
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

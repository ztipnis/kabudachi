//! What a worker reports about a run it holds: that it started, and that it succeeded, with its result's
//! digest, and the step a certified run returned.

use prost::Message as _;

use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::TaskRun;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::result_backend::PersistenceState;
use crate::time::{Clock, WallTime};

use super::composite::Step;
use super::{CONTINUATION_TOO_LARGE, Observer, Scheduler, UNVERSIONED};

/// The leader's word that a run's result is the authoritative one. Until a
/// client holds this, result bytes it received are only provisional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Certification {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    pub result_digest: Digest,
}

/// Whether a certified run ends its task.
#[derive(Debug, Clone, PartialEq)]
pub enum Completion {
    /// The task is over: its coalescing key is freed, its payload stops
    /// counting, and it is forgotten `result_ttl` later.
    Final,
    /// The run returned `step`: it is certified with the step's digest as its
    /// result, and the step is stored on the run itself
    /// (`TaskRun.continuation`), in the revision that certifies it. The step
    /// runs as a composite (`CompositeId::continuation_of(run)`) derived from
    /// that run. The task keeps its coalescing key and its memory until that
    /// composite is over, and then finishes.
    Continues(Step),
}

/// What a worker reports with a run's success besides its digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    /// The result reached its task's client first.
    pub delivered: bool,
    /// Whether the worker will mirror the result (`Pending`) or not (`Disabled`).
    pub persistence: PersistenceState,
    /// The digest of the blob holding the result's manifest, when the worker
    /// stored the result in the shard as blobs.
    pub result_manifest: Option<Digest>,
}

impl Default for Reported {
    fn default() -> Self {
        Self {
            delivered: false,
            persistence: PersistenceState::Disabled,
            result_manifest: None,
        }
    }
}

/// What certifying a run records on it besides its digest.
pub(super) struct Recorded {
    pub(super) delivered: bool,
    /// What the run records about mirroring its result; `None` leaves it
    /// unspecified, as when a rebuild adopts a result and cannot know whether
    /// its worker mirrors.
    pub(super) persistence: Option<PersistenceState>,
    /// Recorded only for a final run.
    pub(super) result_manifest: Option<Digest>,
}

/// Why a worker's report about a run was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReportRejection {
    #[error("this node is not the leader")]
    NotLeader,
    #[error("no such run")]
    UnknownRun,
    #[error("the run does not belong to this worker or is not in the expected state")]
    NotAuthoritative,
    /// The run's task has a newest record this leader cannot know yet: ask again.
    #[error("the run's task is not known for certain yet")]
    NotReady,
    /// The step the run returned would take the task's record past the
    /// largest a record may be, so the run failed instead.
    #[error("the step the run returned does not fit the task's record")]
    ContinuationTooLarge,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// The worker that claimed `run_id` reports that it began executing, at
    /// `execution_version` of its input if it migrated it (`None` leaves the
    /// run's own). The same report again, while the run still runs, is taken
    /// and changes nothing.
    pub fn report_started(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        execution_version: Option<u32>,
    ) -> Result<(), ReportRejection> {
        let outcome = self.start_run(worker, run_id, execution_version);
        self.end_call();
        outcome
    }

    fn start_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        execution_version: Option<u32>,
    ) -> Result<(), ReportRejection> {
        self.require_leader()?;
        self.start_owned(worker, run_id, execution_version)
    }

    /// Starts `worker`'s claimed run. A start repeated by the worker of a run
    /// already running (its first went unanswered, or a new leader rebuilt the
    /// run as running) is taken again and changes nothing, so a report sent
    /// again never stops a healthy run.
    pub(super) fn start_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        execution_version: Option<u32>,
    ) -> Result<(), ReportRejection> {
        let already_running = self.runs.get(run_id).is_some_and(|run| {
            run.current_state() == TaskRunState::Running
                && run.selected_worker().as_ref() == Some(worker)
        });
        if already_running {
            return Ok(());
        }
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Claimed)?;
        run.transition_to(TaskRunState::Running, stamped_at)
            .expect("a Claimed run can always start");
        run.started_at = Some(stamped_at.into());
        if let Some(version) = execution_version {
            run.execution_version = version;
        }
        let task_id = run.task_id();
        // A task some worker could run is no longer waiting on one.
        self.blocked.remove(&task_id);
        self.mark_decided(run_id);
        Ok(())
    }

    /// As [`Self::complete_delivered`], for a run whose result went to no
    /// client: a task with none, as in the one-node runtime.
    pub fn complete(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        self.complete_reported(worker, run_id, result_digest, completion, Reported::default())
    }

    /// As [`Self::complete_reported`] for a worker that mirrors nothing.
    pub fn complete_delivered(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        delivered: bool,
    ) -> Result<Certification, ReportRejection> {
        let reported = Reported {
            delivered,
            ..Reported::default()
        };
        self.complete_reported(worker, run_id, result_digest, completion, reported)
    }

    /// The worker running `run_id` reports success with the digest of its
    /// result, whether it delivered the result to the task's client first,
    /// and whether it will mirror the result, all of which the run records.
    /// Only a `Running` run owned by that worker can complete, so a stale or
    /// repeated report is refused and certifies nothing. `completion` says
    /// whether that ends the task.
    pub fn complete_reported(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        reported: Reported,
    ) -> Result<Certification, ReportRejection> {
        let outcome = self.certify(worker, run_id, result_digest, completion, reported);
        self.end_call();
        outcome
    }

    /// Certifies the result of a running run, and finishes its task unless it continues.
    fn certify(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        reported: Reported,
    ) -> Result<Certification, ReportRejection> {
        self.require_leader()?;
        // A compaction run's result is its fold, reported by `complete_compaction`.
        if self
            .runs
            .get(run_id)
            .is_some_and(|run| self.is_compaction(&run.task_id()))
        {
            return Err(ReportRejection::NotAuthoritative);
        }
        let Reported {
            delivered,
            persistence,
            result_manifest,
        } = reported;
        let recorded = Recorded {
            delivered,
            persistence: Some(persistence),
            result_manifest,
        };
        self.certify_owned(worker, run_id, result_digest, completion, recorded)
    }

    /// Certifies `worker`'s running run, recording `recorded` on it.
    pub(super) fn certify_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
        recorded: Recorded,
    ) -> Result<Certification, ReportRejection> {
        let Recorded {
            delivered,
            persistence,
            result_manifest,
        } = recorded;
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        self.owned_run(worker, run_id, TaskRunState::Running)?;
        if let Completion::Continues(step) = &completion {
            if step.digest() != result_digest {
                return Err(ReportRejection::NotAuthoritative);
            }
            if self.continuation_overflows(run_id, step, &result_digest, stamped_at) {
                // The same body would return the same step, so no retry.
                self.fail_for_good(worker, run_id, CONTINUATION_TOO_LARGE);
                return Err(ReportRejection::ContinuationTooLarge);
            }
        }
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Succeeded, stamped_at)
            .expect("a Running run can always succeed");
        run.result_digest = Some(result_digest.clone().into());
        run.delivered = delivered;
        // A step is not a value to mirror: its composite's results are
        // mirrored by its client.
        let persistence = match completion {
            Completion::Continues(_) => persistence.map(|_| PersistenceState::Disabled),
            Completion::Final => persistence,
        };
        if matches!(completion, Completion::Final) {
            run.result_manifest = result_manifest.map(Into::into);
        }
        if let Some(state) = persistence {
            run.persistence = generated::PersistenceState::from(state) as i32;
        }
        // The step is part of the run, so the revision that certifies the run
        // commits the step with it: a leader that loses everything else still
        // finds it here.
        if let Completion::Continues(step) = &completion {
            run.continuation = Some(step.into());
        }
        let task_id = run.task_id();
        self.mark_decided(run_id);
        match completion {
            Completion::Continues(_) => {
                self.continuing.insert(task_id.clone());
                self.commit_continuation(&task_id, run_id);
            }
            Completion::Final => self.record_finished(&task_id, now),
        }
        Ok(Certification {
            task_id,
            task_run_id: run_id.clone(),
            result_digest,
        })
    }

    /// Whether the record of `run_id`'s task would pass the record limit once
    /// the run is certified carrying `step`, measured on the record with the
    /// certified run in place.
    fn continuation_overflows(
        &self,
        run_id: &TaskRunId,
        step: &Step,
        result_digest: &Digest,
        stamped_at: WallTime,
    ) -> bool {
        let task_id = self.runs[run_id].task_id();
        let mut record = self.record_of(&task_id, UNVERSIONED, stamped_at);
        let certified = record
            .runs
            .iter_mut()
            .find(|run| run.task_run_id() == *run_id)
            .expect("a task's record holds each of its runs");
        certified
            .transition_to(TaskRunState::Succeeded, stamped_at)
            .expect("a Running run can always succeed");
        certified.result_digest = Some(result_digest.clone().into());
        certified.continuation = Some(step.into());
        // Certifying a step records that nothing of it is mirrored.
        certified.persistence = generated::PersistenceState::from(PersistenceState::Disabled) as i32;
        record.encoded_len() as u64 > Self::record_limit()
    }

    /// The worker that ran `run_id` says how mirroring its certified result
    /// went (`Stored` or `Failed`). Only that worker's word is taken, and only
    /// for a run that succeeded; the same word again changes nothing, and a
    /// `Failed` word after `Stored` is ignored.
    pub fn note_persistence(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        state: PersistenceState,
    ) -> Result<(), ReportRejection> {
        let outcome = self.note_persistence_owned(worker, run_id, state);
        self.end_call();
        outcome
    }

    fn note_persistence_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        state: PersistenceState,
    ) -> Result<(), ReportRejection> {
        self.require_leader()?;
        if !matches!(state, PersistenceState::Stored | PersistenceState::Failed) {
            return Err(ReportRejection::NotAuthoritative);
        }
        let run = self.owned_run(worker, run_id, TaskRunState::Succeeded)?;
        let wire = generated::PersistenceState::from(state) as i32;
        let stored = generated::PersistenceState::Stored as i32;
        if run.persistence == wire || (run.persistence == stored && state == PersistenceState::Failed) {
            return Ok(());
        }
        run.persistence = wire;
        self.mark_decided(run_id);
        Ok(())
    }

    /// The step a certified run returned, read from the run.
    pub fn continuation_of_run(&self, run: &TaskRunId) -> Option<Step> {
        let description = self.runs.get(run)?.continuation.as_ref()?;
        match Step::try_from(description) {
            Ok(step) => Some(step),
            Err(malformed) => {
                // A leader wrote it from a step it had validated, so this is a bug.
                tracing::error!(run = run.as_str(), %malformed, "a run's stored step cannot be read");
                None
            }
        }
    }

    /// `task_id`'s continuation is over, however it ended: the task is
    /// finished after all. Says whether it had a continuation to end, so
    /// ending twice, or a task with none, changes nothing.
    pub(super) fn finish_continuation(&mut self, task_id: &TaskId) -> bool {
        if !self.continuing.remove(task_id) {
            return false;
        }
        let now = self.clock.now();
        self.record_finished(task_id, now);
        true
    }

    /// `run_id`'s run, if the run exists, `worker` claimed it and it is in
    /// `expected` state. Does not ask whether this node leads: a rebuild
    /// applies what workers report before it holds a grant.
    pub(super) fn owned_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        expected: TaskRunState,
    ) -> Result<&mut TaskRun, ReportRejection> {
        if !self.runs.contains_key(run_id) {
            let uncertain = self.reconciliation.holds_uncertain_run(run_id);
            return Err(if uncertain {
                ReportRejection::NotReady
            } else {
                ReportRejection::UnknownRun
            });
        }
        let run = self.runs.get_mut(run_id).expect("just seen");
        let owned = run.selected_worker().as_ref() == Some(worker);
        // A run replaced by a retry or a replay is `Failed` or `Lost`, which no
        // report expects, so ownership and state are enough.
        if !owned || run.current_state() != expected {
            return Err(ReportRejection::NotAuthoritative);
        }
        Ok(run)
    }
}

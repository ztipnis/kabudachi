//! How a new leader's scheduler rebuilds from what its shard holds: it installs
//! every record known for certain, holds back the uncertain ones, applies what
//! workers report about their runs, and takes what it learns late through the
//! same rules.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::generated::{CompositePage, CompositeRecord, TaskRecord};
use crate::protocol::ids::{CompositeId, IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::reconcile::{Rebuild, ReconcileTerm};
use crate::time::{Clock, WallTime};

use super::{Certification, Failure, LostRun, Observer, Scheduler};
use install::{chain_of, submission_order};
use settle::Settled;

mod answers;
mod install;
mod settle;

/// Why `Scheduler::reconcile` refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReconcileRejection {
    #[error("this scheduler is not reconciling")]
    NotReconciling,
    #[error("this scheduler has already rebuilt for its office")]
    AlreadyRebuilt,
}

/// A rebuild the scheduler did not take, handed back with the reason.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{rejection}")]
pub struct ReconcileRefused {
    pub rejection: ReconcileRejection,
    pub rebuild: Rebuild,
}

/// What adopting late knowledge did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Adopted {
    /// Tasks whose records were installed.
    pub installed: usize,
    pub lost: Vec<LostRun>,
    pub certified: Vec<Certification>,
    pub failed: Vec<Failure>,
    /// Workers holding a claimed or running run among the installed records
    /// that have not answered: the election must watch them as lost workers.
    pub silent_holders: BTreeSet<WorkerId>,
    /// Workers that answered: they have been heard, so the silence their
    /// node reported for any of them is over (its node must count them heard
    /// now, see `Input::WatchWorkers`).
    pub answered: BTreeSet<WorkerId>,
}

/// What a rebuild did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciled {
    /// Records written again at the new term.
    pub republished: usize,
    /// Tasks held back until their newest record is known.
    pub uncertain: usize,
    /// Workers holding a claimed or running run among the installed records
    /// that did not answer. They may be dead with the old leader and may
    /// never be heard from, so the election must watch them as lost workers.
    pub silent_holders: BTreeSet<WorkerId>,
    /// Workers that answered: they have been heard, so the silence their
    /// node reported for any of them is over (its node must count them heard
    /// now, see `Input::WatchWorkers`).
    pub answered: BTreeSet<WorkerId>,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Its node took office for `term` and reconciles before it leads.
    /// Until the grant of that office arrives, the silences its node reports
    /// are kept, not acted on, and the scheduler,
    /// holding no grant, refuses every claim and report as `NotLeader`. A
    /// grant of `None` meanwhile (the node left office) drops the
    /// reconciliation. It forgets every silence its node reported in an
    /// earlier office: a worker silent then may have been heard by another
    /// leader since, and the new office counts every worker afresh.
    pub fn begin_reconcile(&mut self, term: ReconcileTerm) {
        self.silent.clear();
        self.leadership.hold(None);
        self.check_leader();
        self.reconciliation.begin(term);
    }

    /// The office it reconciles for, until that office's grant arrives.
    pub fn reconciling(&self) -> Option<ReconcileTerm> {
        self.reconciliation.office()
    }

    /// Replaces everything this scheduler holds with what `rebuild` says,
    /// never merging with what it held before: every certain record is
    /// installed as its newest revision says (runs, waiting room with
    /// deadlines from its wall-clock times, retention, coalescing occupancy
    /// and chains, memory use, losses), every uncertain task is held back,
    /// and then every installed record is republished at the office's term,
    /// which fences any late write of an earlier leader.
    ///
    /// When the office is no longer the one the scheduler may publish under
    /// (a stale office, whose grant was superseded), the rebuilt state is
    /// still installed but nothing is republished and `republished` is 0:
    /// there is no live term left to fence a late write at.
    pub fn reconcile(&mut self, rebuild: Rebuild) -> Result<Reconciled, ReconcileRefused> {
        if let Some(rejection) = self.reconciliation.refusal() {
            return Err(ReconcileRefused { rejection, rebuild });
        }
        let Rebuild {
            records,
            composites,
            pages,
            uncertain,
            uncertain_keys,
            uncertain_composites,
            reports,
        } = rebuild;
        self.clear_tasks();
        let office = self.reconciliation.rebuilt(
            reports.keys().cloned().collect(),
            uncertain,
            uncertain_keys,
            uncertain_composites,
        );
        self.install_settled(records, false);
        self.install_composites(composites, pages, false);
        // What applying the reports did (runs lost, certified or failed) is
        // not reported back: the caller learns of it from the revisions the
        // scheduler publishes and from the events it queues.
        let mut applied = Adopted::default();
        self.apply_reports(reports, &mut applied);
        self.recover_composites();
        let republished = if self
            .leadership
            .may_publish_under(office.recovery_epoch, office.term)
        {
            self.publish_unpublished(office.recovery_epoch, office.term)
        } else {
            0
        };
        let silent_holders = self
            .holders_of(self.current_run.keys())
            .difference(self.reconciliation.answered())
            .cloned()
            .collect();
        let answered = self.reconciliation.answered().clone();
        for worker in &answered {
            self.silent.remove(worker);
        }
        Ok(Reconciled {
            republished,
            uncertain: self.reconciliation.uncertain_count(),
            silent_holders,
            answered,
        })
    }

    /// While leading: takes what reconciliation learnt after the rebuild
    /// (records that became certain, answers that came late, a worker's
    /// re-report) through the same table as the rebuild. A record is installed
    /// only for a task this leader does not hold; what it already holds it
    /// decided itself. Changes publish as any call's do.
    ///
    /// A scheduler that does not lead (its grant has not arrived, or ended:
    /// the lease may run out, or the recovery fence lapse, while the node
    /// still holds office) takes nothing and hands `learnt` back. The round
    /// has already given that knowledge up, so the caller gives it back to the
    /// round, which offers it again once the scheduler leads.
    pub fn adopt(&mut self, learnt: Rebuild) -> Result<Adopted, Rebuild> {
        if !self.check_leader() {
            return Err(learnt);
        }
        let Rebuild {
            records,
            composites,
            pages,
            uncertain,
            uncertain_keys,
            uncertain_composites,
            reports,
        } = learnt;
        let tasks = &self.tasks;
        let candidates = self
            .reconciliation
            .learn(records, uncertain, uncertain_keys, |task| {
                tasks.contains_key(task)
            });
        let mut adopted = Adopted::default();
        let installed = self.install_settled(candidates, true);
        adopted.installed = installed.len();
        let learnt_composites = self.install_composites(composites, pages, true);
        let held = &self.composites.held;
        self.reconciliation
            .learn_composites(&learnt_composites, uncertain_composites, |id| held.contains_key(id));
        let reporters: BTreeSet<WorkerId> = reports.keys().cloned().collect();
        for worker in &reporters {
            self.silent.remove(worker);
        }
        let mut answered = reporters.clone();
        answered.extend(self.reconciliation.answered().iter().cloned());
        for task_id in &installed {
            for (worker, run) in self.reconciliation.take_reports_for(task_id) {
                answered.insert(worker.clone());
                self.apply_reported(&worker, run, &mut adopted);
            }
        }
        self.apply_reports(reports, &mut adopted);
        self.recover_composites();
        adopted.silent_holders = self
            .holders_of(installed.iter())
            .difference(&answered)
            .cloned()
            .collect();
        adopted.answered = reporters;
        self.end_call();
        Ok(adopted)
    }

    /// The runs this leader believes `worker` holds: claimed or running, and
    /// selected for it, and those of tasks held back as uncertain that it
    /// reported as claimed or running (a heartbeat digest covers no others).
    /// What `worker`'s heartbeat digest is compared with.
    pub fn active_runs_of(&self, worker: &WorkerId) -> Vec<TaskRunId> {
        self.held_by(worker)
            .iter()
            .map(|task_id| self.current_run[task_id].clone())
            .chain(self.reconciliation.active_runs_reported_by(worker))
            .collect()
    }

    /// Installs the records of `records` that may be installed (see
    /// [`Self::settle`]) and holds the others back, with their tasks marked
    /// uncertain. Returns the tasks installed. `late` says this is not the
    /// rebuild: only the coalescing keys of what is installed are restored.
    fn install_settled(&mut self, records: Vec<TaskRecord>, late: bool) -> Vec<TaskId> {
        let Settled {
            mut install,
            held_back,
            finish_live,
            finished_in_batch,
        } = self.settle(records);
        let now = self.clock.now();
        let wall_now = WallTime::now(&self.clock);
        install.sort_by_cached_key(submission_order);
        let mut kept_chains = BTreeMap::new();
        let mut installed = Vec::new();
        for record in install {
            if let Some(task) = record.task.as_ref() {
                kept_chains.insert(task.task_id(), chain_of(&record));
                installed.push(task.task_id());
            }
            self.install(record, now, wall_now);
        }
        self.reconciliation.installed(&installed);
        for record in held_back {
            self.reconciliation.hold_back(record);
        }
        for older in finished_in_batch {
            self.conclude(&older);
        }
        for (older, newer) in finish_live {
            self.supersede(&older, &newer);
        }
        let touched = late.then(|| {
            installed
                .iter()
                .filter_map(|task_id| self.coalescing_key_of(task_id))
                .collect()
        });
        self.restore_keys(&kept_chains, touched.as_ref());
        self.restore_compactions();
        self.update_pressure();
        installed
    }

    /// Holds the composites of `composites`, each with the outcomes of its
    /// `pages`; when `late`, only those not held already, which this leader
    /// decided itself. Returns the composites held.
    fn install_composites(
        &mut self,
        composites: Vec<CompositeRecord>,
        pages: Vec<CompositePage>,
        late: bool,
    ) -> Vec<CompositeId> {
        let now = self.clock.now();
        let mut pages_of: BTreeMap<CompositeId, Vec<CompositePage>> = BTreeMap::new();
        for page in pages {
            if let Some(id) = page.composite_id.clone() {
                pages_of.entry(CompositeId::from(id)).or_default().push(page);
            }
        }
        let mut installed = Vec::new();
        for record in composites {
            let Some(id) = record.composite_id.clone().map(CompositeId::from) else {
                continue;
            };
            if late && self.composites.held.contains_key(&id) {
                continue;
            }
            let pages = pages_of.remove(&id).unwrap_or_default();
            if self.install_composite(record, &pages, now) {
                // Written again at the office's term, as every installed
                // Task record is.
                self.composites.mark_unpublished(&id);
                installed.push(id);
            }
        }
        self.release_orphaned_members();
        self.update_pressure();
        installed
    }

    /// Forgets every task, run and what hangs on them, keeping the result
    /// TTL and the memory limits.
    fn clear_tasks(&mut self) {
        self.tasks.clear();
        self.runs.clear();
        self.current_run.clear();
        self.runs_of_task.clear();
        self.waiting.clear();
        self.retention.clear();
        self.occupancy.clear();
        self.budget.reset_usage();
        self.member_inputs.clear();
        self.continuing.clear();
        self.losses.clear();
        self.input_digests.clear();
        self.links.clear();
        self.unpublished.clear();
        self.events.clear();
        self.held_cancels.clear();
        self.conclusions.clear();
        self.run_decided_at.clear();
        self.failed_compactions.clear();
        self.turns.clear();
        self.running.clear();
        self.composites.clear();
    }
}

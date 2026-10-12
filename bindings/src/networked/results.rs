//! A task's result on its way to the worker that awaits it: delivered from
//! here before its run is reported, and what arrives here for the tasks
//! submitted here, asked of a new leader when the leader changes.

use std::sync::Arc;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    DeliverResult, OutcomeAnswer, TaskResponse, task_response,
};
use kabudachi_core::protocol::task::TaskRunState as DomainRunState;
use kabudachi_net::executor::Report;
use kabudachi_net::result_transport::ResultEvent;

use super::seam::{HeldRun, Seam, Shared};
use crate::work::PyEvent;

/// Delivers a run's result to its client, then reports the run completed,
/// saying whether the result arrived. The run's reconnect timeout bounds the
/// delivery: past it the leader may replay the run anyway.
pub(super) async fn deliver_then_report(
    shared: Arc<Shared>,
    client: WorkerId,
    held: HeldRun,
    run: TaskRunId,
    digest: Digest,
    payload: Vec<u8>,
) {
    let delivery = DeliverResult {
        task_id: Some(held.task.into()),
        task_run_id: Some(run.clone().into()),
        result_digest: Some(digest.clone().into()),
        payload,
    };
    let delivered = shared
        .net
        .deliver_result(client, delivery, held.reconnect_timeout)
        .await
        .is_ok();
    shared.report(Report::Completed {
        run,
        digest,
        delivered,
    });
}

impl Shared {
    /// Hands Python what arrived for a task submitted here: a result, a
    /// certification, or how the task ended without one.
    pub(super) fn on_result(&self, event: ResultEvent) {
        let told = {
            let mut seam = self.seam();
            let event = seam.told(event);
            let told = event.is_some();
            seam.events.extend(event);
            told
        };
        if told {
            self.wake();
        }
    }
}

impl Seam {
    /// What Python is told of `event`; a task whose outcome it settles is no
    /// longer awaited.
    fn told(&mut self, event: ResultEvent) -> Option<PyEvent> {
        match event {
            ResultEvent::Delivered {
                task, run, payload, ..
            } => Some(PyEvent::result(task.as_str(), run.as_str(), payload)),
            ResultEvent::Certified { task, run, digest } => {
                self.awaiting.remove(&task);
                Some(PyEvent::certified(
                    task.as_str(),
                    run.as_str(),
                    digest.value().to_vec(),
                ))
            }
            ResultEvent::Concluded { task, outcome } => {
                let event = outcome_event(&task, &outcome)?;
                self.awaiting.remove(&task);
                Some(event)
            }
        }
    }
}

/// What Python is told of `task` from `answer`: a certification, an end
/// without a result, or that the shard does not know it; nothing while it
/// is not over.
fn outcome_event(task: &TaskId, answer: &OutcomeAnswer) -> Option<PyEvent> {
    if !answer.known {
        return Some(PyEvent::outcome(
            task.as_str(),
            "",
            None,
            String::new(),
            None,
        ));
    }
    if !answer.finished {
        return None;
    }
    let run = answer
        .task_run_id
        .clone()
        .map(|run| TaskRunId::from(run).as_str().to_owned())
        .unwrap_or_default();
    let state = DomainRunState::try_from(answer.state()).ok()?;
    if state == DomainRunState::Succeeded {
        let digest = answer.result_digest.as_ref()?.value.clone();
        return Some(PyEvent::certified(task.as_str(), &run, digest));
    }
    Some(PyEvent::outcome(
        task.as_str(),
        &run,
        Some(state.into()),
        answer.failure_kind.clone(),
        answer
            .superseded_by
            .clone()
            .map(|newer| TaskId::from(newer).as_str().to_owned()),
    ))
}

/// Whenever the leader this worker's node names changes to another, asks it
/// how each task submitted here, stored and still awaited, stands: a
/// certification or an ending the old leader decided may never have been
/// told. The answers come in as any conclusion does.
pub(super) async fn ask_after_leader_change(shared: Arc<Shared>) {
    let mut observed = shared.observed.clone();
    let mut named = observed.borrow().leader.clone().map(|(leader, _)| leader);
    while observed.changed().await.is_ok() {
        let now = observed
            .borrow_and_update()
            .leader
            .clone()
            .map(|(leader, _)| leader);
        if now.is_none() || now == named {
            continue;
        }
        let had_one = named.is_some();
        named = now;
        if had_one {
            ask_about_awaited(&shared).await;
        }
    }
}

/// Asks the leader how each task submitted here, stored and still awaited,
/// stands, and hands Python each answer that settles one.
async fn ask_about_awaited(shared: &Shared) {
    let tasks: Vec<TaskId> = {
        let seam = shared.seam();
        seam.awaiting
            .difference(&seam.delivering)
            .cloned()
            .collect()
    };
    for task in tasks {
        let answer = shared
            .net
            .query_outcome_at_leader(task.clone(), shared.retry)
            .await;
        if let Some(TaskResponse {
            result: Some(task_response::Result::Outcome(outcome)),
        }) = answer
        {
            shared.on_result(ResultEvent::Concluded { task, outcome });
        }
    }
}

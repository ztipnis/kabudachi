//! A task's result reaches the worker that awaits it, over real sockets.
//! The worker that ran it delivers it, then reports the run delivered, and
//! the leader certifies the run to the client once the report is stored.
//! A client cut off from the runner still hears the run certified, with no
//! result. A task that ends without a result is told to its client, and a
//! new leader certifies a run it adopted and answers how a task stands.

use std::time::Duration as StdDuration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    DeliverResult, OutcomeAnswer, TaskRejectReason, task_response,
};
use kabudachi_core::scheduler::Submission;
use kabudachi_net::executor::Report;
use kabudachi_net::messenger::Net;
use kabudachi_net::result_transport::{DeliveryError, ResultEvent};

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::executor::FakeExecutor;
use crate::support::records::{ThreeVoters, Voters, holding, plain_with, submitted_through};
use crate::support::worker::poll_until;

const RESULT: &[u8] = b"the-result";

fn digest() -> Digest {
    Digest::blake3(RESULT)
}

fn delivery(task: &TaskId, run: &TaskRunId) -> DeliverResult {
    DeliverResult {
        task_id: Some(task.clone().into()),
        task_run_id: Some(run.clone().into()),
        result_digest: Some(digest().into()),
        payload: RESULT.to_vec(),
    }
}

fn delivered_event(task: &TaskId, run: &TaskRunId) -> Vec<ResultEvent> {
    vec![ResultEvent::Delivered {
        task: task.clone(),
        run: run.clone(),
        digest: digest(),
        payload: RESULT.to_vec(),
    }]
}

fn certified(task: &TaskId, run: &TaskRunId) -> Vec<ResultEvent> {
    vec![ResultEvent::Certified {
        task: task.clone(),
        run: run.clone(),
        digest: digest(),
    }]
}

/// The next `count` result events about `task` that `client` takes.
async fn events_about(client: &Net, task: &TaskId, count: usize) -> Vec<ResultEvent> {
    let mut found = Vec::new();
    while found.len() < count {
        client.result_arrived().await;
        found.extend(
            client
                .take_result_events()
                .into_iter()
                .filter(|event| match event {
                    ResultEvent::Delivered { task: of, .. }
                    | ResultEvent::Certified { task: of, .. }
                    | ResultEvent::Concluded { task: of, .. } => of == task,
                }),
        );
    }
    found
}

/// The leader's answer to how `task` stands, asked until it is not
/// refused for now (a new leader still reconciling).
async fn outcome_of(client: &Net, leader: &WorkerId, task: &TaskId) -> OutcomeAnswer {
    loop {
        if let Ok(response) = client.query_outcome(leader.clone(), task.clone()).await {
            match response.result {
                Some(task_response::Result::Outcome(answer)) => return answer,
                Some(task_response::Result::Reject(reject))
                    if matches!(
                        TaskRejectReason::try_from(reject.reason),
                        Ok(TaskRejectReason::TaskRejectNotLeader
                            | TaskRejectReason::TaskRejectNotReady)
                    ) => {}
                other => panic!("not an outcome: {other:?}"),
            }
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
}

/// Three voters with fake executors, one of them leading, a client pending
/// at the leader, and the voter that runs the client's tasks.
struct Delivering {
    shard: Voters,
    client: Net,
    executors: Vec<FakeExecutor>,
    leader: usize,
    runner: usize,
}

impl Delivering {
    async fn start() -> Self {
        let (mut shard, client) = ThreeVoters::start().await;
        let executors = (0..3)
            .map(|voter| {
                let (executor, endpoint) = FakeExecutor::new();
                shard.set_executor(voter, endpoint);
                executor
            })
            .collect();
        let leader = shard.drive_until_a_leader().await;
        shard.join_as_pending(&client, leader).await;
        let runner = shard.others(leader)[0];
        Self {
            shard,
            client,
            executors,
            leader,
            runner,
        }
    }

    fn me(&self) -> WorkerId {
        self.client.local_worker_id()
    }

    /// Submits `submission` for the client to await, and starts its run on
    /// the runner; the claim names the client.
    async fn started(&mut self, submission: Submission) -> (TaskId, TaskRunId) {
        let me = self.me();
        let leader = self.shard.id(self.leader);
        let task = self
            .shard
            .drive_until(submitted_through(
                &self.client,
                &leader,
                submission.with_client(me.clone()),
            ))
            .await;
        // Room offered once the task is queued is claimed at once; room
        // offered earlier finds nothing and waits for the next claim round.
        self.executors[self.runner].grant(1);
        let (run, claim) = self
            .shard
            .drive_until(self.executors[self.runner].next_claim())
            .await;
        assert_eq!(claim.task.as_ref().and_then(|task| task.client()), Some(me));
        self.report(Report::Started(run.clone()));
        (task, run)
    }

    fn report(&self, report: Report) {
        self.executors[self.runner].report(report);
    }

    fn completed(&self, run: &TaskRunId, delivered: bool) {
        self.report(Report::Completed {
            run: run.clone(),
            digest: digest(),
            delivered,
        });
    }

    /// The runner delivers `run`'s result while the client takes the event
    /// (which acknowledges it); returns the events the client took.
    async fn delivered(&mut self, task: &TaskId, run: &TaskRunId) -> Vec<ResultEvent> {
        let runner = self.shard.nets[self.runner].clone();
        let (me, client) = (self.me(), &self.client);
        let (sent, taken) = self
            .shard
            .drive_until(async {
                tokio::join!(
                    runner.deliver_result(me, delivery(task, run), StdDuration::from_secs(5)),
                    events_about(client, task, 1)
                )
            })
            .await;
        assert_eq!(sent, Ok(()));
        taken
    }

    async fn next_events(&mut self, task: &TaskId) -> Vec<ResultEvent> {
        self.shard
            .drive_until(events_about(&self.client, task, 1))
            .await
    }
}

/// The runner delivers, the client takes it, the runner reports the run
/// delivered, and the leader certifies it to the client once a majority
/// stored the report.
async fn a_delivered_result_is_certified(at: &mut Delivering) {
    let (task, run) = at.started(plain_with(b"delivered")).await;
    assert_eq!(
        at.delivered(&task, &run).await,
        delivered_event(&task, &run)
    );
    at.completed(&run, true);
    assert_eq!(at.next_events(&task).await, certified(&task, &run));
    let leader = at.shard.nets[at.leader].clone();
    let record = at
        .shard
        .drive_until(leader.get_record(task.clone()))
        .await
        .expect("the leader's shard holds the record");
    assert!(record.runs.last().unwrap().delivered);
}

/// A worker that is its own task's client delivers to itself, with no
/// socket in between.
async fn a_result_for_this_worker_is_delivered_to_itself(at: &mut Delivering) {
    let leader = at.shard.nets[at.leader].clone();
    let (task, run) = (TaskId::new("own"), TaskRunId::new("own-run"));
    let sent = leader
        .deliver_result(
            at.shard.id(at.leader),
            delivery(&task, &run),
            StdDuration::from_secs(5),
        )
        .await;
    assert_eq!(sent, Ok(()));
    assert_eq!(leader.take_result_events(), delivered_event(&task, &run));
}

/// A client cut off from the runner, not from the leader: the delivery is
/// never acknowledged, the run is reported undelivered, and the client still
/// hears it certified, with no result.
async fn a_client_cut_off_from_the_runner_hears_the_run_certified(at: &mut Delivering) {
    let runner_id = at.shard.id(at.runner);
    at.client.block_peer(runner_id.clone());
    let (task, run) = at.started(plain_with(b"cut-off")).await;
    let runner = at.shard.nets[at.runner].clone();
    // Past the first resend (100 ms) and short of the second, so the delivery
    // is asked again once before it gives up.
    let sent = at
        .shard
        .drive_until(runner.deliver_result(
            at.me(),
            delivery(&task, &run),
            StdDuration::from_millis(200),
        ))
        .await;
    assert_eq!(sent, Err(DeliveryError::Unreached));
    at.completed(&run, false);
    assert_eq!(at.next_events(&task).await, certified(&task, &run));
    at.client.unblock_peer(runner_id);
}

/// A task that ends without a result: an ephemeral task whose run is lost
/// is told to its client as lost.
async fn a_task_that_ends_without_a_result_is_concluded(at: &mut Delivering) {
    let (task, run) = at.started(plain_with(b"lost").ephemeral()).await;
    at.report(Report::Lost(run.clone()));
    let [ResultEvent::Concluded { outcome, .. }]: [ResultEvent; 1] =
        at.next_events(&task).await.try_into().unwrap()
    else {
        panic!("the lost task was not concluded");
    };
    assert_eq!(
        (
            outcome.known,
            outcome.finished,
            outcome.state(),
            outcome.task_run_id.map(TaskRunId::from)
        ),
        (true, true, TaskRunState::Lost, Some(run))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn results_reach_their_client_and_the_leader_tells_it_how_each_task_ended() {
    within_deadline(async {
        let mut at = Delivering::start().await;
        a_delivered_result_is_certified(&mut at).await;
        a_result_for_this_worker_is_delivered_to_itself(&mut at).await;
        a_client_cut_off_from_the_runner_hears_the_run_certified(&mut at).await;
        a_task_that_ends_without_a_result_is_concluded(&mut at).await;
    })
    .await;
}

/// A run whose result was delivered, running once a majority stored it so.
async fn a_running_run_whose_result_was_delivered(at: &mut Delivering) -> (TaskId, TaskRunId) {
    let (task, run) = at.started(plain_with(b"adopted")).await;
    let nets = at.shard.nets.clone();
    let stored = task.clone();
    at.shard
        .drive_until(poll_until("the leader stored the run running", || {
            holding(&nets, &stored, &[TaskRunState::Running]) >= 2
        }))
        .await;
    assert_eq!(
        at.delivered(&task, &run).await,
        delivered_event(&task, &run)
    );
    (task, run)
}

/// The leader dies and only then is the run reported completed, so no leader
/// takes the report; returns the survivor that leads next. Waits on the
/// voters' states alone: a new leader's reconciliation lives in its driver,
/// which a command run on it would restart.
async fn the_leader_dies_before_the_run_is_reported(at: &mut Delivering, run: &TaskRunId) -> usize {
    at.shard.kill(at.leader);
    at.completed(run, true);
    let watch = at.shard.watch();
    at.shard
        .drive_until(async move {
            wait_until(|| watch.leader().is_some()).await;
            watch.leader().expect("a survivor leads")
        })
        .await
}

/// The new leader adopts the run from the runner's reconcile page, with
/// `delivered` as the runner's ledger holds it, and certifies it to the
/// client.
async fn the_new_leader_certifies_the_adopted_run(
    at: &mut Delivering,
    next: usize,
    task: &TaskId,
    run: &TaskRunId,
) {
    assert_eq!(at.next_events(task).await, certified(task, run));
    let next_net = at.shard.nets[next].clone();
    let record = at
        .shard
        .drive_until(next_net.get_record(task.clone()))
        .await
        .expect("the new leader's shard holds the adopted record");
    assert!(
        record.runs.last().unwrap().delivered,
        "adoption dropped `delivered`"
    );
}

/// The new leader answers how a task stands, idempotently, and says it does
/// not know a task never stored, to a client that is not a member of its
/// shard there.
async fn the_new_leader_answers_how_tasks_stand(at: &mut Delivering, next: usize, task: &TaskId) {
    let next_id = at.shard.id(next);
    for _ in 0..2 {
        let answer = at
            .shard
            .drive_until(outcome_of(&at.client, &next_id, task))
            .await;
        assert_eq!(
            (
                answer.known,
                answer.finished,
                answer.state(),
                answer.result_digest
            ),
            (true, true, TaskRunState::Succeeded, Some(digest().into()))
        );
    }
    let never = TaskId::new("never-submitted");
    let unknown = at
        .shard
        .drive_until(outcome_of(&at.client, &next_id, &never))
        .await;
    assert!(!unknown.known);
    let me = at.me();
    assert!(
        !at.shard
            .with(next, move |node, _| node.is_voter_or_pending(&me))
            .await,
        "the client asked as a stranger to the new leader"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn results_survive_a_leader_change() {
    within_deadline(async {
        let mut at = Delivering::start().await;
        let (task, run) = a_running_run_whose_result_was_delivered(&mut at).await;
        let next = the_leader_dies_before_the_run_is_reported(&mut at, &run).await;
        the_new_leader_certifies_the_adopted_run(&mut at, next, &task, &run).await;
        the_new_leader_answers_how_tasks_stand(&mut at, next, &task).await;
    })
    .await;
}

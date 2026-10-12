use std::time::Duration;

use kabudachi_core::election::{AuthorityReply, Input, Issuer, Step, WorkerNode};
use kabudachi_core::protocol::ids::{IdGenerator, TaskRunId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::active_runs_digest;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::{LeaderRecords, RecordOutbox, Settled, Turn, office_to_reconcile};
use kabudachi_core::time::{Clock, Instant};
use tokio::time::Instant as TokioInstant;

use super::DriverConfig;
use super::answers::{
    Local, respond_to_claim_requests, respond_to_join_requests, respond_to_reconcile_requests,
    respond_to_steal_requests, respond_to_task_requests, send_answer,
};
use super::events::{catch_up_if_due, drain_events};
use super::own_work::drive_executor;
use super::stepper::{Collected, Stepper};
use crate::authority::AuthorityClient;
use crate::executor::{Executing, ExecutorEndpoint, HostAbort};
use crate::handoff::{HandedOff, hand_off_held_records};
use crate::leader_search::{DrivenSearch, JoinOverNet};
use crate::messenger::Net;
use crate::reconcile::leader::LeaderReconciliation;
use crate::routing_refresh::{RoutingRefresh, ShardView};
use kabudachi_core::election::HandOffTo;

/// One run of [`super::run_driver`]: everything it keeps between batches.
pub(super) struct DriverRun<'a, C: Clock, I: IdGenerator, O> {
    stepper: Stepper<'a, C, I, O>,
    net: &'a Net,
    clock: C,
    config: DriverConfig,
    /// The driver's side of the executor, for this run of the driver.
    executing: Option<Executing<'a>>,
    /// The leader search: a rejoin while the node is back in
    /// `Bootstrapping` or `Joining`, or a stranded node's search for a
    /// leader to reconnect to.
    search: DrivenSearch<'a, JoinOverNet<'a>>,
    /// While the node holds an office whose scheduler has not been
    /// rebuilt, or has been but still awaits late answers: the office's
    /// reconciliation.
    reconciliation: Option<LeaderReconciliation<'a>>,
    routing: RoutingRefresh,
    /// The step the node still has to have carried out, until the first
    /// batch carries it out.
    first: Option<Step>,
    next_deadline: Option<Instant>,
    /// The scheduler's own next deadline, as the last batch left it.
    scheduler_deadline: Option<Instant>,
    /// The reply whose arrival ended the last sleep, if one did.
    woken_by: Option<AuthorityReply>,
}

impl<'a, C, I, O> DriverRun<'a, C, I, O>
where
    C: Clock,
    I: IdGenerator,
    O: FnMut(&WorkerNode<C>, Option<&Input>, &Step),
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn start(
        node: &'a mut WorkerNode<C>,
        first: Step,
        net: &'a Net,
        scheduler: &'a mut Scheduler<C, I, RecordOutbox>,
        clock: C,
        authority: Option<AuthorityClient>,
        executor: Option<&'a mut ExecutorEndpoint>,
        config: DriverConfig,
        observe: O,
    ) -> Self {
        node.set_runs_compaction(executor.is_some());
        let timings = node.timings();
        let retry_after = Duration::from_millis(timings.heartbeat_interval.as_ticks());
        let executing =
            executor.map(|endpoint| Executing::new(endpoint, net, retry_after, timings.reconnect_timeout));
        net.subscribe_to_shard(node.shard_id());
        let search = leader_search(node, net, &config, authority.is_some());
        let records = LeaderRecords::new(node.office_term(), timings.heartbeat_interval);
        let routing = RoutingRefresh::new(timings.suspect_timeout, config.routing_refresh_period, clock.now());
        DriverRun {
            stepper: Stepper {
                node,
                scheduler,
                net,
                replication_factor: config.replication_factor,
                records,
                authority,
                observe,
                collected: Collected::default(),
            },
            net,
            clock,
            config,
            executing,
            search,
            reconciliation: None,
            routing,
            first: Some(first),
            next_deadline: None,
            scheduler_deadline: None,
            woken_by: None,
        }
    }

    /// One batch (see [`super::run_driver`]'s doc).
    pub(super) async fn batch(&mut self) {
        // What settled answers leave this worker itself to act on this batch.
        let mut local = Local::default();
        self.feed_node();
        self.settle_writes(&mut local);
        self.pass_on_before_answering();
        self.answer_requests(&mut local).await;
        self.tick_and_reconcile();
        self.drive_own_work(&mut local);
        self.compare_heard_runs();
        self.catch_up_and_drain(&mut local);
        self.tell_cancels(std::mem::take(&mut local.cancelled));
    }

    /// Feeds the node, in order: the step still to carry out, the epoch
    /// read the search asked, the authority replies that arrived, the
    /// inputs `net` queued, and the pointer the search found. Every
    /// heartbeat this batch sends carries the runs held now.
    fn feed_node(&mut self) {
        let arrived = self.arrived_replies();
        self.stepper
            .node
            .set_active_runs_digest(active_runs_digest(&self.net.claimed_runs().active_ids()));
        if let Some(first) = self.first.take() {
            self.next_deadline = self.stepper.carry(first, None);
        }
        if let Some(asked) = self.search.take_epoch_asked() {
            self.next_deadline = self.stepper.step(asked);
        }
        for reply in arrived {
            self.take_reply(reply);
        }
        for input in self.net.take_inputs() {
            self.next_deadline = self.stepper.step(input);
        }
        if let Some(pointer) = self.search.take_found() {
            self.next_deadline = self.stepper.step(pointer);
        }
    }

    /// Every reply that has arrived, read before the stepper borrows the
    /// client to perform the node's calls.
    fn arrived_replies(&mut self) -> Vec<AuthorityReply> {
        let mut arrived: Vec<AuthorityReply> = self.woken_by.take().into_iter().collect();
        if let Some(client) = self.stepper.authority.as_mut() {
            arrived.extend(std::iter::from_fn(|| client.try_reply()));
        }
        arrived
    }

    fn take_reply(&mut self, reply: AuthorityReply) {
        match reply.token().issuer {
            Issuer::Node => self.next_deadline = self.stepper.step(Input::Authority(reply)),
            // One net asked for itself: the search's, if it asked it.
            Issuer::Cascade => {
                let floor = self.stepper.node.join_floor();
                if let Some(read) = self.search.offer(reply, floor, TokioInstant::now()) {
                    self.next_deadline = self.stepper.step(read);
                }
            }
        }
    }

    /// Settles the record writes whose outcomes arrived. A new office (even
    /// one won as soon as the last was lost) starts with no memory of the
    /// last one's writes, and tells nothing the last one decided.
    fn settle_writes(&mut self, local: &mut Local) {
        let net = self.net;
        for answer in self.stepper.records.follow_office(self.stepper.node.office_term()) {
            send_answer(net, Settled::NotLeader(answer), local);
        }
        let now = self.clock.now();
        let reconciliation = &mut self.reconciliation;
        let mut ports = self.stepper.ports();
        let settled = self.stepper.records.settle(
            self.stepper.node,
            self.stepper.scheduler,
            net.take_write_outcomes(),
            |outcome| {
                reconciliation
                    .as_mut()
                    .is_some_and(|current| current.reconciling.settle(outcome, now))
            },
            now,
            &mut ports,
        );
        for answer in settled {
            send_answer(net, answer, local);
        }
    }

    /// Hands the scheduler and the executor what this batch's steps told
    /// them before any request is answered.
    fn pass_on_before_answering(&mut self) {
        // A leader makes compaction runs only for members that said they
        // run them: the scheduler is told who they are as the heartbeats say.
        let runners = self.stepper.node.compaction_runners();
        self.stepper.scheduler.set_compaction_runners(runners);
        // A deadline the node reported is handed on before the await in
        // `answer_requests`: it is not reported again, so a driver dropped
        // there must not lose it.
        let deadline = self.stepper.collected.abort_deadline.take();
        if let (Some(executing), Some(deadline)) = (self.executing.as_mut(), deadline) {
            executing.follow_abort_deadline(deadline.map(|by| HostAbort::reported_now(by, &self.clock)));
        }
    }

    async fn answer_requests(&mut self, local: &mut Local) {
        respond_to_join_requests(self.stepper.node, self.net).await;
        let mut deciding = self.stepper.deciding();
        respond_to_claim_requests(&mut deciding, local);
        respond_to_task_requests(&mut deciding, &self.clock, local);
        respond_to_reconcile_requests(self.stepper.node, self.net);
        respond_to_steal_requests(self.net, &self.clock);
        self.stepper.write_revisions();
        let mut ports = self.stepper.ports();
        self.stepper
            .records
            .repair(self.stepper.node, self.stepper.scheduler, self.clock.now(), &mut ports);
    }

    fn tick_and_reconcile(&mut self) {
        // A step can report a deadline that has already come: a voter that
        // begins suspecting its leader starts a roll call at its next
        // `Tick`, due at once. Every `Tick` that is due moves the node on or
        // puts its deadline later (see `Step::next_deadline`), so this ends.
        while self.next_deadline.is_some_and(|deadline| deadline <= self.clock.now()) {
            self.next_deadline = self.stepper.step(Input::Tick);
        }
        if let Some(deadline) =
            reconcile_office(&mut self.reconciliation, self.net, &mut self.stepper, &self.clock)
        {
            self.next_deadline = Some(deadline);
        }
    }

    fn drive_own_work(&mut self, local: &mut Local) {
        // A new abort deadline reaches the executor before any more work.
        let abort_deadline = self.stepper.collected.abort_deadline.take();
        let cancelled_runs = std::mem::take(&mut self.stepper.collected.cancelled_runs);
        let Some(executing) = self.executing.as_mut() else {
            return;
        };
        if let Some(deadline) = abort_deadline {
            executing.follow_abort_deadline(deadline.map(|by| HostAbort::reported_now(by, &self.clock)));
        }
        for run in &cancelled_runs {
            executing.cancel(run);
        }
        let settled = std::mem::take(&mut local.own);
        drive_executor(executing, &mut self.stepper.deciding(), &self.clock, settled);
    }

    fn compare_heard_runs(&mut self) {
        let heard = std::mem::take(&mut self.stepper.collected.runs_heard);
        forget_told_cancels(self.stepper.node, self.stepper.scheduler, &heard);
        compare_run_digests(&mut self.reconciliation, self.stepper.scheduler, &self.clock, heard);
    }

    fn catch_up_and_drain(&mut self, local: &mut Local) {
        let now = self.clock.now();
        let mut deciding = self.stepper.deciding();
        self.scheduler_deadline = catch_up_if_due(&mut deciding, now);
        drain_events(&mut deciding, local);
    }

    /// Each stored cancel of a running run goes to the worker running it:
    /// this worker's own executor, or another worker in its next ack.
    fn tell_cancels(&mut self, cancelled: Vec<(WorkerId, TaskRunId)>) {
        let my_id = self.net.local_worker_id();
        for (worker, run) in cancelled {
            if worker != my_id {
                self.stepper.node.tell_cancelled(worker, run);
            } else if let Some(executing) = self.executing.as_mut() {
                executing.cancel(&run);
            }
        }
    }

    /// Where the node, once it drained, said to hand the records this
    /// worker holds; `None` while it has not drained.
    pub(super) fn drained(&mut self) -> Option<HandOffTo> {
        if self.stepper.node.state() != WorkerState::Stopped {
            return None;
        }
        self.stepper.collected.hand_off.take()
    }

    pub(super) async fn hand_off(&mut self, to: HandOffTo) -> HandedOff {
        let timings = self.stepper.node.timings();
        hand_off_held_records(
            self.net,
            to,
            self.config.replication_factor,
            Duration::from_millis(timings.heartbeat_interval.as_ticks()),
            TokioInstant::now() + Duration::from_millis(timings.drain_wait_limit.as_ticks()),
        )
        .await
    }

    /// Sleeps until the node's next deadline or until something arrives,
    /// whichever is first, re-crawling peer routing meanwhile when that is
    /// due, and lets the leader search follow the node first.
    pub(super) async fn sleep(&mut self) {
        // What the node showed after this batch: the timer arm below
        // decides on this same view, since the node is not stepped between
        // batches.
        let view = ShardView::of(self.stepper.node);
        let mut refresh = self.routing.decide(&view, self.clock.now());
        if refresh.crawl {
            self.net.refresh_peer_routing();
        }
        // A fenced node that found its shard recovered without it rejoins;
        // a stranded node searches for a leader to reconnect to.
        let state = self.stepper.node.state();
        self.search
            .follow(state, self.stepper.authority.as_mut(), self.clock.now(), TokioInstant::now());
        let wakes = self.wakes();
        // A due routing crawl is made between batches, with no batch of its
        // own: the node is stepped only when something arrives or its
        // deadline comes.
        loop {
            tokio::select! {
                () = sleep_until(&self.clock, self.next_deadline) => break,
                () = sleep_until(&self.clock, Some(refresh.next_due)) => {
                    refresh = self.routing.decide(&view, self.clock.now());
                    if refresh.crawl {
                        self.net.refresh_peer_routing();
                    }
                }
                () = self.net.wait_for_arrival() => break,
                () = wake_at(wakes.search) => break,
                () = sleep_until(&self.clock, wakes.stranded) => break,
                () = sleep_until(&self.clock, self.scheduler_deadline) => break,
                () = sleep_until(&self.clock, wakes.reconcile) => break,
                () = sleep_until(&self.clock, wakes.records) => break,
                () = next_reconciliation(&mut self.reconciliation) => break,
                () = executor_wake(&mut self.executing) => break,
                result = self.search.ask_done() => {
                    self.search.asked(result, TokioInstant::now());
                    break;
                }
                Some(reply) = next_reply(&mut self.stepper.authority) => {
                    self.woken_by = Some(reply);
                    break;
                }
            }
        }
    }

    fn wakes(&self) -> Wakes {
        Wakes {
            search: self.search.round_wake_at(),
            stranded: self.search.stranded_wake_at(self.clock.now()),
            reconcile: self
                .reconciliation
                .as_ref()
                .and_then(|current| current.wake_at(self.stepper.node)),
            // Held answers are answered `NotLeader` when the lease ends,
            // and a refused write is published again, with no other wake
            // guaranteed.
            records: self.stepper.records.wake_at(self.stepper.scheduler.lease_end()),
        }
    }
}

/// When the work outside the node falls due, read once before the wait.
struct Wakes {
    search: Option<TokioInstant>,
    stranded: Option<Instant>,
    reconcile: Option<Instant>,
    records: Option<Instant>,
}

/// The search a driven node runs when it is back in `Bootstrapping` or
/// stranded (see `crate::leader_search::DrivenSearch`).
fn leader_search<'a, C: Clock>(
    node: &WorkerNode<C>,
    net: &'a Net,
    config: &DriverConfig,
    authority_present: bool,
) -> DrivenSearch<'a, JoinOverNet<'a>> {
    let suspect_timeout = node.timings().suspect_timeout;
    DrivenSearch::new(
        node.shard_id(),
        net.local_worker_id(),
        JoinOverNet {
            net,
            per_peer_timeout: config.join_peer_timeout,
            grace: Duration::from_millis(suspect_timeout.as_ticks()),
        },
        config.seeds.clone(),
        config.retry_interval,
        suspect_timeout,
        authority_present,
    )
}

/// Compares each run digest a heartbeat reported with the runs `scheduler`
/// believes that worker holds, and asks a worker whose heartbeats keep
/// disagreeing for its runs again (see [`LeaderReconciliation::runs_heard`]).
/// Only a leading scheduler has a belief to compare.
fn compare_run_digests<C: Clock, I: IdGenerator>(
    reconciliation: &mut Option<LeaderReconciliation<'_>>,
    scheduler: &Scheduler<C, I, RecordOutbox>,
    clock: &C,
    heard: Vec<(WorkerId, Vec<u8>)>,
) {
    let Some(current) = reconciliation.as_mut() else {
        return;
    };
    if !scheduler.is_leader() {
        return;
    }
    for (worker, digest) in heard {
        let believed = active_runs_digest(&scheduler.active_runs_of(&worker));
        current.runs_heard(&worker, &digest, &believed, clock.now());
    }
}

/// A worker whose heartbeat shows exactly the runs the scheduler believes
/// it holds no longer holds any run this leader told it was cancelled, so
/// its acks stop listing them.
fn forget_told_cancels<C: Clock, I: IdGenerator>(
    node: &mut WorkerNode<C>,
    scheduler: &Scheduler<C, I, RecordOutbox>,
    heard: &[(WorkerId, Vec<u8>)],
) {
    if !scheduler.is_leader() {
        return;
    }
    for (worker, digest) in heard {
        if active_runs_digest(&scheduler.active_runs_of(worker)).value() == digest.as_slice() {
            node.forget_cancelled(worker);
        }
    }
}

/// The reconciliation's next answer or lookup, taken in; for ever without one.
/// Cancel-safe.
async fn next_reconciliation(reconciliation: &mut Option<LeaderReconciliation<'_>>) {
    match reconciliation {
        Some(reconciliation) => reconciliation.next().await,
        None => std::future::pending().await,
    }
}

/// The executor's next report, the end of a claim or report under way, or
/// the time to claim or report again; for ever without an executor.
/// Cancel-safe.
async fn executor_wake(executing: &mut Option<Executing<'_>>) {
    match executing {
        Some(executing) => executing.wake().await,
        None => std::future::pending().await,
    }
}

/// Runs the reconciliation of the office the stepper's node holds: starts
/// it when the scheduler awaits a rebuild for that office, drops it when the
/// node holds another office or none, and otherwise takes it as far as it
/// goes now (see [`LeaderRecords::reconcile`]), stepping the node when it
/// asks. Returns the node's next deadline when it stepped the node.
fn reconcile_office<'n, C, I, O>(
    reconciliation: &mut Option<LeaderReconciliation<'n>>,
    net: &'n Net,
    stepper: &mut Stepper<'_, C, I, O>,
    clock: &C,
) -> Option<Instant>
where
    C: Clock,
    I: IdGenerator,
    O: FnMut(&WorkerNode<C>, Option<&Input>, &Step),
{
    let office = stepper.node.office_term();
    if reconciliation
        .as_ref()
        .is_some_and(|current| Some(current.office()) != office)
    {
        *reconciliation = None;
    }
    if reconciliation.is_none()
        && let Some(office) = office_to_reconcile(stepper.node, stepper.scheduler)
        && let Some(proof) = stepper.node.reconcile_proof()
    {
        let timings = stepper.node.timings();
        *reconciliation = Some(LeaderReconciliation::start(
            net,
            office,
            proof,
            stepper.node.reconcilees(),
            clock.now(),
            timings.suspect_timeout,
            timings.heartbeat_interval,
        ));
    }
    let current = reconciliation.as_mut()?;
    let mut next_deadline = None;
    loop {
        let now = clock.now();
        current.ask_and_fetch(stepper.node, now);
        let lookups_done = current.lookups_done();
        let mut ports = stepper.ports();
        let turn = stepper.records.reconcile(
            &mut current.reconciling,
            stepper.node,
            stepper.scheduler,
            lookups_done,
            now,
            &mut ports,
        );
        match turn {
            Turn::Wait => return next_deadline,
            Turn::Ended => {
                *reconciliation = None;
                return next_deadline;
            }
            Turn::Step(input) => next_deadline = earlier(next_deadline, stepper.step(input)),
        }
    }
}

/// The earlier of two deadlines, either of which may be absent.
fn earlier(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Sleeps until `clock` reaches `deadline`, or for ever when there is none.
async fn sleep_until<C: Clock>(clock: &C, deadline: Option<Instant>) {
    match deadline {
        // `core::time`'s `Instant` subtraction saturates at zero, and one of
        // its ticks is a millisecond.
        Some(deadline) => {
            let ticks = (deadline - clock.now()).as_ticks();
            tokio::time::sleep(Duration::from_millis(ticks)).await;
        }
        None => std::future::pending().await,
    }
}

/// Sleeps until `at`, or for ever when there is none.
async fn wake_at(at: Option<TokioInstant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// The client's next reply; for ever without a client. Cancel-safe.
async fn next_reply(client: &mut Option<AuthorityClient>) -> Option<AuthorityReply> {
    match client {
        Some(client) => client.next_reply(None).await,
        None => std::future::pending().await,
    }
}
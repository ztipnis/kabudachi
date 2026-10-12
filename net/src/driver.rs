//! Drives a `core::election::WorkerNode` over a real
//! [`crate::messenger::Net`]. The node does no I/O of its own (see
//! `core::election`'s module doc): something outside it has to feed it what
//! happens and carry out what it asks. [`run_driver`] is that something for
//! a worker on the network, as the simulator is in `core`'s tests.
//!
//! It feeds the node every input `Net` queues for it (messages received,
//! connections opened and closed) and steps it with a `Tick` when the
//! deadline the node reports comes. It sends and publishes the messages each
//! step asks to through `Net`, and hands the step's leadership grant to the
//! worker's scheduler. In between it sleeps until that deadline or until
//! something arrives, whichever is first; it never steps the node on a fixed
//! timer.
//!
//! It also answers the three request/response protocols whose answers only
//! this side of the worker holds. A join request is answered with the
//! leader the node knows, at the address `Net` knows for it; a claim request
//! with the scheduler's decision, once the node's roster has been checked: a
//! leader grants a claim only to a voter or a pending member of its shard,
//! and answers any other worker `NOT_MEMBER` without asking the scheduler;
//! and a task-exchange request (a submission, a report on a run, a cancel)
//! with the scheduler's decision, gated like a claim. Every worker, leader or
//! not, also answers a reconcile request with a page of the runs and records it
//! holds, whoever asks, and a steal request with the tasks whose records it
//! holds that look claimable (see `crate::steal`). And it tells `Net` which leader the node
//! names, so the worker's own claims go to that leader. Between batches it
//! re-crawls the worker's peer routing once the node's view of its shard
//! has changed and settled, and periodically (see `crate::routing_refresh`), so
//! the shard's workers stay connected to one another and not only to their
//! leader.
//!
//! A worker given an executor (see `crate::executor`) claims work for it: while
//! the executor has offered room, the node knows its leader, and some leader
//! has vouched for hearing this worker, it discovers and claims work from
//! that leader and hands each run over. It takes each report the executor
//! makes to the leader, in order per run, asking again as leaders change
//! until one takes it. When the node reports an abort deadline, every run
//! handed over is told to abort by its own deadline, from its reconnect
//! timeout, and told again when the deadline is lifted; no more work is
//! claimed while one stands.
//!
//! A leader's scheduler records each decision as a new revision of its
//! task's Task record. The driver writes every revision to the voters
//! the record's placement names (the replication factor nearest the key),
//! and holds the answer a decision produced, a claim's or a task-exchange
//! request's included, until a
//! quorum of those voters has stored it and the scheduler still leads. The
//! held answer is released `NotLeader` when the lease ends, and the driver
//! wakes just past the lease end so that happens even if nothing else
//! arrives. A write that misses its quorum while the leader still leads
//! may or may not have landed, so the driver answers `NotLeader` to every
//! later question about that task until a newer revision of it is stored.
//!
//! A leader places each record on the voters it currently hears: those of its
//! configuration less any it reported lost and has not heard since (see
//! `WorkerNode::placeable_voters`). It notes where each revision went, and
//! whenever those voters change, or a write was refused, publishes the
//! records concerned again so they are written where they belong now, a
//! bounded number at a time (see `kabudachi_core::task_record::Repair`). Once
//! such a write is stored, the holders the record left are sent the revision
//! and drop their stale copies. All of it is forgotten when the office ends,
//! so one leader's refusals never answer questions of the next.
//!
//! A worker that drained returns from [`run_driver`], saying what became of
//! its records (see `crate::handoff`): it asks the leader it followed where
//! each goes, or, if it led, places them among its other voters, and writes
//! each copy to those holders before it returns. A leader answers that
//! question for any asker, a worker it has already taken out of its roster
//! included, as it decides nothing.
//!
//! A node that takes office reconciles before it leads (see
//! `crate::reconcile::leader`). The driver asks every worker of the office's
//! roster what it holds, this worker's own page read locally and the rest
//! over the reconcile exchange, page by page, and looks up the full records it
//! lacks. It stops waiting when every voter answered, or a quorum did and a
//! suspicion timeout has passed, rebuilds the scheduler from the answers, and
//! writes every record the rebuild published again at the office's term, at
//! most 64 at a time, writing again after one heartbeat interval any that was
//! not stored. Only when all are stored does it step the node
//! [`Input::Reconciled`], which makes the node lead and hands the scheduler
//! its grant; until then the scheduler answers every claim and report
//! `NotLeader`. Once the node leads, it keeps asking the workers that have not
//! answered, once per suspicion timeout, and hands the scheduler what their
//! late answers teach: a record that became certain, a run a slow worker
//! held, a worker that was thought silent.
//!
//! Every worker's heartbeats carry a digest of the runs its ledger holds, and
//! the leader compares each with the runs its scheduler believes that worker
//! holds. A difference that lasts two heartbeat intervals (an answer still on
//! its way does not last that long) brings one re-report of that worker's
//! runs, at most one per suspicion timeout per worker, which is adopted like
//! a late answer. Reconciliation work that could not proceed, such as records
//! that could not be placed or writes that were refused, is tried again when
//! the voters change or the time comes, and the driver wakes for it.
//!
//! Time acts on the scheduler here too: each batch lets it catch up once its
//! deadline has come (a delayed task is released, a pending task past its
//! expiry expires, a finished task is forgotten) and writes what that
//! changed, and the driver sleeps no later than that deadline. The events the
//! scheduler raises are drained each batch: they are logged and dropped, as
//! no client lives on a networked worker to hear them, except a cancel of a
//! running run, which is told to the run's worker in its leader's next ack,
//! or to this worker's own executor, once stored. The revisions published
//! when draining ends the scheduler's call are written too.
//!
//! With a coordination authority configured, it is also the sole caller of
//! [`kabudachi_core::election::AuthorityCall::perform`]. Each step's
//! `Output::Authority(call)` is performed by the worker's
//! [`AuthorityClient`], on Tokio's blocking pool
//! (`tokio::task::spawn_blocking`), since an authority is typically a remote
//! service whose calls block, and the reply is fed back to the node as
//! `Input::Authority` in whichever batch it arrives: a slow authority delays
//! nothing else the node does, such as heartbeating its leader. The node
//! times its registration and fence from when it asked, not from when the
//! reply came, so a late reply costs it nothing it counts on, and it
//! ignores a read or swap reply that answers anything but the call it now
//! waits on, so replies arriving out of order are safe. At most one call of
//! each kind is in flight, whoever asked (see [`AuthorityClient`]). With no
//! authority (`None`), every call gets
//! [`kabudachi_core::election::AuthorityCall::unavailable`] at once instead,
//! so a node built with no authority timings never waits on one that does
//! not exist.
//!
//! The same client serves a node that fenced itself and found its shard
//! recovered without it, and so went back to `Bootstrapping`: it rejoins
//! through [`crate::leader_search::DrivenSearch`], which reads the authority's
//! listing through the client and asks the listed workers who leads. The
//! driver is the client's one reader, and hands each reply to whom asked for
//! it: a reply under the node's own token steps the node, and one under
//! net's own goes to the search.
//!
//! A node that stays in `RollCall` or `NoQuorum` for one suspicion timeout
//! has lost touch with its shard, and may be cut off from a leader that
//! still stands: it reruns the same search, over the workers the authority
//! lists and then the configured seeds, whether or not it still hears some
//! workers, since a reachable island smaller than a quorum must heal too.
//! The search's own dials reconnect it, and a leader it reaches acks it as a
//! newly connected peer; nothing is joined from the answer. A search runs
//! rounds, one every retry interval, until a leader is reached or the node
//! leaves those states, and a fresh one starts at most once per suspicion
//! timeout. Unlike a rejoin, it never reads the authority's recovery epoch.
//! A follower keeps no list of its peers: the search asks the authority's
//! listing and the seeds, and never reads the routing table, the connection
//! count or the gossip mesh as membership.

mod answers;
mod events;
mod own_work;
mod stepper;

use std::time::Duration;

use kabudachi_core::election::{AuthorityReply, Input, Issuer, Step, WorkerNode};
use kabudachi_core::protocol::ids::{IdGenerator, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::active_runs_digest;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::{LeaderRecords, RecordOutbox, Settled, Turn, office_to_reconcile};
use kabudachi_core::time::{Clock, Instant};
use libp2p::Multiaddr;

use tokio::time::Instant as TokioInstant;

use crate::authority::AuthorityClient;
use crate::bootstrap::DEFAULT_RETRY_INTERVAL;
use crate::handoff::{HandedOff, hand_off_held_records};
use crate::join::DEFAULT_JOIN_PEER_TIMEOUT;
use crate::leader_search::{DrivenSearch, JoinOverNet};
use crate::messenger::Net;
use crate::reconcile::leader::LeaderReconciliation;
pub use crate::routing_refresh::{DEFAULT_ROUTING_REFRESH_SUSPICIONS, MIN_ROUTING_REFRESH_PERIOD};
use crate::executor::{Executing, ExecutorEndpoint, HostAbort};
use crate::routing_refresh::{RoutingRefresh, ShardView};
use crate::task_store::placement::ReplicationFactor;

use self::answers::{
    Local, respond_to_claim_requests, respond_to_join_requests, respond_to_reconcile_requests,
    respond_to_steal_requests, respond_to_task_requests, send_answer,
};
use self::events::{catch_up_if_due, drain_events};
use self::own_work::drive_executor;
use self::stepper::{Collected, Stepper};

/// How [`run_driver`] runs, beyond the node, transport and scheduler it drives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverConfig {
    /// How often to re-crawl peer routing while nothing else prompts it; `None`
    /// for [`DEFAULT_ROUTING_REFRESH_SUSPICIONS`] suspicion timeouts. Never
    /// under [`MIN_ROUTING_REFRESH_PERIOD`].
    pub routing_refresh_period: Option<Duration>,
    /// Where a node that has lost touch with its shard asks who leads, after
    /// the workers its authority lists (see [`crate::leader_search`]).
    pub seeds: Vec<Multiaddr>,
    /// How long each ask of one listed worker or seed may take.
    pub join_peer_timeout: Duration,
    /// How long the leader search waits between rounds.
    pub retry_interval: Duration,
    /// How many voters each Task record is written to.
    pub replication_factor: ReplicationFactor,
}

impl Default for DriverConfig {
    fn default() -> Self {
        DriverConfig {
            routing_refresh_period: None,
            seeds: Vec::new(),
            join_peer_timeout: DEFAULT_JOIN_PEER_TIMEOUT,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            replication_factor: ReplicationFactor::DEFAULT,
        }
    }
}

pub use crate::authority::SharedAuthority;

/// Drives `node` over `net` in batches, after subscribing `net` to `node`'s
/// shard (see `Net::subscribe_to_shard`). The first batch first carries out `first`, the step `node` still has to have carried out: the
/// one `WorkerNode::start` returned with it, or, for a node driven before,
/// one that asks for nothing and is due now. Each batch feeds `node`
/// every input `net` has queued for it (see `Net::take_inputs`), answers the
/// join and claim requests `net` holds, and steps `node` with a `Tick`
/// while the deadline it reports has come. It carries out every step
/// through `kabudachi_core::election::carry_out`, the grant first: the
/// step's leadership grant goes to `scheduler`, then its messages
/// go out through `net`, sent or published, so a grant the step withdraws is
/// gone before any message the step sends can let another leader act.
/// Between batches it sleeps until `node`'s next deadline or until something
/// arrives on `net`, whichever is first, re-crawling peer routing meanwhile
/// when that is due (see the module doc). It returns only when the node
/// drained, once the records the worker holds are handed over (see the module
/// doc); otherwise callers stop it by dropping (or aborting the task wrapping)
/// the future it returns.
///
/// `observe` is called after every step `node` takes, as `carry_out` calls
/// its own (the step's grant already with `scheduler`, its messages not yet
/// sent), with `node`, the input the step handled (`None` for `first` and
/// for the step a rejoin starts with) and the step itself. It is how a
/// caller watches `node` while this holds it by exclusive borrow, reading
/// `state()` or `known_leader()` from `node` itself; pass `|_, _, _| {}` to
/// ignore it. It runs on the driver's own task, so it should return
/// quickly.
///
/// Each batch first hands `node` the authority replies that have arrived
/// (see the module doc), then the inputs `net` queued.
///
/// `net` queues inputs whether or not this runs, so a run that starts over
/// with a node driven before, or a node built after `net` was created (once
/// the bootstrap cascade has found its shard, say), is first fed everything
/// that happened in between. Authority replies still in flight when a run
/// stops are lost with it, like any unanswered call. Run one `run_driver`
/// per `Net` at a time: two
/// would take inputs from the same queue, each missing what the other took.
///
/// Every driven node carries a `scheduler`, whether or not it is ever
/// leader: whether it leads is the scheduler's call, from the grant it was
/// last handed, and a scheduler that holds no grant, or whose grant's lease
/// has ended, refuses every claim as `NotLeader`.
///
/// The scheduler's observer is a [`RecordOutbox`]: after every step and every
/// batch of claim answers, the driver places each revision the scheduler
/// published on the voters its node leads (see `Net::write_records`), and
/// logs each write that did not reach its quorum.
///
/// A run that starts over with a node driven before must be given the
/// scheduler driven with it before. The node reports a grant only when it
/// changes, so a different scheduler never learns the grant the node already
/// holds, and the one left behind keeps its last grant, for ever if that
/// grant is unbounded.
///
/// `node`, `scheduler` and `clock` must read one clock: pass each a copy of
/// the same clock (a `RealClock` is `Copy`, and its copies share one
/// origin). The scheduler compares instants the node reports against its
/// own clock, and this sleeps until the node's deadlines by `clock`;
/// instants of two clocks with different origins mean different moments.
/// That clock must advance in real time, one tick per millisecond, as a
/// `RealClock` does.
///
/// `config` says how the driver itself runs (see [`DriverConfig`]).
///
/// `authority` is the client of the coordination authority to perform
/// `node`'s `Output::Authority` calls against (see the module doc); `None`
/// for a node built with no authority timings, which never asks for one. It
/// must be a client of the authority whose TTL `node`'s authority timings
/// name: a worker's entry point pairs the two (see `crate::worker`), and
/// passes the client the bootstrap cascade used, so the one-call-per-kind cap
/// holds across the handover. It is also what a node that fenced itself and
/// found its shard recovered without it, and so went back to `Bootstrapping`,
/// reads to learn whom to rejoin through (see
/// [`crate::leader_search::DrivenSearch`]); the driver keeps running meanwhile.
///
/// `executor` is the endpoint of the executor that runs this worker's
/// TaskRuns (see [`crate::executor`]), or `None` for a worker that runs none:
/// it then claims nothing and says it runs no compaction. A run that starts
/// over with a node driven before must be given the same endpoint, which
/// keeps what the driver knew of its executor between runs.
pub async fn run_driver<C, I>(
    node: &mut WorkerNode<C>,
    first: Step,
    net: &Net,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    clock: C,
    mut authority: Option<AuthorityClient>,
    executor: Option<&mut ExecutorEndpoint>,
    config: DriverConfig,
    mut observe: impl FnMut(&WorkerNode<C>, Option<&Input>, &Step),
) -> HandedOff
where
    C: Clock,
    I: IdGenerator,
{
    let my_id = net.local_worker_id();
    node.set_runs_compaction(executor.is_some());
    // The driver's side of the executor, for this run of the driver.
    let retry_after = Duration::from_millis(node.timings().heartbeat_interval.as_ticks());
    let default_reconnect = node.timings().reconnect_timeout;
    let mut executing =
        executor.map(|endpoint| Executing::new(endpoint, net, retry_after, default_reconnect));
    net.subscribe_to_shard(node.shard_id());
    let mut first = Some(first);
    let mut next_deadline = None;
    // The reply whose arrival ended the last sleep, if one did.
    let mut woken_by = None;
    // The leader search: a rejoin while the node is back in `Bootstrapping`
    // or `Joining`, or a stranded node's search for a leader to reconnect to.
    let mut search = DrivenSearch::new(
        node.shard_id(),
        my_id.clone(),
        JoinOverNet {
            net,
            per_peer_timeout: config.join_peer_timeout,
            grace: Duration::from_millis(node.timings().suspect_timeout.as_ticks()),
        },
        config.seeds.clone(),
        config.retry_interval,
        node.timings().suspect_timeout,
        authority.is_some(),
    );
    // The office's record path: the writes made and not yet settled, and the
    // answers decided but not yet sent, each waiting for the writes its
    // decision made.
    let mut records = LeaderRecords::new(node.office_term(), node.timings().heartbeat_interval);
    // While the node holds an office whose scheduler has not been rebuilt, or
    // has been but still awaits late answers: the office's reconciliation.
    let mut reconciliation: Option<LeaderReconciliation<'_>> = None;
    // What the node's steps reported for the batch to act on.
    let mut collected = Collected::default();
    let mut routing = RoutingRefresh::new(
        node.timings().suspect_timeout,
        config.routing_refresh_period,
        clock.now(),
    );

    loop {
        // What settled answers leave this worker itself to act on this batch.
        let mut local = Local::default();
        // Every reply that has arrived, read before the stepper borrows the
        // client to perform the node's calls.
        let mut arrived_replies: Vec<AuthorityReply> = woken_by.take().into_iter().collect();
        if let Some(client) = authority.as_mut() {
            arrived_replies.extend(std::iter::from_fn(|| client.try_reply()));
        }
        // Every heartbeat this batch sends carries the runs held now.
        node.set_active_runs_digest(active_runs_digest(&net.claimed_runs().active_ids()));
        let mut stepper = Stepper {
            node: &mut *node,
            scheduler: &mut *scheduler,
            net,
            replication_factor: config.replication_factor,
            records: &mut records,
            calls: authority.as_mut(),
            observe: &mut observe,
            collected: &mut collected,
        };
        if let Some(first) = first.take() {
            next_deadline = stepper.carry(first, None);
        }
        if let Some(asked) = search.take_epoch_asked() {
            next_deadline = stepper.step(asked);
        }
        for reply in arrived_replies {
            match reply.token().issuer {
                Issuer::Node => next_deadline = stepper.step(Input::Authority(reply)),
                // One net asked for itself: the search's, if it asked it.
                Issuer::Cascade => {
                    let floor = stepper.node.join_floor();
                    if let Some(read) = search.offer(reply, floor, TokioInstant::now()) {
                        next_deadline = stepper.step(read);
                    }
                }
            }
        }
        for input in net.take_inputs() {
            next_deadline = stepper.step(input);
        }
        if let Some(pointer) = search.take_found() {
            next_deadline = stepper.step(pointer);
        }
        // A new office (even one won as soon as the last was lost) starts
        // with no memory of the last one's writes, and tells nothing the last
        // one decided.
        for answer in stepper.records.follow_office(stepper.node.office_term()) {
            send_answer(net, Settled::NotLeader(answer), &mut local);
        }
        let now = clock.now();
        let mut ports = stepper.ports();
        let settled = stepper.records.settle(
            stepper.node,
            stepper.scheduler,
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
            send_answer(net, answer, &mut local);
        }
        // A leader makes compaction runs only for members that said they run
        // them: the scheduler is told who they are as the heartbeats say.
        stepper
            .scheduler
            .set_compaction_runners(stepper.node.compaction_runners());
        // A deadline the node reported is handed on before the await below: it
        // is not reported again, so a driver dropped there must not lose it.
        if let (Some(executing), Some(deadline)) = (executing.as_mut(), stepper.collected.abort_deadline.take()) {
            executing.follow_abort_deadline(deadline.map(|by| HostAbort::reported_now(by, &clock)));
        }
        respond_to_join_requests(stepper.node, net).await;
        respond_to_claim_requests(
            stepper.node,
            stepper.scheduler,
            net,
            stepper.records,
            config.replication_factor,
            &mut local,
        );
        respond_to_task_requests(
            stepper.node,
            stepper.scheduler,
            net,
            &clock,
            stepper.records,
            config.replication_factor,
            &mut local,
        );
        respond_to_reconcile_requests(stepper.node, net);
        respond_to_steal_requests(net, &clock);
        stepper.write_revisions();
        let mut ports = stepper.ports();
        stepper
            .records
            .repair(stepper.node, stepper.scheduler, clock.now(), &mut ports);
        // A step can report a deadline that has already come: a voter that
        // begins suspecting its leader starts a roll call at its next
        // `Tick`, due at once. Every `Tick` that is due moves the node on or
        // puts its deadline later (see `Step::next_deadline`), so this ends.
        while next_deadline.is_some_and(|deadline| deadline <= clock.now()) {
            next_deadline = stepper.step(Input::Tick);
        }
        if let Some(deadline) = reconcile_office(&mut reconciliation, net, &mut stepper, &clock) {
            next_deadline = Some(deadline);
        }
        // A new abort deadline reaches the executor before any more work.
        let abort_deadline = stepper.collected.abort_deadline.take();
        let cancelled_runs = std::mem::take(&mut stepper.collected.cancelled_runs);
        if let Some(executing) = executing.as_mut() {
            if let Some(deadline) = abort_deadline {
                executing.follow_abort_deadline(deadline.map(|by| HostAbort::reported_now(by, &clock)));
            }
            for run in &cancelled_runs {
                executing.cancel(run);
            }
            drive_executor(
                executing,
                stepper.node,
                stepper.scheduler,
                net,
                &clock,
                stepper.records,
                config.replication_factor,
                std::mem::take(&mut local.own),
            );
        }
        let heard = std::mem::take(&mut stepper.collected.runs_heard);
        forget_told_cancels(stepper.node, stepper.scheduler, &heard);
        compare_run_digests(&mut reconciliation, stepper.scheduler, &clock, heard);
        let mut ports = stepper.ports();
        let scheduler_deadline = catch_up_if_due(
            stepper.node,
            stepper.scheduler,
            stepper.records,
            &mut ports,
            clock.now(),
        );
        drain_events(
            stepper.node,
            stepper.scheduler,
            stepper.records,
            &mut ports,
            net,
            &mut local,
        );
        let my_id = net.local_worker_id();
        for (worker, run) in std::mem::take(&mut local.cancelled) {
            if worker == my_id {
                if let Some(executing) = executing.as_mut() {
                    executing.cancel(&run);
                }
            } else {
                node.tell_cancelled(worker, run);
            }
        }
        // A node that drained has nothing more to do but hand its records
        // over; the worker may exit once that is done.
        if node.state() == WorkerState::Stopped
            && let Some(to) = collected.hand_off.take()
        {
            let timings = node.timings();
            return hand_off_held_records(
                net,
                to,
                config.replication_factor,
                Duration::from_millis(timings.heartbeat_interval.as_ticks()),
                TokioInstant::now() + Duration::from_millis(timings.drain_wait_limit.as_ticks()),
            )
            .await;
        }
        // What the node showed after this batch: the timer arm below decides
        // on this same view, since the node is not stepped between batches.
        let view = ShardView::of(node);
        let mut refresh = routing.decide(&view, clock.now());
        if refresh.crawl {
            net.refresh_peer_routing();
        }

        // A fenced node that found its shard recovered without it rejoins; a
        // stranded node searches for a leader to reconnect to.
        search.follow(node.state(), authority.as_mut(), clock.now(), TokioInstant::now());
        let search_wake = search.round_wake_at();
        let stranded_wake = search.stranded_wake_at(clock.now());
        let reconcile_wake = reconciliation
            .as_ref()
            .and_then(|current| current.wake_at(node));
        // Held answers are answered `NotLeader` when the lease ends, and a
        // refused write is published again, with no other wake guaranteed.
        let records_wake = records.wake_at(scheduler.lease_end());

        // A due routing crawl is made between batches, with no batch of its
        // own: the node is stepped only when something arrives or its
        // deadline comes.
        loop {
            tokio::select! {
                () = sleep_until(&clock, next_deadline) => break,
                () = sleep_until(&clock, Some(refresh.next_due)) => {
                    refresh = routing.decide(&view, clock.now());
                    if refresh.crawl {
                        net.refresh_peer_routing();
                    }
                }
                () = net.wait_for_arrival() => break,
                () = wake_at(search_wake) => break,
                () = sleep_until(&clock, stranded_wake) => break,
                () = sleep_until(&clock, scheduler_deadline) => break,
                () = sleep_until(&clock, reconcile_wake) => break,
                () = sleep_until(&clock, records_wake) => break,
                () = next_reconciliation(&mut reconciliation) => break,
                () = executor_wake(&mut executing) => break,
                result = search.ask_done() => {
                    search.asked(result, TokioInstant::now());
                    break;
                }
                Some(reply) = next_reply(&mut authority) => {
                    woken_by = Some(reply);
                    break;
                }
            }
        }
    }
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


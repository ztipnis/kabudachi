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
//! no client lives on a networked worker to hear them, and the revisions
//! published when that ends the scheduler's call are written too.
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
//! through [`crate::leader_search::Rejoin`], which reads the authority's
//! listing through the client and asks the listed workers who leads. The
//! driver is the client's one reader, and hands each reply to whom asked for
//! it: a reply under the node's own token steps the node, and one under
//! net's own goes to the rejoin.
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

use std::collections::BTreeSet;
use std::time::Duration;

use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, HandOffTo, Input, Issuer, MessageSink,
    Output, Step, WorkerNode, carry_out,
};
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    ClaimResponse, ElectionMessage, JoinResponse, TaskResponse, claim_request, task_request,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::active_runs_digest;
use kabudachi_core::scheduler::{ReconcileRefused, Scheduler};
use kabudachi_core::task_record::{
    EffectGate, RecordOutbox, Repair, Settled, Settlement, Waits, Write, WriteLedger, WriteOrder,
};
use kabudachi_core::time::{Clock, Instant, WallTime};
use libp2p::Multiaddr;

use tokio::time::Instant as TokioInstant;

use crate::authority::AuthorityClient;
use crate::bootstrap::DEFAULT_RETRY_INTERVAL;
use crate::claim::{self, ClaimRequestHandle};
use crate::handoff::{HandedOff, hand_off_held_records};
use crate::join::{DEFAULT_JOIN_PEER_TIMEOUT, LeaderSearch, pointer_for};
use crate::leader_search::{JoinOverNet, Rejoin, StrandedWatch};
use crate::messenger::{Net, PlacedWrite, WriteOutcome};
use crate::reconcile::leader::{LeaderReconciliation, Progress, Stuck};
use crate::reconcile::report::page_of;
use crate::steal::candidates_for_steal;
pub use crate::routing_refresh::{DEFAULT_ROUTING_REFRESH_SUSPICIONS, MIN_ROUTING_REFRESH_PERIOD};
use crate::routing_refresh::{RoutingRefresh, ShardView};
use crate::task_exchange::{self, TaskRequestHandle};
use crate::task_store::placement::{Placement, ReplicationFactor, placement};

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

/// The search the driver runs for a node in `state`, if any. A rejoin needs
/// an authority to confirm a pointer against. A stranded node's leader
/// search needs someone to ask, an authority's listing or a seed: with
/// neither it would run empty rounds every retry interval.
fn search_purpose(
    state: WorkerState,
    authority_present: bool,
    seeds_present: bool,
    stranded_search: bool,
) -> Option<SearchFor> {
    match state {
        WorkerState::Bootstrapping | WorkerState::Joining if authority_present => {
            Some(SearchFor::Rejoin)
        }
        _ if stranded_search && (authority_present || seeds_present) => Some(SearchFor::Leader),
        _ => None,
    }
}

/// Why the driver runs a leader search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchFor {
    /// The node fenced itself and is back in `Bootstrapping` (or `Joining` on
    /// a pointer it took): a pointer is joined through `Input::JoinAnswer`.
    Rejoin,
    /// The node is stranded: the search's own dials reconnect it, and a leader
    /// it reaches acks it as a newly connected peer.
    Leader,
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
/// [`crate::leader_search::Rejoin`]); the driver keeps running meanwhile.
pub async fn run_driver<C, I>(
    node: &mut WorkerNode<C>,
    first: Step,
    net: &Net,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    clock: C,
    mut authority: Option<AuthorityClient>,
    config: DriverConfig,
    mut observe: impl FnMut(&WorkerNode<C>, Option<&Input>, &Step),
) -> HandedOff
where
    C: Clock,
    I: IdGenerator,
{
    let my_id = net.local_worker_id();
    net.subscribe_to_shard(node.shard_id());
    let mut first = Some(first);
    let mut next_deadline = None;
    // The reply whose arrival ended the last sleep, if one did.
    let mut woken_by = None;
    // While the node is back in `Bootstrapping` or `Joining`: its search for a
    // leader to rejoin, and the pointer the last round found.
    // A node in `RollCall` or `NoQuorum` for a suspicion timeout runs one too,
    // for a leader to reconnect it.
    let mut search: Option<(SearchFor, Rejoin<'_, JoinOverNet<'_>>)> = None;
    let mut found: Option<(SearchFor, JoinResponse)> = None;
    let mut stranded = StrandedWatch::new(node.timings().suspect_timeout);
    // Whether the last firing of `stranded` still calls for a search.
    let mut stranded_search = false;
    // The epoch read the rejoin asked last round, to tell the node of before
    // its answer arrives.
    let mut epoch_read_asked = None;
    // The claim answers decided but not yet sent: each waits for the writes
    // its decision made.
    let mut held_answers = EffectGate::new();
    // Every write made and not yet settled, whichever call made it.
    let mut unsettled = RecordWrites::new(node.timings().heartbeat_interval);
    // The office the last batch found the node in, to tell one office from
    // the next even when the node lost one and won another between two batches.
    let mut office = node.office_term();
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
            unsettled: &mut unsettled,
            calls: authority.as_mut(),
            observe: &mut observe,
            collected: &mut collected,
        };
        if let Some(first) = first.take() {
            next_deadline = stepper.carry(first, None);
        }
        if let Some(token) = epoch_read_asked.take() {
            next_deadline = stepper.step(Input::AuthorityEpochAsked(token));
        }
        for reply in arrived_replies {
            match reply.token().issuer {
                Issuer::Node => next_deadline = stepper.step(Input::Authority(reply)),
                // One net asked for itself: the rejoin's read, if this is it.
                Issuer::Cascade => match search.as_mut() {
                    Some((purpose, search)) => {
                        let epoch = search.offer(
                            reply,
                            TokioInstant::now(),
                            stepper.node.join_floor(),
                        );
                        // Only a rejoin's node takes the epoch as its floor.
                        if let Some((token, held)) = epoch
                            && *purpose == SearchFor::Rejoin
                        {
                            next_deadline =
                                stepper.step(Input::AuthorityEpochRead { token, held });
                        }
                    }
                    None => tracing::debug!(
                        "dropping a reply to a call net asked for itself: no search is running"
                    ),
                },
            }
        }
        for input in net.take_inputs() {
            next_deadline = stepper.step(input);
        }
        if let Some((purpose, pointer)) = found.take() {
            match purpose {
                // A pointer the node would not take (to its old epoch's leader,
                // or another lineage's) leaves it in `Bootstrapping`, and the
                // rejoin, which has already paced its next round, goes on.
                SearchFor::Rejoin => next_deadline = stepper.rejoin(pointer),
                // Reaching the leader was the search's own work: it has dialed
                // it, and the leader's ack follows as an ordinary input.
                SearchFor::Leader => {
                    tracing::info!(
                        shard = stepper.node.shard_id().as_str(),
                        leader = ?pointer.leader_id,
                        "a node that had lost touch with its shard reached a leader"
                    );
                    stranded_search = false;
                    search = None;
                }
            }
        }
        // A new office (even one won as soon as the last was lost) starts
        // with no memory of the last one's writes.
        if stepper.node.office_term() != office {
            office = stepper.node.office_term();
            stepper.unsettled.forget_office();
        }
        // A write that moved a record away from holders has them drop their
        // copies once it is stored.
        let outcomes = net.take_write_outcomes();
        for outcome in &outcomes {
            if let Some(retirement) = stepper.unsettled.repair.settled(outcome, clock.now()) {
                net.retire_copies(retirement.record, retirement.former);
            }
        }
        // The republish's own writes first; every other outcome is the gate's.
        let outcomes = settle_republish(&mut reconciliation, outcomes, clock.now());
        settle_answers(
            stepper.node,
            stepper.scheduler,
            net,
            config.replication_factor,
            &mut held_answers,
            stepper.unsettled,
            outcomes,
        );
        respond_to_join_requests(stepper.node, net).await;
        respond_to_claim_requests(
            stepper.node,
            stepper.scheduler,
            net,
            &mut held_answers,
            stepper.unsettled,
            config.replication_factor,
        );
        respond_to_task_requests(
            stepper.node,
            stepper.scheduler,
            net,
            &clock,
            &mut held_answers,
            stepper.unsettled,
            config.replication_factor,
        );
        respond_to_reconcile_requests(stepper.node, net);
        respond_to_steal_requests(net, &clock);
        stepper.write_revisions();
        repair_placements(
            stepper.node,
            stepper.scheduler,
            net,
            config.replication_factor,
            stepper.unsettled,
            clock.now(),
        );
        // A step can report a deadline that has already come: a voter that
        // begins suspecting its leader starts a roll call at its next
        // `Tick`, due at once. Every `Tick` that is due moves the node on or
        // puts its deadline later (see `Step::next_deadline`), so this ends.
        while next_deadline.is_some_and(|deadline| deadline <= clock.now()) {
            next_deadline = stepper.step(Input::Tick);
        }
        if let Some(deadline) = reconcile_office(
            &mut reconciliation,
            node,
            scheduler,
            net,
            &clock,
            config.replication_factor,
            &mut unsettled,
            authority.as_mut(),
            &mut observe,
            &mut collected,
        ) {
            next_deadline = Some(deadline);
        }
        compare_run_digests(
            &mut reconciliation,
            scheduler,
            &clock,
            std::mem::take(&mut collected.runs_heard),
        );
        let scheduler_deadline = catch_up_if_due(
            node,
            scheduler,
            net,
            config.replication_factor,
            &mut unsettled,
            clock.now(),
        );
        drain_events(node, scheduler, net, config.replication_factor, &mut unsettled);
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

        // A fenced node that found its shard recovered without it went back
        // to `Bootstrapping` to join again. Only a node with an authority
        // fences itself, and that authority lists whom to ask. A stranded node
        // searches too, for a leader to reconnect to, and a stranded search
        // restarts each time the watch fires, once per suspicion timeout.
        if stranded.observe(node.state(), clock.now()) {
            stranded_search = true;
            if matches!(search, Some((SearchFor::Leader, _))) {
                search = None;
            }
        }
        if !matches!(node.state(), WorkerState::RollCall | WorkerState::NoQuorum) {
            stranded_search = false;
        }
        let purpose = search_purpose(
            node.state(),
            authority.is_some(),
            !config.seeds.is_empty(),
            stranded_search,
        );
        match purpose {
            Some(purpose) => {
                if search.as_ref().map(|(running, _)| *running) != Some(purpose) {
                    let rejoin = Rejoin::new(
                        node.shard_id(),
                        my_id.clone(),
                        JoinOverNet {
                            net,
                            per_peer_timeout: config.join_peer_timeout,
                            grace: Duration::from_millis(
                                node.timings().suspect_timeout.as_ticks(),
                            ),
                        },
                        config.seeds.clone(),
                        config.retry_interval,
                        TokioInstant::now(),
                    );
                    search = Some((
                        purpose,
                        if purpose == SearchFor::Leader {
                            rejoin.for_leader_search()
                        } else {
                            rejoin
                        },
                    ));
                }
                let (_, running) = search.as_mut().expect("set above");
                if node.state() == WorkerState::Joining {
                    // `Joining` on a pointer it took: the authority's epoch
                    // is what confirms it.
                    if let Some(client) = authority.as_mut() {
                        running.validate(client, clock.now(), TokioInstant::now());
                    }
                } else {
                    running.tick(authority.as_mut(), clock.now(), TokioInstant::now());
                }
                let asked = running.take_asked_epoch_read();
                if purpose == SearchFor::Rejoin {
                    epoch_read_asked = asked.or(epoch_read_asked);
                }
            }
            None => search = None,
        }
        // A held answer is released `NotLeader` when the scheduler's lease
        // ends, and nothing else guarantees a wake then: the election's own
        // deadlines are not shown to coincide with the grant's end (the
        // earlier of its quorum and fence ends), and no write outcome or
        // arrival need come. One tick past the end, so the sleep, which
        // counts whole ticks, never fires before the lease has ended.
        let lease_wake = if held_answers.is_empty() {
            None
        } else {
            scheduler
                .lease_end()
                .map(|end| end + kabudachi_core::time::Duration::from_ticks(1))
        };
        let search_wake = search.as_ref().and_then(|(_, search)| search.wake_at());
        let stranded_wake = stranded.wake_at(clock.now());
        let reconcile_wake = reconciliation
            .as_ref()
            .and_then(|current| current.wake_at(node));
        let repair_wake = unsettled.repair.wake_at();

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
                () = sleep_until(&clock, lease_wake) => break,
                () = sleep_until(&clock, scheduler_deadline) => break,
                () = sleep_until(&clock, reconcile_wake) => break,
                () = sleep_until(&clock, repair_wake) => break,
                () = next_reconciliation(&mut reconciliation) => break,
                result = ask_done(&mut search) => {
                    if let Some((purpose, search)) = search.as_mut() {
                        found = search
                            .asked(result, TokioInstant::now())
                            .map(|pointer| (*purpose, pointer));
                    }
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

/// Hands the reconciliation the outcomes of its republished writes and
/// returns the rest.
fn settle_republish(
    reconciliation: &mut Option<LeaderReconciliation<'_>>,
    mut outcomes: Vec<WriteOutcome>,
    now: Instant,
) -> Vec<WriteOutcome> {
    if let Some(current) = reconciliation.as_mut() {
        outcomes.retain(|outcome| !current.settle(outcome, now));
    }
    outcomes
}

/// The reconciliation's next answer or lookup, taken in; for ever without one.
/// Cancel-safe.
async fn next_reconciliation(reconciliation: &mut Option<LeaderReconciliation<'_>>) {
    match reconciliation {
        Some(reconciliation) => reconciliation.next().await,
        None => std::future::pending().await,
    }
}

/// Runs the reconciliation of the office `node` holds: starts it when the
/// scheduler awaits a rebuild for that office, drops it when the node holds
/// another office or none, rebuilds the scheduler when the round may stop,
/// writes its records again at the office's term, steps the node
/// [`Input::Reconciled`] once all are stored, and, once it leads, hands the
/// scheduler what late answers teach. Returns the node's next deadline when
/// it stepped the node.
#[allow(clippy::too_many_arguments)]
fn reconcile_office<'n, C, I, O>(
    reconciliation: &mut Option<LeaderReconciliation<'n>>,
    node: &mut WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &'n Net,
    clock: &C,
    factor: ReplicationFactor,
    unsettled: &mut RecordWrites,
    mut calls: Option<&mut AuthorityClient>,
    observe: &mut O,
    collected: &mut Collected,
) -> Option<Instant>
where
    C: Clock,
    I: IdGenerator,
    O: FnMut(&WorkerNode<C>, Option<&Input>, &Step),
{
    let office = node.office_term();
    if reconciliation
        .as_ref()
        .is_some_and(|current| Some(current.office()) != office)
    {
        *reconciliation = None;
    }
    if reconciliation.is_none()
        && let Some(office) = office
        && let Some(proof) = node.reconcile_proof()
        && scheduler.reconciling() == Some(office)
    {
        *reconciliation = Some(LeaderReconciliation::start(
            net,
            office,
            proof,
            node.reconcilees(),
            clock.now(),
            node.timings().suspect_timeout,
            node.timings().heartbeat_interval,
        ));
    }
    let current = reconciliation.as_mut()?;
    let mut next_deadline = None;
    loop {
        match current.progress(node, scheduler.is_leader(), clock.now()) {
            Progress::Waiting => return next_deadline,
            Progress::Rebuild(rebuild) => match scheduler.reconcile(rebuild) {
                Ok(rebuilt) => {
                    tracing::info!(
                        republished = rebuilt.republished,
                        uncertain = rebuilt.uncertain,
                        "a new leader rebuilt its scheduler from its shard"
                    );
                    let revisions = scheduler.observer_mut().take();
                    place_republish(current, node, factor, &mut unsettled.repair, revisions);
                    if !rebuilt.silent_holders.is_empty() {
                        let due = Stepper {
                            node: &mut *node,
                            scheduler: &mut *scheduler,
                            net,
                            replication_factor: factor,
                            unsettled: &mut *unsettled,
                            calls: calls.as_deref_mut(),
                            observe: &mut *observe,
                            collected: &mut *collected,
                        }
                        .step(Input::WatchWorkers(rebuilt.silent_holders));
                        next_deadline = match (next_deadline, due) {
                            (Some(held), Some(due)) => Some(held.min(due)),
                            (held, due) => held.or(due),
                        };
                    }
                }
                Err(ReconcileRefused { rejection, rebuild }) => {
                    // Nothing was installed, so the same rebuild is offered
                    // again; leading without it would serve an empty shard.
                    tracing::error!(%rejection, "the scheduler did not take the rebuild: not leading");
                    current.stuck(Stuck::Rebuild(rebuild), node.placeable_voters(), clock.now());
                }
            },
            Progress::Place(records) => {
                place_republish(current, node, factor, &mut unsettled.repair, records);
            }
            Progress::RePlace => {
                let voters = node.placeable_voters();
                let repair = &mut unsettled.repair;
                current.re_place(
                    |write| match placement(&Write::of(&write.record).task_id, &voters, factor) {
                        Some(Placement { holders, quorum }) => {
                            write.record.placement = holders.into_iter().map(Into::into).collect();
                            write.quorum = quorum;
                            repair.written(&write.record);
                        }
                        None => tracing::error!(
                            task = Write::of(&write.record).task_id.as_str(),
                            "a republished record could not be placed on the new voters"
                        ),
                    },
                    clock.now(),
                );
            }
            Progress::Republished(office) => {
                let mut stepper = Stepper {
                    node: &mut *node,
                    scheduler: &mut *scheduler,
                    net,
                    replication_factor: factor,
                    unsettled: &mut *unsettled,
                    calls: calls.as_deref_mut(),
                    observe: &mut *observe,
                    collected: &mut *collected,
                };
                // The node leads now: answers that came while it was
                // republishing are taken on the next turn of this loop.
                next_deadline = stepper.step(Input::Reconciled(office));
                // The grant applied what was lost while reconciling.
                write_revisions(node, scheduler, net, factor, unsettled);
            }
            Progress::Learnt(learnt) => {
                // As on every input, the node checks its lease first: one
                // that ended since the node last stepped takes it out of
                // office, and with it the reconciliation, before the
                // scheduler is handed anything.
                let due = Stepper {
                    node: &mut *node,
                    scheduler: &mut *scheduler,
                    net,
                    replication_factor: factor,
                    unsettled: &mut *unsettled,
                    calls: calls.as_deref_mut(),
                    observe: &mut *observe,
                    collected: &mut *collected,
                }
                .step(Input::Tick);
                next_deadline = match (next_deadline, due) {
                    (Some(held), Some(due)) => Some(held.min(due)),
                    (held, due) => held.or(due),
                };
                if node.office_term() != Some(current.office()) {
                    *reconciliation = None;
                    return next_deadline;
                }
                let mut silent_holders = BTreeSet::new();
                match scheduler.adopt(learnt) {
                    Ok(adopted) => silent_holders.extend(adopted.silent_holders),
                    // Holding office does not mean leading: the grant also
                    // ends with the recovery fence, which the node can renew,
                    // and has not arrived before a quorum confirms the
                    // office. The round has given the knowledge up, so it
                    // takes it back and offers it again once the scheduler
                    // leads.
                    Err(learnt) => {
                        tracing::debug!("late reconciliation answers were not adopted: not leading");
                        current.give_back(learnt);
                        return next_deadline;
                    }
                }
                if !silent_holders.is_empty() {
                    let due = Stepper {
                        node: &mut *node,
                        scheduler: &mut *scheduler,
                        net,
                        replication_factor: factor,
                        unsettled: &mut *unsettled,
                        calls: calls.as_deref_mut(),
                        observe: &mut *observe,
                        collected: &mut *collected,
                    }
                    .step(Input::WatchWorkers(silent_holders));
                    next_deadline = match (next_deadline, due) {
                        (Some(held), Some(due)) => Some(held.min(due)),
                        (held, due) => held.or(due),
                    };
                }
                write_revisions(node, scheduler, net, factor, unsettled);
            }
        }
    }
}

/// Places the republished `records` on the voters and starts writing them. If
/// any cannot be placed none is written, and all are kept to be placed again
/// when the voters change or after a suspicion timeout: leading without every
/// record written again would leave a late write of the last leader unfenced.
fn place_republish<C: Clock>(
    current: &mut LeaderReconciliation<'_>,
    node: &WorkerNode<C>,
    factor: ReplicationFactor,
    repair: &mut Repair,
    records: Vec<TaskRecord>,
) {
    let (writes, unplaced) = place(node, factor, records);
    if unplaced.is_empty() {
        for write in &writes {
            repair.written(&write.record);
        }
        current.republishing(writes, node.timings().heartbeat_interval, node.placeable_voters());
    } else {
        tracing::error!(
            unplaced = unplaced.len(),
            "records could not be placed on the voters: not leading"
        );
        let all = writes.into_iter().map(|write| write.record).chain(unplaced).collect();
        current.stuck(Stuck::Place(all), node.placeable_voters(), node.now());
    }
}

/// Has a leader publish again the records its voters' changes, or the writes
/// refused, call for, and writes what that published where it belongs now (see
/// [`Repair`]).
fn repair_placements<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    factor: ReplicationFactor,
    unsettled: &mut RecordWrites,
    now: Instant,
) {
    let in_office = node.office_term().is_some();
    let placeable = if in_office { node.placeable_voters() } else { Vec::new() };
    let tasks = unsettled.repair.check(
        in_office,
        scheduler.is_leader(),
        &placeable,
        |task| scheduler.holds(task),
        |task| placement(task, &placeable, factor).map(|placed| placed.holders),
        now,
    );
    if !tasks.is_empty() && scheduler.republish(&tasks) > 0 {
        write_revisions(node, scheduler, net, factor, unsettled);
    }
}

/// Lets time act on `scheduler` if its deadline has come (delays released,
/// pending tasks expired, finished tasks forgotten), writes what that
/// changed, and returns its next deadline. Nothing waits on those writes: no
/// one asked for them.
fn catch_up_if_due<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    factor: ReplicationFactor,
    unsettled: &mut RecordWrites,
    now: Instant,
) -> Option<Instant> {
    if scheduler.next_deadline().is_some_and(|due| due <= now) {
        scheduler.catch_up();
        write_revisions(node, scheduler, net, factor, unsettled);
    }
    scheduler.next_deadline()
}

/// No client lives on a worker of a networked shard yet, so the events the
/// scheduler raises for one are logged and dropped rather than kept for ever.
/// Taking them ends the scheduler's call, which can publish the revisions
/// the call made, so they are written too; nothing waits on those writes.
fn drain_events<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    factor: ReplicationFactor,
    unsettled: &mut RecordWrites,
) {
    for event in scheduler.take_events() {
        tracing::debug!(?event, "scheduler event with no client to tell");
    }
    write_revisions(node, scheduler, net, factor, unsettled);
}

/// What one batch of [`run_driver`] steps its node with.
struct Stepper<'a, C: Clock, I: IdGenerator, O> {
    node: &'a mut WorkerNode<C>,
    scheduler: &'a mut Scheduler<C, I, RecordOutbox>,
    net: &'a Net,
    replication_factor: ReplicationFactor,
    /// [`run_driver`]'s `unsettled`.
    unsettled: &'a mut RecordWrites,
    /// The client to perform the node's calls with; `None` answers each at
    /// once as `Unavailable`.
    calls: Option<&'a mut AuthorityClient>,
    /// [`run_driver`]'s `observe`.
    observe: &'a mut O,
    /// What the node's steps reported that the batch acts on once they are
    /// all taken.
    collected: &'a mut Collected,
}

/// What steps of the node reported that [`run_driver`] acts on after them.
#[derive(Default)]
struct Collected {
    /// The run digests that heartbeats heard in office reported, with the
    /// workers that sent them, for the batch to compare (see
    /// [`LeaderReconciliation::runs_heard`]).
    runs_heard: Vec<(WorkerId, Vec<u8>)>,
    /// Where the node, once it drained, said to hand the records it holds.
    hand_off: Option<HandOffTo>,
}

impl<C, I, O> Stepper<'_, C, I, O>
where
    C: Clock,
    I: IdGenerator,
    O: FnMut(&WorkerNode<C>, Option<&Input>, &Step),
{
    /// Steps the node with `input`, carries that step out (see
    /// [`Self::carry`]), and returns the node's next deadline.
    fn step(&mut self, input: Input) -> Option<Instant> {
        let stepped = self.node.step(input.clone());
        self.carry(stepped, Some(&input))
    }

    /// Joins the node, back in `Bootstrapping`, to the leader `pointer`
    /// names, as [`Self::step`] does with an [`Input::JoinAnswer`]. A node
    /// with a floor only enters `Joining`: its registration restarts when a
    /// read of the authority validates the pointer.
    fn rejoin(&mut self, pointer: JoinResponse) -> Option<Instant> {
        self.step(Input::JoinAnswer(pointer))
    }

    /// Carries out `stepped`, a step the node has just taken on `input`,
    /// through `carry_out`: its messages go out through the `Net` and its
    /// authority calls to the [`AuthorityClient`]. With no authority, each
    /// call's `Unavailable` reply is fed back at once instead, carrying out
    /// that step too, until a step asks for no more. Logs every step's
    /// alerts (see [`log_alerts`]), hands every step to `observe`, and
    /// returns the node's next deadline.
    fn carry(&mut self, stepped: Step, input: Option<&Input>) -> Option<Instant> {
        let mut performer = Calls(self.calls.as_deref_mut());
        let observe = &mut *self.observe;
        let collected = &mut *self.collected;
        let next_deadline = carry_out(
            &mut *self.node,
            stepped,
            &mut *self.scheduler,
            &mut &*self.net,
            &mut performer,
            |node, _, reply, step| {
                log_alerts(node, &step.outputs);
                for output in &step.outputs {
                    match output {
                        Output::RunsHeard { worker, digest } => {
                            collected.runs_heard.push((worker.clone(), digest.clone()));
                        }
                        Output::HandOff(to) => collected.hand_off = Some(to.clone()),
                        _ => {}
                    }
                }
                // Only the first step has no reply for its input.
                observe(node, reply.or(input), step);
            },
        );
        self.write_revisions();
        next_deadline
    }

    /// Places and writes every revision the scheduler published since the
    /// last call.
    fn write_revisions(&mut self) {
        write_revisions(
            self.node,
            self.scheduler,
            self.net,
            self.replication_factor,
            self.unsettled,
        );
    }
}

/// Places and writes every revision `scheduler` published since the last
/// call, returns their writes and adds them to `unsettled`. Each write's
/// outcome arrives at `net`.
fn write_revisions<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    factor: ReplicationFactor,
    unsettled: &mut RecordWrites,
) -> Vec<Write> {
    let revisions = scheduler.observer_mut().take();
    let writes: Vec<Write> = revisions.iter().map(Write::of).collect();
    let admitted = unsettled.order.admit(revisions);
    place_and_write(node, net, factor, &mut unsettled.repair, admitted);
    unsettled.ledger.made(&writes);
    writes
}

/// Places `records` on the node's voters and writes them; each write's
/// outcome arrives at `net`.
fn place_and_write<C: Clock>(
    node: &WorkerNode<C>,
    net: &Net,
    factor: ReplicationFactor,
    repair: &mut Repair,
    records: Vec<TaskRecord>,
) {
    let (placed, unplaced) = place(node, factor, records);
    for write in &placed {
        repair.written(&write.record);
    }
    // No placement means either the leader has just stopped leading (a
    // scheduler publishes only while it leads, so its own roster no longer
    // holds it), or a voter id is not a peer id, so it cannot be placed:
    // either way the write counts as refused.
    for record in unplaced {
        net.refuse_write(Write::of(&record));
    }
    net.write_records(placed);
}

/// `records` placed on the node's voters, and those that could not be.
fn place<C: Clock>(
    node: &WorkerNode<C>,
    factor: ReplicationFactor,
    records: Vec<TaskRecord>,
) -> (Vec<PlacedWrite>, Vec<TaskRecord>) {
    let voters = node.placeable_voters();
    let (mut placed, mut unplaced) = (Vec::new(), Vec::new());
    for mut record in records {
        match placement(&Write::of(&record).task_id, &voters, factor) {
            Some(Placement { holders, quorum }) => {
                record.placement = holders.into_iter().map(Into::into).collect();
                placed.push(PlacedWrite { record, quorum });
            }
            None => unplaced.push(record),
        }
    }
    (placed, unplaced)
}

/// What the driver has written and not yet seen settled: the ledger of the
/// writes bearing on what the leader may tell, and the order that holds a
/// superseded generation's revision behind its successor's.
struct RecordWrites {
    ledger: WriteLedger,
    order: WriteOrder,
    /// Where each record went, for putting it where it belongs once the
    /// voters change or a write is refused.
    repair: Repair,
    /// How long after a refusal a record is published again.
    retry_after: kabudachi_core::time::Duration,
}

impl RecordWrites {
    fn new(retry_after: kabudachi_core::time::Duration) -> Self {
        RecordWrites {
            ledger: WriteLedger::default(),
            order: WriteOrder::default(),
            repair: Repair::new(retry_after),
            retry_after,
        }
    }

    /// Forgets everything about the writes of an office that ended: a
    /// refusal of that office's must not answer questions of the next,
    /// which republishes every record itself.
    fn forget_office(&mut self) {
        self.ledger.clear();
        self.order.clear();
        self.repair = Repair::new(self.retry_after);
    }
}

/// Performs the node's authority calls through its client, or, with none,
/// answers each at once as `Unavailable`.
struct Calls<'a>(Option<&'a mut AuthorityClient>);

impl AuthorityPerformer for Calls<'_> {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        match self.0.as_deref_mut() {
            Some(client) => client.perform(call),
            None => Some(call.unavailable()),
        }
    }
}

impl MessageSink for &Net {
    fn send(&mut self, to: WorkerId, message: ElectionMessage) {
        Net::send(self, to, message);
    }

    fn publish(&mut self, message: ElectionMessage) {
        Net::publish(self, message);
    }
}

/// Logs what `outputs`, one step of `node`'s, report that an operator must
/// know of and nothing yet acts on: the deadline by which the worker must
/// abort its TaskRuns, or its lifting, and the shard's recovery epoch gone
/// from the authority.
fn log_alerts<C: Clock>(node: &WorkerNode<C>, outputs: &[Output]) {
    for output in outputs {
        match output {
            Output::AbortDeadline(Some(by)) => tracing::warn!(
                shard = node.shard_id().as_str(),
                by = ?by,
                "this worker cannot show that its leader still hears it, or has fenced itself, \
                 and must abort every TaskRun it is running by this instant unless that \
                 changes; no task executor exists yet in Phase 2 to carry that out"
            ),
            Output::AbortDeadline(None) => tracing::info!(
                shard = node.shard_id().as_str(),
                "this worker's leader hears it again: it keeps its TaskRuns"
            ),
            Output::ShardAbandoned => tracing::error!(
                shard = node.shard_id().as_str(),
                recovery_epoch = node.recovery_epoch(),
                "the shard's recovery epoch is gone from the coordination authority; this \
                 worker has stopped"
            ),
            Output::Send { .. }
            | Output::Publish { .. }
            | Output::StateChanged(_)
            | Output::Grant(_)
            | Output::Reconcile(_)
            | Output::Authority(_)
            | Output::RunsHeard { .. }
            | Output::HandOff(_)
            | Output::WorkerLost(_) => {}
        }
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

/// The round's ask of a running rejoin, once it finishes; for ever when none
/// runs. Cancel-safe.
async fn ask_done<'a>(
    search: &mut Option<(SearchFor, Rejoin<'a, JoinOverNet<'a>>)>,
) -> LeaderSearch {
    match search {
        Some((_, search)) => search.ask_done().await,
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

/// Answers every inbound `/kabudachi/join/1` request queued on `net` with a
/// pointer to the shard's leader (see `crate::join::pointer_for`).
async fn respond_to_join_requests<C>(node: &WorkerNode<C>, net: &Net)
where
    C: Clock,
{
    let pending = net.poll_join_requests();
    // A node back in `Bootstrapping` answers no one, as the bootstrap
    // cascade does not (see `crate::bootstrap`): "no leader known" would
    // tell another bootstrapper a shard exists. Dropping the requests
    // closes their streams.
    if pending.is_empty() || node.state() == WorkerState::Bootstrapping {
        return;
    }

    let response = pointer_for(node, net).await;
    for handle in pending {
        net.respond_join(handle, response.clone());
    }
}

/// An answer the leader has decided and holds until the writes its decision
/// made settle.
enum HeldAnswer {
    Claim {
        handle: ClaimRequestHandle,
        response: ClaimResponse,
    },
    Task {
        handle: TaskRequestHandle,
        response: TaskResponse,
    },
}

/// Decides every inbound `/kabudachi/claim/1` request queued on `net` with
/// `scheduler`'s decision (see `claim::answer`) and writes the revisions that
/// decision made. The answer goes out once those writes are acknowledged,
/// while the scheduler still leads (see [`settle_answers`]). One that made no
/// write of its own, because the task it names was already decided, waits
/// for that task's writes still unsettled instead (or, once one was refused,
/// is answered `NotLeader` at once): it tells the asker of
/// what an earlier answer decided, which a leader elected next may never
/// see until those writes land.
fn respond_to_claim_requests<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    held: &mut EffectGate<HeldAnswer>,
    unsettled: &mut RecordWrites,
    factor: ReplicationFactor,
) {
    for handle in net.poll_claim_requests() {
        if scheduler.is_leader() && !node.is_voter_or_pending(&handle.from()) {
            net.respond_claim(handle, claim::not_member());
            continue;
        }
        let response = claim::answer(scheduler, &handle.from(), handle.request());
        let mut writes = write_revisions(node, scheduler, net, factor, unsettled);
        if let (true, claim_request::Request::TaskId(named)) = (writes.is_empty(), handle.request())
        {
            let task = TaskId::from(named.clone());
            match unsettled.ledger.waits_on(&task) {
                Waits::Refused => {
                    send_answer(net, Settled::NotLeader(HeldAnswer::Claim { handle, response }));
                    continue;
                }
                Waits::Writes(pending) => writes.extend(pending),
            }
        }
        if let Some(settled) = held.hold(HeldAnswer::Claim { handle, response }, writes) {
            send_answer(net, settled);
        }
    }
}

/// Decides every inbound `/kabudachi/task/1` request queued on `net` with
/// `scheduler`'s decision (see `task_exchange::answer`) and writes the
/// revisions that decision made, then holds the answer as
/// [`respond_to_claim_requests`] does. As claims are, task requests are
/// answered only for the voters and pending members of the shard the leader
/// leads: any other worker is answered `NotMember` at once, and its request
/// never reaches the scheduler. A submission, a cancel or a report
/// that made no write of its own (its task was already decided, or the report
/// repeats one) waits for that task's writes still unsettled, for the same
/// reason.
fn respond_to_task_requests<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    clock: &C,
    held: &mut EffectGate<HeldAnswer>,
    unsettled: &mut RecordWrites,
    factor: ReplicationFactor,
) {
    for handle in net.poll_task_requests() {
        // A worker that drained has been taken out of the roster by the time
        // it asks where its records go, so this answers any asker; it
        // decides nothing and names only voters.
        if let task_request::Request::Place(place) = handle.request() {
            let response = if scheduler.is_leader() {
                let asker = handle.from();
                let voters: Vec<WorkerId> = node
                    .placeable_voters()
                    .into_iter()
                    .filter(|voter| *voter != asker)
                    .collect();
                task_exchange::placements(place, &voters, factor)
            } else {
                task_exchange::not_leader()
            };
            net.respond_task(handle, response);
            continue;
        }
        if scheduler.is_leader() && !node.is_voter_or_pending(&handle.from()) {
            net.respond_task(handle, task_exchange::not_member());
            continue;
        }
        let named = task_named(scheduler, handle.request());
        let response = task_exchange::answer(scheduler, &handle.from(), handle.request(), clock);
        let mut writes = write_revisions(node, scheduler, net, factor, unsettled);
        if let (true, Some(task)) = (writes.is_empty(), named) {
            match unsettled.ledger.waits_on(&task) {
                Waits::Refused => {
                    send_answer(net, Settled::NotLeader(HeldAnswer::Task { handle, response }));
                    continue;
                }
                Waits::Writes(pending) => writes.extend(pending),
            }
        }
        if let Some(settled) = held.hold(HeldAnswer::Task { handle, response }, writes) {
            send_answer(net, settled);
        }
    }
}

/// Answers every inbound `/kabudachi/reconcile/1` request queued on `net`
/// that `node` may answer (see `WorkerNode::may_answer_reconcile`) with a
/// page of what this worker holds, from its own runs and records alone. A
/// request that proves neither that its sender is the leader this worker
/// follows nor an office no earlier than the highest term it has seen is
/// logged and left unanswered, which the asker reads as no answer.
fn respond_to_reconcile_requests<C: Clock>(node: &WorkerNode<C>, net: &Net) {
    for handle in net.poll_reconcile_requests() {
        let request = handle.request();
        if !node.may_answer_reconcile(&handle.from(), request.proof.as_ref()) {
            tracing::warn!(
                from = handle.from().as_str(),
                recovery_epoch = request.recovery_epoch,
                term = request.term,
                highest_term_seen = node.highest_term_seen(),
                "refusing a reconciliation request: its sender is not the leader this worker \
                 follows and proves no office for a term this worker could still honour"
            );
            continue;
        }
        tracing::debug!(
            from = handle.from().as_str(),
            recovery_epoch = request.recovery_epoch,
            term = request.term,
            "answering a reconciliation request"
        );
        let page = page_of(
            handle.request(),
            &net.claimed_runs(),
            &net.held_records(),
        );
        net.respond_reconcile(handle, page);
    }
}

/// Answers every inbound `/kabudachi/steal/1` request queued on `net` with
/// the tasks this worker holds records of that look claimable. Every worker
/// answers: it needs no leadership and decides nothing, since a task it names
/// must still be claimed from the leader.
fn respond_to_steal_requests<C: Clock>(net: &Net, clock: &C) {
    let now = WallTime::now(clock);
    for handle in net.poll_steal_requests() {
        let task_ids = candidates_for_steal(&net.held_records(), now, handle.limit());
        net.respond_steal(handle, task_ids);
    }
}

/// The task a request names, for the requests that name one, or whose run
/// `scheduler` holds (a report names its run).
fn task_named<C: Clock, I: IdGenerator>(
    scheduler: &Scheduler<C, I, RecordOutbox>,
    request: &task_request::Request,
) -> Option<TaskId> {
    let of_run = |run: &Option<_>| {
        let run = TaskRunId::from(Clone::clone(run.as_ref()?));
        scheduler
            .task_run(&run)
            .and_then(|held| held.identity.as_ref()?.task_id.clone())
            .map(TaskId::from)
    };
    match request {
        task_request::Request::Submit(task) => task.task_id.clone().map(TaskId::from),
        task_request::Request::Cancel(cancel) => cancel.task_id.clone().map(TaskId::from),
        task_request::Request::Started(report) => of_run(&report.task_run_id),
        task_request::Request::Completed(report) => of_run(&report.task_run_id),
        task_request::Request::Failed(report) => of_run(&report.task_run_id),
        task_request::Request::Place(_) => None,
    }
}

/// Settles the held claim answers on `outcomes`, the write outcomes that
/// arrived (those of a republish are settled by it first), and answers every
/// held one `NotLeader` once the scheduler no longer leads.
fn settle_answers<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &Scheduler<C, I, RecordOutbox>,
    net: &Net,
    factor: ReplicationFactor,
    held: &mut EffectGate<HeldAnswer>,
    unsettled: &mut RecordWrites,
    outcomes: Vec<WriteOutcome>,
) {
    // Read once per call. A loss and regain of leadership inside one driver
    // iteration would keep the old term's refused entries, but the scheduler
    // exposes no term identity to tell the terms apart (a lease's end moves
    // with every renewal), and the leftover only errs safe: it answers
    // `NotLeader` for a task until a newer revision of it is stored.
    let leading = scheduler.is_leader();
    for outcome in outcomes {
        unsettled.ledger.settled(&outcome.write, outcome.stored, leading);
        // A superseded generation's revision is written only once its
        // successor's is stored while the leader still leads.
        let mut refused = Vec::new();
        match unsettled.order.settled(&outcome.write, outcome.stored && leading) {
            Settlement::Release(records) => {
                place_and_write(node, net, factor, &mut unsettled.repair, records);
            }
            Settlement::Refuse(writes) => {
                for write in writes {
                    unsettled.ledger.settled(&write, false, leading);
                    refused.push(write);
                }
            }
        }
        if outcome.stored {
            for settled in held.acknowledged(&outcome.write, leading) {
                send_answer(net, settled);
            }
        } else {
            tracing::debug!(
                task = outcome.write.task_id.as_str(),
                "a record revision was not stored at its quorum"
            );
            refused.push(outcome.write);
        }
        for write in refused {
            for answer in held.refused(&write) {
                send_answer(net, Settled::NotLeader(answer));
            }
        }
    }
    if !leading {
        for answer in held.lease_ended() {
            send_answer(net, Settled::NotLeader(answer));
        }
        unsettled.ledger.clear();
        unsettled.order.clear();
    }
}

fn send_answer(net: &Net, settled: Settled<HeldAnswer>) {
    match settled {
        Settled::Released(HeldAnswer::Claim { handle, response }) => {
            net.respond_claim(handle, response);
        }
        Settled::NotLeader(HeldAnswer::Claim { handle, .. }) => {
            net.respond_claim(handle, claim::not_leader());
        }
        Settled::Released(HeldAnswer::Task { handle, response }) => {
            net.respond_task(handle, response);
        }
        Settled::NotLeader(HeldAnswer::Task { handle, .. }) => {
            net.respond_task(handle, task_exchange::not_leader());
        }
    }
}

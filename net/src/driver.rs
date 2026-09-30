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
//! It also answers the two request/response protocols whose answers only
//! this side of the worker holds. A join request is answered with the
//! leader the node knows, at the address `Net` knows for it; a claim request
//! with the scheduler's decision. And it tells `Net` which leader the node
//! names, so the worker's own claims go to that leader. Between batches it
//! re-crawls the worker's peer routing once the node's view of its shard
//! has changed and settled, and periodically (see `RoutingRefresh`), so
//! the shard's workers stay connected to one another and not only to their
//! leader.
//!
//! With a coordination authority configured, it is also the sole caller of
//! [`kabudachi_core::election::AuthorityCall::perform`]. Each step's
//! `Output::Authority(call)` is performed on Tokio's blocking pool
//! (`tokio::task::spawn_blocking`), since an authority is typically a remote
//! service whose calls block, and the reply is fed back to the node as
//! `Input::Authority` in whichever batch it arrives: a slow authority delays
//! nothing else the node does, such as heartbeating its leader. The node
//! times its registration and fence from when it asked, not from when the
//! reply came, so a late reply costs it nothing it counts on, and it
//! ignores a read or swap reply that answers anything but the call it now
//! waits on, so replies arriving out of order are safe. At most one call of
//! each kind is in flight (see `PoolPerformer`). With no
//! authority (`None`), every call gets
//! [`kabudachi_core::election::AuthorityCall::unavailable`] at once instead,
//! so a node built with no authority timings never waits on one that does
//! not exist.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::configuration::{Configuration, Generation};
use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, CallKind, Input, MessageSink, Output, Step,
    WorkerNode, carry_out,
};
use kabudachi_core::protocol::ids::{IdGenerator, ShardId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::protocol::messages::{
    Claim, ClaimBatch, ClaimReject, ClaimRejectReason, ClaimResponse, JoinResponse, claim_request,
    claim_response,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{self, ClaimRejection, Scheduler};
use kabudachi_core::time::{Clock, Instant};

use tokio::sync::mpsc;

use crate::bootstrap::{DEFAULT_RETRY_INTERVAL, WaitLog};
use crate::framing::MAX_MESSAGE_BYTES;
use crate::join::{DEFAULT_JOIN_PEER_TIMEOUT, LeaderSearch, find_leader, pointer_for};
use crate::messenger::{DEFAULT_ROUTING_REFRESH_SUSPICIONS, MIN_ROUTING_REFRESH_PERIOD, Net};

/// A coordination authority shared by every task that calls it: the
/// driver's, and the blocking-pool tasks that perform its calls.
pub type SharedAuthority = Arc<dyn CoordinationAuthority + Send + Sync>;

/// Drives `node` over `net` for ever, in batches, after subscribing `net` to
/// `node`'s shard (see `Net::subscribe_to_shard`). The first batch first
/// carries out `first`, the step `node` still has to have carried out: the
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
/// when that is due (see the module doc). Callers stop it by
/// dropping (or aborting the task wrapping) the future it returns — there
/// is no internal exit condition, mirroring `WorkerNode` itself having no
/// concept of being "done".
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
/// `authority` is the coordination authority to perform `node`'s
/// `Output::Authority` calls against (see the module doc); `None` for a node
/// built with no authority timings, which never asks for one. It must be the
/// authority whose TTL `node`'s authority timings name: a worker's entry
/// point pairs the two (see `crate::worker`). It is also whom a node that
/// fenced itself and found its shard recovered without it, and so went back
/// to `Bootstrapping`, asks for the workers to rejoin through (see
/// [`find_leader_to_rejoin`]); the driver keeps running meanwhile.
pub async fn run_driver<C, I>(
    node: &mut WorkerNode<C>,
    first: Step,
    net: &Net,
    scheduler: &mut Scheduler<C, I>,
    clock: C,
    authority: Option<SharedAuthority>,
    mut observe: impl FnMut(&WorkerNode<C>, Option<&Input>, &Step),
) -> Infallible
where
    C: Clock,
    I: IdGenerator,
{
    let my_id = net.local_worker_id();
    net.subscribe_to_shard(node.shard_id());
    let mut first = Some(first);
    let mut next_deadline = None;
    let (replies, mut replied) = mpsc::unbounded_channel();
    // The reply whose arrival ended the last sleep, if one did.
    let mut woken_by = None;
    let mut in_flight = BTreeSet::new();
    // While the node is back in `Bootstrapping`: the search for a leader to
    // rejoin (see [`find_leader_to_rejoin`]), how many have run, the log of
    // why none has found one yet, and the pointer the last one found.
    let mut rejoin_search: Option<Pin<Box<dyn Future<Output = Rejoin> + Send + '_>>> = None;
    let mut rejoin_log = Some(WaitLog::new(node.shard_id()));
    let mut rejoin_attempts = 0_usize;
    let mut rejoin_pointer = None;
    let mut rejoin_pointer_refused = false;
    let mut routing = RoutingRefresh::new(node, net, clock.now());

    loop {
        let mut stepper = Stepper {
            node: &mut *node,
            scheduler: &mut *scheduler,
            net,
            authority: authority.as_ref(),
            replies: &replies,
            in_flight: &mut in_flight,
            my_id: &my_id,
            observe: &mut observe,
        };
        if let Some(first) = first.take() {
            next_deadline = stepper.carry(first, None);
        }
        let arrived_replies =
            std::iter::from_fn(|| woken_by.take().or_else(|| replied.try_recv().ok()));
        for reply in arrived_replies {
            stepper.in_flight.remove(&reply.token().kind);
            next_deadline = stepper.step(Input::Authority(reply));
        }
        for input in net.take_inputs() {
            next_deadline = stepper.step(input);
        }
        if let Some(pointer) = rejoin_pointer.take() {
            next_deadline = stepper.rejoin(pointer);
            // A pointer it would not take (to its old epoch's leader, or
            // another lineage's) is not asked for again at once.
            rejoin_pointer_refused = stepper.node.state() == WorkerState::Bootstrapping;
        }
        respond_to_join_requests(stepper.node, net).await;
        respond_to_claim_requests(stepper.scheduler, net);
        // A step can report a deadline that has already come: a voter that
        // begins suspecting its leader starts a roll call at its next
        // `Tick`, due at once. Every `Tick` that is due moves the node on or
        // puts its deadline later (see `Step::next_deadline`), so this ends.
        while next_deadline.is_some_and(|deadline| deadline <= clock.now()) {
            next_deadline = stepper.step(Input::Tick);
        }
        let mut refresh_at = routing.after_batch(node, net, clock.now());

        // A fenced node that found its shard recovered without it went back
        // to `Bootstrapping` to join again (ADR-0001 decision 12). Only a
        // node with an authority fences itself, and that authority lists
        // whom to ask.
        if node.state() == WorkerState::Bootstrapping
            && rejoin_search.is_none()
            && let Some(authority) = &authority
        {
            let log = rejoin_log
                .take()
                .unwrap_or_else(|| WaitLog::new(node.shard_id()));
            rejoin_search = Some(Box::pin(find_leader_to_rejoin(
                net,
                Arc::clone(authority),
                node.shard_id().clone(),
                my_id.clone(),
                rejoin_attempts,
                std::mem::take(&mut rejoin_pointer_refused),
                log,
            )));
            rejoin_attempts += 1;
        }

        // A due routing crawl is made between batches, with no batch of its
        // own: the node is stepped only when something arrives or its
        // deadline comes.
        loop {
            tokio::select! {
                () = sleep_until(&clock, next_deadline) => break,
                () = sleep_until(&clock, Some(refresh_at)) => {
                    refresh_at = routing.crawl_if_due(net, clock.now());
                }
                () = net.wait_for_arrival() => break,
                found = async {
                    match rejoin_search.as_mut() {
                        Some(search) => search.await,
                        None => std::future::pending().await,
                    }
                } => {
                    rejoin_search = None;
                    rejoin_pointer = found.pointer;
                    rejoin_log = Some(found.log);
                    break;
                }
                // `replies` lives as long as this loop, so the channel never
                // closes.
                Some(reply) = replied.recv() => {
                    woken_by = Some(reply);
                    break;
                }
            }
        }
    }
}

/// When [`run_driver`] re-crawls its node's peer routing
/// (`Net::refresh_peer_routing`), so that the workers of a shard stay
/// connected to one another and not only to their leader.
///
/// A worker's JOIN connects it to its seed and its leader alone, and `kad`'s
/// own crawl on that first connection finds only the peers its leader knew
/// by then: a burst joining through the leader would be left a star, which
/// no roll call crosses once the leader is gone. So the driver crawls again
/// once the node's view of its shard has changed (it names another leader,
/// takes on another configuration, or is admitted: peers it should reach
/// may have arrived) and then held still for [`ROUTING_SETTLE_DIVISOR`]th of
/// a suspicion timeout, and, failing that, every
/// [`DEFAULT_ROUTING_REFRESH_SUSPICIONS`] suspicion timeouts (see
/// `Net::with_routing_refresh_period`).
///
/// Waiting for the view to settle bounds the cost. A crawl's first run
/// connects the node to every peer it finds, a burst of connection
/// handshakes; a burst of joiners that each crawled at every change of an
/// admission in flight would spend them while the batch commits, and slow
/// it by whole seconds on one host. Settled, each node crawls once after
/// the burst, and a view that never settles still crawls within a period of
/// its first unserved change. The routing table is never read as
/// membership (see `crate::swarm`).
struct RoutingRefresh {
    /// The leader, configuration generation and admission last seen.
    seen: Option<(Option<WorkerId>, Option<Generation>, bool)>,
    /// When the view last changed.
    last_change: Instant,
    /// When the view first changed since the last crawl; `None` while no
    /// change waits for one.
    first_unserved: Option<Instant>,
    last_crawl: Option<Instant>,
    settle: kabudachi_core::time::Duration,
    period: kabudachi_core::time::Duration,
}

/// See [`RoutingRefresh`]: a changed view is crawled once it has held still
/// for this fraction of a suspicion timeout.
const ROUTING_SETTLE_DIVISOR: u64 = 4;

impl RoutingRefresh {
    fn new<C: Clock>(node: &WorkerNode<C>, net: &Net, now: Instant) -> Self {
        let suspect = node.timings().suspect_timeout.as_ticks();
        let period_millis = match net.routing_refresh_period() {
            Some(period) => u64::try_from(period.as_millis()).unwrap_or(u64::MAX),
            // A tick is a millisecond.
            None => suspect.saturating_mul(u64::from(DEFAULT_ROUTING_REFRESH_SUSPICIONS)),
        };
        let min_millis = u64::try_from(MIN_ROUTING_REFRESH_PERIOD.as_millis()).unwrap_or(u64::MAX);
        let period = kabudachi_core::time::Duration::from_millis(period_millis.max(min_millis));
        RoutingRefresh {
            seen: None,
            last_change: now,
            first_unserved: None,
            last_crawl: None,
            settle: kabudachi_core::time::Duration::from_ticks(suspect / ROUTING_SETTLE_DIVISOR),
            period,
        }
    }

    /// Notes what `node` shows after a batch, crawls if a settled change or
    /// the period calls for it, and returns when the next crawl is due.
    fn after_batch<C: Clock>(&mut self, node: &WorkerNode<C>, net: &Net, now: Instant) -> Instant {
        self.note(node, now);
        self.crawl_if_due(net, now)
    }

    /// Notes what `node` shows.
    fn note<C: Clock>(&mut self, node: &WorkerNode<C>, now: Instant) {
        let view = (
            node.known_leader().map(|(leader, _)| leader),
            node.configuration().map(Configuration::generation),
            node.admission().is_some(),
        );
        if self.seen.as_ref() != Some(&view) {
            self.seen = Some(view);
            self.last_change = now;
            self.first_unserved.get_or_insert(now);
        }
    }

    /// Crawls if a settled change or the period calls for it at `now`, and
    /// returns when the next crawl is due.
    fn crawl_if_due(&mut self, net: &Net, now: Instant) -> Instant {
        let due = match self.first_unserved {
            Some(first) => std::cmp::min(self.last_change + self.settle, first + self.period),
            None => self.last_crawl.map_or(now, |last| last + self.period),
        };
        if due > now {
            return due;
        }
        net.refresh_peer_routing();
        self.last_crawl = Some(now);
        self.first_unserved = None;
        now + self.period
    }
}

/// What one batch of [`run_driver`] steps its node with.
struct Stepper<'a, C: Clock, I: IdGenerator, O> {
    node: &'a mut WorkerNode<C>,
    scheduler: &'a mut Scheduler<C, I>,
    net: &'a Net,
    /// The authority to perform the node's calls against; `None` answers
    /// each at once as `Unavailable`.
    authority: Option<&'a SharedAuthority>,
    /// Where a call performed on the blocking pool sends its reply.
    replies: &'a mpsc::UnboundedSender<AuthorityReply>,
    /// The kinds of call performed and not yet answered.
    in_flight: &'a mut BTreeSet<CallKind>,
    my_id: &'a WorkerId,
    /// [`run_driver`]'s `observe`.
    observe: &'a mut O,
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
    /// names, as [`Self::step`] does with an [`Input::JoinAnswer`]: joining
    /// restarts its registration, which it asks for at once.
    fn rejoin(&mut self, pointer: JoinResponse) -> Option<Instant> {
        self.step(Input::JoinAnswer(pointer))
    }

    /// Carries out `stepped`, a step the node has just taken on `input`,
    /// through `carry_out`: its messages go out through the `Net` and its
    /// authority calls to a [`PoolPerformer`]. With no authority, each
    /// call's `Unavailable` reply is fed back at once instead, carrying out
    /// that step too, until a step asks for no more. Logs every step's
    /// alerts (see [`log_alerts`]), hands every step to `observe`, and
    /// returns the node's next deadline.
    fn carry(&mut self, stepped: Step, input: Option<&Input>) -> Option<Instant> {
        let mut performer = PoolPerformer {
            authority: self.authority,
            replies: self.replies,
            in_flight: &mut *self.in_flight,
            net: self.net,
            my_id: self.my_id,
            shard_id: self.node.shard_id().clone(),
        };
        let observe = &mut *self.observe;
        carry_out(
            &mut *self.node,
            stepped,
            &mut *self.scheduler,
            &mut &*self.net,
            &mut performer,
            |node, _, reply, step| {
                log_alerts(node, &step.outputs);
                // Only the first step has no reply for its input.
                observe(node, reply.or(input), step);
            },
        )
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

/// Performs authority calls for [`run_driver`]'s node, and for the
/// bootstrap cascade before it (see `crate::bootstrap`): on Tokio's blocking
/// pool, each reply sent to `replies`, whose reader removes the reply's kind
/// from `in_flight` (`reply.token().kind`) as it takes it. The driver
/// hands a reply to the node in the batch it arrives in. With no authority,
/// it answers every call at once as `Unavailable` instead. A call that
/// panics is answered as `Unavailable` too, so its kind is not left in
/// flight for ever.
///
/// At most one call of each kind is in flight: while one is unanswered, a
/// later call of the same kind is dropped, as if the authority had not
/// answered it. So an authority that hangs holds at most one blocking thread
/// per kind, rather than one more at every renewal. The node asks again on
/// its own schedule: its renewals come round, a fenced node reads the epoch
/// again at its next registration, and a forced recovery whose step was
/// dropped is replaced at its next roll call.
pub(crate) struct PoolPerformer<'a> {
    pub(crate) authority: Option<&'a SharedAuthority>,
    /// Where a call performed on the blocking pool sends its reply.
    pub(crate) replies: &'a mpsc::UnboundedSender<AuthorityReply>,
    /// The kinds of call performed and not yet answered.
    pub(crate) in_flight: &'a mut BTreeSet<CallKind>,
    /// Whose address a registration names.
    pub(crate) net: &'a Net,
    pub(crate) my_id: &'a WorkerId,
    pub(crate) shard_id: ShardId,
}

impl AuthorityPerformer for PoolPerformer<'_> {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        let Some(authority) = self.authority else {
            return Some(call.unavailable());
        };
        if !self.in_flight.insert(call.token.kind) {
            tracing::debug!(
                request = ?call.request,
                "not asking the coordination authority again while the same kind of call is \
                 unanswered"
            );
            return None;
        }
        let authority = Arc::clone(authority);
        let shard_id = self.shard_id.clone();
        let my_id = self.my_id.clone();
        let address = self
            .net
            .local_multiaddr()
            .map(|address| address.to_string())
            .unwrap_or_default();
        let replies = self.replies.clone();
        tokio::task::spawn_blocking(move || {
            let performed = std::panic::catch_unwind(AssertUnwindSafe(|| {
                call.perform(&*authority, &shard_id, &my_id, &address)
            }));
            let reply = performed.unwrap_or_else(|_| {
                tracing::error!(
                    request = ?call.request,
                    "a coordination authority call panicked; answering it as unavailable"
                );
                call.unavailable()
            });
            // Whoever asked has stopped: no one is left to hand it to.
            let _ = replies.send(reply);
        });
        None
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
            | Output::Authority(_)
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

/// What one [`find_leader_to_rejoin`] found: the pointer to rejoin through,
/// if any, and the log of why no search has found one, handed on to the
/// next search.
struct Rejoin {
    pointer: Option<JoinResponse>,
    log: WaitLog,
}

/// One search for the leader a node back in `Bootstrapping` rejoins, after
/// a recovery that went on without it: asks the workers `authority` lists as
/// live, at the addresses they registered, who leads (see
/// `crate::join::find_leader`, which asks a worker this node is still
/// connected to over that connection). No pointer, after a retry interval,
/// when the authority cannot be read, lists no one else, or no one points at
/// a reachable leader; the driver then searches again. Each search starts
/// `attempt` workers further along the list, so one worker the recovery also
/// left behind, whose pointer the node rejects as older than the epoch it
/// rejoins (see `Input::JoinAnswer`), cannot answer first for ever;
/// a search after such a refused pointer first waits a retry interval. It
/// never founds the shard: the epoch the node rejoins shows the shard
/// exists.
async fn find_leader_to_rejoin(
    net: &Net,
    authority: SharedAuthority,
    shard_id: ShardId,
    my_id: WorkerId,
    attempt: usize,
    after_a_refused_pointer: bool,
    mut log: WaitLog,
) -> Rejoin {
    if after_a_refused_pointer {
        tokio::time::sleep(DEFAULT_RETRY_INTERVAL).await;
    }
    let search = find_leader(
        net,
        &authority,
        &shard_id,
        &my_id,
        attempt,
        DEFAULT_JOIN_PEER_TIMEOUT,
        &mut log,
    )
    .await;
    log.end_round();
    if let LeaderSearch::Found(pointer) = search {
        return Rejoin {
            pointer: Some(pointer),
            log,
        };
    }
    tokio::time::sleep(DEFAULT_RETRY_INTERVAL).await;
    Rejoin { pointer: None, log }
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

/// Answers every inbound `/kabudachi/claim/1` request queued on `net` with
/// `scheduler`'s decision (README §8.2): `REQUEST_CLAIM` through
/// `Scheduler::request_claim`, `CLAIM_OLDEST` through
/// `Scheduler::claim_oldest`.
///
/// Whether this node leads is the scheduler's own call, from the leadership
/// grant [`carry_out`] last handed it and its clock, so nothing about the
/// node is read here: a worker holding no grant, or one whose grant's lease
/// has ended, refuses every claim as `NOT_LEADER`.
fn respond_to_claim_requests<C: Clock, I: IdGenerator>(scheduler: &mut Scheduler<C, I>, net: &Net) {
    for handle in net.poll_claim_requests() {
        let claimant = handle.from();
        let result = match handle.request() {
            claim_request::Request::TaskId(task_id) => scheduler
                .request_claim(&claimant, &TaskId::from(task_id.clone()))
                .map(|claim| claim_response::Result::Accept(wire_claim(claim))),
            claim_request::Request::Oldest(oldest) => {
                // A limit past what this platform can count is no limit.
                let limit = usize::try_from(oldest.limit).unwrap_or(usize::MAX);
                let mut batch = Batch::default();
                // Every claim the scheduler makes is one `batch` accepted,
                // in the same order, so `batch` already holds the answer.
                scheduler
                    .claim_oldest_fitting(&claimant, limit, |claim| batch.try_add(claim))
                    .map(|_| {
                        claim_response::Result::Batch(ClaimBatch {
                            claims: batch.claims,
                        })
                    })
            }
        };
        let result = result.unwrap_or_else(|rejection| {
            claim_response::Result::Reject(ClaimReject {
                reason: claim_reject_reason(rejection) as i32,
            })
        });
        net.respond_claim(
            handle,
            ClaimResponse {
                result: Some(result),
            },
        );
    }
}

/// A `CLAIM_OLDEST` answer as claims are added to it. A claim the
/// claimant could not decode would stay claimed by a worker that never
/// received it, so the leader claims only what fits in one message
/// (`MAX_MESSAGE_BYTES`); the tasks left over stay pending for the next ask.
#[derive(Default)]
struct Batch {
    claims: Vec<Claim>,
    /// The encoded length of `ClaimBatch { claims }`.
    encoded_len: usize,
}

impl Batch {
    /// Adds `claim` if the answer holding it still fits in one message, and
    /// says whether it did.
    fn try_add(&mut self, claim: &scheduler::Claim) -> bool {
        use prost::encoding::{encoded_len_varint, key_len, message};

        const CLAIMS_TAG: u32 = 1; // ClaimBatch.claims
        const BATCH_TAG: u32 = 3; // ClaimResponse.batch
        let claim = wire_claim(claim.clone());
        let encoded_len = self.encoded_len + message::encoded_len(CLAIMS_TAG, &claim);
        let response_len =
            key_len(BATCH_TAG) + encoded_len_varint(encoded_len as u64) + encoded_len;
        let fits = response_len <= MAX_MESSAGE_BYTES as usize;
        if fits {
            self.claims.push(claim);
            self.encoded_len = encoded_len;
        }
        fits
    }
}

fn wire_claim(claim: scheduler::Claim) -> Claim {
    Claim {
        task: Some(claim.task),
        task_run_id: Some(claim.task_run_id.into()),
        attempt_number: claim.attempt_number,
        chain: claim.chain,
    }
}

/// `core::scheduler::ClaimRejection` -> wire `ClaimRejectReason`, one arm per
/// variant and no wildcard arm, so a new `ClaimRejection` fails to compile
/// here instead of going out as the wrong reason.
fn claim_reject_reason(rejection: ClaimRejection) -> ClaimRejectReason {
    match rejection {
        ClaimRejection::NotLeader => ClaimRejectReason::ClaimRejectNotLeader,
        ClaimRejection::TaskUnknown => ClaimRejectReason::ClaimRejectTaskUnknown,
        ClaimRejection::NotReady => ClaimRejectReason::ClaimRejectNotReady,
        ClaimRejection::AlreadySelected => ClaimRejectReason::ClaimRejectAlreadySelected,
        ClaimRejection::Finished => ClaimRejectReason::ClaimRejectFinished,
        ClaimRejection::Superseded => ClaimRejectReason::ClaimRejectSuperseded,
        ClaimRejection::KeyBusy => ClaimRejectReason::ClaimRejectKeyBusy,
    }
}

#[cfg(test)]
mod tests {
    use kabudachi_core::configuration::{Configuration, Generation, Single};
    use kabudachi_core::election::{
        AuthorityTimings, ElectionTimings, Entry, Identity, KnownConfiguration,
    };
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids};
    use kabudachi_core::protocol::messages::{
        ElectionMessage, LeaderHeartbeatAck, election_message,
    };
    use kabudachi_core::time::{Duration as TickDuration, RealClock};
    use kabudachi_core::election::CallKind;
    use kabudachi_testkit::FaultingAuthority;
    use libp2p::identity;
    use tokio::sync::watch;
    use tokio::time::timeout;

    use super::*;
    use crate::swarm::build_swarm;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    fn ack_from(leader: &WorkerId) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(
                LeaderHeartbeatAck {
                    shard_id: Some(ShardId::new("shard-1").into()),
                    leader_id: Some(leader.clone().into()),
                    recovery_epoch: 0,
                    term: 1,
                    configuration: Some((&two_voters()).into()),
                    recipient_admission: Some(Generation::genesis(0).into()),
                    send_token: 0,
                    recipient_prior_admission: None,
                    heartbeat_token: None,
                    recovery_epoch_lineage: None,
                },
            )),
        }
    }

    type TestNode = WorkerNode<RealClock>;

    /// A configuration of two voters, both admitted at genesis.
    fn two_voters() -> Configuration {
        Configuration::single(Single {
            generation: Generation::genesis(0),
            base: Generation::genesis(0),
            voter_count: 2,
        })
    }

    /// `me`'s node, a voter of a configuration of two, suspecting a leader
    /// only after `suspect_timeout`, and the first step it starts with.
    fn node_of_two(
        clock: RealClock,
        me: &WorkerId,
        suspect_timeout: TickDuration,
    ) -> (TestNode, Step) {
        node_of_two_with(clock, me, suspect_timeout, None)
    }

    /// [`node_of_two`], with `authority` as its authority timings.
    fn node_of_two_with(
        clock: RealClock,
        me: &WorkerId,
        suspect_timeout: TickDuration,
        authority: Option<AuthorityTimings>,
    ) -> (TestNode, Step) {
        let identity = Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-0"),
            shard: ShardId::new("shard-1"),
            // Twice 40 ms fits inside the lease of the shortest suspicion
            // timeout the tests here use (100 ms, less a tenth).
            timings: ElectionTimings::new(suspect_timeout, TickDuration::from_millis(40))
                .with_roll_call_deadline(TickDuration::from_millis(100)),
        };
        let known = KnownConfiguration {
            configuration: two_voters(),
            admission: Some(Generation::genesis(0)),
        };
        WorkerNode::start(identity, Entry::Known(known), clock, authority)
    }

    /// Takes `net`'s queued inputs until one is `expected`.
    async fn wait_for_input(net: &Net, expected: &Input) {
        timeout(TEST_TIMEOUT, async {
            while !net.take_inputs().contains(expected) {
                net.wait_for_arrival().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{expected:?} arrived within the timeout"));
    }

    /// A `Net` standing in for the leader of the returned node's `Net`, and
    /// that node's `Net`, connected. Only the stand-in's inputs are taken.
    async fn stand_in_leader_and_node_nets() -> (Net, Net) {
        let net_leader = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_node = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let node_addr = timeout(
            TEST_TIMEOUT,
            net_node.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_node produced a listen address within the timeout");
        net_leader.dial(node_addr);
        wait_for_input(
            &net_leader,
            &Input::PeerConnected(net_node.local_worker_id()),
        )
        .await;
        (net_leader, net_node)
    }

    #[tokio::test]
    async fn run_driver_steps_the_node_when_a_message_arrives_and_not_on_a_timer() {
        let (net_leader, net_node) = stand_in_leader_and_node_nets().await;
        let (leader, me) = (net_leader.local_worker_id(), net_node.local_worker_id());
        let clock = RealClock::new();
        // Its deadline is a minute away, so only an arrival can wake it.
        let (mut node, first) = node_of_two(clock, &me, TickDuration::from_secs(60));
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let (steps, mut observed) = watch::channel((0_usize, None));

        timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = run_driver(&mut node, first, &net_node, &mut scheduler, clock, None, |node, _, _| {
                    steps.send_modify(|(count, leader)| {
                        *count += 1;
                        *leader = node.known_leader();
                    });
                }) => unreachable!("run_driver never returns"),
                () = async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    // The step it started with, the tick that is due at
                    // once, and one for the connection to its stand-in
                    // leader.
                    let quiet_steps = observed.borrow().0;
                    assert!(
                        quiet_steps <= 3,
                        "the driver stepped the node {quiet_steps} times with nothing to do"
                    );

                    net_leader.send(me.clone(), ack_from(&leader));

                    observed
                        .wait_for(|(_, known)| *known == Some((leader.clone(), 1)))
                        .await
                        .expect("the observer is still alive");
                    // Accepting the ack, the node starts heartbeating its
                    // leader: its outputs go out through its `Net`.
                    timeout(TEST_TIMEOUT, async {
                        loop {
                            let heartbeat_arrived = net_leader.take_inputs().iter().any(|input| {
                                matches!(
                                    input,
                                    Input::Message { from, message }
                                        if *from == me
                                            && matches!(
                                                message.payload,
                                                Some(election_message::Payload::Heartbeat(_))
                                            )
                                )
                            });
                            if heartbeat_arrived {
                                return;
                            }
                            net_leader.wait_for_arrival().await;
                        }
                    })
                    .await
                    .expect("the node's heartbeat reached its leader within the timeout");
                } => {}
            }
        })
        .await
        .expect("the node accepted its leader's ack within the timeout");
    }

    // A node registers at its first step. An authority slow to answer that
    // must not hold up the rest of what the node does, such as heartbeating
    // the leader whose ack it accepts meanwhile.
    #[tokio::test]
    async fn a_slow_authority_holds_up_nothing_else_the_node_does() {
        let (net_leader, net_node) = stand_in_leader_and_node_nets().await;
        let (leader, me) = (net_leader.local_worker_id(), net_node.local_worker_id());
        let clock = RealClock::new();
        let (mut node, first) = node_of_two_with(
            clock,
            &me,
            TickDuration::from_secs(60),
            Some(AuthorityTimings::default()),
        );
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        // The authority is down, and its answer to the node's registration
        // is held: the call is still in flight while everything else runs.
        let authority = FaultingAuthority::new(clock, TickDuration::from_secs(60));
        authority.set_available(false);
        authority.hold_next(CallKind::Register);

        let heartbeated = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = run_driver(
                    &mut node,
                    first,
                    &net_node,
                    &mut scheduler,
                    clock,
                    Some(Arc::new(authority.clone())),
                    |_, _, _| {},
                ) => unreachable!("run_driver never returns"),
                () = async {
                    net_leader.send(me.clone(), ack_from(&leader));
                    loop {
                        let heartbeat_arrived = net_leader.take_inputs().iter().any(|input| {
                            matches!(
                                input,
                                Input::Message { from, message }
                                    if *from == me
                                        && matches!(
                                            message.payload,
                                            Some(election_message::Payload::Heartbeat(_))
                                        )
                            )
                        });
                        if heartbeat_arrived {
                            return;
                        }
                        net_leader.wait_for_arrival().await;
                    }
                } => {}
            }
        })
        .await;

        // Released before anything can fail, so no path leaves the held
        // thread parked and hangs the runtime's shutdown.
        let held = authority.is_holding(CallKind::Register);
        authority.release(CallKind::Register);
        heartbeated.expect("the node heartbeated its leader within the timeout");
        assert!(
            held,
            "the heartbeat arrived only after the authority call was released"
        );
    }

    #[tokio::test]
    async fn run_driver_ticks_the_node_when_its_deadline_comes() {
        let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let me = net.local_worker_id();
        let started = std::time::Instant::now();
        let clock = RealClock::new();
        let suspect_timeout = TickDuration::from_millis(100);
        let (mut node, first) = node_of_two(clock, &me, suspect_timeout);
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let (steps, mut observed) = watch::channel(Vec::<(Duration, Vec<Output>)>::new());

        let steps = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = run_driver(&mut node, first, &net, &mut scheduler, clock, None, |_, _, step| {
                    steps.send_modify(|steps| {
                        steps.push((started.elapsed(), step.outputs.clone()));
                    });
                }) => unreachable!("run_driver never returns"),
                steps = observed.wait_for(|steps| steps.iter().any(|(_, outputs)| !outputs.is_empty())) => {
                    steps.expect("the observer is still alive").clone()
                }
            }
        })
        .await
        .expect("the driver moved the node on within the timeout");

        // Nothing arrives here, so the driver steps the node at once to
        // learn its deadline (the step it started with, and the tick that
        // is due at once) and then not until that deadline, whose tick
        // moves it on.
        let moved_at = steps
            .iter()
            .position(|(_, outputs)| !outputs.is_empty())
            .expect("it waited for a step with outputs");
        let (moved, before) = (&steps[moved_at], &steps[..moved_at]);
        let deadline = Duration::from_millis(suspect_timeout.as_ticks());
        assert!(
            before.iter().all(|(at, _)| *at < deadline / 2),
            "the node was stepped before its deadline: {before:?}"
        );
        assert!(
            moved.0 >= deadline,
            "the node was moved on {:?} after it was built, before its deadline",
            moved.0
        );
    }

    #[tokio::test]
    async fn run_driver_first_feeds_the_node_what_arrived_before_it_started() {
        let (net_leader, net_node) = stand_in_leader_and_node_nets().await;
        let (leader, me) = (net_leader.local_worker_id(), net_node.local_worker_id());
        // The connection first, then the ack, each arriving while no driver
        // runs.
        timeout(TEST_TIMEOUT, net_node.wait_for_arrival())
            .await
            .expect("the connection reached net_node within the timeout");
        net_leader.send(me.clone(), ack_from(&leader));
        timeout(TEST_TIMEOUT, net_node.wait_for_arrival())
            .await
            .expect("the ack reached net_node within the timeout");

        let clock = RealClock::new();
        let (mut node, first) = node_of_two(clock, &me, TickDuration::from_secs(60));
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let (known, mut observed) = watch::channel(None);

        // Nothing arrives once it runs, and its deadline is a minute away,
        // so only its first batch can feed it the ack.
        let known_leader = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = run_driver(&mut node, first, &net_node, &mut scheduler, clock, None, |node, _, _| {
                    known.send_replace(node.known_leader());
                }) => unreachable!("run_driver never returns"),
                known = observed.wait_for(Option::is_some) => {
                    known.expect("the observer is still alive").clone()
                }
            }
        })
        .await
        .expect("the driver fed the node the ack within the timeout");

        assert_eq!(known_leader, Some((leader, 1)));
    }

}

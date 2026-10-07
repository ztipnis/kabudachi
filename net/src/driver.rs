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
//! has changed and settled, and periodically (see `crate::routing_refresh`), so
//! the shard's workers stay connected to one another and not only to their
//! leader.
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

use std::convert::Infallible;
use std::time::Duration;

use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, Input, Issuer, MessageSink, Output, Step,
    WorkerNode, carry_out,
};
use kabudachi_core::protocol::ids::{IdGenerator, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, JoinResponse};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Clock, Instant};

use tokio::time::Instant as TokioInstant;

use crate::authority::AuthorityClient;
use crate::bootstrap::DEFAULT_RETRY_INTERVAL;
use crate::claim;
use crate::join::{DEFAULT_JOIN_PEER_TIMEOUT, LeaderSearch, pointer_for};
use crate::leader_search::{JoinOverNet, Rejoin};
use crate::messenger::Net;
pub use crate::routing_refresh::{DEFAULT_ROUTING_REFRESH_SUSPICIONS, MIN_ROUTING_REFRESH_PERIOD};
use crate::routing_refresh::{RoutingRefresh, ShardView};

/// How [`run_driver`] runs, beyond the node, transport and scheduler it drives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DriverConfig {
    /// How often to re-crawl peer routing while nothing else prompts it; `None`
    /// for [`DEFAULT_ROUTING_REFRESH_SUSPICIONS`] suspicion timeouts. Never
    /// under [`MIN_ROUTING_REFRESH_PERIOD`].
    pub routing_refresh_period: Option<Duration>,
}

pub use crate::authority::SharedAuthority;

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
    scheduler: &mut Scheduler<C, I>,
    clock: C,
    mut authority: Option<AuthorityClient>,
    config: DriverConfig,
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
    // The reply whose arrival ended the last sleep, if one did.
    let mut woken_by = None;
    // While the node is back in `Bootstrapping` or `Joining`: its search for a
    // leader to rejoin, and the pointer the last round found.
    let mut rejoin: Option<Rejoin<'_, JoinOverNet<'_>>> = None;
    let mut rejoin_pointer = None;
    // The epoch read the rejoin asked last round, to tell the node of before
    // its answer arrives.
    let mut epoch_read_asked = None;
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
        let mut stepper = Stepper {
            node: &mut *node,
            scheduler: &mut *scheduler,
            net,
            calls: authority.as_mut(),
            observe: &mut observe,
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
                Issuer::Cascade => match rejoin.as_mut() {
                    Some(rejoin) => {
                        if let Some((token, held)) = rejoin.offer(
                            reply,
                            TokioInstant::now(),
                            stepper.node.join_floor(),
                        ) {
                            next_deadline =
                                stepper.step(Input::AuthorityEpochRead { token, held });
                        }
                    }
                    None => tracing::debug!(
                        "dropping a reply to a call net asked for itself: no rejoin is running"
                    ),
                },
            }
        }
        for input in net.take_inputs() {
            next_deadline = stepper.step(input);
        }
        if let Some(pointer) = rejoin_pointer.take() {
            // A pointer the node would not take (to its old epoch's leader,
            // or another lineage's) leaves it in `Bootstrapping`, and the
            // rejoin, which has already paced its next round, goes on.
            next_deadline = stepper.rejoin(pointer);
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
        // What the node showed after this batch: the timer arm below decides
        // on this same view, since the node is not stepped between batches.
        let view = ShardView::of(node);
        let mut refresh = routing.decide(&view, clock.now());
        if refresh.crawl {
            net.refresh_peer_routing();
        }

        // A fenced node that found its shard recovered without it went back
        // to `Bootstrapping` to join again. Only a
        // node with an authority fences itself, and that authority lists
        // whom to ask.
        match authority.as_mut() {
            Some(client)
                if matches!(
                    node.state(),
                    WorkerState::Bootstrapping | WorkerState::Joining
                ) =>
            {
                let rejoin = rejoin.get_or_insert_with(|| {
                    Rejoin::new(
                        node.shard_id(),
                        my_id.clone(),
                        JoinOverNet {
                            net,
                            per_peer_timeout: DEFAULT_JOIN_PEER_TIMEOUT,
                            grace: Duration::from_millis(node.timings().suspect_timeout.as_ticks()),
                        },
                        DEFAULT_RETRY_INTERVAL,
                        TokioInstant::now(),
                    )
                });
                if node.state() == WorkerState::Bootstrapping {
                    rejoin.tick(client, clock.now(), TokioInstant::now());
                } else {
                    // `Joining` on a pointer it took: the authority's epoch
                    // is what confirms it.
                    rejoin.validate(client, clock.now(), TokioInstant::now());
                }
                epoch_read_asked = rejoin.take_asked_epoch_read().or(epoch_read_asked);
            }
            _ => rejoin = None,
        }
        let rejoin_wake = rejoin.as_ref().and_then(Rejoin::wake_at);

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
                () = wake_at(rejoin_wake) => break,
                found = ask_done(&mut rejoin) => {
                    rejoin_pointer = rejoin
                        .as_mut()
                        .and_then(|rejoin| rejoin.asked(found, TokioInstant::now()));
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

/// What one batch of [`run_driver`] steps its node with.
struct Stepper<'a, C: Clock, I: IdGenerator, O> {
    node: &'a mut WorkerNode<C>,
    scheduler: &'a mut Scheduler<C, I>,
    net: &'a Net,
    /// The client to perform the node's calls with; `None` answers each at
    /// once as `Unavailable`.
    calls: Option<&'a mut AuthorityClient>,
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

/// Sleeps until `at`, or for ever when there is none.
async fn wake_at(at: Option<TokioInstant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// The round's ask of a running rejoin, once it finishes; for ever when none
/// runs. Cancel-safe.
async fn ask_done<'a>(rejoin: &mut Option<Rejoin<'a, JoinOverNet<'a>>>) -> LeaderSearch {
    match rejoin {
        Some(rejoin) => rejoin.ask_done().await,
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

/// Answers every inbound `/kabudachi/claim/1` request queued on `net` with
/// `scheduler`'s decision (see `claim::answer`).
fn respond_to_claim_requests<C: Clock, I: IdGenerator>(scheduler: &mut Scheduler<C, I>, net: &Net) {
    for handle in net.poll_claim_requests() {
        let response = claim::answer(scheduler, &handle.from(), handle.request());
        net.respond_claim(handle, response);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kabudachi_core::configuration::{Configuration, Generation, Single};
    use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch};
    use kabudachi_core::election::{
        AuthorityTimings, ElectionTimings, Entry, Identity, KnownConfiguration,
    };
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids};
    use kabudachi_core::protocol::messages::{
        ElectionMessage, LeaderHeartbeatAck, election_message,
    };
    use kabudachi_core::time::{Duration as TickDuration, RealClock};
    use kabudachi_core::election::CallKind;
    use kabudachi_core::protocol::worker_state::WorkerState;
    use kabudachi_testkit::FaultingAuthority;
    use tokio::sync::watch;
    use tokio::time::timeout;

    use super::*;
    use crate::test_support::{TEST_TIMEOUT, listening_net, spawn_join_responder};

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
        }).expect("valid")
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
                .with_roll_call_deadline(TickDuration::from_millis(50)),
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
        let net_leader = Net::new();
        let net_node = Net::new();
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
                _ = run_driver(&mut node, first, &net_node, &mut scheduler, clock, None, DriverConfig::default(), |node, _, _| {
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
                                                message.message().payload,
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
                    Some(AuthorityClient::new(
                        &net_node,
                        ShardId::new("shard-1"),
                        Arc::new(authority.clone()),
                    )),
                    DriverConfig::default(),
                    |_, _, _| {},
                ) => unreachable!("run_driver never returns"),
                () = async {
                    // The registration must be held before the ack arrives:
                    // only then does a heartbeat show that the held call
                    // stops nothing else.
                    while !authority.is_holding(CallKind::Register) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    net_leader.send(me.clone(), ack_from(&leader));
                    loop {
                        let heartbeat_arrived = net_leader.take_inputs().iter().any(|input| {
                            matches!(
                                input,
                                Input::Message { from, message }
                                    if *from == me
                                        && matches!(
                                            message.message().payload,
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
        let net = Net::new();
        let me = net.local_worker_id();
        let started = std::time::Instant::now();
        let clock = RealClock::new();
        let suspect_timeout = TickDuration::from_millis(100);
        let (mut node, first) = node_of_two(clock, &me, suspect_timeout);
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let (steps, mut observed) = watch::channel(Vec::<(Duration, Vec<Output>)>::new());

        let steps = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = run_driver(&mut node, first, &net, &mut scheduler, clock, None, DriverConfig::default(), |_, _, step| {
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
                _ = run_driver(&mut node, first, &net_node, &mut scheduler, clock, None, DriverConfig::default(), |node, _, _| {
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

    #[tokio::test]
    async fn a_joiner_heartbeats_its_leader_at_once_rather_than_an_interval_later() {
        let (leader_net, leader_addr) = listening_net().await;
        let leader_net = Arc::new(leader_net);
        let pointer = JoinResponse {
            leader_id: Some(leader_net.local_worker_id().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        };
        let _responder = spawn_join_responder(Arc::clone(&leader_net), pointer);
        let joining_net = Net::new();
        let joiner = joining_net.local_worker_id();
        // A heartbeat that waited for its interval would come long after
        // this test gives up.
        let timings =
            ElectionTimings::new(TickDuration::from_secs(120), TickDuration::from_secs(50));

        let clock = RealClock::new();
        let joined_and_heard = async {
            let entry = crate::bootstrap::bootstrap(
                &joining_net,
                &clock,
                None,
                &ShardId::new("shard-1"),
                &joiner,
                std::slice::from_ref(&leader_addr),
                Duration::from_secs(5),
                Duration::from_secs(5),
                DEFAULT_RETRY_INTERVAL,
            )
            .await;
            let identity = Identity {
                id: joiner.clone(),
                incarnation: IncarnationId::new("incarnation-1"),
                shard: ShardId::new("shard-1"),
                timings,
            };
            let (mut node, first) = WorkerNode::start(identity, entry, clock, None);
            let mut scheduler = Scheduler::new(clock, Uuid7Ids);
            let driven = run_driver(
                &mut node,
                first,
                &joining_net,
                &mut scheduler,
                clock,
                None,
                DriverConfig::default(),
                |_, _, _| {},
            );
            let heard = async {
                loop {
                    let heard = leader_net.take_inputs().into_iter().any(|input| {
                        matches!(
                            input,
                            Input::Message { from, message }
                                if from == joiner
                                    && matches!(
                                        message.message().payload,
                                        Some(election_message::Payload::Heartbeat(_))
                                    )
                        )
                    });
                    if heard {
                        return;
                    }
                    leader_net.wait_for_arrival().await;
                }
            };
            tokio::select! {
                _ = driven => unreachable!("run_driver never returns"),
                () = heard => {}
            }
        };

        timeout(TEST_TIMEOUT, joined_and_heard)
            .await
            .expect("the joiner's first heartbeat reached its leader within the timeout");
    }

    // A node fenced from its authority while the shard recovers without it
    // goes back to `Bootstrapping`; the driver then reads the authority's
    // live registrations itself and joins the worker listed there that leads.
    #[tokio::test]
    async fn a_fenced_node_the_shard_recovered_without_rejoins_the_leader_the_authority_lists() {
        let shard = ShardId::new("shard-1");
        let (leader_net, leader_addr) = listening_net().await;
        let leader_net = Arc::new(leader_net);
        let leader = leader_net.local_worker_id();
        let _responder = spawn_join_responder(
            Arc::clone(&leader_net),
            JoinResponse {
                leader_id: Some(leader.clone().into()),
                leader_multiaddr: leader_addr.to_string(),
                term: 1,
                recovery_epoch: 1,
                recovery_epoch_lineage: 0,
            },
        );
        let node_net = Net::new();
        let me = node_net.local_worker_id();

        let clock = RealClock::new();
        let ttl = TickDuration::from_millis(1_000);
        let authority = FaultingAuthority::new(clock, ttl);
        authority
            .compare_and_swap_recovery_epoch(&shard, None, RecoveryEpoch::new(0, 0))
            .expect("a fresh authority holds no epoch");
        // Cut off from the start, the node's registration lapses and it
        // fences itself.
        let node_handle = authority.for_another_worker();
        node_handle.set_reachable(false);
        let (mut node, first) = node_of_two_with(
            clock,
            &me,
            TickDuration::from_millis(300),
            Some(AuthorityTimings { ttl }),
        );
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let (seen, mut observed) = watch::channel((Vec::<WorkerState>::new(), None));
        let client = AuthorityClient::new(&node_net, shard.clone(), Arc::new(node_handle.clone()));

        let driven = run_driver(
            &mut node,
            first,
            &node_net,
            &mut scheduler,
            clock,
            Some(client),
            DriverConfig::default(),
            |node, _, _| {
                seen.send_modify(|(states, known)| {
                    if states.last() != Some(&node.state()) {
                        states.push(node.state());
                    }
                    *known = node.known_leader();
                });
            },
        );
        let scenario = async {
            observed
                .wait_for(|(states, _)| states.contains(&WorkerState::Fenced))
                .await
                .expect("the observer is alive");
            // The other worker leads at epoch 1 and is registered; the
            // fenced node can reach the authority again.
            authority
                .compare_and_swap_recovery_epoch(
                    &shard,
                    Some(RecoveryEpoch::new(0, 0)),
                    RecoveryEpoch::new(1, 0),
                )
                .expect("the epoch is still 0");
            authority
                .register(&shard, &leader, &leader_addr.to_string())
                .expect("the authority is reachable");
            node_handle.set_reachable(true);
            observed
                .wait_for(|(states, known)| {
                    states.contains(&WorkerState::Bootstrapping)
                        && states.last() == Some(&WorkerState::Active)
                        && known.is_some()
                })
                .await
                .expect("the observer is alive")
                .clone()
        };
        let (_, known) = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = driven => unreachable!("run_driver never returns"),
                states = scenario => states,
            }
        })
        .await
        .expect("the fenced node rejoined within the timeout");
        assert_eq!(known, Some((leader, 1)));
    }

    /// Registers every one of `leaders` at `authority` again and again, for
    /// ever: a registration lasts one `ttl`, so a test that must keep a worker
    /// listed while it waits keeps registering it.
    async fn keep_registered(
        authority: &FaultingAuthority<RealClock>,
        shard: &ShardId,
        leaders: &[(WorkerId, String)],
    ) {
        loop {
            for (id, address) in leaders {
                authority
                    .register(shard, id, address)
                    .expect("the authority is reachable");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // A node that fenced itself and found its shard recovered at epoch 2 of
    // one lineage rejoins at that floor. The shard is then flushed and founded
    // afresh at the same number under another lineage: the epoch the authority
    // holds must reach the node, or its leader's pointer, at no higher number,
    // is refused for ever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejoining_node_takes_a_leader_of_a_refounded_shard_at_the_floors_number() {
        let clock = RealClock::new();
        let ttl = TickDuration::from_millis(1_000);
        let authority = FaultingAuthority::new(clock, ttl);
        let shard = ShardId::new("shard-1");
        let elsewhere = authority.for_another_worker();
        elsewhere
            .compare_and_swap_recovery_epoch(&shard, None, RecoveryEpoch::new(0, 0))
            .expect("a fresh authority holds no epoch");
        let (leader_net, leader_addr) = listening_net().await;
        let leader_net = Arc::new(leader_net);
        let leader = leader_net.local_worker_id();
        let _responder = spawn_join_responder(
            Arc::clone(&leader_net),
            JoinResponse {
                leader_id: Some(leader.clone().into()),
                leader_multiaddr: leader_addr.to_string(),
                term: 1,
                recovery_epoch: 2,
                recovery_epoch_lineage: 2,
            },
        );
        let net = Net::new();
        let me = net.local_worker_id();
        let (mut node, first) = node_of_two_with(
            clock,
            &me,
            TickDuration::from_secs(60),
            Some(AuthorityTimings { ttl }),
        );
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let mine = authority.for_another_worker();
        let (seen_tx, mut seen) = watch::channel((WorkerState::Active, None));
        let driven = run_driver(
            &mut node,
            first,
            &net,
            &mut scheduler,
            clock,
            Some(AuthorityClient::new(&net, shard.clone(), Arc::new(mine.clone()))),
            DriverConfig::default(),
            |node, _, _| {
                seen_tx.send_replace((node.state(), node.recovery_lineage()));
            },
        );
        let last = seen.clone();
        let listed = tokio::sync::Notify::new();
        let register_leader = async {
            listed.notified().await;
            keep_registered(&elsewhere, &shard, &[(leader.clone(), leader_addr.to_string())]).await;
        };
        let scenario = async {
            let mut wait_for = async |what: &str, holds: fn(&(WorkerState, Option<u64>)) -> bool| {
                timeout(TEST_TIMEOUT, seen.wait_for(holds))
                    .await
                    .unwrap_or_else(|_| panic!("{what} within the timeout: {:?}", *last.borrow()))
                    .expect("the driver is running");
            };
            mine.set_reachable(false);
            wait_for("the node fenced itself", |seen| seen.0 == WorkerState::Fenced).await;
            elsewhere
                .compare_and_swap_recovery_epoch(
                    &shard,
                    Some(RecoveryEpoch::new(0, 0)),
                    RecoveryEpoch::new(2, 1),
                )
                .expect("a recovery elsewhere moved the epoch on");
            mine.set_reachable(true);
            wait_for("the node rejoined at epoch 2 of lineage 1", |seen| {
                *seen == (WorkerState::Bootstrapping, Some(1))
            })
            .await;
            // The shard is flushed and founded afresh at the same number, and
            // its leader is listed only now.
            elsewhere
                .compare_and_swap_recovery_epoch(
                    &shard,
                    Some(RecoveryEpoch::new(2, 1)),
                    RecoveryEpoch::new(2, 2),
                )
                .expect("the shard is founded afresh");
            listed.notify_one();
            wait_for("the node joined the new shard's leader", |seen| {
                *seen == (WorkerState::Active, Some(2))
            })
            .await;
        };
        tokio::select! {
            _ = driven => unreachable!("run_driver never returns"),
            () = register_leader => unreachable!("registering never ends"),
            () = scenario => {}
        }
    }

    // A rejoining node takes the pointer of a stale leader of the lineage its
    // floor stands at, while the authority holds the shard refounded under
    // another lineage at that same number. The node must not become that
    // leader's member: it holds the pointer in `Joining` until a read of the
    // authority validates it, finds the epoch differs, drops the pointer and
    // joins the new lineage's leader.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_that_took_a_pointer_of_a_refounded_lineage_ends_active_in_the_new_one() {
        let clock = RealClock::new();
        let ttl = TickDuration::from_millis(1_000);
        let authority = FaultingAuthority::new(clock, ttl);
        let shard = ShardId::new("shard-1");
        let elsewhere = authority.for_another_worker();
        elsewhere
            .compare_and_swap_recovery_epoch(&shard, None, RecoveryEpoch::new(0, 0))
            .expect("a fresh authority holds no epoch");
        // Two leaders answer JOIN, each pointing at itself: the old lineage's
        // and the refounded one's.
        let mut leaders = Vec::new();
        for lineage in [1, 2] {
            let (net, address) = listening_net().await;
            let net = Arc::new(net);
            let id = net.local_worker_id();
            let responder = spawn_join_responder(
                Arc::clone(&net),
                JoinResponse {
                    leader_id: Some(id.clone().into()),
                    leader_multiaddr: address.to_string(),
                    term: 1,
                    recovery_epoch: 2,
                    recovery_epoch_lineage: lineage,
                },
            );
            leaders.push((net, id, address, responder));
        }
        let net = Net::new();
        let me = net.local_worker_id();
        let (mut node, first) = node_of_two_with(
            clock,
            &me,
            TickDuration::from_secs(60),
            Some(AuthorityTimings { ttl }),
        );
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        let mine = authority.for_another_worker();
        let (seen_tx, mut seen) = watch::channel((WorkerState::Active, None));
        let driven = run_driver(
            &mut node,
            first,
            &net,
            &mut scheduler,
            clock,
            Some(AuthorityClient::new(&net, shard.clone(), Arc::new(mine.clone()))),
            DriverConfig::default(),
            |node, _, _| {
                seen_tx.send_replace((node.state(), node.recovery_lineage()));
            },
        );
        let last = seen.clone();
        let listed = tokio::sync::Notify::new();
        let register_leaders = async {
            listed.notified().await;
            let listed: Vec<_> = leaders
                .iter()
                .map(|(_, id, address, _)| (id.clone(), address.to_string()))
                .collect();
            keep_registered(&elsewhere, &shard, &listed).await;
        };
        let scenario = async {
            let mut wait_for = async |what: &str, holds: fn(&(WorkerState, Option<u64>)) -> bool| {
                timeout(TEST_TIMEOUT, seen.wait_for(holds))
                    .await
                    .unwrap_or_else(|_| panic!("{what} within the timeout: {:?}", *last.borrow()))
                    .expect("the driver is running");
            };
            mine.set_reachable(false);
            wait_for("the node fenced itself", |seen| seen.0 == WorkerState::Fenced).await;
            elsewhere
                .compare_and_swap_recovery_epoch(
                    &shard,
                    Some(RecoveryEpoch::new(0, 0)),
                    RecoveryEpoch::new(2, 1),
                )
                .expect("a recovery elsewhere moved the epoch on");
            mine.set_reachable(true);
            wait_for("the node rejoined at epoch 2 of lineage 1", |seen| {
                *seen == (WorkerState::Bootstrapping, Some(1))
            })
            .await;
            // The node's next read of the epoch lags, and the shard is
            // refounded under lineage 2 at the same number. Both leaders are
            // listed only now: the old one's pointer is the only one the
            // node's floor accepts.
            mine.hold_next(CallKind::ReadRecoveryEpoch);
            elsewhere
                .compare_and_swap_recovery_epoch(
                    &shard,
                    Some(RecoveryEpoch::new(2, 1)),
                    RecoveryEpoch::new(2, 2),
                )
                .expect("the shard is refounded");
            listed.notify_one();
            wait_for("the node took the old lineage's pointer and awaits the authority", |seen| {
                *seen == (WorkerState::Joining, Some(1))
            })
            .await;
            mine.release(CallKind::ReadRecoveryEpoch);
            wait_for("the node is a member in the refounded lineage", |seen| {
                *seen == (WorkerState::Active, Some(2))
            })
            .await;
        };
        tokio::select! {
            _ = driven => unreachable!("run_driver never returns"),
            () = register_leaders => unreachable!("registering never ends"),
            () = scenario => {}
        }
    }
}

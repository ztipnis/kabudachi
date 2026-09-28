//! Worker-entry-point helpers shared by the `net/tests/*.rs` files that
//! drive a shard through [`kabudachi_net::worker::Worker`] rather than a
//! bare `Net` and node: each spawns a real process-shaped worker (its own
//! identity, its own `Net` listening on real loopback TCP) on its own task,
//! and hands back a handle to observe and, when a test needs it, to cut.
//!
//! First written for `bootstrap_join_test.rs` (E9); reused, not duplicated,
//! by later chunks that also drive workers end to end (E8's leaderless
//! election after a cut leader among them).

use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::configuration::{Admission, Configuration};
use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{AuthorityTimings, ElectionTimings, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::{SharedAuthority, run_driver};
use kabudachi_net::messenger::Net;
use kabudachi_net::worker::{Worker, WorkerConfig};
use libp2p::Multiaddr;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::net::ask_until_pointed_at_a_leader;

/// Generous whole-test backstop for everything a spawned worker is expected
/// to do. Not a tuning knob: individual tests bound their own timing
/// expectations more tightly with their own waits and windows.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// What a worker's driver last showed of its node.
#[derive(Clone, Debug, PartialEq)]
pub struct Seen {
    pub state: WorkerState,
    pub pending: bool,
    pub leader: Option<WorkerId>,
    pub term: u64,
    pub configuration: Option<Configuration>,
    pub admission: Admission,
}

impl Seen {
    fn of(node: &WorkerNode<RealClock>) -> Self {
        Seen {
            state: node.state(),
            pending: node.is_pending_member(),
            leader: node.known_leader().map(|(leader, _)| leader),
            term: node.term(),
            configuration: node.configuration().cloned(),
            admission: Admission {
                current: node.admission(),
                prior: node.prior_admission(),
            },
        }
    }

    /// Whether this node counts as a voter of `configuration`, by the
    /// admission generations it holds.
    pub fn is_voter_of(&self, configuration: &Configuration) -> bool {
        configuration.is_voter(self.admission)
    }
}

/// One running worker: its id, the address it listens on, its `Net` (for
/// what a test needs beyond what the driven node does itself), what its
/// driver last showed of its node, and the task driving it.
///
/// Dropping a `RunningWorker` cuts it: `Drop` aborts the driving task, and
/// dropping this struct's own `Arc<Net>` clone alongside it drops the last
/// reference once the task's own clone goes with it, which drops the `Net`
/// in turn (see `kabudachi_net::messenger::Net`'s `Drop`, which aborts its
/// swarm-driving task). That closes the worker's real listening socket and
/// every connection it held: the process is gone. Harder and more real
/// than `Net::disconnect`, which only hangs up a connection its own side
/// may redial; a deliberate choice over `Net::block_peer` (E13) too, which
/// isolates a still-alive leader rather than removing it — see
/// `docs/superpowers/plans/notes/e8.md`, E8-R1.
pub struct RunningWorker {
    pub id: WorkerId,
    pub address: Multiaddr,
    pub net: Arc<Net>,
    pub seen: watch::Receiver<Option<Seen>>,
    task: JoinHandle<()>,
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RunningWorker {
    /// Waits until this worker's node shows something `until` accepts.
    pub async fn wait_until(&mut self, until: impl Fn(&Seen) -> bool) -> Seen {
        wait_for_seen(&mut self.seen, until).await
    }

    /// What this worker's node last showed, if it has bootstrapped.
    pub fn last_seen_if_any(&self) -> Option<Seen> {
        self.seen.borrow().clone()
    }

    /// What this worker's node last showed.
    pub fn last_seen(&self) -> Seen {
        self.last_seen_if_any()
            .expect("the worker has bootstrapped")
    }

    /// Waits until this worker's node follows `leader`.
    pub async fn wait_to_follow(&mut self, leader: &WorkerId) -> Seen {
        self.wait_until(|seen| seen.leader.as_ref() == Some(leader))
            .await
    }
}

/// Waits until `seen` shows something `until` accepts, on an already-cloned
/// receiver: lets a test race more than one worker's observations (a clone
/// of `RunningWorker::seen` tracks its own "last seen" independently of the
/// original).
pub async fn wait_for_seen(
    seen: &mut watch::Receiver<Option<Seen>>,
    until: impl Fn(&Seen) -> bool,
) -> Seen {
    let result = timeout(
        TEST_TIMEOUT,
        seen.wait_for(|seen| seen.as_ref().is_some_and(&until)),
    )
    .await
    .map(|result| result.expect("the worker's task is still running").clone());
    match result {
        Ok(result) => result.expect("the node has bootstrapped"),
        Err(_) => panic!(
            "the worker's node got there within the timeout; it last showed {:?}",
            *seen.borrow()
        ),
    }
}

/// Starts a worker from `config` and runs it on its own task, observing its
/// node after every batch the driver runs.
pub async fn spawn_worker(config: WorkerConfig) -> RunningWorker {
    spawn_worker_observing(config, |_, _| {}).await
}

/// [`spawn_worker`], also handing `observe` every batch the driver runs
/// (see `run_driver`'s `observe`), for a test that records more than the
/// latest [`Seen`].
pub async fn spawn_worker_observing(
    config: WorkerConfig,
    mut observe: impl FnMut(&WorkerNode<RealClock>, &[Output]) + Send + 'static,
) -> RunningWorker {
    let worker = timeout(TEST_TIMEOUT, Worker::start(config))
        .await
        .expect("the worker started listening within the timeout")
        .expect("the worker can listen on its address");
    let (id, address, net) = (
        worker.id(),
        worker.address().expect("a listening worker has an address"),
        worker.net(),
    );
    let (tx, seen) = watch::channel(None);
    let task = tokio::spawn(async move {
        worker
            .run(move |node, outputs| {
                observe(node, outputs);
                let _ = tx.send(Some(Seen::of(node)));
            })
            .await;
    });
    RunningWorker {
        id,
        address,
        net,
        seen,
        task,
    }
}

/// Drives an already-bootstrapped `node` on `net`, on its own task, exactly
/// as [`spawn_worker`] would once `Worker::run` has bootstrapped one, and
/// wraps it as a [`RunningWorker`] (same observation and cutting behaviour).
/// For a node a caller bootstrapped some other way than [`spawn_worker`] —
/// see [`join_without_heartbeating`] — because it needed to hold the node at
/// a precise state before anything driven sends on its behalf.
pub fn drive_bootstrapped_node(
    mut node: WorkerNode<RealClock>,
    id: WorkerId,
    address: Multiaddr,
    net: Arc<Net>,
    clock: RealClock,
    authority: Option<SharedAuthority>,
) -> RunningWorker {
    let (tx, seen) = watch::channel(None);
    let driven_net = Arc::clone(&net);
    let task = tokio::spawn(async move {
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        run_driver(
            &mut node,
            &driven_net,
            &mut scheduler,
            clock,
            authority,
            move |node, _| {
                let _ = tx.send(Some(Seen::of(node)));
            },
        )
        .await;
    });
    RunningWorker {
        id,
        address,
        net,
        seen,
        task,
    }
}

/// Joins `net`'s worker to the leader reachable at `leader_seed` (a real
/// `/kabudachi/join/1` round trip: `Net::ask_for_leader`, retried until one
/// answers), builds the resulting node with `WorkerNode::bootstrapping` +
/// `WorkerNode::finish_joining`, and registers it with `authority` directly
/// if given — everything `net::bootstrap::bootstrap_node` itself does,
/// **except** sending the heartbeat joining produces.
///
/// `bootstrap_node`'s own doc is explicit that this is not an accident to
/// route around: "[a joined node] does heartbeat the leader at once, and
/// that heartbeat goes out now rather than an interval later" — sent
/// synchronously inside `bootstrap_node`, before it ever returns a node to
/// its caller (confirmed by reading `net/src/bootstrap.rs`'s `Entry::Join`
/// arm: it flushes `finish_joining`'s `Output::Send` through `net.send`
/// itself). No `ElectionTimings` configuration, at any level `Worker`
/// exposes, can observe a real joiner before that heartbeat has gone out:
/// admission is heartbeat-triggered
/// (`WorkerNode::admit_waiting_joiners` runs from `on_heartbeat`), and it
/// can complete within single-digit milliseconds of the join. A test that
/// needs a joiner provably never having heartbeated its leader — the two
/// "before admission" controls in `election_over_net_test.rs` — has no way
/// to hold that real, but fleeting, state stable through the public API, so
/// this helper reconstructs it directly instead: the node ends up in the
/// exact state `bootstrap_node` would have returned it in, minus the one
/// wire send, which is exactly what "cut the leader before it ever hears
/// from this joiner" needs. See `docs/superpowers/plans/notes/e8.md`,
/// E8-R3.
///
/// The returned node also already holds its shard's configuration: waiting
/// for it is this function's own job, not something a caller can add later,
/// since the leader that sends it (the still-alive one, on the connection
/// `Net::ask_for_leader` opened, `on_peer_connected`'s unsolicited ack, not
/// a heartbeat's answer) needs to still be reachable to send it. It is fed
/// into `node` by hand, one queued input at a time, skipping `Input::Tick`
/// entirely so nothing time-based ever fires, and discarding whatever
/// `Output`s that produces instead of acting on them. That is not a no-op:
/// accepting the ack itself flips the node's configuration from `None` to
/// `Some`, which clears its due-for-a-heartbeat timer, so processing it
/// produces the very heartbeat `Output::Send` this function withholds — the
/// discard, not the choice of which input to feed, is what stops it from
/// going out. Anything else queued by then (a bare `PeerConnected`, say) is
/// safe to fold into the node's state the same way, since nothing here is
/// wired to send on `net`'s behalf regardless of what a `Step` asks for.
/// Bounded by [`TEST_TIMEOUT`]: an ack that never arrives fails the test
/// loudly instead of hanging it.
///
/// The caller must not have started driving `net`'s node before calling
/// this (nothing else may have sent on `net`'s behalf), and must cut the
/// leader (or otherwise ensure it cannot answer) before driving the
/// returned node: driving is what finally sends the withheld heartbeat, now
/// pointed at whatever the leader's address still resolves to.
#[allow(clippy::too_many_arguments)]
pub async fn join_without_heartbeating(
    net: &Net,
    my_id: WorkerId,
    shard_id: ShardId,
    clock: RealClock,
    authority: Option<(
        &kabudachi_core::in_memory_authority::InMemoryAuthority<RealClock>,
        AuthorityTimings,
    )>,
    election_timings: ElectionTimings,
    leader_seed: &Multiaddr,
    per_peer_timeout: StdDuration,
) -> WorkerNode<RealClock> {
    let pointer =
        ask_until_pointed_at_a_leader(net, std::slice::from_ref(leader_seed), per_peer_timeout)
            .await;
    let incarnation_id = IncarnationId::new(format!("{}-incarnation-0", my_id.as_str()));
    let authority_timings = authority.map(|(_, timings)| timings);
    let mut node = WorkerNode::bootstrapping(
        my_id.clone(),
        incarnation_id,
        shard_id.clone(),
        clock,
        authority_timings,
        election_timings,
    );
    let joined = node.finish_joining(&pointer);
    for output in joined.outputs {
        debug_assert!(
            !matches!(output, Output::Publish { .. }),
            "joining publishes nothing"
        );
        // Deliberately not acted on: the join heartbeat (`Output::Send`)
        // this function's doc explains withholding, and, with an
        // authority, its registration request (`Output::Authority`) —
        // registration happens below instead, directly.
    }
    // The leader's on-connect ack gives the node its configuration; fed in
    // by hand (see this function's doc), never a `Tick`, so nothing
    // time-based fires and nothing this produces is ever sent.
    timeout(TEST_TIMEOUT, async {
        while node.configuration().is_none() {
            for input in net.take_inputs() {
                let _ = node.step(input);
            }
            if node.configuration().is_some() {
                break;
            }
            net.wait_for_arrival().await;
        }
    })
    .await
    .expect("the leader's on-connect ack arrived within the timeout");
    if let Some((authority, _)) = authority {
        let address = net
            .local_multiaddr()
            .expect("a listening net has an address")
            .to_string();
        authority
            .register(&shard_id, &my_id, &address)
            .expect("the in-memory authority is always reachable");
    }
    node
}

/// An [`kabudachi_core::in_memory_authority::InMemoryAuthority`] that has
/// finished warming up for `shard_id`, so it reports an authoritative count
/// of live registrations at once, with TTL `ttl`.
pub async fn warmed_up_in_memory_authority(
    shard_id: &kabudachi_core::protocol::ids::ShardId,
    ttl: Duration,
) -> kabudachi_core::in_memory_authority::InMemoryAuthority<RealClock> {
    let authority =
        kabudachi_core::in_memory_authority::InMemoryAuthority::new(RealClock::new(), ttl);
    while authority
        .live_registrations(shard_id)
        .expect("the in-memory authority is always reachable")
        .authoritative_count()
        .is_none()
    {
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    authority
}

/// Waits until `until` holds, polling: for state a test observes some other
/// way than a worker's own `Seen` (an authority's bookkeeping, a `Net`'s).
pub async fn poll_until(what: &str, until: impl Fn() -> bool) {
    timeout(TEST_TIMEOUT, async {
        while !until() {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within the timeout"));
}

// --- Folded in from E12's `leader_loss_test.rs` (2026-09-28, per the
// contractor: "once your support/worker.rs is in, fold them into it") ---
//
// A second, richer worker handle: where `RunningWorker` gives a test the
// driver's latest `Seen` snapshot, `TimelineWorker` keeps every batch a
// driver ran (state, leader, outputs) for a test that needs to reason about
// *how* a shard got where it is — which respondents a winning roll call
// counted, when it published, what it sent and to whom, not just where
// things ended up. Kept as its own type rather than folded into
// `RunningWorker`/`Seen` (which several other files already depend on the
// shape of): the two serve different jobs, and giving `Seen` a growing
// `Vec` of every output on every worker `spawn_worker` builds would cost
// every existing caller of it memory it never asked for.

/// One batch a `TimelineWorker`'s driver ran (see `run_driver`'s
/// `observe`), stamped with `StdInstant::now()` when `observe` saw it.
#[derive(Debug, Clone)]
pub struct Batch {
    pub at: StdInstant,
    pub state: WorkerState,
    pub leader: Option<(WorkerId, u64)>,
    /// A voter of a committed (not joint) configuration.
    pub settled_member: bool,
    pub roll_call_respondents: Vec<WorkerId>,
    pub outputs: Vec<Output>,
}

/// One running worker, and every batch its driver has run.
///
/// Dropping a `TimelineWorker` cuts it the same way dropping a
/// `RunningWorker` does (see its own doc, E8-R1): its task aborts, and its
/// `Net` goes with it once every clone does.
pub struct TimelineWorker {
    pub id: WorkerId,
    pub address: Multiaddr,
    pub net: Arc<Net>,
    timeline: Arc<Mutex<Vec<Batch>>>,
    task: JoinHandle<()>,
}

impl TimelineWorker {
    pub fn latest(&self) -> Option<Batch> {
        self.timeline.lock().unwrap().last().cloned()
    }

    pub fn batches_since(&self, since: StdInstant) -> Vec<Batch> {
        self.timeline
            .lock()
            .unwrap()
            .iter()
            .filter(|batch| batch.at >= since)
            .cloned()
            .collect()
    }
}

impl Drop for TimelineWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Cuts `cut_off` off from every one of `others`, on both sides of each
/// pair (see `Net::block_peer`, E13): a network partition, `cut_off` kept
/// running rather than removed — the shape `leader_loss_test.rs` needs, as
/// opposed to `RunningWorker`'s drop-to-cut (E8-R1).
pub fn isolate(cut_off: &TimelineWorker, others: &[TimelineWorker]) {
    for other in others {
        cut_off.net.block_peer(other.id.clone());
        other.net.block_peer(cut_off.id.clone());
    }
}

/// Starts a worker from `config` and runs it on its own task, recording
/// every batch its driver runs (see [`TimelineWorker::latest`],
/// [`TimelineWorker::batches_since`]) rather than only the latest [`Seen`]
/// snapshot [`spawn_worker`] does.
pub async fn spawn_timeline_worker(config: WorkerConfig) -> TimelineWorker {
    let worker = timeout(TEST_TIMEOUT, Worker::start(config))
        .await
        .expect("the worker started listening within the timeout")
        .expect("the worker can listen on its address");
    let (id, address, net) = (
        worker.id(),
        worker.address().expect("a listening worker has an address"),
        worker.net(),
    );
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&timeline);
    let task = tokio::spawn(async move {
        worker
            .run(move |node: &WorkerNode<RealClock>, outputs: &[Output]| {
                let batch = Batch {
                    at: StdInstant::now(),
                    state: node.state(),
                    leader: node.known_leader(),
                    settled_member: !node.is_pending_member()
                        && node
                            .configuration()
                            .is_some_and(|configuration| !configuration.is_joint()),
                    roll_call_respondents: node.roll_call_respondents().cloned().collect(),
                    outputs: outputs.to_vec(),
                };
                recorded.lock().unwrap().push(batch);
            })
            .await;
    });
    TimelineWorker {
        id,
        address,
        net,
        timeline,
        task,
    }
}

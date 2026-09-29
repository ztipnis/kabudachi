//! Worker-entry-point helpers shared by the `net/tests/<area>/` crates that
//! drive a shard through [`kabudachi_net::worker::Worker`] rather than a
//! bare `Net` and node: each spawns a real process-shaped worker (its own
//! identity, its own `Net` listening on real loopback TCP) on its own task,
//! and hands back a handle to observe and, when a test needs it, to cut.
//!
//! First written for `net/tests/bootstrap/bootstrap_join.rs` (E9); reused, not duplicated,
//! by later chunks that also drive workers end to end (E8's leaderless
//! election after a cut leader among them).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::configuration::{Admission, Configuration};
use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{Input, Output, Step, WorkerNode};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd};
use kabudachi_core::time::{Duration, Instant, RealClock};
use kabudachi_testkit::StepRecord;
use kabudachi_net::messenger::Net;
use kabudachi_net::worker::{Worker, WorkerConfig};
use libp2p::Multiaddr;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

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
/// node after every step the driver carries out.
pub async fn spawn_worker(config: WorkerConfig) -> RunningWorker {
    spawn_worker_observing(config, |_, _, _| {}).await
}

/// [`spawn_worker`], also handing `observe` every step the driver carries
/// out (see `run_driver`'s `observe`), for a test that records more than the
/// latest [`Seen`].
pub async fn spawn_worker_observing(
    config: WorkerConfig,
    mut observe: impl FnMut(&WorkerNode<RealClock>, Option<&Input>, &Step) + Send + 'static,
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
            .run(move |node, input, step| {
                observe(node, input, step);
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

// --- Folded in from E12's `net/tests/election/leader_loss.rs` (2026-09-28, per the
// contractor: "once your support/worker.rs is in, fold them into it") ---
//
// A second, richer worker handle: where `RunningWorker` gives a test the
// driver's latest `Seen` snapshot, `TimelineWorker` keeps every step a
// driver carried out, as a `StepRecord`, for a test that needs to reason
// about *how* a shard got where it is — which respondents a winning roll
// call counted, when it published, what it sent and to whom, whether two
// grants overlapped — not just where things ended up. Kept as its own type
// rather than folded into `RunningWorker`/`Seen` (which several other files
// already depend on the shape of): the two serve different jobs, and giving
// `Seen` a growing `Vec` of every output on every worker `spawn_worker`
// builds would cost every existing caller of it memory it never asked for.

/// One step a `TimelineWorker`'s driver carried out (see `run_driver`'s
/// `observe`): its [`StepRecord`] on the timeline (see [`on_timeline`]), so
/// its `at` and its grants' lease ends count microseconds, not a
/// `RealClock`'s milliseconds, plus what a record leaves out.
#[derive(Debug, Clone)]
pub struct TimelineStep {
    pub record: StepRecord,
    /// A voter of a committed (not joint) configuration.
    pub settled_member: bool,
    pub roll_call_respondents: Vec<WorkerId>,
}

/// `at` on the timeline every timeline worker's records share: microseconds
/// since the first instant any timeline worker of this process put on it.
/// Each worker's node reads a `RealClock` of its own, with its own origin,
/// so no node's instants are comparable with another's until moved here.
pub fn on_timeline(at: StdInstant) -> Instant {
    static ORIGIN: OnceLock<StdInstant> = OnceLock::new();
    let origin = *ORIGIN.get_or_init(|| at);
    Instant::at(u64::try_from(at.duration_since(origin).as_micros()).unwrap_or(u64::MAX))
}

/// `record`, of `node`, with the lease end of every grant it reports moved
/// from the node's own clock onto the timeline (see [`on_timeline`]), so
/// `kabudachi_testkit`'s grant checks can compare it with other workers'
/// records: the time the lease had left when the node's clock was read,
/// counted from `seen`, a moment just before that read. The node's clock
/// counts whole milliseconds, so a moved lease end is up to a millisecond
/// late, which can only make the checks stricter. The other instants in its
/// outputs stay on the node's clock.
fn lease_ends_on_timeline(
    mut record: StepRecord,
    node: &WorkerNode<RealClock>,
    seen: StdInstant,
) -> StepRecord {
    let node_now = node.now();
    for output in &mut record.outputs {
        if let Output::Grant(Some(LeadershipGrant {
            valid_until: LeaseEnd::At(end),
            ..
        })) = output
        {
            // A `RealClock` tick is a millisecond.
            let left = StdDuration::from_millis((*end - node_now).as_ticks());
            *end = on_timeline(seen + left);
        }
    }
    record
}

/// One running worker, and every step its driver has carried out.
///
/// Dropping a `TimelineWorker` cuts it the same way dropping a
/// `RunningWorker` does (see its own doc, E8-R1): its task aborts, and its
/// `Net` goes with it once every clone does.
pub struct TimelineWorker {
    pub id: WorkerId,
    pub address: Multiaddr,
    pub net: Arc<Net>,
    timeline: Arc<Mutex<Vec<TimelineStep>>>,
    task: JoinHandle<()>,
}

impl TimelineWorker {
    pub fn latest(&self) -> Option<TimelineStep> {
        self.timeline.lock().unwrap().last().cloned()
    }

    /// Every step the driver has carried out, as records on the timeline
    /// (microseconds; see [`on_timeline`]).
    pub fn records(&self) -> Vec<StepRecord> {
        self.timeline
            .lock()
            .unwrap()
            .iter()
            .map(|step| step.record.clone())
            .collect()
    }

    /// Every step the driver carried out at or after `since`, an instant of
    /// the timeline (see [`on_timeline`]).
    pub fn steps_since(&self, since: Instant) -> Vec<TimelineStep> {
        self.timeline
            .lock()
            .unwrap()
            .iter()
            .filter(|step| step.record.at >= since)
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
/// running rather than removed — the shape `leader_loss.rs` needs, as
/// opposed to `RunningWorker`'s drop-to-cut (E8-R1).
pub fn isolate(cut_off: &TimelineWorker, others: &[TimelineWorker]) {
    for other in others {
        cut_off.net.block_peer(other.id.clone());
        other.net.block_peer(cut_off.id.clone());
    }
}

/// Starts a worker from `config` and runs it on its own task, recording
/// every step its driver carries out (see [`TimelineWorker::latest`],
/// [`TimelineWorker::steps_since`]) rather than only the latest [`Seen`]
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
            .run(move |node: &WorkerNode<RealClock>, input: Option<&Input>, step: &Step| {
                let seen = StdInstant::now();
                let record = StepRecord::of(node, input, step, on_timeline(seen));
                let step = TimelineStep {
                    record: lease_ends_on_timeline(record, node, seen),
                    settled_member: !node.is_pending_member()
                        && node
                            .configuration()
                            .is_some_and(|configuration| !configuration.is_joint()),
                    roll_call_respondents: node.roll_call_respondents().cloned().collect(),
                };
                recorded.lock().unwrap().push(step);
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

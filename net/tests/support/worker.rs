//! Worker-entry-point helpers shared by the `net/tests/<area>/` modules that
//! drive a shard through [`kabudachi_net::worker::Worker`] rather than a
//! bare `Net` and node: each spawns a real process-shaped worker (its own
//! identity, its own `Net` listening on real loopback TCP) on its own task,
//! and hands back a handle to observe and, when a test needs it, to cut.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, RecoveryEpoch, ShardRecord,
};
use kabudachi_core::election::{ElectionTimings, WorkerNode};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::messenger::Net;
use kabudachi_net::worker::{AuthorityConfig, Worker, WorkerConfig};
use libp2p::Multiaddr;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// Generous whole-test backstop for everything a spawned worker is expected
/// to do. Not a tuning knob: individual tests bound their own timing
/// expectations more tightly with their own waits and windows.
pub const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// How long bootstrap waits on each seed or registered peer it asks.
pub const PER_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(1);

/// Short, so a waiting worker goes round its cascade many times per test.
pub const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// The name a shard identified by `shard_id` lives under: the same string.
pub fn name_of(shard_id: &ShardId) -> ShardName {
    shard_id.name()
}

/// The epoch the authority holds under `shard_id`'s name.
pub fn read_epoch(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
) -> Result<Option<RecoveryEpoch>, AuthorityError> {
    Ok(authority
        .read_shard(&name_of(shard_id))?
        .map(|held| held.recovery_epoch))
}

/// Compare-and-swaps `shard_id`'s record from `expected` to `new`.
pub fn swap_epoch(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
    expected: Option<RecoveryEpoch>,
    new: RecoveryEpoch,
) -> Result<(), AuthorityError> {
    let record = |recovery_epoch| ShardRecord {
        shard_id: shard_id.clone(),
        recovery_epoch,
    };
    authority.compare_and_swap_shard(
        &name_of(shard_id),
        expected.map(record).as_ref(),
        &record(new),
    )
}

/// A worker of `shard` bound to `bind`, bootstrapping through `seeds` on
/// `timings`, with the test-scale join timeout and retry interval above
/// and no authority.
pub fn worker_config(
    shard: ShardId,
    bind: &str,
    timings: ElectionTimings,
    seeds: Vec<Multiaddr>,
) -> WorkerConfig {
    WorkerConfig::new(
        name_of(&shard),
        bind.parse().expect("a valid multiaddr"),
        timings,
    )
    .with_seeds(seeds)
    .with_join_peer_timeout(PER_PEER_TIMEOUT)
    .with_retry_interval(RETRY_INTERVAL)
}

/// `config` with `authority` as its coordination authority.
pub fn with_in_memory_authority(
    config: WorkerConfig,
    authority: InMemoryAuthority<RealClock>,
) -> WorkerConfig {
    config.with_authority(AuthorityConfig::new(Arc::new(authority)))
}

/// What a worker's driver last showed of its node.
#[derive(Clone, Debug, PartialEq)]
pub struct Seen {
    pub state: WorkerState,
    pub pending: bool,
    /// Whether the configuration the node holds counts it as a voter. A
    /// promise of admission already clears `pending`, before any
    /// configuration counts the joiner.
    pub voter: bool,
    pub leader: Option<WorkerId>,
}

impl Seen {
    fn of(node: &WorkerNode<RealClock>) -> Self {
        Seen {
            state: node.state(),
            pending: node.is_pending_member(),
            voter: node
                .configuration()
                .is_some_and(|configuration| configuration.is_voter(node.admission())),
            leader: node.known_leader().map(|(leader, _)| leader),
        }
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
/// every connection it held: the process is gone. Deliberately not
/// `Net::block_peer`, which isolates a still-alive worker rather than
/// removing the process.
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

    /// Waits until this worker's node is `Active` under a leader, whichever
    /// worker leads by then. Not a particular one: on a loaded host a leader
    /// can lose its office to a later election while a test waits, and a
    /// wait for that worker would then wait for good.
    pub async fn wait_to_follow_a_leader(&mut self) -> Seen {
        self.wait_until(|seen| seen.state == WorkerState::Active && seen.leader.is_some())
            .await
    }
}

/// Waits until `seen` shows something `until` accepts, or panics showing what
/// it last held.
async fn wait_for_seen(
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
            .run(move |node, _input, _step| {
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
        .live_registrations(&name_of(shard_id), shard_id)
        .expect("the in-memory authority is always reachable")
        .authoritative_count()
        .is_none()
    {
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    authority
}

/// Waits until `until` holds, polling every 5 ms: for state a test observes
/// some other way than a worker's own `Seen` (an authority's bookkeeping, a
/// `Net`'s).
pub async fn poll_until(what: &str, mut until: impl FnMut() -> bool) {
    timeout(TEST_TIMEOUT, async {
        while !until() {
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within the timeout"));
}

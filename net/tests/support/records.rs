//! A shard of three driven voters over real loopback sockets, for tests of
//! what a leader does with its records.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{
    ElectionTimings, Entry, Identity, Input, KnownConfiguration, Step, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::RecordOutbox;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::messenger::Net;
use tokio::sync::watch;
use tokio::time::timeout;

use super::election::{built_on_one_tick, drive_three_until, wait_until};
use super::net::{connect_full_mesh, driven_scheduler};

const SHARD: &str = "shard-1";
/// Long enough that a leader whose threads a loaded host starves for a
/// moment keeps its lease (it lasts a suspicion timeout less its drift
/// share), and short enough that a lease that does end ends well before
/// [`kabudachi_net::task_store::RECORD_WRITE_TIMEOUT`].
const SUSPECT_TIMEOUT_MS: u64 = 2000;
const HEARTBEAT_INTERVAL_MS: u64 = 10;
const ROLL_CALL_DEADLINE_MS: u64 = 100;
/// A "something is actually broken" backstop for every wait of the fixture.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

type Scheduled = Scheduler<RealClock, Uuid7Ids, RecordOutbox>;

/// Three voters of one known configuration, fully meshed, each with its own
/// `Net::for_shard`, node and scheduler. Nothing runs until a `drive_*` call;
/// each call drives all three drivers and leaves the nodes where they stood.
pub struct ThreeVoters {
    pub nets: [Net; 3],
    pub nodes: [WorkerNode<RealClock>; 3],
    pub schedulers: [Scheduled; 3],
    pub clock: RealClock,
    ids: [WorkerId; 3],
    hosts: [Hosting; 3],
    states: [watch::Receiver<WorkerState>; 3],
    senders: [watch::Sender<WorkerState>; 3],
}

impl ThreeVoters {
    /// The voters, and a fourth connected `Net` for the shard that has no
    /// node: the client that asks the leader for claims.
    pub async fn start() -> (ThreeVoters, Net) {
        let shard = ShardId::new(SHARD);
        let hosted = [0, 1, 2].map(|_| host(shard.clone()));
        let [(net_a, host_a), (net_b, host_b), (net_c, host_c)] = hosted;
        let nets = [net_a, net_b, net_c];
        let hosts = [host_a, host_b, host_c];
        let claimant = Net::for_shard(shard, None);
        let ids = connect_full_mesh(&[&nets[0], &nets[1], &nets[2], &claimant]).await;
        let ids = [ids[0].clone(), ids[1].clone(), ids[2].clone()];
        let clock = RealClock::new();
        let nodes = built_on_one_tick(&clock, || ids.clone().map(|id| voter(clock, id)));
        let schedulers = [0, 1, 2].map(|_| driven_scheduler(clock));
        let channels = [0, 1, 2].map(|i| watch::channel(nodes[i].state()));
        let senders = channels.each_ref().map(|(sender, _)| sender.clone());
        let states = channels.map(|(_, receiver)| receiver);
        (
            ThreeVoters {
                nets,
                nodes,
                schedulers,
                clock,
                ids,
                hosts,
                states,
                senders,
            },
            claimant,
        )
    }

    pub fn id(&self, voter: usize) -> WorkerId {
        self.ids[voter].clone()
    }

    pub fn others(&self, voter: usize) -> Vec<usize> {
        (0..3).filter(|other| *other != voter).collect()
    }

    /// Freezes `voter`'s network: its connections stay open, but it answers
    /// nothing, stores nothing and sends nothing, as a host that has
    /// stopped without its sockets closing would. Returns once it is frozen.
    pub fn freeze(&self, voter: usize) {
        let host = &self.hosts[voter];
        host.frozen.store(true, Ordering::SeqCst);
        let frozen_by = std::time::Instant::now() + TEST_TIMEOUT;
        while !host.parked.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < frozen_by,
                "the hosting thread froze within the timeout"
            );
            std::thread::sleep(StdDuration::from_millis(1));
        }
    }

    /// Drives the three until one leads with a lease its scheduler holds
    /// (so it accepts submissions), and returns its index; panics if that
    /// takes past the backstop.
    pub async fn drive_until_a_leader(&mut self) -> usize {
        timeout(TEST_TIMEOUT, self.drive_to_a_leader())
            .await
            .expect("one of the three led within the timeout")
    }

    async fn drive_to_a_leader(&mut self) -> usize {
        let states = self.states.clone();
        let leader = self
            .drive_until(async move {
                wait_until(|| states.iter().any(|state| *state.borrow() == WorkerState::Leader))
                    .await;
                states
                    .iter()
                    .position(|state| *state.borrow() == WorkerState::Leader)
                    .expect("one of the three leads")
            })
            .await;
        // The lease starts once the followers' acknowledgements arrive.
        while !self.schedulers[leader].is_leader() {
            self.drive_until(tokio::time::sleep(StdDuration::from_millis(10)))
                .await;
        }
        leader
    }

    /// Drives the three until `until` completes, and returns what it
    /// returned; panics if that takes past the backstop.
    pub async fn drive_until<T>(&mut self, until: impl Future<Output = T>) -> T {
        let [net_a, net_b, net_c] = &self.nets;
        let [tx_a, tx_b, tx_c] = self.senders.clone();
        timeout(
            TEST_TIMEOUT,
            drive_three_until(
                &mut self.nodes,
                [net_a, net_b, net_c],
                &mut self.schedulers,
                self.clock,
                [None, None, None],
                [publish(tx_a), publish(tx_b), publish(tx_c)],
                until,
            ),
        )
        .await
        .expect("the awaited event happened within the timeout")
    }
}

/// Runs a `Net`'s swarm on a runtime of its own, so a test can freeze it.
fn host(shard: ShardId) -> (Net, Hosting) {
    let hosting = Hosting::default();
    let (frozen, parked, stopped) = (
        hosting.frozen.clone(),
        hosting.parked.clone(),
        hosting.stopped.clone(),
    );
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime to host a net");
        runtime.block_on(async {
            sender
                .send(Net::for_shard(shard, None))
                .expect("the fixture is waiting for the net");
            // The swarm's task runs whenever this yields. Blocking the
            // thread instead stops it with its sockets still open.
            while !stopped.load(Ordering::SeqCst) {
                if frozen.load(Ordering::SeqCst) {
                    parked.store(true, Ordering::SeqCst);
                    std::thread::sleep(StdDuration::from_millis(5));
                } else {
                    tokio::time::sleep(StdDuration::from_millis(5)).await;
                }
            }
        });
    });
    let net = receiver.recv().expect("the hosting thread built the net");
    (net, hosting)
}

/// The runtime a `Net` is hosted on: freezes it on request, and ends it when
/// dropped.
#[derive(Default)]
struct Hosting {
    frozen: Arc<AtomicBool>,
    /// Set by the hosting thread once it has stopped running the swarm.
    parked: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

impl Drop for Hosting {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

fn publish(
    state: watch::Sender<WorkerState>,
) -> impl FnMut(&WorkerNode<RealClock>, Option<&Input>, &Step) {
    move |node, _, _| {
        let _ = state.send(node.state());
    }
}

fn voter(clock: RealClock, id: WorkerId) -> WorkerNode<RealClock> {
    WorkerNode::start(
        Identity {
            incarnation: IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
            id,
            shard: ShardId::new(SHARD),
            timings: ElectionTimings::new(
                Duration::from_millis(SUSPECT_TIMEOUT_MS),
                Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            )
            .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: 3,
            })
            .expect("valid"),
            admission: Some(Generation::genesis(0)),
        }),
        clock,
        None,
    )
    .0
}

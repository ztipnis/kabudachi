//! A shard of driven voters over real loopback sockets, for tests of what a
//! leader does with its records.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{
    ElectionTimings, Entry, Identity, Input, KnownConfiguration, Step, WorkerNode,
};
use kabudachi_core::protocol::generated::ElectionCertificate;
use kabudachi_core::protocol::ids::{
    IncarnationId, ShardId, TaskDefinitionId, TaskId, TaskRunId, Uuid7Ids, WorkerId,
};
use kabudachi_core::protocol::messages::{
    ElectionMessage, WorkerHeartbeat, claim_response, election_message, task_response,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{Scheduler, Submission, Submitted, mint};
use kabudachi_core::task_record::RecordOutbox;
use kabudachi_core::time::{Duration, RealClock, WallTime};
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::handoff::HandedOff;
use kabudachi_net::messenger::Net;
use kabudachi_net::task_store::placement::ReplicationFactor;
use libp2p::futures::future::select_all;
use tokio::sync::watch;
use tokio::time::timeout;

use super::election::{built_on_one_tick, due_now, wait_until};
use super::net::{connect_full_mesh, driven_scheduler};

const SHARD: &str = "shard-1";
/// Long enough that a leader whose threads a loaded host starves for a
/// moment keeps its lease (it lasts a suspicion timeout less its drift
/// share), and short enough that a lease that does end ends well before
/// [`kabudachi_net::task_store::RECORD_WRITE_TIMEOUT`].
pub const SUSPECT_TIMEOUT_MS: u64 = 2000;
/// A twentieth of the suspicion timeout: a leader's newest confirmation is
/// at most two intervals and a round trip old, well inside its lease. Not
/// shorter: every heartbeat and every ack is a stream of its own, and at
/// 10 ms a five-voter leader carries hundreds a second, which an
/// unoptimised build on a host running several test binaries at once
/// cannot keep up with. Its acks and the confirmations of them then fall
/// seconds behind, and the leader loses its lease with every follower alive.
const HEARTBEAT_INTERVAL_MS: u64 = 100;
/// Short, so that a worker reported lost a suspicion timeout and a reconnect
/// timeout after it was last heard fits a test.
pub const RECONNECT_TIMEOUT_MS: u64 = 1000;
const ROLL_CALL_DEADLINE_MS: u64 = 100;
/// A "something is actually broken" backstop for every wait of the fixture.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

type Scheduled = Scheduler<RealClock, Uuid7Ids, RecordOutbox>;

/// Voters of one known configuration, fully meshed, each with its own
/// `Net::for_shard`, node and scheduler. Nothing runs until a `drive_*` call;
/// each call drives every voter not killed and leaves the nodes where they
/// stood.
pub struct Voters {
    /// Shared, so a test can ask through a voter's net while driving the shard.
    pub nets: Vec<Arc<Net>>,
    pub nodes: Vec<WorkerNode<RealClock>>,
    pub schedulers: Vec<Scheduled>,
    pub clock: RealClock,
    ids: Vec<WorkerId>,
    replication_factor: ReplicationFactor,
    /// Keeps each net's hosting thread running for as long as the voters live.
    _hosts: Vec<Hosting>,
    states: Vec<watch::Receiver<WorkerState>>,
    senders: Vec<watch::Sender<WorkerState>>,
    killed: Vec<bool>,
    /// What each voter's driver returned once it had drained and handed its
    /// records over; such a voter is driven no more.
    handed_off: HandedOffBy,
}

/// What the drivers of drained voters returned, readable while the shard is
/// driven.
#[derive(Clone)]
pub struct HandedOffBy(Arc<watch::Sender<Vec<Option<HandedOff>>>>);

impl HandedOffBy {
    fn new(voters: usize) -> Self {
        HandedOffBy(Arc::new(watch::channel(vec![None; voters]).0))
    }

    fn of(&self, voter: usize) -> Option<HandedOff> {
        self.0.borrow()[voter].clone()
    }

    fn record(&self, voter: usize, handed_off: HandedOff) {
        self.0.send_modify(|returned| returned[voter] = Some(handed_off));
    }

    /// Resolves once `voter`'s driver has returned (it does once the voter
    /// drained and handed its records over), with what it returned. Callers
    /// bound the wait.
    pub async fn returned(&self, voter: usize) -> HandedOff {
        let mut returned = self.0.subscribe();
        let returned = returned
            .wait_for(|returned| returned[voter].is_some())
            .await
            .expect("the sender lives as long as the shard");
        returned[voter].clone().expect("waited for it")
    }
}

/// A shard of three voters.
pub struct ThreeVoters;

impl ThreeVoters {
    pub async fn start() -> (Voters, Net) {
        Voters::start(3).await
    }
}

/// What a test can read of the voters' states while the shard is driven.
pub struct Watch {
    states: Vec<watch::Receiver<WorkerState>>,
    killed: Vec<bool>,
}

impl Watch {
    pub fn state(&self, voter: usize) -> WorkerState {
        *self.states[voter].borrow()
    }

    /// The voter that leads, among those not killed when this was taken.
    pub fn leader(&self) -> Option<usize> {
        (0..self.states.len())
            .find(|voter| !self.killed[*voter] && self.state(*voter) == WorkerState::Leader)
    }
}

impl Voters {
    /// `count` voters, and a further connected `Net` for the shard that has no
    /// node: a client the leader's roster does not hold until
    /// [`Self::join_as_pending`] makes it a pending member.
    pub async fn start(count: usize) -> (Voters, Net) {
        Self::start_with(count, ReplicationFactor::DEFAULT).await
    }

    /// Like [`Self::start`], with each record written to `replication_factor`
    /// voters, so that with fewer than `count` of them, voters differ in what
    /// they hold.
    pub async fn start_with(count: usize, replication_factor: ReplicationFactor) -> (Voters, Net) {
        let claimant = Net::for_shard(ShardId::new(SHARD), None);
        let shard = Self::meshed(count, replication_factor, Some(&claimant)).await;
        (shard, claimant)
    }

    /// Like [`Self::start_with`], with no client: the client speaks the
    /// shard's records protocol, so it would sit in each voter's records
    /// routing table, and with it a voter's steal targets would not be the
    /// other voters alone.
    pub async fn start_without_client(count: usize, replication_factor: ReplicationFactor) -> Voters {
        Self::meshed(count, replication_factor, None).await
    }

    /// `count` hosted voters, fully meshed with each other and with `client`.
    async fn meshed(count: usize, replication_factor: ReplicationFactor, client: Option<&Net>) -> Voters {
        let shard = ShardId::new(SHARD);
        let hosted: Vec<_> = (0..count).map(|_| host(shard.clone())).collect();
        let (nets, hosts): (Vec<_>, Vec<_>) = hosted.into_iter().unzip();
        let nets: Vec<Arc<Net>> = nets.into_iter().map(Arc::new).collect();
        let mut everyone: Vec<&Net> = nets.iter().map(|net| &**net).collect();
        everyone.extend(client);
        let mut ids = connect_full_mesh(&everyone).await;
        ids.truncate(nets.len());
        let clock = RealClock::new();
        let nodes = built_on_one_tick(&clock, || {
            ids.iter().map(|id| voter(clock, id.clone(), count)).collect::<Vec<_>>()
        });
        let schedulers = nodes.iter().map(|_| driven_scheduler(clock)).collect();
        let channels: Vec<_> = nodes.iter().map(|node| watch::channel(node.state())).collect();
        let senders = channels.iter().map(|(sender, _)| sender.clone()).collect();
        let states = channels.into_iter().map(|(_, receiver)| receiver).collect();
        let killed = vec![false; nodes.len()];
        let handed_off = HandedOffBy::new(nodes.len());
        Voters {
            nets,
            nodes,
            schedulers,
            clock,
            ids,
            replication_factor,
            _hosts: hosts,
            states,
            senders,
            killed,
            handed_off,
        }
    }

    /// A reading of every voter's state that stays current while the shard is
    /// driven, for a condition to wait on.
    pub fn watch(&self) -> Watch {
        Watch {
            states: self.states.clone(),
            killed: self.killed.clone(),
        }
    }

    /// Stops driving `voter` and cuts it off from every other voter, as a
    /// host that died would be: its connections close and none reopens.
    pub fn kill(&mut self, voter: usize) {
        self.killed[voter] = true;
        for other in (0..self.nets.len()).filter(|other| *other != voter) {
            self.nets[other].block_peer(self.ids[voter].clone());
            self.nets[voter].block_peer(self.ids[other].clone());
        }
    }

    pub fn id(&self, voter: usize) -> WorkerId {
        self.ids[voter].clone()
    }

    pub fn others(&self, voter: usize) -> Vec<usize> {
        (0..self.nets.len()).filter(|other| *other != voter).collect()
    }

    /// Makes `client` a pending member of the shard, as a worker that has
    /// just started heartbeating to `leader` is: one heartbeat that confirms
    /// no ack, so the leader records it and admits it no further. Drives the
    /// three until the leader's roster holds it.
    pub async fn join_as_pending(&mut self, client: &Net, leader: usize) {
        let joiner = client.local_worker_id();
        let leader_id = self.id(leader);
        timeout(TEST_TIMEOUT, async {
            client.send(leader_id.clone(), heartbeat_from(&joiner));
            while !self.nodes[leader].is_voter_or_pending(&joiner) {
                self.drive_until(tokio::time::sleep(StdDuration::from_millis(10)))
                    .await;
                // A heartbeat sent before the leader's lease or the
                // connection was ready is dropped, so send another.
                if !self.nodes[leader].is_voter_or_pending(&joiner) {
                    client.send(leader_id.clone(), heartbeat_from(&joiner));
                }
            }
        })
        .await
        .expect("the leader's roster held the joiner within the timeout");
    }

    /// Drives the voters until one leads with a lease its scheduler holds
    /// (so it accepts submissions), and returns its index; panics if that
    /// takes past the backstop.
    pub async fn drive_until_a_leader(&mut self) -> usize {
        timeout(TEST_TIMEOUT, self.drive_to_a_leader())
            .await
            .expect("one of the voters led within the timeout")
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
                    .expect("one of the voters leads")
            })
            .await;
        // The lease starts once the followers' acknowledgements arrive.
        while !self.schedulers[leader].is_leader() {
            self.drive_until(tokio::time::sleep(StdDuration::from_millis(10)))
                .await;
        }
        leader
    }

    /// Drives every voter not killed, and not one that has handed its
    /// records over, until `until` completes, and returns what it returned;
    /// panics if that takes past the backstop. A voter whose driver returns
    /// meanwhile is driven no more (see [`Self::handed_off`]).
    pub async fn drive_until<T>(&mut self, until: impl Future<Output = T>) -> T {
        let clock = self.clock;
        let (nets, senders, killed) = (&self.nets, &self.senders, &self.killed);
        let handed_off = self.handed_off.clone();
        let config = DriverConfig {
            replication_factor: self.replication_factor,
            ..DriverConfig::default()
        };
        let drivers: Vec<Pin<Box<dyn Future<Output = (usize, HandedOff)> + '_>>> = self
            .nodes
            .iter_mut()
            .zip(self.schedulers.iter_mut())
            .enumerate()
            .filter(|(voter, _)| !killed[*voter] && handed_off.of(*voter).is_none())
            .map(|(voter, (node, scheduler))| {
                let observe = publish(senders[voter].clone());
                let config = config.clone();
                Box::pin(async move {
                    let returned = run_driver(
                        node,
                        due_now(&clock),
                        &*nets[voter],
                        scheduler,
                        clock,
                        None,
                        config,
                        observe,
                    )
                    .await;
                    (voter, returned)
                }) as Pin<Box<dyn Future<Output = (usize, HandedOff)> + '_>>
            })
            .collect();
        let output = timeout(TEST_TIMEOUT, async {
            let mut running = drivers;
            let mut until = std::pin::pin!(until);
            loop {
                if running.is_empty() {
                    return until.await;
                }
                tokio::select! {
                    ((voter, done), _, rest) = select_all(running) => {
                        handed_off.record(voter, done);
                        running = rest;
                    }
                    output = &mut until => return output,
                }
            }
        })
        .await
        .expect("the awaited event happened within the timeout");
        output
    }

    /// What the drivers of drained voters returned, to wait on inside
    /// [`Self::drive_until`]: the shard's drivers keep what they know of their
    /// leader's writes only for as long as one call drives them.
    pub fn handed_off(&self) -> HandedOffBy {
        self.handed_off.clone()
    }
}

/// A first heartbeat from `worker`, which confirms no ack.
fn heartbeat_from(worker: &WorkerId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
            worker_id: Some(worker.clone().into()),
            incarnation_id: Some(
                IncarnationId::new(format!("{}-incarnation-0", worker.as_str())).into(),
            ),
            recovery_epoch_seen: 0,
            term_seen: 0,
            available_capacity: 0,
            active_task_runs_digest: Vec::new(),
            shard_id: Some(ShardId::new(SHARD).into()),
            newest_accepted_ack: None,
            configuration_generation: None,
            send_token: 0,
            routing_crawled: false,
            admission_generation: None,
        })),
    }
}

/// Runs a `Net`'s swarm on a runtime of its own.
fn host(shard: ShardId) -> (Net, Hosting) {
    let hosting = Hosting::default();
    let stopped = hosting.stopped.clone();
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
            // The swarm's task runs whenever this yields.
            while !stopped.load(Ordering::SeqCst) {
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        });
    });
    let net = receiver.recv().expect("the hosting thread built the net");
    (net, hosting)
}

/// The runtime a `Net` is hosted on, ended when dropped.
#[derive(Default)]
struct Hosting {
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

fn voter(clock: RealClock, id: WorkerId, count: usize) -> WorkerNode<RealClock> {
    WorkerNode::start(
        Identity {
            incarnation: IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
            id,
            shard: ShardId::new(SHARD),
            timings: ElectionTimings::new(
                Duration::from_millis(SUSPECT_TIMEOUT_MS),
                Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            )
            .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS))
            .with_reconnect_timeout(Duration::from_millis(RECONNECT_TIMEOUT_MS)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: count,
            })
            .expect("valid"),
            admission: Some(Generation::genesis(0)),
        }),
        clock,
        None,
    )
    .0
}

/// A plain submission whose input is `input`.
pub fn plain_with(input: &[u8]) -> Submission {
    Submission::new(
        TaskDefinitionId::new("demo.task"),
        1,
        input.to_vec(),
        "default",
    )
}

/// Resolves once `net` holds a record of every task in `tasks`.
pub async fn wait_until_held(net: Arc<Net>, tasks: Vec<TaskId>) {
    wait_until(|| {
        let held = net.held_records();
        tasks.iter().all(|task| held.get(task).is_some())
    })
    .await;
}

/// Has `worker` submit `submission` to `leader` as a client would, asking
/// again until the leader's lease is ready, and returns the task's id.
pub async fn submitted_through(worker: &Net, leader: &WorkerId, submission: Submission) -> TaskId {
    submitted_as(worker, leader, mint(submission, &Uuid7Ids, &RealClock::new())).await
}

/// Has `worker` submit `submission` to `leader` under the given task id and
/// submission time, so a test can set the order of ids apart from the order of
/// submission, and returns the task's id.
pub async fn submitted_with(
    worker: &Net,
    leader: &WorkerId,
    (task, at): (&str, WallTime),
    submission: Submission,
) -> TaskId {
    let submitted = Submitted::received(TaskId::new(task), at, submission, &RealClock::new());
    submitted_as(worker, leader, submitted).await
}

/// Like [`submitted_through`], for a submission already minted.
async fn submitted_as(worker: &Net, leader: &WorkerId, submitted: Submitted) -> TaskId {
    loop {
        let answer = worker.submit(leader.clone(), submitted.clone()).await;
        if matches!(answer.map(|answer| answer.result), Ok(Some(task_response::Result::Submitted(_)))) {
            return submitted.task_id;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
}

/// Has `worker` claim `task` from `leader`, and returns the run.
pub async fn claimed(worker: &Net, leader: &WorkerId, task: &TaskId) -> TaskRunId {
    let claimed = worker
        .request_claim(leader.clone(), task.clone())
        .await
        .expect("the leader answered");
    let Some(claim_response::Result::Accept(claim)) = claimed.result else {
        panic!("expected an accepted claim, got {claimed:?}");
    };
    claim.task_run_id.expect("a claim names its run").into()
}

/// Has `worker` claim `task` from `leader` and report the run started, and
/// returns the run.
pub async fn claimed_and_started(worker: &Net, leader: &WorkerId, task: &TaskId) -> TaskRunId {
    let run = claimed(worker, leader, task).await;
    let started = worker
        .report_started(leader.clone(), run.clone())
        .await
        .expect("the leader answered");
    assert!(
        matches!(started.result, Some(task_response::Result::Started(_))),
        "expected the start acknowledged, got {started:?}"
    );
    run
}

/// A certificate that `leader` leads `term` of the voters' shard, over the
/// configuration the voters start in.
pub fn proof_of_office(leader: &WorkerId, term: u64) -> ElectionCertificate {
    let genesis = Generation::genesis(0);
    let configuration = Configuration::single(Single {
        generation: genesis,
        base: genesis,
        voter_count: 3,
    })
    .expect("valid");
    ElectionCertificate {
        shard_id: Some(ShardId::new(SHARD).into()),
        recovery_epoch: 0,
        term,
        leader_id: Some(leader.clone().into()),
        configuration: Some((&configuration).into()),
        recipient_admission: None,
        recipient_prior_admission: None,
    }
}

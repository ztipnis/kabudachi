//! A shard of voters driven in the background over real loopback sockets, for
//! tests of what a leader does with its records.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::{
    ElectionTimings, Entry, Identity, Input, KnownConfiguration, Step, WorkerNode,
};
use kabudachi_core::blob::BlobOwner;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::generated::{CompositeRecord, ElectionCertificate, TaskRunState};
use kabudachi_core::protocol::ids::{
    CompositeId, IdGenerator, IncarnationId, ShardId, TaskDefinitionId, TaskId, TaskRunId, Uuid7Ids, WorkerId,
};
use kabudachi_core::protocol::messages::{
    CompositeAccepted, ElectionMessage, TaskRejectReason, WorkerHeartbeat, claim_response, election_message, task_response,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::queues::{QueuePolicy, Subscription};
use kabudachi_core::scheduler::composite::{
    BoundInput, BoundStep, CompositeFinished, CompositeKind, ItemInput, MemberBatch, OnError, Stage, Step as CompositeStep,
    StreamedMember, SubmittedComposite, TaskOptions,
};
use kabudachi_core::scheduler::{Claim, Scheduler, Submission, Submitted, mint};
use kabudachi_core::task_record::{RecordKey, RecordOutbox, RecordVersion};
use kabudachi_core::time::{Duration, RealClock, WallTime};
use kabudachi_core::versions::RunnableVersions;
use kabudachi_net::discovery::Takes;
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::executor::ExecutorEndpoint;
use kabudachi_net::handoff::HandedOff;
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::messenger::Net;
use kabudachi_net::result_transport::ResultEvent;
use kabudachi_net::task_exchange::{LeaderRetry, placement_with_asker_of};
use kabudachi_net::task_store::placement::ReplicationFactor;
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

pub type Scheduled = Scheduler<RealClock, Uuid7Ids, RecordOutbox>;

/// A voter's node and scheduler, and how its driver runs.
struct Driven {
    node: WorkerNode<RealClock>,
    scheduler: Scheduled,
    executor: Option<ExecutorEndpoint>,
}

/// Something a test does to a voter's node and scheduler between two runs of
/// its driver.
type Command = Box<dyn FnOnce(&mut WorkerNode<RealClock>, &mut Scheduled) + Send>;

/// A voter: idle until the shard is first driven, then driven on a task of its
/// own until it is killed or its driver returns.
enum Voter {
    Idle(Box<Driven>),
    Running {
        commands: tokio::sync::mpsc::UnboundedSender<Command>,
        task: tokio::task::JoinHandle<()>,
    },
    /// Killed, or its driver returned after the voter drained.
    Gone,
}

/// Voters of one known configuration, fully meshed, each with its own
/// `Net::for_shard`, node and scheduler. Nothing runs until the first
/// `drive_*` call; from then on every voter not killed is driven on a task of
/// its own until its driver returns. A test reaches a voter's node or
/// scheduler through [`Self::with`].
pub struct Voters {
    /// Shared, so a test can ask through a voter's net while the shard runs.
    pub nets: Vec<Arc<Net>>,
    pub clock: RealClock,
    voters: Vec<Voter>,
    ids: Vec<WorkerId>,
    replication_factor: ReplicationFactor,
    /// The queues each voter takes work from.
    queues: Vec<Subscription>,
    /// The queue policy every voter is given.
    queue_policy: QueuePolicy,
    /// Keeps each net's hosting thread running for as long as the voters live.
    _hosts: Vec<Hosting>,
    states: Vec<watch::Receiver<WorkerState>>,
    senders: Vec<watch::Sender<WorkerState>>,
    crawled: Vec<watch::Receiver<bool>>,
    crawled_senders: Vec<watch::Sender<bool>>,
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

    fn record(&self, voter: usize, handed_off: HandedOff) {
        self.0.send_modify(|returned| returned[voter] = Some(handed_off));
    }

    fn has_returned(&self, voter: usize) -> bool {
        self.0.borrow()[voter].is_some()
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
    crawled: Vec<watch::Receiver<bool>>,
    killed: Vec<bool>,
}

impl Watch {
    pub fn state(&self, voter: usize) -> WorkerState {
        *self.states[voter].borrow()
    }

    /// Whether `voter`'s node has crawled its routing, as of its last step.
    /// Read this rather than asking the node through [`Voters::with`], which
    /// restarts the voter's driver and with it the settling of its routing.
    pub fn routing_crawled(&self, voter: usize) -> bool {
        *self.crawled[voter].borrow()
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
        let claimant = Net::for_shard(ShardId::new(SHARD).name(), None);
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
        let channels: Vec<_> = nodes.iter().map(|node| watch::channel(node.state())).collect();
        let senders = channels.iter().map(|(sender, _)| sender.clone()).collect();
        let states = channels.into_iter().map(|(_, receiver)| receiver).collect();
        let (crawled_senders, crawled): (Vec<_>, Vec<_>) = nodes
            .iter()
            .map(|node| watch::channel(node.routing_crawled()))
            .unzip();
        let killed = vec![false; count];
        let handed_off = HandedOffBy::new(count);
        let voters = nodes
            .into_iter()
            .map(|node| {
                Voter::Idle(Box::new(Driven {
                    node,
                    scheduler: driven_scheduler(clock),
                    executor: None,
                }))
            })
            .collect();
        Voters {
            nets,
            clock,
            voters,
            ids,
            replication_factor,
            queues: vec![Subscription::Every; count],
            queue_policy: QueuePolicy::new(),
            _hosts: hosts,
            states,
            senders,
            crawled,
            crawled_senders,
            killed,
            handed_off,
        }
    }

    /// Gives `voter`'s driver `executor` to run the tasks it claims; its
    /// worker then also says it runs compaction. Only before the shard is
    /// first driven.
    pub fn set_executor(&mut self, voter: usize, executor: ExecutorEndpoint) {
        let Voter::Idle(driven) = &mut self.voters[voter] else {
            panic!("set before the shard is first driven");
        };
        driven.executor = Some(executor);
    }

    /// Makes `voter` take work only from `queues`. Only before the shard is
    /// first driven.
    pub fn set_queues(&mut self, voter: usize, queues: &[&str]) {
        assert!(
            matches!(self.voters[voter], Voter::Idle(_)),
            "set before the shard is first driven"
        );
        self.queues[voter] =
            Subscription::Only(queues.iter().map(|name| (*name).to_owned()).collect());
    }

    /// Gives every voter `policy`. Only before the shard is first driven.
    pub fn set_queue_policy(&mut self, policy: QueuePolicy) {
        self.queue_policy = policy;
    }

    /// Runs `act` on `voter`'s node and scheduler and returns what it
    /// returned. A running voter's driver stops for it and starts again after,
    /// as a driver does at every new run; an idle voter's runs it in place.
    /// Panics for a voter killed or done, or past the backstop.
    pub async fn with<R: Send + 'static>(
        &mut self,
        voter: usize,
        act: impl FnOnce(&mut WorkerNode<RealClock>, &mut Scheduled) -> R + Send + 'static,
    ) -> R {
        self.check_drivers().await;
        let gone = || panic!("voter {voter} is no longer driven");
        match &mut self.voters[voter] {
            Voter::Idle(driven) => act(&mut driven.node, &mut driven.scheduler),
            Voter::Running { commands, .. } => {
                let (reply, answer) = tokio::sync::oneshot::channel();
                // A panic in `act` goes back to the caller, so the test fails
                // with its own message and the driver keeps running.
                let command: Command = Box::new(move |node, scheduler| {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        act(node, scheduler)
                    }));
                    let _ = reply.send(outcome);
                });
                if commands.send(command).is_err() {
                    gone();
                }
                match timeout(TEST_TIMEOUT, answer).await {
                    Ok(Ok(Ok(answer))) => answer,
                    Ok(Ok(Err(panic))) => std::panic::resume_unwind(panic),
                    Ok(Err(_)) => gone(),
                    Err(_) => panic!("voter {voter} ran the command within the timeout"),
                }
            }
            Voter::Gone => gone(),
        }
    }

    /// Fails the test if a voter's driver ended without handing off: a driver
    /// runs on a task nobody awaits, so its panic would otherwise go unseen.
    async fn check_drivers(&mut self) {
        for voter in 0..self.voters.len() {
            let Voter::Running { task, .. } = &mut self.voters[voter] else {
                continue;
            };
            if !task.is_finished() || self.handed_off.has_returned(voter) {
                continue;
            }
            // The task has finished, so this resolves at once.
            match timeout(StdDuration::ZERO, task).await {
                Ok(Err(error)) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                _ => panic!("voter {voter}'s driver stopped without handing off"),
            }
        }
    }

    /// Starts driving every idle voter not killed.
    fn start_idle(&mut self) {
        for voter in 0..self.voters.len() {
            if self.killed[voter] || !matches!(self.voters[voter], Voter::Idle(_)) {
                continue;
            }
            let Voter::Idle(driven) = std::mem::replace(&mut self.voters[voter], Voter::Gone) else {
                unreachable!("checked idle above");
            };
            let config = DriverConfig {
                replication_factor: self.replication_factor,
                queues: self.queues[voter].clone(),
                queue_policy: self.queue_policy.clone(),
                ..DriverConfig::default()
            };
            let (commands, receiver) = tokio::sync::mpsc::unbounded_channel();
            let task = tokio::spawn(drive_in_background(
                voter,
                *driven,
                self.nets[voter].clone(),
                self.clock,
                config,
                self.senders[voter].clone(),
                self.crawled_senders[voter].clone(),
                receiver,
                self.handed_off.clone(),
            ));
            self.voters[voter] = Voter::Running { commands, task };
        }
    }

    /// A reading of every voter's state that stays current while the shard is
    /// driven, for a condition to wait on.
    pub fn watch(&self) -> Watch {
        Watch {
            states: self.states.clone(),
            crawled: self.crawled.clone(),
            killed: self.killed.clone(),
        }
    }

    /// Stops driving `voter` and cuts it off from every other voter, as a
    /// host that died would be: its connections close and none reopens.
    pub fn kill(&mut self, voter: usize) {
        if let Voter::Running { task, .. } = &self.voters[voter] {
            task.abort();
        }
        self.voters[voter] = Voter::Gone;
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
            while !self.holds_pending(leader, &joiner).await {
                self.drive_until(tokio::time::sleep(StdDuration::from_millis(10)))
                    .await;
                // A heartbeat sent before the leader's lease or the
                // connection was ready is dropped, so send another.
                if !self.holds_pending(leader, &joiner).await {
                    client.send(leader_id.clone(), heartbeat_from(&joiner));
                }
            }
        })
        .await
        .expect("the leader's roster held the joiner within the timeout");
    }

    async fn holds_pending(&mut self, leader: usize, joiner: &WorkerId) -> bool {
        let joiner = joiner.clone();
        self.with(leader, move |node, _| node.is_voter_or_pending(&joiner)).await
    }

    /// Drives the voters until one leads with a lease its scheduler holds
    /// (so it accepts submissions), and every voter holds the committed
    /// configuration it leads, which counts them all; returns its index, and
    /// panics if that takes past the backstop.
    ///
    /// An election founds its configuration on the voters whose roll-call
    /// replies came in by the deadline. On a loaded host one can come in
    /// late, and the leader then admits it through a joint change of its
    /// own. Until that change commits, the shard has a voter fewer than a
    /// test built it with, and a test that kills a minority of the voters
    /// it built may leave no quorum to elect a successor.
    pub async fn drive_until_a_leader(&mut self) -> usize {
        timeout(TEST_TIMEOUT, self.drive_to_a_leader())
            .await
            .expect("one of the voters led within the timeout")
    }

    async fn drive_to_a_leader(&mut self) -> usize {
        loop {
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
            // The lease starts once the followers' acknowledgements arrive,
            // and a late voter's admission commits a round of acks later. A
            // loaded host can cost the leader its office meanwhile: then wait
            // for the next one.
            while *self.states[leader].borrow() == WorkerState::Leader {
                let (leads, led) = self
                    .with(leader, |node, scheduler| {
                        (scheduler.is_leader(), node.configuration().cloned())
                    })
                    .await;
                if leads && self.all_hold(led).await {
                    return leader;
                }
                self.drive_until(tokio::time::sleep(StdDuration::from_millis(10)))
                    .await;
            }
        }
    }

    /// Whether `led`, a leader's configuration, is committed with every voter
    /// of the shard in it, and every voter still driven holds it as one of
    /// its voters.
    async fn all_hold(&mut self, led: Option<Configuration>) -> bool {
        let Some(led) = led else {
            return false;
        };
        if led.voter_count() != Some(self.voters.len()) {
            return false;
        }
        for voter in 0..self.voters.len() {
            if matches!(self.voters[voter], Voter::Gone) {
                continue;
            }
            let led = led.clone();
            let holds = self
                .with(voter, move |node, _| {
                    node.configuration().map(Configuration::generation) == Some(led.generation())
                        && led.is_voter(node.admission())
                })
                .await;
            if !holds {
                return false;
            }
        }
        true
    }

    /// Starts driving any idle voter, then waits for `until` while the voters
    /// run, and returns what it returned; panics if that takes past the
    /// backstop. A voter whose driver returns meanwhile is driven no more
    /// (see [`Self::handed_off`]).
    pub async fn drive_until<T>(&mut self, until: impl Future<Output = T>) -> T {
        self.start_idle();
        let awaited = timeout(TEST_TIMEOUT, until)
            .await
            .expect("the awaited event happened within the timeout");
        self.check_drivers().await;
        awaited
    }

    /// What the drivers of drained voters returned, to wait on inside
    /// [`Self::drive_until`]; resolves once the voter's driver returned.
    pub fn handed_off(&self) -> HandedOffBy {
        self.handed_off.clone()
    }
}

impl Drop for Voters {
    /// Stops every voter's task, so none outlives the test.
    fn drop(&mut self) {
        for voter in &self.voters {
            if let Voter::Running { task, .. } = voter {
                task.abort();
            }
        }
    }
}

/// Drives `driven` on `net` until its driver returns, running each command
/// between two runs of the driver.
#[allow(clippy::too_many_arguments)]
async fn drive_in_background(
    voter: usize,
    mut driven: Driven,
    net: Arc<Net>,
    clock: RealClock,
    config: DriverConfig,
    state: watch::Sender<WorkerState>,
    crawled: watch::Sender<bool>,
    mut commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
    handed_off: HandedOffBy,
) {
    loop {
        let Driven { node, scheduler, executor } = &mut driven;
        let command = tokio::select! {
            returned = run_driver(node, due_now(&clock), &net, scheduler, clock, None, executor.as_mut(), config.clone(), publish(state.clone(), crawled.clone())) => {
                handed_off.record(voter, returned);
                return;
            }
            command = commands.recv() => command,
        };
        // The sender lives in `Voters`; once it is gone, so is the test.
        let Some(command) = command else { return };
        command(&mut driven.node, &mut driven.scheduler);
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
            recovery_epoch_lineage: 0,
            term_seen: 0,
            available_capacity: 0,
            active_task_runs_digest: Vec::new(),
            shard_id: Some(ShardId::new(SHARD).into()),
            newest_accepted_ack: None,
            configuration_generation: None,
            send_token: 0,
            routing_crawled: false,
            admission_generation: None,
            runs_compaction: false,
            queues: Vec::new(),
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
                .send(Net::for_shard(shard.name(), None))
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
    crawled: watch::Sender<bool>,
) -> impl FnMut(&WorkerNode<RealClock>, Option<&Input>, &Step) {
    move |node, _, _| {
        let _ = state.send(node.state());
        let _ = crawled.send(node.routing_crawled());
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
                generation: Generation::genesis(RecoveryEpoch::new(0, 0)),
                base: Generation::genesis(RecoveryEpoch::new(0, 0)),
                voter_count: count,
            })
            .expect("valid"),
            admission: Some(Generation::genesis(RecoveryEpoch::new(0, 0))),
        }),
        clock,
        None,
    )
    .0
}

/// What a worker takes that runs compaction as asked and takes every queue,
/// with the default queue policy.
pub fn takes(runs_compaction: bool) -> Takes {
    Takes {
        runs_compaction,
        queues: Subscription::Every,
        policy: QueuePolicy::new(),
        runnable: RunnableVersions::default(),
    }
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
    let genesis = Generation::genesis(RecoveryEpoch::new(0, 0));
    let configuration = Configuration::single(Single {
        generation: genesis,
        base: genesis,
        voter_count: 3,
    })
    .expect("valid");
    ElectionCertificate {
        shard_id: Some(ShardId::new(SHARD).into()),
        recovery_epoch: 0,
        recovery_epoch_lineage: 0,
        term,
        leader_id: Some(leader.clone().into()),
        configuration: Some((&configuration).into()),
        recipient_admission: None,
        recipient_prior_admission: None,
    }
}

/// How many of `nets` hold `task` with exactly these runs, oldest first.
pub fn holding(nets: &[Arc<Net>], task: &TaskId, runs: &[TaskRunState]) -> usize {
    nets.iter()
        .filter_map(|net| net.held_records().get(task))
        .filter(|record| record.runs.iter().map(|run| run.state()).eq(runs.iter().copied()))
        .count()
}

/// The record of the composite `id` as a leader of `term` wrote it at
/// `revision`, placed on no one yet.
pub fn composite_record(id: &CompositeId, term: u64, revision: u64) -> CompositeRecord {
    CompositeRecord {
        version: Some(RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: term, revision }.into()),
        composite_id: Some(id.clone().into()),
        ..CompositeRecord::default()
    }
}

pub const MIB: usize = 1024 * 1024;

/// `len` bytes that differ from chunk to chunk.
pub fn varied(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

/// Every record is placed on two voters.
pub fn two_holders() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::new(2).expect("two"))
}

/// How a call to the leader that stores for a client keeps asking.
pub fn retry() -> LeaderRetry {
    LeaderRetry {
        answer_within: StdDuration::from_secs(2),
        ask_again_after: StdDuration::from_millis(50),
    }
}

/// A flow whose stages run `names` in turn: the first is bound to an input
/// of its own, and each later one takes the result of the one before.
pub fn flow_of_bound(names: &[&str]) -> CompositeStep {
    let stages = names
        .iter()
        .enumerate()
        .map(|(at, name)| {
            let input = if at == 0 { BoundInput::Value(b"start".to_vec()) } else { BoundInput::Previous };
            Stage::Task(bound(name, input))
        })
        .collect();
    CompositeStep { kind: CompositeKind::Flow, on_error: OnError::FailFast, stages, streamed: false, reduce: None }
}

/// A group of one member, bound to the input `manifest` names.
pub fn group_of_chunked(name: &str, manifest: Manifest) -> CompositeStep {
    CompositeStep {
        kind: CompositeKind::Group,
        on_error: OnError::FailFast,
        stages: vec![Stage::Task(bound(name, BoundInput::Chunked(manifest)))],
        streamed: false,
        reduce: None,
    }
}

fn bound(name: &str, input: BoundInput) -> BoundStep {
    BoundStep {
        definition: TaskDefinitionId::new(name),
        version: 1,
        options: TaskOptions::on_queue("default"),
        input,
        fields: Vec::new(),
    }
}

/// Has `client` submit `step` to `leader` as a composite it awaits, asking
/// again until the leader's lease is ready, and returns the composite's id.
pub async fn composite_submitted_through(client: &Net, leader: &WorkerId, step: CompositeStep) -> CompositeId {
    composite_submitted_as(client, leader, &Uuid7Ids.next_composite_id(), step).await
}

/// Like [`composite_submitted_through`], under the given composite id.
pub async fn composite_submitted_as(
    client: &Net,
    leader: &WorkerId,
    id: &CompositeId,
    step: CompositeStep,
) -> CompositeId {
    composite_accepted_as(client, leader, id, step).await;
    id.clone()
}

/// Has `client` submit `step` to `leader` as the composite `id`, asking again
/// until the leader's lease is ready, and returns the leader's whole answer.
pub async fn composite_accepted_as(
    client: &Net,
    leader: &WorkerId,
    id: &CompositeId,
    step: CompositeStep,
) -> CompositeAccepted {
    let submitted = SubmittedComposite {
        composite_id: id.clone(),
        submitted_at: WallTime::now(&RealClock::new()),
        step,
        client: Some(client.local_worker_id()),
    };
    loop {
        let answer = client.submit_composite(leader.clone(), &submitted).await;
        match answer.map(|answer| answer.result) {
            Ok(Some(task_response::Result::Composite(accepted))) => return accepted,
            Ok(Some(task_response::Result::Reject(reject)))
                if !matches!(
                    TaskRejectReason::try_from(reject.reason),
                    Ok(TaskRejectReason::TaskRejectNotLeader
                        | TaskRejectReason::TaskRejectNotReady
                        | TaskRejectReason::TaskRejectNotMember)
                ) =>
            {
                panic!("the leader refused the composite for good: {reject:?}");
            }
            _ => tokio::time::sleep(StdDuration::from_millis(10)).await,
        }
    }
}

/// A map whose items arrive in batches, each run by a task of its own.
pub fn streamed_map(on_error: OnError) -> CompositeStep {
    CompositeStep {
        kind: CompositeKind::Map,
        on_error,
        stages: vec![Stage::Task(bound("each", BoundInput::Previous))],
        streamed: true,
        reduce: None,
    }
}

/// Items `start..start + count` of `map`, eight bytes each, the last batch
/// when `last`.
pub fn item_batch(map: &CompositeId, start: u64, count: u64, last: bool) -> MemberBatch {
    MemberBatch {
        composite_id: map.clone(),
        start_index: start,
        members: (start..start + count)
            .map(|index| StreamedMember::Item(ItemInput::Value(index.to_be_bytes().to_vec())))
            .collect(),
        last,
    }
}

/// Has `net` claim member `index` of `composite` from `leader`, asking again
/// until the leader grants it: the stage may not exist yet, or the member's
/// last run may not be over.
pub async fn member_claimed_through(net: &Net, leader: &WorkerId, composite: &CompositeId, index: u64) -> Claim {
    let member = composite.member_task(index);
    loop {
        let answer = net.request_claim(leader.clone(), member.clone()).await.expect("the leader answered");
        if let Some(claim_response::Result::Accept(wire)) = answer.result {
            return Claim::try_from(&wire).expect("a well-formed claim");
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
}

/// Has `net` claim member `index` of `composite` and report it started.
pub async fn member_started_through(net: &Net, leader: &WorkerId, composite: &CompositeId, index: u64) -> Claim {
    let claim = member_claimed_through(net, leader, composite, index).await;
    started(net, leader, &claim).await;
    claim
}

/// Has `net` claim member `index` of `composite`, run it to `result` and
/// report it completed.
pub async fn member_run_through(net: &Net, leader: &WorkerId, composite: &CompositeId, index: u64, result: &[u8]) {
    let claim = member_claimed_through(net, leader, composite, index).await;
    claimed_member_run_through(net, leader, &claim, result).await;
}

/// Has `net` run the member `claim` granted it to `result`: start it, store
/// `result` as the member Task's result where the leader places its record,
/// and report it completed.
pub async fn claimed_member_run_through(net: &Net, leader: &WorkerId, claim: &Claim, result: &[u8]) {
    started(net, leader, claim).await;
    let member = TaskId::from(claim.task.task_id.clone().expect("a claim names its task"));
    let owner = BlobOwner::Task(member.clone());
    let key = RecordKey::Task(member);
    let placed = net.place_records(leader.clone(), vec![key.clone()]).await.expect("the leader answered");
    let placement = placement_with_asker_of(&placed, &key).expect("the leader places the member");
    let stored = net
        .store_payload(BlobRole::Result, &owner, result, placement.clone())
        .await
        .expect("the result is stored");
    // The result is the digest of the blob that holds its encoded manifest.
    let holder = net
        .store_payload(BlobRole::Result, &owner, &stored.encode(), placement)
        .await
        .expect("the result's manifest is stored");
    let digest = match holder.entries() {
        [ManifestEntry::Blob(key)] => key.digest(),
        other => panic!("a manifest fits one blob: {other:?}"),
    };
    loop {
        let answer = net
            .complete(leader.clone(), claim.task_run_id.clone(), digest.clone())
            .await
            .expect("the leader answered");
        match answer.result {
            Some(task_response::Result::Certified(_)) => return,
            Some(task_response::Result::Reject(reject))
                if matches!(
                    TaskRejectReason::try_from(reject.reason),
                    Ok(TaskRejectReason::TaskRejectNotLeader | TaskRejectReason::TaskRejectNotReady)
                ) =>
            {
                tokio::time::sleep(StdDuration::from_millis(50)).await;
            }
            other => panic!("expected the run certified, got {other:?}"),
        }
    }
}

async fn started(net: &Net, leader: &WorkerId, claim: &Claim) {
    let answer = net
        .report_started(leader.clone(), claim.task_run_id.clone())
        .await
        .expect("the leader answered");
    assert!(
        matches!(answer.result, Some(task_response::Result::Started(_))),
        "expected the start acknowledged, got {answer:?}"
    );
}

/// Has `net` report `run`, a plain task's run, succeeded by returning `step`,
/// asking again until a leader certifies it.
pub async fn completed_with_continuation(net: &Net, leader: &WorkerId, run: &TaskRunId, step: CompositeStep) {
    loop {
        let answer = net
            .complete_continuing(leader.clone(), run.clone(), &step)
            .await
            .expect("the leader answered");
        match answer.result {
            Some(task_response::Result::Certified(_)) => return,
            Some(task_response::Result::Reject(reject))
                if matches!(
                    TaskRejectReason::try_from(reject.reason),
                    Ok(TaskRejectReason::TaskRejectNotLeader | TaskRejectReason::TaskRejectNotReady)
                ) =>
            {
                tokio::time::sleep(StdDuration::from_millis(50)).await;
            }
            other => panic!("expected the run certified, got {other:?}"),
        }
    }
}

/// Every composite finish `client` is told of, as it reads them.
pub struct FinishesHeard<'n> {
    client: &'n Net,
    heard: Vec<CompositeFinished>,
}

/// Starts listening for the finishes `client` is told of.
pub fn finished_count_heard_by(client: &Net) -> FinishesHeard<'_> {
    FinishesHeard { client, heard: Vec::new() }
}

impl<'n> FinishesHeard<'n> {
    /// Keeps listening until `count` finishes were heard, then waits a
    /// bounded moment more for a repeated one.
    pub async fn until(mut self, count: usize) -> FinishesHeard<'n> {
        loop {
            self.take();
            if self.heard.len() >= count {
                break;
            }
            self.client.result_arrived().await;
        }
        tokio::time::sleep(StdDuration::from_millis(150)).await;
        self.take();
        self
    }

    fn take(&mut self) {
        for event in self.client.take_result_events() {
            if let ResultEvent::CompositeFinished(wire) = event {
                self.heard.push(CompositeFinished::try_from(&wire).expect("the finish message reads"));
            }
        }
    }

    /// How many times `composite` was heard finished.
    pub fn count(&self, composite: &CompositeId) -> usize {
        self.heard.iter().filter(|finished| finished.composite_id == *composite).count()
    }

    /// How many times the continuation of `task` was heard finished.
    pub fn parent_task_count(&self, task: &TaskId) -> usize {
        self.heard.iter().filter(|finished| finished.parent_task.as_ref() == Some(task)).count()
    }
}

/// The first composite `client` is told is over, as it reads it.
pub async fn finished_heard_by(client: &Net) -> CompositeFinished {
    loop {
        for event in client.take_result_events() {
            if let ResultEvent::CompositeFinished(wire) = event {
                return CompositeFinished::try_from(&wire).expect("the finish message reads");
            }
        }
        client.result_arrived().await;
    }
}

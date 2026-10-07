//! A worker process's entry point on the network: [`Worker::start`] gives
//! the process its identity and a listening [`Net`], and [`Worker::run`]
//! bootstraps it into its shard (see `crate::bootstrap`) and then drives its
//! node for good (see `crate::driver`).
//!
//! ## One identity per process
//!
//! A worker's `WorkerId` is its libp2p `PeerId` (see `crate::messenger`),
//! and it lives for one process incarnation. A process that restarted under
//! an old `WorkerId` would come back with its election terms reset, and
//! could grant a vote, or win an election, in a term it had already voted
//! in: two leaders in one term. So [`Worker::start`] generates a fresh
//! keypair every time, and nothing here accepts one: a restarted process is
//! a new worker, which joins its shard again as a new pending member.
//!
//! ## One authority, paired with its timings
//!
//! The bootstrap cascade and the driver consult the same coordination
//! authority, and the node keeps its registration and fence by the TTL it
//! expects that authority to grant. [`AuthorityConfig`] carries the two
//! together, so both paths always get the same authority and timings, and
//! never one with an authority and the other without. Nothing checks the
//! TTL against what the authority grants; the node counts each
//! registration and fence for the shorter of the two, so a mismatch costs
//! renewals, not safety.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::election::{AuthorityTimings, ElectionTimings, Identity, Input, Step, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::RecordOutbox;
use kabudachi_core::time::RealClock;
use libp2p::Multiaddr;

use crate::authority::{AuthorityClient, SharedAuthority};
use crate::bootstrap::{DEFAULT_RETRY_INTERVAL, DEFAULT_SEED_ROUNDS, bootstrap};
use crate::driver::{DriverConfig, run_driver};
use crate::handoff::HandedOff;
use crate::join::DEFAULT_JOIN_PEER_TIMEOUT;
use crate::messenger::{ListenRejected, Net};
use crate::task_store::placement::ReplicationFactor;

/// A coordination authority and the timings a worker's node keeps its
/// registration and recovery fence there by (see
/// `kabudachi_core::election::AuthorityTimings`). `timings.ttl` must be the
/// TTL `authority` grants.
#[derive(Clone)]
pub struct AuthorityConfig {
    pub authority: SharedAuthority,
    pub timings: AuthorityTimings,
}

/// How a worker joins and takes part in its shard.
#[derive(Clone)]
pub struct WorkerConfig {
    /// The shard the worker serves.
    pub shard_id: ShardId,
    /// The address the worker listens on, such as `/ip4/0.0.0.0/tcp/4001`.
    pub listen_on: Multiaddr,
    /// Workers to ask who leads the shard. Empty for none.
    pub seeds: Vec<Multiaddr>,
    /// The shard's coordination authority; `None` for none.
    pub authority: Option<AuthorityConfig>,
    /// The node's election timers. The shard's reconnect timeout travels with
    /// them (see `ElectionTimings::reconnect_timeout`): every worker of a shard
    /// must use the same one.
    pub election_timings: ElectionTimings,
    /// How long bootstrap waits on each seed or registered peer it asks.
    pub join_peer_timeout: StdDuration,
    /// How long bootstrap waits between rounds of its cascade.
    pub retry_interval: StdDuration,
    /// How many full rounds of silent seeds a worker with no authority waits
    /// before it founds its shard alone (see `crate::bootstrap`).
    pub seed_rounds: u32,
    /// How often the driver re-crawls peer routing while nothing else
    /// prompts it (see `crate::driver::DriverConfig`); `None` for the
    /// default.
    pub routing_refresh_period: Option<StdDuration>,
    /// How many inputs the worker's network holds for its node while the
    /// driver is not taking them (see `Net::with_input_limit`); `None` for
    /// the default (`crate::messenger::DEFAULT_INPUT_LIMIT`).
    pub input_limit: Option<usize>,
    /// How many voters each Task record is written to.
    pub replication_factor: ReplicationFactor,
    /// How long a finished task is kept before it is forgotten, by the
    /// scheduler and by every worker's record store; `None` keeps finished
    /// tasks.
    pub result_ttl: Option<StdDuration>,
}

impl WorkerConfig {
    /// A worker of `shard_id` listening on `listen_on`, on `election_timings`,
    /// with no seeds and no authority, and the default bootstrap timeouts
    /// ([`DEFAULT_JOIN_PEER_TIMEOUT`], [`DEFAULT_RETRY_INTERVAL`]).
    ///
    /// `election_timings` are not validated here: starting the worker panics
    /// on a roll-call deadline that is not shorter than the suspicion
    /// timeout (unless the worker is alone a quorum), as it does on an
    /// invalid heartbeat interval (see `WorkerNode::start`).
    pub fn new(shard_id: ShardId, listen_on: Multiaddr, election_timings: ElectionTimings) -> Self {
        WorkerConfig {
            shard_id,
            listen_on,
            seeds: Vec::new(),
            authority: None,
            election_timings,
            join_peer_timeout: DEFAULT_JOIN_PEER_TIMEOUT,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            seed_rounds: DEFAULT_SEED_ROUNDS,
            routing_refresh_period: None,
            input_limit: None,
            replication_factor: ReplicationFactor::DEFAULT,
            result_ttl: None,
        }
    }

    #[must_use]
    pub fn with_seeds(mut self, seeds: Vec<Multiaddr>) -> Self {
        self.seeds = seeds;
        self
    }

    #[must_use]
    pub fn with_authority(mut self, authority: AuthorityConfig) -> Self {
        self.authority = Some(authority);
        self
    }

    #[must_use]
    pub fn with_join_peer_timeout(mut self, timeout: StdDuration) -> Self {
        self.join_peer_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_retry_interval(mut self, interval: StdDuration) -> Self {
        self.retry_interval = interval;
        self
    }

    #[must_use]
    pub fn with_seed_rounds(mut self, rounds: u32) -> Self {
        self.seed_rounds = rounds;
        self
    }

    #[must_use]
    pub fn with_routing_refresh_period(mut self, period: StdDuration) -> Self {
        self.routing_refresh_period = Some(period);
        self
    }

    /// Writes each Task record to `factor` voters.
    #[must_use]
    pub fn with_replication_factor(mut self, factor: ReplicationFactor) -> Self {
        self.replication_factor = factor;
        self
    }

    /// Keeps a finished task, and its record, for `ttl` before forgetting it.
    #[must_use]
    pub fn with_result_ttl(mut self, ttl: StdDuration) -> Self {
        self.result_ttl = Some(ttl);
        self
    }

    /// Bounds the inputs the worker's network holds while its driver is not taking them.
    #[must_use]
    pub fn with_input_limit(mut self, limit: usize) -> Self {
        self.input_limit = Some(limit);
        self
    }
}

/// The retention `config` gives finished tasks, in the core's time.
fn retention_of(config: &WorkerConfig) -> Option<kabudachi_core::time::Duration> {
    config.result_ttl.map(|ttl| {
        kabudachi_core::time::Duration::from_millis(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
    })
}

/// A worker that has its identity and is listening, ready to [`Self::run`].
pub struct Worker {
    net: Arc<Net>,
    config: WorkerConfig,
}

impl Worker {
    /// Starts a new worker: generates its keypair, and so its `WorkerId`
    /// (see this module's "One identity per process"), and listens on
    /// `config.listen_on`, or fails if it cannot.
    pub async fn start(config: WorkerConfig) -> Result<Worker, ListenRejected> {
        let net = Net::for_shard(config.shard_id.clone(), retention_of(&config));
        let net = Arc::new(match config.input_limit {
            Some(limit) => net.with_input_limit(limit),
            None => net,
        });
        net.try_listen_on(config.listen_on.clone()).await?;
        Ok(Worker { net, config })
    }

    /// This worker's id, fresh for this process.
    pub fn id(&self) -> WorkerId {
        self.net.local_worker_id()
    }

    /// The address this worker gives other workers to reach it (see
    /// `Net::local_multiaddr`).
    pub fn address(&self) -> Option<Multiaddr> {
        self.net.local_multiaddr()
    }

    /// This worker's network, for what its node does not do itself, such as
    /// claiming tasks from the leader its node names (`Net::request_claim`,
    /// given the leader `observe` last saw in [`Self::run`]). Only
    /// [`Self::run`] drives a node on it.
    pub fn net(&self) -> Arc<Net> {
        Arc::clone(&self.net)
    }

    /// Bootstraps this worker into its shard, then drives its node for good.
    /// `observe` is called after every step the driver carries out (see
    /// `run_driver`). Stop the worker by dropping the returned future; a
    /// worker stopped this way is gone, and a new one must be started.
    ///
    /// A worker asked to drain (see `Net::request_drain`) leaves its shard,
    /// hands the records it holds to the voters its leader chooses, and then
    /// returns, saying what became of them: the process may exit. It
    /// returns only once every record was stored or passed on, or once the
    /// node's drain wait limit has passed. A leader that drains may spend
    /// that limit twice, once waiting for its voters to crawl and once handing
    /// its records over.
    ///
    /// A node that fences itself and then finds its shard recovered without
    /// it goes back to `Bootstrapping`; the driver rejoins it through the
    /// workers the authority lists (see `run_driver`), without founding
    /// anything: the epoch it rejoins shows the shard exists.
    pub async fn run(
        self,
        observe: impl FnMut(&WorkerNode<RealClock>, Option<&Input>, &Step),
    ) -> HandedOff {
        let Worker { net, config } = self;
        let clock = RealClock::new();
        let my_id = net.local_worker_id();
        let mut authority = config.authority.as_ref().map(|authority| {
            AuthorityClient::new(
                &net,
                config.shard_id.clone(),
                Arc::clone(&authority.authority),
                authority.timings,
            )
        });
        let entry = bootstrap(
            &net,
            &clock,
            authority.as_mut(),
            &config.shard_id,
            &my_id,
            &config.seeds,
            config.join_peer_timeout,
            StdDuration::from_millis(config.election_timings.suspect_timeout.as_ticks()),
            config.retry_interval,
            config.seed_rounds,
        )
        .await;
        let identity = Identity {
            id: my_id.clone(),
            // The worker's id is already unique to this incarnation.
            incarnation: IncarnationId::new(my_id.as_str()),
            shard: config.shard_id.clone(),
            timings: config.election_timings,
        };
        let authority_timings = config.authority.as_ref().map(|authority| authority.timings);
        let (mut node, first) = WorkerNode::start(identity, entry, clock, authority_timings);
        let mut scheduler = Scheduler::with_observer(clock, Uuid7Ids, RecordOutbox::default());
        scheduler.set_result_ttl(retention_of(&config));
        run_driver(
            &mut node,
            first,
            &net,
            &mut scheduler,
            clock,
            authority,
            DriverConfig {
                routing_refresh_period: config.routing_refresh_period,
                seeds: config.seeds.clone(),
                join_peer_timeout: config.join_peer_timeout,
                retry_interval: config.retry_interval,
                replication_factor: config.replication_factor,
                // This entry point runs no executor, so it folds no chain.
                runs_compaction: false,
            },
            observe,
        )
        .await
    }
}

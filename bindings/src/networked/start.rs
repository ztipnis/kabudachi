//! What a networked worker starts from, read from the attributes of a
//! `kabudachi.native_protocol.NetworkedArguments`, and the checks and the
//! worker config made from it.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::ShardName;
use kabudachi_core::time::Duration as CoreDuration;
use kabudachi_net::executor::ExecutorEndpoint;
use kabudachi_net::task_exchange::LeaderRetry;
use kabudachi_net::task_store::placement::ReplicationFactor;
use kabudachi_net::worker::{AuthorityConfig, Multiaddr, WorkerConfig};
use kabudachi_redis_authority::{RedisAuthority, RedisAuthorityConfig};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};

use crate::arguments::memory_limits;

/// The default roll-call deadline, unless a quarter of the suspicion
/// timeout is shorter: a roll call must close well inside it.
const ROLL_CALL_DEADLINE_MS: u64 = 250;

#[derive(FromPyObject)]
pub(super) struct NetworkedArguments {
    shard: String,
    pub(super) listen: String,
    heartbeat_interval_ms: u64,
    heartbeat_timeout_ms: u64,
    pub(super) reconnect_timeout_ms: u64,
    seeds: Vec<String>,
    external_address: Option<String>,
    authority: Option<AuthorityArguments>,
    result_ttl_ms: u64,
    memory_soft_limit: Option<u64>,
    memory_hard_limit: Option<u64>,
    worker_threads: usize,
    roll_call_deadline_ms: Option<u64>,
    replication_factor: usize,
    seed_rounds: u32,
    routing_refresh_ms: Option<u64>,
}

/// The shard's coordination authority: a `redis://` or `rediss://` server.
#[derive(FromPyObject)]
pub(super) struct AuthorityArguments {
    url: String,
    key_prefix: Option<String>,
    database: u16,
    ttl_ms: Option<u64>,
}

impl NetworkedArguments {
    /// Raises `ValueError` for no worker threads, a replication factor or
    /// seed rounds of zero, or a timing or authority TTL of zero.
    pub(super) fn check(&self) -> PyResult<()> {
        if self.worker_threads == 0 {
            return Err(PyValueError::new_err("worker_threads must be at least 1"));
        }
        if self.replication_factor == 0 {
            return Err(PyValueError::new_err("replication_factor must be at least 1"));
        }
        if self.seed_rounds == 0 {
            return Err(PyValueError::new_err("seed_rounds must be at least 1"));
        }
        let authority_ttl_ms = self
            .authority
            .as_ref()
            .and_then(|authority| authority.ttl_ms);
        for (name, value) in [
            ("heartbeat_interval_ms", self.heartbeat_interval_ms),
            ("heartbeat_timeout_ms", self.heartbeat_timeout_ms),
            ("reconnect_timeout_ms", self.reconnect_timeout_ms),
            ("roll_call_deadline_ms", self.roll_call_deadline_ms.unwrap_or(1)),
            ("routing_refresh_ms", self.routing_refresh_ms.unwrap_or(1)),
            ("authority_ttl_ms", authority_ttl_ms.unwrap_or(1)),
        ] {
            if value == 0 {
                return Err(PyValueError::new_err(format!("{name} must be positive")));
            }
        }
        Ok(())
    }

    /// The worker's config, its driver's end of the executor seam included.
    /// Raises `ValueError` for an address that cannot be read, only one
    /// memory limit, or an authority that cannot be used.
    pub(super) fn worker_config(&self, executor: ExecutorEndpoint) -> PyResult<WorkerConfig> {
        let mut config = WorkerConfig::new(
            ShardName::new(self.shard.as_str()),
            address(&self.listen)?,
            self.timings(),
        )
        .with_seeds(
            self.seeds
                .iter()
                .map(|seed| address(seed))
                .collect::<PyResult<_>>()?,
        )
        .with_result_ttl(StdDuration::from_millis(self.result_ttl_ms))
        .with_seed_rounds(self.seed_rounds)
        .with_replication_factor(ReplicationFactor::new(
            NonZeroUsize::new(self.replication_factor).expect("refused by check when zero"),
        ))
        .with_executor(executor);
        if let Some(period) = self.routing_refresh_ms {
            config = config.with_routing_refresh_period(StdDuration::from_millis(period));
        }
        if let Some(external) = &self.external_address {
            config = config.with_external_address(address(external)?);
        }
        if let Some(limits) = memory_limits(self.memory_soft_limit, self.memory_hard_limit)? {
            config = config.with_memory_limits(limits);
        }
        if let Some(authority) = &self.authority {
            config = config.with_authority(authority.connect()?);
        }
        Ok(config)
    }

    /// How submissions and cancels keep asking the leader.
    pub(super) fn retry(&self) -> LeaderRetry {
        LeaderRetry {
            answer_within: StdDuration::from_millis(self.heartbeat_timeout_ms),
            ask_again_after: StdDuration::from_millis(self.heartbeat_interval_ms),
        }
    }

    /// The Tokio runtime the worker runs on.
    pub(super) fn tokio(&self) -> PyResult<Runtime> {
        Builder::new_multi_thread()
            .worker_threads(self.worker_threads)
            .thread_name("kabudachi-native")
            .enable_all()
            .build()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    /// The shard's election timings, the same on every worker of it.
    fn timings(&self) -> ElectionTimings {
        ElectionTimings::new(
            CoreDuration::from_millis(self.heartbeat_timeout_ms),
            CoreDuration::from_millis(self.heartbeat_interval_ms),
        )
        .with_roll_call_deadline(CoreDuration::from_millis(
            self.roll_call_deadline_ms.unwrap_or_else(|| {
                ROLL_CALL_DEADLINE_MS
                    .min(self.heartbeat_timeout_ms / 4)
                    .max(1)
            }),
        ))
        .with_reconnect_timeout(CoreDuration::from_millis(self.reconnect_timeout_ms))
    }
}

impl AuthorityArguments {
    /// Raises `ValueError` if the authority cannot be used.
    fn connect(&self) -> PyResult<AuthorityConfig> {
        let mut redis = RedisAuthorityConfig::new(vec![self.url.clone()]);
        if let Some(prefix) = &self.key_prefix {
            redis.key_prefix = prefix.clone();
        }
        redis.database = self.database;
        if let Some(ttl_ms) = self.ttl_ms {
            redis = redis.with_ttl(CoreDuration::from_millis(ttl_ms));
        }
        let authority = RedisAuthority::connect(redis).map_err(|error| {
            PyValueError::new_err(format!("the authority cannot be used: {error}"))
        })?;
        Ok(AuthorityConfig::new(Arc::new(authority)))
    }
}

fn address(text: &str) -> PyResult<Multiaddr> {
    text.parse()
        .map_err(|error| PyValueError::new_err(format!("{text:?} is not an address: {error}")))
}

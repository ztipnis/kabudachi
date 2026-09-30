//! Membership-change helpers shared by the `net/tests/<area>/` crates that grow,
//! shrink and replace a shard's workers (E10), on top of `super::worker`'s
//! `RunningWorker`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Single};
use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_net::worker::WorkerConfig;
use libp2p::Multiaddr;

use super::worker::{RunningWorker, spawn_worker_observing};

pub const SHARD: &str = "shard-1";

/// How long bootstrap waits on each seed it asks.
const PER_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(1);

/// Short, so a waiting worker goes round the cascade many times per test.
const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

pub fn shard() -> ShardId {
    ShardId::new(SHARD)
}

/// Every worker each term's leaders were, as their own nodes reported, shared
/// by all the workers of a test: one election term must never have two.
#[derive(Clone, Default)]
pub struct LeaderLog(Arc<Mutex<BTreeMap<u64, BTreeSet<WorkerId>>>>);

impl LeaderLog {
    /// Records that `leader` led in `term`.
    pub fn record(&self, term: u64, leader: &WorkerId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(term)
            .or_default()
            .insert(leader.clone());
    }

    /// Panics if any term had more than one leader.
    pub fn assert_one_leader_per_term(&self) {
        let log = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for (term, leaders) in log.iter() {
            assert!(
                leaders.len() <= 1,
                "term {term} had one leader, not {leaders:?}"
            );
        }
    }
}

/// Starts a worker on loopback that bootstraps through `seeds` with no
/// coordination authority, on `timings`, and runs it on its own task,
/// recording each term it leads in `leaders`.
pub async fn spawn_member(
    timings: ElectionTimings,
    seeds: Vec<Multiaddr>,
    leaders: &LeaderLog,
) -> RunningWorker {
    let config = WorkerConfig::new(shard(), "/ip4/127.0.0.1/tcp/0".parse().unwrap(), timings)
        .with_seeds(seeds)
        .with_join_peer_timeout(PER_PEER_TIMEOUT)
        .with_retry_interval(RETRY_INTERVAL);
    let leaders = leaders.clone();
    spawn_worker_observing(config, move |node, _, _| {
        // A leader names itself.
        if node.state() == WorkerState::Leader
            && let Some((me, term)) = node.known_leader()
        {
            leaders.record(term, &me);
        }
    })
    .await
}

/// Whether `configuration` is committed (not joint) with exactly
/// `voter_count` voters.
pub fn is_committed_with(configuration: &Configuration, voter_count: usize) -> bool {
    *configuration
        == Configuration::single(Single {
            generation: configuration.generation(),
            base: configuration.base(),
            voter_count,
        }).expect("valid")
}

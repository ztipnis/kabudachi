//! Workers on loopback against a real valkey server, one server per test:
//! cold bootstrap, a flush under a live leader, a server restart, and a flush
//! after the shard lost its quorum. The authority's TTL is 2 s, so these wait
//! in real time.

// Shared with the net integration tests, which use the rest of it.
#[allow(dead_code)]
#[path = "support/deadline.rs"]
mod deadline;
#[allow(dead_code)]
#[path = "support/worker.rs"]
mod worker;

use std::sync::Arc;

use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_net::worker::AuthorityConfig;
use kabudachi_redis_authority::{RedisAuthority, RedisAuthorityConfig};
use libp2p::Multiaddr;
use valkey_test_support::{ServerMode, ValkeyServer};

use deadline::within_deadline;
use worker::{RunningWorker, Seen, TEST_TIMEOUT, poll_until, spawn_worker, worker_config};

const TTL_MS: u64 = 2_000;
/// Below a third of the TTL, as the adapter requires.
const CALL_TIMEOUT_MS: u64 = 400;
const SUSPECT_TIMEOUT_MS: u64 = 1_000;
const HEARTBEAT_INTERVAL_MS: u64 = 100;
const ROLL_CALL_DEADLINE_MS: u64 = 100;
const SHARD: &str = "e2e";

fn timings() -> ElectionTimings {
    ElectionTimings::new(
        Duration::from_millis(SUSPECT_TIMEOUT_MS),
        Duration::from_millis(HEARTBEAT_INTERVAL_MS),
    )
    .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS))
}

fn redis_config(server: &ValkeyServer) -> RedisAuthorityConfig {
    let mut config =
        RedisAuthorityConfig::new(vec![server.url()]).with_ttl(Duration::from_millis(TTL_MS));
    config.call_timeout = Duration::from_millis(CALL_TIMEOUT_MS);
    config
}

/// A client of the test's own, for reading what the workers left behind.
fn probe(server: &ValkeyServer) -> RedisAuthority {
    RedisAuthority::connect(redis_config(server)).expect("valid config")
}

async fn spawn(server: &ValkeyServer, seeds: Vec<Multiaddr>) -> RunningWorker {
    let authority = AuthorityConfig::new(Arc::new(probe(server)));
    spawn_worker(
        worker_config(ShardId::new(SHARD), "/ip4/127.0.0.1/tcp/0", timings(), seeds)
            .with_authority(authority),
    )
    .await
}

/// Waits until one of `workers` leads and every other follows it, all in one
/// incarnation; returns it and the leader.
async fn led_shard(workers: &[RunningWorker]) -> (ShardId, WorkerId) {
    let mut found = None;
    poll_until("one leader followed by every worker, in one incarnation", || {
        let seen: Vec<(WorkerId, Seen)> = workers
            .iter()
            .filter_map(|worker| Some((worker.id.clone(), worker.seen.borrow().clone()?)))
            .collect();
        let leaders: Vec<&(WorkerId, Seen)> = seen
            .iter()
            .filter(|(_, seen)| seen.state == WorkerState::Leader)
            .collect();
        let [(leader_id, leader)] = leaders[..] else {
            return false;
        };
        let led = seen.len() == workers.len()
            && seen.iter().all(|(_, seen)| {
                seen.shard_id == leader.shard_id
                    && (seen.state == WorkerState::Leader
                        || (seen.state == WorkerState::Active
                            && seen.leader.as_ref() == Some(leader_id)))
            });
        if led {
            found = Some((leader.shard_id.clone(), leader_id.clone()));
        }
        led
    })
    .await;
    found.expect("set when led")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_workers_found_one_shard_cold_and_keep_it_through_a_flush_and_a_restart() {
    within_deadline(async {
        // Cold: one incarnation, however the three race to found it.
        let server = ValkeyServer::start(ServerMode::Standalone);
        let (first, second, third) = tokio::join!(
            spawn(&server, vec![]),
            spawn(&server, vec![]),
            spawn(&server, vec![])
        );
        let mut workers = vec![first, second, third];
        let (id, leader) = led_shard(&workers).await;
        assert!(id.as_str().starts_with("e2e/"), "{id:?}");
        let name = id.name();
        let probe_a = probe(&server);
        let before = probe_a
            .read_shard(&name)
            .expect("reachable")
            .expect("the founder recorded the shard");
        assert_eq!(before.shard_id, id);
        assert_eq!(before.recovery_epoch.number, 0);
        poll_until("the leader hinted itself", || {
            probe_a
                .read_leader_hint(&name)
                .expect("reachable")
                .is_some_and(|hint| hint.leader == leader && hint.shard_id == id)
        })
        .await;

        // Flush: the leader puts back the record it holds, and the hint,
        // rather than the shard being founded again.
        server.flushall();
        poll_until("the live leader republished the shard and its hint", || {
            probe_a.read_shard(&name).expect("reachable").as_ref() == Some(&before)
                && probe_a
                    .read_leader_hint(&name)
                    .expect("reachable")
                    .is_some_and(|hint| hint.leader == leader && hint.shard_id == id)
        })
        .await;
        workers.push(spawn(&server, vec![]).await);
        assert_eq!(led_shard(&workers).await.0, id);
        assert_eq!(probe_a.read_shard(&name).expect("reachable"), Some(before.clone()));

        // Restart with the data kept, once the flush's own warm-up is over, so
        // that only the restart can withhold the count. A client that saw no outage still has
        // its fence and count withheld for one TTL: the server's run id
        // changed, so its writes may have been lost.
        poll_until("the flush's warm-up ended", || {
            probe_a
                .live_registrations(&name, &id)
                .is_ok_and(|live| live.authoritative_count().is_some())
        })
        .await;
        server.shutdown_save();
        server.restart();
        let fresh = probe(&server);
        // Its first answer, before the workers have noticed anything.
        let mut first = None;
        poll_until("the restarted server answers", || {
            first = fresh.live_registrations(&name, &id).ok();
            first.is_some()
        })
        .await;
        assert_eq!(first.expect("answered").authoritative_count(), None);
        let outcome = fresh.acquire_fence(&name, &leader, &before);
        assert!(
            matches!(outcome, Err(AuthorityError::FenceHeld { .. })),
            "{outcome:?}"
        );
        // The shard kept its quorum through it: the same incarnation, its
        // epoch not behind where it was.
        assert_eq!(led_shard(&workers).await.0, id);
        let after = fresh
            .read_shard(&name)
            .expect("reachable")
            .expect("the shard's record survived");
        assert_eq!(after.shard_id, id);
        assert!(after.recovery_epoch >= before.recovery_epoch, "{after:?}");
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shard_that_loses_quorum_while_its_authority_is_flushed_is_abandoned_and_a_new_one_founded()
{
    within_deadline(async {
        let server = ValkeyServer::start(ServerMode::Standalone);
        let (first, second, third) = tokio::join!(
            spawn(&server, vec![]),
            spawn(&server, vec![]),
            spawn(&server, vec![])
        );
        let mut workers = vec![first, second, third];
        let (old_id, leader) = led_shard(&workers).await;

        // The leader and one follower are gone with the data. The survivor
        // cannot count the shard until its warm-up ends, and then finds no
        // record: its shard is lost.
        let follower = workers
            .iter()
            .position(|worker| worker.id != leader)
            .expect("a follower");
        let mut survivor = workers.swap_remove(follower);
        drop(workers);
        server.flushall();
        let seen = tokio::time::timeout(
            TEST_TIMEOUT,
            survivor.wait_until(|seen| seen.state == WorkerState::Stopped),
        )
        .await
        .expect("the survivor stopped within the timeout");
        assert_eq!(seen.shard_id, old_id);

        let newcomer = vec![spawn(&server, vec![]).await];
        let (new_id, _) = led_shard(&newcomer).await;
        assert!(new_id.as_str().starts_with("e2e/"), "{new_id:?}");
        assert_ne!(new_id, old_id);
        let record = probe(&server)
            .read_shard(&new_id.name())
            .expect("reachable")
            .expect("the new incarnation was recorded");
        assert_eq!(record.shard_id, new_id);
        assert_eq!(record.recovery_epoch.number, 0);
    })
    .await
}

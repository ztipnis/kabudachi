//! Membership changes over real sockets (ADR-0001 decisions 9 and 10): a
//! rolling deploy that replaces every worker of a shard, the leader last,
//! each new worker joining by admission batch and each old one leaving by
//! SELF_REMOVE. Every worker starts through its entry point
//! (`kabudachi_net::worker::Worker`) on its own `Net` on loopback TCP, with
//! no coordination authority: every election here is won on the returning
//! quorum alone. The test prints what it measured.


use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use libp2p::Multiaddr;
use tokio::time::timeout;

use crate::support::membership::{LeaderLog, is_committed_with, spawn_member};
use crate::support::worker::RunningWorker;

/// Backstop for each thing a test waits on.
const WAIT: StdDuration = StdDuration::from_secs(30);

fn fast_timings() -> ElectionTimings {
    ElectionTimings::new(Duration::from_millis(500), Duration::from_millis(50))
        .with_roll_call_deadline(Duration::from_millis(150))
}

/// Starts a founder, which founds the shard alone, and `size - 1` workers
/// given its address as their seed, all at once; returns them, the founder
/// first, once the founder leads a committed configuration of all `size`
/// and every other worker is a voter of it.
async fn grow_from_genesis(
    size: usize,
    timings: ElectionTimings,
    leaders: &LeaderLog,
) -> Vec<RunningWorker> {
    let mut founder = spawn_member(timings, vec![], leaders).await;
    founder
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;
    let seeds = vec![founder.address.clone()];
    let mut workers = vec![founder];
    for _ in 1..size {
        workers.push(spawn_member(timings, seeds.clone(), leaders).await);
    }
    let committed = workers[0]
        .wait_until(|seen| {
            seen.configuration
                .as_ref()
                .is_some_and(|configuration| is_committed_with(configuration, size))
        })
        .await
        .configuration
        .expect("the founder holds a configuration");
    for worker in &mut workers[1..] {
        worker
            .wait_until(|seen| {
                seen.configuration.as_ref() == Some(&committed) && seen.is_voter_of(&committed)
            })
            .await;
    }
    workers
}

/// Waits until, at one moment, every one of `workers` follows one leader
/// among them, which leads a committed configuration of exactly `workers`,
/// each a voter of it; returns that leader's index.
async fn wait_for_one_leader_of(workers: &[RunningWorker]) -> usize {
    let ids: Vec<WorkerId> = workers.iter().map(|worker| worker.id.clone()).collect();
    let settled = || -> Option<usize> {
        let seen: Vec<_> = workers
            .iter()
            .map(RunningWorker::last_seen_if_any)
            .collect();
        let leader = seen.first()?.as_ref()?.leader.clone()?;
        let index = ids.iter().position(|id| *id == leader)?;
        let all_follow = seen.iter().all(|seen| {
            seen.as_ref().is_some_and(|seen| {
                seen.leader.as_ref() == Some(&leader)
                    && seen.configuration.as_ref().is_some_and(|configuration| {
                        is_committed_with(configuration, workers.len())
                            && seen.is_voter_of(configuration)
                    })
            })
        });
        let leads = seen[index]
            .as_ref()
            .is_some_and(|seen| seen.state == WorkerState::Leader);
        (all_follow && leads).then_some(index)
    };
    timeout(WAIT, async {
        loop {
            if let Some(index) = settled() {
                return index;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the workers settled on one leader of their own within {WAIT:?}; they last showed \
             {:?}",
            workers
                .iter()
                .map(RunningWorker::last_seen_if_any)
                .collect::<Vec<_>>()
        )
    })
}

// A rolling deploy over real sockets replaces every worker of a three-voter
// shard, one at a time, the leader last: each new worker joins and is
// admitted, then one old worker drains. The old followers' SELF_REMOVEs keep
// N at three; the old leader's own drain announces the configuration
// without it on its final acks (ADR-0001 decision 10). The three new
// workers, left with no leader (whether they learn of it from those acks
// or by suspecting it), elect one of themselves with no authority.
// No term ever has two leaders.
#[tokio::test]
async fn a_rolling_deploy_replaces_every_worker_the_leader_last() {
    let leaders = LeaderLog::default();
    let mut old = grow_from_genesis(3, fast_timings(), &leaders).await;
    let started = StdInstant::now();
    let seeds: Vec<Multiaddr> = vec![old[0].address.clone()];
    let mut new: Vec<RunningWorker> = Vec::new();

    // Followers first, the leader (old[0]) last.
    for replaced in [1, 2, 0] {
        new.push(spawn_member(fast_timings(), seeds.clone(), &leaders).await);
        old[0]
            .wait_until(|seen| {
                seen.configuration
                    .as_ref()
                    .is_some_and(|configuration| is_committed_with(configuration, 4))
            })
            .await;

        old[replaced].net.request_drain();
        old[replaced]
            .wait_until(|seen| seen.state == WorkerState::Stopped)
            .await;
        if replaced != 0 {
            old[0]
                .wait_until(|seen| {
                    seen.configuration
                        .as_ref()
                        .is_some_and(|configuration| is_committed_with(configuration, 3))
                })
                .await;
        }
    }
    wait_for_one_leader_of(&new).await;
    let deployed_after = started.elapsed();

    leaders.assert_one_leader_per_term();
    eprintln!(
        "rolling deploy of 3, leader last: the new workers settled on one leader of their own \
         {deployed_after:?} after the first new worker started"
    );
}

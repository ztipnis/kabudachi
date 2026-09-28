//! Membership changes over real sockets (ADR-0001 decisions 9 and 10): a
//! shard grown from genesis by admission batches, workers leaving it by
//! SELF_REMOVE, and a rolling deploy that replaces every worker, the leader
//! last. Every worker starts through its entry point
//! (`kabudachi_net::worker::Worker`) on its own `Net` on loopback TCP, with
//! no coordination authority: every election here is won on the returning
//! quorum alone. Each test prints what it measured.

mod support;

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use tokio::time::timeout;

use support::membership::{LeaderLog, is_committed_with, spawn_member};
use support::worker::RunningWorker;

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

/// Waits until `net` has received no election message for `quiet`.
async fn wait_until_quiet(net: &Net, quiet: StdDuration) {
    timeout(WAIT, async {
        loop {
            let before = net.traffic().messages_received;
            tokio::time::sleep(quiet).await;
            if net.traffic().messages_received == before {
                return;
            }
        }
    })
    .await
    .expect("the leader went quiet between heartbeat rounds within the timeout");
}

/// Waits until `net` meshes with every one of `peers` on its shard's topic.
async fn wait_until_meshed(net: &Net, peers: &[&WorkerId]) {
    timeout(WAIT, async {
        while !peers.iter().all(|peer| net.shard_mesh().contains(*peer)) {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the workers meshed with one another within {WAIT:?}; this one meshes with {:?}, \
             sees subscribed {:?}, and knows {:?}",
            net.shard_mesh(),
            net.shard_subscribers(),
            net.peer_addresses().keys().collect::<Vec<_>>()
        )
    });
}

// A shard grown from genesis to five by admission batches loses the worker
// that founded it and led it all along, the only seed any of the others
// was given: it stops dead, its connections
// closing. The four others are four of five voters, a quorum of the
// configuration they hold, so they elect one of themselves with no
// authority, and the shard settles on the four of them.
#[tokio::test]
async fn a_shard_grown_from_genesis_to_five_re_elects_after_losing_its_founding_leader() {
    let leaders = LeaderLog::default();
    let mut workers = grow_from_genesis(5, fast_timings(), &leaders).await;
    let founder = workers.remove(0);
    let founding_term = founder.last_seen().term;

    // Each joined through the founder alone; the drivers' routing crawls
    // (after the admissions settle) are what connect them to one another.
    // Without them the four would know only the founder, and no roll call
    // would reach anyone once it is gone.
    for worker in &workers {
        let others: Vec<&WorkerId> = workers
            .iter()
            .map(|other| &other.id)
            .filter(|other| **other != worker.id)
            .collect();
        wait_until_meshed(&worker.net, &others).await;
    }
    let lost_at = StdInstant::now();
    drop(founder);
    let new_leader = wait_for_one_leader_of(&workers).await;
    let settled_after = lost_at.elapsed();

    let seen = workers[new_leader].last_seen();
    assert!(
        seen.term > founding_term,
        "the new leader won a later term than the founder's {founding_term}: {}",
        seen.term
    );
    leaders.assert_one_leader_per_term();
    eprintln!(
        "grown to 5, founder lost: re-elected at term {} and settled on 4 voters {settled_after:?} \
         after the loss",
        seen.term
    );
}

// Four of a seven-voter shard's six followers drain at once: a majority of
// its voters. Each sends SELF_REMOVE to the leader, which applies every one
// it has received since its last announcement in one generation, with no
// commit round (ADR-0001 decision 10): the configuration goes from seven
// voters to three in a single change of the leader's term, the leader keeps
// leading, and the two followers left take on the shrunk configuration.
//
// The heartbeat interval is long here, so the quiet between two rounds of
// heartbeats is long enough for the four SELF_REMOVEs, which leave
// together, to reach the leader with no announcement between them.
#[tokio::test]
async fn a_mass_self_remove_shrinks_n_in_one_generation() {
    let timings = ElectionTimings::new(Duration::from_millis(2_500), Duration::from_millis(500))
        .with_roll_call_deadline(Duration::from_millis(400));
    let leaders = LeaderLog::default();
    let mut workers = grow_from_genesis(7, timings, &leaders).await;
    let leader_seen = workers[0].last_seen();
    let before = leader_seen
        .configuration
        .clone()
        .expect("the leader holds a configuration");
    // Every follower took on the committed configuration at the same moment
    // and heartbeated at once, so their heartbeats stay in step. Drain in the
    // quiet between two rounds of them: a heartbeat the leader receives
    // between two SELF_REMOVEs is acked with an announcement, and the
    // removals after it rightly make a second generation.
    wait_until_quiet(&workers[0].net, StdDuration::from_millis(100)).await;
    let traffic_before = workers[0].net.traffic();

    let drained_at = StdInstant::now();
    for worker in &workers[1..5] {
        worker.net.request_drain();
    }
    for worker in &mut workers[1..5] {
        worker
            .wait_until(|seen| seen.state == WorkerState::Stopped)
            .await;
    }
    let shrunk = workers[0]
        .wait_until(|seen| {
            seen.configuration
                .as_ref()
                .is_some_and(|configuration| is_committed_with(configuration, 3))
        })
        .await
        .configuration
        .expect("the leader holds a configuration");
    let shrunk_after = drained_at.elapsed();
    let traffic = workers[0].net.traffic() - traffic_before;
    for worker in &mut workers[5..] {
        worker
            .wait_until(|seen| {
                seen.configuration.as_ref() == Some(&shrunk) && seen.is_voter_of(&shrunk)
            })
            .await;
    }

    assert_eq!(
        shrunk.generation(),
        before.generation().next_change(leader_seen.term),
        "one generation of the leader's own term"
    );
    let leader = workers[0].last_seen();
    assert_eq!(leader.state, WorkerState::Leader);
    assert_eq!(leader.term, leader_seen.term, "no election on the way");
    leaders.assert_one_leader_per_term();
    eprintln!(
        "mass SELF_REMOVE, 7 -> 3: shrunk {shrunk_after:?} after the drains; the leader carried \
         {traffic:?} meanwhile"
    );
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

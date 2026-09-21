//! A shard of one worker has quorum one: it must elect itself, keep the
//! leadership without any peer to confirm it, and shut down cleanly.

mod support;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use support::harness::Cluster;

const SUSPECT_TIMEOUT_TICKS: u64 = 10;
/// Comfortably more than the suspicion timeout plus the election steps.
const ELECTION_TICK_LIMIT: u64 = 100;
const STEADY_STATE_TICKS: u64 = 1_000;

fn one_tick() -> Duration {
    Duration::from_ticks(1)
}

fn lone_worker() -> WorkerId {
    WorkerId::new("worker-0")
}

fn lone_cluster() -> Cluster {
    Cluster::bootstrap(1, Duration::from_ticks(SUSPECT_TIMEOUT_TICKS))
}

/// Advances one tick at a time until the lone worker is `Leader`. Panics if
/// that takes more than `ELECTION_TICK_LIMIT` ticks.
fn advance_until_leader(cluster: &mut Cluster) {
    for _ in 0..ELECTION_TICK_LIMIT {
        cluster.advance(one_tick());
        if cluster.leader().is_some() {
            return;
        }
    }
    panic!(
        "no leader after {ELECTION_TICK_LIMIT} ticks: {:?}",
        cluster.states()
    );
}

#[test]
fn a_lone_worker_starts_active_with_no_leader() {
    let cluster = lone_cluster();

    assert_eq!(cluster.states()[&lone_worker()], WorkerState::Active);
    assert_eq!(cluster.leader(), None);
}

#[test]
fn a_lone_worker_elects_itself() {
    let mut cluster = lone_cluster();

    advance_until_leader(&mut cluster);

    assert_eq!(cluster.leader(), Some(lone_worker()));
}

#[test]
fn a_lone_worker_waits_out_the_suspicion_timeout_before_leading() {
    let mut cluster = lone_cluster();

    for _ in 0..SUSPECT_TIMEOUT_TICKS {
        cluster.advance(one_tick());
        assert_eq!(cluster.leader(), None);
    }
}

#[test]
fn a_lone_worker_wins_the_first_term() {
    let mut cluster = lone_cluster();

    advance_until_leader(&mut cluster);

    assert_eq!(cluster.node(&lone_worker()).term(), 1);
}

#[test]
fn a_lone_leader_keeps_the_leadership_without_any_peer() {
    let mut cluster = lone_cluster();
    advance_until_leader(&mut cluster);

    for _ in 0..STEADY_STATE_TICKS {
        cluster.advance(one_tick());
        assert_eq!(cluster.states()[&lone_worker()], WorkerState::Leader);
    }

    assert_eq!(cluster.node(&lone_worker()).term(), 1);
}

#[test]
fn a_lone_leader_can_shut_down_gracefully() {
    let mut cluster = lone_cluster();
    advance_until_leader(&mut cluster);

    cluster.drain(&lone_worker());

    assert_eq!(cluster.states()[&lone_worker()], WorkerState::Stopped);
}

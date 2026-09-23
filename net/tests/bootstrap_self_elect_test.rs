//! Chunk C5: the bootstrap cascade's steps (b)/(c) (README §27 Phase 2,
//! spec decision 5) — no seeds, and an authority with nothing registered for
//! the shard, so the node must fall through to self-electing alone.
//!
//! This replaces the coverage `core/tests/election_single_node_test.rs` used
//! to provide (deleted alongside `core/src/single_node.rs` in this chunk):
//! that file proved the *generic* one-member-electorate behavior — a lone
//! worker still waits out its configured `suspect_timeout` before leading,
//! unlike the old `single_node()` helper's hardcoded instant
//! (`Duration::from_ticks(0)`) self-election, which was a Phase-1-only
//! shortcut that moved to `bindings` instead (see
//! `bindings/src/local_node.rs`). This test drives that same generic
//! behavior through `kabudachi_net::bootstrap::bootstrap_node` and a real
//! swarm instead of the in-memory `Cluster` harness, so it exercises the
//! actual production path a node takes when it can reach neither a seed nor
//! a usable authority.

mod support;

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::Duration;
use kabudachi_net::bootstrap::bootstrap_node;
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::Multiaddr;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::clock::RealClock;

const SHARD: &str = "shard-1";

/// A normal, nonzero suspect_timeout — the whole point of this test is that
/// the generic cascade self-election waits this out like any other node,
/// unlike the deleted `single_node()` helper's hardcoded zero. Matches
/// `two_node_election_test.rs`'s own constant.
const SUSPECT_TIMEOUT_MS: u64 = 300;
const TICK_INTERVAL_MS: u64 = 30;

/// Generous whole-test backstop; actual self-election is expected in low
/// hundreds of milliseconds (comfortably above `SUSPECT_TIMEOUT_MS`, well
/// below this).
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

#[tokio::test]
async fn a_node_with_no_seeds_and_no_registered_authority_self_elects_leader_after_the_normal_timeout()
 {
    let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let my_id = net.local_worker_id();

    // Nothing ever registered for this shard: InMemoryAuthority::discover_workers
    // reads back Ok(empty), the "unavailable or empty result" half of spec
    // decision 5 step (c) — see kabudachi_net::bootstrap's doc for why that's
    // enough to fall all the way through to self-election with no dedicated
    // "no authority" type.
    let authority = InMemoryAuthority::new();
    let no_seeds: &[Multiaddr] = &[];

    // Taken before `bootstrap_node` builds the node, so the elapsed time
    // below is an upper bound on the node's own suspicion timer.
    let started = StdInstant::now();
    let mut node = bootstrap_node(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        RealClock::new(),
        &net,
        authority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
        no_seeds,
        StdDuration::from_secs(5),
    )
    .await;

    assert_eq!(
        node.state(),
        WorkerState::Active,
        "finish_joining({{self}}) must have driven Bootstrapping -> Joining -> Active"
    );
    assert_eq!(
        node.electorate(),
        [my_id.clone()].into_iter().collect(),
        "a node with no seeds and no discovered peers must end up alone in its own electorate"
    );

    let (tx, mut rx) = watch::channel(node.state());
    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);
    // This test only exercises the election cascade, not claim arbitration,
    // but every driven node carries a Scheduler regardless (see
    // run_driver's doc) — request_claim's own NotLeader rejection would
    // handle this node correctly even if something did send it a claim
    // request, which nothing here does.
    let mut scheduler = Scheduler::new(RealClock::new(), Uuid7Ids);

    // `run_driver` never returns (see its doc), so race it against watching
    // for Leader, mirroring two_node_election_test.rs's own pattern.
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node,
                &net,
                tick_interval,
                |s| { let _ = tx.send(s); },
                |_| {},
                &mut scheduler,
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                loop {
                    if *rx.borrow() == WorkerState::Leader {
                        return;
                    }
                    rx.changed().await.expect("driver task is still running");
                }
            } => {}
        }
    })
    .await
    .expect("the lone node reached Leader on its own within the timeout");
    assert!(
        started.elapsed() >= StdDuration::from_millis(SUSPECT_TIMEOUT_MS),
        "the lone node must wait out suspect_timeout before self-electing, got {:?}",
        started.elapsed()
    );
}

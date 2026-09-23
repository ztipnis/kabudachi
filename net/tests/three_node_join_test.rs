//! Chunk C4: the bootstrap join protocol end to end over real libp2p swarms
//! — three real `WorkerNode`s, three real `kabudachi_net::messenger::Net`s,
//! real loopback TCP sockets, no simulator, no fakes.
//!
//! ## Topology (a 2.b decision — recorded here per this chunk's brief)
//!
//! The plan's own C4 test description ("three swarms, one seeded with the
//! second's address, second already Active in a two-member shard, third
//! joins via the first") is internally ambiguous about which node is seeded
//! with which — its last clause ("third joins via the first") doesn't match
//! its earlier one ("one seeded with the second's address"). This chunk's
//! brief flagged that ambiguity and left the exact topology to be pinned
//! down and documented here, offering a "strong starting point, not a
//! mandate". This test follows that starting point directly:
//!
//! - `node_a` and `node_b` are constructed as an already-converged, `Active`
//!   two-member shard (`{a, b}`), using the *exact* synchronous-construction
//!   pattern `net/tests/two_node_election_test.rs` (chunk C3) established —
//!   see "Convergence margin" below for why that's load-bearing, not
//!   incidental.
//! - `node_c` starts fresh in `WorkerState::Bootstrapping`, seeded with only
//!   `node_a`'s listen address, and joins via `node_a` (not `node_b`) —
//!   matching the brief's "third joins via the first" clause, which this
//!   test takes as authoritative over the earlier, differently-worded clause
//!   in the plan's own text.
//!
//! `node_a` and `node_b` keep driving (via `run_driver`, including its
//! join-request-answering side — see `kabudachi_net::driver`) throughout the
//! whole test, including while `node_c` joins: `tokio::select!` polls all
//! three concurrently in one task rather than being torn down at
//! convergence, since `node_a` must still be able to answer `node_c`'s
//! `JOIN_REQUEST` after the A/B election settles.
//!
//! ## Convergence margin (the C3-warning question this chunk's brief asks
//! ## about)
//!
//! `net/tests/two_node_election_test.rs`'s doc comment (added in C3's fix,
//! commit 779c8ae) warns that its two-node convergence is an *engineering
//! margin* — synchronous back-to-back `WorkerNode` construction plus a wide
//! tick-interval:suspect-timeout ratio — not a structural guarantee from
//! `core`, and explicitly warns chunk C4 not to assume that margin holds
//! under "arbitrary/asymmetric startup timing" without re-deriving it.
//!
//! This test's A/B baseline reuses that exact pattern unchanged (same
//! `SUSPECT_TIMEOUT_MS`/`TICK_INTERVAL_MS` values, same synchronous
//! construction immediately after `connected_pair`), so the original margin
//! argument carries over verbatim — nothing about A/B's setup differs from
//! C3.
//!
//! `node_c` is the "asymmetric/later startup" case the warning is about, but
//! it does **not** reintroduce the risk the warning flags: C3's race is
//! specifically about the one-shot `RollCall`/`Candidate` path (two nodes
//! must each cross into `RollCall` before the other's forwarded roll call
//! arrives, or the election deadlocks). `node_c` never participates in that
//! race at all — it becomes `Active` by a direct, deterministic call
//! (`WorkerNode::finish_joining`), not by winning or losing a roll call. So
//! `node_c` joining strictly after A/B converge, on its own asynchronous
//! schedule, introduces no new timing dependency for *this* chunk's test to
//! reason about.

mod support;

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::Duration;
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::authority::AlwaysUnavailableAuthority;
use support::clock::RealClock;

const SHARD: &str = "shard-1";

/// Identical to `two_node_election_test.rs`'s own constant, and for the same
/// reason — see this file's "Convergence margin" doc above: reusing C3's
/// exact timing is what lets C3's margin argument carry over unchanged.
const SUSPECT_TIMEOUT_MS: u64 = 300;
const TICK_INTERVAL_MS: u64 = 30;

/// How long `node_c`'s join is allowed to take once A/B have converged.
const JOIN_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Generous whole-test backstop, matching `two_node_election_test.rs`'s own
/// reasoning: actual convergence plus a join is expected in well under a
/// second; this is a "something is actually broken" ceiling, not the
/// expected runtime.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// Connects `net_a` and `net_b` over a real loopback TCP socket (duplicated
/// from `messenger`'s own private test helper and from
/// `two_node_election_test.rs`, both `#[cfg(test)]`-private) and returns each
/// side's `WorkerId` plus `net_a`'s resolved listen address — the latter
/// doubles as `node_c`'s seed address later in this test.
async fn connected_pair(net_a: &Net, net_b: &Net) -> (WorkerId, WorkerId, libp2p::Multiaddr) {
    let listen_addr = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");

    net_b.dial(listen_addr.clone());

    let worker_a = net_a.local_worker_id();
    let worker_b = net_b.local_worker_id();

    timeout(TEST_TIMEOUT, async {
        loop {
            use kabudachi_core::transport::PeerMessenger;
            if net_a.reachable_peers(worker_a.clone()).contains(&worker_b) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("net_a saw net_b as reachable within the timeout");
    timeout(TEST_TIMEOUT, async {
        loop {
            use kabudachi_core::transport::PeerMessenger;
            if net_b.reachable_peers(worker_b.clone()).contains(&worker_a) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("net_b saw net_a as reachable within the timeout");

    // net_b starts listening only *after* its outbound connection to net_a is
    // up, and that ordering is load-bearing for the connectivity assertion at
    // the end of this test — see that assertion's own comment. Nothing orders
    // a real worker's listener against its outbound seed dial either, so this
    // is a legitimate ordering, not a contrived one.
    timeout(
        TEST_TIMEOUT,
        net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_b produced a listen address within the timeout");

    (worker_a, worker_b, listen_addr)
}

#[allow(clippy::type_complexity)]
fn make_active_node<'a>(
    my_id: WorkerId,
    electorate: &BTreeSet<WorkerId>,
    transport: &'a Net,
) -> WorkerNode<RealClock, &'a Net, RingMembership, AlwaysUnavailableAuthority> {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        RealClock::new(),
        transport,
        RingMembership::new(electorate.clone()),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
    )
}

#[allow(clippy::type_complexity)]
fn make_bootstrapping_node(
    my_id: WorkerId,
    transport: &Net,
) -> WorkerNode<RealClock, &Net, RingMembership, AlwaysUnavailableAuthority> {
    WorkerNode::bootstrapping(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        RealClock::new(),
        transport,
        RingMembership::new(BTreeSet::new()),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
    )
}

/// Blocks until `rx_a`/`rx_b` report exactly one of {Leader, Active} in the
/// pattern chunk C3 established — see its own `wait_for_convergence` for the
/// full reasoning; unchanged here.
async fn wait_for_convergence(
    mut rx_a: watch::Receiver<WorkerState>,
    mut rx_b: watch::Receiver<WorkerState>,
) -> (WorkerState, WorkerState) {
    loop {
        let (a, b) = (*rx_a.borrow(), *rx_b.borrow());
        let converged = matches!(
            (a, b),
            (WorkerState::Leader, WorkerState::Active) | (WorkerState::Active, WorkerState::Leader)
        );
        if converged {
            return (a, b);
        }
        tokio::select! {
            result = rx_a.changed() => { result.expect("node_a's driver task is still running"); }
            result = rx_b.changed() => { result.expect("node_b's driver task is still running"); }
        }
    }
}

#[tokio::test]
async fn a_third_node_joins_a_converged_two_member_shard_via_the_first() {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

    // Same reasoning as two_node_election_test.rs: connect first, then
    // construct both WorkerNodes synchronously back to back, so their
    // suspicion timers start within sub-millisecond real-wall-clock
    // distance of each other. See this file's "Convergence margin" doc.
    let (worker_a, worker_b, listen_addr_a) = connected_pair(&net_a, &net_b).await;

    let electorate: BTreeSet<WorkerId> = [worker_a.clone(), worker_b.clone()].into_iter().collect();
    let mut node_a = make_active_node(worker_a.clone(), &electorate, &net_a);
    let mut node_b = make_active_node(worker_b.clone(), &electorate, &net_b);

    let (tx_a, rx_a) = watch::channel(node_a.state());
    let (tx_b, rx_b) = watch::channel(node_b.state());

    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);
    // This test only exercises the bootstrap join cascade, not claim
    // arbitration, but every driven node carries a Scheduler regardless —
    // see run_driver's doc.
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);
    let mut scheduler_b = Scheduler::new(RealClock::new(), Uuid7Ids);

    let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let worker_c = net_c.local_worker_id();
    let mut node_c = make_bootstrapping_node(worker_c.clone(), &net_c);
    assert_eq!(node_c.state(), WorkerState::Bootstrapping);

    // node_a/node_b keep driving (including answering join requests) for the
    // whole test: select! polls all three branches concurrently, so neither
    // driver stops once A/B converge — node_a must still be able to answer
    // node_c's JOIN_REQUEST afterward. Only the third branch (converge, then
    // join) is expected to ever complete.
    let joined_members = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                &net_a,
                tick_interval,
                |s| { let _ = tx_a.send(s); },
                |_| {},
                &mut scheduler_a,
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = run_driver(
                &mut node_b,
                &net_b,
                tick_interval,
                |s| { let _ = tx_b.send(s); },
                |_| {},
                &mut scheduler_b,
            ) => {
                unreachable!("run_driver never returns")
            }
            members = async {
                wait_for_convergence(rx_a, rx_b).await;
                net_c
                    .join_via_seeds(std::slice::from_ref(&listen_addr_a), JOIN_TIMEOUT)
                    .await
            } => members,
        }
    })
    .await
    .expect("A/B converged and node_c's join completed within the test timeout");

    let members = joined_members.expect(
        "node_c's join_via_seeds should have resolved a membership from node_a \
         (a Full-support join responder answering from an already-converged shard)",
    );
    assert_eq!(
        members,
        electorate,
        "the JOIN_RESPONSE should carry exactly node_a's current electorate {{a, b}}"
    );

    // The WorkerId-set assertion above only proves node_c was *told* about
    // node_b — nothing about whether the address it was told is one anything
    // can dial. `join_via_seeds` dials every address in the JOIN_RESPONSE
    // (see its own doc), so an actual connection to node_b is the only
    // end-to-end check of that, and it matters precisely here: `connected_pair`
    // has net_b dial net_a, so from net_a's side node_b is a
    // `ConnectedPoint::Listener`, whose `get_remote_address` is node_b's
    // ephemeral source address rather than anything node_b listens on (final
    // -review finding I1 — see `kabudachi_net::messenger`'s "Where a peer's
    // address comes from"). Only the Identify arm added by that fix gives
    // net_a node_b's real listen address to advertise.
    //
    // Why `connected_pair` has net_b listen only *after* dialing: libp2p-tcp
    // 0.45.0's port reuse (`PortReuse::local_dial_addr`, and
    // `DialOpts`' default `PortUse::Reuse` in libp2p-swarm 0.48.0) binds an
    // outgoing dial to an *already registered* listening port of a matching
    // IP family and loopback status. Had net_b listened first, its ephemeral
    // source address would have coincidentally *equalled* its listen address
    // on this single-interface loopback host, and the un-dialable-address bug
    // would have been invisible here — exactly the accident that let it
    // survive four chunk reviews. Dialing before the listener exists removes
    // that coincidence, the same way a multi-homed or NATed host would.
    // Verified by probe: with the Identify arm disabled, this wait times out;
    // with it, the connection is up in milliseconds.
    timeout(TEST_TIMEOUT, async {
        loop {
            use kabudachi_core::transport::PeerMessenger;
            if net_c.reachable_peers(worker_c.clone()).contains(&worker_b) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect(
        "node_c must actually connect to node_b at the address node_a advertised in the \
         JOIN_RESPONSE, not merely learn node_b's WorkerId",
    );

    node_c.finish_joining(members);

    assert_eq!(
        node_c.state(),
        WorkerState::Active,
        "finish_joining must drive Bootstrapping -> Joining -> Active"
    );
    assert_eq!(
        node_c.electorate(),
        [worker_a, worker_b, worker_c].into_iter().collect(),
        "node_c must end up with the full membership, including itself"
    );
}

/// Regression test for the bug this fix addresses: a JOIN_RESPONSE used to
/// build its `members` list with `filter_map`, silently dropping any
/// electorate member the responder had no known dialable address for. That
/// didn't just cost the joiner a direct connection to that member (the
/// originally-understood, more benign framing) — it made the joiner's
/// resulting electorate *incomplete*, since `WorkerNode::finish_joining`
/// rebuilds membership from exactly the `WorkerId`s the response names. This
/// test proves the fix: `node_a`'s electorate names a member (`phantom_b`)
/// `node_a` has never connected to and so has no address for, and `node_c`'s
/// join must still learn `phantom_b`'s `WorkerId` from the response, only
/// missing a live connection to it.
///
/// `node_a` here is constructed directly into `WorkerState::Active`
/// (`WorkerNode::new`, same as `make_active_node`) with `phantom_b` already
/// in its electorate — no second live node or election convergence is
/// needed, since `WorkerNode::new` bypasses the election protocol entirely
/// (see its doc). This isolates the join-response bug from the unrelated
/// convergence-timing concerns the rest of this file documents.
#[tokio::test]
async fn a_joiner_learns_every_electorate_member_even_one_the_responder_cannot_dial() {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let worker_a = net_a.local_worker_id();
    let listen_addr_a = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");

    let phantom_b = WorkerId::new("phantom-b");
    let electorate: BTreeSet<WorkerId> = [worker_a.clone(), phantom_b.clone()]
        .into_iter()
        .collect();
    let mut node_a = make_active_node(worker_a.clone(), &electorate, &net_a);
    let (tx_a, _rx_a) = watch::channel(node_a.state());
    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);

    let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

    let joined_members = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                &net_a,
                tick_interval,
                |s| { let _ = tx_a.send(s); },
                |_| {},
                &mut scheduler_a,
            ) => {
                unreachable!("run_driver never returns")
            }
            members = net_c.join_via_seeds(std::slice::from_ref(&listen_addr_a), JOIN_TIMEOUT) => members,
        }
    })
    .await
    .expect("node_c's join completed within the test timeout");

    let members = joined_members.expect(
        "node_c's join_via_seeds should have resolved a membership from node_a",
    );
    assert_eq!(
        members,
        electorate,
        "the JOIN_RESPONSE must name every electorate member, including phantom_b, \
         whose WorkerId node_a knows but has no address for"
    );
}

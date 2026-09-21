//! The production pieces of a one-worker shard: a messenger with nobody to
//! talk to, an authority that is never reachable, and a node built from them
//! that elects itself almost immediately.

mod support;

use std::collections::BTreeSet;

use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::single_node::{NoAuthority, NoPeers, SingleNode, single_node};
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;

/// Comfortably more than the ticks a lone node needs to win.
const MAX_TICKS_TO_LEAD: u32 = 10;

fn me() -> WorkerId {
    WorkerId::new("only-worker")
}

fn node(clock: &FakeClock) -> SingleNode<FakeClock> {
    single_node(
        me(),
        IncarnationId::new("incarnation-1"),
        ShardId::new("shard-1"),
        clock.clone(),
    )
}

#[test]
fn no_peers_can_be_sent_nothing_and_hears_nothing() {
    let peers = NoPeers;

    peers.send(me(), WorkerId::new("ghost"), ElectionMessage::default());

    assert!(peers.poll_inbox(me()).is_empty());
    assert_eq!(peers.reachable_peers(me()), BTreeSet::new());
}

#[test]
fn no_authority_is_always_unavailable() {
    let authority = NoAuthority;
    let shard = ShardId::new("shard-1");

    assert_eq!(
        authority.discover_workers(&shard),
        Err(AuthorityError::Unavailable)
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard),
        Err(AuthorityError::Unavailable)
    );
    assert_eq!(
        authority.force_reconfigure(&shard, 0, BTreeSet::from([me()])),
        Err(AuthorityError::Unavailable)
    );
}

/// Advances one tick at a time, ticking the node, until it leads.
fn tick_until_leader(clock: &FakeClock, node: &mut SingleNode<FakeClock>) {
    for _ in 0..MAX_TICKS_TO_LEAD {
        clock.advance(Duration::from_ticks(1));
        node.tick();
        if node.state() == WorkerState::Leader {
            return;
        }
    }
    panic!(
        "not leader after {MAX_TICKS_TO_LEAD} ticks: {:?}",
        node.state()
    );
}

#[test]
fn a_single_node_starts_active() {
    let clock = FakeClock::new();

    let node = node(&clock);

    assert_eq!(node.state(), WorkerState::Active);
}

#[test]
fn a_single_node_leads_promptly_without_waiting_out_a_suspicion_timeout() {
    let clock = FakeClock::new();
    let mut node = node(&clock);

    tick_until_leader(&clock, &mut node);

    assert_eq!(node.term(), 1);
}

#[test]
fn a_single_node_leader_stays_leader() {
    let clock = FakeClock::new();
    let mut node = node(&clock);
    tick_until_leader(&clock, &mut node);

    for _ in 0..1_000 {
        clock.advance(Duration::from_ticks(1));
        node.tick();
        assert_eq!(node.state(), WorkerState::Leader);
    }

    assert_eq!(node.term(), 1);
}

#[test]
fn a_drained_single_node_stays_stopped() {
    let clock = FakeClock::new();
    let mut node = node(&clock);
    tick_until_leader(&clock, &mut node);

    node.begin_drain();
    assert_eq!(node.state(), WorkerState::Stopped);

    for _ in 0..10 {
        clock.advance(Duration::from_ticks(1));
        node.tick();
        assert_eq!(node.state(), WorkerState::Stopped);
    }
    assert_eq!(node.term(), 1);
}

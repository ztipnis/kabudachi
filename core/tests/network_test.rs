mod support;

use support::builders::worker;

use std::collections::BTreeSet;
use std::rc::Rc;

use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{self, ElectionMessage, SelfRemove};
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::network::FakeNetwork;

fn self_remove_message(worker_id: &str) -> ElectionMessage {
    ElectionMessage {
        payload: Some(messages::election_message::Payload::SelfRemove(
            SelfRemove {
                worker_id: Some(worker(worker_id).into()),
                incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
                shard_id: Some(ShardId::new("shard-1").into()),
                membership_generation: 0,
            },
        )),
    }
}

#[test]
fn baseline_no_faults_delivers_message_once_and_drains() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    network.register(a.clone());
    network.register(b.clone());

    let message = self_remove_message("a");
    network.send(a.clone(), b.clone(), message.clone());
    let delivered = network.pump();
    assert_eq!(delivered, 1);

    let inbox = network.poll_inbox(b.clone());
    assert_eq!(inbox, vec![(a, message)]);

    // Draining again with nothing new delivered returns empty.
    let inbox_again = network.poll_inbox(b);
    assert!(inbox_again.is_empty());
}

#[test]
fn drop_rate_one_delivers_nothing() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    network.register(a.clone());
    network.register(b.clone());
    network.set_drop_rate(1.0);

    network.send(a, b.clone(), self_remove_message("a"));
    let delivered = network.pump();
    assert_eq!(delivered, 0);
    assert!(network.poll_inbox(b).is_empty());
}

#[test]
fn duplicate_rate_one_delivers_exactly_twice() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    network.register(a.clone());
    network.register(b.clone());
    network.set_drop_rate(0.0);
    network.set_duplicate_rate(1.0);

    let message = self_remove_message("a");
    network.send(a.clone(), b.clone(), message.clone());
    let delivered = network.pump();
    assert_eq!(delivered, 2);

    let inbox = network.poll_inbox(b);
    assert_eq!(inbox, vec![(a.clone(), message.clone()), (a, message)]);
}

#[test]
fn delay_defers_delivery_until_clock_catches_up() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(Rc::clone(&clock));
    let a = worker("a");
    let b = worker("b");
    network.register(a.clone());
    network.register(b.clone());
    network.set_delay(Duration::from_ticks(5));

    let message = self_remove_message("a");
    network.send(a.clone(), b.clone(), message.clone());

    // Immediate pump: nothing due yet.
    assert_eq!(network.pump(), 0);
    assert!(network.poll_inbox(b.clone()).is_empty());

    // Advance by less than the delay: still nothing due.
    clock.advance(Duration::from_ticks(3));
    assert_eq!(network.pump(), 0);
    assert!(network.poll_inbox(b.clone()).is_empty());

    // Advance the rest of the way (total 5): now it's due.
    clock.advance(Duration::from_ticks(2));
    assert_eq!(network.pump(), 1);
    assert_eq!(network.poll_inbox(b), vec![(a, message)]);
}

#[test]
fn partition_blocks_cross_group_delivery_and_heal_restores_it() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    let c = worker("c");
    network.register(a.clone());
    network.register(b.clone());
    network.register(c.clone());

    let group_a: BTreeSet<WorkerId> = [a.clone()].into_iter().collect();
    let group_b: BTreeSet<WorkerId> = [b.clone(), c.clone()].into_iter().collect();
    network.partition(group_a, group_b);

    // Cross-partition send is dropped.
    network.send(a.clone(), b.clone(), self_remove_message("a"));
    assert_eq!(network.pump(), 0);
    assert!(network.poll_inbox(b.clone()).is_empty());

    // A is cut off from both B and C (the other group).
    let reachable_from_a = network.reachable_peers(a.clone());
    assert!(!reachable_from_a.contains(&b));
    assert!(!reachable_from_a.contains(&c));

    // B and C remain mutually reachable (same partition group).
    let reachable_from_b = network.reachable_peers(b.clone());
    assert!(reachable_from_b.contains(&c));

    // Same-group send still succeeds.
    let same_group_message = self_remove_message("b");
    network.send(b.clone(), c.clone(), same_group_message.clone());
    assert_eq!(network.pump(), 1);
    assert_eq!(
        network.poll_inbox(c.clone()),
        vec![(b.clone(), same_group_message)]
    );

    // Healing restores full connectivity.
    network.heal_partition();
    let healed_message = self_remove_message("a");
    network.send(a.clone(), b.clone(), healed_message.clone());
    assert_eq!(network.pump(), 1);
    assert_eq!(
        network.poll_inbox(b.clone()),
        vec![(a.clone(), healed_message)]
    );

    let reachable_from_a_after_heal = network.reachable_peers(a);
    assert!(reachable_from_a_after_heal.contains(&b));
    assert!(reachable_from_a_after_heal.contains(&c));
}

#[test]
fn broadcast_default_impl_reaches_all_recipients() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    let c = worker("c");
    network.register(a.clone());
    network.register(b.clone());
    network.register(c.clone());

    let message = self_remove_message("a");
    network.broadcast(a.clone(), vec![b.clone(), c.clone()], message.clone());
    assert_eq!(network.pump(), 2);

    assert_eq!(network.poll_inbox(b), vec![(a.clone(), message.clone())]);
    assert_eq!(network.poll_inbox(c), vec![(a, message)]);
}

/// Exercises `set_reorder`: delivery still works when enabled (reordering itself is not asserted).
#[test]
fn reorder_enabled_still_delivers_every_message_exactly_once() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    network.register(a.clone());
    network.register(b.clone());
    network.set_reorder(true);

    for i in 0..5 {
        network.send(a.clone(), b.clone(), self_remove_message(&format!("a{i}")));
    }
    assert_eq!(network.pump(), 5);
    assert_eq!(network.poll_inbox(b).len(), 5);
}

/// The same seed reproduces the same drop decisions (mid-range rate; 0.0 and 1.0 skip the PRNG).
#[test]
fn seeding_the_prng_makes_fault_injection_reproducible() {
    let run = |seed: u64| -> usize {
        let clock = Rc::new(FakeClock::new());
        let network = FakeNetwork::new(clock);
        let a = worker("a");
        let b = worker("b");
        network.register(a.clone());
        network.register(b.clone());
        network.seed(seed);
        network.set_drop_rate(0.5);

        for i in 0..20 {
            network.send(a.clone(), b.clone(), self_remove_message(&format!("a{i}")));
        }
        network.pump();
        network.poll_inbox(b).len()
    };

    assert_eq!(run(42), run(42));
}

/// An unregistered sender is a bug in the calling test, so `send` panics.
#[test]
#[should_panic(expected = "was never registered")]
fn send_panics_when_sender_is_unregistered() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    network.register(b.clone());
    // `a` was never registered.

    network.send(a, b, self_remove_message("a"));
}

#[test]
#[should_panic(expected = "was never registered")]
fn send_panics_when_recipient_is_unregistered() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let a = worker("a");
    let b = worker("b");
    network.register(a.clone());
    // `b` was never registered.

    network.send(a, b, self_remove_message("a"));
}

#[test]
#[should_panic(expected = "was never registered")]
fn poll_inbox_panics_when_worker_is_unregistered() {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(clock);
    let unregistered = worker("ghost");

    network.poll_inbox(unregistered);
}

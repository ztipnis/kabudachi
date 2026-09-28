mod support;

use support::builders::worker;

use std::collections::BTreeSet;
use std::rc::Rc;

use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{self, ElectionMessage, SelfRemove};
use kabudachi_core::time::{Clock, Duration};
use support::clock::FakeClock;
use support::network::FakeNetwork;

fn self_remove_message(worker_id: &str) -> ElectionMessage {
    ElectionMessage {
        payload: Some(messages::election_message::Payload::SelfRemove(
            SelfRemove {
                worker_id: Some(worker(worker_id).into()),
                incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
                shard_id: Some(ShardId::new("shard-1").into()),
                configuration_generation: None,
                term_seen: 0,
                leader_term: 0,
            },
        )),
    }
}

/// Every message due now, as `(from, to, message)`, in delivery order.
fn take_due(network: &FakeNetwork) -> Vec<(WorkerId, WorkerId, ElectionMessage)> {
    network
        .take_due()
        .into_iter()
        .map(|due| (due.from, due.to, due.message))
        .collect()
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

    assert_eq!(take_due(&network), vec![(a, b, message)]);

    // Taking again with nothing new sent returns empty.
    assert!(take_due(&network).is_empty());
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

    network.send(a, b, self_remove_message("a"));

    assert!(take_due(&network).is_empty());
    assert!(network.pending().is_empty());
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

    assert_eq!(
        take_due(&network),
        vec![(a.clone(), b.clone(), message.clone()), (a, b, message)]
    );
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
    let sent_at = clock.now();

    let message = self_remove_message("a");
    network.send(a.clone(), b.clone(), message.clone());

    // Nothing is due yet, and the network says when something will be.
    assert!(take_due(&network).is_empty());
    assert_eq!(
        network.next_delivery_at(),
        Some(sent_at + Duration::from_ticks(5))
    );
    assert_eq!(network.pending(), vec![(b.clone(), message.clone())]);

    // Advance by less than the delay: still nothing due.
    clock.advance(Duration::from_ticks(3));
    assert!(take_due(&network).is_empty());

    // Advance the rest of the way (total 5): now it's due.
    clock.advance(Duration::from_ticks(2));
    assert_eq!(take_due(&network), vec![(a, b, message)]);
    assert_eq!(network.next_delivery_at(), None);
    assert!(network.pending().is_empty());
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
    assert!(take_due(&network).is_empty());

    // A is cut off from both B and C (the other group), in both directions.
    assert!(network.is_partitioned(&a, &b));
    assert!(network.is_partitioned(&c, &a));

    // B and C remain connected (same partition group).
    assert!(!network.is_partitioned(&b, &c));

    // Same-group send still succeeds.
    let same_group_message = self_remove_message("b");
    network.send(b.clone(), c.clone(), same_group_message.clone());
    assert_eq!(
        take_due(&network),
        vec![(b.clone(), c.clone(), same_group_message)]
    );

    // Healing restores full connectivity.
    network.heal_partition();
    let healed_message = self_remove_message("a");
    network.send(a.clone(), b.clone(), healed_message.clone());
    assert_eq!(
        take_due(&network),
        vec![(a.clone(), b.clone(), healed_message)]
    );
    assert!(!network.is_partitioned(&a, &b));
    assert!(!network.is_partitioned(&a, &c));
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
    assert_eq!(take_due(&network).len(), 5);
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
        take_due(&network).len()
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

/// A network of registered workers `a`, `b`, `c` and `d`, with its clock.
fn four_worker_network() -> (Rc<FakeClock>, FakeNetwork, [WorkerId; 4]) {
    let clock = Rc::new(FakeClock::new());
    let network = FakeNetwork::new(Rc::clone(&clock));
    let workers = [worker("a"), worker("b"), worker("c"), worker("d")];
    for id in &workers {
        network.register(id.clone());
    }
    (clock, network, workers)
}

#[test]
fn a_publish_reaches_every_registered_worker_but_the_publisher() {
    let (_clock, network, [a, b, c, d]) = four_worker_network();

    let message = self_remove_message("b");
    network.publish(b.clone(), message.clone());

    assert_eq!(
        take_due(&network),
        vec![
            (b.clone(), a, message.clone()),
            (b.clone(), c, message.clone()),
            (b, d, message),
        ]
    );
}

#[test]
fn a_publish_does_not_reach_a_worker_partitioned_from_the_publisher() {
    let (_clock, network, [a, b, c, d]) = four_worker_network();
    network.partition(
        [a.clone(), b.clone()].into_iter().collect(),
        [c.clone()].into_iter().collect(),
    );

    let message = self_remove_message("a");
    network.publish(a.clone(), message.clone());

    // `d` is in neither group, so the partition does not cut it off.
    assert_eq!(
        take_due(&network),
        vec![(a.clone(), b, message.clone()), (a, d, message)]
    );
}

#[test]
fn a_publish_with_drop_rate_one_delivers_nothing() {
    let (_clock, network, [a, ..]) = four_worker_network();
    network.set_drop_rate(1.0);

    network.publish(a, self_remove_message("a"));

    assert!(take_due(&network).is_empty());
    assert!(network.pending().is_empty());
}

#[test]
fn duplication_and_delay_apply_to_each_delivery_of_a_publish() {
    let (clock, network, [a, b, c, d]) = four_worker_network();
    network.set_duplicate_rate(1.0);
    network.set_delay(Duration::from_ticks(5));
    let published_at = clock.now();

    let message = self_remove_message("a");
    network.publish(a.clone(), message.clone());

    assert!(take_due(&network).is_empty());
    assert_eq!(
        network.next_delivery_at(),
        Some(published_at + Duration::from_ticks(5))
    );
    clock.advance(Duration::from_ticks(5));
    assert_eq!(
        take_due(&network),
        vec![
            (a.clone(), b.clone(), message.clone()),
            (a.clone(), b, message.clone()),
            (a.clone(), c.clone(), message.clone()),
            (a.clone(), c, message.clone()),
            (a.clone(), d.clone(), message.clone()),
            (a, d, message),
        ]
    );
}

#[test]
#[should_panic(expected = "was never registered")]
fn publish_panics_when_the_publisher_is_unregistered() {
    let (_clock, network, _) = four_worker_network();

    network.publish(worker("e"), self_remove_message("e"));
}

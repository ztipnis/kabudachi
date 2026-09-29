use crate::support::builders::worker;

use std::collections::BTreeSet;
use std::rc::Rc;

use crate::support::clock::FakeClock;
use crate::support::network::FakeNetwork;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{self, ElectionMessage, SelfRemove};
use kabudachi_core::time::{Clock, Duration};

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
    assert_eq!(network.next_delivery_at(), None);
    assert!(network.pending().is_empty());
}

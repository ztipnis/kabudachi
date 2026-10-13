//! Queues: the order a leader hands out waiting work in. A higher
//! priority goes first; queues of one priority share claims by weight; and
//! a task that waited past its queue's starvation age goes before all.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use kabudachi_core::protocol::ids::{TaskDefinitionId, WorkerId};
use kabudachi_core::queues::{QueueOptions, QueuePolicy, Subscription};
use kabudachi_core::scheduler::{ClaimRejection, Submission};
use kabudachi_core::time::Duration;

use crate::support::scheduler::{Fixture, TestScheduler};

fn submit_to(scheduler: &mut TestScheduler, queue: &str, count: usize) {
    for _ in 0..count {
        scheduler
            .submit(Submission::new(
                TaskDefinitionId::new("demo.task"),
                1,
                b"input".to_vec(),
                queue,
            ))
            .expect("room for a small task");
    }
}

/// The queue of each task a claim of up to `limit` hands out, in order.
fn claimed_queues(scheduler: &mut TestScheduler, limit: usize) -> Vec<String> {
    scheduler
        .claim_oldest(&WorkerId::new("w1"), limit)
        .expect("the scheduler leads")
        .into_iter()
        .map(|claim| claim.task.queue)
        .collect()
}

#[test]
fn a_higher_priority_goes_first_weights_share_a_tier_and_a_starving_task_goes_before_all() {
    let mut fixture = Fixture::leading();
    let weight = |weight| NonZeroU32::new(weight).expect("a positive weight");
    fixture.scheduler.set_queue_policy(
        QueuePolicy::new()
            .with_queue(
                "urgent",
                QueueOptions {
                    priority: 10,
                    ..QueueOptions::DEFAULT
                },
            )
            .with_queue(
                "a",
                QueueOptions {
                    weight: weight(3),
                    ..QueueOptions::DEFAULT
                },
            )
            .with_queue(
                "b",
                QueueOptions {
                    weight: weight(1),
                    ..QueueOptions::DEFAULT
                },
            )
            .with_queue(
                "old",
                QueueOptions {
                    priority: -5,
                    starvation_age: Some(Duration::from_secs(60)),
                    ..QueueOptions::DEFAULT
                },
            ),
    );
    // Oldest first: a low-priority task, then two tiers' worth of work, then
    // the most urgent last.
    submit_to(&mut fixture.scheduler, "old", 1);
    submit_to(&mut fixture.scheduler, "a", 6);
    submit_to(&mut fixture.scheduler, "b", 6);
    submit_to(&mut fixture.scheduler, "urgent", 2);
    fixture.clock.advance(Duration::from_secs(10));

    assert_eq!(
        claimed_queues(&mut fixture.scheduler, 10),
        ["urgent", "urgent", "a", "a", "a", "b", "a", "a", "a", "b"],
        "the urgent tier first, then a and b three to one"
    );
    assert_eq!(
        claimed_queues(&mut fixture.scheduler, 1),
        ["b"],
        "a low priority waits behind a higher one until it starves"
    );
    fixture.clock.advance(Duration::from_secs(60));
    assert_eq!(
        claimed_queues(&mut fixture.scheduler, 1),
        ["old"],
        "past its starvation age it goes before the higher tier"
    );
    assert_eq!(claimed_queues(&mut fixture.scheduler, 10), ["b", "b", "b"]);
}

#[test]
fn scaffold_a_claim_outside_the_workers_queues_or_past_a_full_queue_is_refused() {
    let mut fixture = Fixture::leading();
    fixture
        .scheduler
        .set_queue_policy(QueuePolicy::new().with_queue(
            "gpu",
            QueueOptions {
                max_running: NonZeroU32::new(1),
                ..QueueOptions::DEFAULT
            },
        ));
    let cpu = WorkerId::new("cpu");
    let gpu = WorkerId::new("gpu");
    fixture.scheduler.set_subscriptions(BTreeMap::from([
        (
            cpu.clone(),
            Subscription::Only(BTreeSet::from(["default".to_owned()])),
        ),
        (
            gpu.clone(),
            Subscription::Only(BTreeSet::from(["gpu".to_owned()])),
        ),
    ]));
    let first = fixture
        .scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("t"),
            1,
            b"1".to_vec(),
            "gpu",
        ))
        .unwrap();
    let second = fixture
        .scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("t"),
            1,
            b"2".to_vec(),
            "gpu",
        ))
        .unwrap();

    assert_eq!(
        fixture.scheduler.request_claim(&cpu, &first).unwrap_err(),
        ClaimRejection::NotSubscribed
    );
    assert!(fixture.scheduler.claim_oldest(&cpu, 5).unwrap().is_empty());
    fixture.scheduler.request_claim(&gpu, &first).unwrap();
    assert_eq!(
        fixture.scheduler.request_claim(&gpu, &second).unwrap_err(),
        ClaimRejection::QueueFull
    );
    assert!(fixture.scheduler.claim_oldest(&gpu, 5).unwrap().is_empty());
    // A worker the scheduler knows no queues of may take any queue.
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&WorkerId::new("new"), &second)
            .unwrap_err(),
        ClaimRejection::QueueFull
    );
}

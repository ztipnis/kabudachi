//! Queues: the order a leader hands out waiting work in. A higher
//! priority goes first; queues of one priority share claims by weight; and
//! a task that waited past its queue's starvation age goes before all.

use std::num::NonZeroU32;

use kabudachi_core::protocol::ids::{TaskDefinitionId, WorkerId};
use kabudachi_core::queues::{QueueOptions, QueuePolicy, Subscription};
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::{ClaimRejection, Submission};
use kabudachi_core::time::Duration;

use crate::support::scheduler::{
    Fixture, OFFICE, TestScheduler, grant_of, newest_records, reconciling_after,
};

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
fn a_new_leader_counts_the_runs_it_rebuilds_against_their_queues_limit() {
    let policy = || {
        QueuePolicy::new().with_queue(
            "gpu",
            QueueOptions {
                max_running: NonZeroU32::new(2),
                ..QueueOptions::DEFAULT
            },
        )
    };
    let mut old = Fixture::leading();
    old.scheduler.set_queue_policy(policy());
    let definition = TaskDefinitionId::new("demo.task");
    for input in [b"1", b"2"] {
        let task = old
            .scheduler
            .submit(Submission::new(definition.clone(), 1, input.to_vec(), "gpu"))
            .unwrap();
        old.scheduler
            .request_claim(&WorkerId::new("w1"), &task)
            .unwrap();
    }
    let mut new = reconciling_after(&old);
    new.scheduler.set_queue_policy(policy());
    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    let third = new
        .scheduler
        .submit(Submission::new(definition, 1, b"3".to_vec(), "gpu"))
        .unwrap();

    assert_eq!(
        new.scheduler
            .request_claim(&WorkerId::new("w2"), &third)
            .unwrap_err(),
        ClaimRejection::QueueFull,
        "the two runs it inherited fill the queue"
    );
}

#[test]
fn a_front_the_caller_refuses_does_not_spend_its_queues_turn() {
    let mut fixture = Fixture::leading();
    let definition = TaskDefinitionId::new("demo.task");
    // Oldest first: two tasks of queue `a`, then one of `b`.
    for (input, queue) in [("refused", "a"), ("a-next", "a"), ("b-only", "b")] {
        fixture
            .scheduler
            .submit(Submission::new(
                definition.clone(),
                1,
                input.as_bytes().to_vec(),
                queue,
            ))
            .unwrap();
    }

    let claims = fixture
        .scheduler
        .claim_oldest_fitting(
            &WorkerId::new("w1"),
            &Subscription::Every,
            2,
            |claim| claim.task.serialized_input != b"refused",
        )
        .unwrap();

    let inputs: Vec<&[u8]> = claims
        .iter()
        .map(|claim| claim.task.serialized_input.as_slice())
        .collect();
    assert_eq!(
        inputs,
        [b"a-next".as_slice(), b"b-only".as_slice()],
        "the refused front passed, so a's turn is still its own for the task behind it"
    );
}

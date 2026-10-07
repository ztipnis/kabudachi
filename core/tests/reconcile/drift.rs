//! When a worker's heartbeats disagree with its leader about the runs it
//! holds for long enough to ask it again.

use kabudachi_core::protocol::ids::{TaskRunId, WorkerId};
use kabudachi_core::reconcile::{DriftWatch, active_runs_digest};
use kabudachi_core::time::{Duration, Instant};

#[test]
fn only_a_difference_that_lasts_brings_a_re_report_and_not_too_often() {
    let mut watch = DriftWatch::new(Duration::from_millis(50), Duration::from_millis(1_000));
    let w1 = WorkerId::new("w1");
    let agreed = active_runs_digest([]);
    let other = active_runs_digest([&TaskRunId::new("r")]);

    assert!(!watch.heard(&w1, agreed.value(), &agreed, at(0)));
    assert!(!watch.heard(&w1, other.value(), &agreed, at(10)), "a difference just begun");
    assert!(!watch.heard(&w1, agreed.value(), &agreed, at(60)), "it ended: an answer arrived");
    assert!(!watch.heard(&w1, other.value(), &agreed, at(70)));
    assert!(watch.heard(&w1, other.value(), &agreed, at(170)), "two heartbeat intervals of difference");
    watch.asked(&w1, at(170));
    assert!(!watch.heard(&w1, other.value(), &agreed, at(400)), "asked once; not again within a suspicion timeout");
    assert!(watch.heard(&w1, other.value(), &agreed, at(1_170)));
    watch.asked(&w1, at(1_170));
}

#[test]
fn each_worker_is_watched_on_its_own_and_forgetting_one_restarts_its_clock() {
    let mut watch = DriftWatch::new(Duration::from_millis(50), Duration::from_millis(1_000));
    let (w1, w2) = (WorkerId::new("w1"), WorkerId::new("w2"));
    let agreed = active_runs_digest([]);
    let other = active_runs_digest([&TaskRunId::new("r")]);

    assert!(!watch.heard(&w1, other.value(), &agreed, at(0)));
    assert!(!watch.heard(&w2, other.value(), &agreed, at(90)));
    assert!(watch.heard(&w1, other.value(), &agreed, at(100)));
    watch.asked(&w1, at(100));
    assert!(!watch.heard(&w2, other.value(), &agreed, at(100)), "w2's difference began at 90");

    watch.forget(&w1);
    assert!(!watch.heard(&w1, other.value(), &agreed, at(200)), "a difference begins afresh");
    assert!(watch.heard(&w1, other.value(), &agreed, at(300)), "and is asked again at once, not after a suspicion timeout");
    watch.asked(&w1, at(300));
}

#[test]
fn a_worker_due_but_not_asked_stays_due_until_it_is_asked() {
    let mut watch = DriftWatch::new(Duration::from_millis(50), Duration::from_millis(1_000));
    let w1 = WorkerId::new("w1");
    let agreed = active_runs_digest([]);
    let other = active_runs_digest([&TaskRunId::new("r")]);

    assert!(!watch.heard(&w1, other.value(), &agreed, at(0)));
    assert!(watch.heard(&w1, other.value(), &agreed, at(100)));
    assert!(watch.heard(&w1, other.value(), &agreed, at(110)), "no ask was sent, so no slot was spent");
    watch.asked(&w1, at(110));
    assert!(!watch.heard(&w1, other.value(), &agreed, at(200)));
}

#[test]
fn a_heartbeat_with_no_digest_differs_from_nothing() {
    let mut watch = DriftWatch::new(Duration::from_millis(50), Duration::from_millis(1_000));
    let w1 = WorkerId::new("w1");
    let expected = active_runs_digest([&TaskRunId::new("r")]);

    assert!(!watch.heard(&w1, &[], &expected, at(0)));
    assert!(!watch.heard(&w1, &[], &expected, at(500)));
}

fn at(ticks: u64) -> Instant {
    Instant::at(ticks)
}

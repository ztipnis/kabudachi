//! What a leader keeps to put its records where they belong: it publishes a
//! moved record or a refused write again, a bounded number at a time, and
//! names the holders a record left once the write that moved it is stored.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{RecordVersion, Repair, Write, WriteOutcome};
use kabudachi_core::time::{Duration, Instant};

const RETRY_AFTER: Duration = Duration::from_ticks(100);

fn worker(id: &str) -> WorkerId {
    WorkerId::new(id)
}

fn record(task: &str, revision: u64, holders: &[&str]) -> TaskRecord {
    TaskRecord {
        version: Some(
            RecordVersion {
                recovery_epoch: RecoveryEpoch::new(0, 0),
                leader_term: 1,
                revision,
            }
            .into(),
        ),
        task: Some(Task {
            task_id: Some(TaskId::new(task).into()),
            task_definition_id: Some(TaskDefinitionId::new("definition").into()),
            ..Task::default()
        }),
        placement: holders.iter().map(|holder| worker(holder).into()).collect(),
        ..TaskRecord::default()
    }
}

fn outcome(record: &TaskRecord, stored: bool) -> WriteOutcome {
    WriteOutcome { write: Write::of(record), stored }
}

/// What `check` returns for a leader that leads, may place records on
/// `placeable`, holds every task, and would place each on `holders`.
fn check(repair: &mut Repair, placeable: &[&str], holders: &[&str], at: u64) -> Vec<TaskId> {
    let placeable: Vec<WorkerId> = placeable.iter().map(|id| worker(id)).collect();
    let holders: Vec<WorkerId> = holders.iter().map(|id| worker(id)).collect();
    repair.check(true, true, &placeable, |_| true, |_| Some(holders.clone()), Instant::at(at))
}

#[test]
fn a_record_placed_anew_at_the_version_being_written_retires_nothing_when_that_write_is_stored() {
    let mut repair = Repair::new(RETRY_AFTER);
    let first = record("t", 0, &["a"]);
    repair.written(&first);
    // The same revision, placed on other voters while its write is in flight:
    // nothing writes it there, so the holder it left holds the only copy.
    repair.written(&record("t", 0, &["b"]));

    assert_eq!(repair.settled(&outcome(&first, true), Instant::at(1)), None);

    // The record is still held by both until a later revision is written to
    // the new holders alone: it is published again, and that write retires.
    let placeable = [worker("b")];
    let republish = repair.check(true, true, &placeable, |_| true, |_| Some(vec![worker("b")]), Instant::at(2));
    assert_eq!(republish, [TaskId::new("t")]);
    let next = record("t", 1, &["b"]);
    repair.written(&next);
    let retirement = repair.settled(&outcome(&next, true), Instant::at(3)).expect("it left a holder");
    assert_eq!(retirement.former, [worker("a")]);
}

#[test]
fn a_refused_write_is_published_again_after_the_delay_even_while_the_scheduler_does_not_lead() {
    let mut repair = Repair::new(RETRY_AFTER);
    let first = record("t", 0, &["a", "b"]);
    repair.written(&first);
    assert_eq!(repair.settled(&outcome(&first, false), Instant::at(10)), None);
    assert_eq!(repair.wake_at(), None, "not before a check has found it leads");
    assert!(check(&mut repair, &["a", "b"], &["a", "b"], 11).is_empty());
    assert_eq!(repair.wake_at(), Some(Instant::at(110)));
    assert!(check(&mut repair, &["a", "b"], &["a", "b"], 109).is_empty(), "not before the delay");

    // Leadership lapses and returns: the refusal is still owed a republish,
    // and nothing wakes the driver for it meanwhile.
    let placeable = [worker("a"), worker("b")];
    assert!(repair.check(true, false, &placeable, |_| true, |_| None, Instant::at(120)).is_empty());
    assert_eq!(repair.wake_at(), None);
    assert_eq!(check(&mut repair, &["a", "b"], &["a", "b"], 130), [TaskId::new("t")]);
}

#[test]
fn with_no_room_under_the_writes_in_flight_nothing_is_due_and_the_driver_is_not_woken() {
    let mut repair = Repair::new(RETRY_AFTER);
    let records: Vec<TaskRecord> = (0..65).map(|n| record(&format!("t{n:03}"), 0, &["a"])).collect();
    for record in &records {
        repair.written(record);
    }
    repair.settled(&outcome(&records[64], false), Instant::at(0));

    assert!(check(&mut repair, &["a"], &["a"], 500).is_empty(), "64 writes are in flight");
    assert_eq!(repair.wake_at(), None, "an outcome will wake the driver, a past instant would spin it");
    repair.settled(&outcome(&records[0], true), Instant::at(501));
    assert_eq!(check(&mut repair, &["a"], &["a"], 502), [TaskId::new("t064")]);
}

#[test]
fn a_write_that_moved_a_record_names_the_holders_it_left_once_stored_and_a_refusal_hands_the_debt_on() {
    let mut repair = Repair::new(RETRY_AFTER);
    repair.written(&record("t", 0, &["a", "b", "c"]));

    let moved = record("t", 1, &["a", "b", "d"]);
    repair.written(&moved);
    assert_eq!(repair.settled(&outcome(&moved, false), Instant::at(0)), None, "nothing is retired by a refused write");

    let moved_again = record("t", 2, &["a", "d", "e"]);
    repair.written(&moved_again);
    let retirement = repair.settled(&outcome(&moved_again, true), Instant::at(1)).expect("it left holders");
    assert_eq!(retirement.record, moved_again);
    assert_eq!(retirement.former, [worker("b"), worker("c")], "c left before the refused write, b since");

    let same_holders = record("t", 3, &["a", "d", "e"]);
    repair.written(&same_holders);
    assert_eq!(repair.settled(&outcome(&same_holders, true), Instant::at(2)), None, "no one is owed");
}

#[test]
fn a_change_of_voters_republishes_moved_records_a_bounded_number_at_a_time_and_only_while_leading() {
    let mut repair = Repair::new(RETRY_AFTER);
    let records: Vec<TaskRecord> = (0..100).map(|n| record(&format!("t{n:03}"), 0, &["a", "b"])).collect();
    for record in &records {
        repair.written(record);
    }
    for record in &records {
        repair.settled(&outcome(record, true), Instant::at(0));
    }
    assert!(check(&mut repair, &["a", "b"], &["a", "b"], 0).is_empty(), "nothing moved");

    let batch = check(&mut repair, &["a", "c"], &["a", "c"], 1);
    assert_eq!(batch.len(), 64, "no more than fit under the limit");
    let republished: Vec<TaskRecord> =
        batch.iter().map(|task| record(task.as_str(), 1, &["a", "c"])).collect();
    for again in &republished {
        repair.written(again);
    }
    assert!(check(&mut repair, &["a", "c"], &["a", "c"], 2).is_empty(), "the batch is still in flight");
    for again in &republished {
        repair.settled(&outcome(again, true), Instant::at(3));
    }
    assert_eq!(check(&mut repair, &["a", "c"], &["a", "c"], 4).len(), 36, "the rest follow");

    let placeable = [worker("a")];
    let held_nothing = repair.check(true, false, &placeable, |_| true, |_| None, Instant::at(5));
    assert!(held_nothing.is_empty(), "a scheduler that does not lead publishes nothing");
}

//! What a leader keeps to put its records where they belong: it publishes a
//! moved record or a refused write again, a bounded number at a time, and
//! reaches, for a write that moves a record, only the holders of earlier
//! placements that are still in the configuration.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{PlacedWrite, PriorPlacement, RecordVersion, Repair, Write, WriteOutcome};
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

/// Notes `record` as written and returns the placements the write must reach
/// besides its own.
fn written(repair: &mut Repair, record: &TaskRecord) -> Vec<PriorPlacement> {
    let mut placed = PlacedWrite::new(record.clone(), record.placement.len() / 2 + 1);
    repair.written(&mut placed, |_| true);
    placed.prior
}


fn holders(ids: &[&str]) -> Vec<WorkerId> {
    ids.iter().map(|id| worker(id)).collect()
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
fn a_refused_write_is_published_again_after_the_delay_even_while_the_scheduler_does_not_lead() {
    let mut repair = Repair::new(RETRY_AFTER);
    let first = record("t", 0, &["a", "b"]);
    written(&mut repair, &first);
    repair.settled(&outcome(&first, false), Instant::at(10));
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
        written(&mut repair, record);
    }
    repair.settled(&outcome(&records[64], false), Instant::at(0));

    assert!(check(&mut repair, &["a"], &["a"], 500).is_empty(), "64 writes are in flight");
    assert_eq!(repair.wake_at(), None, "an outcome will wake the driver, a past instant would spin it");
    repair.settled(&outcome(&records[0], true), Instant::at(501));
    assert_eq!(check(&mut repair, &["a"], &["a"], 502), [TaskId::new("t064")]);
}

#[test]
fn a_change_of_voters_republishes_moved_records_a_bounded_number_at_a_time_and_only_while_leading() {
    let mut repair = Repair::new(RETRY_AFTER);
    let records: Vec<TaskRecord> = (0..100).map(|n| record(&format!("t{n:03}"), 0, &["a", "b"])).collect();
    for record in &records {
        written(&mut repair, record);
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
        written(&mut repair, again);
    }
    assert!(check(&mut repair, &["a", "c"], &["a", "c"], 2).is_empty(), "the batch is still in flight");
    for again in &republished {
        repair.settled(&outcome(again, true), Instant::at(3));
    }
    assert_eq!(
        check(&mut repair, &["a", "c"], &["a", "c"], 4).len(),
        64,
        "the rest follow with the ends of the moves just stored, again no more than fit"
    );

    let placeable = [worker("a")];
    let held_nothing = repair.check(true, false, &placeable, |_| true, |_| None, Instant::at(5));
    assert!(held_nothing.is_empty(), "a scheduler that does not lead publishes nothing");
}

#[test]
fn a_placement_a_record_moves_from_is_reached_only_at_the_holders_still_in_the_configuration() {
    let mut repair = Repair::new(RETRY_AFTER);
    written(&mut repair, &record("t", 0, &["a", "b", "c"]));

    // `b` and `c` left the configuration but still answer: they are not asked,
    // and so cannot stand in for `a`, the one holder that can know the record.
    let mut moved = PlacedWrite::new(record("t", 1, &["a", "d", "e"]), 2);
    repair.written(&mut moved, |holder| *holder != worker("b") && *holder != worker("c"));

    assert_eq!(moved.prior, [PriorPlacement { holders: holders(&["a"]), quorum: 1 }]);
}

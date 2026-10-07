//! What a leader may tell of a task while its record writes are in flight.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::task_record::{RecordVersion, Waits, Write, WriteLedger};

fn write(task: &str, revision: u64) -> Write {
    Write {
        task_id: TaskId::new(task),
        version: RecordVersion {
            recovery_epoch: RecoveryEpoch::new(0, 0),
            leader_term: 1,
            revision,
        },
    }
}

fn waits_on(ledger: &WriteLedger, task: &str) -> Waits {
    ledger.waits_on(&TaskId::new(task))
}

#[test]
fn a_task_waits_on_its_own_pending_writes_until_they_settle() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0), write("b", 0)]);

    assert_eq!(waits_on(&ledger, "a"), Waits::Writes(vec![write("a", 0)]));

    ledger.settled(&write("a", 0), true, true);
    assert_eq!(waits_on(&ledger, "a"), Waits::Writes(vec![]));
    assert_eq!(waits_on(&ledger, "b"), Waits::Writes(vec![write("b", 0)]));
}

#[test]
fn a_refused_write_gates_its_task_until_a_newer_revision_is_stored() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0), write("a", 1)]);
    ledger.settled(&write("a", 0), false, true);
    assert_eq!(waits_on(&ledger, "a"), Waits::Refused);

    ledger.settled(&write("a", 1), true, true);
    assert_eq!(waits_on(&ledger, "a"), Waits::Writes(vec![]));
}

#[test]
fn a_stored_revision_that_is_not_newer_leaves_a_refused_one_gating() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0), write("a", 1)]);
    ledger.settled(&write("a", 1), false, true);
    ledger.settled(&write("a", 0), true, true);

    assert_eq!(waits_on(&ledger, "a"), Waits::Refused);
}

#[test]
fn a_refusal_arriving_after_a_newer_revision_was_stored_does_not_gate() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0), write("a", 1)]);
    ledger.settled(&write("a", 1), true, true);
    ledger.settled(&write("a", 0), false, true);

    assert_eq!(waits_on(&ledger, "a"), Waits::Writes(vec![]));
}

#[test]
fn a_refusal_of_a_newer_revision_than_the_stored_one_still_gates() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0), write("a", 1)]);
    ledger.settled(&write("a", 0), true, true);
    ledger.settled(&write("a", 1), false, true);

    assert_eq!(waits_on(&ledger, "a"), Waits::Refused);
}

#[test]
fn an_outcome_of_a_term_that_is_over_gates_nothing() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0)]);
    ledger.settled(&write("a", 0), false, false);

    assert_eq!(waits_on(&ledger, "a"), Waits::Writes(vec![]));
}

#[test]
fn clearing_forgets_pending_and_refused_writes() {
    let mut ledger = WriteLedger::default();
    ledger.made(&[write("a", 0), write("b", 0)]);
    ledger.settled(&write("a", 0), false, true);
    ledger.clear();

    assert_eq!(waits_on(&ledger, "a"), Waits::Writes(vec![]));
    assert_eq!(waits_on(&ledger, "b"), Waits::Writes(vec![]));
}

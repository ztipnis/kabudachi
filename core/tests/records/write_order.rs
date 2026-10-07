//! A supersession's two revisions reach the store in order: the successor
//! first, then the generation it superseded, never the other way.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{CoalescingLink, Task, TaskRecord};
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::task_record::{RecordVersion, Settlement, Write, WriteOrder};

/// A record of `task` at term 1 and `revision`, marked superseded by
/// `superseded_by` when that is given.
fn revision(task: &str, revision: u64, superseded_by: Option<&str>) -> TaskRecord {
    TaskRecord {
        version: Some(
            RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision }
                .into(),
        ),
        task: Some(Task { task_id: Some(TaskId::new(task).into()), ..Default::default() }),
        link: superseded_by.map(|newer| CoalescingLink {
            superseded_by: Some(TaskId::new(newer).into()),
            absorbed: Vec::new(),
        }),
        ..Default::default()
    }
}

#[test]
fn a_superseded_revision_waits_for_its_successor_and_is_dropped_if_that_write_fails() {
    let newer = revision("new", 0, None);
    let older = revision("old", 1, Some("new"));
    let mut order = WriteOrder::default();

    assert_eq!(order.admit(vec![newer.clone(), older.clone()]), vec![newer.clone()]);
    assert!(
        matches!(order.settled(&Write::of(&newer), true), Settlement::Release(released) if released == vec![older.clone()])
    );

    let mut failing = WriteOrder::default();
    failing.admit(vec![newer.clone(), older.clone()]);
    assert!(
        matches!(failing.settled(&Write::of(&newer), false), Settlement::Refuse(refused) if refused == vec![Write::of(&older)])
    );
}

#[test]
fn a_revision_whose_successor_is_not_in_the_same_call_is_written_at_once() {
    let older = revision("old", 1, Some("published-earlier"));
    assert_eq!(WriteOrder::default().admit(vec![older.clone()]), vec![older]);
}

#[test]
fn clearing_forgets_held_revisions_so_a_later_settlement_releases_nothing() {
    let newer = revision("new", 0, None);
    let older = revision("old", 1, Some("new"));
    let mut order = WriteOrder::default();
    order.admit(vec![newer.clone(), older]);

    order.clear();

    assert!(
        matches!(order.settled(&Write::of(&newer), true), Settlement::Release(released) if released.is_empty())
    );
}

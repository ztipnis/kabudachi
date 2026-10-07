//! A node that stores its own records names itself as their placement.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::ids::{TaskDefinitionId, Uuid7Ids, WorkerId};
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Scheduler, Submission};
use kabudachi_core::task_record::LocalRecords;
use kabudachi_core::time::RealClock;

#[test]
fn a_submitted_task_is_stored_with_this_node_as_its_placement() {
    let clock = RealClock::new();
    let node = WorkerId::new("worker-1");
    let mut scheduler =
        Scheduler::with_observer(clock, Uuid7Ids, LocalRecords::new(node.clone(), clock, None));
    scheduler.set_leadership_grant(Some(LeadershipGrant {
        term: 1,
        recovery_epoch: RecoveryEpoch::new(0, 0),
        valid_until: LeaseEnd::Unbounded,
    }));

    let task = scheduler
        .submit(Submission::new(TaskDefinitionId::new("billing.charge"), 0, b"in".to_vec(), "default"))
        .unwrap();

    let record = scheduler.observer_mut().records().get(&task).expect("the record is stored");
    assert_eq!(record.placement, vec![node.into()]);
}

//! A leader's answers wait for the writes they depend on, and for the lease.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::task_record::{EffectGate, RecordVersion, Settled, Write};

fn write(task: &str, revision: u64) -> Write {
    Write {
        task_id: TaskId::new(task),
        version: RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision },
    }
}

#[test]
fn an_effect_is_released_once_every_write_it_waits_for_is_acknowledged_while_leading() {
    let mut gate = EffectGate::new();
    assert_eq!(gate.hold("claim", [write("a", 0), write("b", 1)]), None);

    assert!(gate.acknowledged(&write("a", 0), true).is_empty(), "one write is still out");
    assert_eq!(gate.acknowledged(&write("b", 1), true), vec![Settled::Released("claim")]);
    assert!(gate.is_empty());
}

#[test]
fn effects_waiting_on_the_same_write_are_released_in_the_order_they_were_held() {
    let mut gate = EffectGate::new();
    assert_eq!(gate.hold("first", [write("a", 0)]), None);
    assert_eq!(gate.hold("second", [write("a", 0)]), None);

    assert_eq!(
        gate.acknowledged(&write("a", 0), true),
        vec![Settled::Released("first"), Settled::Released("second")]
    );
    assert!(gate.is_empty());
}

#[test]
fn an_effect_naming_the_same_write_twice_is_released_by_one_acknowledgement() {
    let mut gate = EffectGate::new();
    let w = write("a", 0);
    assert_eq!(gate.hold("claim", [w.clone(), w.clone()]), None);

    assert_eq!(gate.acknowledged(&w, true), vec![Settled::Released("claim")]);
    assert!(gate.is_empty());
}

#[test]
fn an_acknowledgement_that_arrives_after_the_lease_ended_answers_not_leader() {
    let mut gate = EffectGate::new();
    assert_eq!(gate.hold("certify", [write("a", 0)]), None);
    assert_eq!(gate.hold("unrelated", [write("b", 1)]), None);

    assert_eq!(gate.acknowledged(&write("a", 0), false), vec![Settled::NotLeader("certify")]);
    assert!(!gate.is_empty(), "an effect on another write stays held");
    assert_eq!(gate.acknowledged(&write("b", 1), true), vec![Settled::Released("unrelated")]);
    assert!(gate.is_empty());
}

#[test]
fn a_refused_write_answers_not_leader_for_every_effect_waiting_on_it_and_no_other() {
    let mut gate = EffectGate::new();
    assert_eq!(gate.hold("first", [write("a", 0)]), None);
    assert_eq!(gate.hold("second", [write("a", 0), write("b", 1)]), None);
    assert_eq!(gate.hold("unrelated", [write("c", 2)]), None);

    assert_eq!(gate.refused(&write("a", 0)), vec!["first", "second"]);
    assert_eq!(gate.acknowledged(&write("c", 2), true), vec![Settled::Released("unrelated")]);
}

#[test]
fn an_effect_that_wrote_nothing_is_released_at_once() {
    let mut gate: EffectGate<&str> = EffectGate::new();
    assert_eq!(gate.hold("rejection", []), Some(Settled::Released("rejection")));
}

#[test]
fn the_end_of_the_lease_answers_every_held_effect_not_leader() {
    let mut gate = EffectGate::new();
    assert_eq!(gate.hold("submit", [write("a", 0)]), None);
    assert_eq!(gate.hold("cancel", [write("b", 1)]), None);

    assert_eq!(gate.lease_ended(), vec!["submit", "cancel"]);
    assert!(gate.is_empty());
}

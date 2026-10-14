//! A reconciliation report that does not decode is refused, and the digest of
//! a set of runs depends on the set only.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::Task;
use kabudachi_core::protocol::ids::{CompositeId, TaskId, TaskRunId};
use kabudachi_core::reconcile::wire::{MalformedReport, page, report};
use kabudachi_core::reconcile::{HeldKey, ReportPage, ReportedRun, ReportedState, active_runs_digest};
use kabudachi_core::scheduler::Claim;
use kabudachi_core::task_record::{RecordKey, RecordVersion};

use crate::support::composites::{flow_of, one_stage_flow, task_stage};

fn held(key: RecordKey, revision: u64) -> HeldKey {
    HeldKey {
        key,
        version: RecordVersion {
            recovery_epoch: kabudachi_core::coordination_authority::RecoveryEpoch::new(0, 0),
            leader_term: 2,
            revision,
        },
        input_digest: None,
        latest_run: None,
        placement: Vec::new(),
        finished: false,
        coalescing: None,
    }
}

fn claim(run: &str) -> Claim {
    Claim {
        task: Task {
            task_id: Some(TaskId::new("task-1").into()),
            ..Task::default()
        },
        task_run_id: TaskRunId::new(run),
        attempt_number: 2,
        reconnect_timeout: kabudachi_core::election::ElectionTimings::DEFAULT_RECONNECT_TIMEOUT,
        chain: vec![b"absorbed".to_vec()],
    }
}

#[test]
fn the_run_digest_depends_on_the_set_of_runs_only() {
    let (a, b) = (TaskRunId::new("a"), TaskRunId::new("bc"));
    assert_eq!(active_runs_digest([&a, &b]), active_runs_digest([&b, &a]));
    assert_ne!(
        active_runs_digest([&a, &b]),
        active_runs_digest([&TaskRunId::new("ab"), &TaskRunId::new("c")])
    );
    assert_eq!(
        active_runs_digest([]),
        active_runs_digest(std::iter::empty())
    );
}

#[test]
fn a_report_with_a_succeeded_run_but_no_usable_result_digest_is_refused() {
    let sent = ReportPage {
        runs: vec![ReportedRun {
            claim: claim("r1"),
            state: ReportedState::Succeeded {
                result_digest: Digest::blake3(b"out"),
                delivered: false,
            },
            continuation: None,
        }],
        keys: vec![],
        last: true,
    };
    let mut message = report(&sent);
    message.runs[0].result_digest = None;

    assert_eq!(page(&message), Err(MalformedReport));

    for unknown_algorithm in [0, 99] {
        let mut message = report(&sent);
        let digest = message.runs[0].result_digest.as_mut().unwrap();
        digest.algorithm = unknown_algorithm;

        assert_eq!(
            page(&message),
            Err(MalformedReport),
            "algorithm {unknown_algorithm}"
        );
    }
}

#[test]
fn a_report_with_a_run_that_has_no_claim_is_refused() {
    let sent = ReportPage {
        runs: vec![ReportedRun {
            claim: claim("r1"),
            state: ReportedState::Running,
            continuation: None,
        }],
        keys: vec![],
        last: true,
    };
    let mut message = report(&sent);
    message.runs[0].claim = None;

    assert_eq!(page(&message), Err(MalformedReport));
}

#[test]
fn composite_keys_and_a_run_that_returned_a_step_cross_the_wire_whole() {
    let id = CompositeId::new("c1");
    let sent = ReportPage {
        runs: vec![ReportedRun {
            claim: Claim { chain: Vec::new(), ..claim("r1") },
            state: ReportedState::Succeeded {
                result_digest: Digest::blake3(b"out"),
                delivered: false,
            },
            continuation: Some(one_stage_flow()),
        }],
        keys: vec![
            held(RecordKey::Task(TaskId::new("t1")), 4),
            held(RecordKey::Composite(id.clone()), 2),
            held(RecordKey::Page(id, 3), 2),
        ],
        last: true,
    };

    assert_eq!(page(&report(&sent)), Ok(sent));
}

#[test]
fn a_held_key_naming_no_record_or_two_is_refused() {
    let sent = ReportPage {
        keys: vec![held(RecordKey::Composite(CompositeId::new("c1")), 1)],
        last: true,
        ..ReportPage::default()
    };
    let mut neither = report(&sent);
    neither.keys[0].composite_id = None;
    let mut both = report(&sent);
    both.keys[0].task_id = Some(TaskId::new("t1").into());

    assert_eq!(page(&neither), Err(MalformedReport));
    assert_eq!(page(&both), Err(MalformedReport));
}

#[test]
fn a_continuation_that_is_not_a_drivable_step_is_refused() {
    let sent = ReportPage {
        runs: vec![ReportedRun {
            claim: claim("r1"),
            state: ReportedState::Succeeded {
                result_digest: Digest::blake3(b"out"),
                delivered: false,
            },
            continuation: Some(one_stage_flow()),
        }],
        last: true,
        ..ReportPage::default()
    };
    let mut message = report(&sent);
    message.runs[0].continuation.as_mut().unwrap().stages.clear();

    assert_eq!(page(&message), Err(MalformedReport));
}

#[test]
fn a_continuation_whose_first_stage_waits_for_an_input_is_refused() {
    let sent = ReportPage {
        runs: vec![ReportedRun {
            claim: claim("r1"),
            state: ReportedState::Succeeded {
                result_digest: Digest::blake3(b"out"),
                delivered: false,
            },
            continuation: Some(flow_of(vec![task_stage("shout", None)])),
        }],
        last: true,
        ..ReportPage::default()
    };

    assert_eq!(page(&report(&sent)), Err(MalformedReport), "nothing runs before it to give it one");
}

//! A reconciliation report that does not decode is refused, and the digest of
//! a set of runs depends on the set only.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::Task;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::reconcile::wire::{MalformedReport, page, report};
use kabudachi_core::reconcile::{ReportPage, ReportedRun, ReportedState, active_runs_digest};
use kabudachi_core::scheduler::Claim;

fn claim(run: &str) -> Claim {
    Claim {
        task: Task {
            task_id: Some(TaskId::new("task-1").into()),
            ..Task::default()
        },
        task_run_id: TaskRunId::new(run),
        attempt_number: 2,
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
            },
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
        }],
        keys: vec![],
        last: true,
    };
    let mut message = report(&sent);
    message.runs[0].claim = None;

    assert_eq!(page(&message), Err(MalformedReport));
}

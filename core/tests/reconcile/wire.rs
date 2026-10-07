//! A worker's reconciliation report survives the wire, and a run's claim
//! travels without its chain.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{Task, TaskRecord, TaskRun, TaskRunIdentity};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::reconcile::wire::{MalformedReport, held_key, page, report};
use kabudachi_core::reconcile::{
    CoalescingKey, HeldKey, ReportPage, ReportedRun, ReportedState, active_runs_digest,
};
use kabudachi_core::scheduler::Claim;
use kabudachi_core::task_record::RecordVersion;

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
fn a_report_page_survives_the_wire_and_drops_each_claims_chain() {
    let sent = ReportPage {
        runs: vec![
            ReportedRun {
                claim: claim("r1"),
                state: ReportedState::Running,
            },
            ReportedRun {
                claim: claim("r2"),
                state: ReportedState::Succeeded {
                    result_digest: Digest::blake3(b"out"),
                },
            },
            ReportedRun {
                claim: claim("r3"),
                state: ReportedState::Failed {
                    failure_kind: "ValueError".into(),
                },
            },
        ],
        keys: vec![HeldKey {
            task_id: TaskId::new("task-1"),
            version: RecordVersion {
                recovery_epoch: RecoveryEpoch::new(1, 4),
                leader_term: 3,
                revision: 9,
            },
            input_digest: Some(Digest::blake3(b"in")),
            latest_run: Some(TaskRunId::new("r1")),
            placement: vec![WorkerId::new("a"), WorkerId::new("b")],
            finished: false,
            coalescing: Some(CoalescingKey {
                definition: TaskDefinitionId::new("d"),
                key: "k".into(),
            }),
        }],
        last: true,
    };

    let received = page(&report(&sent)).unwrap();

    assert!(received.runs.iter().all(|run| run.claim.chain.is_empty()));
    let mut without_chains = sent.clone();
    without_chains
        .runs
        .iter_mut()
        .for_each(|run| run.claim.chain.clear());
    assert_eq!(received, without_chains);
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
fn a_held_key_summarises_a_record_by_its_newest_run() {
    let version = RecordVersion {
        recovery_epoch: RecoveryEpoch::new(2, 1),
        leader_term: 5,
        revision: 7,
    };
    let run = |id: &str| TaskRun {
        identity: Some(TaskRunIdentity {
            task_run_id: Some(TaskRunId::new(id).into()),
            ..TaskRunIdentity::default()
        }),
        ..TaskRun::default()
    };
    let record = TaskRecord {
        version: Some(version.into()),
        task: Some(Task {
            task_id: Some(TaskId::new("task-1").into()),
            task_definition_id: Some(TaskDefinitionId::new("billing.charge").into()),
            coalescing_key: Some(String::new()),
            ..Task::default()
        }),
        runs: vec![run("r1"), run("r2")],
        input_digest: Some(Digest::blake3(b"in").into()),
        placement: vec![WorkerId::new("a").into(), WorkerId::new("b").into()],
        finished: true,
        ..TaskRecord::default()
    };

    assert_eq!(
        held_key(&record),
        Ok(HeldKey {
            task_id: TaskId::new("task-1"),
            version,
            input_digest: Some(Digest::blake3(b"in")),
            latest_run: Some(TaskRunId::new("r2")),
            placement: vec![WorkerId::new("a"), WorkerId::new("b")],
            finished: true,
            coalescing: Some(CoalescingKey {
                definition: TaskDefinitionId::new("billing.charge"),
                key: String::new(),
            }),
        })
    );
}

#[test]
fn a_report_with_a_succeeded_run_but_no_result_digest_is_refused() {
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

//! Any worker tells a leader that asks what it holds, page by page, within
//! the message limit: its runs, then its records. A failure kind it reports is
//! cut to a bound.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::protocol::messages::claim_response;
use kabudachi_core::reconcile::wire::held_key;
use kabudachi_core::reconcile::{Cursor, ReconcileTerm, ReportedState};

use crate::support::deadline::within_deadline;
use crate::support::records::{ThreeVoters, plain_with, wait_until_held};

const RESULT: &[u8] = b"result";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_reports_its_runs_then_every_record_it_holds_across_pages() {
    within_deadline(async {
        let (mut shard, _claimant) = ThreeVoters::start().await;
        let leader = shard.drive_until_a_leader().await;
        let worker = shard.others(leader)[0];
        let (leader_id, worker_id) = (shard.id(leader), shard.id(worker));
        let (leader_net, worker_net) = (shard.nets[leader].clone(), shard.nets[worker].clone());
        // A run is reported with the task it was claimed for, so eight claimed
        // runs of 200 KiB inputs cannot fit one page.
        let big = vec![7u8; 200 * 1024];
        let mut tasks = Vec::new();
        for _ in 0..8 {
            tasks.push(
                shard.schedulers[leader]
                    .submit(plain_with(&big))
                    .expect("the leader accepts a submission"),
            );
        }
        // One more run, which the worker reports failed to a voter that does not
        // lead, so that no leader records it and the worker still holds it failed.
        // A failure kind is an exception class name in practice; this one is
        // pathologically long, in three-byte characters, so that a cut at any
        // fixed number of bytes would split one.
        let long_kind = "\u{20ac}".repeat(300);
        tasks.push(
            shard.schedulers[leader]
                .submit(plain_with(b"input"))
                .expect("the leader accepts a submission"),
        );
        let failing = tasks.last().expect("a task was just added").clone();
        let mut failed_run = None;
        let mut first_run = None;
        for task in &tasks {
            let claimed = shard
                .drive_until(worker_net.request_claim(leader_id.clone(), task.clone()))
                .await
                .expect("the leader answered the claim");
            let Some(claim_response::Result::Accept(claim)) = claimed.result else {
                panic!("expected an accepted claim, got {claimed:?}");
            };
            if *task == failing {
                failed_run = claim.task_run_id.map(TaskRunId::from);
            } else if first_run.is_none() {
                first_run = claim.task_run_id.map(TaskRunId::from);
            }
        }
        let failed_run = failed_run.expect("the failing task was claimed");
        let succeeded_run = first_run.expect("another task was claimed");
        let bystander_id = shard.id(shard.others(leader)[1]);
        shard
            .drive_until(worker_net.fail(bystander_id.clone(), failed_run.clone(), long_kind.clone()))
            .await
            .expect("the bystander answered");
        // Another run, started and then completed the same way: the worker still
        // holds it succeeded, with its result.
        shard
            .drive_until(worker_net.report_started(leader_id.clone(), succeeded_run.clone()))
            .await
            .expect("the leader answered");
        shard
            .drive_until(worker_net.complete(
                bystander_id.clone(),
                succeeded_run.clone(),
                Digest::blake3(RESULT),
            ))
            .await
            .expect("the bystander answered");
        shard
            .drive_until(wait_until_held(worker_net.clone(), tasks.clone()))
            .await;

        // Any office: a worker answers whoever asks, and the term only labels its logs.
        let term = ReconcileTerm {
            recovery_epoch: RecoveryEpoch::new(0, 0),
            term: 99,
        };
        let (mut runs, mut keys, mut cursor, mut pages) = (Vec::new(), Vec::new(), None, 0);
        loop {
            let page = shard
                .drive_until(leader_net.ask_reconcile(worker_id.clone(), term, cursor.clone(), false))
                .await
                .expect("the worker answered");
            pages += 1;
            runs.extend(page.runs.clone());
            keys.extend(page.keys.clone());
            if page.last {
                break;
            }
            cursor = Some(match (page.keys.last(), page.runs.last()) {
                (Some(key), _) => Cursor::AfterKey(key.task_id.clone()),
                (None, Some(run)) => Cursor::AfterRun(run.claim.task_run_id.clone()),
                (None, None) => panic!("a page that is not the last holds something"),
            });
        }

        assert!(pages > 1, "nine runs, eight of them of 200 KiB, take more than one page");
        for run in &runs {
            if run.claim.task_run_id == failed_run {
                let ReportedState::Failed { failure_kind } = &run.state else {
                    panic!("the run the worker failed was reported {:?}", run.state);
                };
                assert!(!failure_kind.is_empty() && failure_kind.len() < long_kind.len());
                assert!(
                    long_kind.starts_with(failure_kind.as_str()),
                    "the kind is cut, not changed, and not through a character"
                );
            } else if run.claim.task_run_id == succeeded_run {
                assert_eq!(
                    run.state,
                    ReportedState::Succeeded { result_digest: Digest::blake3(RESULT) }
                );
            } else {
                assert_eq!(run.state, ReportedState::Claimed);
            }
        }
        let run_ids: Vec<_> = runs.iter().map(|run| run.claim.task_run_id.clone()).collect();
        assert!(run_ids.windows(2).all(|pair| pair[0] < pair[1]), "every run, in id order, once");
        let mut run_tasks: Vec<_> = runs
            .iter()
            .map(|run| TaskId::from(run.claim.task.task_id.clone().expect("a claimed task has an id")))
            .collect();
        run_tasks.sort();
        let mut expected = tasks.clone();
        expected.sort();
        assert_eq!(run_tasks, expected, "a run for every task claimed");
        let key_tasks: Vec<_> = keys.iter().map(|key| key.task_id.clone()).collect();
        assert_eq!(key_tasks, expected, "every record held, in id order, once");
        for key in keys {
            let held = worker_net
                .held_records()
                .get(&key.task_id)
                .expect("the worker holds the record it reported");
            assert_eq!(
                Ok(key),
                held_key(&held),
                "a record is summarised by its version, input digest, latest run, placement and coalescing key"
            );
        }
    })
    .await
}

//! Any worker tells a leader that asks what it holds, page by page, within
//! the message limit: its runs, then its records.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::messages::claim_response;
use kabudachi_core::reconcile::{Cursor, ReconcileTerm, ReportedState};

use crate::support::records::{ThreeVoters, plain_with, wait_until_held};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_reports_its_runs_then_every_record_it_holds_across_pages() {
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
    for task in &tasks {
        let claimed = shard
            .drive_until(worker_net.request_claim(leader_id.clone(), task.clone()))
            .await
            .expect("the leader answered the claim");
        assert!(matches!(claimed.result, Some(claim_response::Result::Accept(_))));
    }
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
        keys.extend(page.keys.iter().map(|key| key.task_id.clone()));
        if page.last {
            break;
        }
        cursor = Some(match (page.keys.last(), page.runs.last()) {
            (Some(key), _) => Cursor::AfterKey(key.task_id.clone()),
            (None, Some(run)) => Cursor::AfterRun(run.claim.task_run_id.clone()),
            (None, None) => panic!("a page that is not the last holds something"),
        });
    }

    assert!(pages > 1, "eight 200 KiB runs take more than one page");
    assert!(runs.iter().all(|run| run.state == ReportedState::Claimed));
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
    assert_eq!(keys, expected, "every record held, in id order, once");
}

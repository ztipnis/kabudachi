//! Over real sockets a leader makes compaction runs only once a worker says
//! it runs them (it has an executor), hands them only to such a worker, and
//! applies the fold that worker's executor makes.

use kabudachi_core::protocol::generated::chain_entry;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::messages::{ClaimRejectReason, claim_response};
use kabudachi_core::scheduler::MemoryLimits;
use kabudachi_net::executor::Report;

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::records::{
    ThreeVoters, claimed_and_started, plain_with, submitted_through,
};
use crate::support::worker::poll_until;

/// Not associative, so a fold in the wrong order or grouping shows.
fn merge(older: &[u8], newer: &[u8]) -> Vec<u8> {
    [b"(".as_slice(), older, b">", newer, b")"].concat()
}

fn fold_all(payloads: &[Vec<u8>]) -> Vec<u8> {
    payloads[1..]
        .iter()
        .fold(payloads[0].clone(), |folded, next| merge(&folded, next))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_runs_compaction_folds_the_chain_its_leader_hands_it() {
    within_deadline(async {
        let (mut shard, client) = ThreeVoters::start().await;
        let mut executors = Vec::new();
        for voter in 0..3 {
            let (executor, endpoint) = FakeExecutor::new();
            shard.set_executor(voter, endpoint);
            executors.push(executor);
            shard
                .with(voter, |_, scheduler| {
                    scheduler.set_memory_limits(Some(MemoryLimits {
                        soft: 300,
                        hard: 1_000_000,
                    }))
                })
                .await;
        }
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        shard.join_as_pending(&client, leader).await;
        let runner = shard.others(leader)[0];
        let runner_net = shard.nets[runner].clone();
        let nets = shard.nets.clone();
        let keyed = |payload: &[u8]| plain_with(payload).with_coalescing_key("k");

        let newest = shard
            .drive_until(async {
                let holder = submitted_through(&client, &leader_id, keyed(b"h")).await;
                claimed_and_started(&runner_net, &leader_id, &holder).await;
                let mut newest = None;
                for letter in b'a'..b'g' {
                    newest = Some(submitted_through(&client, &leader_id, keyed(&[letter; 80])).await);
                }
                // The leader makes the run once it has heard a worker say it
                // runs them.
                let compaction = find_compaction(&nets).await;

                // A peer offers the run only to a worker that says it runs them.
                let holder = nets
                    .iter()
                    .find(|net| net.held_records().get(&compaction).is_some())
                    .expect("a majority holds the run's record")
                    .local_worker_id();
                let offered = client.steal(holder.clone(), 10, true).await.expect("the peer answered");
                assert!(offered.contains(&compaction));
                let withheld = client.steal(holder, 10, false).await.expect("the peer answered");
                assert!(!withheld.contains(&compaction));

                let refused = client
                    .request_claim(leader_id.clone(), compaction.clone())
                    .await
                    .expect("the leader answered");
                let Some(claim_response::Result::Reject(reject)) = refused.result else {
                    panic!("expected the claim refused, got {refused:?}");
                };
                assert_eq!(reject.reason, ClaimRejectReason::ClaimRejectCannotRun as i32);

                // The runner's executor offers a place: its driver claims the
                // compaction run and hands it over as a fold to make.
                executors[runner].grant(1);
                let (run, claim) = executors[runner].next_claim().await;
                assert_eq!(
                    claim.task.as_ref().and_then(|task| task.task_id.clone()).map(TaskId::from),
                    Some(compaction)
                );
                executors[runner].report(Report::Compacted {
                    run,
                    payload: fold_all(&claim.chain),
                });
                newest.expect("generations were submitted")
            })
            .await;

        let nets_now = nets.clone();
        poll_until("a majority holds the record with the fold", || {
            nets_now
                .iter()
                .filter_map(|net| net.held_records().get(&newest))
                .filter(|record| {
                    matches!(
                        record.retained_chain.first().and_then(|entry| entry.entry.as_ref()),
                        Some(chain_entry::Entry::Folded(_))
                    )
                })
                .count()
                >= 2
        })
        .await;
    })
    .await
}

/// The compaction run some voter's records hold, once there is one.
async fn find_compaction(nets: &[std::sync::Arc<kabudachi_net::messenger::Net>]) -> TaskId {
    loop {
        let found = nets
            .iter()
            .flat_map(|net| {
                let held = net.held_records();
                held.task_ids()
                    .into_iter()
                    .filter_map(move |task| held.get(&task))
            })
            .filter(|record| !record.finished)
            .filter_map(|record| record.task)
            .find(|task| task.compacts.is_some());
        if let Some(task) = found {
            return task.task_id.map(TaskId::from).expect("a task has an id");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

//! A claimant's whole run over real sockets, as the one smoke test of the
//! task exchange: refused while outside the leader's roster, then submitted
//! through the leader with a delay, claimed once the leader's driver released
//! it, started and completed. Each answer comes only once a majority of the
//! task's placement stored the decision, and only the result's digest reaches
//! the leader. The cases of the exchange (retries, cancels, repeated
//! submissions, answers withheld without a quorum) are the scheduler's and
//! the simulator's.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, Uuid7Ids};
use kabudachi_core::protocol::messages::{
    ClaimRejectReason, TaskRejectReason, TaskResponse, claim_response, task_response,
};
use kabudachi_core::scheduler::{Submission, mint};
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::claimed_runs::HeldRun;
use kabudachi_net::messenger::Net;

use crate::support::deadline::within_deadline;
use crate::support::records::{ThreeVoters, Voters};

const RESULT: &[u8] = b"the-result";

/// A submission that sets every field a client chooses, so that each is seen
/// to reach the records the voters hold.
fn plain() -> Submission {
    Submission::new(TaskDefinitionId::new("billing.charge"), 7, b"in".to_vec(), "billing")
        .with_retries(3)
        .with_delay(Duration::from_secs(2))
        .with_expiry(Duration::from_secs(600))
        .with_coalescing_key("charge-1")
        .ephemeral()
        .non_retriable()
}

fn reject_reason(response: &TaskResponse) -> Option<TaskRejectReason> {
    match &response.result {
        Some(task_response::Result::Reject(reject)) => TaskRejectReason::try_from(reject.reason).ok(),
        _ => None,
    }
}

/// How many of the shard's workers hold, as the newest run of `task`, one in
/// `state`.
fn holders_at(shard: &Voters, task: &TaskId, state: TaskRunState) -> usize {
    shard
        .nets
        .iter()
        .filter_map(|net| net.held_records().get(task))
        .filter(|record| record.runs.last().map(|run| run.state()) == Some(state))
        .count()
}

/// The majority of three, which an answer waits for.
const QUORUM: usize = 2;

/// Claims `task` for `client` through `leader`, returning the run.
async fn claim(shard: &mut Voters, client: &Net, leader: usize, task: &TaskId) -> TaskRunId {
    let claimed = shard
        .drive_until(client.request_claim(shard.id(leader), task.clone()))
        .await
        .expect("the leader answered");
    let Some(claim_response::Result::Accept(claim)) = claimed.result else {
        panic!("expected an accepted claim, got {claimed:?}");
    };
    claim.task_run_id.expect("a claim names its run").into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_submits_claims_starts_and_completes_a_task_through_the_leader() {
    within_deadline(async {
        let (mut shard, client) = ThreeVoters::start().await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);

        // The leader's roster does not hold the client yet: it is refused, and
        // nothing of the request takes effect. The delay is long enough for the
        // quorum write, the answer and the checks below to finish while the task
        // is still held back, even if the runtime stalls for a while.
        let submitted = mint(plain(), &Uuid7Ids, &RealClock::new());
        let refused = shard
            .drive_until(client.submit(leader_id.clone(), submitted.clone()))
            .await
            .expect("the leader answered");
        assert_eq!(reject_reason(&refused), Some(TaskRejectReason::TaskRejectNotMember));
        let task = submitted.task_id.clone();
        assert!(shard.with(leader, move |_, scheduler| scheduler.runs_of(&task).is_empty()).await);
        let refused = shard
            .drive_until(client.request_claim(leader_id.clone(), submitted.task_id.clone()))
            .await
            .expect("the leader answered");
        let Some(claim_response::Result::Reject(reject)) = refused.result else {
            panic!("expected the claim refused, got {refused:?}");
        };
        assert_eq!(reject.reason, ClaimRejectReason::ClaimRejectNotMember as i32);
        shard.join_as_pending(&client, leader).await;

        let answer = shard
            .drive_until(client.submit(leader_id.clone(), submitted.clone()))
            .await
            .expect("the leader answered");
        let Some(task_response::Result::Submitted(accepted)) = answer.result else {
            panic!("expected the submission accepted, got {answer:?}");
        };
        assert_eq!(accepted.task_id.map(TaskId::from), Some(submitted.task_id.clone()));
        let held = shard
            .nets
            .iter()
            .find_map(|net| net.held_records().get(&submitted.task_id))
            .expect("a majority stored the record before the leader answered");
        let task = held.task.expect("the record holds the task");
        let wanted = &submitted.submission;
        assert_eq!(task.task_definition_id.map(TaskDefinitionId::from), Some(wanted.definition_id.clone()));
        assert_eq!(task.source_version, wanted.source_version);
        assert_eq!(task.serialized_input, wanted.serialized_input);
        assert_eq!(task.queue, wanted.queue);
        assert_eq!(task.max_retries, wanted.retries);
        assert_eq!(task.coalescing_key, wanted.coalescing_key);
        assert_eq!(task.ephemeral, wanted.ephemeral);
        assert_eq!(task.non_retriable, wanted.non_retriable);
        assert_eq!(task.delay_millis, wanted.delay.map(|delay| delay.as_ticks()));
        assert_eq!(task.expiry_millis, wanted.expiry.map(|expiry| expiry.as_ticks()));
        assert_eq!(task.submitted_at, Some(submitted.submitted_at.into()));
        assert_eq!(
            holders_at(&shard, &submitted.task_id, TaskRunState::Queued),
            0,
            "the delay has not passed"
        );

        // The leader's driver releases the task when its delay passes, with no
        // one asking it to.
        let nets = shard.nets.clone();
        shard
            .drive_until(async {
                loop {
                    let queued = nets
                        .iter()
                        .filter_map(|net| net.held_records().get(&submitted.task_id))
                        .filter(|record| {
                            record.runs.last().map(|run| run.state()) == Some(TaskRunState::Queued)
                        })
                        .count();
                    if queued >= QUORUM {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await;

        let run = claim(&mut shard, &client, leader, &submitted.task_id).await;
        assert_eq!(
            client.claimed_runs().get(&run).map(|held| held.state),
            Some(HeldRun::Claimed),
            "an accepted claim enters the ledger"
        );

        let started = shard
            .drive_until(client.report_started(leader_id.clone(), run.clone()))
            .await
            .expect("the leader answered");
        assert!(matches!(started.result, Some(task_response::Result::Started(_))), "{started:?}");
        assert!(holders_at(&shard, &submitted.task_id, TaskRunState::Running) >= QUORUM);
        assert_eq!(
            client.claimed_runs().get(&run).map(|held| held.state),
            Some(HeldRun::Running),
            "an accepted start moves the run on in the ledger"
        );

        let completed = shard
            .drive_until(client.complete(leader_id, run.clone(), Digest::blake3(RESULT)))
            .await
            .expect("the leader answered");
        let Some(task_response::Result::Certified(certified)) = completed.result else {
            panic!("expected the result certified, got {completed:?}");
        };
        assert_eq!(
            certified.result_digest.as_ref().map(|digest| Digest::try_from(digest).expect("a digest")),
            Some(Digest::blake3(RESULT))
        );
        assert!(holders_at(&shard, &submitted.task_id, TaskRunState::Succeeded) >= QUORUM);
        assert_eq!(client.claimed_runs().get(&run), None, "a certified run leaves the ledger");
    })
    .await
}

//! A leader dies over real sockets; what its successor rebuilds is what the
//! shard's records and the survivors' answers say.
//!
//! The scenario after the death runs inside one drive of the shard: a
//! leader's reconciliation lives in its driver, so driving again would end it.

use std::sync::Arc;

use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_net::messenger::Net;

use crate::support::election::wait_until;
use crate::support::records::{
    RECONNECT_TIMEOUT_MS, SUSPECT_TIMEOUT_MS, Voters, claimed_and_started, plain_with,
    submitted_through,
};

/// How many voters hold, as the states of `task`'s runs in order, `states`.
fn holders_of(nets: &[Arc<Net>], task: &TaskId, states: &[TaskRunState]) -> usize {
    nets.iter()
        .filter_map(|net| net.held_records().get(task))
        .filter(|record| record.runs.iter().map(|run| run.state()).eq(states.iter().copied()))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_died_with_the_leader_is_lost_a_suspicion_and_a_reconnect_timeout_after_the_takeover_and_only_a_newest_generation_is_replayed(
) {
    let (mut shard, _client) = Voters::start(5).await;
    let first = shard.drive_until_a_leader().await;
    let worker = shard.others(first)[0];
    let (nets, ids) = (shard.nets.clone(), (0..5).map(|voter| shard.id(voter)).collect::<Vec<_>>());
    let (newest, stale, waiting) = shard
        .drive_until({
            let (nets, ids) = (nets.clone(), ids.clone());
            async move {
                let newest = submitted_through(
                    &nets[worker],
                    &ids[first],
                    plain_with(b"newest").with_coalescing_key("alone"),
                )
                .await;
                claimed_and_started(&nets[worker], &ids[first], &newest).await;
                let stale = submitted_through(
                    &nets[worker],
                    &ids[first],
                    plain_with(b"stale").with_coalescing_key("pair"),
                )
                .await;
                claimed_and_started(&nets[worker], &ids[first], &stale).await;
                // A newer generation of the stale one's key waits behind it.
                let waiting = submitted_through(
                    &nets[worker],
                    &ids[first],
                    plain_with(b"waiting").with_coalescing_key("pair"),
                )
                .await;
                wait_until(|| {
                    holders_of(&nets, &newest, &[TaskRunState::Running]) >= 3
                        && holders_of(&nets, &stale, &[TaskRunState::Running]) >= 3
                        && holders_of(&nets, &waiting, &[TaskRunState::Queued]) >= 3
                })
                .await;
                (newest, stale, waiting)
            }
        })
        .await;

    // The worker dies together with the leader, so the successor never hears
    // from it: it is in no roster the successor builds.
    shard.kill(first);
    shard.kill(worker);
    let watch = shard.watch();
    let (lost_after_takeover, stale_replayed, waiting_states) = shard
        .drive_until({
            let nets = nets.clone();
            async move {
                wait_until(|| {
                    (0..5).any(|voter| {
                        voter != first
                            && matches!(watch.state(voter), WorkerState::Leader | WorkerState::LeaderReconciling)
                    })
                })
                .await;
                let took_office = std::time::Instant::now();
                wait_until(|| {
                    holders_of(&nets, &newest, &[TaskRunState::Lost, TaskRunState::Queued]) >= 3
                        && holders_of(&nets, &stale, &[TaskRunState::Lost]) >= 3
                })
                .await;
                // Both losses were decided by the one call that lost the worker.
                (
                    took_office.elapsed(),
                    holders_of(&nets, &stale, &[TaskRunState::Lost, TaskRunState::Queued]),
                    holders_of(&nets, &waiting, &[TaskRunState::Queued]),
                )
            }
        })
        .await;

    let lost_after = std::time::Duration::from_millis(SUSPECT_TIMEOUT_MS + RECONNECT_TIMEOUT_MS);
    // Polling can be late in noticing the takeover, never early.
    assert!(
        lost_after_takeover + std::time::Duration::from_millis(200) >= lost_after,
        "its run was lost {lost_after_takeover:?} after the takeover, before a suspicion and a reconnect timeout"
    );
    assert_eq!(stale_replayed, 0, "a newer generation waits for the key: the lost stale one is not replayed");
    assert!(waiting_states >= 3, "the newer generation is untouched and still waits");
}

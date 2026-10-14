//! A leader asked to drain hands its leadership over: another voter leads
//! well within a suspicion timeout, the old leader leaves through it, and a
//! run claimed under the old leader certifies under the new one.

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::result_backend::PersistenceState;
use kabudachi_net::executor::Report;

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::executor::FakeExecutor;
use crate::support::records::{SUSPECT_TIMEOUT_MS, ThreeVoters, holding, plain_with, submitted_through};
use crate::support::worker::poll_until;

/// The majority of three, which every decision waits for.
const QUORUM: usize = 2;

fn holds_office(state: WorkerState) -> bool {
    matches!(state, WorkerState::LeaderReconciling | WorkerState::Leader)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_draining_leader_hands_over_within_a_roll_call_and_its_successor_certifies_the_runs() {
    within_deadline(async {
        let (mut shard, _client) = ThreeVoters::start().await;
        let mut executors: Vec<FakeExecutor> = (0..3)
            .map(|voter| {
                let (executor, endpoint) = FakeExecutor::new();
                shard.set_executor(voter, endpoint);
                executor
            })
            .collect();
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        let followers = shard.others(leader);
        let runner = followers[0];
        let nets = shard.nets.clone();
        let watch = shard.watch();
        let handed_off = shard.handed_off();

        // A follower runs a task under the old leader, and has not finished it.
        executors[runner].grant(1);
        let task = shard
            .drive_until(submitted_through(&nets[runner], &leader_id, plain_with(b"handed over")))
            .await;
        let (run, _) = shard.drive_until(executors[runner].next_claim()).await;
        executors[runner].report(Report::Started { run: run.clone(), execution_version: None });
        shard
            .drive_until(poll_until("the old leader stored the run running", || {
                holding(&nets, &task, &[TaskRunState::Running]) >= QUORUM
            }))
            .await;
        // Both followers have crawled their routing, which their next
        // heartbeats report to the leader, so it hands over as soon as it is
        // asked. Read from the watch: asking a node through `with` restarts
        // its driver, which would keep its routing from ever settling.
        shard
            .drive_until(wait_until(|| followers.iter().all(|follower| watch.routing_crawled(*follower))))
            .await;

        let asked_at = StdInstant::now();
        nets[leader].request_drain();
        let successor = shard
            .drive_until(async {
                wait_until(|| followers.iter().any(|follower| holds_office(watch.state(*follower)))).await;
                *followers
                    .iter()
                    .find(|follower| holds_office(watch.state(**follower)))
                    .expect("a follower holds office")
            })
            .await;
        let took_over_in = asked_at.elapsed();
        // Without a hand-over no follower even suspects before a suspicion
        // timeout has passed since the leader's last ack, which is at most a
        // heartbeat interval before the drain. The bound leaves an eighth of
        // that timeout for the successor's roll call, which waits out its
        // deadline when a reply comes late on a loaded host.
        assert!(
            took_over_in < StdDuration::from_millis(SUSPECT_TIMEOUT_MS * 7 / 8),
            "a follower took office {took_over_in:?} after the drain"
        );

        let handed = shard.drive_until(handed_off.returned(leader)).await;
        assert!(handed.abandoned.is_empty(), "{handed:?}");
        assert_eq!(watch.state(leader), WorkerState::Stopped);

        executors[runner].report(Report::Completed {
            run,
            digest: Digest::blake3(b"done"),
            delivered: false,
            continuation: None,
            persistence: PersistenceState::Disabled,
            result_manifest: None,
        });
        shard
            .drive_until(poll_until("the successor certified the run", || {
                holding(&nets, &task, &[TaskRunState::Succeeded]) >= QUORUM
            }))
            .await;
        // The successor drops the old leader from its roster.
        while shard.with(successor, |node, _| node.voters().len()).await != 2 {
            shard.drive_until(tokio::time::sleep(StdDuration::from_millis(20))).await;
        }
    })
    .await;
}

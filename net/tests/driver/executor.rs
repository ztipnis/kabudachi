//! A worker's driver and its executor over real sockets: the driver claims
//! nothing for its executor until a leader has vouched for hearing the
//! worker, and tells the executor to abort its runs before any other leader
//! can replay them.

use std::time::Duration as StdDuration;

use kabudachi_core::election::{ElectionTimings, Entry, Identity, Input, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId};
use kabudachi_core::protocol::messages::election_message;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::{DriverConfig, run_driver};
use tokio::sync::watch;

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::net::{driven_scheduler, listening_net, pointer_to, wait_until_registered};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_claims_nothing_for_its_executor_before_a_leader_vouches_for_hearing_it() {
    within_deadline(async {
        let clock = RealClock::new();
        // A leader that hears the joiner's heartbeats and never acks them.
        let (leader_net, leader_address) = listening_net().await;
        let leader = leader_net.local_worker_id();
        let (joiner_net, _) = listening_net().await;
        joiner_net.dial(leader_address.clone());
        wait_until_registered(&joiner_net, &leader).await;
        let joiner = joiner_net.local_worker_id();
        let (mut node, first) = WorkerNode::start(
            Identity {
                id: joiner.clone(),
                incarnation: IncarnationId::new("joiner-incarnation-0"),
                shard: ShardId::new("shard-1"),
                timings: ElectionTimings::new(
                    Duration::from_millis(2_000),
                    Duration::from_millis(100),
                )
                .with_roll_call_deadline(Duration::from_millis(100)),
            },
            Entry::Joining(pointer_to(&leader, &leader_address)),
            clock,
            None,
        );
        let mut scheduler = driven_scheduler(clock);
        let (mut executor, mut endpoint) = FakeExecutor::new();
        executor.grant(1);
        let (seen, watched) = watch::channel((None, false));
        let driven = run_driver(
            &mut node,
            first,
            &joiner_net,
            &mut scheduler,
            clock,
            None,
            Some(&mut endpoint),
            DriverConfig::default(),
            move |node, _, _| {
                let leader = node.known_leader().map(|(leader, _)| leader);
                let _ = seen.send((leader, node.has_contact_floor()));
            },
        );
        tokio::select! {
            _ = driven => unreachable!("this test never drains the joiner, so its driver never returns"),
            () = async {
                // Three heartbeats: all the while the joiner follows the
                // leader it was pointed at, with room to run a task.
                let mut heartbeats = 0;
                while heartbeats < 3 {
                    for input in leader_net.take_inputs() {
                        if let Input::Message { from, message } = input
                            && from == joiner
                            && matches!(
                                message.message().payload,
                                Some(election_message::Payload::Heartbeat(_))
                            )
                        {
                            heartbeats += 1;
                        }
                    }
                    leader_net.wait_for_arrival().await;
                }
                assert_eq!(
                    *watched.borrow(),
                    (Some(leader.clone()), false),
                    "the joiner follows a leader that has not vouched for hearing it"
                );
                assert!(
                    leader_net.poll_claim_requests().is_empty(),
                    "the joiner asked its leader for work"
                );
                executor.expect_no_work_for(StdDuration::from_millis(50)).await;
            } => {}
        }
    })
    .await
}

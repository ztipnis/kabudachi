//! Scaffolding for the result transport: a result delivered to a peer and
//! to this worker itself arrives and is acknowledged; one sent to a peer
//! that refuses the connection is not.

use std::time::Duration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::protocol::messages::DeliverResult;
use kabudachi_net::result_transport::{DeliveryError, ResultEvent};

use crate::support::deadline::within_deadline;
use crate::support::net::{connect_to, listening_net};

fn delivery(text: &[u8]) -> DeliverResult {
    DeliverResult {
        task_id: Some(TaskId::new("t").into()),
        task_run_id: Some(TaskRunId::new("r").into()),
        result_digest: Some(Digest::blake3(text).into()),
        payload: text.to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scaffold_a_result_reaches_its_client_and_one_cut_off_is_not_acknowledged() {
    within_deadline(async {
        let (runner, _) = listening_net().await;
        let (client, client_at) = listening_net().await;
        connect_to(&client, &client_at, &runner).await;

        let taking = async {
            loop {
                client.result_arrived().await;
                let events = client.take_result_events();
                if !events.is_empty() {
                    return events;
                }
            }
        };
        let (delivered, events) = tokio::join!(
            runner.deliver_result(
                client.local_worker_id(),
                delivery(b"remote"),
                Duration::from_secs(5)
            ),
            taking
        );
        assert_eq!(delivered, Ok(()));
        assert!(
            matches!(&events[..], [ResultEvent::Delivered { payload, .. }] if payload == b"remote")
        );

        assert_eq!(
            runner
                .deliver_result(
                    runner.local_worker_id(),
                    delivery(b"own"),
                    Duration::from_secs(1)
                )
                .await,
            Ok(())
        );
        assert!(
            matches!(&runner.take_result_events()[..], [ResultEvent::Delivered { payload, .. }] if payload == b"own")
        );

        client.block_peer(runner.local_worker_id());
        assert_eq!(
            runner
                .deliver_result(
                    client.local_worker_id(),
                    delivery(b"cut"),
                    Duration::from_millis(400)
                )
                .await,
            Err(DeliveryError::Unreached)
        );
    })
    .await;
}

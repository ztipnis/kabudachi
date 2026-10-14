//! A bare `Net`'s input queue while no driver takes from it: past its
//! limit the oldest election message makes room for the newest, and a
//! connection that opened and closed before either event was taken leaves
//! no event behind.

use kabudachi_core::election::Input;
use kabudachi_core::protocol::checked;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, SelfRemove, election_message};
use kabudachi_net::messenger::Net;

use crate::support::deadline::within_deadline;
use crate::support::net::{connect_to, listening_net, wait_for_diagnostics};

/// A well-formed message from `worker`, told apart by `term_seen`.
fn self_remove(worker: &WorkerId, term_seen: u64) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::SelfRemove(SelfRemove {
            worker_id: Some(worker.clone().into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            shard_id: Some(ShardId::new("shard-1").into()),
            configuration_generation: None,
            term_seen,
            leader_term: 0,
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_untaken_input_queue_keeps_the_newest_message_and_forgets_a_connection_that_came_and_went() {
    within_deadline(async {
        let (receiver, receiver_at) = listening_net().await;
        let receiver = receiver.with_input_limit(2);
        let sender = Net::new();
        connect_to(&receiver, &receiver_at, &sender).await;
        let (from, to) = (sender.local_worker_id(), receiver.local_worker_id());

        // Queued on the receiver and never taken: the sender's connection,
        // then three messages, each waited for so they arrive in order. A
        // limit of two keeps the connection and the last message to arrive.
        for term_seen in 1..=3 {
            sender.send(to.clone(), self_remove(&from, term_seen));
            wait_for_diagnostics(&receiver, "the message arrived", |diagnostics| {
                diagnostics.traffic.messages_received == term_seen
            })
            .await;
        }
        // The disconnect cancels the connect still queued.
        receiver.block_peer(from.clone());
        wait_for_diagnostics(&receiver, "the sender's connection closed", |diagnostics| {
            !diagnostics.connected.contains(&from)
        })
        .await;

        // The swarm's own routing crawls may queue `RoutingCrawled`, which
        // the limit never drops; it says nothing of this queue's bound.
        let inputs: Vec<Input> = receiver
            .take_inputs()
            .into_iter()
            .filter(|input| *input != Input::RoutingCrawled)
            .collect();
        let newest = checked::decode(self_remove(&from, 3));
        assert!(
            matches!(&inputs[..], [Input::Message { from: sender, message }]
                if *sender == from && Ok(message.clone()) == newest),
            "only the newest message left and no connection event: {inputs:?}"
        );
    })
    .await;
}

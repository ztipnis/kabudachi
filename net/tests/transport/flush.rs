//! A message sent just before a `Net` goes away reaches its peer once the
//! sender has flushed.

use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, SelfRemove, election_message};
use kabudachi_net::messenger::Net;

use crate::support::deadline::within_deadline;
use crate::support::net::{connect_to, listening_net, wait_for_diagnostics};

fn self_remove(worker: &WorkerId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::SelfRemove(SelfRemove {
            worker_id: Some(worker.clone().into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            shard_id: Some(ShardId::new("shard-1").into()),
            configuration_generation: None,
            term_seen: 1,
            leader_term: 0,
        })),
    }
}

/// On a single-threaded runtime the sender's swarm task cannot run between
/// `send` and the drop unless the sender awaits something, so only a flush
/// can get the message onto the wire.
#[tokio::test]
async fn a_message_sent_before_the_sender_is_dropped_arrives_once_the_sender_flushed() {
    within_deadline(async {
        let (receiver, receiver_at) = listening_net().await;
        let sender = Net::new();
        connect_to(&receiver, &receiver_at, &sender).await;
        let from = sender.local_worker_id();

        sender.send(receiver.local_worker_id(), self_remove(&from));
        sender.flush_sent().await;
        drop(sender);

        wait_for_diagnostics(&receiver, "the message arrived", |diagnostics| {
            diagnostics.traffic.messages_received == 1
        })
        .await;
    })
    .await;
}

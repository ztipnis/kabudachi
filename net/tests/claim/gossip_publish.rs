//! A `Net::publish` over real sockets reaches every other worker subscribed
//! to the publisher's shard, as a message from the publisher, whether or not
//! the worker is connected to the publisher, and never reaches a worker
//! subscribed to another shard.


use std::time::Duration as StdDuration;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, SelfRemove, election_message};
use kabudachi_net::messenger::Net;
use tokio::time::timeout;

use crate::support::net::{connect_to, wait_until_subscribed};

const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn new_net() -> Net {
    Net::new()
}

/// A well-formed message naming `worker` in `shard`.
fn self_remove(worker: &WorkerId, shard: &ShardId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::SelfRemove(SelfRemove {
            worker_id: Some(worker.clone().into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            shard_id: Some(shard.clone().into()),
            configuration_generation: None,
            term_seen: 0,
            leader_term: 0,
        })),
    }
}

/// Every message among `inputs`, with its sender, in order.
fn messages(inputs: Vec<Input>) -> Vec<(WorkerId, ElectionMessage)> {
    inputs
        .into_iter()
        .filter_map(|input| match input {
            Input::Message { from, message } => Some((from, message.into_message())),
            _ => None,
        })
        .collect()
}

/// Takes `net`'s queued inputs until at least one message has arrived, and
/// returns the messages among them. Every other input is dropped: `net` must
/// be a bare `Net`.
async fn take_messages(net: &Net) -> Vec<(WorkerId, ElectionMessage)> {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let arrived = messages(net.take_inputs());
            if !arrived.is_empty() {
                return arrived;
            }
            net.wait_for_arrival().await;
        }
    })
    .await
    .expect("a message arrived within the timeout")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_reaches_the_shards_subscribers_as_its_authors_message_even_through_a_relay() {
    // A line, author - relay - far, with a worker of another shard on the
    // relay: the far worker is not connected to the author, so the publish
    // can reach it only through the relay, and the relay would pass it to the
    // outsider too if shard scoping failed.
    let [author, relay, far, outsider] = [new_net(), new_net(), new_net(), new_net()];
    let relay_addr = timeout(
        WAIT_TIMEOUT,
        relay.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("the relay produced a listen address within the timeout");
    for spoke in [&author, &far, &outsider] {
        connect_to(&relay, &relay_addr, spoke).await;
    }
    let shard = ShardId::new("shard-1");
    for net in [&author, &relay, &far] {
        net.subscribe_to_shard(&shard);
    }
    outsider.subscribe_to_shard(&ShardId::new("shard-2"));
    wait_until_subscribed(&author, &[&relay.local_worker_id()]).await;
    wait_until_subscribed(&relay, &[&author.local_worker_id(), &far.local_worker_id()]).await;
    wait_until_subscribed(&far, &[&relay.local_worker_id()]).await;

    let message = self_remove(&author.local_worker_id(), &shard);
    author.publish(message.clone());

    let expected = vec![(author.local_worker_id(), message)];
    assert_eq!(take_messages(&relay).await, expected);
    assert_eq!(take_messages(&far).await, expected);
    // The relay forwards to the outsider, if it ever would, about as soon as
    // to `far`; give those inputs a moment to land before asserting none did.
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(
        messages(outsider.take_inputs()),
        vec![],
        "a worker of another shard received the publish"
    );
    assert!(
        !far.diagnostics()
            .await
            .peer_addresses
            .contains_key(&author.local_worker_id()),
        "the far worker never connected to the author, so the relay passed the publish on"
    );
}

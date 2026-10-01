//! A `Net::publish` over real sockets reaches every other worker subscribed
//! to the publisher's shard exactly once, as a message from the publisher,
//! and never reaches a worker subscribed to another shard.


use std::time::Duration as StdDuration;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, SelfRemove, election_message};
use kabudachi_net::messenger::Net;
use tokio::time::timeout;

use crate::support::net::{connect_full_mesh, connect_to, wait_until_subscribed};

const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// How long a test waits, after the expected copies of a publish arrived,
/// for an unexpected one. Gossipsub also passes a message on through every
/// subscriber that receives it, and advertises it again at its heartbeat
/// (every second by default), so a second copy, or a copy to a worker that
/// must not get one, would come within this.
const QUIET_PERIOD: StdDuration = StdDuration::from_secs(3);

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
async fn a_publish_reaches_each_other_subscriber_of_the_shard_once_as_the_publishers_message() {
    // `nets[3]` is in the mesh but subscribed to another shard: it must
    // receive nothing published on this one.
    let nets = [new_net(), new_net(), new_net(), new_net()];
    let ids = connect_full_mesh(&nets.iter().collect::<Vec<_>>()).await;
    let shard = ShardId::new("shard-1");
    for net in &nets[..3] {
        net.subscribe_to_shard(&shard);
    }
    nets[3].subscribe_to_shard(&ShardId::new("shard-2"));
    for (i, net) in nets[..3].iter().enumerate() {
        let others: Vec<&WorkerId> = ids[..3].iter().filter(|id| **id != ids[i]).collect();
        wait_until_subscribed(net, &others).await;
    }

    let message = self_remove(&ids[0], &shard);
    nets[0].publish(message.clone());

    let expected = vec![(ids[0].clone(), message)];
    assert_eq!(take_messages(&nets[1]).await, expected);
    assert_eq!(take_messages(&nets[2]).await, expected);
    tokio::time::sleep(QUIET_PERIOD).await;
    for (net, id) in nets.iter().zip(&ids) {
        assert_eq!(
            messages(net.take_inputs()),
            vec![],
            "{id:?} received a message beyond one copy for each other subscriber"
        );
    }
    assert!(
        nets[3].diagnostics().await.shard_subscribers.is_empty(),
        "no peer of the shard-2 worker shares its shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_relayed_by_another_worker_arrives_as_its_authors_message() {
    // A line, author - relay - far: the far worker is not connected to the
    // author, so the publish can reach it only through the relay.
    let [author, relay, far] = [new_net(), new_net(), new_net()];
    let relay_addr = timeout(
        WAIT_TIMEOUT,
        relay.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("the relay produced a listen address within the timeout");
    connect_to(&relay, &relay_addr, &author).await;
    connect_to(&relay, &relay_addr, &far).await;
    let shard = ShardId::new("shard-1");
    for net in [&author, &relay, &far] {
        net.subscribe_to_shard(&shard);
    }
    wait_until_subscribed(&author, &[&relay.local_worker_id()]).await;
    wait_until_subscribed(&relay, &[&author.local_worker_id(), &far.local_worker_id()]).await;
    wait_until_subscribed(&far, &[&relay.local_worker_id()]).await;

    let message = self_remove(&author.local_worker_id(), &shard);
    author.publish(message.clone());

    assert_eq!(
        take_messages(&far).await,
        vec![(author.local_worker_id(), message)]
    );
    assert!(
        !far
            .diagnostics()
            .await
            .peer_addresses
            .contains_key(&author.local_worker_id()),
        "the far worker never connected to the author, so the relay passed the publish on"
    );
}

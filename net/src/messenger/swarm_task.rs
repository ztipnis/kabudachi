//! The task that owns the swarm and the peer book (see the module doc of
//! `crate::messenger`'s "Synchronization"): it applies `Net`'s commands,
//! turns swarm events into the inputs and requests `Inbound` queues, and
//! tells the peer book what it sees. One function per command and per
//! swarm event; the record queries live in `record_queries`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::checked;
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
use libp2p::core::ConnectedPoint;
use libp2p::core::transport::ListenerId;
use libp2p::futures::StreamExt;
use libp2p::request_response;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm, gossipsub, identify, kad};
use prost::Message as _;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::record_queries::RecordQueries;
use super::{Command, Correlated, DialTarget, Exchanges, Inbound};
use crate::claim::codec::ClaimCodec;
use crate::codec::Ack;
use crate::framing::decode_election;
use crate::join_codec::JoinCodec;
use crate::peers::{Carried, Observation, Peers, Side, is_dialable_listen_addr, worker_id_of};
use crate::reconcile::codec::ReconcileCodec;
use crate::steal::codec::StealCodec;
use crate::swarm::{Behaviour, BehaviourEvent, hide_listen_addresses};
use crate::task_exchange::codec::TaskCodec;

/// The input an arriving gossip `message` is for this worker's node: a
/// message from its author (see the module doc's "Gossip"). `None`, and the
/// message dropped, when it names no author or its body is not a well-formed
/// `ElectionMessage`.
fn gossip_input(message: gossipsub::Message) -> Option<Input> {
    let Some(author) = message.source else {
        tracing::debug!(topic = %message.topic, "dropping a gossip message with no author");
        return None;
    };
    match decode_election(&message.data) {
        Ok(election_message) => Some(Input::Message {
            from: worker_id_of(&author),
            message: election_message,
        }),
        Err(error) => {
            tracing::debug!(
                %author,
                %error,
                "dropping a gossip message that is not a well-formed election message"
            );
            None
        }
    }
}

/// Writes `own` into `message` as its sender's address, if `message` is a
/// roll call or a roll call reply: the two messages whose recipient may hold
/// no connection to the sender and still has to answer it directly (see the
/// module doc's "Where a peer's address comes from"). Every other message is
/// left as it is.
fn stamp_own_address(message: &mut ElectionMessage, own: &Multiaddr) {
    match &mut message.payload {
        Some(election_message::Payload::RollCall(call)) => {
            call.initiator_address = own.to_string();
        }
        Some(election_message::Payload::RollCallReply(reply)) => {
            reply.responder_address = own.to_string();
        }
        _ => {}
    }
}

/// The requests `drive` has made of the swarm on a caller's behalf and not
/// yet answered, each keyed by what the swarm reports its outcome under.
#[derive(Default)]
struct Pending {
    listens: HashMap<ListenerId, oneshot::Sender<Multiaddr>>,
    exchanges: Exchanges,
    dials: HashMap<ConnectionId, PendingDial>,
    /// The shard's record writes, hand-offs and lookups in flight.
    records: RecordQueries,
    /// Bootstrap queries that have had a failed step, until their last step.
    crawls: CrawlTracker,
}

/// A `Command::DialForConnection` in flight.
struct PendingDial {
    /// The address whoever answers is recorded at, for an
    /// [`DialTarget::Address`] dial.
    asked_at: Option<Multiaddr>,
    respond_to: oneshot::Sender<Option<PeerId>>,
}

impl PendingDial {
    /// Ends the dial: tells the peer book what it found (for a dial of
    /// whoever answers at an address) and answers the caller, who may have
    /// stopped waiting.
    fn finish(self, outcome: Option<PeerId>, peers: &mut Peers, now: Instant) {
        if let Some(address) = self.asked_at {
            peers.observe(Observation::Dialed { address, outcome }, now);
        }
        let _ = self.respond_to.send(outcome);
    }
}

/// What the swarm task's peer book has to learn from the swarm itself after
/// each event or command: gossipsub raises no event for GRAFT/PRUNE, so the
/// mesh and shard subscribers are read, not observed.
fn gossip_of(swarm: &Swarm<Behaviour>) -> Observation<'static> {
    let gossipsub = &swarm.behaviour().gossipsub;
    let my_topics: BTreeSet<&gossipsub::TopicHash> = gossipsub.topics().collect();
    Observation::Gossip {
        mesh: gossipsub.all_mesh_peers().copied().collect(),
        subscribers: gossipsub
            .all_peers()
            .filter(|(_, topics)| topics.iter().any(|topic| my_topics.contains(topic)))
            .map(|(peer, _)| *peer)
            .collect(),
    }
}

/// Owns `swarm` and `peers` exclusively and polls the swarm for ever,
/// applying commands, queueing what arrives on `inbound`, and telling `peers`
/// what it sees. Each loop iteration reads the clock once, and every
/// observation of that iteration carries the reading.
pub(super) async fn drive(
    mut swarm: Swarm<Behaviour>,
    identify_config: identify::Config,
    mut commands: mpsc::UnboundedReceiver<Command>,
    inbound: Arc<Inbound>,
    mut peers: Peers,
) {
    let mut pending = Pending::default();
    let mut redial_ticker = tokio::time::interval(peers.redial_check_interval());
    redial_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let now = tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    return; // Every Net handle for this swarm was dropped.
                };
                let now = Instant::now();
                handle_command(&mut swarm, &identify_config, command, &mut pending, &inbound, &mut peers, now);
                now
            }
            event = swarm.select_next_some() => {
                let now = Instant::now();
                handle_event(&mut swarm, event, &mut pending, &inbound, &mut peers, now);
                now
            }
            _ = redial_ticker.tick() => {
                let now = Instant::now();
                // Whether a redial worked shows as the connection opening,
                // which ends the redial.
                for (peer, addr) in peers.redials_due(now) {
                    // Also asks kad's routing table for an address of its
                    // own, as `crate::join` does for a leader: the last-known
                    // address is not the only one that might still reach
                    // `peer`.
                    let _ = swarm.dial(
                        DialOpts::peer_id(peer)
                            .addresses(vec![addr])
                            .extend_addresses_through_behaviour()
                            .build(),
                    );
                }
                now
            }
        };
        // After the event's own observations: a connection that closed must be
        // judged against the mesh as of before this read, or gossipsub having
        // already dropped the peer from it would keep an organic drop from
        // ever being redialed.
        peers.observe(gossip_of(&swarm), now);
    }
}

fn handle_command(
    swarm: &mut Swarm<Behaviour>,
    identify_config: &identify::Config,
    command: Command,
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
    now: Instant,
) {
    match command {
        Command::Send { to, mut message } => {
            if let Some(own) = peers.own_address() {
                stamp_own_address(&mut message, &own);
            }
            peers.observe(Observation::Carried(Carried::Sent), now);
            // Over a connection to `to` if there is one; else libp2p dials
            // `to` at this address of record (one stamped on a roll call
            // from a peer this node never connected to, say), along with any
            // its behaviours know.
            let address = peers.dialable_address(&to);
            swarm
                .behaviour_mut()
                .request_response
                .send_request_with_addresses(&to, message, address.into_iter().collect());
        }
        Command::Subscribe { topic } => {
            if let Err(error) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                tracing::warn!(
                    %topic,
                    %error,
                    "could not subscribe to the shard's gossip topic"
                );
            }
        }
        Command::Publish { topic, mut message } => {
            if let Some(own) = peers.own_address() {
                stamp_own_address(&mut message, &own);
            }
            peers.observe(Observation::Carried(Carried::Published), now);
            // Lost like any other unreliable message (see the module doc's
            // "Gossip").
            if let Err(error) = swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), message.encode_to_vec())
            {
                tracing::debug!(%topic, %error, "dropping a gossip publish");
            }
        }
        Command::Dial { addr } => {
            let _ = swarm.dial(addr);
        }
        Command::RefreshPeerRouting => {
            // `NoKnownPeers`: nothing to crawl from yet; the next refresh
            // tries again.
            let _ = swarm.behaviour_mut().kad.bootstrap();
        }
        Command::Block { peer } => {
            swarm.behaviour_mut().blocked.block_peer(peer);
        }
        Command::Unblock { peer } => {
            swarm.behaviour_mut().blocked.unblock_peer(peer);
        }
        Command::DialForConnection { target, respond_to } => {
            let (opts, asked_at) = match target {
                DialTarget::Address(address) => (
                    DialOpts::unknown_peer_id().address(address.clone()).build(),
                    Some(address),
                ),
                DialTarget::Peer { peer, address } => (
                    DialOpts::peer_id(peer)
                        .addresses(vec![address])
                        .extend_addresses_through_behaviour()
                        .build(),
                    None,
                ),
            };
            let connection_id = opts.connection_id();
            let dial = PendingDial {
                asked_at,
                respond_to,
            };
            match swarm.dial(opts) {
                Ok(()) => {
                    pending.dials.insert(connection_id, dial);
                }
                // Swarm::dial can fail synchronously (e.g. no addresses
                // survive filtering) without ever producing a SwarmEvent for
                // this connection_id — answer inline rather than leaving the
                // caller to wait out the full timeout.
                Err(_) => dial.finish(None, peers, now),
            }
        }
        Command::ListenOn { addr, respond_to } => {
            if let Ok(listener_id) = swarm.listen_on(addr) {
                pending.listens.insert(listener_id, respond_to);
            }
            // On error, dropping `respond_to` closes the channel;
            // `Net::listen_on`'s awaiter observes that as a panic with a
            // message pointing at the cause.
        }
        Command::SetExternalAddress { address } => {
            hide_listen_addresses(swarm, identify_config);
            swarm.add_external_address(address.clone());
            peers.observe(Observation::ExternalAddress(address), now);
        }
        Command::Exchange(run) => run(swarm, &mut pending.exchanges),
        Command::WithPeers(read) => read(peers),
        Command::WriteRecords(writes) => pending.records.start_writes(swarm, writes, inbound),
        Command::HandOff { write, respond_to } => pending.records.start_hand_off(swarm, write, respond_to),
        Command::GetRecord { task, respond_to } => pending.records.start_read(swarm, task, respond_to),
    }
}

/// The bootstrap queries that have had a failed step so far.
///
/// A bootstrap is several steps (the lookup of the node's own key, then
/// bucket refreshes), and a step may time out while a later one succeeds. The
/// last step's result alone would then vouch for a crawl that missed part of
/// the routing table, so a failed earlier step voids the query's report.
#[derive(Default)]
struct CrawlTracker {
    failed: HashSet<kad::QueryId>,
}

impl CrawlTracker {
    /// Notes one step of bootstrap query `id`; on its last step, what the
    /// finished crawl tells the node: a crawl that failed anywhere reached
    /// nothing it can vouch for, so it reports none.
    fn step(
        &mut self,
        id: kad::QueryId,
        result: &kad::BootstrapResult,
        step: &kad::ProgressStep,
    ) -> Option<Input> {
        if !step.last {
            if result.is_err() {
                self.failed.insert(id);
            }
            return None;
        }
        let failed = self.failed.remove(&id);
        (result.is_ok() && !failed).then_some(Input::RoutingCrawled)
    }
}

fn handle_event(
    swarm: &mut Swarm<Behaviour>,
    event: SwarmEvent<BehaviourEvent>,
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
    now: Instant,
) {
    match event {
        SwarmEvent::NewListenAddr {
            listener_id,
            address,
        } => {
            // A node listening on a wildcard bind gets one of these per
            // interface, in no set order, loopback among them: the peer book
            // keeps the best (see `crate::peers`).
            peers.observe(Observation::ListeningOn(address.clone()), now);
            if let Some(respond_to) = pending.listens.remove(&listener_id) {
                let _ = respond_to.send(address);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
            result: kad::QueryResult::Bootstrap(result),
            id,
            step,
            ..
        })) => {
            if let Some(input) = pending.crawls.step(id, &result, &step) {
                inbound.queue_input(input);
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            ..
        } => {
            if num_established.get() == 1 {
                inbound.queue_input(Input::PeerConnected(worker_id_of(&peer_id)));
            }
            // The two sides of a connection observe different addresses here
            // (see `crate::peers`'s "Where a peer's address comes from"), so
            // the peer book is told which end this node is.
            let side = match endpoint {
                ConnectedPoint::Dialer { .. } => Side::Dialer,
                ConnectedPoint::Listener { .. } => Side::Listener,
            };
            peers.observe(
                Observation::ConnectionOpened {
                    peer: peer_id,
                    side,
                    address: endpoint.get_remote_address().clone(),
                },
                now,
            );
            // Only ever resolves `Net::dial_for_connection`'s own oneshot
            // for *this* connection_id — see the module doc and that
            // method's doc comment for why matching on ConnectionId (rather
            // than diffing the connected-peer set) is what makes a late
            // connection from an abandoned dial harmless: if the caller
            // already stopped awaiting this entry (timeout elapsed, cascade
            // moved on), answering it just fails silently instead of
            // resolving some other, unrelated await.
            if let Some(dial) = pending.dials.remove(&connection_id) {
                dial.finish(Some(peer_id), peers, now);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
            peer_id,
            info,
            ..
        })) => {
            // The whole point of having `identify` in the swarm:
            // `info.listen_addrs` is the peer's own advertised
            // listen address, which is what a joining node needs to dial —
            // unlike either endpoint-derived address above. See
            // `crate::peers`'s "Where a peer's address comes from" for the
            // ranking and for which of the advertised addresses is taken.
            //
            // Every dialable one, not just the one taken below, feeds `kad`
            // (see `crate::swarm`'s "kad: peer routing, not membership"):
            // Kademlia keeps several addresses per peer, and it is the peer
            // routing this crate wants from it, not this node's own
            // one-address-of-record bookkeeping.
            // Only a peer that speaks this shard's records protocol is a
            // routing candidate for the records kad: another shard's peer
            // cannot answer it, and would only take up its buckets.
            let speaks_records = swarm.behaviour().records.as_ref().is_some_and(|records| {
                records
                    .protocol_names()
                    .iter()
                    .any(|name| info.protocols.contains(name))
            });
            for addr in info
                .listen_addrs
                .iter()
                .filter(|addr| is_dialable_listen_addr(addr))
            {
                swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
                // The shard's records kad learns them the same way: a peer
                // that only dialed this node has no address there otherwise
                // (an inbound connection carries none), so a lookup would
                // see no one but this node's own store.
                if speaks_records && let Some(records) = swarm.behaviour_mut().records.as_mut() {
                    records.add_address(&peer_id, addr.clone());
                }
            }
            peers.observe(
                Observation::Identified {
                    peer: peer_id,
                    listen_addresses: info.listen_addrs,
                },
                now,
            );
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            num_established: 0,
            ..
        } => {
            peers.observe(Observation::ConnectionClosed(peer_id), now);
            inbound.queue_input(Input::PeerDisconnected(worker_id_of(&peer_id)));
        }
        SwarmEvent::OutgoingConnectionError {
            connection_id,
            peer_id,
            ..
        } => {
            if let Some(dial) = pending.dials.remove(&connection_id) {
                dial.finish(None, peers, now);
            }
            // A dial that names its peer and fails while no other connection
            // to that peer is open: the peer cannot be reached.
            if let Some(peer) = peer_id
                && !swarm.is_connected(&peer)
            {
                inbound.queue_input(Input::PeerDisconnected(worker_id_of(&peer)));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        })) => {
            // The codec already refused a malformed request, so this decode
            // does not fail; a message it did refuse would be dropped.
            match checked::decode(request) {
                Ok(message) => {
                    let input = Input::Message {
                        from: worker_id_of(&peer),
                        message,
                    };
                    // Recorded before the node is told, so a direct answer
                    // the node sends finds the address on file.
                    peers.observe(Observation::MessageArrived(&input), now);
                    inbound.queue_input(input);
                }
                Err(error) => {
                    tracing::debug!(%peer, %error, "dropping a direct message that is not a well-formed election message");
                }
            }
            // Best-effort: nothing reads this ack back (see module doc), and
            // a channel that is already closed just means the peer stopped
            // waiting on it.
            let _ = swarm
                .behaviour_mut()
                .request_response
                .send_response(channel, Ack);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) => {
            if let Some(input) = gossip_input(message) {
                // Same ordering as for a direct message above.
                peers.observe(Observation::MessageArrived(&input), now);
                inbound.queue_input(input);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Records(event)) => pending.records.on_event(event, inbound),
        SwarmEvent::Behaviour(BehaviourEvent::Join(event)) => {
            settle::<JoinCodec>(&mut pending.exchanges, event, inbound, peers, now);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Claim(event)) => {
            settle::<ClaimCodec>(&mut pending.exchanges, event, inbound, peers, now);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Task(event)) => {
            settle::<TaskCodec>(&mut pending.exchanges, event, inbound, peers, now);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Reconcile(event)) => {
            settle::<ReconcileCodec>(&mut pending.exchanges, event, inbound, peers, now);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Steal(event)) => {
            settle::<StealCodec>(&mut pending.exchanges, event, inbound, peers, now);
        }
        _ => {}
    }
}

/// Settles what `event` of protocol `C` settles (see `Exchange::on_event`),
/// and queues an arriving request for the driver, counting it in the peer
/// book's traffic.
fn settle<C: Correlated>(
    exchanges: &mut Exchanges,
    event: request_response::Event<C::Request, C::Response>,
    inbound: &Inbound,
    peers: &mut Peers,
    now: Instant,
) {
    if let Some(asked) = C::exchange(exchanges).on_event(event) {
        peers.observe(Observation::Carried(C::ARRIVAL), now);
        inbound.queue_asked(C::queue(inbound), asked);
    }
}

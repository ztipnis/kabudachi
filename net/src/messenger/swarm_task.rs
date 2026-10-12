//! The task that owns the swarm and the peer book (see the module doc of
//! `crate::messenger`'s "Synchronization"): it applies `Net`'s commands,
//! turns swarm events into the inputs and requests `Inbound` queues, and
//! tells the peer book what it sees. One function per command and per
//! swarm event; the record queries live in `record_queries`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use kabudachi_core::blob::BlobKey;
use kabudachi_core::election::Input;
use kabudachi_core::protocol::checked;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
use libp2p::core::ConnectedPoint;
use libp2p::core::transport::ListenerId;
use libp2p::futures::StreamExt;
use libp2p::kad::store::RecordStore as _;
use libp2p::request_response;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm, gossipsub, identify, kad};
use prost::Message as _;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::record_queries::RecordQueries;
use super::{Command, Correlated, DialTarget, Exchanges, Inbound, Queued};
use crate::blob_exchange::codec::BlobCodec;
use crate::claim::codec::ClaimCodec;
use crate::codec::Ack;
use crate::framing::decode_election;
use crate::join_codec::JoinCodec;
use crate::peers::{Carried, Observation, Peers, Side, is_dialable_listen_addr, worker_id_of};
use crate::reconcile::codec::ReconcileCodec;
use crate::result_transport::codec::ResultCodec;
use crate::steal::codec::StealCodec;
use crate::swarm::{Behaviour, BehaviourEvent, hide_listen_addresses};
use crate::task_exchange::codec::TaskCodec;

/// The input an arriving gossip `message` is for this worker's node: a
/// message from its author (see "Gossip" in the `crate::messenger` module
/// doc). `None`, and the message dropped, when it names no author or its body
/// is not a well-formed `ElectionMessage`.
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
/// no connection to the sender and still has to answer it directly (see
/// "Reconnect/backoff and where a peer's address comes from" in the
/// `crate::messenger` module doc).
/// Every other message is left as it is.
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
        Command::Send { to, message } => send_direct(swarm, peers, to, message, now),
        Command::Subscribe { topic } => subscribe(swarm, &topic),
        Command::Publish { topic, message } => publish(swarm, peers, topic, message, now),
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
            dial_for_connection(swarm, pending, peers, target, respond_to, now);
        }
        Command::ListenOn { addr, respond_to } => listen_on(swarm, pending, addr, respond_to),
        Command::SetExternalAddress { address } => {
            set_external_address(swarm, identify_config, peers, address, now);
        }
        Command::Exchange(run) => run(swarm, &mut pending.exchanges),
        Command::WithPeers(read) => read(peers),
        Command::WriteRecords(writes) => pending.records.start_writes(swarm, writes, inbound),
        Command::HandOff { write, respond_to } => pending.records.start_hand_off(swarm, write, respond_to),
        Command::GetRecord { task, respond_to } => pending.records.start_read(swarm, task, respond_to),
        Command::PutBlob { key, record, placement, respond_to } => {
            pending.records.start_blob_put(swarm, key, record, &placement, respond_to);
        }
        Command::LookUpBlob { key, respond_to } => pending.records.start_blob_lookup(swarm, key, respond_to),
    }
}

fn send_direct(
    swarm: &mut Swarm<Behaviour>,
    peers: &mut Peers,
    to: PeerId,
    mut message: ElectionMessage,
    now: Instant,
) {
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

fn subscribe(swarm: &mut Swarm<Behaviour>, topic: &gossipsub::IdentTopic) {
    if let Err(error) = swarm.behaviour_mut().gossipsub.subscribe(topic) {
        tracing::warn!(
            %topic,
            %error,
            "could not subscribe to the shard's gossip topic"
        );
    }
}

fn publish(
    swarm: &mut Swarm<Behaviour>,
    peers: &mut Peers,
    topic: gossipsub::TopicHash,
    mut message: ElectionMessage,
    now: Instant,
) {
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

fn dial_for_connection(
    swarm: &mut Swarm<Behaviour>,
    pending: &mut Pending,
    peers: &mut Peers,
    target: DialTarget,
    respond_to: oneshot::Sender<Option<PeerId>>,
    now: Instant,
) {
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

fn listen_on(
    swarm: &mut Swarm<Behaviour>,
    pending: &mut Pending,
    addr: Multiaddr,
    respond_to: oneshot::Sender<Multiaddr>,
) {
    if let Ok(listener_id) = swarm.listen_on(addr) {
        pending.listens.insert(listener_id, respond_to);
    }
    // On error, dropping `respond_to` closes the channel;
    // `Net::listen_on`'s awaiter observes that as a panic with a
    // message pointing at the cause.
}

fn set_external_address(
    swarm: &mut Swarm<Behaviour>,
    identify_config: &identify::Config,
    peers: &mut Peers,
    address: Multiaddr,
    now: Instant,
) {
    hide_listen_addresses(swarm, identify_config);
    swarm.add_external_address(address.clone());
    peers.observe(Observation::ExternalAddress(address), now);
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
            listening(pending, peers, listener_id, address, now);
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            ..
        } => connection_established(
            pending,
            inbound,
            peers,
            peer_id,
            connection_id,
            &endpoint,
            num_established,
            now,
        ),
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
        } => dial_failed(swarm, pending, inbound, peers, connection_id, peer_id, now),
        SwarmEvent::Behaviour(event) => handle_behaviour_event(swarm, event, pending, inbound, peers, now),
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn connection_established(
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
    peer_id: PeerId,
    connection_id: ConnectionId,
    endpoint: &ConnectedPoint,
    num_established: NonZeroU32,
    now: Instant,
) {
    if num_established.get() == 1 {
        inbound.queue_input(Input::PeerConnected(worker_id_of(&peer_id)));
    }
    observe_connection(peers, peer_id, endpoint, now);
    finish_dial(pending, peers, connection_id, Some(peer_id), now);
}

fn dial_failed(
    swarm: &Swarm<Behaviour>,
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
    connection_id: ConnectionId,
    peer_id: Option<PeerId>,
    now: Instant,
) {
    finish_dial(pending, peers, connection_id, None, now);
    // A dial that names its peer and fails while no other connection
    // to that peer is open: the peer cannot be reached.
    if let Some(peer) = peer_id
        && !swarm.is_connected(&peer)
    {
        inbound.queue_input(Input::PeerDisconnected(worker_id_of(&peer)));
    }
}

/// A listen address the swarm reported. A node listening on a wildcard bind
/// gets one per interface, in no set order, loopback among them: the peer
/// book keeps the best (see `crate::peers`). The first answers the
/// `Net::listen_on` that asked for its listener.
fn listening(
    pending: &mut Pending,
    peers: &mut Peers,
    listener_id: ListenerId,
    address: Multiaddr,
    now: Instant,
) {
    peers.observe(Observation::ListeningOn(address.clone()), now);
    if let Some(respond_to) = pending.listens.remove(&listener_id) {
        let _ = respond_to.send(address);
    }
}

/// Tells the peer book of a connection that opened. The two sides of a
/// connection observe different addresses here (see `crate::peers`'s "Where
/// a peer's address comes from"), so it is told which end this node is.
fn observe_connection(peers: &mut Peers, peer: PeerId, endpoint: &ConnectedPoint, now: Instant) {
    let side = match endpoint {
        ConnectedPoint::Dialer { .. } => Side::Dialer,
        ConnectedPoint::Listener { .. } => Side::Listener,
    };
    peers.observe(
        Observation::ConnectionOpened {
            peer,
            side,
            address: endpoint.get_remote_address().clone(),
        },
        now,
    );
}

/// Ends `Net::dial_for_connection`'s own dial for *this* connection id, if
/// one waits on it. Matching on the `ConnectionId`, rather than diffing the
/// connected-peer set, is what makes a late connection from an abandoned
/// dial harmless: if the caller already stopped awaiting this entry (timeout
/// elapsed, cascade moved on), answering it fails silently instead of
/// resolving some other, unrelated await.
fn finish_dial(
    pending: &mut Pending,
    peers: &mut Peers,
    connection_id: ConnectionId,
    outcome: Option<PeerId>,
    now: Instant,
) {
    if let Some(dial) = pending.dials.remove(&connection_id) {
        dial.finish(outcome, peers, now);
    }
}

/// One behaviour's event: a finished routing crawl, a peer identified, an
/// election message direct or gossiped, a records query's progress, or a
/// correlated protocol's event (see [`settle_exchange`]).
fn handle_behaviour_event(
    swarm: &mut Swarm<Behaviour>,
    event: BehaviourEvent,
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
    now: Instant,
) {
    match event {
        BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
            result: kad::QueryResult::Bootstrap(result),
            id,
            step,
            ..
        }) => {
            if let Some(input) = pending.crawls.step(id, &result, &step) {
                inbound.queue_input(input);
            }
        }
        BehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
            identified(swarm, peers, peer_id, info, now);
        }
        BehaviourEvent::RequestResponse(request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        }) => {
            if let Some(input) = direct_input(peer, request) {
                arrived(inbound, peers, input, now);
            }
            // Best-effort: nothing reads this ack back (see the module doc
            // of `crate::messenger`), and a channel that is already closed
            // just means the peer stopped waiting on it.
            let _ = swarm
                .behaviour_mut()
                .request_response
                .send_response(channel, Ack);
        }
        BehaviourEvent::Gossipsub(gossipsub::Event::Message { message, .. }) => {
            if let Some(input) = gossip_input(message) {
                arrived(inbound, peers, input, now);
            }
        }
        BehaviourEvent::Records(event) => records_event(swarm, pending, event, inbound),
        BehaviourEvent::Blob(request_response::Event::Message {
            message: request_response::Message::Request { request, channel, .. },
            ..
        }) => answer_blob_request(swarm, peers, request, channel, now),
        other => settle_exchange(other, pending, inbound, peers, now),
    }
}

/// Takes one event of the shard's records `kad`, and ends the query it says
/// has done its work: the rest of a blob lookup that found its copy is wasted.
fn records_event(swarm: &mut Swarm<Behaviour>, pending: &mut Pending, event: kad::Event, inbound: &Inbound) {
    let Some(done) = pending.records.on_event(event, inbound) else {
        return;
    };
    let records = swarm.behaviour_mut().records.as_mut();
    if let Some(mut running) = records.and_then(|records| records.query_mut(&done)) {
        running.finish();
    }
}

/// Answers a holder's direct ask for a blob from this worker's own store: the
/// blob record, or an empty one when it holds no such blob.
fn answer_blob_request(
    swarm: &mut Swarm<Behaviour>,
    peers: &mut Peers,
    request: generated::Digest,
    channel: request_response::ResponseChannel<generated::BlobRecord>,
    now: Instant,
) {
    peers.observe(Observation::Carried(Carried::BlobRequest), now);
    let answer = Digest::try_from(&request)
        .ok()
        .map(|digest| BlobKey::from(&digest))
        .and_then(|key| {
            let records = swarm.behaviour_mut().records.as_mut()?;
            let found = records.store_mut().get(&kad::RecordKey::new(&key.record_key()))?;
            generated::BlobRecord::decode(&found.value[..]).ok()
        })
        .unwrap_or_default();
    // Best-effort: an asker that stopped waiting just drops it.
    let _ = swarm.behaviour_mut().blob.send_response(channel, answer);
}

/// Feeds `kad`, and the shard's records `kad`, every dialable address
/// Identify reported for `peer`, and tells the peer book.
///
/// This is why `identify` is in the swarm: `info.listen_addrs` is the
/// peer's own advertised listen address, which is what a joining node
/// needs to dial, unlike either endpoint-derived address. See
/// `crate::peers`'s "Where a peer's address comes from" for the ranking and
/// for which advertised address is taken. Every dialable one, not just
/// that one, feeds `kad` (see `crate::swarm`'s "kad: peer routing, not
/// membership"): Kademlia keeps several addresses per peer, and peer
/// routing is what this crate wants from it. Only a peer that speaks this
/// shard's records protocol is a routing candidate for the records `kad`:
/// another shard's peer cannot answer it, and would only take up its
/// buckets. A peer that only dialed this node has no address there
/// otherwise (an inbound connection carries none), so a lookup would see
/// no one but this node's own store.
fn identified(
    swarm: &mut Swarm<Behaviour>,
    peers: &mut Peers,
    peer: PeerId,
    info: identify::Info,
    now: Instant,
) {
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
        swarm.behaviour_mut().kad.add_address(&peer, addr.clone());
        if speaks_records && let Some(records) = swarm.behaviour_mut().records.as_mut() {
            records.add_address(&peer, addr.clone());
        }
    }
    peers.observe(
        Observation::Identified {
            peer,
            listen_addresses: info.listen_addrs,
        },
        now,
    );
}

/// The input a direct election message from `peer` is for this worker's
/// node. The codec already refused a malformed request, so the decode does
/// not fail; a message it did refuse would be dropped.
fn direct_input(peer: PeerId, request: ElectionMessage) -> Option<Input> {
    match checked::decode(request) {
        Ok(message) => Some(Input::Message {
            from: worker_id_of(&peer),
            message,
        }),
        Err(error) => {
            tracing::debug!(%peer, %error, "dropping a direct message that is not a well-formed election message");
            None
        }
    }
}

/// Queues an arrived election message for the node, after the peer book
/// records it, so a direct answer the node sends finds the address on file.
fn arrived(inbound: &Inbound, peers: &mut Peers, input: Input, now: Instant) {
    peers.observe(Observation::MessageArrived(&input), now);
    inbound.queue_input(input);
}

/// Settles an event of a correlated protocol (see [`settle`]). Every other
/// event is ignored.
fn settle_exchange(
    event: BehaviourEvent,
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
    now: Instant,
) {
    let exchanges = &mut pending.exchanges;
    match event {
        BehaviourEvent::Join(event) => {
            settle::<JoinCodec>(exchanges, event, inbound, peers, now);
        }
        BehaviourEvent::Claim(event) => {
            settle::<ClaimCodec>(exchanges, event, inbound, peers, now);
        }
        BehaviourEvent::Task(event) => {
            settle::<TaskCodec>(exchanges, event, inbound, peers, now);
        }
        BehaviourEvent::Reconcile(event) => {
            settle::<ReconcileCodec>(exchanges, event, inbound, peers, now);
        }
        BehaviourEvent::Steal(event) => {
            settle::<StealCodec>(exchanges, event, inbound, peers, now);
        }
        // Only a result message another worker sent is news to the client;
        // the answer to one this worker sent, or its failure, is not.
        BehaviourEvent::Result(event) => {
            if settle::<ResultCodec>(exchanges, event, inbound, peers, now) {
                inbound.results_arrived.notify_one();
            }
        }
        BehaviourEvent::Blob(event) => {
            // Only this worker's own asks remain: the exchange settles them.
            let _ = BlobCodec::exchange(exchanges).on_event(event);
        }
        _ => {}
    }
}

/// Settles what `event` of protocol `C` settles (see `Exchange::on_event`),
/// and queues an arriving request for the driver, counting it in the peer
/// book's traffic; whether a request was queued.
fn settle<C: Queued>(
    exchanges: &mut Exchanges,
    event: request_response::Event<C::Request, C::Response>,
    inbound: &Inbound,
    peers: &mut Peers,
    now: Instant,
) -> bool {
    let Some(asked) = C::exchange(exchanges).on_event(event) else {
        return false;
    };
    peers.observe(Observation::Carried(C::ARRIVAL), now);
    inbound.queue_asked(C::queue(inbound), asked);
    true
}

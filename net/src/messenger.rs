//! `Net`: the network a driver runs a `core::election::WorkerNode` over, on
//! top of the swarm built by `crate::swarm`. It sends the election messages
//! the node asks to send, publishes the ones it asks to publish to its whole
//! shard (see "Gossip" below), and queues the node's inputs as they happen:
//! every election message received, and every connection to a peer opening
//! or closing. It also carries the join, claim and task-exchange protocols,
//! whose requests only the driver can answer (see
//! `crate::driver::run_driver`), and keeps this worker's ledger of the runs
//! it claimed (`crate::claimed_runs::ClaimedRuns`), which the claim and task
//! exchange calls update from the leader's answers. A completed
//! routing crawl is reported to the node as `Input::RoutingCrawled`.
//!
//! ## `WorkerId` <-> `PeerId` mapping
//!
//! A `WorkerId` *is* the base58 string form of the node's libp2p `PeerId`
//! (`PeerId::to_string()` / `PeerId::from_str()`, which round-trip via
//! `bs58`/multihash — see `libp2p_identity::PeerId`'s `Display`/`FromStr`
//! impls). This avoids standing up a second identity system: whatever
//! process holds the keypair that produced a `PeerId` is, by construction,
//! the only process that can act as that `WorkerId`.
//!
//! ## Synchronization: a background task, not a locked `Swarm`
//!
//! `Net`'s methods take `&self`, but driving a libp2p `Swarm` needs `&mut
//! self` (`swarm.select_next_some()`). `Net::new` hands the `Swarm` to a
//! dedicated tokio task (`drive`) that owns it exclusively and polls it
//! continuously, together with everything the task learns about its peers
//! (`crate::peers::Peers`: connections, addresses, redials, gossip mesh,
//! traffic), which no other code touches. `Net` itself holds a command
//! channel to that task, the queues of inputs and requests the task fills
//! for the driver, and this node's own address, which the task publishes on
//! a `watch` channel. A method either pushes a command and returns, awaits
//! the task's answer to a command (a read of its peers among them: see
//! [`Net::diagnostics`]), or takes a short lock on a queue; nothing holds a
//! lock across an await, so a `&self` caller never deadlocks against the
//! task.
//!
//! ## Inputs for the node
//!
//! `drive` turns swarm events into `core::election::Input`s and queues them
//! from the moment the `Net` is created ([`Net::take_inputs`]), so a node
//! built later (after the bootstrap cascade, say), or driven again after a
//! pause, is told of everything that happened in between:
//!
//! - an inbound election message, sent or published, becomes
//!   `Input::Message`;
//! - a peer's first connection opening becomes `Input::PeerConnected`;
//! - its last connection closing becomes `Input::PeerDisconnected`, and so
//!   does a failed dial that names a peer this `Net` holds no connection to.
//!
//! - a completed routing crawl (see [`Net::refresh_peer_routing`]; the swarm's
//!   own periodic crawls count too) becomes `Input::RoutingCrawled`, of which
//!   at most one is queued.
//!
//! The queue stays bounded however long no driver takes from it (the
//! bootstrap cascade can wait indefinitely, say). A connection event that
//! repeats the peer's last queued one is not queued again, since the node
//! would ignore it; a disconnect that follows a still-queued connect removes
//! that connect and is not queued either, since together they change nothing
//! the node needs to hear, so at most two connection events per peer are
//! ever queued. Election messages, unreliable anyway, are capped instead:
//! past [`Net::with_input_limit`]'s limit, the oldest queued one is dropped.
//! Each input, like each join or claim request, also wakes whoever waits in
//! [`Net::wait_for_arrival`], so a driver can sleep until something arrives
//! instead of polling.
//!
//! ## Response shape
//!
//! Election messages are never correlated with a response: [`Net::send`] is
//! fire-and-forget and delivery is unreliable. `drive` answers every inbound
//! election message with `codec::Ack` as soon as it has queued it, purely so
//! the substream closes cleanly on the sender's side; nothing here ever reads
//! that ack back.
//!
//! ## Gossip
//!
//! A `Net` serves one shard. [`Net::subscribe_to_shard`] subscribes it to
//! that shard's gossipsub topic, `/kabudachi/<name>/election/2`, and
//! [`Net::publish`] publishes on it, fire-and-forget like `send`. A publish
//! gossipsub refuses (no subscribed peer yet, say) is dropped like a lost
//! message, logged at `debug`. Every gossip message is signed by its author
//! (see `crate::swarm`), so an arriving one becomes `Input::Message` from the
//! author, not from whichever peer relayed it. One with no author, or whose
//! body is not a well-formed `ElectionMessage`, is dropped.
//!
//! The topic and the records protocol are the shard's name, shared by every
//! incarnation of the shard under it. Election messages carry the `ShardId`,
//! and a node drops another incarnation's.
//!
//! A relayed roll call can reach a worker that holds no connection to its
//! initiator, and that worker answers the initiator directly. So a roll call
//! and a reply to one carry their sender's address (see "Where a peer's
//! address comes from" below), and [`Net::send`] dials a peer it holds no
//! connection to at the address it has on file.
//!
//! ## The shard's Task records
//!
//! A `Net` built with [`Net::for_shard`] also stores the shard's Task records
//! for its peers, over a records protocol of that shard alone (see
//! `crate::swarm`'s "records"), and exposes what it holds
//! ([`Net::held_records`]). The shard's leader hands it revisions to write to
//! the voters that hold them, and reads the outcome of each write back;
//! see `crate::task_store`.
//!
//! ## The bootstrap join protocol
//!
//! Join and claim are both correlated: `crate::exchange` holds the swarm
//! task's half of one, and every ask and answer of either reaches the swarm
//! task as one `Command::Exchange` (see `Net::ask`, `Net::take_asked` and
//! `Net::answer`). The typed wrappers live with their protocol
//! (`crate::join`, `crate::claim`).
//!
//! `/kabudachi/join/1` (see `crate::join_codec`) is a genuine correlated
//! request/response, unlike the election protocol above, so it needs its own
//! machinery: the joining side sends a `JOIN_REQUEST` over a connection a
//! dial of its own opened (see `crate::join`, the JOIN client), and
//! [`Net::poll_join_requests`] / [`Net::respond_join`] serve the answering
//! side. A join response names the shard's leader, which only
//! `core::election::WorkerNode` knows, so the driver answers it (see
//! `crate::join::pointer_for`). [`Net::dialable_address`] and
//! [`Net::local_multiaddr`] expose what `Net` knows about addresses, the
//! other half of what a join response needs.
//!
//! ## The claim arbitration protocol
//!
//! `/kabudachi/claim/1` (see `crate::claim`) is the same shape of
//! genuine correlated request/response as join, so it gets the same
//! machinery: `Net::request_claim` and `Net::claim_oldest` (the asking
//! side, sent to the leader the caller names: the transport keeps no leader
//! of its own) and [`Net::poll_claim_requests`] / [`Net::respond_claim`]
//! (the answering side). Whether to grant a claim is
//! `core::scheduler::Scheduler`'s decision (`crate::claim::answer`), so the
//! driver answers it too.
//!
//! ## The task exchange protocol
//!
//! `/kabudachi/task/1` (see `crate::task_exchange`) is a third correlated
//! protocol of the same shape: `Net::submit`, `Net::report_started`,
//! `Net::complete`, `Net::fail` and `Net::cancel` ask the leader the caller
//! names, and [`Net::poll_task_requests`] / [`Net::respond_task`] serve the
//! answering side. The calls about a run also keep [`Net::claimed_runs`].
//!
//! ## The reconcile protocol
//!
//! `/kabudachi/reconcile/1` (see `crate::reconcile`) is a fourth correlated
//! protocol of the same shape: [`Net::ask_reconcile`] asks a worker for one
//! page of what it holds, and [`Net::poll_reconcile_requests`] /
//! [`Net::respond_reconcile`] serve the answering side, which every worker
//! does for whichever leader asks.
//!
//! ## The steal protocol
//!
//! `/kabudachi/steal/1` (see `crate::steal`) is a fifth correlated protocol of
//! the same shape: [`Net::steal`] asks a shard peer which tasks it holds
//! records of that look claimable, and [`Net::poll_steal_requests`] /
//! [`Net::respond_steal`] serve the answering side, which every worker does.
//! [`Net::steal_targets`] reads the records `kad` routing table, afresh on
//! every call, to say which peers to ask; nothing keeps that list.
//!
//! ## Reconnect/backoff and where a peer's address comes from
//!
//! What a `Net` knows about its peers, and the fast-then-slow redial of a dropped
//! one, belong to `crate::peers`: its module doc has the redial rules (which
//! drops qualify, why the default policy is short) and how a peer's address
//! of record is ranked by its source. This task tells the peer book what it
//! saw (`crate::peers::Observation`) and dials what it says is due.
//!
//! What stays here is the outgoing half of address stamping. A relayed roll
//! call can reach a worker that holds no connection to its initiator, and
//! that worker answers the initiator directly, so a `Net` stamps its own
//! address on every roll call it publishes and every roll call reply it
//! sends (see `stamp_own_address`), and [`Net::send`] dials a peer it holds no
//! connection to at the address the peer book has for it.
//!
//! The peer book is not the only place a dial can find an address, though:
//! [`Net::send`] hands `request_response` an empty address list whenever the
//! book has nothing, and the swarm underneath that call still asks `kad`'s
//! routing table for one regardless, fed from the same Identify events in
//! `handle_event` below (see `crate::swarm`'s "kad: peer routing, not
//! membership"). So a peer this node never itself connected to can still be
//! reached, once some other peer's Identify has given `kad` a route to it.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::election::Input;
use kabudachi_core::protocol::checked;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{ShardId, ShardName, TaskId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
pub use kabudachi_core::task_record::{PlacedWrite, WriteOutcome};
use kabudachi_core::task_record::{RecordVersion, VersionOrder, Write, identify};
use libp2p::core::ConnectedPoint;
use libp2p::core::transport::ListenerId;
use libp2p::futures::StreamExt;
use libp2p::kad::store::RecordStore as _;
use libp2p::request_response::{self, ResponseChannel};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm, gossipsub, identify, kad};
use prost::Message as _;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::claim::codec::ClaimCodec;
use crate::claimed_runs::ClaimedRuns;
use crate::codec::Ack;
use crate::exchange::{Asked, Exchange};
use crate::framing::decode_election;
use crate::join_codec::JoinCodec;
pub use crate::peers::{Diagnostics, RedialPolicy, Traffic};
use crate::peers::{Carried, Observation, Peers, Side, is_dialable_listen_addr, worker_id_of};
use crate::reconcile::codec::ReconcileCodec;
use crate::steal::codec::StealCodec;
use crate::swarm::{Behaviour, BehaviourEvent, build_swarm, hide_listen_addresses};
use crate::task_exchange::codec::TaskCodec;
use crate::task_store::{HeldRecords, record_key};

/// What [`Net::dial_for_connection`] dials.
pub(crate) enum DialTarget {
    /// Whoever answers at this address (a seed or a registered peer asked who
    /// leads). The swarm task records the peer it finds there
    /// (`Observation::Dialed`).
    Address(Multiaddr),
    /// This peer, at `address` and any address the swarm's behaviours know for
    /// it (`DialOpts::extend_addresses_through_behaviour`).
    Peer { peer: PeerId, address: Multiaddr },
}

/// Instructions for the driver task. `Net`'s methods and setup
/// helpers only ever push onto this channel; they never touch the `Swarm`
/// directly.
enum Command {
    Send {
        to: PeerId,
        message: ElectionMessage,
    },
    Subscribe {
        topic: gossipsub::IdentTopic,
    },
    Publish {
        topic: gossipsub::TopicHash,
        message: ElectionMessage,
    },
    Dial {
        addr: Multiaddr,
    },
    /// See `Net::refresh_peer_routing`.
    RefreshPeerRouting,
    /// See `Net::block_peer`.
    Block {
        peer: PeerId,
    },
    /// Like `Dial`, but the caller wants to know *which* connection this
    /// specific dial produces — see `Net::dial_for_connection`, the only
    /// caller. The `DialOpts` built from `target` carries a `ConnectionId`
    /// (`DialOpts::connection_id`) that libp2p attaches to every
    /// `SwarmEvent::ConnectionEstablished` /
    /// `SwarmEvent::OutgoingConnectionError` this dial attempt produces, so
    /// the driver can correlate the outcome to this exact call instead of
    /// guessing from the connected-peer set.
    DialForConnection {
        target: DialTarget,
        respond_to: oneshot::Sender<Option<PeerId>>,
    },
    ListenOn {
        addr: Multiaddr,
        respond_to: oneshot::Sender<Multiaddr>,
    },
    /// See `Net::set_external_address`.
    SetExternalAddress {
        address: Multiaddr,
    },
    /// See `Net::get_record`.
    GetRecord {
        task: TaskId,
        respond_to: oneshot::Sender<Option<TaskRecord>>,
    },
    /// Runs an ask or an answer of one correlated protocol on the swarm
    /// task, which alone owns the swarm and its [`Exchanges`]; see
    /// `Net::ask` and `Net::answer`.
    Exchange(Box<dyn FnOnce(&mut Swarm<Behaviour>, &mut Exchanges) + Send>),
    /// Runs a read or small change of the swarm task's [`Peers`] on that
    /// task, which alone owns them; see `Net::with_peers`.
    WithPeers(Box<dyn FnOnce(&mut Peers) + Send>),
    /// See `Net::write_records`.
    WriteRecords(Vec<PlacedWrite>),
    /// See `Net::hand_off`.
    HandOff {
        write: HandOffWrite,
        respond_to: oneshot::Sender<bool>,
    },
}

/// A copy a draining worker hands to the holders a placement named (see
/// `Net::hand_off`).
#[derive(Debug, Clone)]
pub(crate) struct HandOffWrite {
    /// The record exactly as the worker holds it.
    pub record: TaskRecord,
    /// The voters to send it to, this worker not among them.
    pub holders: Vec<WorkerId>,
    /// How many of them must store it for the hand-off to count.
    pub quorum: usize,
}

/// The swarm task's asks in flight, one [`Exchange`] per correlated protocol.
#[derive(Default)]
pub(crate) struct Exchanges {
    join: Exchange<JoinCodec>,
    claim: Exchange<ClaimCodec>,
    task: Exchange<TaskCodec>,
    reconcile: Exchange<ReconcileCodec>,
    steal: Exchange<StealCodec>,
}

/// A correlated protocol `Net` carries through an [`Exchange`]
/// (implemented here for `JoinCodec`, `ClaimCodec`, `TaskCodec` and
/// `ReconcileCodec`).
pub(crate) trait Correlated:
    request_response::Codec + Clone + Send + Sized + 'static
{
    /// The traffic count an arriving request adds.
    const ARRIVAL: Carried;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self>;
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self>;
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>>;
}

impl Correlated for JoinCodec {
    const ARRIVAL: Carried = Carried::JoinRequest;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.join
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.join
    }
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.joins
    }
}

impl Correlated for ClaimCodec {
    const ARRIVAL: Carried = Carried::ClaimRequest;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.claim
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.claim
    }
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.claims
    }
}

impl Correlated for TaskCodec {
    const ARRIVAL: Carried = Carried::TaskRequest;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.task
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.task
    }
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.tasks
    }
}

impl Correlated for ReconcileCodec {
    const ARRIVAL: Carried = Carried::ReconcileRequest;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.reconcile
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.reconcile
    }
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.reconciles
    }
}

impl Correlated for StealCodec {
    const ARRIVAL: Carried = Carried::StealRequest;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.steal
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.steal
    }
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.steals
    }
}

/// [`Net::try_listen_on`] could not listen on this address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenRejected(pub Multiaddr);

impl std::fmt::Display for ListenRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not listen on {}", self.0)
    }
}

impl std::error::Error for ListenRejected {}

/// How many inputs [`Net`] holds for its node, by default, before it drops
/// the oldest election message to make room (see [`Net::with_input_limit`]).
pub const DEFAULT_INPUT_LIMIT: usize = 1024;

/// What `drive` hands over for the driver of this `Net`'s node, each queue
/// in arrival order: the node's inputs, the join, claim, task, reconcile and steal requests the
/// driver answers, and the outcomes of the record writes it asked for.
/// `arrived` is signalled whenever any of them grows.
pub(crate) struct Inbound {
    inputs: Mutex<VecDeque<Input>>,
    /// See [`Net::with_input_limit`].
    input_limit: AtomicUsize,
    joins: Mutex<VecDeque<Asked<JoinCodec>>>,
    claims: Mutex<VecDeque<Asked<ClaimCodec>>>,
    tasks: Mutex<VecDeque<Asked<TaskCodec>>>,
    reconciles: Mutex<VecDeque<Asked<ReconcileCodec>>>,
    steals: Mutex<VecDeque<Asked<StealCodec>>>,
    writes: Mutex<VecDeque<WriteOutcome>>,
    arrived: Notify,
}

impl Default for Inbound {
    fn default() -> Self {
        Self {
            inputs: Mutex::default(),
            input_limit: AtomicUsize::new(DEFAULT_INPUT_LIMIT),
            joins: Mutex::default(),
            claims: Mutex::default(),
            tasks: Mutex::default(),
            reconciles: Mutex::default(),
            steals: Mutex::default(),
            writes: Mutex::default(),
            arrived: Notify::new(),
        }
    }
}

impl Inbound {
    /// Queues `input` for the node, keeping the queue bounded while no
    /// driver takes from it (see the module doc's "Inputs for the node").
    fn queue_input(&self, input: Input) {
        let mut inputs = self.inputs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(peer) = connection_event_peer(&input) {
            let latest = inputs
                .iter()
                .rposition(|queued| connection_event_peer(queued) == Some(peer));
            match latest.map(|index| (index, &inputs[index])) {
                // The node would ignore a repeat of the peer's last event.
                Some((_, queued)) if *queued == input => return,
                // The peer connected and disconnected again before the node
                // heard either: together they change nothing the node
                // needs to know.
                Some((index, Input::PeerConnected(_))) => {
                    inputs.remove(index);
                    return;
                }
                _ => {}
            }
        } else if input == Input::RoutingCrawled {
            // The node handles one the same as two, and it is never dropped
            // for the limit, so at most one is ever queued.
            if inputs.contains(&input) {
                return;
            }
        } else if inputs.len() >= self.input_limit.load(Ordering::Relaxed) {
            // Connection events are never dropped: at most two per peer are
            // ever queued, so only messages can fill the queue.
            if let Some(oldest) = inputs
                .iter()
                .position(|queued| matches!(queued, Input::Message { .. }))
            {
                inputs.remove(oldest);
                tracing::debug!(
                    limit = self.input_limit.load(Ordering::Relaxed),
                    "dropping the oldest queued election message: no driver has taken this \
                     node's inputs"
                );
            }
        }
        inputs.push_back(input);
        drop(inputs);
        self.arrived.notify_one();
    }

    fn queue_write(&self, outcome: WriteOutcome) {
        push(&self.writes, outcome);
        self.arrived.notify_one();
    }

    fn queue_asked<T>(&self, queue: &Mutex<VecDeque<T>>, asked: T) {
        push(queue, asked);
        self.arrived.notify_one();
    }
}

/// The peer a connection event is about; `None` for any other input.
fn connection_event_peer(input: &Input) -> Option<&WorkerId> {
    match input {
        Input::PeerConnected(peer) | Input::PeerDisconnected(peer) => Some(peer),
        _ => None,
    }
}

fn push<T>(queue: &Mutex<VecDeque<T>>, item: T) {
    queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push_back(item);
}

fn drain<T>(queue: &Mutex<VecDeque<T>>) -> Vec<T> {
    queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .drain(..)
        .collect()
}

/// The election network of one worker on top of a real libp2p swarm. One
/// `Net` represents exactly one worker/node — see the module doc for the
/// `WorkerId <-> PeerId` mapping and the actor pattern behind `&self`.
pub struct Net {
    local_worker_id: WorkerId,
    commands: mpsc::UnboundedSender<Command>,
    inbound: Arc<Inbound>,
    /// The address this `Net` gives other nodes for itself (see
    /// `Self::local_multiaddr`), as the swarm task last published it.
    local_addr: watch::Receiver<Option<Multiaddr>>,
    /// The shard this `Net` subscribed to (see `Self::subscribe_to_shard`).
    shard: Mutex<Option<ShardId>>,
    /// The shard whose Task records this `Net` stores, for a `Net` built
    /// with [`Self::for_shard`].
    records_shard: Option<ShardName>,
    /// The Task records this `Net` holds (see [`Self::held_records`]).
    held: HeldRecords,
    /// The runs this worker claimed (see [`Self::claimed_runs`]).
    pub(crate) claimed: ClaimedRuns,
    driver: JoinHandle<()>,
}

impl Net {
    /// Builds a swarm under a fresh identity and spawns the task that drives
    /// it. Does not listen or dial; use `listen_on`/`dial` for that.
    ///
    /// The identity is generated here, never passed in: a worker's id is its
    /// peer id and lives for one process incarnation (see `crate::worker`'s
    /// "One identity per process"), so no caller has a keypair worth
    /// choosing.
    ///
    /// Uses [`RedialPolicy::default`] for the redial policy applied to a peer
    /// that drops unexpectedly; see [`Self::new_with_redial_policy`] to use
    /// different parameters (e.g. short, test-scale ones).
    pub fn new() -> Self {
        Self::new_with_redial_policy(RedialPolicy::default())
    }

    /// Like [`Self::new`], but with an explicit [`RedialPolicy`] instead of
    /// its default. Exists so a caller with different needs, chiefly this
    /// crate's own tests, which need much shorter backoff/check intervals
    /// than the production default to stay fast and deterministic, doesn't
    /// have to change what every other `Net::new` caller gets.
    pub fn new_with_redial_policy(redial_policy: RedialPolicy) -> Self {
        Self::build(redial_policy, None, None)
    }

    /// A `Net` for a worker of the shard `name`: besides everything [`Self::new`]
    /// gives, it holds the shard's Task records and stores and serves them
    /// over a protocol only the shard's workers speak, so a record never
    /// lands in another shard. Finished records are dropped `retention`
    /// after they finish.
    pub fn for_shard(
        name: ShardName,
        retention: Option<kabudachi_core::time::Duration>,
    ) -> Self {
        Self::build(RedialPolicy::default(), Some(name), retention)
    }

    fn build(
        redial_policy: RedialPolicy,
        records_shard: Option<ShardName>,
        retention: Option<kabudachi_core::time::Duration>,
    ) -> Self {
        let held = HeldRecords::new(retention);
        let (swarm, identify_config) = build_swarm(records_shard.as_ref(), held.clone());
        let local_worker_id = WorkerId::new(swarm.local_peer_id().to_string());
        let (commands, command_rx) = mpsc::unbounded_channel();
        let inbound = Arc::new(Inbound::default());
        let (local_addr_tx, local_addr) = watch::channel(None);
        let peers = Peers::new(local_addr_tx, redial_policy);

        let driver = tokio::spawn(drive(swarm, identify_config, command_rx, inbound.clone(), peers));

        Self {
            local_worker_id,
            commands,
            inbound,
            local_addr,
            shard: Mutex::new(None),
            records_shard,
            held,
            claimed: ClaimedRuns::default(),
            driver,
        }
    }

    /// The records this worker holds (empty for a `Net` built with
    /// [`Self::new`]).
    pub fn held_records(&self) -> HeldRecords {
        self.held.clone()
    }

    /// Holds at most `limit` inputs for this `Net`'s node (default
    /// [`DEFAULT_INPUT_LIMIT`]) while no driver takes them: past it, the
    /// oldest election message queued is dropped, like any lost message, to
    /// make room. Connection events are compacted instead of dropped (see the
    /// module doc's "Inputs for the node"), so they alone can take the queue
    /// past `limit`, by at most two per peer. A `limit` below 1 counts as 1:
    /// the message just arrived is always kept.
    #[must_use]
    pub fn with_input_limit(self, limit: usize) -> Self {
        self.inbound.input_limit.store(limit.max(1), Ordering::Relaxed);
        self
    }

    /// This `Net`'s own `WorkerId`, per the `WorkerId <-> PeerId` mapping
    /// documented on this module.
    pub fn local_worker_id(&self) -> WorkerId {
        self.local_worker_id.clone()
    }

    /// Starts listening on `addr` and resolves once the driver task
    /// observes the concrete address the OS actually bound (relevant for
    /// `.../tcp/0` ephemeral-port addresses).
    ///
    /// # Panics
    ///
    /// Panics if the driver task has already stopped (i.e. this `Net` was
    /// already dropped and called through anyway), or if it drops the
    /// responder without resolving an address (which only happens if
    /// `Swarm::listen_on` itself rejects `addr`).
    pub async fn listen_on(&self, addr: Multiaddr) -> Multiaddr {
        self.try_listen_on(addr)
            .await
            .expect("Swarm::listen_on rejected the address given to Net::listen_on")
    }

    /// [`Self::listen_on`], returning [`ListenRejected`] instead of
    /// panicking when the swarm cannot listen on `addr`, or this `Net` has
    /// stopped.
    pub async fn try_listen_on(&self, addr: Multiaddr) -> Result<Multiaddr, ListenRejected> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::ListenOn {
                addr: addr.clone(),
                respond_to,
            })
            .map_err(|_| ListenRejected(addr.clone()))?;
        response.await.map_err(|_| ListenRejected(addr))
    }

    /// Gives `address` as this node's own from now on: in its registration,
    /// its leader hint, the JOIN pointers it hands out and its message stamps,
    /// in place of any address it listens on, and to the peers that connect
    /// to it through Identify, which then names no listen address. For a node
    /// behind NAT or bound to a wildcard. Send before listening and before
    /// any connection, so no listen address is ever given.
    pub(crate) fn set_external_address(&self, address: Multiaddr) {
        let _ = self.commands.send(Command::SetExternalAddress { address });
    }

    /// Fire-and-forget dial: initiates a connection attempt and returns
    /// immediately, mirroring `Self::send`'s own best-effort delivery
    /// contract. The outcome is reported only as an input for the node: the
    /// connection's `Input::PeerConnected`, or an `Input::PeerDisconnected`
    /// when a dial whose address names its peer (`.../p2p/<peer id>`) fails.
    /// A failed dial to an address that names no peer is not reported.
    pub fn dial(&self, addr: Multiaddr) {
        let _ = self.commands.send(Command::Dial { addr });
    }

    /// Cuts this node off from `peer` for the life of this node, as a
    /// network partition would: every connection to `peer` closes (each side
    /// reports `Input::PeerDisconnected`, as when the peer closes it), and
    /// no message crosses either way, whether this node dials `peer` (a
    /// send, a redial), which fails at once, or `peer` dials it, which this
    /// node refuses as soon as the handshake names `peer`. A message either
    /// way meanwhile is lost like any undeliverable one. Only this node
    /// refuses: `peer` may see its own dial connect for an instant before
    /// the refusal closes it. So a partition is blocked on both sides, or
    /// the unblocked side would see each of its dials connect for an
    /// instant. There is no way back: a test that needs the peer again
    /// starts a new node.
    /// Gossip still reaches `peer` through any other peer both are
    /// connected to, as it would across a partial partition: a test that
    /// cuts one group off from another blocks every pair across the cut.
    /// Blocking changes nothing about redial eligibility: a redial of a
    /// blocked peer fails like one of an unreachable peer and counts against
    /// the redial budget. Fire-and-forget, like `dial`; a no-op for a
    /// `peer` this mapping never produced.
    ///
    /// Backed by `libp2p::allow_block_list`, whose `block_peer` closes the
    /// peer's connections and whose connection checks deny every later one.
    pub fn block_peer(&self, peer: WorkerId) {
        let Ok(peer) = PeerId::from_str(peer.as_str()) else {
            return;
        };
        let _ = self.commands.send(Command::Block { peer });
    }

    /// Makes the dial `target` describes and resolves to the `PeerId` of the
    /// connection *this specific dial* establishes — never a connection some
    /// other dial (or an unrelated inbound connection) produced. `None` if
    /// the driver task is gone or this dial attempt fails
    /// (`SwarmEvent::OutgoingConnectionError`, including libp2p's `WrongPeerId`
    /// when `target` names a peer and another answers, or `Swarm::dial`
    /// rejecting it outright).
    ///
    /// Correlation is by `ConnectionId` (`DialOpts::connection_id`), not by
    /// diffing the connected-peer set before/after — see its callers in
    /// `crate::join` for why that distinction matters:
    /// a late `ConnectionEstablished` for an abandoned dial carries *that*
    /// dial's `ConnectionId`, so it can only ever resolve (or fail to
    /// resolve, if the caller already stopped awaiting it) that dial's own
    /// response channel — it can never be mistaken for a different, later
    /// dial's result.
    pub(crate) async fn dial_for_connection(&self, target: DialTarget) -> Option<PeerId> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::DialForConnection { target, respond_to })
            .ok()?;
        response.await.ok()?
    }

    /// The address this `Net` listens on that it gives other nodes, if any:
    /// the most recent one it was told of, except that a loopback address
    /// never replaces a non-loopback one (see the module doc's "Where a
    /// peer's address comes from"). Used to include this node's own address
    /// in a `JOIN_RESPONSE` it composes (`crate::driver::run_driver`'s
    /// join-request responder).
    pub fn local_multiaddr(&self) -> Option<Multiaddr> {
        self.local_addr.borrow().clone()
    }

    /// A receiver that follows [`Self::local_multiaddr`], for a task that
    /// must read it without holding this `Net`.
    pub(crate) fn own_address_watch(&self) -> watch::Receiver<Option<Multiaddr>> {
        self.local_addr.clone()
    }

    /// One read of what this `Net`'s transport knows (see [`Diagnostics`]),
    /// for tests and logs. Empty if the swarm task has stopped.
    pub async fn diagnostics(&self) -> Diagnostics {
        self.with_peers(|peers| peers.snapshot())
            .await
            .unwrap_or_default()
    }

    /// Runs `read` on the swarm task's [`Peers`], which that task alone
    /// owns, and returns what it returns: commands are handled in the order
    /// they were sent, so `read` sees the effect of every command sent
    /// before it. `None` if the swarm task has stopped.
    pub(crate) async fn with_peers<T: Send + 'static>(
        &self,
        read: impl FnOnce(&mut Peers) -> T + Send + 'static,
    ) -> Option<T> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::WithPeers(Box::new(move |peers| {
                let _ = respond_to.send(read(peers));
            })))
            .ok()?;
        response.await.ok()
    }

    /// `peer`'s address of record, but only if something can dial it: one
    /// `peer` advertised through Identify or stamped on a roll call or reply
    /// it sent, or one this node dialed itself. `None` for a peer known only
    /// from its own inbound connection, whose source address is not one
    /// `peer` listens on (see the module doc's "Where a peer's address comes
    /// from").
    pub async fn dialable_address(&self, peer: &WorkerId) -> Option<Multiaddr> {
        let peer = PeerId::from_str(peer.as_str()).ok()?;
        self.with_peers(move |peers| peers.dialable_address(&peer))
            .await
            .flatten()
    }

    /// Sends `request` to `to` over protocol `C` and awaits its answer. `None`
    /// if the swarm task is gone, the request fails outright
    /// (`OutboundFailure`), or the peer disconnects before answering.
    pub(crate) async fn ask<C: Correlated>(
        &self,
        to: PeerId,
        request: C::Request,
    ) -> Option<C::Response> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::Exchange(Box::new(move |swarm, exchanges| {
                C::exchange(exchanges).ask(C::behaviour(swarm.behaviour_mut()), &to, request, respond_to);
            })))
            .ok()?;
        response.await.ok()?
    }

    /// Drains every inbound request of protocol `C` not yet answered.
    pub(crate) fn take_asked<C: Correlated>(&self) -> Vec<Asked<C>> {
        drain(C::queue(&self.inbound))
    }

    /// Answers an inbound request of protocol `C`. Fire-and-forget like
    /// `send`: if the swarm task has already stopped, there is nowhere for the
    /// answer to go, and that is fine to drop.
    pub(crate) fn answer<C: Correlated>(
        &self,
        channel: ResponseChannel<C::Response>,
        response: C::Response,
    ) {
        let _ = self
            .commands
            .send(Command::Exchange(Box::new(move |swarm, _| {
                Exchange::<C>::answer(C::behaviour(swarm.behaviour_mut()), channel, response);
            })));
    }
}

impl Drop for Net {
    fn drop(&mut self) {
        // The driver task holds no state worth flushing on shutdown (an
        // in-flight send is already best-effort); abort it outright rather
        // than negotiating a graceful stop.
        self.driver.abort();
    }
}

impl Net {
    /// Sends `message` to `to`, fire-and-forget: delivery need not be
    /// immediate or reliable, and nothing reports whether it arrived. A `to`
    /// that is not a `WorkerId` this `Net`'s mapping produces goes nowhere.
    ///
    /// With no connection to `to`, this dials it first, at its dialable
    /// address of record (see [`Self::dialable_address`]) and any address
    /// the swarm's behaviours know for it. A roll call reply carries this
    /// node's own address (see the module doc's "Where a peer's address comes
    /// from").
    pub fn send(&self, to: WorkerId, message: ElectionMessage) {
        let Ok(peer) = PeerId::from_str(to.as_str()) else {
            // Not a WorkerId this mapping ever produced: nowhere to send.
            return;
        };
        // If the driver task has already stopped (this Net is concurrently
        // being dropped), there is nowhere for the command to go, and that
        // is fine to drop.
        let _ = self.commands.send(Command::Send { to: peer, message });
    }

    /// Subscribes this `Net` to `shard`'s gossip topic, so it receives what
    /// the shard's workers publish and [`Self::publish`] reaches them.
    /// Subscribing again to the same shard changes nothing. A `Net` serves
    /// one shard: subscribing to a second, different one is a caller bug,
    /// which fails a debug assertion and is otherwise ignored.
    pub fn subscribe_to_shard(&self, shard: &ShardId) {
        if let Some(records_shard) = &self.records_shard {
            debug_assert_eq!(
                *records_shard,
                shard.name(),
                "a Net stores the records of one shard; it cannot serve another"
            );
        }
        let mut subscribed = self.shard.lock().unwrap_or_else(PoisonError::into_inner);
        match &*subscribed {
            Some(current) => debug_assert_eq!(
                current, shard,
                "a Net serves one shard; it cannot subscribe to a second"
            ),
            None => {
                *subscribed = Some(shard.clone());
                // Same reasoning as in `send` for a stopped driver task.
                let _ = self.commands.send(Command::Subscribe {
                    topic: shard_topic(&shard.name()),
                });
            }
        }
    }

    /// Publishes `message` to every worker subscribed to this `Net`'s shard,
    /// fire-and-forget like [`Self::send`]: it may reach some of them and
    /// not others, and nothing reports which. Before
    /// [`Self::subscribe_to_shard`] there is no shard to publish to, and the
    /// message goes nowhere.
    pub fn publish(&self, message: ElectionMessage) {
        let Some(shard) = self
            .shard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        else {
            return;
        };
        // Same reasoning as in `send` for a stopped driver task.
        let _ = self.commands.send(Command::Publish {
            topic: shard_topic(&shard.name()).hash(),
            message,
        });
    }

    /// Writes each record to its placement. A holder that is this worker
    /// gets its copy put into this worker's own store, which counts towards
    /// the quorum; the others get it over the shard's records protocol, never
    /// with this worker named as publisher (a holder would acknowledge a
    /// record naming itself as publisher without storing it). Each outcome
    /// arrives once, through [`Self::take_write_outcomes`]. A `Net` that
    /// stores no shard's records ([`Self::new`]) refuses every write at once.
    pub fn write_records(&self, writes: Vec<PlacedWrite>) {
        if let Err(mpsc::error::SendError(Command::WriteRecords(writes))) =
            self.commands.send(Command::WriteRecords(writes))
        {
            // The swarm task has stopped: no write will ever be stored.
            for placed in writes {
                self.refuse_write(Write::of(&placed.record));
            }
        }
    }

    /// Hands `write.record`, a copy this worker holds, to `write.holders`
    /// over the shard's records protocol, naming this worker as its
    /// publisher, so a holder keeps the copy whatever holders the record
    /// names. Resolves once `write.quorum` of them stored it (`true`) or it
    /// could not be stored there (`false`): a holder that holds a newer
    /// revision refuses it. A `Net` that stores no shard's records
    /// ([`Self::new`]) resolves `false` at once.
    pub(crate) fn hand_off(&self, write: HandOffWrite) -> impl Future<Output = bool> + use<> {
        let (respond_to, stored) = oneshot::channel();
        let _ = self.commands.send(Command::HandOff { write, respond_to });
        async move { stored.await.unwrap_or(false) }
    }

    /// The newest revision of `task`'s record that this worker or any shard
    /// peer kad reaches holds: every answer of the lookup, this worker's own
    /// copy included, is compared by version and the newest kept; an
    /// undecodable one is skipped. Returns when the lookup ends; `None` if no
    /// one holds the record, this `Net` stores no shard's records
    /// ([`Self::new`]), or the swarm task has stopped.
    pub async fn get_record(&self, task: TaskId) -> Option<TaskRecord> {
        let (respond_to, answer) = oneshot::channel();
        self.commands
            .send(Command::GetRecord { task, respond_to })
            .ok()?;
        answer.await.ok().flatten()
    }

    /// The peers in this worker's records `kad` routing table: the shard
    /// peers a lookup can ask. Empty for a `Net` built with [`Self::new`], or
    /// if the swarm task has stopped. For tests and logs: these are routing
    /// candidates, not the shard's membership; nothing decides who belongs to
    /// the shard from this list.
    pub async fn records_routing_peers(&self) -> Vec<WorkerId> {
        let (respond_to, answer) = oneshot::channel();
        let sent = self.commands.send(Command::Exchange(Box::new(move |swarm, _| {
            let peers = match swarm.behaviour_mut().records.as_mut() {
                Some(records) => records
                    .kbuckets()
                    .flat_map(|bucket| {
                        bucket
                            .iter()
                            .map(|entry| WorkerId::new(entry.node.key.preimage().to_string()))
                            .collect::<Vec<_>>()
                    })
                    .collect(),
                None => Vec::new(),
            };
            let _ = respond_to.send(peers);
        })));
        if sent.is_err() {
            return Vec::new();
        }
        answer.await.unwrap_or_default()
    }

    /// The shard peers this worker's records `kad` routing table knows,
    /// grouped by distance class from this worker's key, nearest class
    /// first: the order a steal asks in. `kad` keeps a peer in the bucket
    /// that holds peers at its XOR distance, and the buckets run from near
    /// to far, so a class is one bucket. Read afresh on every call and kept
    /// by no one: it routes asks and says nothing about who is a member of
    /// the shard. Empty for a `Net` built with [`Self::new`], or if the swarm
    /// task has stopped.
    pub async fn steal_targets(&self) -> Vec<Vec<WorkerId>> {
        let (respond_to, answer) = oneshot::channel();
        let sent = self.commands.send(Command::Exchange(Box::new(move |swarm, _| {
            let classes = match swarm.behaviour_mut().records.as_mut() {
                Some(records) => records
                    .kbuckets()
                    .map(|bucket| {
                        bucket
                            .iter()
                            .map(|entry| worker_id_of(entry.node.key.preimage()))
                            .collect::<Vec<_>>()
                    })
                    .filter(|class| !class.is_empty())
                    .collect(),
                None => Vec::new(),
            };
            let _ = respond_to.send(classes);
        })));
        if sent.is_err() {
            return Vec::new();
        }
        answer.await.unwrap_or_default()
    }

    /// Ends `write` as refused, for a write that was never made.
    pub(crate) fn refuse_write(&self, write: Write) {
        self.inbound.queue_write(WriteOutcome {
            write,
            stored: false,
        });
    }

    /// Every write outcome not yet taken, in arrival order.
    pub fn take_write_outcomes(&self) -> Vec<WriteOutcome> {
        drain(&self.inbound.writes)
    }

    /// Takes every input for this worker's node queued since the last call,
    /// oldest first: each election message received, each peer this `Net`
    /// first connected to, and each peer it last lost its connection to or
    /// failed to dial while holding none. Inputs queue from the moment this
    /// `Net` is created, so a node built later, or driven again after a
    /// pause, is told of everything that happened in between, bar what the
    /// queue's bound leaves out (see the module doc's "Inputs for the
    /// node"): connection events that changed nothing, and the oldest
    /// messages past [`Self::with_input_limit`].
    pub fn take_inputs(&self) -> Vec<Input> {
        drain(&self.inbound.inputs)
    }

    /// Re-crawls this node's peer routing: starts a `kad` bootstrap, which
    /// asks the peers this node knows for the peers closest to it and
    /// connects to those it finds (see `crate::swarm`'s "kad: peer routing,
    /// not membership"). Gossipsub then meshes with the connected peers that
    /// serve the same shard, so a roll call published after the leader is
    /// lost still reaches them. `crate::driver::run_driver` calls it when
    /// its node's view of the shard changes and periodically (see
    /// `crate::driver::DriverConfig`). Fire-and-forget; with no peer
    /// known yet it does nothing.
    pub(crate) fn refresh_peer_routing(&self) {
        let _ = self.commands.send(Command::RefreshPeerRouting);
    }

    /// Asks this worker's node to leave its shard gracefully: queues
    /// `Input::Drain` for it, which the driver hands it in its next batch like
    /// any other input (see `kabudachi_core::election::Input::Drain` for what
    /// the node then does). This is how a worker being shut down, one replaced
    /// in a rolling deploy say, leaves without waiting to be suspected. Ask
    /// once: the node ignores a second request.
    pub fn request_drain(&self) {
        self.inbound.queue_input(Input::Drain);
    }

    /// Waits until something has arrived for this worker's driver since this
    /// last returned: an input for its node, a join request or a claim
    /// request. An arrival while no one waits is kept, so a caller that
    /// takes everything queued after each wait misses nothing; that also
    /// means this can return when what arrived was already taken. Wait from
    /// one task at a time.
    pub async fn wait_for_arrival(&self) {
        self.inbound.arrived.notified().await;
    }
}

/// The gossip topic the workers of the shard `name` publish election messages on. Every
/// worker in the shard must name it the same way, or they cannot hear each
/// other. It carries the same `ElectionMessage` schema as the direct
/// protocol ([`crate::codec::PROTOCOL`]), so its version moves in step with
/// that protocol's: a peer of another schema version never hears a message
/// it would misread.
fn shard_topic(name: &ShardName) -> gossipsub::IdentTopic {
    gossipsub::IdentTopic::new(format!("/kabudachi/{name}/election/2"))
}

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
    /// The `kad` queries of record writes in flight to other workers, each
    /// to the write it is part of.
    writes: HashMap<kad::QueryId, u64>,
    /// The record writes with a query still in flight, by the number each is
    /// known by here.
    unfinished: HashMap<u64, UnfinishedWrite>,
    /// The number the next record write is known by.
    next_write: u64,
    /// Hand-offs in flight to other workers, by their `kad` query, each to
    /// say whether it reached its quorum.
    hand_offs: HashMap<kad::QueryId, oneshot::Sender<bool>>,
    /// Record lookups in flight, by their `kad` query.
    reads: HashMap<kad::QueryId, Read>,
    /// Bootstrap queries that have had a failed step, until their last step.
    crawls: CrawlTracker,
}

/// A record lookup collecting the answers of its `kad` query.
struct Read {
    /// The newest answer so far, with its version.
    best: Option<(RecordVersion, TaskRecord)>,
    respond_to: oneshot::Sender<Option<TaskRecord>>,
}

impl Read {
    /// Keeps `value` if it decodes to a record newer than the best so far.
    fn offer(&mut self, value: &[u8]) {
        let Ok(record) = TaskRecord::decode(value) else {
            return;
        };
        let Ok((_, version)) = identify(&record) else {
            return;
        };
        let newer = self
            .best
            .as_ref()
            .is_none_or(|(best, _)| best.order(&version) == VersionOrder::Newer);
        if newer {
            self.best = Some((version, record));
        }
    }

    /// Ends the lookup with the best answer, to a caller who may have
    /// stopped waiting.
    fn finish(self) {
        let _ = self.respond_to.send(self.best.map(|(_, record)| record));
    }
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
async fn drive(
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
        Command::WriteRecords(writes) => {
            for placed in writes {
                write_record(swarm, placed, pending, inbound);
            }
        }
        Command::HandOff { write, respond_to } => hand_off_record(swarm, write, respond_to, pending),
        Command::GetRecord { task, respond_to } => {
            match swarm.behaviour_mut().records.as_mut() {
                Some(records) => {
                    let query = records.get_record(record_key(&task));
                    pending.reads.insert(
                        query,
                        Read {
                            best: None,
                            respond_to,
                        },
                    );
                }
                None => {
                    let _ = respond_to.send(None);
                }
            }
        }
    }
}

/// Starts one record write (see [`Net::write_records`]): puts the copy of a
/// holder that is this worker into its own store, and writes the others'
/// over the records protocol, to each placement the write must reach at the
/// quorum of it still missing. A holder of two placements is written twice,
/// once for each quorum that counts it. The write is stored once every
/// placement's quorum has stored it, and refused as soon as one cannot.
fn write_record(
    swarm: &mut Swarm<Behaviour>,
    placed: PlacedWrite,
    pending: &mut Pending,
    inbound: &Inbound,
) {
    let write = Write::of(&placed.record);
    let local = *swarm.local_peer_id();
    let Some(records) = swarm.behaviour_mut().records.as_mut() else {
        inbound.queue_write(WriteOutcome {
            write,
            stored: false,
        });
        return;
    };
    let value = placed.record.encode_to_vec();
    let key = record_key(&write.task_id);
    // Every holder the write goes to: this worker's own copy first, so its
    // acknowledgement counts for each placement it belongs to.
    let goes_here = placed
        .recipients()
        .iter()
        .any(|holder| PeerId::from_str(holder.as_str()).is_ok_and(|peer| peer == local));
    let stored_here = goes_here
        && records
            .store_mut()
            .put(kad::Record::new(key.clone(), value.clone()))
            .is_ok();
    let mut queries = Vec::new();
    let placements = std::iter::once((placed.holders(), placed.quorum))
        .chain(placed.prior.iter().map(|prior| (prior.holders.clone(), prior.quorum)));
    for (holders, quorum) in placements {
        let mut acked = 0;
        let mut remote = Vec::new();
        for holder in holders {
            let Ok(peer) = PeerId::from_str(holder.as_str()) else {
                continue;
            };
            if peer == local {
                acked += usize::from(stored_here);
            } else {
                remote.push(peer);
            }
        }
        let needed = quorum.saturating_sub(acked);
        if needed == 0 {
            continue;
        }
        if remote.len() < needed {
            inbound.queue_write(WriteOutcome {
                write,
                stored: false,
            });
            return;
        }
        queries.push((remote, needed));
    }
    if queries.is_empty() {
        inbound.queue_write(WriteOutcome {
            write,
            stored: true,
        });
        return;
    }
    let number = pending.next_write;
    pending.next_write += 1;
    pending.unfinished.insert(
        number,
        UnfinishedWrite {
            write,
            outstanding: queries.len(),
        },
    );
    for (remote, needed) in queries {
        let quorum = kad::Quorum::N(NonZeroUsize::new(needed).expect("needed is not zero here"));
        let query = records.put_record_to(
            kad::Record::new(key.clone(), value.clone()),
            remote.into_iter(),
            quorum,
        );
        pending.writes.insert(query, number);
    }
}

/// Starts one hand-off (see [`Net::hand_off`]): the copy goes to each holder
/// but this worker, under this worker's name as its publisher, at the quorum
/// the hand-off asks.
fn hand_off_record(
    swarm: &mut Swarm<Behaviour>,
    write: HandOffWrite,
    respond_to: oneshot::Sender<bool>,
    pending: &mut Pending,
) {
    let local = *swarm.local_peer_id();
    let Some(records) = swarm.behaviour_mut().records.as_mut() else {
        let _ = respond_to.send(false);
        return;
    };
    let Ok((task, _)) = identify(&write.record) else {
        let _ = respond_to.send(false);
        return;
    };
    let remote: Vec<PeerId> = write
        .holders
        .iter()
        .filter_map(|holder| PeerId::from_str(holder.as_str()).ok())
        .filter(|peer| *peer != local)
        .collect();
    let Some(needed) = NonZeroUsize::new(write.quorum.max(1)).filter(|needed| needed.get() <= remote.len()) else {
        let _ = respond_to.send(false);
        return;
    };
    let mut record = kad::Record::new(record_key(&task), write.record.encode_to_vec());
    record.publisher = Some(local);
    let query = records.put_record_to(record, remote.into_iter(), kad::Quorum::N(needed));
    pending.hand_offs.insert(query, respond_to);
}

/// A record write that waits for the queries it started: one for each
/// placement it must reach, each to a quorum of that placement.
struct UnfinishedWrite {
    write: Write,
    /// The queries not answered yet.
    outstanding: usize,
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
        SwarmEvent::Behaviour(BehaviourEvent::Records(kad::Event::OutboundQueryProgressed {
            id,
            result: kad::QueryResult::PutRecord(result),
            ..
        })) => {
            if let Some(number) = pending.writes.remove(&id) {
                // A write already refused has no entry left: its other queries
                // end unheard.
                if let Some(mut unfinished) = pending.unfinished.remove(&number) {
                    unfinished.outstanding -= 1;
                    match &result {
                        Err(error) => {
                            tracing::debug!(
                                task = unfinished.write.task_id.as_str(),
                                %error,
                                "a record write did not reach its quorum"
                            );
                            inbound.queue_write(WriteOutcome {
                                write: unfinished.write,
                                stored: false,
                            });
                        }
                        Ok(_) if unfinished.outstanding == 0 => {
                            inbound.queue_write(WriteOutcome {
                                write: unfinished.write,
                                stored: true,
                            });
                        }
                        Ok(_) => {
                            pending.unfinished.insert(number, unfinished);
                        }
                    }
                }
            } else if let Some(respond_to) = pending.hand_offs.remove(&id) {
                if let Err(error) = &result {
                    tracing::debug!(%error, "a record hand-off did not reach its quorum");
                }
                let _ = respond_to.send(result.is_ok());
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Records(kad::Event::OutboundQueryProgressed {
            id,
            result: kad::QueryResult::GetRecord(result),
            step,
            ..
        })) => {
            if let Some(read) = pending.reads.get_mut(&id) {
                if let Ok(kad::GetRecordOk::FoundRecord(found)) = &result {
                    read.offer(&found.record.value);
                }
            }
            // The lookup's last step, whether it found records or not.
            if step.last {
                if let Some(read) = pending.reads.remove(&id) {
                    read.finish();
                }
            }
        }
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

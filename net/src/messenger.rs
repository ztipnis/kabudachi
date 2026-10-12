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
//! ## Blobs
//!
//! A `Net` built with [`Net::for_shard`] stores the shard's blobs in the
//! same records store ([`Net::put_blob`]), and fetches one from the holders
//! of its owner's record over `/kabudachi/blob/1` (see
//! `crate::blob_exchange`), which the swarm task answers itself, falling
//! back to the kad lookup ([`Net::get_blob`]).
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
//! the swarm task's `identified` (see `crate::swarm`'s "kad: peer routing, not
//! membership"). So a peer this node never itself connected to can still be
//! reached, once some other peer's Identify has given `kad` a route to it.

mod record_queries;
mod swarm_task;

use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord, MAX_BLOB_BYTES};
use kabudachi_core::election::Input;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{ShardId, ShardName, TaskId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
pub use kabudachi_core::task_record::{PlacedWrite, WriteOutcome};
use kabudachi_core::task_record::{Placement, Write};
use libp2p::futures::StreamExt as _;
use libp2p::futures::stream::FuturesUnordered;
use libp2p::request_response::{self, ResponseChannel};
use libp2p::{Multiaddr, PeerId, Swarm, gossipsub};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::blob_exchange::codec::BlobCodec;
use crate::claim::codec::ClaimCodec;
use crate::claimed_runs::ClaimedRuns;
use crate::exchange::{Asked, Exchange};
use crate::join_codec::JoinCodec;
pub use crate::peers::{Diagnostics, RedialPolicy, Traffic};
use crate::payload_memory::PayloadMemory;
use crate::peers::{Carried, Peers, worker_id_of};
use crate::reconcile::codec::ReconcileCodec;
use crate::result_transport::ResultEvent;
use crate::result_transport::codec::ResultCodec;
use crate::steal::codec::StealCodec;
use crate::swarm::{Behaviour, build_swarm};
use crate::task_exchange::{NamedLeader, OwnTask};
use crate::task_exchange::codec::TaskCodec;
use crate::task_store::HeldRecords;

/// The first pause before [`Net::ask_until_accepted`] asks again.
const ASK_AGAIN_FIRST: Duration = Duration::from_millis(100);
/// The longest pause between its asks.
const ASK_AGAIN_LONGEST: Duration = Duration::from_secs(1);
/// How long a direct blob ask waits for its holder: an answer is one read of
/// a store, so a holder that takes longer is passed over for the next.
const BLOB_ASK_TIMEOUT: Duration = Duration::from_secs(2);

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
    /// See `Net::unblock_peer`.
    Unblock {
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
    /// Stores a blob here and at a quorum of its placement (see
    /// [`Net::put_blob`]).
    PutBlob {
        key: BlobKey,
        record: BlobRecord,
        placement: Placement,
        respond_to: oneshot::Sender<Result<(), PutBlobError>>,
    },
    /// Finds a blob by the kad lookup (see [`Net::get_blob`]'s last resort).
    LookUpBlob {
        key: BlobKey,
        respond_to: oneshot::Sender<Option<Vec<u8>>>,
    },
}

/// Why a blob was not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutBlobError {
    #[error("a blob of {size} bytes is past the {limit} a holder keeps")]
    TooLarge { size: usize, limit: usize },
    /// This worker's own store, which keeps a copy of every blob it writes,
    /// has no room for it.
    #[error("this worker's blob store is full")]
    StoreFull,
    /// Not enough of the placement stored it before the write timed out, or
    /// this `Net` stores no shard's records.
    #[error("the blob was not stored by a quorum of its placement")]
    NotStored,
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
    result: Exchange<ResultCodec>,
    blob: Exchange<BlobCodec>,
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
}

/// A correlated protocol whose inbound requests wait in a queue for the
/// driver to answer. The blob exchange is answered by the swarm task itself,
/// so it has none.
pub(crate) trait Queued: Correlated {
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
}

impl Queued for JoinCodec {
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
}

impl Queued for ClaimCodec {
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
}

impl Queued for TaskCodec {
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
}

impl Queued for ReconcileCodec {
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
}

impl Queued for StealCodec {
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.steals
    }
}

impl Correlated for ResultCodec {
    const ARRIVAL: Carried = Carried::ResultMessage;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.result
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.result
    }
}

impl Queued for ResultCodec {
    fn queue(inbound: &Inbound) -> &Mutex<VecDeque<Asked<Self>>> {
        &inbound.results
    }
}

impl Correlated for BlobCodec {
    const ARRIVAL: Carried = Carried::BlobRequest;
    fn behaviour(behaviour: &mut Behaviour) -> &mut request_response::Behaviour<Self> {
        &mut behaviour.blob
    }
    fn exchange(exchanges: &mut Exchanges) -> &mut Exchange<Self> {
        &mut exchanges.blob
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
/// driver answers, this worker's own task requests, the outcomes of the record writes it
/// asked for, and the result messages for this worker as a client.
/// `arrived` is signalled whenever any of the driver's queues grows; `results_arrived` whenever a
/// result message arrives.
pub(crate) struct Inbound {
    inputs: Mutex<VecDeque<Input>>,
    /// See [`Net::with_input_limit`].
    input_limit: AtomicUsize,
    joins: Mutex<VecDeque<Asked<JoinCodec>>>,
    claims: Mutex<VecDeque<Asked<ClaimCodec>>>,
    tasks: Mutex<VecDeque<Asked<TaskCodec>>>,
    reconciles: Mutex<VecDeque<Asked<ReconcileCodec>>>,
    steals: Mutex<VecDeque<Asked<StealCodec>>>,
    /// `None` once the driver has stopped: nothing will answer a request
    /// made then, so it is dropped (and its asker told) at once.
    own_tasks: Mutex<Option<VecDeque<OwnTask>>>,
    writes: Mutex<VecDeque<WriteOutcome>>,
    arrived: Notify,
    results: Mutex<VecDeque<Asked<ResultCodec>>>,
    /// Result messages this worker sent itself, as a client of its own task.
    own_results: Mutex<VecDeque<ResultEvent>>,
    /// Signalled when a result message arrives, for whoever reads them.
    pub(crate) results_arrived: Notify,
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
            own_tasks: Mutex::new(Some(VecDeque::new())),
            writes: Mutex::default(),
            arrived: Notify::new(),
            results: Mutex::default(),
            own_results: Mutex::default(),
            results_arrived: Notify::new(),
        }
    }
}

impl Inbound {
    /// Queues `input` for the node, keeping the queue bounded while no
    /// driver takes from it (see the module doc's "Inputs for the node").
    fn queue_input(&self, input: Input) {
        let mut inputs = self.inputs.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.admit(&mut inputs, &input) {
            return;
        }
        inputs.push_back(input);
        drop(inputs);
        self.arrived.notify_one();
    }

    /// Whether `input` joins the queue, after making room for it if the
    /// queue is at its limit. A connection event joins unless the node
    /// would learn nothing from it (see [`admit_connection_event`]). A
    /// routing crawl joins unless one is queued already: the node handles
    /// one the same as two, and it is never dropped for the limit. Any
    /// other input joins, and at the limit the oldest queued message is
    /// dropped for it: connection events are never dropped, since at most
    /// two per peer are ever queued, so only messages can fill the queue.
    fn admit(&self, inputs: &mut VecDeque<Input>, input: &Input) -> bool {
        if let Some(peer) = connection_event_peer(input) {
            return admit_connection_event(inputs, peer, input);
        }
        if *input == Input::RoutingCrawled {
            return !inputs.contains(input);
        }
        let limit = self.input_limit.load(Ordering::Relaxed);
        if inputs.len() >= limit {
            drop_oldest_message(inputs, limit);
        }
        true
    }

    fn queue_write(&self, outcome: WriteOutcome) {
        push(&self.writes, outcome);
        self.arrived.notify_one();
    }

    fn queue_asked<T>(&self, queue: &Mutex<VecDeque<T>>, asked: T) {
        push(queue, asked);
        self.arrived.notify_one();
    }

    /// Queues a task-exchange request this worker made of itself. Once the
    /// own-task queue is closed the request is dropped, which its asker hears
    /// as unanswered.
    pub(crate) fn queue_own_task(&self, task: OwnTask) {
        let mut own_tasks = self.own_tasks.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(queue) = own_tasks.as_mut() else {
            return;
        };
        queue.push_back(task);
        drop(own_tasks);
        self.arrived.notify_one();
    }

    pub(crate) fn take_own_tasks(&self) -> Vec<OwnTask> {
        self.own_tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .map(|queue| queue.drain(..).collect())
            .unwrap_or_default()
    }

    /// Closes the own-task queue for good, once no driver will answer it:
    /// each request queued and each made later is dropped, so its asker hears
    /// it unanswered instead of waiting forever.
    pub(crate) fn close_own_tasks(&self) {
        let queued = self
            .own_tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(queued);
    }

    pub(crate) fn queue_own_result(&self, event: ResultEvent) {
        push(&self.own_results, event);
        self.results_arrived.notify_one();
    }

    pub(crate) fn take_own_results(&self) -> Vec<ResultEvent> {
        drain(&self.own_results)
    }
}

/// Whether `input`, a connection event of `peer`'s, joins the queue. A
/// repeat of the peer's last queued event does not: the node would ignore
/// it. Nor does a disconnect while the peer's connect is still queued: it
/// removes that connect instead, since together they change nothing the
/// node needs to know.
fn admit_connection_event(inputs: &mut VecDeque<Input>, peer: &WorkerId, input: &Input) -> bool {
    let latest = inputs
        .iter()
        .rposition(|queued| connection_event_peer(queued) == Some(peer));
    match latest.map(|index| (index, &inputs[index])) {
        Some((_, queued)) if queued == input => false,
        Some((index, Input::PeerConnected(_))) => {
            inputs.remove(index);
            false
        }
        _ => true,
    }
}

fn drop_oldest_message(inputs: &mut VecDeque<Input>, limit: usize) {
    if let Some(oldest) = inputs
        .iter()
        .position(|queued| matches!(queued, Input::Message { .. }))
    {
        inputs.remove(oldest);
        tracing::debug!(
            limit,
            "dropping the oldest queued election message: no driver has taken this \
             node's inputs"
        );
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
    pub(crate) inbound: Arc<Inbound>,
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
    /// The blob payloads this worker holds whole in flight (see
    /// [`Self::payload_memory`]).
    payload_memory: Arc<PayloadMemory>,
    /// The runs this worker claimed (see [`Self::claimed_runs`]).
    pub(crate) claimed: ClaimedRuns,
    /// The leader this worker's node last named, and whether the worker has
    /// left its shard (see [`Self::submit_to_leader`]).
    pub(crate) named_leader: watch::Sender<NamedLeader>,
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

        let driver = tokio::spawn(swarm_task::drive(swarm, identify_config, command_rx, inbound.clone(), peers));

        Self {
            local_worker_id,
            commands,
            inbound,
            local_addr,
            shard: Mutex::new(None),
            records_shard,
            held,
            payload_memory: PayloadMemory::new(None),
            claimed: ClaimedRuns::default(),
            named_leader: watch::Sender::new(NamedLeader::default()),
            driver,
        }
    }

    /// The records this worker holds (empty for a `Net` built with
    /// [`Self::new`]).
    pub fn held_records(&self) -> HeldRecords {
        self.held.clone()
    }

    /// The ledger of blob payloads this worker holds whole while they cross
    /// the network. Unlimited until [`crate::worker::Worker::run`] gives it
    /// its share of the worker's memory limits.
    pub fn payload_memory(&self) -> &Arc<PayloadMemory> {
        &self.payload_memory
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
    /// instant. [`Net::unblock_peer`] heals the partition.
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

    /// Lets `peer` through again after [`Net::block_peer`], as a healed
    /// partition would: connections either way are allowed from now on, and
    /// the next send or redial to `peer` connects. Heal both sides, as both
    /// were blocked. Fire-and-forget, like `block_peer`; a no-op for a peer
    /// never blocked, or one this mapping never produced.
    pub fn unblock_peer(&self, peer: WorkerId) {
        let Ok(peer) = PeerId::from_str(peer.as_str()) else {
            return;
        };
        let _ = self.commands.send(Command::Unblock { peer });
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
}

/// The correlated protocols' requests and answers, through [`Exchanges`].
impl Net {
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

    /// Asks `to` over protocol `C` until it answers with something
    /// `accepted` takes, or `within` has passed; whether it did. A
    /// failed or refused ask is asked again after a pause that starts at
    /// [`ASK_AGAIN_FIRST`] and doubles up to [`ASK_AGAIN_LONGEST`]. The
    /// future owns all it needs, so it can be spawned.
    pub(crate) fn ask_until_accepted<C: Correlated>(
        &self,
        to: PeerId,
        request: C::Request,
        accepted: fn(&C::Response) -> bool,
        within: Duration,
    ) -> impl Future<Output = bool> + Send + use<C>
    where
        C::Request: Clone + Send + 'static,
        C::Response: Send + 'static,
    {
        let commands = self.commands.clone();
        async move {
            let deadline = tokio::time::Instant::now() + within;
            let mut pause = ASK_AGAIN_FIRST;
            loop {
                let (respond_to, response) = oneshot::channel();
                let asked = request.clone();
                let sent = commands.send(Command::Exchange(Box::new(move |swarm, exchanges| {
                    C::exchange(exchanges).ask(C::behaviour(swarm.behaviour_mut()), &to, asked, respond_to);
                })));
                if sent.is_err() {
                    return false;
                }
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if let Ok(Ok(Some(answer))) = tokio::time::timeout(left, response).await
                    && accepted(&answer)
                {
                    return true;
                }
                if tokio::time::Instant::now() + pause >= deadline {
                    return false;
                }
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(ASK_AGAIN_LONGEST);
            }
        }
    }

    /// Drains every inbound request of protocol `C` not yet answered.
    pub(crate) fn take_asked<C: Queued>(&self) -> Vec<Asked<C>> {
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

    /// Stores `bytes` as a blob owned by `owner`, in this worker's own store
    /// and at `placement.quorum` of `placement.holders`. This worker's copy
    /// counts towards the quorum when it is one of the holders, and is kept
    /// either way. Resolves with the blob's digest once the quorum has
    /// stored it.
    pub async fn put_blob(
        &self,
        owner: BlobOwner,
        bytes: Vec<u8>,
        placement: Placement,
    ) -> Result<Digest, PutBlobError> {
        put_blob_through(&self.commands, owner, bytes, placement, false).await
    }

    /// The bytes of the blob `digest` names, which `owner` stored. Looked
    /// for, in turn, in this worker's own store; at the holders of `owner`'s
    /// record, asked directly and all at once; and, if they all miss, by the
    /// kad lookup of the blob's key. Every answer is checked against the
    /// digest. `None` if no one reachable holds it.
    pub async fn get_blob(&self, owner: BlobOwner, digest: &Digest) -> Option<Vec<u8>> {
        let key = BlobKey::from(digest);
        if let Some(here) = self.held.blob(&key) {
            return Some(here.value);
        }
        if let Some(found) = self.ask_holders_of(&owner, digest, &key).await {
            return Some(found);
        }
        let (respond_to, answer) = oneshot::channel();
        self.commands.send(Command::LookUpBlob { key, respond_to }).ok()?;
        answer.await.ok().flatten()
    }

    /// The blob `key` names, from the first holder of `owner`'s record to
    /// answer with bytes that check out.
    async fn ask_holders_of(&self, owner: &BlobOwner, digest: &Digest, key: &BlobKey) -> Option<Vec<u8>> {
        let holders = match owner {
            BlobOwner::Task(task) => match self.held.get(task) {
                Some(record) => record.placement,
                None => self.get_record(task.clone()).await.map(|record| record.placement).unwrap_or_default(),
            },
            // A composite's record holders are found once composite records exist.
            BlobOwner::Composite(_) => Vec::new(),
        };
        let me = self.local_worker_id();
        let mut asks: FuturesUnordered<_> = holders
            .into_iter()
            .map(WorkerId::from)
            .filter(|holder| *holder != me)
            .filter_map(|holder| PeerId::from_str(holder.as_str()).ok())
            .map(|peer| {
                let asked = self.ask::<BlobCodec>(peer, digest.clone().into());
                async move { tokio::time::timeout(BLOB_ASK_TIMEOUT, asked).await.ok().flatten() }
            })
            .collect();
        while let Some(answer) = asks.next().await {
            if let Some(record) = answer
                && key.holds(&record.value)
            {
                return Some(record.value);
            }
        }
        None
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
/// Sends one blob write to the swarm task (see [`Net::put_blob`]). `result`
/// says the blob is stored as a run's result, not as an input.
async fn put_blob_through(
    commands: &mpsc::UnboundedSender<Command>,
    owner: BlobOwner,
    bytes: Vec<u8>,
    placement: Placement,
    result: bool,
) -> Result<Digest, PutBlobError> {
    if bytes.len() > MAX_BLOB_BYTES {
        return Err(PutBlobError::TooLarge { size: bytes.len(), limit: MAX_BLOB_BYTES });
    }
    let key = BlobKey::of(&bytes);
    let record = BlobRecord { value: bytes, owner, placement: placement.holders.clone(), result };
    let (respond_to, stored) = oneshot::channel();
    commands
        .send(Command::PutBlob { key: key.clone(), record, placement, respond_to })
        .map_err(|_| PutBlobError::NotStored)?;
    stored.await.unwrap_or(Err(PutBlobError::NotStored))?;
    Ok(key.digest())
}

fn shard_topic(name: &ShardName) -> gossipsub::IdentTopic {
    gossipsub::IdentTopic::new(format!("/kabudachi/{name}/election/2"))
}

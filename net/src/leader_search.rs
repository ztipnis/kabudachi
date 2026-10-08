//! The leader search: how a worker that is not in its shard finds a leader to
//! join, over the JOIN client ([`crate::join::ask_for_leader`]).
//!
//! The one thing it needs from a socket is [`AskWhoLeads`], a pass over
//! addresses that says who leads. Everything else is decided here, without
//! I/O, so it runs in-process over a scripted port:
//!
//! - [`SearchRounds`] is the search, round by round: which addresses each
//!   round asks, what an answer means, and what a round that found no leader
//!   logs. It is synchronous around the port call: the caller runs the call,
//!   so neither the port nor the authority is borrowed across an await.
//! - [`Rejoin`] searches for a node back in `Bootstrapping`, and for a node
//!   stranded with no reachable peer.
//! - [`DrivenSearch`] is the search a driven node runs: which one its state
//!   calls for, the replies to the calls net asked for itself, and what the
//!   node must be told.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::{LiveRegistrations, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityReply, AuthorityRequest, CallKind, Input, JoinFloor, ReplyToken,
};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, Instant as CoreInstant};
use libp2p::Multiaddr;
use tokio::time::Instant;

use crate::authority::AuthorityClient;
use crate::join::{LeaderSearch, ask_for_leader};
use crate::messenger::Net;
use crate::wait_log::{WaitLog, WaitReason};

/// Asks addresses who leads the shard: the leader search's one socket
/// dependency.
pub(crate) trait AskWhoLeads {
    /// One pass over `addresses`: `Found` with the newest pointer `floor`
    /// accepts (see [`JoinFloor::newest_first`]) to a leader this worker then
    /// reaches (the one asked first among equally new), else
    /// `NoReachableLeader` if any answered, else `NoAnswer`.
    fn ask(
        &mut self,
        addresses: &[Multiaddr],
        floor: JoinFloor,
    ) -> impl Future<Output = LeaderSearch> + Send;
}

/// The JOIN client over a real `Net` ([`ask_for_leader`]).
#[derive(Clone, Copy)]
pub(crate) struct JoinOverNet<'a> {
    pub(crate) net: &'a Net,
    pub(crate) per_peer_timeout: StdDuration,
    /// How long past the first pointer a pass keeps listening for others (see
    /// [`ask_for_leader`]).
    pub(crate) grace: StdDuration,
}

impl AskWhoLeads for JoinOverNet<'_> {
    fn ask(
        &mut self,
        addresses: &[Multiaddr],
        floor: JoinFloor,
    ) -> impl Future<Output = LeaderSearch> + Send {
        ask_for_leader(self.net, addresses, floor, self.per_peer_timeout, self.grace)
    }
}

/// Whether a search asks seeds and learns that its shard exists (bootstrap),
/// or knows it exists and starts each round one listed worker further along
/// (rejoin).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Bootstrap,
    Rejoin,
}

/// A leader search, round by round.
pub(crate) struct SearchRounds {
    my_id: WorkerId,
    seeds: Vec<Multiaddr>,
    mode: Mode,
    round: usize,
    /// Set once any seed or listed worker answers: the shard exists, so this
    /// worker must never found it.
    shard_exists: bool,
    /// The workers the last [`Self::to_ask`] listed, while a pass over them
    /// is due an answer.
    listed: Vec<WorkerId>,
    log: WaitLog,
}

impl SearchRounds {
    pub(crate) fn for_bootstrap(shard_id: &ShardId, my_id: WorkerId, seeds: Vec<Multiaddr>) -> Self {
        SearchRounds {
            my_id,
            seeds,
            mode: Mode::Bootstrap,
            round: 0,
            shard_exists: false,
            listed: Vec::new(),
            log: WaitLog::new(&shard_id.name()),
        }
    }

    pub(crate) fn for_rejoin(shard_id: &ShardId, my_id: WorkerId, seeds: Vec<Multiaddr>) -> Self {
        SearchRounds {
            my_id,
            seeds,
            mode: Mode::Rejoin,
            round: 0,
            shard_exists: true,
            listed: Vec::new(),
            log: WaitLog::new(&shard_id.name()),
        }
    }

    /// The seeds a round asks, in order. A rejoin keeps them in place: only
    /// the listed workers rotate.
    pub(crate) fn seeds(&self) -> &[Multiaddr] {
        &self.seeds
    }

    /// What an ask of the seeds found: the pointer, if any. Any answer shows
    /// the shard exists.
    pub(crate) fn heard_from_seeds(&mut self, found: LeaderSearch) -> Option<JoinResponse> {
        match found {
            LeaderSearch::Found(pointer) => Some(pointer),
            LeaderSearch::NoReachableLeader => {
                self.shard_exists = true;
                None
            }
            LeaderSearch::NoAnswer => None,
        }
    }

    /// What an ask of the seeds alone found, in a round with no listing: the
    /// pointer, if any. Any answer shows the shard exists. Logs `SeedsNotAnswering`
    /// when none answered.
    pub(crate) fn heard_from_seeds_alone(&mut self, found: LeaderSearch) -> Option<JoinResponse> {
        if matches!(found, LeaderSearch::NoAnswer) {
            self.log.log(WaitReason::SeedsNotAnswering);
        }
        self.heard_from_seeds(found)
    }

    /// The registered addresses of `peers` to ask this round. An address that
    /// does not parse is logged and skipped, and so is a list none of whose
    /// addresses parse. They are asked from the first in a bootstrap, and
    /// rotated by the round number in a rejoin.
    pub(crate) fn to_ask(&mut self, peers: &BTreeMap<WorkerId, String>) -> Vec<Multiaddr> {
        let mut addresses: Vec<Multiaddr> = Vec::new();
        for (worker, address) in peers {
            match address.parse() {
                Ok(address) => addresses.push(address),
                Err(error) => self.log.log(WaitReason::UnparseableAddress {
                    worker: worker.clone(),
                    address: address.clone(),
                    error: error.to_string(),
                }),
            }
        }
        if addresses.is_empty() {
            // No one else listed is not an address problem: stay quiet.
            if !peers.is_empty() {
                self.log.log(WaitReason::NoRegisteredAddressParses {
                    peers: peers.keys().cloned().collect(),
                });
            }
            self.listed.clear();
            return addresses;
        }
        self.listed = peers.keys().cloned().collect();
        if self.mode == Mode::Rejoin {
            let len = addresses.len();
            addresses.rotate_left(self.round % len);
        }
        addresses
    }

    /// What an ask of [`Self::to_ask`]'s addresses found: the pointer, if any.
    /// Any answer shows the shard exists. Logs `RegisteredPeersSilent` when
    /// none answered.
    pub(crate) fn heard_from_listed(&mut self, found: LeaderSearch) -> Option<JoinResponse> {
        let listed = std::mem::take(&mut self.listed);
        match found {
            LeaderSearch::Found(pointer) => Some(pointer),
            LeaderSearch::NoReachableLeader => {
                self.shard_exists = true;
                None
            }
            LeaderSearch::NoAnswer => {
                if !listed.is_empty() {
                    self.log.log(WaitReason::RegisteredPeersSilent { peers: listed });
                }
                None
            }
        }
    }

    /// The workers `registrations` lists other than this one.
    pub(crate) fn others_in(&self, registrations: &LiveRegistrations) -> BTreeMap<WorkerId, String> {
        others_listed(registrations, &self.my_id)
    }

    pub(crate) fn shard_exists(&self) -> bool {
        self.shard_exists
    }

    pub(crate) fn log(&mut self, reason: WaitReason) {
        self.log.log(reason);
    }

    /// Ends the round: logs `NoReachableLeader` if the shard exists (bootstrap
    /// only: a rejoin knows it does, and says nothing of it), compares this
    /// round's reasons with the last, and moves the rotation on.
    pub(crate) fn end_round(&mut self) {
        if self.mode == Mode::Bootstrap && self.shard_exists {
            self.log.log(WaitReason::NoReachableLeader);
        }
        self.log.end_round();
        self.round += 1;
    }
}

/// The workers `registrations` lists other than `my_id`, at their registered
/// addresses.
pub(crate) fn others_listed(
    registrations: &LiveRegistrations,
    my_id: &WorkerId,
) -> BTreeMap<WorkerId, String> {
    registrations
        .addresses()
        .iter()
        .filter(|(worker_id, _)| *worker_id != my_id)
        .map(|(worker_id, address)| (worker_id.clone(), address.clone()))
        .collect()
}

/// A node back in `Bootstrapping`, rejoining its shard through the driver.
/// It never founds: nothing here can register or
/// swap an epoch. Each round reads the authority's listing through the
/// driver's client, under the client's `Issuer::Cascade` mint, bounded by one
/// retry interval, then asks the listed workers through the port. It also
/// reads the authority's recovery epoch beside the listing and hands it to
/// its caller, whose node may take it as its floor; each round asks the port
/// with the floor its caller gives with the listing's answer. Rounds are
/// one retry interval apart, so a round after a refused pointer waits one
/// first. A node `Joining` on a pointer it took keeps this rejoin alive:
/// it reads the epoch, at most once a retry interval, until the node is told.
pub(crate) struct Rejoin<'a, P> {
    search: SearchRounds,
    port: P,
    /// The floor of the last listing's answer, for a round that asks the seeds
    /// alone.
    floor: JoinFloor,
    retry_interval: StdDuration,
    /// When the next round is due, while none is under way.
    next_round_at: Option<Instant>,
    /// The listing this rejoin asked for and has not been answered.
    read: Option<PendingRead>,
    /// The authority's recovery epoch this rejoin asked for and has not been
    /// answered. Best effort: a round neither waits on it nor ends for it. A
    /// read past its bound is given up on (see [`Self::ask_epoch`]).
    epoch_read: Option<PendingEpochRead>,
    /// The earliest an epoch read is asked again: a retry interval after the
    /// last was asked (or after a busy kind was found), so an authority that
    /// holds no epoch, or lags, is never asked in a loop, and a reply that
    /// arrives late never delays the next read past the next round.
    epoch_ask_due: Instant,
    /// Whether a round also reads the authority's epoch. A search for a leader
    /// to reconnect to has no node waiting on that epoch, so it skips the read
    /// and leaves that call kind free.
    reads_epoch: bool,
    /// The node is `Joining` on a pointer it took, so this rejoin runs no
    /// round: it only reads the epoch until the node is told of it.
    validating: bool,
    /// An epoch read asked and not yet reported to the node, which keeps the
    /// latest read it was told of (see [`Self::take_asked_epoch_read`]).
    asked_epoch_read: Option<ReplyToken>,
    /// The round's ask of the listed workers, while it runs.
    asking: Option<Pin<Box<dyn Future<Output = LeaderSearch> + Send + 'a>>>,
    /// Whether the round's ask is of the seeds alone.
    asking_seeds_only: bool,
}

/// A read of the authority's epoch, asked and not yet answered or given up
/// on.
#[derive(Clone, Copy)]
struct PendingEpochRead {
    token: ReplyToken,
    /// When the read is given up on.
    bound: Instant,
}

/// A read of the listing, asked and not yet answered.
struct PendingRead {
    token: ReplyToken,
    /// When the round waiting on it gives up; `None` once it has, while the
    /// read stays pending for a later round.
    bound: Option<Instant>,
}

impl<'a, P: AskWhoLeads + Clone + Send + 'a> Rejoin<'a, P> {
    /// The first round is due at `now`.
    pub(crate) fn new(
        shard_id: &ShardId,
        my_id: WorkerId,
        port: P,
        seeds: Vec<Multiaddr>,
        retry_interval: StdDuration,
        now: Instant,
    ) -> Self {
        Rejoin {
            search: SearchRounds::for_rejoin(shard_id, my_id, seeds),
            port,
            floor: JoinFloor::none(),
            retry_interval,
            next_round_at: Some(now),
            read: None,
            epoch_read: None,
            epoch_ask_due: now,
            reads_epoch: true,
            validating: false,
            asked_epoch_read: None,
            asking: None,
            asking_seeds_only: false,
        }
    }

    /// This search runs for a stranded node's leader, not to rejoin: its rounds
    /// never read the authority's epoch.
    pub(crate) fn for_leader_search(mut self) -> Self {
        self.reads_epoch = false;
        self
    }

    /// When the driver must next wake for this rejoin: a round due, or a
    /// read's bound; while validating, the epoch read's bound, or when the
    /// next is due.
    pub(crate) fn wake_at(&self) -> Option<Instant> {
        if self.validating {
            return Some(match self.epoch_read {
                Some(read) => read.bound,
                None => self.epoch_ask_due,
            });
        }
        if self.asking.is_some() {
            return None;
        }
        match &self.read {
            Some(PendingRead { bound: Some(bound), .. }) => Some(*bound),
            _ => self.next_round_at,
        }
    }

    /// At `now`, a due round asks `client` for the listing, and for the
    /// authority's recovery epoch unless that read is still out, stamping the
    /// calls `sent_at` on the node's clock. A round whose read is past its bound
    /// ends (`AuthorityNotAnswering`), and the read stays pending for a later
    /// round. A due round that finds the kind busy with a call it did not ask
    /// (a read the cascade left in flight) asks nothing and ends the same
    /// way, so the next round is one retry interval later: a busy kind never
    /// stalls the rejoin and never spins it. Where a round has no listing to
    /// ask (no `client`, a failed read, a missed bound, a busy kind), it asks
    /// the seeds alone, and ends with no seeds.
    pub(crate) fn tick(
        &mut self,
        client: Option<&mut AuthorityClient>,
        sent_at: kabudachi_core::time::Instant,
        now: Instant,
    ) {
        self.validating = false;
        if self.asking.is_some() {
            return;
        }
        let Some(client) = client else {
            if self.next_round_at.is_some_and(|due| now >= due) {
                self.next_round_at = None;
                self.ask_seeds_or_end(now);
            }
            return;
        };
        if let Some(read) = &mut self.read
            && let Some(bound) = read.bound
            && now >= bound
        {
            read.bound = None;
            self.end_round_unanswered(now);
            return;
        }
        if !self.next_round_at.is_some_and(|due| now >= due) {
            return;
        }
        if let Some(read) = &mut self.read {
            // Its own read is still out: wait on that one again.
            read.bound = Some(now + self.retry_interval);
            self.next_round_at = None;
            return;
        }
        match client.ask(AuthorityRequest::ReadLiveRegistrations, sent_at) {
            Some(token) => {
                self.read = Some(PendingRead {
                    token,
                    bound: Some(now + self.retry_interval),
                });
                self.next_round_at = None;
                // Left unasked when the kind is busy: a later round asks again.
                if self.reads_epoch {
                    self.ask_epoch(client, sent_at, now);
                }
            }
            // Busy with a call this rejoin did not ask.
            None => self.end_round_unanswered(now),
        }
    }

    /// For a node `Joining` on a pointer it took: asks `client` for the
    /// authority's recovery epoch, unless that read is still out or was asked a
    /// retry interval ago or less, for the node to check the pointer against.
    /// A read asked before the node took the pointer says nothing of it, so the
    /// first call drops it and asks afresh.
    pub(crate) fn validate(
        &mut self,
        client: &mut AuthorityClient,
        sent_at: kabudachi_core::time::Instant,
        now: Instant,
    ) {
        if !self.validating {
            self.validating = true;
            self.epoch_read = None;
            self.epoch_ask_due = now;
        }
        self.ask_epoch(client, sent_at, now);
    }

    /// Asks for the authority's recovery epoch when none is out and the last
    /// is a retry interval past. A read still out past its bound is given up
    /// on: its late reply is dropped (see [`Self::offer`]), never applied, and
    /// the next read is asked once that reply has freed the client's call of
    /// that kind (a client keeps one in flight per kind, so a slow authority
    /// holds one blocking thread, not one more at every retry).
    fn ask_epoch(
        &mut self,
        client: &mut AuthorityClient,
        sent_at: kabudachi_core::time::Instant,
        now: Instant,
    ) {
        if self.epoch_read.is_some_and(|read| now >= read.bound) {
            self.epoch_read = None;
            self.epoch_ask_due = now;
        }
        if self.epoch_read.is_some() || now < self.epoch_ask_due {
            return;
        }
        match client.ask(AuthorityRequest::ReadRecoveryEpoch, sent_at) {
            Some(token) => {
                self.epoch_read = Some(PendingEpochRead {
                    token,
                    bound: now + self.retry_interval,
                });
                self.asked_epoch_read = Some(token);
            }
            // Busy with a read this rejoin did not ask, or gave up on.
            None => self.epoch_ask_due = now + self.retry_interval,
        }
    }

    /// The epoch read asked since this was last called, if any, for the
    /// caller to tell the node of before it offers the read's answer.
    pub(crate) fn take_asked_epoch_read(&mut self) -> Option<ReplyToken> {
        self.asked_epoch_read.take()
    }

    /// Takes `reply` if it answers this rejoin's pending listing, and starts
    /// the round's ask; or, if it answers the pending epoch read, returns that
    /// read's token with the recovery epoch the authority holds, for the node
    /// to read. Drops any
    /// other reply: the driver offers only Cascade-issued ones, and one that
    /// is not this rejoin's was left in flight by the cascade or an earlier
    /// rejoin.
    pub(crate) fn offer(
        &mut self,
        reply: AuthorityReply,
        now: Instant,
        floor: JoinFloor,
    ) -> Option<(ReplyToken, RecoveryEpoch)> {
        if let Some(read) = self.epoch_read
            && read.token == reply.token()
        {
            self.epoch_read = None;
            self.epoch_ask_due = read.bound;
            return match reply {
                AuthorityReply::RecoveryEpoch { token, result: Ok(Some(held)), .. } => {
                    Some((token, held))
                }
                _ => None,
            };
        }
        if reply.token().kind == CallKind::ReadRecoveryEpoch {
            // A read this rejoin gave up on, or the cascade's: it frees its
            // kind, so the next read may be asked at once.
            self.epoch_ask_due = now;
            return None;
        }
        if self.read.as_ref().map(|read| read.token) != Some(reply.token()) {
            return None;
        }
        self.read = None;
        let AuthorityReply::LiveRegistrations { result, .. } = reply else {
            return None;
        };
        if self.asking.is_some() {
            // A late reply: the round gave up on this read and is asking the
            // seeds alone, and that ask is the round's.
            return None;
        }
        let was_in_round = self.next_round_at.is_none();
        match result {
            Err(error) => {
                if was_in_round {
                    self.search.log(WaitReason::AuthorityUnreachable(error));
                    self.floor = floor;
                    self.ask_seeds_or_end(now);
                }
            }
            Ok(registrations) => {
                if !was_in_round {
                    // A late reply: its round already ended, and the next is
                    // paced by the retry interval, not by this reply.
                    return None;
                }
                self.next_round_at = None;
                self.floor = floor;
                let peers = self.search.others_in(&registrations);
                let mut addresses = self.search.to_ask(&peers);
                let seeds = self.search.seeds().to_vec();
                for seed in seeds {
                    if !addresses.contains(&seed) {
                        addresses.push(seed);
                    }
                }
                self.ask(addresses, now, false);
            }
        }
        None
    }

    /// Waits for the round's ask, for ever while none runs. Cancel-safe.
    pub(crate) async fn ask_done(&mut self) -> LeaderSearch {
        match self.asking.as_mut() {
            Some(asking) => asking.await,
            None => std::future::pending().await,
        }
    }

    /// Takes what the round's ask found and paces the next round: the pointer
    /// to rejoin through, if any.
    pub(crate) fn asked(&mut self, found: LeaderSearch, now: Instant) -> Option<JoinResponse> {
        self.asking = None;
        let pointer = if self.asking_seeds_only {
            self.search.heard_from_seeds_alone(found)
        } else {
            self.search.heard_from_listed(found)
        };
        self.end_round(now);
        pointer
    }

    /// The round ends with no answer from the authority within its bound.
    fn end_round_unanswered(&mut self, now: Instant) {
        self.search.log(WaitReason::AuthorityNotAnswering);
        self.ask_seeds_or_end(now);
    }

    /// A round with no listing asks the seeds alone, or ends with none.
    fn ask_seeds_or_end(&mut self, now: Instant) {
        let seeds = self.search.seeds().to_vec();
        self.ask(seeds, now, true);
    }

    /// Starts the round's ask of `addresses`, or ends the round if none.
    fn ask(&mut self, addresses: Vec<Multiaddr>, now: Instant, seeds_only: bool) {
        if addresses.is_empty() {
            self.end_round(now);
            return;
        }
        self.asking_seeds_only = seeds_only;
        let (mut port, floor) = (self.port.clone(), self.floor);
        self.asking = Some(Box::pin(async move { port.ask(&addresses, floor).await }));
    }

    fn end_round(&mut self, now: Instant) {
        self.search.end_round();
        self.next_round_at = Some(now + self.retry_interval);
    }
}

/// When a node that has lost touch with its shard searches for a leader
/// again: once it has been in `RollCall` or `NoQuorum` for `window`, whether
/// or not it hears some workers (a reachable island smaller than a quorum
/// must heal too). It keys on the node's state and time alone. After it
/// reports a node stranded it waits another `window` before it says so again,
/// so searches are one window apart.
pub(crate) struct StrandedWatch {
    window: Duration,
    /// Since when the node has been in those states, or since it was last
    /// reported stranded.
    since: Option<CoreInstant>,
}

impl StrandedWatch {
    pub(crate) fn new(window: Duration) -> Self {
        StrandedWatch { window, since: None }
    }

    /// Notes the node's state after a batch. Whether it is stranded now.
    pub(crate) fn observe(&mut self, state: WorkerState, now: CoreInstant) -> bool {
        if !matches!(state, WorkerState::RollCall | WorkerState::NoQuorum) {
            self.since = None;
            return false;
        }
        let since = *self.since.get_or_insert(now);
        let stranded = now - since >= self.window;
        if stranded {
            self.since = Some(now);
        }
        stranded
    }

    /// When the node would next be reported stranded, while that is ahead.
    pub(crate) fn wake_at(&self, now: CoreInstant) -> Option<CoreInstant> {
        self.since.map(|since| since + self.window).filter(|due| *due > now)
    }
}

/// The search the driver runs for a node in `state`, if any. A rejoin needs
/// an authority to confirm a pointer against. A stranded node's leader
/// search needs someone to ask, an authority's listing or a seed: with
/// neither it would run empty rounds every retry interval.
pub(crate) fn search_purpose(
    state: WorkerState,
    authority_present: bool,
    seeds_present: bool,
    stranded_search: bool,
) -> Option<SearchFor> {
    match state {
        WorkerState::Bootstrapping | WorkerState::Joining if authority_present => {
            Some(SearchFor::Rejoin)
        }
        _ if stranded_search && (authority_present || seeds_present) => Some(SearchFor::Leader),
        _ => None,
    }
}

/// Why the driver runs a leader search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchFor {
    /// The node fenced itself and is back in `Bootstrapping` (or `Joining` on
    /// a pointer it took): a pointer is joined through `Input::JoinAnswer`.
    Rejoin,
    /// The node is stranded: the search's own dials reconnect it, and a leader
    /// it reaches acks it as a newly connected peer.
    Leader,
}

/// The leader search a driven node runs: the rejoin of a node back in
/// `Bootstrapping` or `Joining`, and the search of a node stranded in
/// `RollCall` or `NoQuorum` for a leader to reconnect to. It decides which
/// search runs from the node's state after each batch, starts and replaces
/// it, takes the replies to the calls net asked for itself, and hands back
/// what the node must be told: that an epoch read was asked, what that read
/// found, and a pointer to join. It never steps the node; the driver does,
/// at the points of its batch each call names.
pub(crate) struct DrivenSearch<'a, P> {
    shard_id: ShardId,
    my_id: WorkerId,
    port: P,
    seeds: Vec<Multiaddr>,
    retry_interval: StdDuration,
    /// Whether the driver has an authority client: a rejoin needs one.
    authority_present: bool,
    /// The search running and why, while one runs.
    running: Option<(SearchFor, Rejoin<'a, P>)>,
    /// The pointer the last round found, and why its search ran, until the
    /// driver takes it.
    found: Option<(SearchFor, JoinResponse)>,
    stranded: StrandedWatch,
    /// Whether the last firing of `stranded` still calls for a search.
    stranded_search: bool,
    /// The epoch read a rejoin asked last batch, to tell the node of before
    /// its answer arrives.
    epoch_read_asked: Option<ReplyToken>,
}

impl<'a, P: AskWhoLeads + Clone + Send + 'a> DrivenSearch<'a, P> {
    /// No search runs until [`Self::follow`] finds one called for. A node is
    /// stranded once it has been in `RollCall` or `NoQuorum` for
    /// `stranded_after` (one suspicion timeout).
    pub(crate) fn new(
        shard_id: &ShardId,
        my_id: WorkerId,
        port: P,
        seeds: Vec<Multiaddr>,
        retry_interval: StdDuration,
        stranded_after: Duration,
        authority_present: bool,
    ) -> Self {
        DrivenSearch {
            shard_id: shard_id.clone(),
            my_id,
            port,
            seeds,
            retry_interval,
            authority_present,
            running: None,
            found: None,
            stranded: StrandedWatch::new(stranded_after),
            stranded_search: false,
            epoch_read_asked: None,
        }
    }

    /// Called first in each batch, before the batch's authority replies are
    /// handed over: [`Input::AuthorityEpochAsked`] for the epoch read a rejoin
    /// asked since the last batch, so the node expects its answer.
    pub(crate) fn take_epoch_asked(&mut self) -> Option<Input> {
        self.epoch_read_asked.take().map(Input::AuthorityEpochAsked)
    }

    /// Takes a reply to a call net asked for itself (`Issuer::Cascade`). A
    /// rejoin's own epoch read comes back as [`Input::AuthorityEpochRead`]
    /// for the node to take as its floor; a stranded search reads no epoch
    /// for the node. Any other reply is dropped (logged at debug when no
    /// search runs). `floor` is the node's join floor now.
    pub(crate) fn offer(
        &mut self,
        reply: AuthorityReply,
        floor: JoinFloor,
        now: Instant,
    ) -> Option<Input> {
        match self.running.as_mut() {
            Some((purpose, search)) => {
                let epoch = search.offer(reply, now, floor);
                // Only a rejoin's node takes the epoch as its floor.
                if let Some((token, held)) = epoch
                    && *purpose == SearchFor::Rejoin
                {
                    return Some(Input::AuthorityEpochRead { token, held });
                }
                None
            }
            None => {
                tracing::debug!(
                    "dropping a reply to a call net asked for itself: no search is running"
                );
                None
            }
        }
    }

    /// Called after the batch's net inputs: the pointer the last round found.
    /// A rejoin's pointer comes back as [`Input::JoinAnswer`]; a pointer that
    /// node will not take leaves it in `Bootstrapping`, and the rejoin, which
    /// has already paced its next round, goes on. A node with a floor only
    /// enters `Joining` on it: its registration restarts when a read of the
    /// authority validates the pointer. A stranded search that reached a
    /// leader is done: this logs it, ends the search and returns `None`,
    /// since the leader's ack follows as an ordinary input.
    pub(crate) fn take_found(&mut self) -> Option<Input> {
        let (purpose, pointer) = self.found.take()?;
        match purpose {
            SearchFor::Rejoin => Some(Input::JoinAnswer(pointer)),
            // Reaching the leader was the search's own work: it has dialed
            // it, and the leader's ack follows as an ordinary input.
            SearchFor::Leader => {
                tracing::info!(
                    shard = self.shard_id.as_str(),
                    leader = ?pointer.leader_id,
                    "a node that had lost touch with its shard reached a leader"
                );
                self.stranded_search = false;
                self.running = None;
                None
            }
        }
    }

    /// Called once per batch, after the node's steps: notes the node's
    /// `state` at `sent_at` (the node's clock), decides which search it
    /// calls for, starts or replaces the running one, and lets it run its
    /// round (or, for a node `Joining` on a pointer it took, validate that
    /// pointer against the authority's epoch). `now` is tokio's clock.
    pub(crate) fn follow(
        &mut self,
        state: WorkerState,
        client: Option<&mut AuthorityClient>,
        sent_at: CoreInstant,
        now: Instant,
    ) {
        // A fenced node that found its shard recovered without it went back
        // to `Bootstrapping` to join again. Only a node with an authority
        // fences itself, and that authority lists whom to ask. A stranded node
        // searches too, for a leader to reconnect to, and a stranded search
        // restarts each time the watch fires, once per suspicion timeout.
        if self.stranded.observe(state, sent_at) {
            self.stranded_search = true;
            if matches!(self.running, Some((SearchFor::Leader, _))) {
                self.running = None;
            }
        }
        if !matches!(state, WorkerState::RollCall | WorkerState::NoQuorum) {
            self.stranded_search = false;
        }
        let purpose = search_purpose(
            state,
            self.authority_present,
            !self.seeds.is_empty(),
            self.stranded_search,
        );
        match purpose {
            Some(purpose) => {
                if self.running.as_ref().map(|(running, _)| *running) != Some(purpose) {
                    let rejoin = Rejoin::new(
                        &self.shard_id,
                        self.my_id.clone(),
                        self.port.clone(),
                        self.seeds.clone(),
                        self.retry_interval,
                        now,
                    );
                    self.running = Some((
                        purpose,
                        if purpose == SearchFor::Leader {
                            rejoin.for_leader_search()
                        } else {
                            rejoin
                        },
                    ));
                }
                let (_, running) = self.running.as_mut().expect("set above");
                if state == WorkerState::Joining {
                    // `Joining` on a pointer it took: the authority's epoch
                    // is what confirms it.
                    if let Some(client) = client {
                        running.validate(client, sent_at, now);
                    }
                } else {
                    running.tick(client, sent_at, now);
                }
                let asked = running.take_asked_epoch_read();
                if purpose == SearchFor::Rejoin {
                    self.epoch_read_asked = asked.or(self.epoch_read_asked);
                }
            }
            None => self.running = None,
        }
    }

    /// When the running search's next round or read bound is due.
    pub(crate) fn round_wake_at(&self) -> Option<Instant> {
        self.running.as_ref().and_then(|(_, search)| search.wake_at())
    }

    /// When the node, still in `RollCall` or `NoQuorum`, would next be found
    /// stranded, on the node's clock.
    pub(crate) fn stranded_wake_at(&self, now: CoreInstant) -> Option<CoreInstant> {
        self.stranded.wake_at(now)
    }

    /// The running round's ask, once it finishes; for ever when none runs.
    /// Cancel-safe.
    pub(crate) async fn ask_done(&mut self) -> LeaderSearch {
        match &mut self.running {
            Some((_, search)) => search.ask_done().await,
            None => std::future::pending().await,
        }
    }

    /// Takes what the round's ask found, keeping a pointer for
    /// [`Self::take_found`].
    pub(crate) fn asked(&mut self, found: LeaderSearch, now: Instant) {
        if let Some((purpose, search)) = self.running.as_mut() {
            self.found = search.asked(found, now).map(|pointer| (*purpose, pointer));
        }
    }
}

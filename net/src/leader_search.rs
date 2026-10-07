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
//! - [`Rejoin`] is the driver's search for a node back in `Bootstrapping`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::{LiveRegistrations, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityReply, AuthorityRequest, CallKind, JoinFloor, ReplyToken,
};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
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
/// or knows it exists, has no seeds, and starts each round one listed worker
/// further along (rejoin).
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
            log: WaitLog::new(shard_id),
        }
    }

    pub(crate) fn for_rejoin(shard_id: &ShardId, my_id: WorkerId) -> Self {
        SearchRounds {
            my_id,
            seeds: Vec::new(),
            mode: Mode::Rejoin,
            round: 0,
            shard_exists: true,
            listed: Vec::new(),
            log: WaitLog::new(shard_id),
        }
    }

    /// The seeds this round asks, in order (none for a rejoin).
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
    /// The node is `Joining` on a pointer it took, so this rejoin runs no
    /// round: it only reads the epoch until the node is told of it.
    validating: bool,
    /// An epoch read asked and not yet reported to the node, which keeps the
    /// latest read it was told of (see [`Self::take_asked_epoch_read`]).
    asked_epoch_read: Option<ReplyToken>,
    /// The round's ask of the listed workers, while it runs.
    asking: Option<Pin<Box<dyn Future<Output = LeaderSearch> + Send + 'a>>>,
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
        retry_interval: StdDuration,
        now: Instant,
    ) -> Self {
        Rejoin {
            search: SearchRounds::for_rejoin(shard_id, my_id),
            port,
            retry_interval,
            next_round_at: Some(now),
            read: None,
            epoch_read: None,
            epoch_ask_due: now,
            validating: false,
            asked_epoch_read: None,
            asking: None,
        }
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
    /// stalls the rejoin and never spins it.
    pub(crate) fn tick(
        &mut self,
        client: &mut AuthorityClient,
        sent_at: kabudachi_core::time::Instant,
        now: Instant,
    ) {
        self.validating = false;
        if self.asking.is_some() {
            return;
        }
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
                self.ask_epoch(client, sent_at, now);
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
        let was_in_round = self.next_round_at.is_none();
        match result {
            Err(error) => {
                if was_in_round {
                    self.search.log(WaitReason::AuthorityUnreachable(error));
                    self.end_round(now);
                }
            }
            Ok(registrations) => {
                self.next_round_at = None;
                let peers = self.search.others_in(&registrations);
                let addresses = self.search.to_ask(&peers);
                if addresses.is_empty() {
                    self.end_round(now);
                    return None;
                }
                let mut port = self.port.clone();
                self.asking = Some(Box::pin(async move { port.ask(&addresses, floor).await }));
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
        let pointer = self.search.heard_from_listed(found);
        self.end_round(now);
        pointer
    }

    /// The round ends with no answer from the authority within its bound.
    fn end_round_unanswered(&mut self, now: Instant) {
        self.search.log(WaitReason::AuthorityNotAnswering);
        self.end_round(now);
    }

    fn end_round(&mut self, now: Instant) {
        self.search.end_round();
        self.next_round_at = Some(now + self.retry_interval);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, Uuid7Lineages};
    use kabudachi_core::election::{AuthorityRequest, CallKind};
    use kabudachi_core::time::Instant as CoreInstant;
    use kabudachi_testkit::FaultingAuthority;
    use tokio::time::{Instant, timeout};

    use super::*;
    use crate::authority::AuthorityClient;
    use crate::test_support::{
        Answer, Scripted, TEST_TIMEOUT, TokioClock, address, epoch_number, pointer_to, register,
        wait_until_held, warm_authority,
    };

    const RETRY: Duration = Duration::from_millis(50);
    const ME: &str = "me";

    struct Fixture {
        authority: FaultingAuthority<TokioClock>,
        clock: TokioClock,
        port: Scripted,
        client: AuthorityClient,
    }

    async fn fixture() -> Fixture {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        let client = AuthorityClient::with_parts(
            Arc::new(authority.clone()),
            ShardId::new("shard-1"),
            WorkerId::new(ME),
            tokio::sync::watch::channel(None).1,
        );
        Fixture {
            authority,
            clock,
            port: Scripted::default(),
            client,
        }
    }

    fn rejoin_of(fixture: &Fixture) -> Rejoin<'static, Scripted> {
        Rejoin::new(
            &ShardId::new("shard-1"),
            WorkerId::new(ME),
            fixture.port.clone(),
            RETRY,
            Instant::now(),
        )
    }

    /// The core-clock reading a driver stamps its calls with.
    fn stamp(clock: TokioClock) -> CoreInstant {
        kabudachi_core::time::Clock::now(&clock)
    }

    /// The driver's loop around a rejoin: tick, then take whichever comes
    /// first of a reply, the round's ask finishing, or the next wake. Pushes
    /// every pointer a round finds, and returns once `until` of them have
    /// been found, noting each distinct time the rejoin asked to be woken.
    async fn drive(
        rejoin: &mut Rejoin<'static, Scripted>,
        fixture: &mut Fixture,
        found: &mut Vec<(JoinResponse, Instant)>,
        wakes: &mut Vec<Instant>,
        until: usize,
    ) {
        while found.len() < until {
            rejoin.tick(&mut fixture.client, stamp(fixture.clock), Instant::now());
            let wake = rejoin.wake_at();
            if let Some(at) = wake
                && wakes.last() != Some(&at)
            {
                wakes.push(at);
            }
            tokio::select! {
                Some(reply) = fixture.client.next_reply(None) => {
                    rejoin.offer(reply, Instant::now(), JoinFloor::none());
                }
                result = rejoin.ask_done() => {
                    if let Some(pointer) = rejoin.asked(result, Instant::now()) {
                        found.push((pointer, Instant::now()));
                    }
                }
                () = async {
                    match wake {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {}
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejoin_rotates_past_a_worker_whose_pointer_was_refused() {
        let mut fixture = fixture().await;
        let (a1, a2) = (address(1), address(2));
        register(&fixture.authority, "w1", &a1.to_string());
        register(&fixture.authority, "w2", &a2.to_string());
        let (stale, current) = (pointer_to("stale", &a1), pointer_to("current", &a2));
        fixture.port.script(&a1, [Answer::Pointer(stale.clone())]);
        fixture.port.script(&a2, [Answer::Pointer(current.clone())]);
        let mut rejoin = rejoin_of(&fixture);
        let (mut found, mut wakes) = (Vec::new(), Vec::new());

        // The node refuses the first pointer; the test simply goes on.
        timeout(
            TEST_TIMEOUT,
            drive(&mut rejoin, &mut fixture, &mut found, &mut wakes, 2),
        )
        .await
        .expect("both rounds found a pointer within the timeout");

        let pointers: Vec<_> = found.iter().map(|(pointer, _)| pointer.clone()).collect();
        assert_eq!(pointers, [stale, current]);
        assert_eq!(fixture.port.passes(), [vec![a1.clone(), a2.clone()], vec![a2, a1]]);
        assert!(
            found[1].1 - found[0].1 >= RETRY,
            "the second round started sooner than a retry interval after the first ended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejoin_asks_the_pass_with_the_floor_it_is_given() {
        let mut fixture = fixture().await;
        let (a1, a2) = (address(1), address(2));
        register(&fixture.authority, "w1", &a1.to_string());
        register(&fixture.authority, "w2", &a2.to_string());
        // The floor is epoch 5 of lineage 1. Listed first, w1 leads epoch 5 of
        // lineage 2 at a far later term, which the floor refuses.
        let in_lineage = |leader: &str, at: &Multiaddr, lineage: u64, term: u64| JoinResponse {
            recovery_epoch: 5,
            recovery_epoch_lineage: lineage,
            term,
            ..pointer_to(leader, at)
        };
        let refused = in_lineage("w1", &a1, 2, 10);
        let accepted = in_lineage("w2", &a2, 1, 1);
        fixture.port.script(&a1, [Answer::Pointer(refused)]);
        fixture.port.script(&a2, [Answer::Pointer(accepted.clone())]);
        let mut rejoin = rejoin_of(&fixture);
        let floor = JoinFloor::at(RecoveryEpoch::new(5, 1));

        rejoin.tick(&mut fixture.client, stamp(fixture.clock), Instant::now());
        for _ in 0..2 {
            let reply = fixture
                .client
                .next_reply(Some(TEST_TIMEOUT))
                .await
                .expect("each read is answered");
            rejoin.offer(reply, Instant::now(), floor);
        }
        let result = rejoin.ask_done().await;

        assert_eq!(rejoin.asked(result, Instant::now()), Some(accepted));
    }

    #[tokio::test(start_paused = true)]
    async fn an_epoch_read_that_fails_or_finds_no_epoch_is_asked_again_next_round() {
        for reachable in [false, true] {
            // Unreachable, the read is an error; reachable, a fresh authority
            // holds no epoch.
            let mut fixture = fixture().await;
            fixture.authority.set_reachable(reachable);
            let mut rejoin = rejoin_of(&fixture);

            for round in 0..2 {
                tokio::time::advance(RETRY).await;
                rejoin.tick(&mut fixture.client, stamp(fixture.clock), Instant::now());
                let mut epoch_reads = 0;
                for _ in 0..2 {
                    let reply = fixture
                        .client
                        .next_reply(Some(TEST_TIMEOUT))
                        .await
                        .expect("the listing and the epoch are each answered");
                    if matches!(reply, AuthorityReply::RecoveryEpoch { .. }) {
                        epoch_reads += 1;
                        assert_eq!(
                            rejoin.offer(reply, Instant::now(), JoinFloor::none()),
                            None,
                            "reachable: {reachable}, round {round}: no epoch to hand over"
                        );
                    } else {
                        rejoin.offer(reply, Instant::now(), JoinFloor::none());
                    }
                }
                assert_eq!(
                    epoch_reads, 1,
                    "reachable: {reachable}, round {round}: the epoch is asked again"
                );
            }
        }
    }

    /// Founds epoch 3 in the fixture's authority, from another worker's handle.
    fn found_epoch_3(fixture: &Fixture) -> RecoveryEpoch {
        let epoch = RecoveryEpoch::founding(3, &mut Uuid7Lineages);
        fixture
            .authority
            .for_another_worker()
            .compare_and_swap_recovery_epoch(&ShardId::new("shard-1"), None, epoch)
            .unwrap();
        epoch
    }

    #[tokio::test(start_paused = true)]
    async fn a_round_reads_the_epoch_again_when_the_last_answer_came_late_in_its_interval() {
        let mut fixture = fixture().await;
        let held = found_epoch_3(&fixture);
        let mut rejoin = rejoin_of(&fixture);
        let sent_at = stamp(fixture.clock);

        // Nobody is listed, so the listing's answer ends the round at once; the
        // epoch's answer follows part of an interval later.
        rejoin.tick(&mut fixture.client, sent_at, Instant::now());
        let asked = rejoin.take_asked_epoch_read().expect("the first round asks for the epoch");
        let mut replies = Vec::new();
        for _ in 0..2 {
            replies.push(fixture.client.next_reply(Some(TEST_TIMEOUT)).await.unwrap());
        }
        replies.sort_by_key(|reply| reply.token() == asked);
        let round_ended = Instant::now();
        rejoin.offer(replies.remove(0), round_ended, JoinFloor::none());
        tokio::time::advance(RETRY / 2).await;
        assert_eq!(
            rejoin.offer(replies.remove(0), Instant::now(), JoinFloor::none()),
            Some((asked, held))
        );

        tokio::time::advance(RETRY / 2).await;
        assert_eq!(Instant::now(), round_ended + RETRY);
        rejoin.tick(&mut fixture.client, sent_at, Instant::now());
        assert!(
            rejoin.take_asked_epoch_read().is_some(),
            "the next round reads the epoch too"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_validating_a_pointer_reads_the_epoch_again_once_a_lagging_read_is_answered() {
        let mut fixture = fixture().await;
        let held = found_epoch_3(&fixture);
        fixture.authority.hold_next(CallKind::ReadRecoveryEpoch);
        let mut rejoin = rejoin_of(&fixture);
        let sent_at = stamp(fixture.clock);
        let t0 = Instant::now();

        rejoin.validate(&mut fixture.client, sent_at, t0);
        let lagging = rejoin.take_asked_epoch_read().expect("validation asks for the epoch");
        wait_until_held(&fixture.authority, CallKind::ReadRecoveryEpoch).await;
        assert_eq!(rejoin.wake_at(), Some(t0 + RETRY), "the read is bounded");

        tokio::time::advance(RETRY).await;
        rejoin.validate(&mut fixture.client, sent_at, Instant::now());
        assert_eq!(rejoin.take_asked_epoch_read(), None, "the kind is busy with the lagging read");
        fixture.authority.release(CallKind::ReadRecoveryEpoch);
        let late = fixture.client.next_reply(Some(TEST_TIMEOUT)).await.unwrap();
        assert_eq!(late.token(), lagging);
        assert_eq!(rejoin.offer(late, Instant::now(), JoinFloor::none()), None);

        rejoin.validate(&mut fixture.client, sent_at, Instant::now());
        let fresh = rejoin.take_asked_epoch_read().expect("the freed kind is asked again at once");
        let reply = fixture.client.next_reply(Some(TEST_TIMEOUT)).await.unwrap();
        assert_eq!(
            rejoin.offer(reply, Instant::now(), JoinFloor::none()),
            Some((fresh, held))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_validating_a_pointer_does_not_spin_on_an_authority_with_no_epoch() {
        // A fresh authority holds no epoch, so each read answers nothing.
        let mut fixture = fixture().await;
        let mut rejoin = rejoin_of(&fixture);
        let sent_at = stamp(fixture.clock);

        for round in 0..3 {
            let now = Instant::now();
            rejoin.validate(&mut fixture.client, sent_at, now);
            assert!(
                rejoin.take_asked_epoch_read().is_some(),
                "round {round}: a read is asked once the interval has passed"
            );
            let reply = fixture.client.next_reply(Some(TEST_TIMEOUT)).await.unwrap();
            assert_eq!(rejoin.offer(reply, now, JoinFloor::none()), None);

            rejoin.validate(&mut fixture.client, sent_at, now);
            assert_eq!(rejoin.take_asked_epoch_read(), None, "round {round}: no read inside the interval");
            assert_eq!(rejoin.wake_at(), Some(now + RETRY));
            tokio::time::advance(RETRY).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejoin_never_founds_even_when_no_one_else_is_listed() {
        let mut fixture = fixture().await;
        // Only this worker is listed, and the epoch exists: the case in which
        // the cascade would re-found the shard one epoch on.
        register(&fixture.authority, ME, "/ip4/127.0.0.1/tcp/9");
        fixture
            .authority
            .for_another_worker()
            .compare_and_swap_recovery_epoch(&ShardId::new("shard-1"), None, RecoveryEpoch::founding(3, &mut Uuid7Lineages))
            .unwrap();
        let mut rejoin = rejoin_of(&fixture);
        let (mut found, mut wakes) = (Vec::new(), Vec::new());

        let waiting = timeout(
            RETRY * 20,
            drive(&mut rejoin, &mut fixture, &mut found, &mut wakes, 1),
        )
        .await;

        assert!(waiting.is_err() && found.is_empty(), "the rejoin found a leader from nothing");
        assert_eq!(epoch_number(&fixture.authority), Some(3), "nothing swapped the epoch");
        assert!(fixture.port.passes().is_empty(), "no one else was listed to ask");
        assert!(wakes.len() >= 19, "it kept going round: {wakes:?}");
        assert!(
            wakes.windows(2).all(|pair| pair[1] - pair[0] == RETRY),
            "each round is a retry interval after the last: {wakes:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejoin_read_that_panics_ends_its_round_and_reads_again() {
        let mut fixture = fixture().await;
        let a1 = address(1);
        register(&fixture.authority, "w1", &a1.to_string());
        fixture.port.script(&a1, [Answer::Pointer(pointer_to("w1", &a1))]);
        fixture.authority.panic_next(CallKind::ReadLiveRegistrations);
        let mut rejoin = rejoin_of(&fixture);
        let (mut found, mut wakes) = (Vec::new(), Vec::new());
        let started = Instant::now();

        timeout(
            TEST_TIMEOUT,
            drive(&mut rejoin, &mut fixture, &mut found, &mut wakes, 1),
        )
        .await
        .expect("the second round found a pointer within the timeout");

        assert!(found[0].1 - started >= RETRY, "the first round's read had panicked");
        assert_eq!(fixture.port.passes(), [vec![a1]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejoin_read_that_lags_ends_its_round_and_its_late_reply_is_still_used() {
        let mut fixture = fixture().await;
        let a1 = address(1);
        register(&fixture.authority, "w1", &a1.to_string());
        fixture.port.script(&a1, [Answer::Pointer(pointer_to("w1", &a1))]);
        fixture.authority.hold_next(CallKind::ReadLiveRegistrations);
        let t0 = Instant::now();
        let mut rejoin = rejoin_of(&fixture);
        let sent_at = stamp(fixture.clock);

        rejoin.tick(&mut fixture.client, sent_at, t0);
        wait_until_held(&fixture.authority, CallKind::ReadLiveRegistrations).await;
        assert_eq!(rejoin.wake_at(), Some(t0 + RETRY), "the read is bounded");
        tokio::time::advance(RETRY).await;
        rejoin.tick(&mut fixture.client, sent_at, Instant::now());
        assert_eq!(rejoin.wake_at(), Some(t0 + RETRY * 2), "the lagging read ended the round");

        // The next round asks nothing new: the held read is its own.
        tokio::time::advance(RETRY).await;
        rejoin.tick(&mut fixture.client, sent_at, Instant::now());
        fixture.authority.release(CallKind::ReadLiveRegistrations);
        // The epoch read the first round asked beside it was answered at once.
        for _ in 0..2 {
            let reply = fixture
                .client
                .next_reply(Some(TEST_TIMEOUT))
                .await
                .expect("each read is answered once the held one is released");
            rejoin.offer(reply, Instant::now(), JoinFloor::none());
        }
        let result = rejoin.ask_done().await;

        let pointer = rejoin.asked(result, Instant::now());
        assert_eq!(pointer, Some(pointer_to("w1", &a1)));
        assert_eq!(fixture.port.passes(), [vec![a1]]);
        assert!(
            fixture.client.next_reply(Some(RETRY)).await.is_none(),
            "no second read was asked while the first was held"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_the_cascade_left_held_delays_the_rejoin_by_a_round_and_never_stalls_it() {
        let mut fixture = fixture().await;
        let a1 = address(1);
        register(&fixture.authority, "w1", &a1.to_string());
        fixture.port.script(&a1, [Answer::Pointer(pointer_to("w1", &a1))]);
        // The cascade's bounded first read, left held at handover.
        fixture.authority.hold_next(CallKind::ReadLiveRegistrations);
        fixture
            .client
            .ask(AuthorityRequest::ReadLiveRegistrations, CoreInstant::at(0))
            .expect("the cascade's read is asked");
        wait_until_held(&fixture.authority, CallKind::ReadLiveRegistrations).await;
        let t0 = Instant::now();
        let mut rejoin = rejoin_of(&fixture);
        let sent_at = stamp(fixture.clock);

        // Nothing is asked: the kind is busy with a read the rejoin did not
        // ask. The round ends, and the next is a retry interval away.
        rejoin.tick(&mut fixture.client, sent_at, t0);
        assert_eq!(rejoin.wake_at(), Some(t0 + RETRY));
        assert!(fixture.port.passes().is_empty());

        // The cascade's read comes back late. It answers no read of the
        // rejoin's, so the rejoin drops it.
        fixture.authority.release(CallKind::ReadLiveRegistrations);
        let cascade_reply = fixture
            .client
            .next_reply(Some(TEST_TIMEOUT))
            .await
            .expect("the cascade's read is answered once released");
        rejoin.offer(cascade_reply, Instant::now(), JoinFloor::none());
        tokio::select! {
            biased;
            _ = rejoin.ask_done() => panic!("the rejoin asked on a reply that was not its own"),
            () = std::future::ready(()) => {}
        }
        assert!(fixture.port.passes().is_empty());

        // A retry interval on, the rejoin reads for itself and gets on.
        tokio::time::advance(RETRY).await;
        rejoin.tick(&mut fixture.client, sent_at, Instant::now());
        let own = fixture
            .client
            .next_reply(Some(TEST_TIMEOUT))
            .await
            .expect("the rejoin's own read is answered");
        rejoin.offer(own, Instant::now(), JoinFloor::none());
        let result = rejoin.ask_done().await;
        assert!(rejoin.asked(result, Instant::now()).is_some());
        assert_eq!(fixture.port.passes(), [vec![a1]]);
    }
}

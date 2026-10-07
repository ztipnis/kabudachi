//! The JOIN protocol's two halves over [`Net`] (`/kabudachi/join/1`, see
//! `crate::join_codec`), one for the bootstrap cascade and the driver's
//! rejoin alike:
//!
//! - the client: [`ask_for_leader`] asks peers who lead the shard, all at
//!   once, and connects to the newest leader they point at (the leader
//!   search, `crate::leader_search`, decides whom to ask and when);
//! - the responder's answer: [`pointer_for`], the pointer a node hands a
//!   joiner. A pointer names the shard's leader, which only
//!   `core::election::WorkerNode` knows, and its address, which only `Net`
//!   knows, so it is put together here.
//!
//! A JOIN is a correlated request/response, unlike the election protocol:
//! each ask dials its peer and waits, bounded by a per-peer timeout, for
//! *that dial's own* connection, identified by the dial's `ConnectionId`
//! rather than by diffing the connected-peer set, so a late connection from
//! an abandoned ask is never mistaken for a later one's.

use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use kabudachi_core::election::{JoinFloor, WorkerNode};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::{JoinRequest, JoinResponse};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::time::Clock;
use libp2p::futures::StreamExt;
use libp2p::futures::stream::FuturesUnordered;
use libp2p::{Multiaddr, PeerId};
use tokio::time::Instant;

use crate::exchange::Asked;
use crate::join_codec::JoinCodec;
use crate::messenger::{DialTarget, Net};
use crate::peers::worker_id_of;

/// An unanswered inbound `/kabudachi/join/1` request, returned by
/// [`Net::poll_join_requests`]. Answer it with [`Net::respond_join`];
/// dropping it unanswered just lets the requester's substream eventually
/// fail with `OutboundFailure` on their side (nothing here relies on that
/// happening).
pub struct JoinRequestHandle(Asked<JoinCodec>);

impl JoinRequestHandle {
    /// The `WorkerId` of whoever sent this join request.
    pub fn from(&self) -> WorkerId {
        worker_id_of(&self.0.from)
    }
}

impl Net {
    /// Drains every inbound `/kabudachi/join/1` request not yet answered.
    /// Answer each with `Self::respond_join`.
    pub fn poll_join_requests(&self) -> Vec<JoinRequestHandle> {
        self.take_asked::<JoinCodec>()
            .into_iter()
            .map(JoinRequestHandle)
            .collect()
    }

    /// Answers a join request obtained from `Self::poll_join_requests`.
    /// Fire-and-forget like `send`: if the driver task has already stopped,
    /// there's nowhere for the answer to go, and that's fine to drop.
    pub fn respond_join(&self, handle: JoinRequestHandle, response: JoinResponse) {
        self.answer::<JoinCodec>(handle.0.channel, response);
    }

    /// Sends a `JOIN_REQUEST` to `to` (which must already be connected, see
    /// this module's `ask_for_leader`, the only caller) and awaits its
    /// `JOIN_RESPONSE`. `None` if the driver task is gone, the request fails
    /// outright (`OutboundFailure`), or the peer disconnects before
    /// answering.
    pub(crate) async fn send_join_request(&self, to: PeerId) -> Option<JoinResponse> {
        self.ask::<JoinCodec>(to, JoinRequest {}).await
    }
}

/// How long [`ask_for_leader`] waits, per peer, for a connection and then a
/// `JOIN_RESPONSE` before giving up on that peer.
pub const DEFAULT_JOIN_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// What one pass of [`ask_for_leader`] over its peers found.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaderSearch {
    /// A peer pointed at a leader, the newest of the pass this node could
    /// reach, and it is now connected to it.
    Found(JoinResponse),
    /// Some peer answered, but none pointed at a leader this node could
    /// reach: the shard exists, and its leader may not be elected yet.
    NoReachableLeader,
    /// No peer answered at all.
    NoAnswer,
}

/// One pass of the JOIN client: asks every one of `peers` who leads the
/// shard, over `net`, all at once: every seed is dialed at the same time, so a
/// pass may leave a connection to each live seed. A peer that fails to connect or answer
/// within `per_peer_timeout`, answers "no leader known", or points at an
/// address that does not parse, contributes no pointer.
///
/// `floor` is the recovery epoch the caller rejoins at, [`JoinFloor::none`]
/// for a first join. It alone judges the pointers: one it does not accept is
/// treated as no pointer, and it never starts the grace window below.
///
/// The pass ends when every peer has answered or timed out, or `grace` after
/// the first answer that points at an acceptable leader arrived, whichever
/// comes first: a live seed's answer is not held up by a dead seed's full
/// timeout, and a seed that answers within `grace` of the first still has its
/// say. Callers pass the shard's suspicion timeout, the time after which a
/// silent leader is no longer waited on.
///
/// It then takes the acceptable pointers newest first, as
/// [`JoinFloor::newest_first`] ranks them (equally new ones in `peers`
/// order), and returns
/// [`LeaderSearch::Found`] with the first whose leader this node is then
/// connected to (see [`connect_to_leader`]); the caller enters the shard
/// with it (`core::election::Entry::Joining`). A pointer to a leader that
/// cannot be reached is passed over for the next newest. These connections
/// are made one at a time after the gathering ends, each bounded by
/// `per_peer_timeout` on its own.
///
/// Otherwise the pass says whether anyone answered at all:
/// [`LeaderSearch::NoReachableLeader`] when some peer did (even "no leader
/// known", or a pointer that does not parse or cannot be reached), which
/// shows the shard exists; [`LeaderSearch::NoAnswer`] when none did. Asking
/// again is the caller's (see `crate::bootstrap`). A peer asked again is
/// asked over the connection this node already has to it, if that is still
/// up, rather than dialed afresh.
pub async fn ask_for_leader(
    net: &Net,
    peers: &[Multiaddr],
    floor: JoinFloor,
    per_peer_timeout: StdDuration,
    grace: StdDuration,
) -> LeaderSearch {
    let asks = peers
        .iter()
        .map(|peer| {
            Box::pin(ask_peer_for_leader(net, peer, per_peer_timeout)) as PeerAsk<'_>
        })
        .collect();
    let answers = gather_answers(asks, grace, |pointer| {
        floor.accepts(pointer) && pointed_leader(pointer).is_some()
    })
    .await;
    let a_peer_answered = answers.iter().any(Option::is_some);
    let answers: Vec<_> = answers.into_iter().flatten().collect();
    let pointers: Vec<_> = floor
        .newest_first(&answers)
        .into_iter()
        .filter_map(|response| {
            let (leader, leader_addr) = pointed_leader(response)?;
            Some((response, leader, leader_addr))
        })
        .collect();
    for (response, leader, leader_addr) in pointers {
        if connect_to_leader(net, &leader, leader_addr, per_peer_timeout).await {
            return LeaderSearch::Found(response.clone());
        }
    }
    if a_peer_answered {
        LeaderSearch::NoReachableLeader
    } else {
        LeaderSearch::NoAnswer
    }
}

/// One peer's ask of a pass, in flight.
type PeerAsk<'a> = Pin<Box<dyn Future<Output = Option<JoinResponse>> + Send + 'a>>;

/// Drives every ask at once and returns what each answered, in the order
/// the asks were given (`None` for one that did not answer). It stops early
/// when `grace` has passed since the first answer `starts_grace` holds of
/// arrived, dropping the asks still in flight: their dials and requests are
/// abandoned exactly as a per-peer timeout abandons them.
async fn gather_answers(
    asks: Vec<PeerAsk<'_>>,
    grace: StdDuration,
    starts_grace: impl Fn(&JoinResponse) -> bool,
) -> Vec<Option<JoinResponse>> {
    let mut answers = vec![None; asks.len()];
    let mut in_flight: FuturesUnordered<_> = asks
        .into_iter()
        .enumerate()
        .map(|(index, ask)| async move { (index, ask.await) })
        .collect();
    let mut grace_ends = None;
    loop {
        let next = match grace_ends {
            Some(end) => match tokio::time::timeout_at(end, in_flight.next()).await {
                Ok(next) => next,
                Err(_) => break,
            },
            None => in_flight.next().await,
        };
        let Some((index, answer)) = next else { break };
        if grace_ends.is_none() && answer.as_ref().is_some_and(&starts_grace) {
            grace_ends = Some(Instant::now() + grace);
        }
        answers[index] = answer;
    }
    answers
}

/// The `JOIN_RESPONSE` `node`, running over `net`, gives a joiner right
/// now: `WorkerNode::join_response`, at the address `net` can give a joiner
/// for the leader `node.known_leader()` names.
///
/// "No leader known" (an empty response) when the node knows no leader, or
/// has no dialable address for it: this node's own address before its first
/// successful `listen_on`, or a leader known only by the source address of
/// its inbound connection (see `Net::dialable_address`). A pointer the
/// joiner cannot dial would strand it, so the joiner is sent on to its next
/// seed instead.
pub async fn pointer_for<C: Clock>(node: &WorkerNode<C>, net: &Net) -> JoinResponse {
    // The node's answer, read once, names the leader whose address it
    // still needs.
    let Some(mut pointer) = node.join_response(String::new()) else {
        return JoinResponse::default();
    };
    let Some(leader_id) = pointer.leader_id() else {
        return JoinResponse::default();
    };
    let leader_addr = if &leader_id == node.id() {
        net.local_multiaddr()
    } else {
        net.dialable_address(&leader_id).await
    };
    let Some(leader_addr) = leader_addr else {
        return JoinResponse::default();
    };
    pointer.leader_multiaddr = leader_addr.to_string();
    pointer
}

/// One peer of [`ask_for_leader`]'s pass: send `JOIN_REQUEST` to the peer at
/// `address` and await the response, bounded by `per_peer_timeout`. While
/// the peer an earlier ask found at `address` is still connected, it is
/// asked directly. Otherwise `address` is dialed and this waits, also
/// bounded by `per_peer_timeout`, for that dial's own connection (see the
/// module doc); the swarm task records the peer it finds there.
async fn ask_peer_for_leader(
    net: &Net,
    address: &Multiaddr,
    per_peer_timeout: StdDuration,
) -> Option<JoinResponse> {
    // A peer this node already holds a connection to at `address` is asked
    // over it: dialing `address` afresh would only open a second connection
    // to the same peer. A worker fenced for losing the authority alone
    // keeps its connections, and rejoins through the addresses its peers
    // registered.
    let peer = match connected_peer_at(net, address).await {
        Some(peer) => peer,
        None => {
            // The swarm task records whoever answers there (or forgets whoever
            // did before) as the dial ends.
            tokio::time::timeout(
                per_peer_timeout,
                net.dial_for_connection(DialTarget::Address(address.clone())),
            )
            .await
            .ok()
            .flatten()?
        }
    };

    tokio::time::timeout(per_peer_timeout, net.send_join_request(peer))
        .await
        .ok()?
}

/// A peer this node is connected to at `address`: the one an earlier ask
/// found there, while still connected, or else a connected peer whose
/// address of record is `address`.
async fn connected_peer_at(net: &Net, address: &Multiaddr) -> Option<PeerId> {
    let address = address.clone();
    net.with_peers(move |peers| peers.peer_connected_at(&address))
        .await
        .flatten()
}

/// Whether this node ends up connected to `leader`: at once if it already
/// is (the leader was the seed that answered, say); otherwise by dialing
/// `leader` at `leader_addr`, and any address `kad`'s routing table
/// separately knows for it (see `crate::swarm`'s "kad: peer routing, not
/// membership" — `DialOpts::extend_addresses_through_behaviour` is what asks
/// for that here; `WithPeerIdWithAddresses::addresses` alone defaults it
/// off), and waiting, bounded by `per_peer_timeout`, for that dial's own
/// connection. So a `leader_addr` that is loopback or otherwise unreachable
/// from here is not the only way to reach `leader`: a `kad` entry for it,
/// learned from any other peer's Identify, gives this dial a second address
/// to try.
///
/// The dial names `leader`'s peer id, so if some other peer answers at
/// `leader_addr` (a leader restarted under a new identity, or the address
/// reused), libp2p refuses it as `WrongPeerId` and closes that connection. A
/// stale pointer asked about on every pass of [`ask_for_leader`] therefore
/// costs one failed dial per pass, never a connection left open.
async fn connect_to_leader(
    net: &Net,
    leader: &WorkerId,
    leader_addr: Multiaddr,
    per_peer_timeout: StdDuration,
) -> bool {
    let Ok(leader_peer) = PeerId::from_str(leader.as_str()) else {
        return false;
    };
    if net
        .with_peers(move |peers| peers.is_connected(&leader_peer))
        .await
        .unwrap_or(false)
    {
        return true;
    }
    let target = DialTarget::Peer {
        peer: leader_peer,
        address: leader_addr,
    };
    let dialed = tokio::time::timeout(per_peer_timeout, net.dial_for_connection(target))
        .await
        .ok()
        .flatten();
    dialed == Some(leader_peer)
}

/// The leader a `JOIN_RESPONSE` points at and the address to dial it on.
/// `None` for "no leader known" (see join.proto), and for an address that
/// does not parse — which [`ask_for_leader`] treats like "no leader known":
/// the peer did answer, so the shard exists. The codec has already rejected
/// a leader without an address (`WellFormed`), so a named leader always
/// comes with some string.
fn pointed_leader(response: &JoinResponse) -> Option<(WorkerId, Multiaddr)> {
    let leader = response.leader_id()?;
    let leader_addr = response.leader_multiaddr.parse().ok()?;
    Some((leader, leader_addr))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use kabudachi_core::coordination_authority::RecoveryEpoch;
    use kabudachi_core::election::{ElectionTimings, Entry, Identity, Input};
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId};
    use kabudachi_core::protocol::worker_state::WorkerState;
    use kabudachi_core::time::{Duration as TickDuration, RealClock};
    
    use tokio::time::timeout;

    use super::*;
    use crate::test_support::{
        TEST_TIMEOUT, listening_net, spawn_join_responder, worker_that_never_runs,
    };

    /// Takes `net`'s queued inputs until one is `expected`, failing the test
    /// with `what` if it does not arrive within the timeout.
    async fn expect_input(net: &Net, expected: Input, what: &str) {
        timeout(TEST_TIMEOUT, async {
            while !net.take_inputs().contains(&expected) {
                net.wait_for_arrival().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what} within the timeout"));
    }

    /// How long past the first pointer a pass keeps listening for others.
    const GRACE: Duration = Duration::from_secs(1);
    /// Grace for tests over real sockets: far longer than a loopback answer
    /// takes, so a slow host never cuts a pass short.
    const REAL_SOCKET_GRACE: Duration = Duration::from_secs(5);

    fn pointer_to(leader: &WorkerId, leader_addr: &Multiaddr) -> JoinResponse {
        JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        }
    }

    fn pointer_at(
        leader: &WorkerId,
        leader_addr: &Multiaddr,
        recovery_epoch: u64,
        term: u64,
    ) -> JoinResponse {
        JoinResponse {
            recovery_epoch,
            term,
            ..pointer_to(leader, leader_addr)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ask_for_leader_asks_every_peer_and_takes_the_newest_pointer() {
        // Each pair: what the first seed points at, what the second points at.
        // The second is newer each time: a later epoch whatever the term, or a
        // later term at the same epoch.
        for ((first_epoch, first_term), (second_epoch, second_term)) in
            [((0, 5), (1, 1)), ((2, 2), (2, 3))]
        {
            let (net_a, addr_a) = listening_net().await;
            let (net_b, addr_b) = listening_net().await;
            let net_c = Net::new();
            let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
            let older = pointer_at(&worker_a, &addr_a, first_epoch, first_term);
            let newer = pointer_at(&worker_b, &addr_b, second_epoch, second_term);
            let _responder_a = spawn_join_responder(Arc::new(net_a), older);
            let _responder_b = spawn_join_responder(Arc::new(net_b), newer.clone());

            let search = timeout(
                TEST_TIMEOUT,
                ask_for_leader(
                    &net_c,
                    &[addr_a, addr_b],
                    JoinFloor::none(),
                    Duration::from_secs(5),
                    REAL_SOCKET_GRACE,
                ),
            )
            .await
            .expect("ask_for_leader completed within the test timeout");

            assert_eq!(search, LeaderSearch::Found(newer));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ask_for_leader_takes_the_pointer_the_floor_accepts_over_a_higher_term_of_another_lineage() {
        // The floor is epoch 5 of lineage 1. Seed A, listed first, points at
        // epoch 5 of lineage 2 at a much later term: a leader the floor
        // refuses. Seed B points at the floor's own lineage.
        let (net_a, addr_a) = listening_net().await;
        let (net_b, addr_b) = listening_net().await;
        let net_c = Net::new();
        let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
        let other_lineage = JoinResponse {
            recovery_epoch_lineage: 2,
            ..pointer_at(&worker_a, &addr_a, 5, 10)
        };
        let own_lineage = JoinResponse {
            recovery_epoch_lineage: 1,
            ..pointer_at(&worker_b, &addr_b, 5, 1)
        };
        let _responder_a = spawn_join_responder(Arc::new(net_a), other_lineage);
        let _responder_b = spawn_join_responder(Arc::new(net_b), own_lineage.clone());

        let search = timeout(
            TEST_TIMEOUT,
            ask_for_leader(
                &net_c,
                &[addr_a, addr_b],
                JoinFloor::at(RecoveryEpoch::new(5, 1)),
                Duration::from_secs(5),
                REAL_SOCKET_GRACE,
            ),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(search, LeaderSearch::Found(own_lineage));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pointer_the_floor_refuses_does_not_start_the_grace_window() {
        // Seed A answers at once with a pointer the floor (epoch 5) refuses.
        // Seed B answers 200 ms later with an acceptable one. With no grace,
        // a pass that counted A's answer would end before B's.
        let (net_a, addr_a) = listening_net().await;
        let (net_b, addr_b) = listening_net().await;
        let net_c = Net::new();
        let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
        let refused = pointer_at(&worker_a, &addr_a, 4, 9);
        let accepted = pointer_at(&worker_b, &addr_b, 5, 1);
        let _responder_a = spawn_join_responder(Arc::new(net_a), refused);
        let late_b = {
            let accepted = accepted.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                spawn_join_responder(Arc::new(net_b), accepted).await
            })
        };

        let search = timeout(
            TEST_TIMEOUT,
            ask_for_leader(
                &net_c,
                &[addr_a, addr_b],
                JoinFloor::at(RecoveryEpoch::new(5, 0)),
                Duration::from_secs(5),
                Duration::ZERO,
            ),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");
        late_b.abort();

        assert_eq!(search, LeaderSearch::Found(accepted));
    }

    fn answers_after(delay: Duration, answer: Option<JoinResponse>) -> PeerAsk<'static> {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            answer
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_pass_with_no_pointer_runs_until_every_peer_answered_or_timed_out() {
        // "No leader known" is an answer but not a pointer: it starts no grace,
        // so the pass waits for the slow peer, and for the silent one's full
        // timeout.
        let asks = vec![
            answers_after(Duration::from_secs(1), Some(JoinResponse::default())),
            answers_after(Duration::from_secs(4), Some(JoinResponse::default())),
            answers_after(Duration::from_secs(10), None),
        ];
        let started = Instant::now();

        let answers = gather_answers(asks, GRACE, |pointer| pointed_leader(pointer).is_some()).await;

        assert_eq!(
            answers,
            vec![Some(JoinResponse::default()), Some(JoinResponse::default()), None]
        );
        assert_eq!(started.elapsed(), Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn a_pass_ends_one_grace_after_the_first_pointer_keeping_answers_in_peer_order() {
        let leader = WorkerId::new("leader-1");
        let leader_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let pointer = pointer_to(&leader, &leader_addr);
        let asks = vec![
            // Silent and listed first: its timeout must not hold up the pass.
            answers_after(Duration::from_secs(10), None),
            // Starts the grace at one second.
            answers_after(Duration::from_secs(1), Some(pointer.clone())),
            // Listed after the first pointer but arrives within the grace.
            answers_after(Duration::from_millis(1500), Some(pointer.clone())),
            // Arrives after the grace has ended.
            answers_after(Duration::from_millis(2100), Some(pointer.clone())),
        ];
        let started = Instant::now();

        let answers = gather_answers(asks, GRACE, |pointer| pointed_leader(pointer).is_some()).await;

        assert_eq!(
            answers,
            vec![None, Some(pointer.clone()), Some(pointer), None]
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1) + GRACE);
    }

    #[test]
    fn pointed_leader_needs_a_leader_and_an_address_that_parses() {
        let leader = WorkerId::new("leader-1");
        let leader_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let pointer = pointer_to(&leader, &leader_addr);

        assert_eq!(pointed_leader(&pointer), Some((leader, leader_addr)));
        assert_eq!(pointed_leader(&JoinResponse::default()), None);
        let garbled = JoinResponse {
            leader_multiaddr: "not a multiaddr".into(),
            ..pointer
        };
        assert_eq!(pointed_leader(&garbled), None);
    }

    #[tokio::test]
    async fn ask_for_leader_passes_over_a_pointer_to_a_leader_it_cannot_reach() {
        let (net_a, addr_a) = listening_net().await;
        let (net_b, addr_b) = listening_net().await;
        let (net_z, addr_z) = listening_net().await;
        let net_c = Net::new();
        let worker_b = net_b.local_worker_id();

        // Seed A names a leader that never runs, at an address where some
        // other worker (net_z) answers: the dial connects, but not to that
        // leader.
        let absent_leader = worker_that_never_runs();
        let _responder_a =
            spawn_join_responder(Arc::new(net_a), pointer_to(&absent_leader, &addr_z));
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(Arc::new(net_b), response_b.clone());

        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[addr_a, addr_b], JoinFloor::none(), Duration::from_secs(5), REAL_SOCKET_GRACE),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::Found(response_b));
        // A connection to the wrong peer is not kept: a stale pointer asked
        // about on every pass would otherwise pile up connections.
        let worker_z = net_z.local_worker_id();
        let mut still_connected_to_z = false;
        for input in net_c.take_inputs() {
            match input {
                Input::PeerConnected(peer) if peer == worker_z => still_connected_to_z = true,
                Input::PeerDisconnected(peer) if peer == worker_z => still_connected_to_z = false,
                _ => {}
            }
        }
        assert!(
            !still_connected_to_z,
            "net_c must not stay connected to net_z, which is not the leader it was pointed at"
        );
    }

    #[tokio::test]
    async fn a_pointer_only_to_an_undialable_leader_is_an_answer_with_no_reachable_leader() {
        let (net_a, addr_a) = listening_net().await;
        let net_c = Net::new();

        // Nothing listens at this address, so the pointed leader cannot be
        // reached; the seed did answer, so the shard exists.
        let unreachable_leader_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let absent_leader = worker_that_never_runs();
        let _responder = spawn_join_responder(
            Arc::new(net_a),
            pointer_to(&absent_leader, &unreachable_leader_addr),
        );

        let search = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[addr_a], JoinFloor::none(), Duration::from_secs(1), REAL_SOCKET_GRACE),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(search, LeaderSearch::NoReachableLeader);
    }

    #[tokio::test]
    async fn ask_for_leader_dials_the_leader_it_is_pointed_at() {
        let (net_leader, leader_addr) = listening_net().await;
        let (net_a, seed_addr) = listening_net().await;
        let net_c = Net::new();

        let worker_leader = net_leader.local_worker_id();

        let _responder =
            spawn_join_responder(Arc::new(net_a), pointer_to(&worker_leader, &leader_addr));

        timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[seed_addr], JoinFloor::none(), Duration::from_secs(5), REAL_SOCKET_GRACE),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        expect_input(
            &net_c,
            Input::PeerConnected(worker_leader),
            "net_c connected to the leader it was pointed at",
        )
        .await;
    }

    #[tokio::test]
    async fn ask_for_leader_returns_none_when_the_only_seed_never_responds() {
        let net_c = Net::new();
        // Nothing listens here, so dialing it fails to connect.
        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();

        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[unreachable_seed], JoinFloor::none(), Duration::from_secs(2), REAL_SOCKET_GRACE),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::NoAnswer);
    }

    #[tokio::test]
    async fn ask_for_leader_falls_through_a_non_responding_seed_to_the_next() {
        // A seed that never answers does not stop the join: the live seed
        // listed after it still gets its answer heard.
        let (net_a, listen_addr) = listening_net().await;
        let net_c = Net::new();

        let worker_a = net_a.local_worker_id();

        let response = pointer_to(&worker_a, &listen_addr);
        let _responder = spawn_join_responder(Arc::new(net_a), response.clone());

        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let seeds = vec![unreachable_seed, listen_addr];

        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &seeds, JoinFloor::none(), Duration::from_secs(5), REAL_SOCKET_GRACE),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::Found(response));
    }

    /// Regression test for the bug this fix addresses: `ask_peer_for_leader`
    /// used to identify "the newly connected peer" for a seed by diffing
    /// the connected-peer set before/after the dial, with no correlation to
    /// the specific dial in progress. If a seed's dial connects *late* —
    /// after its per-seed timeout has elapsed and the cascade has moved on
    /// to the next seed — that late `ConnectionEstablished` could be
    /// misattributed as the *next* seed's peer, and `JOIN_REQUEST` would go
    /// to the wrong node.
    /// `ask_for_leader_falls_through_a_non_responding_seed_to_the_next`
    /// (above) doesn't catch this: its non-responding seed is genuinely
    /// unreachable (nothing listens on that port), so it fails fast
    /// (`OutgoingConnectionError`) rather than ever connecting late.
    ///
    /// This test forces the hazard deterministically where it can be forced
    /// deterministically (seed A's dial is abandoned — its response channel
    /// dropped — exactly as a timed-out `tokio::time::timeout` would do it,
    /// *before* seed B's dial is even issued, so the two are genuinely
    /// in flight concurrently) and documents the one piece that's left to
    /// real, uncontrolled scheduling: whether seed A's real connection
    /// happens to land while seed B's own dial is still pending in
    /// `pending_dials` (likely, since both are driven by the same
    /// single-threaded driver task polling both dials concurrently, but not
    /// something this test can force to happen on every run). The fix makes
    /// the outcome correct either way — seed B's slot always resolves to
    /// seed B's own connection, correlated by that dial's own
    /// `ConnectionId` — which is what this test asserts.
    #[tokio::test]
    async fn ask_for_leader_ignores_a_late_connection_from_an_abandoned_seed() {
        let (net_a, addr_a) = listening_net().await;
        let (net_b, addr_b) = listening_net().await;
        let net_c = Net::new();

        let worker_a = net_a.local_worker_id();
        let worker_b = net_b.local_worker_id();

        // Seed A would point at itself, which is obviously wrong for this
        // test, so a misattribution is easy to detect.
        let _responder_a = spawn_join_responder(Arc::new(net_a), pointer_to(&worker_a, &addr_a));

        // Seed B points at itself too: a distinct leader.
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(Arc::new(net_b), response_b.clone());

        // Simulate `ask_peer_for_leader` abandoning seed A's dial once its
        // per-seed timeout elapses: a timeout of zero issues the dial and
        // drops its response channel at once, *before* seed B's dial is
        // issued, so seed A's real connection is free to keep completing in
        // the background while seed B's dial is in flight.
        let abandoned = timeout(
            Duration::ZERO,
            net_c.dial_for_connection(DialTarget::Address(addr_a.clone())),
        )
        .await;
        assert!(abandoned.is_err(), "seed A's dial cannot have connected at once");

        // Run the real cascade against seed B only. If seed A's abandoned
        // dial connects while this is in flight, the fix must not let that
        // leak into seed B's result.
        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[addr_b], JoinFloor::none(), Duration::from_secs(5), REAL_SOCKET_GRACE),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(
            pointer,
            LeaderSearch::Found(response_b),
            "seed B's join must resolve to seed B's own answer, never seed A's, \
             even though seed A's abandoned dial may still be completing concurrently"
        );

        // Confirm seed A's dial really did complete in the background
        // (proving this test actually exercised a live late connection, not
        // a dial that simply never connected) — its already-abandoned
        // response channel makes that harmless, which is exactly the
        // property under test.
        expect_input(
            &net_c,
            Input::PeerConnected(worker_a),
            "seed A's abandoned dial still connected in the background",
        )
        .await;
    }

    #[tokio::test]
    async fn a_pending_member_that_suspects_its_leader_still_points_joiners_at_it() {
        let (net_leader, leader_addr) = listening_net().await;
        let net_joiner = Net::new();
        let leader = net_leader.local_worker_id();
        let joiner = net_joiner.local_worker_id();

        // The joiner dials the leader it was pointed at, as ask_for_leader
        // does, so it holds a dialable address for it.
        net_joiner.dial(leader_addr.clone());
        timeout(TEST_TIMEOUT, async {
            while net_joiner.dialable_address(&leader).await.is_none() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("net_joiner connected to the leader within the timeout");

        let pointer = JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 3,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        };
        let identity = Identity {
            id: joiner.clone(),
            incarnation: IncarnationId::new("joiner-incarnation-0"),
            shard: ShardId::new("shard-1"),
            // A short suspicion timeout whose lease still fits two heartbeat
            // intervals, as a joining node's must.
            timings: ElectionTimings::new(
                TickDuration::from_millis(5),
                TickDuration::from_millis(1),
            )
            .with_roll_call_deadline(TickDuration::from_millis(2)),
        };
        let (mut node, _) = WorkerNode::start(
            identity,
            Entry::Joining(pointer.clone()),
            RealClock::new(),
            None,
        );

        // No leader node runs here to ack the pending member's heartbeats,
        // so it suspects its leader once its suspicion timeout, lengthened
        // by less than half by its jitter, has passed.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = node.step(Input::Tick);
        assert_eq!(node.state(), WorkerState::LeaderSuspect);

        assert_eq!(pointer_for(&node, &net_joiner).await, pointer);
    }

    #[tokio::test]
    async fn a_request_the_answering_side_drops_is_no_answer_at_once() {
        let (seed, seed_addr) = listening_net().await;
        let seed = Arc::new(seed);
        let dropping = Arc::clone(&seed);
        let _dropper = tokio::spawn(async move {
            loop {
                drop(dropping.poll_join_requests());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let net_c = Net::new();

        // Well under the per-peer timeout: a dropped request settles as a
        // failure, not a wait.
        let search = timeout(
            Duration::from_secs(2),
            ask_for_leader(&net_c, &[seed_addr], JoinFloor::none(), Duration::from_secs(5), REAL_SOCKET_GRACE),
        )
        .await
        .expect("the dropped request settled at once");

        assert_eq!(search, LeaderSearch::NoAnswer);
        assert_eq!(seed.diagnostics().await.traffic.join_requests_received, 1);
    }
}

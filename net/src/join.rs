//! The JOIN protocol's two halves over [`Net`] (`/kabudachi/join/1`, see
//! `crate::join_codec`), one for the bootstrap cascade and the driver's
//! rejoin alike:
//!
//! - the client: [`ask_for_leader`] asks peers in order who leads the shard
//!   and connects to the first leader one points at; [`find_leader`] asks
//!   the workers the coordination authority lists as live, for a node that
//!   rejoins its shard;
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

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::AuthorityError;
use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::{JoinRequest, JoinResponse};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::time::Clock;
use libp2p::{Multiaddr, PeerId};

use crate::bootstrap::{WaitLog, WaitReason};
use crate::driver::SharedAuthority;
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
/// `JOIN_RESPONSE` before moving on to the next peer.
pub const DEFAULT_JOIN_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// What one pass of [`ask_for_leader`] over its peers found.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaderSearch {
    /// A peer pointed at a leader, and this node is now connected to it.
    Found(JoinResponse),
    /// Some peer answered, but none pointed at a leader this node could
    /// reach: the shard exists, and its leader may not be elected yet.
    NoReachableLeader,
    /// No peer answered at all.
    NoAnswer,
}

/// One pass of the JOIN client (README §27 Phase 2, spec decision 5 step
/// (a)): asks each of `peers` in order who leads the shard, over `net`, and
/// returns [`LeaderSearch::Found`] with the first `JOIN_RESPONSE` that
/// points at a leader this node is then connected to; the caller enters the
/// shard with it (`core::election::Entry::Joining`). A peer that fails to
/// connect or answer within `per_peer_timeout`, answers "no leader known",
/// or points at a leader this node cannot reach (see [`connect_to_leader`]),
/// is passed over for the next one.
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
    per_peer_timeout: StdDuration,
) -> LeaderSearch {
    let mut a_peer_answered = false;
    for peer in peers {
        let Some(response) = ask_peer_for_leader(net, peer, per_peer_timeout).await else {
            continue;
        };
        a_peer_answered = true;
        let Some((leader, leader_addr)) = pointed_leader(&response) else {
            continue;
        };
        if connect_to_leader(net, &leader, leader_addr, per_peer_timeout).await {
            return LeaderSearch::Found(response);
        }
    }
    if a_peer_answered {
        LeaderSearch::NoReachableLeader
    } else {
        LeaderSearch::NoAnswer
    }
}

/// Asks the workers `authority` lists as live for `shard_id`, other than
/// `my_id`, who leads, at the addresses they registered (see
/// [`ask_registered_peers`]), for a node that rejoins its shard after a
/// recovery went on without it. The list is asked starting `start_at`
/// workers along it, so one worker whose pointer the node keeps refusing
/// cannot answer first every time. [`LeaderSearch::NoAnswer`] when the
/// authority cannot be read or lists no one else; why is logged on `log`.
/// An authority whose read panics counts as unreachable: the driver running
/// this search keeps running, and searches again.
pub(crate) async fn find_leader(
    net: &Net,
    authority: &SharedAuthority,
    shard_id: &ShardId,
    my_id: &WorkerId,
    start_at: usize,
    per_peer_timeout: StdDuration,
    log: &mut WaitLog,
) -> LeaderSearch {
    let (reader, shard) = (Arc::clone(authority), shard_id.clone());
    let read = tokio::task::spawn_blocking(move || reader.live_registrations(&shard)).await;
    let registrations = match read {
        Ok(Ok(registrations)) => registrations,
        Ok(Err(error)) => {
            log.log(WaitReason::AuthorityUnreachable(error));
            return LeaderSearch::NoAnswer;
        }
        // The read panicked on the blocking pool.
        Err(_) => {
            log.log(WaitReason::AuthorityUnreachable(AuthorityError::Unavailable));
            return LeaderSearch::NoAnswer;
        }
    };
    let registered: BTreeMap<WorkerId, String> = registrations
        .addresses()
        .iter()
        .filter(|(worker, _)| *worker != my_id)
        .map(|(worker, address)| (worker.clone(), address.clone()))
        .collect();
    if registered.is_empty() {
        return LeaderSearch::NoAnswer;
    }
    ask_registered_peers(net, &registered, start_at, per_peer_timeout, log).await
}

/// Asks `peers`, at their registered addresses, who leads the shard, the way
/// seeds are asked ([`ask_for_leader`]), starting `start_at` peers along the
/// list and wrapping round. An address that does not parse is skipped, and
/// logged on `log`, as is a list none of whose addresses parse, or whose
/// peers none answer.
pub(crate) async fn ask_registered_peers(
    net: &Net,
    peers: &BTreeMap<WorkerId, String>,
    start_at: usize,
    per_peer_timeout: StdDuration,
    log: &mut WaitLog,
) -> LeaderSearch {
    let mut addresses: Vec<Multiaddr> = Vec::new();
    for (worker, address) in peers {
        match address.parse() {
            Ok(address) => addresses.push(address),
            Err(error) => log.log(WaitReason::UnparseableAddress {
                worker: worker.clone(),
                address: address.clone(),
                error: error.to_string(),
            }),
        }
    }
    let peer_ids: Vec<WorkerId> = peers.keys().cloned().collect();
    if addresses.is_empty() {
        log.log(WaitReason::NoRegisteredAddressParses { peers: peer_ids });
        return LeaderSearch::NoAnswer;
    }

    let len = addresses.len();
    addresses.rotate_left(start_at % len);
    let search = ask_for_leader(net, &addresses, per_peer_timeout).await;
    if search == LeaderSearch::NoAnswer {
        log.log(WaitReason::RegisteredPeersSilent { peers: peer_ids });
    }
    search
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
    use std::time::Duration;

    use kabudachi_core::election::{ElectionTimings, Entry, Identity, Input};
    use kabudachi_core::protocol::ids::IncarnationId;
    use kabudachi_core::protocol::worker_state::WorkerState;
    use kabudachi_core::time::{Duration as TickDuration, RealClock};
    use libp2p::identity;
    use tokio::time::timeout;

    use super::*;
    use crate::swarm::build_swarm;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// The `WorkerId` of a fresh keypair no `Net` was ever built from.
    fn worker_that_never_runs() -> WorkerId {
        let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        WorkerId::new(peer.to_string())
    }

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

    fn pointer_to(leader: &WorkerId, leader_addr: &Multiaddr) -> JoinResponse {
        JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        }
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


    /// Spawns a background task that answers every `/kabudachi/join/1`
    /// request `net` receives with `response`, mirroring (at the messenger
    /// level, not through `core::election::WorkerNode`) what
    /// `crate::driver::run_driver`'s join responder does in production.
    fn spawn_join_responder(net: Net, response: JoinResponse) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                for handle in net.poll_join_requests() {
                    net.respond_join(handle, response.clone());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    #[tokio::test]
    async fn ask_for_leader_passes_over_a_pointer_to_a_leader_it_cannot_reach() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_z = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let mut addrs = Vec::new();
        for net in [&net_a, &net_b, &net_z] {
            let addr = timeout(
                TEST_TIMEOUT,
                net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
            )
            .await
            .expect("the net produced a listen address within the timeout");
            addrs.push(addr);
        }
        let (addr_a, addr_b, addr_z) = (addrs[0].clone(), addrs[1].clone(), addrs[2].clone());
        let worker_b = net_b.local_worker_id();

        // Seed A names a leader that never runs, at an address where some
        // other worker (net_z) answers: the dial connects, but not to that
        // leader.
        let absent_leader = worker_that_never_runs();
        let _responder_a = spawn_join_responder(net_a, pointer_to(&absent_leader, &addr_z));
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(net_b, response_b.clone());

        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[addr_a, addr_b], Duration::from_secs(5)),
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
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let addr_a = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");

        // Nothing listens at this address, so the pointed leader cannot be
        // reached; the seed did answer, so the shard exists.
        let unreachable_leader_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let absent_leader = worker_that_never_runs();
        let _responder =
            spawn_join_responder(net_a, pointer_to(&absent_leader, &unreachable_leader_addr));

        let search = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[addr_a], Duration::from_secs(1)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(search, LeaderSearch::NoReachableLeader);
    }

    #[tokio::test]
    async fn ask_for_leader_dials_the_leader_it_is_pointed_at() {
        let net_leader = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let leader_addr = timeout(
            TEST_TIMEOUT,
            net_leader.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_leader produced a listen address within the timeout");
        let seed_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let worker_leader = net_leader.local_worker_id();

        let _responder = spawn_join_responder(net_a, pointer_to(&worker_leader, &leader_addr));

        timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[seed_addr], Duration::from_secs(5)),
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
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // Nothing listens here, so dialing it fails to connect.
        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();

        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &[unreachable_seed], Duration::from_secs(2)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::NoAnswer);
    }

    #[tokio::test]
    async fn ask_for_leader_falls_through_a_non_responding_seed_to_the_next() {
        // Proves spec decision 5 step (a)'s cascade: seeds are dialed in
        // order, and a seed that doesn't answer doesn't stop the join —
        // the next seed in the list still gets a chance.
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let worker_a = net_a.local_worker_id();

        let response = pointer_to(&worker_a, &listen_addr);
        let _responder = spawn_join_responder(net_a, response.clone());

        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let seeds = vec![unreachable_seed, listen_addr];

        let pointer = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&net_c, &seeds, Duration::from_secs(5)),
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
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let addr_a = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");

        let worker_a = net_a.local_worker_id();
        let worker_b = net_b.local_worker_id();

        // Seed A would point at itself, which is obviously wrong for this
        // test, so a misattribution is easy to detect.
        let _responder_a = spawn_join_responder(net_a, pointer_to(&worker_a, &addr_a));

        // Seed B points at itself too: a distinct leader.
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(net_b, response_b.clone());

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
            ask_for_leader(&net_c, &[addr_b], Duration::from_secs(5)),
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
        let net_leader = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_joiner = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let leader_addr = timeout(
            TEST_TIMEOUT,
            net_leader.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_leader produced a listen address within the timeout");
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
            ),
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
        let seed = Arc::new(Net::new(build_swarm(identity::Keypair::generate_ed25519())));
        let seed_addr = timeout(
            TEST_TIMEOUT,
            seed.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("the seed produced a listen address within the timeout");
        let dropping = Arc::clone(&seed);
        let _dropper = tokio::spawn(async move {
            loop {
                drop(dropping.poll_join_requests());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        // Well under the per-peer timeout: a dropped request settles as a
        // failure, not a wait.
        let search = timeout(
            Duration::from_secs(2),
            ask_for_leader(&net_c, &[seed_addr], Duration::from_secs(5)),
        )
        .await
        .expect("the dropped request settled at once");

        assert_eq!(search, LeaderSearch::NoAnswer);
        assert_eq!(seed.diagnostics().await.traffic.join_requests_received, 1);
    }
}

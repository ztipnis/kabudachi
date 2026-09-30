//! Helpers shared by the unit tests in this crate. Integration tests are
//! separate crates and keep their own in `tests/support`.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::time::Duration as TickDuration;
use kabudachi_core::election::CallKind;
use kabudachi_testkit::FaultingAuthority;
use libp2p::{Multiaddr, identity};

use crate::join::LeaderSearch;
use crate::leader_search::AskWhoLeads;
use crate::messenger::Net;

/// How long a unit test waits for anything that should happen at once.
pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// A `core` clock that reads tokio's clock, so paused tokio time pauses it too.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TokioClock {
    origin: tokio::time::Instant,
}

impl TokioClock {
    pub(crate) fn new() -> Self {
        TokioClock {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl kabudachi_core::time::Clock for TokioClock {
    fn now(&self) -> kabudachi_core::time::Instant {
        kabudachi_core::time::Instant::at(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    // Only breaks ties between roll calls; a monotonic reading is fine here.
    fn wall_clock_millis(&self) -> u64 {
        kabudachi_core::time::Clock::now(self).as_ticks()
    }
}

/// What a scripted address answers when asked.
#[derive(Clone)]
pub(crate) enum Answer {
    /// Points at a leader this worker then reaches.
    Pointer(JoinResponse),
    /// Answers, but knows no leader.
    NoLeader,
    /// Never answers.
    Silent,
}

/// Scripted answers per address (the last repeats once a queue runs dry; an
/// address with none is silent), and every pass asked, in order. Clones share
/// both, so a test keeps one to script and read while the search owns another.
#[derive(Clone, Default)]
pub(crate) struct Scripted {
    answers: Arc<Mutex<BTreeMap<Multiaddr, VecDeque<Answer>>>>,
    passes: Arc<Mutex<Vec<Vec<Multiaddr>>>>,
}

impl Scripted {
    /// Sets what `address` answers, in order, from now on.
    pub(crate) fn script(&self, address: &Multiaddr, answers: impl IntoIterator<Item = Answer>) {
        self.answers
            .lock()
            .unwrap()
            .insert(address.clone(), answers.into_iter().collect());
    }

    /// The passes asked so far.
    pub(crate) fn passes(&self) -> Vec<Vec<Multiaddr>> {
        self.passes.lock().unwrap().clone()
    }

    fn answer(&self, address: &Multiaddr) -> Answer {
        let mut answers = self.answers.lock().unwrap();
        match answers.get_mut(address) {
            Some(queue) if queue.len() > 1 => queue.pop_front().expect("checked non-empty"),
            Some(queue) => queue.front().cloned().unwrap_or(Answer::Silent),
            None => Answer::Silent,
        }
    }
}

impl AskWhoLeads for Scripted {
    /// Walks the pass as `join::ask_for_leader` walks its peers. A pass over
    /// no address asks no one, and is not recorded.
    fn ask(&mut self, addresses: &[Multiaddr]) -> impl Future<Output = LeaderSearch> + Send {
        if !addresses.is_empty() {
            self.passes.lock().unwrap().push(addresses.to_vec());
        }
        let mut answered = false;
        let mut found = None;
        for address in addresses {
            match self.answer(address) {
                Answer::Silent => {}
                Answer::NoLeader => answered = true,
                Answer::Pointer(pointer) => {
                    found = Some(pointer);
                    break;
                }
            }
        }
        let search = match found {
            Some(pointer) => LeaderSearch::Found(pointer),
            None if answered => LeaderSearch::NoReachableLeader,
            None => LeaderSearch::NoAnswer,
        };
        std::future::ready(search)
    }
}

/// An authority on tokio's clock whose registrations last `ttl`, already
/// warm, and the clock.
pub(crate) async fn warm_authority(ttl: Duration) -> (FaultingAuthority<TokioClock>, TokioClock) {
    let clock = TokioClock::new();
    let authority = FaultingAuthority::new(clock, TickDuration::from_ticks(ttl.as_millis() as u64));
    tokio::time::sleep(ttl).await;
    (authority, clock)
}

pub(crate) fn address(port: u16) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}").parse().expect("a multiaddr")
}

pub(crate) fn pointer_to(leader: &str, at: &Multiaddr) -> JoinResponse {
    JoinResponse {
        leader_id: Some(WorkerId::new(leader).into()),
        leader_multiaddr: at.to_string(),
        term: 1,
        recovery_epoch: 0,
        recovery_epoch_lineage: 0,
    }
}

pub(crate) fn register(authority: &FaultingAuthority<TokioClock>, worker: &str, at: &str) {
    authority
        .register(&ShardId::new("shard-1"), &WorkerId::new(worker), at)
        .expect("the authority is reachable");
}

pub(crate) fn epoch_number(authority: &FaultingAuthority<TokioClock>) -> Option<u64> {
    authority
        .for_another_worker()
        .read_recovery_epoch(&ShardId::new("shard-1"))
        .expect("the authority is reachable")
        .map(|epoch| epoch.number)
}

/// Yields until a call of `kind` is held on `authority`.
pub(crate) async fn wait_until_held(authority: &FaultingAuthority<TokioClock>, kind: CallKind) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !authority.is_holding(kind) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a {kind:?} call was held within the timeout"));
}

/// The `WorkerId` of a fresh keypair no `Net` was ever built from.
pub(crate) fn worker_that_never_runs() -> WorkerId {
    let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
    WorkerId::new(peer.to_string())
}

/// A `Net` listening on a loopback port, and that address.
pub(crate) async fn listening_net() -> (Net, Multiaddr) {
    let net = Net::new();
    let address = tokio::time::timeout(
        TEST_TIMEOUT,
        net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("the net produced a listen address within the timeout");
    (net, address)
}

/// Answers every `/kabudachi/join/1` request `net` receives with `response`,
/// for ever, as `crate::driver::run_driver`'s join responder does in
/// production, but without a node behind it. An `Arc`, because a test that
/// reads the net's inputs meanwhile keeps a handle too.
pub(crate) fn spawn_join_responder(
    net: Arc<Net>,
    response: JoinResponse,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            for handle in net.poll_join_requests() {
                net.respond_join(handle, response.clone());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
}

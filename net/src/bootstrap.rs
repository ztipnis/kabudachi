//! Wires the whole README §27 Phase 2 bootstrap cascade (spec decision 5)
//! for a fresh `core::election::WorkerNode`: (a) try seeds first (chunk C4's
//! [`crate::messenger::Net::join_via_seeds`]); (b) if that fails or there are
//! no seeds, fall back to asking a `CoordinationAuthority` for the shard's
//! current membership; (c) if the authority is unavailable, unconfigured, or
//! reports an empty shard, self-elect as a new one-member shard.
//!
//! ## Why this needs no new `core` state-machine code
//!
//! `core::election::WorkerNode::finish_joining(members)` already does
//! exactly "adopt this membership, transition `Bootstrapping -> Joining ->
//! Active`" — it doesn't care where `members` came from, and it always adds
//! this node's own id to whatever set it's given (see its doc). That means:
//!
//! - Step (a)/(b) both reduce to `finish_joining(members)` for whatever
//!   non-empty membership was discovered (a `JOIN_RESPONSE`'s members, or an
//!   authority's `discover_workers` result) — `finish_joining` unions in
//!   `self` either way, so there's no need to build that union here.
//! - Step (c) reduces to `finish_joining(BTreeSet::new())`: with nothing
//!   discovered, that call still adds `self`, producing exactly the
//!   one-member electorate a lone node needs. Ordinary `tick()`-driven
//!   election then runs unmodified (`Active -> LeaderSuspect -> RollCall ->
//!   Candidate -> LeaderReconciling -> Leader`) with whatever
//!   `suspect_timeout` this node was configured with — the same generic
//!   path every other electorate size takes, not a special near-zero
//!   timeout (that was `core::single_node`'s Phase-1-only shortcut, now
//!   `bindings`-local — see `bindings/src/local_node.rs`).
//!
//! So steps (b) and (c) collapse to one line each: read the authority (if
//! seeds didn't already produce a membership), then call `finish_joining`
//! with whatever was found — empty or not. This module is pure orchestration
//! (which source to try, in which order) over machinery `net` and `core`
//! already have; it holds no election logic of its own.
//!
//! ## Why a plain `InMemoryAuthority` can stand in for "no authority"
//!
//! There's no dedicated "no authority" `CoordinationAuthority` in this
//! chunk: a `kabudachi_core::in_memory_authority::InMemoryAuthority` with
//! nothing ever registered against a shard already answers
//! `discover_workers` with `Ok(empty)` for that shard, which step (c) above
//! treats identically to an authority error. A caller that has no real
//! coordination service to offer can simply pass a fresh, never-populated
//! `InMemoryAuthority`.

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::time::{Clock, Duration};
use libp2p::Multiaddr;

use crate::messenger::Net;

/// Bootstraps a fresh `WorkerNode` for `net`'s local worker (README §27
/// Phase 2, spec decision 5's full cascade): tries `seeds` in order via
/// [`Net::join_via_seeds`]; on failure or an empty seed list, asks
/// `authority.discover_workers(&shard_id)`; if that's also unavailable or
/// empty, self-elects alone. Returns a node already driven
/// `Bootstrapping -> Joining -> Active` — see the module doc for why every
/// branch is just a different membership handed to the same
/// `WorkerNode::finish_joining` call.
///
/// `per_seed_timeout` is forwarded to `Net::join_via_seeds` unchanged.
/// `suspect_timeout` is the node's ordinary election suspicion timeout (see
/// `WorkerNode::new`'s doc) — used as-is by every path, including
/// self-election: a node that lands here alone still waits it out like any
/// other node before leading (contrast `bindings`'s deliberately instant
/// self-election for its single-process runtime, documented on
/// `bindings::local_node`).
///
/// Joining is one-way in Phase 2. Only the joining node adds itself to its
/// electorate; the members that answered it keep their old electorate, and
/// nothing admits the joiner into theirs. After `c` joins `{a, b}`, `c`
/// believes the electorate is `{a, b, c}` while `a` and `b` still believe
/// `{a, b}`, so they disagree about quorum and ring neighbours. Admitting a
/// joiner needs leader-driven membership propagation, which is new election
/// behaviour in `core` and outside Phase 2's scope (README §27 Phase 2).
///
/// The authority fallback does not produce a working node yet.
/// `CoordinationAuthority::discover_workers` returns `WorkerId`s only, with
/// no addresses, so the node becomes `Active` in an electorate it has not
/// dialed; and the node starts at recovery epoch 0 whatever the shard's
/// epoch is. It then cannot make quorum or elect a leader. That fails safe
/// (it never leads alone while other members exist) but does not recover on
/// its own. Fixing it needs an authority that returns addresses and the
/// recovery epoch, which is Phase 4's real `CoordinationAuthority`;
/// `InMemoryAuthority` has neither to give. Use seeds until then.
#[allow(clippy::too_many_arguments)]
pub async fn bootstrap_node<'a, C, A>(
    my_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    clock: C,
    net: &'a Net,
    authority: A,
    suspect_timeout: Duration,
    seeds: &[Multiaddr],
    per_seed_timeout: StdDuration,
) -> WorkerNode<C, &'a Net, RingMembership, A>
where
    C: Clock,
    A: CoordinationAuthority,
{
    let via_seeds = if seeds.is_empty() {
        None
    } else {
        net.join_via_seeds(seeds, per_seed_timeout).await
    };

    // `authority` is only borrowed here, not consumed: the value itself
    // moves into `WorkerNode::bootstrapping` below. An `Err` and an
    // `Ok(empty set)` are handled identically (`unwrap_or_default`) per
    // spec decision 5 step (c)'s "unavailable, or empty result".
    let members = match via_seeds {
        Some(members) => members,
        None => authority.discover_workers(&shard_id).unwrap_or_default(),
    };

    let mut node = WorkerNode::bootstrapping(
        my_id,
        incarnation_id,
        shard_id,
        clock,
        net,
        RingMembership::new(BTreeSet::new()),
        authority,
        suspect_timeout,
    );
    node.finish_joining(members);
    node
}

#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use kabudachi_core::in_memory_authority::InMemoryAuthority;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
    use kabudachi_core::protocol::messages::{JoinMember, JoinResponse};
    use kabudachi_core::protocol::worker_state::WorkerState;
    use kabudachi_core::time::Duration;
    use libp2p::identity;
    use tokio::time::timeout;

    use super::*;
    use crate::swarm::build_swarm;

    const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);

    fn real_clock() -> RealTestClock {
        RealTestClock::new()
    }

    fn suspect_timeout() -> Duration {
        Duration::from_ticks(300)
    }

    /// A minimal `Clock` for this module's own tests, duplicated from
    /// `net/tests/support/clock.rs` (a `#[cfg(test)]`-only, `tests/`-scoped
    /// module this crate's `src/` can't reach) rather than promoted to a
    /// shared, non-test dependency for two small test files.
    #[derive(Debug, Clone, Copy)]
    struct RealTestClock {
        origin: std::time::Instant,
    }

    impl RealTestClock {
        fn new() -> Self {
            Self {
                origin: std::time::Instant::now(),
            }
        }
    }

    impl kabudachi_core::time::Clock for RealTestClock {
        fn now(&self) -> kabudachi_core::time::Instant {
            let millis = self.origin.elapsed().as_millis();
            kabudachi_core::time::Instant::at(u64::try_from(millis).unwrap_or(u64::MAX))
        }
    }

    /// Spawns a background task that answers every `/kabudachi/join/1`
    /// request `net` receives with `response`, duplicated from
    /// `messenger`'s own private `#[cfg(test)]` helper of the same name for
    /// the same reason as `RealTestClock` above.
    fn spawn_join_responder(net: Net, response: JoinResponse) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                for handle in net.poll_join_requests() {
                    net.respond_join(handle, response.clone());
                }
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        })
    }

    #[tokio::test]
    async fn a_responding_seed_wins_over_the_authority() {
        let seed_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let joining_net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let seed_addr = timeout(
            TEST_TIMEOUT,
            seed_net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("seed produced a listen address within the timeout");
        let seed_worker = seed_net.local_worker_id();

        let response = JoinResponse {
            members: vec![JoinMember {
                worker_id: Some(seed_worker.clone().into()),
                multiaddr: seed_addr.to_string(),
            }],
        };
        let _responder = spawn_join_responder(seed_net, response);

        // An authority that, if consulted, would hand back a *different*
        // membership — proving the seed's result is what actually won, not
        // just "a" non-empty result.
        let authority = InMemoryAuthority::new();
        let shard_id = ShardId::new("shard-1");
        authority
            .force_reconfigure(
                &shard_id,
                0,
                [WorkerId::new("wrong-worker")].into_iter().collect(),
            )
            .expect("authority is freshly constructed, so epoch 0 is the expected one");

        let my_id = joining_net.local_worker_id();
        let node = timeout(
            TEST_TIMEOUT,
            bootstrap_node(
                my_id.clone(),
                IncarnationId::new("incarnation-1"),
                shard_id,
                real_clock(),
                &joining_net,
                authority,
                suspect_timeout(),
                &[seed_addr],
                StdDuration::from_secs(5),
            ),
        )
        .await
        .expect("bootstrap_node completed within the timeout");

        assert_eq!(node.state(), WorkerState::Active);
        assert_eq!(
            node.electorate(),
            [seed_worker, my_id].into_iter().collect(),
            "a responding seed's membership must win over the authority's"
        );
    }

    #[tokio::test]
    async fn empty_seeds_fall_back_to_a_populated_authority() {
        let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let my_id = net.local_worker_id();
        let shard_id = ShardId::new("shard-1");

        let authority = InMemoryAuthority::new();
        let other_worker = WorkerId::new("other-worker");
        authority
            .force_reconfigure(&shard_id, 0, [other_worker.clone()].into_iter().collect())
            .expect("authority is freshly constructed, so epoch 0 is the expected one");

        let no_seeds: &[libp2p::Multiaddr] = &[];
        let node = timeout(
            TEST_TIMEOUT,
            bootstrap_node(
                my_id.clone(),
                IncarnationId::new("incarnation-1"),
                shard_id,
                real_clock(),
                &net,
                authority,
                suspect_timeout(),
                no_seeds,
                StdDuration::from_secs(5),
            ),
        )
        .await
        .expect("bootstrap_node completed within the timeout");

        assert_eq!(node.state(), WorkerState::Active);
        assert_eq!(
            node.electorate(),
            [other_worker, my_id].into_iter().collect(),
            "with no seeds, the authority's discovered membership must be adopted, unioned with self"
        );
    }
}

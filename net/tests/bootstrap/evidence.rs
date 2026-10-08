//! A worker that has heard from the shard never founds a second one beside
//! it, however quiet its peers then go, over real sockets: a seed that
//! answered "no leader known" once, or a registered peer that did, shows the
//! shard exists for as long as the worker bootstraps, and it keeps asking
//! until a peer points at a leader.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::coordination_authority::{CoordinationAuthority, LeaderHint, RecoveryEpoch};
use kabudachi_core::election::{AuthorityTimings, Entry};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::{JoinResponse, JoinResponseIds};
use kabudachi_core::time::{Duration as TickDuration, RealClock};
use kabudachi_net::authority::AuthorityClient;
use kabudachi_net::bootstrap::{DEFAULT_SEED_ROUNDS, bootstrap};
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::net::{JoinResponder, listening_net, pointer_to};
use crate::support::worker::{
    PER_PEER_TIMEOUT, RETRY_INTERVAL, TEST_TIMEOUT, name_of, poll_until, read_epoch, swap_epoch,
    warmed_up_in_memory_authority,
};

/// How long a worker that must not found the shard is given to: many times
/// the few rounds, a retry interval and then two apart, after which a worker
/// with no authority and silent seeds founds.
const SILENCE: Duration = Duration::from_millis(400);

/// How long a registration lasts, and so how long the authority warms up.
const TTL: Duration = Duration::from_millis(250);

const GRACE: Duration = Duration::from_secs(5);

fn shard() -> ShardId {
    ShardId::new("shard-1")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seed_that_answered_once_keeps_the_worker_from_founding_until_it_points_at_a_leader() {
    within_deadline(async {
        let (seed, seed_address) = listening_net().await;
        let (leader, leader_address) = listening_net().await;
        let pointer = pointer_to(&leader.local_worker_id(), &leader_address);
        let _leader_answers = JoinResponder::start(Arc::new(leader), Some(pointer.clone()));
        // The seed answers "no leader known" once, then goes quiet.
        let seed_answers = JoinResponder::start(Arc::new(seed), Some(JoinResponse::default()));
        let net = Net::new();
        let me = net.local_worker_id();
        let (clock, shard_id, seeds) = (RealClock::new(), shard(), [seed_address]);
        let name = shard_id.name();
        let mut running = pin!(bootstrap(
            &net,
            &clock,
            None,
            &name,
            &me,
            &seeds,
            PER_PEER_TIMEOUT,
            GRACE,
            RETRY_INTERVAL,
            DEFAULT_SEED_ROUNDS,
        ));

        tokio::select! {
            entry = &mut running => panic!("the worker entered before its seed answered: {entry:?}"),
            () = poll_until("the seed answered", || seed_answers.answered() >= 1) => {}
        }
        seed_answers.set(None);
        let founded = timeout(SILENCE, &mut running).await;
        assert!(founded.is_err(), "the worker founded a shard after its seed showed one exists");

        seed_answers.set(Some(pointer.clone()));
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the worker joined once its seed pointed at a leader");
        assert!(matches!(entry, Entry::Joining(joined) if joined == pointer));
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listed_peer_that_answered_keeps_the_worker_from_founding_after_it_lapses() {
    within_deadline(async {
        let authority: InMemoryAuthority<RealClock> =
            warmed_up_in_memory_authority(&shard(), TickDuration::from_millis(TTL.as_millis() as u64))
                .await;
        // The listed peer answers "no leader known", and is never renewed.
        let (peer, peer_address) = listening_net().await;
        let peer_id = peer.local_worker_id();
        let _peer_answers = JoinResponder::start(Arc::new(peer), Some(JoinResponse::default()));
        authority.register(&name_of(&shard()), &shard(), &peer_id, &peer_address.to_string())
            .expect("the authority is reachable");
        let net = Net::new();
        let me = net.local_worker_id();
        let (clock, shard_id) = (RealClock::new(), shard());
        let name = shard_id.name();
        let timings = AuthorityTimings {
            ttl: TickDuration::from_millis(TTL.as_millis() as u64),
        };
        let mut client =
            AuthorityClient::new(
                &net,
                name_of(&shard_id),
                shard_id.clone(),
                Arc::new(authority.clone()),
                timings,
            );
        let mut running = pin!(bootstrap(
            &net,
            &clock,
            Some(&mut client),
            &name,
            &me,
            &[],
            PER_PEER_TIMEOUT,
            GRACE,
            RETRY_INTERVAL,
            DEFAULT_SEED_ROUNDS,
        ));

        // The listing is warm and empty once the registration lapses, and the
        // worker, which has been told by that peer that the shard exists, still
        // does not take ownership of it.
        let lapsed = async {
            poll_until("the peer's registration lapsed", || {
                authority.live_registrations(&name_of(&shard()), &shard())
                    .expect("the authority is reachable")
                    .addresses()
                    .is_empty()
            })
            .await;
            tokio::time::sleep(SILENCE).await;
        };
        tokio::select! {
            entry = &mut running => panic!("the worker entered after its only peer left the listing: {entry:?}"),
            () = lapsed => {}
        }
        assert_eq!(
            read_epoch(&authority, &shard())
                .expect("the authority is reachable"),
            None,
            "the worker took ownership of a shard its peer showed exists"
        );

        // A leader registers, and the worker joins it through the listing.
        let (leader, leader_address) = listening_net().await;
        let leader_id: WorkerId = leader.local_worker_id();
        let pointer = pointer_to(&leader_id, &leader_address);
        let _leader_answers = JoinResponder::start(Arc::new(leader), Some(pointer.clone()));
        authority.register(&name_of(&shard()), &shard(), &leader_id, &leader_address.to_string())
            .expect("the authority is reachable");
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the worker joined the leader that registered");
        assert!(matches!(entry, Entry::Joining(joined) if joined == pointer));
    })
    .await
}

// A bootstrapper that finds the shard's record asks the leader the authority
// hints at first, even while the authority warms up and lists no one; a hint
// of another incarnation, or of an older epoch, says nothing of the shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bootstrapper_asks_the_leader_its_authority_hints_at_and_ignores_another_incarnations_hint() {
    within_deadline(async {
        let ttl = Duration::from_secs(1);
        let incarnation = ShardId::new("shard-1/a");
        let record_epoch = RecoveryEpoch::new(0, 1);

        // Each case has its own authority, still warming up, holding the
        // shard's record.
        let case = || {
            let authority: InMemoryAuthority<RealClock> =
                InMemoryAuthority::new(RealClock::new(), TickDuration::from_millis(ttl.as_millis() as u64));
            swap_epoch(&authority, &incarnation, None, record_epoch).expect("a fresh authority takes the record");
            authority
        };
        let hint = |shard_id: &ShardId, leader: &WorkerId, address: &Multiaddr| LeaderHint {
            shard_id: shard_id.clone(),
            leader: leader.clone(),
            address: address.to_string(),
            recovery_epoch: record_epoch,
            term: 1,
        };
        let start = |authority: InMemoryAuthority<RealClock>| {
            let net = Net::new();
            let me = net.local_worker_id();
            let client = AuthorityClient::new(
                &net,
                name_of(&incarnation),
                ShardId::new("shard-1/candidate"),
                Arc::new(authority),
                AuthorityTimings {
                    ttl: TickDuration::from_millis(ttl.as_millis() as u64),
                },
            );
            (net, me, client)
        };

        // A leader of the recorded incarnation, which no registration lists.
        let (leader, leader_address) = listening_net().await;
        let leader_id = leader.local_worker_id();
        let pointer = JoinResponse {
            shard_id: Some(incarnation.clone().into()),
            ..pointer_to(&leader_id, &leader_address)
        };
        let _leader_answers = JoinResponder::start(Arc::new(leader), Some(pointer));

        // 1. The hint names the recorded incarnation: the worker joins that
        //    leader before the warm-up ends.
        let authority = case();
        authority
            .publish_leader_hint(&name_of(&incarnation), &hint(&incarnation, &leader_id, &leader_address))
            .expect("the authority is reachable");
        let (net, me, mut client) = start(authority);
        let started = tokio::time::Instant::now();
        let entry = timeout(
            TEST_TIMEOUT,
            bootstrap(
                &net,
                &RealClock::new(),
                Some(&mut client),
                &incarnation.name(),
                &me,
                &[],
                PER_PEER_TIMEOUT,
                GRACE,
                RETRY_INTERVAL,
                DEFAULT_SEED_ROUNDS,
            ),
        )
        .await
        .expect("the worker joined the hinted leader");
        match entry {
            Entry::Joining(pointer) => assert_eq!(pointer.shard_id(), Some(incarnation.clone())),
            other => panic!("expected to join the hinted leader: {other:?}"),
        }
        assert!(started.elapsed() < ttl, "the hinted leader was asked before the warm-up ended");

        // 2. The hint names another incarnation: it is passed over, nothing
        //    joins before the warm-up ends, and after it the worker re-founds
        //    the recorded incarnation, keeping its id.
        let authority = case();
        authority
            .publish_leader_hint(
                &name_of(&incarnation),
                &hint(&ShardId::new("shard-1/b"), &leader_id, &leader_address),
            )
            .expect("the authority is reachable");
        let (net, me, mut client) = start(authority);
        let started = tokio::time::Instant::now();
        let entry = timeout(
            TEST_TIMEOUT,
            bootstrap(
                &net,
                &RealClock::new(),
                Some(&mut client),
                &incarnation.name(),
                &me,
                &[],
                PER_PEER_TIMEOUT,
                GRACE,
                RETRY_INTERVAL,
                DEFAULT_SEED_ROUNDS,
            ),
        )
        .await
        .expect("the worker re-founded once the authority was warm");
        assert!(started.elapsed() >= ttl, "the worker acted on a hint of another incarnation");
        match entry {
            Entry::Founding { shard_id, recovery_epoch, .. } => {
                assert_eq!(shard_id, incarnation);
                assert_eq!(recovery_epoch.number, 1);
            }
            other => panic!("expected a re-founding: {other:?}"),
        }
    })
    .await
}

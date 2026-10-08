//! A worker that has heard from the shard never founds a second one beside
//! it, however quiet its peers then go, over real sockets: a seed that
//! answered "no leader known" once, or a registered peer that did, shows the
//! shard exists for as long as the worker bootstraps, and it keeps asking
//! until a peer points at a leader.

use crate::support::worker::{name_of, read_epoch};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{AuthorityTimings, Entry};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::time::{Duration as TickDuration, RealClock};
use kabudachi_net::authority::AuthorityClient;
use kabudachi_net::bootstrap::{DEFAULT_SEED_ROUNDS, bootstrap};
use kabudachi_net::messenger::Net;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::net::{JoinResponder, listening_net, pointer_to};
use crate::support::worker::{
    PER_PEER_TIMEOUT, RETRY_INTERVAL, TEST_TIMEOUT, poll_until, warmed_up_in_memory_authority,
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
        let mut running = pin!(bootstrap(
            &net,
            &clock,
            None,
            &shard_id,
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
            &shard_id,
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

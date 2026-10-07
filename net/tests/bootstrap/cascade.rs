//! The bootstrap cascade's founding decisions, through its entry point
//! (`kabudachi_net::bootstrap::bootstrap`), when no peer answers: how long a
//! worker waits on silent seeds or an authority that does not answer before
//! it founds the shard, and every case in which it must not found one. A
//! worker that does found takes epoch 0, or one epoch past an ownerless one.
//!
//! The clock is tokio's, paused, so the rounds of a worker that keeps
//! waiting cost no time. A call held on the authority stops the clock's
//! auto-advance until it is released, so a test that holds one moves time by
//! hand. Every address a worker could ask has nothing listening on it.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::coordination_authority::{
    CoordinationAuthority, RecoveryEpoch, Uuid7Lineages,
};
use kabudachi_core::election::{
    AuthorityTimings, CallKind, ElectionTimings, Entry, Identity, Input, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration as TickDuration;
use kabudachi_net::authority::AuthorityClient;
use kabudachi_net::bootstrap::{DEFAULT_SEED_ROUNDS, bootstrap};
use kabudachi_net::messenger::Net;
use kabudachi_testkit::FaultingAuthority;
use libp2p::Multiaddr;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::clock::TokioClock;

const RETRY_INTERVAL: Duration = Duration::from_millis(50);
/// Far longer than the rounds a waiting worker is given, and counted on the
/// paused clock.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a worker that must not found the shard is given to: twenty rounds.
const WAITING: Duration = Duration::from_millis(50 * 20);

/// A worker bootstrapping into `shard-1`, through `seeds` and `authority` if
/// it has them. Its net never listens, so no socket of its own opens.
struct Worker {
    net: Net,
    shard: ShardId,
    me: WorkerId,
    clock: TokioClock,
    client: Option<AuthorityClient>,
    seeds: Vec<Multiaddr>,
}

impl Worker {
    /// A worker whose authority calls are never given up on.
    fn new(
        authority: Option<&FaultingAuthority<TokioClock>>,
        clock: TokioClock,
        seeds: &[Multiaddr],
    ) -> Self {
        Self::giving_up_calls_after(authority, clock, seeds, Duration::from_secs(3600))
    }

    /// A worker that counts an authority call lost after `call_timeout`.
    fn giving_up_calls_after(
        authority: Option<&FaultingAuthority<TokioClock>>,
        clock: TokioClock,
        seeds: &[Multiaddr],
        call_timeout: Duration,
    ) -> Self {
        let net = Net::new();
        let shard = ShardId::new("shard-1");
        let timings = AuthorityTimings {
            ttl: TickDuration::from_ticks(call_timeout.as_millis() as u64),
        };
        let client = authority.map(|authority| {
            AuthorityClient::new(&net, shard.clone(), Arc::new(authority.clone()), timings)
        });
        Worker {
            me: net.local_worker_id(),
            net,
            shard,
            clock,
            client,
            seeds: seeds.to_vec(),
        }
    }

    fn bootstrap(&mut self) -> impl Future<Output = Entry> + '_ {
        bootstrap(
            &self.net,
            &self.clock,
            self.client.as_mut(),
            &self.shard,
            &self.me,
            &self.seeds,
            Duration::from_millis(100),
            Duration::from_secs(1),
            RETRY_INTERVAL,
            DEFAULT_SEED_ROUNDS,
        )
    }
}

fn nowhere(port: u16) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}").parse().expect("a multiaddr")
}

/// An authority on tokio's clock whose registrations last `ttl`, already
/// warm, and the clock.
async fn warm_authority(ttl: Duration) -> (FaultingAuthority<TokioClock>, TokioClock) {
    let clock = TokioClock::new();
    let authority = FaultingAuthority::new(clock, TickDuration::from_ticks(ttl.as_millis() as u64));
    tokio::time::sleep(ttl).await;
    (authority, clock)
}

fn register(authority: &FaultingAuthority<TokioClock>, worker: &str, at: &str) {
    authority
        .register(&ShardId::new("shard-1"), &WorkerId::new(worker), at)
        .expect("the authority is reachable");
}

fn epoch_number(authority: &FaultingAuthority<TokioClock>) -> Option<u64> {
    authority
        .for_another_worker()
        .read_recovery_epoch(&ShardId::new("shard-1"))
        .expect("the authority is reachable")
        .map(|epoch| epoch.number)
}

fn founded_at(entry: &Entry, epoch: u64) -> bool {
    matches!(entry, Entry::Founding { recovery_epoch, .. } if recovery_epoch.number == epoch)
}

/// Yields until a call of `kind` is held on `authority`.
async fn wait_until_held(authority: &FaultingAuthority<TokioClock>, kind: CallKind) {
    timeout(TEST_TIMEOUT, async {
        while !authority.is_holding(kind) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a {kind:?} call was held within the timeout"));
}

// Seeds that never answer are weak evidence that no shard exists: a slow
// seed looks the same. A worker with no authority founds alone only after
// three silent rounds, a retry interval and then two apart, never before,
// and never waits for ever.
#[tokio::test(start_paused = true)]
async fn silent_seeds_with_no_authority_found_only_after_the_bound() {
    within_deadline(async {
        let mut worker = Worker::new(None, TokioClock::new(), &[nowhere(1)]);
        let started = tokio::time::Instant::now();

        let entry = timeout(TEST_TIMEOUT, worker.bootstrap())
            .await
            .expect("the worker founded once the bound passed");

        assert!(founded_at(&entry, 0));
        assert!(
            started.elapsed() >= RETRY_INTERVAL * 3,
            "the worker founded after only {:?}, before its seeds' bound",
            started.elapsed()
        );
    })
    .await
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_authority_never_leads_to_founding_until_it_answers() {
    within_deadline(async {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        authority.set_reachable(false);
        let mut worker = Worker::new(Some(&authority), clock, &[]);
        let mut running = std::pin::pin!(worker.bootstrap());

        let waiting = timeout(WAITING, &mut running).await;
        assert!(waiting.is_err(), "the worker entered while its authority was unreachable");
        assert_eq!(epoch_number(&authority), None);

        authority.set_reachable(true);
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the worker founded the shard once its authority answered");
        assert!(founded_at(&entry, 0));
    })
    .await
}

#[tokio::test(start_paused = true)]
async fn registered_peers_that_never_answer_keep_the_worker_from_founding() {
    within_deadline(async {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        register(&authority, "peer-a", &nowhere(1).to_string());
        register(&authority, "peer-b", &nowhere(2).to_string());
        let mut worker = Worker::new(Some(&authority), clock, &[]);

        let waiting = timeout(WAITING, worker.bootstrap()).await;

        assert!(waiting.is_err(), "the worker entered with peers listed and none answering");
        assert_eq!(epoch_number(&authority), None);
    })
    .await
}

#[tokio::test(start_paused = true)]
async fn losing_the_create_to_an_unseen_rival_does_not_re_found_the_shard() {
    within_deadline(async {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        authority.hold_next(CallKind::SwapRecoveryEpoch);
        let mut worker = Worker::new(Some(&authority), clock, &[]);
        let mut running = std::pin::pin!(worker.bootstrap());

        // The cascade registers and asks to create epoch 0: that call is held. A
        // rival registers and creates it meanwhile, then the create is let
        // through, and loses.
        let rival = authority.for_another_worker();
        tokio::select! {
            entry = &mut running => panic!("the cascade entered while its create was held: {entry:?}"),
            () = async {
                wait_until_held(&authority, CallKind::SwapRecoveryEpoch).await;
                let shard = ShardId::new("shard-1");
                rival.register(&shard, &WorkerId::new("rival"), "not a multiaddr").unwrap();
                rival
                    .compare_and_swap_recovery_epoch(&shard, None, RecoveryEpoch::founding(0, &mut Uuid7Lineages))
                    .unwrap();
                authority.release(CallKind::SwapRecoveryEpoch);
            } => {}
        }

        let waiting = timeout(WAITING, &mut running).await;

        assert!(waiting.is_err(), "the worker entered after losing the create");
        assert_eq!(epoch_number(&authority), Some(0), "no one re-founded the shard");
    })
    .await
}

// An authority whose read hangs holds one blocking thread, not one more
// every round. Were a duplicate read made, it would not be held and would
// find the shard ownerless, so the worker would found it while the first
// read is still held.
#[tokio::test(start_paused = true)]
async fn a_listing_read_that_hangs_is_not_asked_again_while_it_hangs() {
    within_deadline(async {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        authority.hold_next(CallKind::ReadLiveRegistrations);
        let mut worker = Worker::new(Some(&authority), clock, &[]);
        let mut running = std::pin::pin!(worker.bootstrap());

        let (still_held, founded_meanwhile) = tokio::select! {
            entry = &mut running => panic!("the worker entered while its first read was held: {entry:?}"),
            outcome = async {
                wait_until_held(&authority, CallKind::ReadLiveRegistrations).await;
                // Whole rounds run while the read is held. Time moves by hand: a
                // held call stops auto-advance.
                for _ in 0..20 {
                    tokio::time::advance(RETRY_INTERVAL).await;
                    for _ in 0..5 {
                        tokio::task::yield_now().await;
                    }
                }
                (authority.is_holding(CallKind::ReadLiveRegistrations), epoch_number(&authority))
            } => outcome,
        };
        // Released before anything can fail, so no path leaves the held thread
        // parked and hangs the runtime's shutdown.
        authority.release(CallKind::ReadLiveRegistrations);

        assert!(still_held, "the first read was answered during the rounds");
        assert_eq!(founded_meanwhile, None, "a second read found the shard ownerless and took it");
        // The held read, answered at last, is the cascade's: it takes ownership
        // of the ownerless shard.
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the worker founded the shard within the timeout");
        assert!(founded_at(&entry, 0));
    })
    .await
}

// A call the authority never answers is given up after the call timeout, so
// the node proceeds: a later read of the same kind is made, and answered. The
// first read stays held for the whole test, so the worker founding the shard
// shows the second one was asked and answered, not the held one.
#[tokio::test(start_paused = true)]
async fn a_listing_read_that_never_returns_is_given_up_and_asked_again() {
    within_deadline(async {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        authority.hold_next(CallKind::ReadLiveRegistrations);
        let call_timeout = Duration::from_secs(5);
        let mut worker = Worker::giving_up_calls_after(Some(&authority), clock, &[], call_timeout);
        let mut running = std::pin::pin!(worker.bootstrap());

        let entry = tokio::select! {
            entry = &mut running => entry,
            () = async {
                wait_until_held(&authority, CallKind::ReadLiveRegistrations).await;
                // Time moves by hand: a held call stops auto-advance.
                loop {
                    tokio::time::advance(RETRY_INTERVAL).await;
                    for _ in 0..5 {
                        tokio::task::yield_now().await;
                    }
                }
            } => unreachable!("the clock is advanced for ever"),
        };
        assert!(
            authority.is_holding(CallKind::ReadLiveRegistrations),
            "the hung read was answered, so the test proves nothing about giving it up"
        );
        assert!(founded_at(&entry, 0));
    })
    .await
}

// A full-shard restart: the authority is warm and lists no live
// registration, but its recovery epoch already exists, so every worker that
// ever held it is gone or has fenced itself off from leading it. A seedless
// bootstrapper re-founds the shard one epoch on, rather than waiting on
// workers that are never coming back.
#[tokio::test(start_paused = true)]
async fn an_ownerless_epoch_is_re_founded_one_epoch_on() {
    within_deadline(async {
        let (authority, clock) = warm_authority(Duration::from_secs(5)).await;
        authority
            .compare_and_swap_recovery_epoch(
                &ShardId::new("shard-1"),
                None,
                RecoveryEpoch::founding(3, &mut Uuid7Lineages),
            )
            .expect("creating the epoch directly succeeds against a warm, empty authority");
        let mut worker = Worker::new(Some(&authority), clock, &[]);

        let entry = timeout(TEST_TIMEOUT, worker.bootstrap())
            .await
            .expect("the bootstrapper re-founded the shard within the timeout");

        assert!(founded_at(&entry, 4), "the shard is re-founded one epoch past the one that existed: {entry:?}");
        assert_eq!(epoch_number(&authority), Some(4));
    })
    .await
}

// The authority reports an empty listing while it warms up, which proves
// nothing about who is registered, so the worker founds nothing until
// warm-up ends.
#[tokio::test(start_paused = true)]
async fn a_warming_up_authority_keeps_the_worker_bootstrapping_until_warm_up_ends() {
    within_deadline(async {
        let ttl = Duration::from_millis(300);
        let clock = TokioClock::new();
        let authority = FaultingAuthority::new(clock, TickDuration::from_ticks(ttl.as_millis() as u64));
        let started = tokio::time::Instant::now();
        let mut worker = Worker::new(Some(&authority), clock, &[]);

        let entry = timeout(TEST_TIMEOUT, worker.bootstrap())
            .await
            .expect("the worker founded the shard once warm-up ended");

        assert!(
            started.elapsed() >= ttl,
            "the worker founded the shard while the authority was warming up, after {:?}",
            started.elapsed()
        );
        assert!(founded_at(&entry, 0));
        assert_eq!(epoch_number(&authority), Some(0));
    })
    .await
}

// The founder's registration lapses a TTL after the cascade asked for it,
// however long the authority took to answer. Counted from when the answer
// arrived, or from when the node was built, a founder that cannot renew
// would still count itself registered, and able to lead, after another
// bootstrapper had found the shard with no one registered and re-founded it.
#[tokio::test(start_paused = true)]
async fn a_founder_counts_its_registration_from_when_the_cascade_asked_for_it() {
    within_deadline(async {
        let ttl = Duration::from_millis(300);
        let (authority, clock) = warm_authority(ttl).await;
        // The registration is slow: the authority holds it for half a TTL.
        authority.hold_next(CallKind::Register);
        let mut worker = Worker::new(Some(&authority), clock, &[]);
        let (me, shard) = (worker.me.clone(), worker.shard.clone());
        let mut running = std::pin::pin!(worker.bootstrap());

        let held_at = tokio::select! {
            entry = &mut running => panic!("the cascade entered while its registration was held: {entry:?}"),
            held_at = async {
                wait_until_held(&authority, CallKind::Register).await;
                // The cascade has asked to register, so this is no earlier than
                // the instant the registration is measured against.
                let held_at = tokio::time::Instant::now();
                // A held call stops auto-advance: time moves by hand.
                tokio::time::advance(ttl / 2).await;
                authority.release(CallKind::Register);
                held_at
            } => held_at,
        };
        let entry = timeout(TEST_TIMEOUT, running)
            .await
            .expect("the worker founded the shard within the timeout");
        let identity = Identity {
            id: me,
            incarnation: IncarnationId::new("incarnation-0"),
            shard,
            timings: ElectionTimings::new(TickDuration::from_millis(300), TickDuration::from_millis(10)),
        };
        let (mut node, _) = WorkerNode::start(
            identity,
            entry,
            clock,
            Some(AuthorityTimings {
                ttl: TickDuration::from_millis(ttl.as_millis() as u64),
            }),
        );

        // Past the registration's TTL less drift, but short of it counted from
        // when the node was built. The node's own renewal is never answered.
        tokio::time::sleep_until(held_at + ttl * 19 / 20).await;
        let _ = node.step(Input::Tick);

        assert_eq!(node.state(), WorkerState::Fenced);
    })
    .await
}

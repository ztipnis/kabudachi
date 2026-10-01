//! When `kabudachi_net::bootstrap::bootstrap` founds a new shard on its
//! own, and when it must stay in `Bootstrapping` instead. No test here gives
//! the worker a seed, so only the coordination authority, or its absence,
//! decides.
//!
//! A worker with no authority configured founds the shard alone, then waits
//! out its ordinary `suspect_timeout`, and its roll call's deadline, before
//! leading, like any other node (unlike `bindings`'s single-process runtime,
//! which leads almost at once: see `bindings/src/local_node.rs`). A worker
//! with an authority founds the shard only by winning ownership of it there,
//! creating the shard's epoch or, with no live worker left to ask,
//! re-founding it one epoch on. An authority that is unreachable, still
//! warming up, or listing another worker keeps it from founding the shard;
//! here, where no one answers it, that means it waits. The worker's own
//! registration is not another worker.

use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant as StdInstant};

use crate::support::election::due_now;
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, Uuid7Lineages};
use kabudachi_core::election::{
    AuthorityTimings, ElectionTimings, Identity, Input, Step, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::authority::AuthorityClient;
use kabudachi_net::bootstrap::bootstrap;
use kabudachi_net::driver::{DriverConfig, SharedAuthority, run_driver};
use kabudachi_net::messenger::Net;
use kabudachi_core::election::CallKind;
use kabudachi_testkit::FaultingAuthority;
use tokio::sync::watch;
use tokio::time::timeout;

const SHARD: &str = "shard-1";

/// A normal, nonzero suspect_timeout, so the lone-node test shows that
/// genesis waits it out like any other node.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often a follower heartbeats its leader: well inside every suspicion
/// timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// Short, so a waiting worker goes round the cascade many times per test.
const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// The authority's registration TTL, which is also how long it warms up.
const AUTHORITY_TTL_MS: u64 = 300;

/// Generous whole-test backstop for everything that is expected to finish.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn shard() -> ShardId {
    ShardId::new(SHARD)
}

fn authority_ttl() -> Duration {
    Duration::from_millis(AUTHORITY_TTL_MS)
}

/// The timings a node built in these tests keeps its own registration and
/// fence by, matching [`authority_ttl`].
fn authority_timings() -> AuthorityTimings {
    AuthorityTimings {
        ttl: authority_ttl(),
    }
}

fn fresh_net() -> Net {
    Net::new()
}

/// Bootstraps `net`'s worker into `SHARD` with no seeds, on `clock`, and
/// starts its node there: returns the node and the first step it asks its
/// driver to carry out.
async fn bootstrap_without_seeds(
    clock: RealClock,
    net: &Net,
    authority: Option<SharedAuthority>,
) -> (WorkerNode<RealClock>, Step) {
    let my_id = net.local_worker_id();
    let mut client = authority
        .as_ref()
        .map(|authority| AuthorityClient::new(net, shard(), Arc::clone(authority)));
    let entry = bootstrap(
        net,
        &clock,
        client.as_mut(),
        &shard(),
        &my_id,
        &[],
        StdDuration::from_secs(5),
        RETRY_INTERVAL,
    )
    .await;
    let identity = Identity {
        id: my_id.clone(),
        incarnation: IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        shard: shard(),
        timings: ElectionTimings::new(
            Duration::from_millis(SUSPECT_TIMEOUT_MS),
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    };
    let authority_timings = authority.map(|_| authority_timings());
    WorkerNode::start(identity, entry, clock, authority_timings)
}

/// `authority` as a worker's cascade and driver share it.
fn shared(authority: impl CoordinationAuthority + Send + Sync + 'static) -> SharedAuthority {
    Arc::new(authority)
}

/// A client of `authority` for `net`'s worker, as `Worker::run` makes one.
fn client(
    net: &Net,
    authority: impl CoordinationAuthority + Send + Sync + 'static,
) -> AuthorityClient {
    AuthorityClient::new(net, shard(), shared(authority))
}

/// An authority that has finished warming up, so it reports an
/// authoritative count of live registrations.
async fn warmed_up_authority() -> FaultingAuthority<RealClock> {
    let authority = FaultingAuthority::new(RealClock::new(), authority_ttl());
    while authority
        .live_registrations(&shard())
        .expect("the authority is reachable")
        .authoritative_count()
        .is_none()
    {
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    authority
}

/// Waits until `connection` is holding a `kind` call.
async fn wait_until_holding(connection: &FaultingAuthority<RealClock>, kind: CallKind) {
    let deadline = StdInstant::now() + TEST_TIMEOUT;
    while !connection.is_holding(kind) {
        assert!(StdInstant::now() < deadline, "no {kind:?} call was held");
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }
}

/// Registers `worker` at `address` through `connection` before returning,
/// then keeps the registration live by renewing it well within each TTL
/// until the returned task is aborted.
fn keep_registered(
    connection: FaultingAuthority<RealClock>,
    worker: WorkerId,
    address: String,
) -> tokio::task::JoinHandle<()> {
    let register = move || {
        connection
            .register(&shard(), &worker, &address)
            .expect("the authority is reachable");
    };
    register();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(StdDuration::from_millis(AUTHORITY_TTL_MS / 3)).await;
            register();
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_no_seeds_and_no_authority_self_elects_leader_after_the_normal_timeout() {
    let net = fresh_net();

    // Taken before `bootstrap` ends and the node is built, so the elapsed time
    // below is an upper bound on the node's own suspicion timer.
    let started = StdInstant::now();
    let clock = RealClock::new();
    let (mut node, first) = bootstrap_without_seeds(clock, &net, None).await;

    assert_eq!(node.state(), WorkerState::Active);
    assert!(
        !node.is_pending_member(),
        "a node with no seeds and no authority is the one voter of its own electorate"
    );

    let (tx, mut rx) = watch::channel(node.state());
    // This test only exercises the election, not claim arbitration, but
    // every driven node carries a Scheduler regardless (see run_driver's
    // doc). Nothing here sends it a claim request.
    let mut scheduler = Scheduler::new(clock, Uuid7Ids);

    // `run_driver` never returns (see its doc), so race it against watching
    // for Leader.
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node,
                first,
                &net,
                &mut scheduler,
                clock,
                None,
                DriverConfig::default(),
                |node, _, _| { let _ = tx.send(node.state()); },
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                loop {
                    if *rx.borrow() == WorkerState::Leader {
                        return;
                    }
                    rx.changed().await.expect("driver task is still running");
                }
            } => {}
        }
    })
    .await
    .expect("the lone node reached Leader on its own within the timeout");
    assert!(
        started.elapsed() >= StdDuration::from_millis(SUSPECT_TIMEOUT_MS),
        "the lone node must wait out suspect_timeout before self-electing, got {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_warming_up_authority_keeps_the_node_bootstrapping_until_warm_up_ends() {
    let net = fresh_net();
    // Taken before the authority exists, so its warm-up ends at least one
    // TTL after this.
    let started = StdInstant::now();
    let authority = FaultingAuthority::new(RealClock::new(), authority_ttl());

    let (node, _) = timeout(
        TEST_TIMEOUT,
        bootstrap_without_seeds(RealClock::new(), &net, Some(shared(authority.clone()))),
    )
    .await
    .expect("the node founded the shard once warm-up ended");

    assert!(
        started.elapsed() >= StdDuration::from_millis(AUTHORITY_TTL_MS),
        "the node must not found the shard while the authority is warming up, \
         but it returned after {:?}",
        started.elapsed()
    );
    assert_eq!(node.state(), WorkerState::Active);
    assert!(!node.is_pending_member());
    assert_eq!(
        authority
            .read_recovery_epoch(&shard())
            .map(|epoch| epoch.map(|epoch| epoch.number)),
        Ok(Some(0))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_whose_own_registration_is_the_only_one_founds_the_shard() {
    let net = fresh_net();
    let my_address = timeout(
        TEST_TIMEOUT,
        net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("the node produced a listen address within the timeout");
    let authority = warmed_up_authority().await;

    // The node's own registration stays live throughout, so it is always
    // listed; the node must not take itself for a peer to ask.
    let renewer = keep_registered(
        authority.clone(),
        net.local_worker_id(),
        my_address.to_string(),
    );

    let bootstrapped = timeout(
        TEST_TIMEOUT,
        bootstrap_without_seeds(RealClock::new(), &net, Some(shared(authority.clone()))),
    )
    .await;
    renewer.abort();

    let (node, _) = bootstrapped.expect("the node founded the shard within the timeout");
    assert_eq!(node.state(), WorkerState::Active);
    assert!(
        !node.is_pending_member(),
        "the founder is the one voter of its new shard"
    );
    assert_eq!(
        authority
            .read_recovery_epoch(&shard())
            .map(|epoch| epoch.map(|epoch| epoch.number)),
        Ok(Some(0)),
        "the node won ownership by creating the shard's epoch"
    );
}

// The founder's registration lapses a TTL after the cascade asked for it,
// however long the authority took to answer. Counting from when the answer
// arrived, or from when the node was built, a founder that cannot renew would
// still count itself registered, and able to lead, after another
// bootstrapper had found the shard with no one registered and re-founded it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_founder_counts_its_registration_from_when_the_cascade_asked_for_it() {
    let authority = warmed_up_authority().await;
    let connection = authority.for_another_worker();
    let net = fresh_net();
    // The registration is slow: the authority holds it for half a TTL.
    connection.hold_next(CallKind::Register);

    let (bootstrapped, held_at) = timeout(TEST_TIMEOUT, async {
        tokio::join!(
            bootstrap_without_seeds(RealClock::new(), &net, Some(shared(connection.clone()))),
            async {
                wait_until_holding(&connection, CallKind::Register).await;
                // The cascade has asked to register, so this is no earlier
                // than the instant the registration is measured against.
                let held_at = StdInstant::now();
                tokio::time::sleep(StdDuration::from_millis(AUTHORITY_TTL_MS / 2)).await;
                connection.release(CallKind::Register);
                held_at
            }
        )
    })
    .await
    .expect("the worker founded the shard within the timeout");
    let (mut node, _) = bootstrapped;

    // Past the registration's TTL less drift, but short of it counted from
    // when the node was built. The node's own renewal is never answered.
    let lapsed = held_at + StdDuration::from_millis(AUTHORITY_TTL_MS * 19 / 20);
    tokio::time::sleep(lapsed.saturating_duration_since(StdInstant::now())).await;
    let _ = node.step(Input::Tick);

    assert_eq!(node.state(), WorkerState::Fenced);
}

/// A full-shard restart: the authority is warm and lists no live
/// registration for the shard, but its recovery epoch already exists — every
/// worker that ever held it is gone or has fenced itself off from leading it
/// (a live worker renews well before its registration would lapse). A
/// seedless bootstrapper re-founds the shard one epoch on, rather than
/// waiting on workers that are never coming back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bootstrapper_re_founds_a_shard_whose_epoch_exists_with_no_live_registration() {
    let authority = warmed_up_authority().await;
    // Stands in for a shard whose founder's registration has lapsed, or
    // whose create-if-absent was applied with its reply lost: the epoch
    // exists, but nothing is live to ask.
    authority
        .compare_and_swap_recovery_epoch(&shard(), None, RecoveryEpoch::founding(3, &mut Uuid7Lineages))
        .expect("creating the epoch directly succeeds against a warm, empty authority");

    let net = fresh_net();
    let (node, _) = timeout(
        TEST_TIMEOUT,
        bootstrap_without_seeds(RealClock::new(), &net, Some(shared(authority.clone()))),
    )
    .await
    .expect("the bootstrapper re-founded the shard within the timeout");

    assert_eq!(node.state(), WorkerState::Active);
    assert!(
        !node.is_pending_member(),
        "the re-founder is the one voter of its new shard"
    );
    assert_eq!(
        node.recovery_epoch(),
        4,
        "the shard is re-founded one epoch past the one that existed with no one to ask"
    );
    assert_eq!(
        authority
            .read_recovery_epoch(&shard())
            .map(|epoch| epoch.map(|epoch| epoch.number)),
        Ok(Some(4)),
        "the authority's own epoch reflects the re-founding"
    );
}

/// A genesis node driven for real (`net::driver::run_driver`) keeps renewing
/// its registration. After a flush wipes it, a seedless second bootstrapper
/// (gated here on the founder's own renewal, so the race is deterministic)
/// finds the founder registered again and joins it instead of re-founding a
/// second shard beside it. (The founder separately republishes its epoch
/// once a fence attempt finds it missing, README §15.3, but on its own
/// fence-renewal cadence, which this test does not wait on.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seedless_bootstrapper_joins_the_shard_a_flush_left_running_instead_of_founding_a_second_one()
 {
    let net_founder = fresh_net();
    timeout(
        TEST_TIMEOUT,
        net_founder.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("the founder produced a listen address within the timeout");
    let founder_id = net_founder.local_worker_id();

    let authority = warmed_up_authority().await;
    let clock = RealClock::new();
    let (mut founder, first) =
        bootstrap_without_seeds(clock, &net_founder, Some(shared(authority.clone()))).await;
    assert_eq!(founder.state(), WorkerState::Active);
    let mut founder_scheduler = Scheduler::new(clock, Uuid7Ids);

    // Drive the founder for real: renewing its registration is its node's,
    // once `run_driver` drives it. It also needs to reach Leader so it can
    // answer the joiner's JOIN below with a real pointer to itself.
    let (tx, mut rx) = watch::channel(founder.state());
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut founder,
                first,
                &net_founder,
                &mut founder_scheduler,
                clock,
                Some(client(&net_founder, authority.clone())),
                DriverConfig::default(),
                |node, _, _| { let _ = tx.send(node.state()); },
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                loop {
                    if *rx.borrow() == WorkerState::Leader {
                        return;
                    }
                    rx.changed().await.expect("driver task is still running");
                }
            } => {}
        }
    })
    .await
    .expect("the founder self-elected Leader within the timeout");

    // Wipes every registration and the epoch, as a real authority flush
    // would (README §15.3): the running founder is now invisible to the
    // authority until it renews again, and warm-up restarts from here.
    authority.flush();

    // Wait for the founder's own renewal (every third of its TTL) to
    // make it visible again before the joiner's cascade ever starts: without
    // this gate, the race is between the founder's first post-flush renewal
    // and the joiner's warm-up ending, which a slow or contended host could
    // occasionally lose, taking the ownership path instead of this
    // regression's own registered-peer one. Gating on it deterministically
    // leaves only "does the founder keep renewing" to prove, which its every
    // TTL/3 cadence comfortably outlasts the joiner's one-TTL warm-up.
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut founder,
                due_now(&clock),
                &net_founder,
                &mut founder_scheduler,
                clock,
                Some(client(&net_founder, authority.clone())),
                DriverConfig::default(),
                |_, _, _| {},
            ) => {
                unreachable!("run_driver never returns")
            }
            () = async {
                while !authority
                    .live_registrations(&shard())
                    .expect("the authority is reachable")
                    .addresses()
                    .contains_key(&founder_id)
                {
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                }
            } => {}
        }
    })
    .await
    .expect("the founder renewed its registration after the flush within the timeout");

    // A second, seedless worker starts bootstrapping only now. Its cascade
    // waits out the fresh warm-up; the founder, kept driven in the same
    // select below, keeps renewing every third of the TTL and so stays
    // visible throughout. (The founder may also republish its own epoch
    // around now; this gate is only about registration, which is what
    // decides which path the joiner's cascade takes.)
    let net_joiner = fresh_net();
    let (node, _) = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut founder,
                due_now(&clock),
                &net_founder,
                &mut founder_scheduler,
                clock,
                Some(client(&net_founder, authority.clone())),
                DriverConfig::default(),
                |_, _, _| {},
            ) => {
                unreachable!("run_driver never returns")
            }
            joined = bootstrap_without_seeds(RealClock::new(), &net_joiner, Some(shared(authority.clone()))) => {
                joined
            }
        }
    })
    .await
    .expect("the joiner joined the still-running shard within the timeout");

    assert_eq!(node.state(), WorkerState::Active);
    assert!(
        node.is_pending_member(),
        "a node that joins an existing shard waits to be admitted before it votes"
    );
    assert_eq!(
        node.known_leader().map(|(leader, _)| leader),
        Some(founder_id),
        "the joiner found the founder through its renewed registration, not a fresh genesis"
    );
}

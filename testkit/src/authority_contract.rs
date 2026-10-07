//! The contract every `CoordinationAuthority` adapter keeps, as one suite
//! an adapter's own tests run: see [`check_authority_contract`].
//!
//! The suite lets time pass only through a [`PassTime`], so the same
//! clauses run on a simulated clock in an instant, or against a real
//! service with real waits. Every check sits at least a quarter of a TTL
//! away from the instant its outcome changes, so a real-time run passes
//! as long as each call completes well within a quarter TTL.

use std::collections::BTreeMap;

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, RecoveryEpoch,
};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::time::Duration;

/// How the suite lets time pass on the clock the adapter's authorities
/// run on.
pub trait PassTime {
    /// Returns once at least `duration` has passed on that clock.
    fn pass(&self, duration: Duration);
}

/// What the suite needs of one adapter: authorities to test, and the two
/// events the contract describes that no trait call causes, a loss of data
/// and an outage.
pub trait AuthorityAdapter {
    type Authority: CoordinationAuthority;

    /// The TTL every registration, fence and warm-up of the adapter's
    /// authorities lasts.
    fn ttl(&self) -> Duration;

    /// An authority that holds no data and starts its warm-up now.
    fn fresh(&self) -> Self::Authority;

    /// Makes `authority` lose all its data now, as a flush does.
    fn flush(&self, authority: &Self::Authority);

    /// Takes `authority` down. It keeps its data. The suite makes no call
    /// to it until [`Self::come_back`].
    fn go_down(&self, authority: &Self::Authority);

    /// Brings `authority` back from [`Self::go_down`] now.
    fn come_back(&self, authority: &Self::Authority);
}

/// Runs every clause of the contract against fresh authorities from
/// `adapter`, passing time with `time`. Panics, naming the clause, on the
/// first one the adapter breaks.
pub fn check_authority_contract(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    registrations_last_one_ttl_unless_renewed(adapter, time);
    shards_are_independent(adapter, time);
    the_count_is_withheld_for_one_ttl_after_start(adapter, time);
    a_missing_epoch_is_created_by_exactly_one_swap(adapter);
    a_swap_changes_the_epoch_only_from_the_expected_one(adapter);
    a_fence_needs_the_current_epoch(adapter, time);
    a_fence_held_by_another_is_waited_out_across_epochs(adapter, time);
    no_fence_for_one_ttl_after_start(adapter, time);
    a_flush_loses_everything_and_restarts_both_waits(adapter, time);
    an_outage_keeps_the_data_and_withholds_only_the_count(adapter, time);
}

fn shard() -> ShardId {
    ShardId::new("contract-shard")
}

fn worker_a() -> WorkerId {
    WorkerId::new("contract-worker-a")
}

fn worker_b() -> WorkerId {
    WorkerId::new("contract-worker-b")
}

/// The founding epoch of lineage 1, and the same number of another lineage.
const FOUNDED: RecoveryEpoch = RecoveryEpoch::new(0, 1);
const RIVAL: RecoveryEpoch = RecoveryEpoch::new(0, 2);

/// `quarters` quarters of the adapter's TTL.
fn quarters(adapter: &impl AuthorityAdapter, quarters: u64) -> Duration {
    Duration::from_ticks(adapter.ttl().as_ticks() / 4 * quarters)
}

/// A fresh authority past its warm-up: five quarters of a TTL have passed.
fn warmed_up<A: AuthorityAdapter>(adapter: &A, time: &impl PassTime) -> A::Authority {
    let authority = adapter.fresh();
    time.pass(quarters(adapter, 5));
    authority
}

fn live(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
    clause: &str,
) -> BTreeMap<WorkerId, String> {
    authority
        .live_registrations(shard_id)
        .unwrap_or_else(|error| panic!("{clause}: live_registrations failed: {error}"))
        .addresses()
        .clone()
}

fn count(authority: &impl CoordinationAuthority, clause: &str) -> Option<usize> {
    authority
        .live_registrations(&shard())
        .unwrap_or_else(|error| panic!("{clause}: live_registrations failed: {error}"))
        .authoritative_count()
}

fn register(
    authority: &impl CoordinationAuthority,
    adapter: &impl AuthorityAdapter,
    worker: &WorkerId,
    address: &str,
    clause: &str,
) {
    assert_eq!(
        authority.register(&shard(), worker, address),
        Ok(adapter.ttl()),
        "{clause}: register returns the registration TTL"
    );
}

fn create(authority: &impl CoordinationAuthority, epoch: RecoveryEpoch, clause: &str) {
    assert_eq!(
        authority.compare_and_swap_recovery_epoch(&shard(), None, epoch),
        Ok(()),
        "{clause}: create-if-absent on a shard with no epoch succeeds"
    );
}

/// The time left on a `FenceHeld` answer; panics, naming `clause`, on any
/// other answer, or if the time left is zero or more than `at_most`.
fn fence_held(
    result: Result<Duration, AuthorityError>,
    at_most: Duration,
    clause: &str,
) -> Duration {
    match result {
        Err(AuthorityError::FenceHeld { remaining }) => {
            assert!(
                remaining.as_ticks() > 0 && remaining <= at_most,
                "{clause}: FenceHeld must leave between 0 and {} ms, left {} ms",
                at_most.as_ticks(),
                remaining.as_ticks()
            );
            remaining
        }
        other => panic!("{clause}: expected FenceHeld, got {other:?}"),
    }
}

fn registrations_last_one_ttl_unless_renewed(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "a registration lasts one TTL from its last register";
    let authority = adapter.fresh();
    register(&authority, adapter, &worker_a(), "address-a-1", clause);
    register(&authority, adapter, &worker_b(), "address-b", clause);
    time.pass(quarters(adapter, 2));
    register(&authority, adapter, &worker_a(), "address-a-2", clause);
    time.pass(quarters(adapter, 3));
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::from([(worker_a(), "address-a-2".to_string())]),
        "{clause}: b lapsed a TTL after registering; a, renewed, is listed at its new address"
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::new(),
        "{clause}: a lapsed a TTL after its renewal"
    );
}

fn shards_are_independent(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "shards are independent";
    let authority = warmed_up(adapter, time);
    let other = ShardId::new("contract-other-shard");
    register(&authority, adapter, &worker_a(), "address-a", clause);
    create(&authority, FOUNDED, clause);
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );
    assert_eq!(
        live(&authority, &other, clause),
        BTreeMap::new(),
        "{clause}: registrations"
    );
    assert_eq!(
        authority.read_recovery_epoch(&other),
        Ok(None),
        "{clause}: epoch"
    );
    assert_eq!(
        authority.compare_and_swap_recovery_epoch(&other, None, RIVAL),
        Ok(()),
        "{clause}: the other shard is created on its own"
    );
    assert_eq!(
        authority.acquire_fence(&other, &worker_b(), RIVAL),
        Ok(adapter.ttl()),
        "{clause}: a fence on one shard does not hold another"
    );
}

fn the_count_is_withheld_for_one_ttl_after_start(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "no authoritative count for one TTL after start";
    let authority = adapter.fresh();
    register(&authority, adapter, &worker_a(), "address-a", clause);
    assert_eq!(count(&authority, clause), None, "{clause}: warming up");
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::from([(worker_a(), "address-a".to_string())]),
        "{clause}: addresses are reported during warm-up"
    );
    time.pass(quarters(adapter, 3));
    register(&authority, adapter, &worker_a(), "address-a", clause);
    time.pass(quarters(adapter, 2));
    assert_eq!(count(&authority, clause), Some(1), "{clause}: warmed up");
}

fn a_missing_epoch_is_created_by_exactly_one_swap(adapter: &impl AuthorityAdapter) {
    let clause = "create-if-absent succeeds exactly once";
    let authority = adapter.fresh();
    assert_eq!(
        authority.read_recovery_epoch(&shard()),
        Ok(None),
        "{clause}: never created"
    );
    create(&authority, FOUNDED, clause);
    assert_eq!(
        authority.compare_and_swap_recovery_epoch(&shard(), None, RIVAL),
        Err(AuthorityError::EpochConflict {
            current: Some(FOUNDED)
        }),
        "{clause}: the second founder loses the race and learns the winner"
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard()),
        Ok(Some(FOUNDED)),
        "{clause}: a lost race changes nothing"
    );
}

fn a_swap_changes_the_epoch_only_from_the_expected_one(adapter: &impl AuthorityAdapter) {
    let clause = "a swap needs the exact current epoch";
    let authority = adapter.fresh();
    create(&authority, FOUNDED, clause);
    let next = FOUNDED.next().expect("0 has a successor");
    for wrong in [RIVAL, next] {
        assert_eq!(
            authority.compare_and_swap_recovery_epoch(
                &shard(),
                Some(wrong),
                RecoveryEpoch::new(5, 2)
            ),
            Err(AuthorityError::EpochConflict {
                current: Some(FOUNDED)
            }),
            "{clause}: expected {wrong} is not the current {FOUNDED}"
        );
    }
    assert_eq!(
        authority.compare_and_swap_recovery_epoch(&shard(), Some(FOUNDED), next),
        Ok(()),
        "{clause}: from the current epoch"
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard()),
        Ok(Some(next)),
        "{clause}: swapped"
    );
}

fn a_fence_needs_the_current_epoch(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "a fence needs the current epoch";
    let authority = warmed_up(adapter, time);
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Err(AuthorityError::EpochConflict { current: None }),
        "{clause}: the shard has no epoch"
    );
    create(&authority, FOUNDED, clause);
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), RIVAL),
        Err(AuthorityError::EpochConflict {
            current: Some(FOUNDED)
        }),
        "{clause}: same number, other lineage"
    );
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: acquired, returning the fence TTL"
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: its holder renews it"
    );
}

fn a_fence_held_by_another_is_waited_out_across_epochs(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "another holder's fence is waited out whatever its epoch";
    let authority = warmed_up(adapter, time);
    create(&authority, FOUNDED, clause);
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );
    time.pass(quarters(adapter, 1));
    fence_held(
        authority.acquire_fence(&shard(), &worker_b(), FOUNDED),
        quarters(adapter, 3),
        clause,
    );
    let next = FOUNDED.next().expect("0 has a successor");
    assert_eq!(
        authority.compare_and_swap_recovery_epoch(&shard(), Some(FOUNDED), next),
        Ok(()),
        "{clause}: setup: a forced recovery moves the epoch on"
    );
    fence_held(
        authority.acquire_fence(&shard(), &worker_b(), next),
        quarters(adapter, 3),
        clause,
    );
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Err(AuthorityError::EpochConflict {
            current: Some(next)
        }),
        "{clause}: the old holder cannot renew at the old epoch"
    );
    time.pass(quarters(adapter, 4));
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_b(), next),
        Ok(adapter.ttl()),
        "{clause}: once the old fence has expired"
    );
}

fn no_fence_for_one_ttl_after_start(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "no fence for one TTL after start";
    let authority = adapter.fresh();
    create(&authority, FOUNDED, clause);
    time.pass(quarters(adapter, 1));
    fence_held(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        quarters(adapter, 3),
        clause,
    );
    time.pass(quarters(adapter, 4));
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: a TTL after start"
    );
}

fn a_flush_loses_everything_and_restarts_both_waits(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "a flush loses all data and restarts both waits";
    let authority = warmed_up(adapter, time);
    create(&authority, FOUNDED, clause);
    register(&authority, adapter, &worker_a(), "address-a", clause);
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );

    adapter.flush(&authority);
    assert_eq!(
        authority.read_recovery_epoch(&shard()),
        Ok(None),
        "{clause}: epoch lost"
    );
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::new(),
        "{clause}: registrations lost"
    );
    create(&authority, RIVAL, clause);
    time.pass(quarters(adapter, 1));
    fence_held(
        authority.acquire_fence(&shard(), &worker_b(), RIVAL),
        quarters(adapter, 3),
        clause,
    );
    register(&authority, adapter, &worker_b(), "address-b", clause);
    assert_eq!(
        count(&authority, clause),
        None,
        "{clause}: count withheld after the flush"
    );

    time.pass(quarters(adapter, 4));
    register(&authority, adapter, &worker_b(), "address-b", clause);
    assert_eq!(
        count(&authority, clause),
        Some(1),
        "{clause}: count back a TTL after the flush"
    );
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_b(), RIVAL),
        Ok(adapter.ttl()),
        "{clause}: fences granted a TTL after the flush"
    );
}

fn an_outage_keeps_the_data_and_withholds_only_the_count(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "an outage keeps the data and withholds only the count";
    let authority = warmed_up(adapter, time);
    create(&authority, FOUNDED, clause);
    register(&authority, adapter, &worker_a(), "address-a", clause);
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );

    adapter.go_down(&authority);
    time.pass(quarters(adapter, 1));
    adapter.come_back(&authority);

    assert_eq!(
        authority.read_recovery_epoch(&shard()),
        Ok(Some(FOUNDED)),
        "{clause}: the epoch is kept"
    );
    assert_eq!(
        authority.acquire_fence(&shard(), &worker_a(), FOUNDED),
        Ok(adapter.ttl()),
        "{clause}: the fence is kept, so its holder renews with no wait"
    );
    fence_held(
        authority.acquire_fence(&shard(), &worker_b(), FOUNDED),
        adapter.ttl(),
        clause,
    );
    register(&authority, adapter, &worker_a(), "address-a", clause);
    assert_eq!(
        count(&authority, clause),
        None,
        "{clause}: count withheld after the outage"
    );

    time.pass(quarters(adapter, 3));
    register(&authority, adapter, &worker_a(), "address-a", clause);
    time.pass(quarters(adapter, 2));
    assert_eq!(
        count(&authority, clause),
        Some(1),
        "{clause}: count back a TTL after the outage"
    );
}

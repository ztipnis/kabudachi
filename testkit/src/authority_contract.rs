//! The contract every `CoordinationAuthority` adapter keeps, as one suite
//! an adapter's own tests run: see [`check_authority_contract`].
//!
//! The suite lets time pass only through a [`PassTime`], so the same
//! clauses run on a simulated clock in an instant, or against a real
//! service with real waits. Every check sits at least a quarter of a TTL
//! away from the instant its outcome changes, so a real-time run passes
//! as long as the calls between a timed check and the event it counts
//! from, `go_down` and `come_back` included, together take well within a
//! quarter TTL.

use std::collections::BTreeMap;

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, LeaderHint, RecoveryEpoch, ShardRecord,
};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
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

    /// Whether the adapter can take an authority down and bring it back
    /// within a fraction of its TTL. When `false`, the suite skips only the
    /// outage clause; another run of the contract must cover outages.
    fn has_outages(&self) -> bool {
        true
    }
}

/// Runs every clause of the contract against fresh authorities from
/// `adapter`, passing time with `time`. Panics, naming the clause, on the
/// first one the adapter breaks.
pub fn check_authority_contract(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    registrations_last_one_ttl_unless_renewed(adapter, time);
    shard_names_are_independent(adapter, time);
    the_count_is_withheld_for_one_ttl_after_start(adapter, time);
    registrations_count_only_their_own_shard_id(adapter, time);
    a_leader_hint_lasts_one_ttl_and_the_last_write_wins(adapter, time);
    the_authority_reports_the_ttl_it_grants(adapter, time);
    a_missing_record_is_created_by_exactly_one_swap(adapter);
    a_swap_changes_the_record_only_from_the_expected_one(adapter);
    a_fence_needs_the_current_record(adapter, time);
    a_fence_held_by_another_is_waited_out_across_records(adapter, time);
    no_fence_for_one_ttl_after_start(adapter, time);
    a_flush_loses_everything_and_restarts_both_waits(adapter, time);
    if adapter.has_outages() {
        an_outage_keeps_the_data_and_may_withhold_the_count_and_the_fence(adapter, time);
    }
}

fn name() -> ShardName {
    ShardName::new("contract-shard")
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

/// The founding record of lineage 1, and the same number of another lineage
/// under the same shard id.
fn founded() -> ShardRecord {
    ShardRecord {
        shard_id: shard(),
        recovery_epoch: RecoveryEpoch::new(0, 1),
    }
}

fn rival() -> ShardRecord {
    ShardRecord {
        shard_id: shard(),
        recovery_epoch: RecoveryEpoch::new(0, 2),
    }
}

/// `founded()` after a forced recovery: the next number, the same lineage.
fn founded_next() -> ShardRecord {
    ShardRecord {
        shard_id: shard(),
        recovery_epoch: founded().recovery_epoch.next().expect("0 has a successor"),
    }
}

/// `founded()`'s epoch under another shard id.
fn successor() -> ShardRecord {
    ShardRecord {
        shard_id: ShardId::new("contract-shard/successor"),
        recovery_epoch: founded().recovery_epoch,
    }
}

fn hint(leader: WorkerId, term: u64) -> LeaderHint {
    LeaderHint {
        shard_id: shard(),
        address: format!("{}-address", leader.as_str()),
        leader,
        recovery_epoch: founded().recovery_epoch,
        term,
    }
}

/// `quarters` quarters of the adapter's TTL.
fn quarters(adapter: &impl AuthorityAdapter, quarters: u64) -> Duration {
    let ticks = adapter
        .ttl()
        .as_ticks()
        .checked_mul(quarters)
        .expect("a TTL times a few quarters fits in ticks");
    Duration::from_ticks(ticks / 4)
}

/// What remains of the adapter's TTL once `quarters` quarters of it have
/// passed.
fn left_after(adapter: &impl AuthorityAdapter, quarters: u64) -> Duration {
    let passed = self::quarters(adapter, quarters).as_ticks();
    Duration::from_ticks(adapter.ttl().as_ticks() - passed)
}

/// A fresh authority past its warm-up: five quarters of a TTL have passed.
fn warmed_up<A: AuthorityAdapter>(adapter: &A, time: &impl PassTime) -> A::Authority {
    let authority = adapter.fresh();
    // The name is first touched right after `fresh`, so an adapter that
    // starts a name's warm-up at its first use is warmed up for it below.
    assert_eq!(
        authority.read_shard(&name()),
        Ok(None),
        "a fresh authority holds no record"
    );
    time.pass(quarters(adapter, 5));
    authority
}

fn live(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
    clause: &str,
) -> BTreeMap<WorkerId, String> {
    authority
        .live_registrations(&name(), shard_id)
        .unwrap_or_else(|error| panic!("{clause}: live_registrations failed: {error}"))
        .addresses()
        .clone()
}

fn count(
    authority: &impl CoordinationAuthority,
    shard_id: &ShardId,
    clause: &str,
) -> Option<usize> {
    authority
        .live_registrations(&name(), shard_id)
        .unwrap_or_else(|error| panic!("{clause}: live_registrations failed: {error}"))
        .authoritative_count()
}

fn register(
    authority: &impl CoordinationAuthority,
    adapter: &impl AuthorityAdapter,
    shard_id: &ShardId,
    worker: &WorkerId,
    address: &str,
    clause: &str,
) {
    assert_eq!(
        authority.register(&name(), shard_id, worker, address),
        Ok(adapter.ttl()),
        "{clause}: register returns the registration TTL"
    );
}

fn publish(authority: &impl CoordinationAuthority, hint: &LeaderHint, clause: &str) {
    assert_eq!(
        authority.publish_leader_hint(&name(), hint),
        Ok(()),
        "{clause}: publish_leader_hint"
    );
}

fn read_hint(authority: &impl CoordinationAuthority, clause: &str) -> Option<LeaderHint> {
    authority
        .read_leader_hint(&name())
        .unwrap_or_else(|error| panic!("{clause}: read_leader_hint failed: {error}"))
}

fn create(authority: &impl CoordinationAuthority, record: &ShardRecord, clause: &str) {
    assert_eq!(
        authority.compare_and_swap_shard(&name(), None, record),
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
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a-1",
        clause,
    );
    register(
        &authority,
        adapter,
        &shard(),
        &worker_b(),
        "address-b",
        clause,
    );
    time.pass(quarters(adapter, 2));
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a-2",
        clause,
    );
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

fn shard_names_are_independent(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "shard names are independent";
    let authority = adapter.fresh();
    let other = ShardName::new("contract-other-shard");
    // Both names are first touched right after `fresh`, so an adapter that
    // starts a name's warm-up at its first use is warmed up for both below.
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    assert_eq!(
        authority
            .live_registrations(&other, &shard())
            .map(|live| live.addresses().clone()),
        Ok(BTreeMap::new()),
        "{clause}: registrations"
    );
    time.pass(quarters(adapter, 5));
    create(&authority, &founded(), clause);
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );
    assert_eq!(authority.read_shard(&other), Ok(None), "{clause}: record");
    publish(&authority, &hint(worker_a(), 1), clause);
    assert_eq!(
        authority.read_leader_hint(&other),
        Ok(None),
        "{clause}: hint"
    );
    assert_eq!(
        authority.compare_and_swap_shard(&other, None, &rival()),
        Ok(()),
        "{clause}: the other shard is created on its own"
    );
    assert_eq!(
        authority.acquire_fence(&other, &worker_b(), &rival()),
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
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    assert_eq!(
        count(&authority, &shard(), clause),
        None,
        "{clause}: warming up"
    );
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::from([(worker_a(), "address-a".to_string())]),
        "{clause}: addresses are reported during warm-up"
    );
    time.pass(quarters(adapter, 3));
    assert_eq!(
        count(&authority, &shard(), clause),
        None,
        "{clause}: still warming up a quarter TTL before the end"
    );
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        count(&authority, &shard(), clause),
        Some(1),
        "{clause}: warmed up"
    );
}

fn registrations_count_only_their_own_shard_id(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "registrations count only their own shard id";
    let authority = warmed_up(adapter, time);
    let successor = successor().shard_id;
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    register(
        &authority,
        adapter,
        &successor,
        &worker_b(),
        "address-b",
        clause,
    );
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::from([(worker_a(), "address-a".to_string())]),
        "{clause}: b registered with another shard id"
    );
    register(
        &authority,
        adapter,
        &successor,
        &worker_a(),
        "address-a",
        clause,
    );
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::new(),
        "{clause}: a re-registered with the other id is no longer listed under the first"
    );
    assert_eq!(
        live(&authority, &successor, clause).len(),
        2,
        "{clause}: both are listed under the other id"
    );
}

fn a_leader_hint_lasts_one_ttl_and_the_last_write_wins(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "a leader hint lasts one TTL from its write and the last write wins";
    let authority = adapter.fresh();
    assert_eq!(
        read_hint(&authority, clause),
        None,
        "{clause}: never published"
    );
    publish(&authority, &hint(worker_a(), 1), clause);
    assert_eq!(
        read_hint(&authority, clause),
        Some(hint(worker_a(), 1)),
        "{clause}: published"
    );
    time.pass(quarters(adapter, 2));
    publish(&authority, &hint(worker_b(), 2), clause);
    time.pass(quarters(adapter, 3));
    assert_eq!(
        read_hint(&authority, clause),
        Some(hint(worker_b(), 2)),
        "{clause}: the second write replaced the first and lasts a TTL from its own write"
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        read_hint(&authority, clause),
        None,
        "{clause}: lapsed a TTL after the second write"
    );
}

fn the_authority_reports_the_ttl_it_grants(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "ttl() is the TTL register and acquire_fence grant";
    let authority = warmed_up(adapter, time);
    assert_eq!(
        authority.ttl(),
        adapter.ttl(),
        "{clause}: the adapter's TTL"
    );
    create(&authority, &founded(), clause);
    assert_eq!(
        authority.register(&name(), &shard(), &worker_a(), "address-a"),
        Ok(authority.ttl()),
        "{clause}: register"
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(authority.ttl()),
        "{clause}: acquire_fence"
    );
}

fn a_missing_record_is_created_by_exactly_one_swap(adapter: &impl AuthorityAdapter) {
    let clause = "create-if-absent succeeds exactly once";
    let authority = adapter.fresh();
    assert_eq!(
        authority.read_shard(&name()),
        Ok(None),
        "{clause}: never created"
    );
    create(&authority, &founded(), clause);
    assert_eq!(
        authority.compare_and_swap_shard(&name(), None, &successor()),
        Err(AuthorityError::ShardConflict {
            current: Some(founded())
        }),
        "{clause}: the second founder loses the race and learns the winner"
    );
    assert_eq!(
        authority.read_shard(&name()),
        Ok(Some(founded())),
        "{clause}: a lost race changes nothing"
    );
}

fn a_swap_changes_the_record_only_from_the_expected_one(adapter: &impl AuthorityAdapter) {
    let clause = "a swap needs the exact current record";
    let authority = adapter.fresh();
    create(&authority, &founded(), clause);
    let next = founded_next();
    for wrong in [rival(), next.clone(), successor()] {
        assert_eq!(
            authority.compare_and_swap_shard(
                &name(),
                Some(&wrong),
                &ShardRecord {
                    shard_id: shard(),
                    recovery_epoch: RecoveryEpoch::new(5, 2)
                }
            ),
            Err(AuthorityError::ShardConflict {
                current: Some(founded())
            }),
            "{clause}: expected {} is not the current {}",
            wrong.recovery_epoch,
            founded().recovery_epoch
        );
    }
    assert_eq!(
        authority.compare_and_swap_shard(&name(), Some(&founded()), &next),
        Ok(()),
        "{clause}: from the current epoch"
    );
    assert_eq!(
        authority.read_shard(&name()),
        Ok(Some(next)),
        "{clause}: swapped"
    );
}

fn a_fence_needs_the_current_record(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "a fence needs the current record";
    let authority = warmed_up(adapter, time);
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Err(AuthorityError::ShardConflict { current: None }),
        "{clause}: the shard has no epoch"
    );
    create(&authority, &founded(), clause);
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &rival()),
        Err(AuthorityError::ShardConflict {
            current: Some(founded())
        }),
        "{clause}: same number, other lineage"
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &successor()),
        Err(AuthorityError::ShardConflict {
            current: Some(founded())
        }),
        "{clause}: same epoch, other shard id"
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: acquired, returning the fence TTL"
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: its holder renews it"
    );
    time.pass(quarters(adapter, 3));
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &founded()),
        left_after(adapter, 3),
        clause,
    );
}

fn a_fence_held_by_another_is_waited_out_across_records(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "another holder's fence is waited out whatever its record";
    let authority = warmed_up(adapter, time);
    create(&authority, &founded(), clause);
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );
    time.pass(quarters(adapter, 1));
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &founded()),
        left_after(adapter, 1),
        clause,
    );
    let next = founded_next();
    assert_eq!(
        authority.compare_and_swap_shard(&name(), Some(&founded()), &next),
        Ok(()),
        "{clause}: setup: a forced recovery moves the epoch on"
    );
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &next),
        left_after(adapter, 1),
        clause,
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Err(AuthorityError::ShardConflict {
            current: Some(next.clone())
        }),
        "{clause}: the old holder cannot renew at the old epoch"
    );
    time.pass(quarters(adapter, 2));
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &next),
        left_after(adapter, 3),
        clause,
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        authority.acquire_fence(&name(), &worker_b(), &next),
        Ok(adapter.ttl()),
        "{clause}: once the old fence has expired"
    );
}

fn no_fence_for_one_ttl_after_start(adapter: &impl AuthorityAdapter, time: &impl PassTime) {
    let clause = "no fence for one TTL after start";
    let authority = adapter.fresh();
    create(&authority, &founded(), clause);
    time.pass(quarters(adapter, 1));
    fence_held(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        left_after(adapter, 1),
        clause,
    );
    time.pass(quarters(adapter, 2));
    fence_held(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        left_after(adapter, 3),
        clause,
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
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
    create(&authority, &founded(), clause);
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );

    publish(&authority, &hint(worker_a(), 1), clause);
    adapter.flush(&authority);
    assert_eq!(
        authority.read_shard(&name()),
        Ok(None),
        "{clause}: record lost"
    );
    assert_eq!(
        authority.read_leader_hint(&name()),
        Ok(None),
        "{clause}: hint lost"
    );
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::new(),
        "{clause}: registrations lost"
    );
    create(&authority, &rival(), clause);
    time.pass(quarters(adapter, 1));
    fence_held(
        authority.acquire_fence(&name(), &worker_a(), &rival()),
        left_after(adapter, 1),
        clause,
    );
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &rival()),
        left_after(adapter, 1),
        clause,
    );
    register(
        &authority,
        adapter,
        &shard(),
        &worker_b(),
        "address-b",
        clause,
    );
    assert_eq!(
        count(&authority, &shard(), clause),
        None,
        "{clause}: count withheld after the flush"
    );

    time.pass(quarters(adapter, 2));
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &rival()),
        left_after(adapter, 3),
        clause,
    );
    assert_eq!(
        count(&authority, &shard(), clause),
        None,
        "{clause}: count still withheld a quarter TTL before the end"
    );

    time.pass(quarters(adapter, 2));
    register(
        &authority,
        adapter,
        &shard(),
        &worker_b(),
        "address-b",
        clause,
    );
    assert_eq!(
        count(&authority, &shard(), clause),
        Some(1),
        "{clause}: count back a TTL after the flush"
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_b(), &rival()),
        Ok(adapter.ttl()),
        "{clause}: fences granted a TTL after the flush"
    );
}

fn an_outage_keeps_the_data_and_may_withhold_the_count_and_the_fence(
    adapter: &impl AuthorityAdapter,
    time: &impl PassTime,
) {
    let clause = "an outage keeps the data and may withhold the count and the fence for a TTL";
    let authority = warmed_up(adapter, time);
    create(&authority, &founded(), clause);
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: setup: a takes the fence"
    );

    adapter.go_down(&authority);
    time.pass(quarters(adapter, 1));
    adapter.come_back(&authority);

    assert_eq!(
        authority.read_shard(&name()),
        Ok(Some(founded())),
        "{clause}: the epoch is kept"
    );
    assert_eq!(
        live(&authority, &shard(), clause),
        BTreeMap::from([(worker_a(), "address-a".to_string())]),
        "{clause}: the registration is kept"
    );
    // A restart or failover may lose acknowledged writes, so an adapter may
    // refuse every fence for up to one TTL after it comes back; it may not
    // hand a second holder the fence while the first one's lasts.
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &founded()),
        adapter.ttl(),
        clause,
    );
    match authority.acquire_fence(&name(), &worker_a(), &founded()) {
        Ok(ttl) => assert_eq!(ttl, adapter.ttl(), "{clause}: the holder renews for a TTL"),
        held => {
            fence_held(held, adapter.ttl(), clause);
        }
    }
    fence_held(
        authority.acquire_fence(&name(), &worker_b(), &founded()),
        adapter.ttl(),
        clause,
    );
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    assert_eq!(
        count(&authority, &shard(), clause),
        None,
        "{clause}: count withheld after the outage"
    );

    time.pass(quarters(adapter, 3));
    assert_eq!(
        count(&authority, &shard(), clause),
        None,
        "{clause}: count still withheld a quarter TTL before the end"
    );
    register(
        &authority,
        adapter,
        &shard(),
        &worker_a(),
        "address-a",
        clause,
    );
    time.pass(quarters(adapter, 2));
    assert_eq!(
        count(&authority, &shard(), clause),
        Some(1),
        "{clause}: count back a TTL after the outage"
    );
    assert_eq!(
        authority.acquire_fence(&name(), &worker_a(), &founded()),
        Ok(adapter.ttl()),
        "{clause}: the fence is granted again within a TTL and a quarter of the outage's end"
    );
}

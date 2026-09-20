mod support;

use support::builders::{shard, worker};

use std::collections::BTreeSet;

use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::protocol::ids::WorkerId;
use support::coordination_authority::FakeCoordinationAuthority;

fn set(ids: &[&str]) -> BTreeSet<WorkerId> {
    ids.iter().map(|id| worker(id)).collect()
}

#[test]
fn force_reconfigure_succeeds_when_expected_epoch_matches() {
    let authority = FakeCoordinationAuthority::new();
    let shard_id = shard("shard-1");

    let result = authority.force_reconfigure(&shard_id, 0, set(&["a", "b"]));
    assert_eq!(result, Ok(1));

    assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(1));
    assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a", "b"])));
}

#[test]
fn force_reconfigure_fails_with_cas_conflict_and_no_state_change() {
    let authority = FakeCoordinationAuthority::new();
    let shard_id = shard("shard-1");

    // Set up: epoch now 1, membership {a, b}.
    assert_eq!(
        authority.force_reconfigure(&shard_id, 0, set(&["a", "b"])),
        Ok(1)
    );

    // Stale expected epoch (0, but current is 1).
    let result = authority.force_reconfigure(&shard_id, 0, set(&["c"]));
    assert_eq!(result, Err(AuthorityError::CasConflict { current: 1 }));

    // No state change from the failed CAS.
    assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(1));
    assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a", "b"])));
}

#[test]
fn discover_workers_respects_partition_from() {
    let authority = FakeCoordinationAuthority::new();
    let shard_id = shard("shard-1");

    assert_eq!(
        authority.force_reconfigure(&shard_id, 0, set(&["a", "b", "c"])),
        Ok(1)
    );

    authority.partition_from(set(&["b"]));
    assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a", "c"])));

    authority.partition_from(BTreeSet::new());
    assert_eq!(
        authority.discover_workers(&shard_id),
        Ok(set(&["a", "b", "c"]))
    );
}

#[test]
fn set_available_false_makes_every_method_return_unavailable() {
    let authority = FakeCoordinationAuthority::new();
    let shard_id = shard("shard-1");

    assert_eq!(
        authority.force_reconfigure(&shard_id, 0, set(&["a", "b"])),
        Ok(1)
    );

    authority.set_available(false);

    assert_eq!(
        authority.discover_workers(&shard_id),
        Err(AuthorityError::Unavailable)
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard_id),
        Err(AuthorityError::Unavailable)
    );
    assert_eq!(
        authority.force_reconfigure(&shard_id, 1, set(&["a", "b", "c"])),
        Err(AuthorityError::Unavailable)
    );

    authority.set_available(true);

    // State was preserved, not flushed, while unavailable.
    assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(1));
    assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["a", "b"])));
}

#[test]
fn flush_all_clears_state_back_to_never_seen() {
    let authority = FakeCoordinationAuthority::new();
    let shard_id = shard("shard-1");

    assert_eq!(
        authority.force_reconfigure(&shard_id, 0, set(&["a", "b"])),
        Ok(1)
    );

    authority.flush_all();

    assert_eq!(authority.read_recovery_epoch(&shard_id), Ok(0));
    // Membership for a never-seen shard is an empty set (documented choice —
    // see coordination_authority.rs support module docs).
    assert_eq!(authority.discover_workers(&shard_id), Ok(BTreeSet::new()));

    // The shard is genuinely back to "never seen": the first force_reconfigure
    // after a flush must succeed with expected_recovery_epoch: 0 again.
    assert_eq!(
        authority.force_reconfigure(&shard_id, 0, set(&["x"])),
        Ok(1)
    );
    assert_eq!(authority.discover_workers(&shard_id), Ok(set(&["x"])));
}

//! The floor a rejoining node holds, as the one order JOIN pointers are
//! taken by: which pointers it accepts, and which of the accepted is newest.
//! `net` holds a copy of the floor while it searches and asks it, never
//! comparing epochs itself.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::JoinFloor;
use kabudachi_core::protocol::messages::JoinResponse;

use crate::support::builders::worker;

const A: u64 = 1;
const B: u64 = 2;

fn pointer(number: u64, lineage: u64, term: u64) -> JoinResponse {
    JoinResponse {
        leader_id: Some(worker(&format!("leader-{number}-{lineage}-{term}")).into()),
        leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
        term,
        recovery_epoch: number,
        recovery_epoch_lineage: lineage,
    }
}

fn floor_at(number: u64, lineage: u64) -> JoinFloor {
    JoinFloor::at(RecoveryEpoch::new(number, lineage))
}

// Of two lineages at one number the higher is the newer epoch whichever
// floor asks, so two rejoining nodes take the same leader.
#[test]
fn every_floor_takes_the_higher_of_two_lineages_at_one_number_as_newer() {
    let lower = pointer(5, A, 10);
    let higher = pointer(5, B, 1);

    let low_floor = floor_at(5, A);
    assert!(low_floor.accepts(&higher));
    assert_eq!(low_floor.newest([&lower, &higher]), Some(&higher));

    let high_floor = floor_at(5, B);
    assert!(!high_floor.accepts(&lower));
    assert_eq!(high_floor.newest([&lower, &higher]), Some(&higher));
}

#[test]
fn a_floor_ranks_accepted_pointers_by_epoch_then_by_term() {
    let floor = floor_at(5, A);
    let same_epoch_late_term = pointer(5, A, 9);
    let same_epoch_early_term = pointer(5, A, 2);
    let later_epoch_foreign = pointer(6, B, 1);
    let below_floor = pointer(4, A, 99);

    let ranked = floor.newest_first([
        &same_epoch_early_term,
        &below_floor,
        &same_epoch_late_term,
        &later_epoch_foreign,
    ]);

    assert_eq!(
        ranked,
        vec![&later_epoch_foreign, &same_epoch_late_term, &same_epoch_early_term],
        "the epoch of another lineage numbered above the floor outranks lower numbers, and the \
         pointer below the floor is left out"
    );

    let (lower, higher) = (pointer(7, B, 50), pointer(7, 3, 1));
    assert_eq!(floor.newest([&lower, &higher]), Some(&higher));
}

#[test]
fn a_node_that_never_joined_has_no_floor_and_accepts_every_pointer() {
    let floor = JoinFloor::none();

    assert!(floor.accepts(&pointer(0, A, 1)));
    assert_eq!(
        floor.newest([&pointer(3, A, 1), &pointer(3, A, 8)]),
        Some(&pointer(3, A, 8))
    );
}

// An authority epoch of another lineage replaces the floor even below its
// number; one of the floor's own lineage leaves it where it is.
#[test]
fn a_floor_refreshes_to_the_authoritys_epoch_only_when_its_lineage_differs() {
    let mut floor = floor_at(5, A);

    floor.refresh(RecoveryEpoch::new(9, A));
    assert_eq!(floor, floor_at(5, A));

    floor.refresh(RecoveryEpoch::new(2, B));
    assert_eq!(floor, floor_at(2, B));
}

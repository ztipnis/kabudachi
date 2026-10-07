//! Which voters hold a Task record: the nearest to its key, up to the
//! replication factor, with a majority of them as the quorum.

use std::num::NonZeroUsize;
use std::str::FromStr;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_net::task_store::placement::{Placement, ReplicationFactor, placement};
use kabudachi_net::task_store::record_key;
use libp2p::PeerId;
use libp2p::kad::{KBucketDistance, KBucketKey};

fn voters(count: usize) -> Vec<WorkerId> {
    (0..count)
        .map(|_| WorkerId::new(PeerId::random().to_string()))
        .collect()
}

fn distance(task: &TaskId, voter: &WorkerId) -> KBucketDistance {
    KBucketKey::new(record_key(task))
        .distance(&KBucketKey::from(PeerId::from_str(voter.as_str()).unwrap()))
}

#[test]
fn a_placement_is_the_nearest_voters_up_to_the_factor_with_a_majority_quorum() {
    let task = TaskId::new("task-1");
    let two = ReplicationFactor::new(NonZeroUsize::new(2).unwrap());
    // (voters known, factor, holders, quorum)
    let rows = [
        (7, ReplicationFactor::DEFAULT, 3, 2),
        (7, two, 2, 2),
        (2, ReplicationFactor::DEFAULT, 2, 2),
        (1, ReplicationFactor::DEFAULT, 1, 1),
    ];
    for (known, factor, holders, quorum) in rows {
        let voters = voters(known);

        let placed = placement(&task, &voters, factor).expect("voters are known");

        assert_eq!((placed.holders.len(), placed.quorum), (holders, quorum), "{known} voters");
        let farthest_held = placed.holders.iter().map(|voter| distance(&task, voter)).max().unwrap();
        assert!(
            voters
                .iter()
                .filter(|voter| !placed.holders.contains(voter))
                .all(|voter| distance(&task, voter) > farthest_held),
            "every voter left out is farther than every holder ({known} voters)"
        );
    }

    let no_voters: Option<Placement> = placement(&task, &[], ReplicationFactor::DEFAULT);
    assert_eq!(no_voters, None);
}

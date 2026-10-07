//! Which voters hold a Task record, and how many must store a revision of it
//! before the write counts.

use std::num::NonZeroUsize;
use std::str::FromStr;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use libp2p::PeerId;
use libp2p::kad::{KBucketDistance, KBucketKey};

use super::record_key;

/// How many voters each Task record is written to: three unless configured
/// otherwise, never more than the leader knows by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationFactor(NonZeroUsize);

impl ReplicationFactor {
    pub const DEFAULT: ReplicationFactor = ReplicationFactor(NonZeroUsize::new(3).unwrap());

    pub fn new(factor: NonZeroUsize) -> Self {
        ReplicationFactor(factor)
    }
}

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Where one revision of a record is written and how many must store it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The voters nearest the record's key by kad's XOR distance, nearest first.
    pub holders: Vec<WorkerId>,
    /// How many of them must store it before the write counts: a majority,
    /// so any later read of `holders.len() - quorum + 1` of them meets it.
    pub quorum: usize,
}

/// The `factor` voters nearest `task`'s key (the task id's bytes, as kad
/// keys the record), each voter at its peer id's kad key. `None` without
/// voters, or when a voter's id is not a peer id.
pub fn placement(
    task: &TaskId,
    voters: &[WorkerId],
    factor: ReplicationFactor,
) -> Option<Placement> {
    let key = KBucketKey::new(record_key(task));
    let mut ranked: Vec<(KBucketDistance, WorkerId)> = voters
        .iter()
        .map(|voter| {
            PeerId::from_str(voter.as_str())
                .map(|peer| (key.distance(&KBucketKey::from(peer)), voter.clone()))
        })
        .collect::<Result<_, _>>()
        .ok()?;
    ranked.sort();
    let r = factor.0.get().min(ranked.len());
    (r > 0).then(|| Placement {
        holders: ranked.into_iter().take(r).map(|(_, voter)| voter).collect(),
        quorum: r / 2 + 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_placement_is_the_factor_nearest_voters_with_a_majority_quorum() {
        let voters: Vec<WorkerId> = (0..7)
            .map(|_| WorkerId::new(PeerId::random().to_string()))
            .collect();
        let task = TaskId::new("task-1");

        let placed = placement(&task, &voters, ReplicationFactor::DEFAULT).unwrap();

        assert_eq!(placed.holders.len(), 3);
        assert_eq!(placed.quorum, 2);
        let key = KBucketKey::new(record_key(&task));
        let distance = |voter: &WorkerId| {
            key.distance(&KBucketKey::from(PeerId::from_str(voter.as_str()).unwrap()))
        };
        let farthest_held = placed.holders.iter().map(distance).max().unwrap();
        assert!(
            voters
                .iter()
                .filter(|v| !placed.holders.contains(v))
                .all(|v| distance(v) > farthest_held)
        );
    }

    #[test]
    fn the_factor_is_capped_at_the_voters_known() {
        let one = vec![WorkerId::new(PeerId::random().to_string())];
        let placed = placement(&TaskId::new("t"), &one, ReplicationFactor::DEFAULT).unwrap();
        assert_eq!((placed.holders.len(), placed.quorum), (1, 1));
        assert_eq!(
            placement(&TaskId::new("t"), &[], ReplicationFactor::DEFAULT),
            None
        );
    }
}

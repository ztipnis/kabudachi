//! Which voters hold a record, and how many must store a revision of it
//! before the write counts.

use std::num::NonZeroUsize;
use std::str::FromStr;

use kabudachi_core::protocol::ids::WorkerId;
pub use kabudachi_core::task_record::Placement;
use kabudachi_core::task_record::RecordKey;
use libp2p::PeerId;
use libp2p::kad::{KBucketDistance, KBucketKey};

/// How many voters each record is written to: three unless configured
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

/// The `factor` voters nearest `key` by kad's XOR distance (the bytes kad
/// keys the record by: a task id's own, or a composite record's or page's
/// key), each voter at its peer id's kad key. `None` without voters, or when
/// a voter's id is not a peer id.
pub fn placement(
    key: &RecordKey,
    voters: &[WorkerId],
    factor: ReplicationFactor,
) -> Option<Placement> {
    let key = KBucketKey::new(key.kad_bytes());
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

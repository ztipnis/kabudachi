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

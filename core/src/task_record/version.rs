use std::cmp::Ordering;

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::generated;

/// Which revision of a Task record a record is: ordered by recovery epoch
/// (number, then lineage), then leader term, then revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordVersion {
    pub recovery_epoch: RecoveryEpoch,
    pub leader_term: u64,
    pub revision: u64,
}

/// Where one version stands against another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionOrder {
    Newer,
    Same,
    Older,
}

impl RecordVersion {
    /// Where `other` stands against `self`.
    pub fn order(&self, other: &RecordVersion) -> VersionOrder {
        match other.cmp(self) {
            Ordering::Greater => VersionOrder::Newer,
            Ordering::Equal => VersionOrder::Same,
            Ordering::Less => VersionOrder::Older,
        }
    }
}

impl From<RecordVersion> for generated::RecordVersion {
    fn from(version: RecordVersion) -> Self {
        generated::RecordVersion {
            recovery_epoch: version.recovery_epoch.number,
            recovery_lineage: version.recovery_epoch.lineage,
            leader_term: version.leader_term,
            revision: version.revision,
        }
    }
}

impl From<&generated::RecordVersion> for RecordVersion {
    fn from(wire: &generated::RecordVersion) -> Self {
        RecordVersion {
            recovery_epoch: RecoveryEpoch::new(wire.recovery_epoch, wire.recovery_lineage),
            leader_term: wire.leader_term,
            revision: wire.revision,
        }
    }
}

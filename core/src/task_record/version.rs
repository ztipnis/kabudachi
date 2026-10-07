use std::cmp::Ordering;

use crate::coordination_authority::RecoveryEpoch;
use crate::election::{EpochOrder, HeardEpoch, order_epochs};
use crate::protocol::generated;

/// Which revision of a Task record a record is (see [`RecordVersion::order`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Where `other` stands against `self`. Recovery epochs are compared as
    /// a worker compares an epoch it hears of with its own: a later epoch of
    /// the same lineage, or another lineage's epoch numbered above this one,
    /// is newer; another lineage's at or below this one's number is older,
    /// whatever its term. Within one epoch, term then revision decide.
    pub fn order(&self, other: &RecordVersion) -> VersionOrder {
        match order_epochs(&self.recovery_epoch, HeardEpoch::from(other.recovery_epoch)) {
            EpochOrder::Later => VersionOrder::Newer,
            EpochOrder::Stale => VersionOrder::Older,
            EpochOrder::Mine => {
                match (other.leader_term, other.revision).cmp(&(self.leader_term, self.revision)) {
                    Ordering::Greater => VersionOrder::Newer,
                    Ordering::Equal => VersionOrder::Same,
                    Ordering::Less => VersionOrder::Older,
                }
            }
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

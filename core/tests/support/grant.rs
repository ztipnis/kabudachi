//! The leadership grant scheduler tests give a scheduler that has to lead.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd};

/// A grant that never runs out, for a test that needs its scheduler to lead
/// and is not about when leadership ends.
pub fn unbounded_grant() -> LeadershipGrant {
    LeadershipGrant {
        term: 1,
        recovery_epoch: RecoveryEpoch::new(0, 0),
        valid_until: LeaseEnd::Unbounded,
        reconnect_timeout: kabudachi_core::election::ElectionTimings::DEFAULT_RECONNECT_TIMEOUT,
    }
}

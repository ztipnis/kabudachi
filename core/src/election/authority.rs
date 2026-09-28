//! The calls a [`super::WorkerNode`] asks its driver to make on the
//! coordination authority, and the replies the driver feeds back (design
//! 3.1). The node does no I/O: it names a call in an [`super::Output`], the
//! driver performs it (see [`AuthorityCall::perform`]) and hands the node
//! the reply as an [`super::Input`]. So an authority that answers late, or
//! from another task, times the node's lease exactly as one that answers
//! at once.

use crate::coordination_authority::{
    AuthorityError, CoordinationAuthority, LiveRegistrations, RecoveryEpoch,
};
use crate::protocol::ids::{ShardId, WorkerId};
use crate::time::{Duration, Instant};

/// How long a node with a coordination authority expects its registration
/// and, while it leads, its recovery fence to last (ADR-0001 decision 11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityTimings {
    /// The TTL the node expects the authority to grant. The node renews its
    /// registration, and while it leads its fence, every third of it, and
    /// treats each as lapsing a tenth of it early, for clock drift, or
    /// earlier still when the authority grants a shorter TTL. Every worker
    /// of a shard must use the same value: a worker that renews less often
    /// than the authority expires registrations drops out of its count.
    pub ttl: Duration,
}

impl AuthorityTimings {
    /// ADR-0001's default TTL: 30 s, renewed every 10 s.
    pub const DEFAULT_TTL: Duration = Duration::from_secs(30);
}

impl Default for AuthorityTimings {
    fn default() -> Self {
        AuthorityTimings {
            ttl: Self::DEFAULT_TTL,
        }
    }
}

/// One call on the coordination authority, for the node's own shard and on
/// behalf of the node's own worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityRequest {
    /// Register or renew the worker at its address, which the driver
    /// supplies.
    Register,
    /// Read the shard's live registrations.
    ReadLiveRegistrations,
    /// Read the shard's recovery epoch.
    ReadRecoveryEpoch,
    /// Compare-and-swap the shard's recovery epoch from `expected` to `new`.
    SwapRecoveryEpoch {
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
    },
    /// Acquire or renew the shard's recovery fence at `recovery_epoch`.
    AcquireFence { recovery_epoch: RecoveryEpoch },
}

/// A call the node asks for, stamped with the instant it asked, on its own
/// clock. The reply carries the stamp back: a registration or fence lasts
/// from when it was asked for, not from when the reply arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityCall {
    pub request: AuthorityRequest,
    pub sent_at: Instant,
}

/// What the authority answered to one [`AuthorityCall`]: one variant per
/// request, with the request's arguments and its `sent_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityReply {
    Registered {
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
    },
    LiveRegistrations {
        sent_at: Instant,
        result: Result<LiveRegistrations, AuthorityError>,
    },
    RecoveryEpoch {
        sent_at: Instant,
        result: Result<Option<RecoveryEpoch>, AuthorityError>,
    },
    RecoveryEpochSwapped {
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        sent_at: Instant,
        result: Result<(), AuthorityError>,
    },
    Fence {
        recovery_epoch: RecoveryEpoch,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
    },
}

impl AuthorityCall {
    /// Makes this call on `authority` for `shard_id` and `worker_id`, whose
    /// registration names `address`, and returns the reply to hand back to
    /// the node that asked for it.
    pub fn perform(
        &self,
        authority: &dyn CoordinationAuthority,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> AuthorityReply {
        let sent_at = self.sent_at;
        match self.request {
            AuthorityRequest::Register => AuthorityReply::Registered {
                sent_at,
                result: authority.register(shard_id, worker_id, address),
            },
            AuthorityRequest::ReadLiveRegistrations => AuthorityReply::LiveRegistrations {
                sent_at,
                result: authority.live_registrations(shard_id),
            },
            AuthorityRequest::ReadRecoveryEpoch => AuthorityReply::RecoveryEpoch {
                sent_at,
                result: authority.read_recovery_epoch(shard_id),
            },
            AuthorityRequest::SwapRecoveryEpoch { expected, new } => {
                AuthorityReply::RecoveryEpochSwapped {
                    expected,
                    new,
                    sent_at,
                    result: authority.compare_and_swap_recovery_epoch(shard_id, expected, new),
                }
            }
            AuthorityRequest::AcquireFence { recovery_epoch } => AuthorityReply::Fence {
                recovery_epoch,
                sent_at,
                result: authority.acquire_fence(shard_id, worker_id, recovery_epoch),
            },
        }
    }

    /// The reply a driver with no authority to reach hands back: the call
    /// failed as `Unavailable`.
    pub fn unavailable(&self) -> AuthorityReply {
        let sent_at = self.sent_at;
        match self.request {
            AuthorityRequest::Register => AuthorityReply::Registered {
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
            AuthorityRequest::ReadLiveRegistrations => AuthorityReply::LiveRegistrations {
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
            AuthorityRequest::ReadRecoveryEpoch => AuthorityReply::RecoveryEpoch {
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
            AuthorityRequest::SwapRecoveryEpoch { expected, new } => {
                AuthorityReply::RecoveryEpochSwapped {
                    expected,
                    new,
                    sent_at,
                    result: Err(AuthorityError::Unavailable),
                }
            }
            AuthorityRequest::AcquireFence { recovery_epoch } => AuthorityReply::Fence {
                recovery_epoch,
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
        }
    }
}

/// `duration` less its share for clock drift: `1 / divisor` of it, rounded
/// up, so it never lasts longer than it promises (see
/// `ElectionTimings::clock_drift_divisor`). `divisor` must not be zero.
pub(crate) fn less_drift(duration: Duration, divisor: u64) -> Duration {
    let ticks = duration.as_ticks();
    Duration::from_ticks(ticks - ticks.div_ceil(divisor))
}

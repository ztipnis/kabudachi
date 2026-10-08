//! The calls a [`super::WorkerNode`] asks its driver to make on the
//! coordination authority, and the replies the driver feeds back. The node
//! does no I/O: it names a call in an [`super::Output`], the
//! driver performs it (see [`AuthorityCall::perform`]) and hands the node
//! the reply as an [`super::Input`]. So an authority that answers late, or
//! from another task, times the node's lease exactly as one that answers
//! at once.

use crate::coordination_authority::{
    AuthorityError, CoordinationAuthority, LiveRegistrations, RecoveryEpoch, ShardRecord,
};
use crate::protocol::ids::{ShardId, ShardName, WorkerId};
use crate::time::{Duration, Instant};

/// How long a node with a coordination authority expects its registration
/// and, while it leads, its recovery fence to last.
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
    /// The default TTL: 30 s, renewed every 10 s.
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

/// Who minted a reply token. Each issuer numbers its own calls from 0, so
/// the issuer is what keeps a token of one from equalling a token of the
/// other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Issuer {
    /// A `WorkerNode`, for the calls its steps ask.
    Node,
    /// Net, for the calls it asks for itself, not for a node: the bootstrap
    /// cascade's, and the driver's rejoin reads.
    Cascade,
}

/// A kind of authority call: one per [`AuthorityRequest`] variant, named as
/// the request is. The one definition net's one-in-flight rule and
/// testkit's `FaultingAuthority::hold_next` key on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CallKind {
    Register,
    ReadLiveRegistrations,
    ReadRecoveryEpoch,
    SwapRecoveryEpoch,
    AcquireFence,
}

impl CallKind {
    /// The kind of call `request` asks for.
    pub fn of(request: &AuthorityRequest) -> Self {
        match request {
            AuthorityRequest::Register => CallKind::Register,
            AuthorityRequest::ReadLiveRegistrations => CallKind::ReadLiveRegistrations,
            AuthorityRequest::ReadRecoveryEpoch => CallKind::ReadRecoveryEpoch,
            AuthorityRequest::SwapRecoveryEpoch { .. } => CallKind::SwapRecoveryEpoch,
            AuthorityRequest::AcquireFence { .. } => CallKind::AcquireFence,
        }
    }
}

/// What matches an authority reply to the call that asked it: who issued
/// the call, its kind, and a number that issuer never reuses. Numbers are
/// unique per issuer only, so a token of one issuer never equals a token of
/// the other: they differ in `issuer` whatever their kind and number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplyToken {
    pub issuer: Issuer,
    pub kind: CallKind,
    pub number: u64,
}

/// One issuer's token mint. Every token it gives names its issuer and has a
/// number it never gave before (it wraps only after `u64::MAX` calls).
#[derive(Debug, Clone)]
pub struct ReplyTokens {
    issuer: Issuer,
    next_number: u64,
}

impl ReplyTokens {
    /// A mint for `issuer`, counting from 0.
    pub fn new(issuer: Issuer) -> Self {
        ReplyTokens {
            issuer,
            next_number: 0,
        }
    }

    /// The token for a call of `request`: this mint's issuer,
    /// `CallKind::of(request)` and a fresh number.
    pub fn next(&mut self, request: &AuthorityRequest) -> ReplyToken {
        let number = self.next_number;
        self.next_number = self.next_number.wrapping_add(1);
        ReplyToken {
            issuer: self.issuer,
            kind: CallKind::of(request),
            number,
        }
    }
}

/// A call the node asks for, stamped with the instant it asked, on its own
/// clock, and with the token its reply carries back. A registration or fence
/// lasts from when it was asked for, not from when the reply arrived, so the
/// reply also carries the stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityCall {
    pub request: AuthorityRequest,
    pub token: ReplyToken,
    /// When the call was asked for, on the asker's clock: it times the
    /// lease, and becomes a founder's `registered_at`.
    pub sent_at: Instant,
}

/// What the authority answered to one [`AuthorityCall`]: one variant per
/// request, with the request's arguments, its token and its `sent_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityReply {
    Registered {
        token: ReplyToken,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
    },
    LiveRegistrations {
        token: ReplyToken,
        sent_at: Instant,
        result: Result<LiveRegistrations, AuthorityError>,
    },
    RecoveryEpoch {
        token: ReplyToken,
        sent_at: Instant,
        result: Result<Option<RecoveryEpoch>, AuthorityError>,
    },
    RecoveryEpochSwapped {
        token: ReplyToken,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        sent_at: Instant,
        result: Result<(), AuthorityError>,
    },
    Fence {
        token: ReplyToken,
        recovery_epoch: RecoveryEpoch,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
    },
}

impl AuthorityReply {
    /// The token of the call this replies to.
    pub fn token(&self) -> ReplyToken {
        match self {
            AuthorityReply::Registered { token, .. }
            | AuthorityReply::LiveRegistrations { token, .. }
            | AuthorityReply::RecoveryEpoch { token, .. }
            | AuthorityReply::RecoveryEpochSwapped { token, .. }
            | AuthorityReply::Fence { token, .. } => *token,
        }
    }
}

impl AuthorityCall {
    /// The call `request`, asked at `sent_at`, with the next token from
    /// `tokens`. Its token's kind is therefore always
    /// `CallKind::of(&request)`.
    pub fn new(request: AuthorityRequest, tokens: &mut ReplyTokens, sent_at: Instant) -> Self {
        AuthorityCall {
            request,
            token: tokens.next(&request),
            sent_at,
        }
    }

    /// Makes this call on `authority` for the shard `shard_id` under `name`
    /// and for `worker_id`, whose registration names `address`, and returns
    /// the reply to hand back to the node that asked for it. The node speaks
    /// in epochs; this is where an epoch becomes a record of `shard_id`, and
    /// a record back its epoch.
    pub fn perform(
        &self,
        authority: &dyn CoordinationAuthority,
        name: &ShardName,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> AuthorityReply {
        let (token, sent_at) = (self.token, self.sent_at);
        let record = |recovery_epoch| ShardRecord {
            shard_id: shard_id.clone(),
            recovery_epoch,
        };
        match self.request {
            AuthorityRequest::Register => AuthorityReply::Registered {
                token,
                sent_at,
                result: authority.register(name, shard_id, worker_id, address),
            },
            AuthorityRequest::ReadLiveRegistrations => AuthorityReply::LiveRegistrations {
                token,
                sent_at,
                result: authority.live_registrations(name, shard_id),
            },
            AuthorityRequest::ReadRecoveryEpoch => AuthorityReply::RecoveryEpoch {
                token,
                sent_at,
                result: authority
                    .read_shard(name)
                    .map(|held| held.map(|held| held.recovery_epoch)),
            },
            AuthorityRequest::SwapRecoveryEpoch { expected, new } => {
                AuthorityReply::RecoveryEpochSwapped {
                    token,
                    expected,
                    new,
                    sent_at,
                    result: authority.compare_and_swap_shard(
                        name,
                        expected.map(record).as_ref(),
                        &record(new),
                    ),
                }
            }
            AuthorityRequest::AcquireFence { recovery_epoch } => AuthorityReply::Fence {
                token,
                recovery_epoch,
                sent_at,
                result: authority.acquire_fence(name, worker_id, &record(recovery_epoch)),
            },
        }
    }

    /// The reply a driver with no authority to reach hands back: the call
    /// failed as `Unavailable`.
    pub fn unavailable(&self) -> AuthorityReply {
        let (token, sent_at) = (self.token, self.sent_at);
        match self.request {
            AuthorityRequest::Register => AuthorityReply::Registered {
                token,
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
            AuthorityRequest::ReadLiveRegistrations => AuthorityReply::LiveRegistrations {
                token,
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
            AuthorityRequest::ReadRecoveryEpoch => AuthorityReply::RecoveryEpoch {
                token,
                sent_at,
                result: Err(AuthorityError::Unavailable),
            },
            AuthorityRequest::SwapRecoveryEpoch { expected, new } => {
                AuthorityReply::RecoveryEpochSwapped {
                    token,
                    expected,
                    new,
                    sent_at,
                    result: Err(AuthorityError::Unavailable),
                }
            }
            AuthorityRequest::AcquireFence { recovery_epoch } => AuthorityReply::Fence {
                token,
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

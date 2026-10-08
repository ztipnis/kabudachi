//! The external coordination service: it holds each worker's TTL
//! registration, each shard name's shard record (which incarnation lives
//! under it, and its recovery epoch), its recovery fence and its leader hint.
//! Workers consult it for bootstrap, forced recovery and fencing; it is off
//! the hot path.

use std::collections::BTreeMap;

use crate::protocol::ids::{ShardId, ShardName, WorkerId};
use crate::time::Duration;

/// One coordination service shared by every worker of a shard. Every
/// registration, fence and hint it grants lasts one TTL, the same TTL it
/// reports back, unless renewed.
///
/// Calls block, and a worker makes them off its event loop, at most one of
/// each kind at a time. An implementation over a remote service must bound
/// every call in time (well inside a third of the TTL, the renewal
/// interval) and answer [`AuthorityError::Unavailable`] when the bound
/// passes: a call that hangs holds up that worker's next call of the same
/// kind, and a worker that cannot renew fences itself.
pub trait CoordinationAuthority {
    /// The TTL every registration, fence, hint and warm-up lasts; `register`
    /// and `acquire_fence` return exactly it. Every worker of a shard derives
    /// its renewal timing from it.
    fn ttl(&self) -> Duration;

    /// Registers `worker_id` at `address` under `name`, tagged `shard_id`, or
    /// renews an existing registration (replacing its address and id), and
    /// returns the registration TTL. The registration lapses one TTL from now
    /// unless it is renewed before then.
    fn register(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> Result<Duration, AuthorityError>;

    /// The unexpired registrations under `name`. Only registrations tagged
    /// `shard_id` are listed; a worker has one registration under a name,
    /// tagged with the id it last registered. See [`LiveRegistrations`] for
    /// why their count may not be authoritative yet.
    fn live_registrations(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
    ) -> Result<LiveRegistrations, AuthorityError>;

    /// The record under `name`, or `None` when the name has none: it was
    /// never created, or the authority lost it (a flush).
    fn read_shard(&self, name: &ShardName) -> Result<Option<ShardRecord>, AuthorityError>;

    /// Sets the record under `name` to `new`, but only if it is currently
    /// `expected`, shard id, number and lineage alike. `expected = None` means
    /// "the record is missing", which makes this a create-if-absent: of
    /// several workers racing to found a shard, exactly one succeeds.
    /// `expected` and the current record are compared field by field; on a
    /// mismatch it returns [`AuthorityError::ShardConflict`] carrying the
    /// current record, and changes nothing.
    fn compare_and_swap_shard(
        &self,
        name: &ShardName,
        expected: Option<&ShardRecord>,
        new: &ShardRecord,
    ) -> Result<(), AuthorityError>;

    /// Acquires the fence under `name` for `holder`, or renews it if `holder`
    /// already holds it, and returns the fence TTL. A leader must hold the
    /// fence to act.
    ///
    /// `record` must be the current record under `name`, shard id, number and
    /// lineage alike; otherwise this returns [`AuthorityError::ShardConflict`],
    /// so a leader from before a forced recovery, or of a shard founded
    /// afresh after the authority lost its data, cannot renew. While another holder's fence is
    /// unexpired, it returns [`AuthorityError::FenceHeld`] with the time left
    /// on that fence, whatever record that fence was taken at: a new leader
    /// waits the old one out.
    ///
    /// A fence taken before the authority lost its data is lost with the
    /// data, so the authority cannot make a new holder wait it out. Instead,
    /// until one TTL has passed since it started or last lost its data, it
    /// grants no fence at all and answers [`AuthorityError::FenceHeld`] with
    /// the rest of that TTL: every fence taken before the loss has expired
    /// by then. After an outage that kept its data, it still knows every
    /// fence and needs no such wait.
    ///
    /// The fence is a lease, not a fencing token: it returns only a TTL, and
    /// the authority mints no token of its own. It bounds how long a leader
    /// may act without hearing from the authority, and makes a new leader
    /// wait out the old one's fence. A late election message from an earlier
    /// leader (a leader ack, a roll call or a vote) is rejected by the
    /// recovery epoch and election term it carries: both only ever increase,
    /// and an ordinary handover raises the term. A claim response carries
    /// neither, so the claims an earlier leader grants are bounded by its
    /// leader lease instead.
    fn acquire_fence(
        &self,
        name: &ShardName,
        holder: &WorkerId,
        record: &ShardRecord,
    ) -> Result<Duration, AuthorityError>;

    /// Replaces the name's hint. It lapses one TTL after this write and is
    /// lost with the authority's data.
    fn publish_leader_hint(
        &self,
        name: &ShardName,
        hint: &LeaderHint,
    ) -> Result<(), AuthorityError>;

    /// The unexpired hint, or `None`.
    fn read_leader_hint(&self, name: &ShardName) -> Result<Option<LeaderHint>, AuthorityError>;
}

/// A shard's recovery epoch as the authority holds it: its number, which
/// only ever rises while the authority keeps its data, and the lineage it
/// belongs to.
///
/// Numbers alone are not unique across a flush. An authority that loses its
/// data forgets every epoch, so a worker that later finds the shard gone
/// and founds it afresh starts again at 0, a number the lost shard may have
/// held too. The lineage tells the two apart: whoever founds a shard (creates
/// its epoch, or re-founds it with no worker left) picks a fresh one
/// ([`Self::founding`]), every epoch recovered from it keeps it, and a
/// leader that republishes its epoch after a flush puts back
/// the same one. A worker cut off from the authority resumes only if the
/// epoch it finds there is exactly its own, lineage included.
///
/// Epochs are ordered by number, then lineage: lineages are random, so the
/// lineage is a fixed tie-break between two foundings at one number, not a
/// sign of which came later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecoveryEpoch {
    pub number: u64,
    pub lineage: u64,
}

/// Where a founder draws the lineage of a shard it founds (see
/// [`RecoveryEpoch::founding`]), so that a test can fix it.
pub trait LineageSource {
    /// A lineage no other founding, before or after any flush, is to pick.
    fn fresh_lineage(&mut self) -> u64;
}

/// The production [`LineageSource`]: each lineage is drawn from a fresh
/// UUIDv7, so no other founding picks the same one (short of a chance of
/// about 1 in 2^62: the UUID's time and counter bits are not all random).
#[derive(Debug, Clone, Copy, Default)]
pub struct Uuid7Lineages;

impl LineageSource for Uuid7Lineages {
    fn fresh_lineage(&mut self) -> u64 {
        let (high, low) = uuid::Uuid::now_v7().as_u64_pair();
        high ^ low
    }
}

impl RecoveryEpoch {
    pub const fn new(number: u64, lineage: u64) -> Self {
        RecoveryEpoch { number, lineage }
    }

    /// The first epoch of a shard founded now, at `number`, of a new lineage
    /// drawn from `lineages` (production draws from [`Uuid7Lineages`]).
    pub fn founding(number: u64, lineages: &mut impl LineageSource) -> Self {
        RecoveryEpoch {
            number,
            lineage: lineages.fresh_lineage(),
        }
    }

    /// The epoch a forced recovery swaps this one for: the next number, of
    /// the same lineage. `None` at `u64::MAX`, which has no successor.
    pub fn next(self) -> Option<Self> {
        Some(RecoveryEpoch {
            number: self.number.checked_add(1)?,
            lineage: self.lineage,
        })
    }
}

impl std::fmt::Display for RecoveryEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (lineage {:x})", self.number, self.lineage)
    }
}

/// What the authority holds for one shard name: which incarnation of the
/// shard lives under it, and that incarnation's recovery epoch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShardRecord {
    pub shard_id: ShardId,
    pub recovery_epoch: RecoveryEpoch,
}

/// Where a shard's leader last said it could be reached. A hint only says
/// whom to ask first: the asked worker's answer decides.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderHint {
    pub shard_id: ShardId,
    pub leader: WorkerId,
    pub address: String,
    pub recovery_epoch: RecoveryEpoch,
    pub term: u64,
}

/// A shard's unexpired registrations, each worker with the address it
/// registered.
///
/// An authority reports an authoritative count only once one full TTL has
/// passed since it last became available: since it started, since it lost
/// its data, or since an outage ended. That TTL is the warm-up. Until it
/// ends, the authority cannot tell a worker that has not registered yet from
/// one that is gone. A registration made before it started or lost its data
/// is unknown to it until renewed, and after an outage longer than a TTL
/// every registration has lapsed, so the first few workers to register
/// again could pass for a majority. By the end of the warm-up, every live
/// worker has had to renew.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRegistrations {
    addresses: BTreeMap<WorkerId, String>,
    warmed_up: bool,
}

impl LiveRegistrations {
    /// For `CoordinationAuthority` implementations. `warmed_up` is whether
    /// one full TTL has passed since the authority last became available.
    pub fn new(addresses: BTreeMap<WorkerId, String>, warmed_up: bool) -> Self {
        Self {
            addresses,
            warmed_up,
        }
    }

    /// Each worker with an unexpired registration, and the address it
    /// registered. They are reported during warm-up too; only the count is
    /// withheld then (see [`Self::authoritative_count`]).
    pub fn addresses(&self) -> &BTreeMap<WorkerId, String> {
        &self.addresses
    }

    /// The number of live registrations, or `None` while the authority is
    /// still warming up and the count may be missing workers.
    pub fn authoritative_count(&self) -> Option<usize> {
        self.warmed_up.then_some(self.addresses.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthorityError {
    #[error("coordination authority is unavailable")]
    Unavailable,
    #[error("shard record conflict: {}", describe_record(.current))]
    ShardConflict { current: Option<ShardRecord> },
    #[error(
        "recovery fence is held by another worker for {} more ms",
        .remaining.as_ticks()
    )]
    FenceHeld { remaining: Duration },
}

fn describe_record(current: &Option<ShardRecord>) -> String {
    match current {
        Some(record) => format!(
            "the authority holds shard {} at epoch {}",
            record.shard_id.as_str(),
            record.recovery_epoch
        ),
        None => "the authority holds no record for the shard".to_string(),
    }
}

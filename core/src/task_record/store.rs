use std::collections::{BTreeMap, BTreeSet};

use crate::blob::{BlobKey, BlobOwner, BlobRecord, MAX_BLOB_BYTES};
use crate::protocol::generated::{self, TaskRecord};
use crate::protocol::ids::{TaskId, WorkerId};
use crate::task_record::version::{RecordVersion, VersionOrder};
use crate::time::{Duration, Instant};

/// How long a holder keeps a blob for an owner it has never held a record of
/// before it asks the shard whether that owner is still unfinished: long
/// enough for a submission to be recorded after its input's blobs were
/// stored. Asking again waits as long.
pub const ORPHAN_BLOB_GRACE: Duration = Duration::from_ticks(10 * 60 * 1000);

/// What putting a record into a [`VersionedRecords`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Put {
    /// It was newer than what was held, or nothing was held: it is kept now.
    Stored,
    /// It was exactly the record already held: a republish.
    Unchanged,
    /// The leader wrote a revision whose holders do not include this one, and
    /// that is not older than what is held: the copy is replaced by a stub,
    /// which names the revision and its holders but holds none of the record.
    Retired,
}

/// Who sent a revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The shard's leader, writing to the holders it chose.
    Leader,
    /// A draining worker handing over a copy it held.
    HandOff,
}

/// Why a record was not kept. A writer must not count a refused put as
/// stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutRefusal {
    #[error("a newer revision of the record is held")]
    Older,
    #[error("another record with the same version is held")]
    Conflicting,
    #[error("the record lacks its version, its task or its task id")]
    Malformed,
}

/// Why a blob was not kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlobRefusal {
    #[error("the blob's bytes are not the ones its key names")]
    WrongKey,
    #[error("the blob is larger than a holder keeps")]
    TooLarge,
}

/// What a look-up across the shard found of a blob owner this store has
/// never held a record of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerState {
    /// Its record is held unfinished somewhere: keep its blobs.
    Unfinished,
    /// Its record is finished: its blobs go a grace from now.
    Finished,
    /// No one holds its record: its blobs go now.
    Gone,
}

/// The newest revision of each Task record a worker holds. A put never
/// replaces a record with an older one, nor with a different one of the same
/// version, unless it differs only in the voters it is placed on. A store
/// that knows its holder keeps only a stub of a revision the leader moves to
/// other holders: the task, the version and the holders, no record. The stub
/// is not a copy anyone reads (it is not served, handed off or claimed from),
/// but it is reported (see [`Self::reported`]), so a reader that asks this
/// holder learns that a newer revision exists and where it went.
///
/// A finished record, or the stub of one, is dropped once its retention has
/// passed, counted on this node's own clock from when it first held a
/// finished revision of it. Unfinished records are never dropped, and a put
/// is never refused for lack of room: the leader's memory budget bounds how
/// much there is.
///
/// It also holds blobs, each for the owners that stored it (see
/// [`Self::put_blob`]). An owner whose record (or stub) it holds keeps its
/// blobs exactly as long as that record. A leader's revision that moves the
/// record to other holders is noted ([`Self::take_blob_moves`]), so the blobs
/// can follow it. An owner whose record it never held keeps its blobs until a
/// look-up across the shard finds the record finished, a grace later, or gone
/// ([`Self::owners_to_check`], [`Self::owner_checked`]). A blob goes when its
/// last owner lets go.
#[derive(Debug, Clone, Default)]
pub struct VersionedRecords {
    records: BTreeMap<TaskId, Held>,
    retention: Option<Duration>,
    holder: Option<WorkerId>,
    blobs: BTreeMap<BlobKey, HeldBlob>,
    blob_owners: BTreeMap<BlobOwner, BlobsOf>,
    /// For each owner whose record a leader's revision moved to other holders,
    /// the holders it moved to, until taken.
    blob_moves: BTreeMap<BlobOwner, Vec<WorkerId>>,
    /// How long a blob is kept for an owner never seen here before the shard
    /// is asked about it; `None` never asks and waits for every owner.
    orphan_grace: Option<Duration>,
}

#[derive(Debug, Clone)]
struct HeldBlob {
    value: Vec<u8>,
    placement: Vec<WorkerId>,
    owners: BTreeSet<BlobOwner>,
}

/// What a store knows of one owner of the blobs it holds.
#[derive(Debug, Clone)]
struct BlobsOf {
    keys: BTreeSet<BlobKey>,
    /// The store has held the owner's record (or a stub of it): its blobs
    /// then stay exactly as long as that record does.
    seen: bool,
    /// For an owner never seen: when its grace started, from its first blob
    /// or the last look-up across the shard.
    since: Instant,
    /// For an owner never seen: the last look-up found its record finished.
    finished: bool,
}

#[derive(Debug, Clone)]
struct Held {
    record: TaskRecord,
    finished_since: Option<Instant>,
    /// Whether `record` is only the key of a revision held elsewhere.
    stub: bool,
}

/// Whether `record` is `held` placed on other holders (and so written to other
/// prior placements), and nothing else differs.
fn only_placed_elsewhere(held: &TaskRecord, record: &TaskRecord) -> bool {
    let mut replaced = record.clone();
    replaced.placement.clone_from(&held.placement);
    replaced.prior_placements.clone_from(&held.prior_placements);
    replaced == *held
}

/// Whether `records` holds `owner`'s record or its stub. Only a task's record
/// is ever held here.
fn owner_held(records: &BTreeMap<TaskId, Held>, owner: &BlobOwner) -> bool {
    match owner {
        BlobOwner::Task(task) => records.contains_key(task),
        BlobOwner::Composite(_) => false,
    }
}

/// The holders `record` is placed on.
fn holders_of(record: &TaskRecord) -> Vec<WorkerId> {
    record.placement.iter().cloned().map(WorkerId::from).collect()
}

/// What a store does with a revision of a task it already holds.
enum Admission {
    /// It keeps the revision, or its stub.
    Keep,
    /// What it holds already says everything the revision does.
    Unchanged,
}

/// Whether a store holding `held` keeps `record`, a revision of `version`
/// sent by `origin`, leaves what it holds unchanged, or refuses it.
fn admission(
    held: &Held,
    record: &TaskRecord,
    version: RecordVersion,
    origin: Origin,
) -> Result<Admission, PutRefusal> {
    let (_, held_version) = identify(&held.record)?;
    match held_version.order(&version) {
        VersionOrder::Newer => Ok(Admission::Keep),
        VersionOrder::Same => same_version(held, record, origin),
        VersionOrder::Older => Err(PutRefusal::Older),
    }
}

/// What a store does with `record` when it holds the same version, `held`.
fn same_version(held: &Held, record: &TaskRecord, origin: Origin) -> Result<Admission, PutRefusal> {
    // Only the key of this revision is held: a record that names this
    // holder now is the revision itself, and one that does not is the
    // same stub, perhaps placed elsewhere again.
    if held.stub {
        return Ok(Admission::Keep);
    }
    if held.record == *record {
        return Ok(Admission::Unchanged);
    }
    if !only_placed_elsewhere(&held.record, record) {
        return Err(PutRefusal::Conflicting);
    }
    if origin == Origin::HandOff {
        // The same revision, as another holder placed it: nothing
        // to learn, and its placement is stale where this is not.
        return Ok(Admission::Unchanged);
    }
    // A leader that placed the record anew after its voters
    // changed writes it again to a holder that kept the first
    // write: the holder takes the new placement, or keeps only a
    // stub of it if the new placement leaves it out.
    Ok(Admission::Keep)
}

impl VersionedRecords {
    /// A store that drops a finished record `retention` after it first held
    /// a finished revision of it, or keeps finished records with `None`.
    pub fn with_retention(retention: Option<Duration>) -> Self {
        VersionedRecords {
            records: BTreeMap::new(),
            retention,
            holder: None,
            ..VersionedRecords::default()
        }
    }

    /// The store of `holder`: a leader's revision whose holders leave `holder`
    /// out leaves it a stub of the revision instead of a copy (see
    /// [`Put::Retired`]).
    #[must_use]
    pub fn held_by(mut self, holder: WorkerId) -> Self {
        self.holder = Some(holder);
        self
    }

    /// Puts `record`, at `now` by this node's monotonic clock, after dropping
    /// every finished record whose retention has passed.
    pub fn put(&mut self, record: TaskRecord, now: Instant) -> Result<Put, PutRefusal> {
        self.put_from(record, Origin::Leader, now)
    }

    /// Like [`Self::put`], for a revision sent by `origin`. A leader's
    /// revision, not older than what is held, whose placement names holders
    /// but not this store's own, replaces the held copy with a stub of it
    /// ([`Put::Retired`]); an older one is refused as by `put`. A
    /// handed-off copy is stored by version alone, whatever its placement
    /// names, and a copy of the revision already held changes nothing.
    pub fn put_from(
        &mut self,
        record: TaskRecord,
        origin: Origin,
        now: Instant,
    ) -> Result<Put, PutRefusal> {
        self.sweep(now);
        let (task, version) = identify(&record)?;
        let leaves_me_out = self.leaves_me_out(&record, origin);
        let Some(held) = self.records.get(&task) else {
            return Ok(self.keep(task, record, leaves_me_out, origin, (None, None), now));
        };
        let before: BTreeSet<WorkerId> = holders_of(&held.record).into_iter().collect();
        let since = held.finished_since;
        match admission(held, &record, version, origin)? {
            Admission::Keep => Ok(self.keep(task, record, leaves_me_out, origin, (since, Some(before)), now)),
            Admission::Unchanged => Ok(Put::Unchanged),
        }
    }

    /// Whether `record`, a leader's revision whose placement names holders,
    /// leaves this store's own holder out.
    fn leaves_me_out(&self, record: &TaskRecord, origin: Origin) -> bool {
        origin == Origin::Leader
            && self.holder.as_ref().is_some_and(|me| {
                !record.placement.is_empty()
                    && !record.placement.iter().any(|holder| WorkerId::from(holder.clone()) == *me)
            })
    }

    /// Holds `record`, or only its stub if it leaves this holder out.
    /// `leaves_me_out` says whether it does, and `origin` who sent it;
    /// `finished_since` and the holders of the revision held before it
    /// (`None` if none was) are what that revision already established.
    fn keep(
        &mut self,
        task: TaskId,
        record: TaskRecord,
        leaves_me_out: bool,
        origin: Origin,
        (finished_since, before): (Option<Instant>, Option<BTreeSet<WorkerId>>),
        now: Instant,
    ) -> Put {
        self.see_owner(&task, &record, origin, before);
        if leaves_me_out {
            self.records.insert(task, Held::stub_of(&record, finished_since, now));
            Put::Retired
        } else {
            self.records.insert(task, Held::new(record, finished_since, now));
            Put::Stored
        }
    }

    /// Marks the owner `task` seen, once its record is held, and notes a move
    /// if a leader's `record` placed it on other holders than `before`. Only
    /// a leader's re-placement is a move: a handed-off copy names its old
    /// holders.
    fn see_owner(
        &mut self,
        task: &TaskId,
        record: &TaskRecord,
        origin: Origin,
        before: Option<BTreeSet<WorkerId>>,
    ) {
        let owner = BlobOwner::Task(task.clone());
        let Some(of) = self.blob_owners.get_mut(&owner) else {
            return;
        };
        of.seen = true;
        let after = holders_of(record);
        let moved = origin == Origin::Leader
            && !after.is_empty()
            && before.is_some_and(|before| before != after.iter().cloned().collect());
        if moved {
            self.blob_moves.insert(owner, after);
        }
    }

    /// Drops every finished record whose retention has passed by `now`, then
    /// lets go every owner whose record (seen here) has gone with it, or
    /// whose record (never seen here) was found finished a grace ago,
    /// dropping the blobs no owner holds any more. An owner never seen here
    /// is otherwise never dropped by a sweep: only a look-up that finds its
    /// record gone drops it (see [`Self::owner_checked`]). Says how many
    /// records and blobs it dropped.
    pub fn sweep(&mut self, now: Instant) -> usize {
        let mut dropped = 0;
        if let Some(retention) = self.retention {
            let before = self.records.len();
            self.records
                .retain(|_, held| held.finished_since.is_none_or(|since| since + retention > now));
            dropped += before - self.records.len();
        }
        let blobs_before = self.blobs.len();
        for owner in self.owners_let_go(now) {
            self.forget_blobs_of(&owner);
        }
        dropped + blobs_before - self.blobs.len()
    }

    /// The owners whose blobs go by `now`: those seen here whose record has
    /// gone, and those never seen found finished a grace ago.
    fn owners_let_go(&self, now: Instant) -> Vec<BlobOwner> {
        self.blob_owners
            .iter()
            .filter(|(owner, of)| !owner_held(&self.records, owner) && self.lets_go(of, now))
            .map(|(owner, _)| owner.clone())
            .collect()
    }

    fn lets_go(&self, of: &BlobsOf, now: Instant) -> bool {
        of.seen || (of.finished && self.orphan_grace.is_some_and(|grace| of.since + grace <= now))
    }

    /// When the next finished record's retention, or finished-elsewhere
    /// owner's grace, passes, if any.
    pub fn next_due(&self) -> Option<Instant> {
        let records = self.retention.and_then(|retention| {
            self.records.values().filter_map(|held| held.finished_since).min().map(|since| since + retention)
        });
        let blobs = self.orphan_grace.and_then(|grace| {
            self.blob_owners.values().filter(|of| !of.seen && of.finished).map(|of| of.since + grace).min()
        });
        records.into_iter().chain(blobs).min()
    }

    /// The record held of `task`, if this store holds one and not only a
    /// stub of it.
    pub fn get(&self, task: &TaskId) -> Option<&TaskRecord> {
        self.records.get(task).filter(|held| !held.stub).map(|held| &held.record)
    }

    /// Removes the record held of `task`; a stub stays, for it is no copy to
    /// remove and a reader still learns from it.
    pub fn remove(&mut self, task: &TaskId) -> Option<TaskRecord> {
        if self.records.get(task).is_some_and(|held| held.stub) {
            return None;
        }
        self.records.remove(task).map(|held| held.record)
    }

    /// The records held, the newest revision of each task: no stub.
    pub fn iter(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values().filter(|held| !held.stub).map(|held| &held.record)
    }

    /// What a holder reports of its tasks: each record, and the stub of each
    /// revision held elsewhere, the newest revision of each task.
    pub fn reported(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values().map(|held| &held.record)
    }

    /// The tasks held, as records or as stubs.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl VersionedRecords {
    /// A store that asks about an owner it has never held a record of once
    /// `grace` has passed since its first blob arrived.
    #[must_use]
    pub fn with_orphan_blob_grace(mut self, grace: Duration) -> Self {
        self.orphan_grace = Some(grace);
        self
    }

    /// Keeps `record`'s bytes under `key`, for `record`'s owner, after
    /// sweeping. A blob already held gains the owner. Refused if the bytes
    /// are not those `key` names or are larger than one chunk.
    pub fn put_blob(&mut self, key: &BlobKey, record: BlobRecord, now: Instant) -> Result<(), BlobRefusal> {
        self.sweep(now);
        if record.value.len() > MAX_BLOB_BYTES {
            return Err(BlobRefusal::TooLarge);
        }
        if !key.holds(&record.value) {
            return Err(BlobRefusal::WrongKey);
        }
        let seen = owner_held(&self.records, &record.owner);
        self.blob_owners
            .entry(record.owner.clone())
            .or_insert_with(|| BlobsOf { keys: BTreeSet::new(), seen, since: now, finished: false })
            .keys
            .insert(key.clone());
        let held = self.blobs.entry(key.clone()).or_insert_with(|| HeldBlob {
            value: record.value,
            placement: Vec::new(),
            owners: BTreeSet::new(),
        });
        held.placement = record.placement;
        held.owners.insert(record.owner);
        Ok(())
    }

    /// The blob held under `key`, as a record naming its first owner.
    pub fn blob(&self, key: &BlobKey) -> Option<BlobRecord> {
        let held = self.blobs.get(key)?;
        Some(BlobRecord {
            value: held.value.clone(),
            owner: held.owners.first()?.clone(),
            placement: held.placement.clone(),
        })
    }

    /// Every blob held, with its owners, in key order.
    pub fn blobs(&self) -> Vec<(BlobKey, Vec<BlobOwner>)> {
        self.blobs
            .iter()
            .map(|(key, held)| (key.clone(), held.owners.iter().cloned().collect()))
            .collect()
    }

    /// The blobs `owner` holds here, with their bytes.
    pub fn blobs_of(&self, owner: &BlobOwner) -> Vec<(BlobKey, Vec<u8>)> {
        let Some(of) = self.blob_owners.get(owner) else {
            return Vec::new();
        };
        of.keys
            .iter()
            .filter_map(|key| self.blobs.get(key).map(|held| (key.clone(), held.value.clone())))
            .collect()
    }

    /// `owner` lets go of its blobs at once: a blob no other owner holds is
    /// dropped.
    pub fn forget_blobs_of(&mut self, owner: &BlobOwner) {
        self.blob_moves.remove(owner);
        let Some(of) = self.blob_owners.remove(owner) else {
            return;
        };
        for key in of.keys {
            if let Some(held) = self.blobs.get_mut(&key) {
                held.owners.remove(owner);
                if held.owners.is_empty() {
                    self.blobs.remove(&key);
                }
            }
        }
    }

    /// Each owner whose record a leader's revision moved to other holders
    /// since the last call, with the holders it moved to, newest move only.
    /// Whoever takes them puts the owner's blobs there.
    pub fn take_blob_moves(&mut self) -> Vec<(BlobOwner, Vec<WorkerId>)> {
        std::mem::take(&mut self.blob_moves).into_iter().collect()
    }

    /// The owners never seen here whose grace has passed by `now` and whose
    /// record no look-up has found finished: ask the shard about each, and
    /// say what was found with [`Self::owner_checked`].
    pub fn owners_to_check(&self, now: Instant) -> Vec<BlobOwner> {
        let Some(grace) = self.orphan_grace else {
            return Vec::new();
        };
        self.blob_owners
            .iter()
            .filter(|(_, of)| !of.seen && !of.finished && of.since + grace <= now)
            .map(|(owner, _)| owner.clone())
            .collect()
    }

    /// What a look-up across the shard found of `owner` at `now`.
    pub fn owner_checked(&mut self, owner: &BlobOwner, state: OwnerState, now: Instant) {
        match state {
            OwnerState::Gone => self.forget_blobs_of(owner),
            OwnerState::Unfinished | OwnerState::Finished => {
                if let Some(of) = self.blob_owners.get_mut(owner) {
                    of.since = now;
                    of.finished = state == OwnerState::Finished;
                }
            }
        }
    }
}

impl Held {
    /// `finished_since` is kept once set; otherwise a finished `record`
    /// starts its retention at `now`.
    fn new(record: TaskRecord, finished_since: Option<Instant>, now: Instant) -> Self {
        let finished_since = finished_since.or(record.finished.then_some(now));
        Held {
            record,
            finished_since,
            stub: false,
        }
    }

    /// The stub of `record`: its task (named by its id, its definition and its
    /// coalescing key), version, holders and whether it is finished, and
    /// nothing else.
    fn stub_of(record: &TaskRecord, finished_since: Option<Instant>, now: Instant) -> Self {
        let key = TaskRecord {
            task: record.task.as_ref().map(|task| generated::Task {
                task_id: task.task_id.clone(),
                task_definition_id: task.task_definition_id.clone(),
                coalescing_key: task.coalescing_key.clone(),
                ..Default::default()
            }),
            version: record.version,
            placement: record.placement.clone(),
            finished: record.finished,
            ..Default::default()
        };
        Held {
            stub: true,
            ..Held::new(key, finished_since, now)
        }
    }
}

/// The task and version a well-formed record names.
pub fn identify(record: &TaskRecord) -> Result<(TaskId, RecordVersion), PutRefusal> {
    let version = record.version.as_ref().ok_or(PutRefusal::Malformed)?;
    let task_id = record
        .task
        .as_ref()
        .and_then(|task| task.task_id.as_ref())
        .ok_or(PutRefusal::Malformed)?;
    Ok((TaskId::new(task_id.value.clone()), RecordVersion::from(version)))
}

use crate::blob::{BlobKey, BlobOwner, BlobRecord};
use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::WorkerId;
use crate::scheduler::Observer;
use crate::task_record::gate::Write;
use crate::task_record::store::VersionedRecords;
use crate::time::{Clock, Duration};

/// The record sink of a node that stores its own records, as the one-node
/// runtime does: every revision is put into its own store as it is
/// published, so each write is acknowledged, or refused, before the call
/// that made it returns.
#[derive(Debug)]
pub struct LocalRecords<C: Clock> {
    worker: WorkerId,
    clock: C,
    records: VersionedRecords,
    settled: Vec<(Write, bool)>,
}

impl<C: Clock> LocalRecords<C> {
    /// The store of `worker`, whose finished records are dropped `retention`
    /// after they finish by `clock`, or kept with `None`. Every record names
    /// `worker` as its placement: with one node, it is the one that holds
    /// each record.
    pub fn new(worker: WorkerId, clock: C, retention: Option<Duration>) -> Self {
        LocalRecords {
            worker,
            clock,
            records: VersionedRecords::with_retention(retention),
            settled: Vec::new(),
        }
    }

    /// The store.
    pub fn records(&self) -> &VersionedRecords {
        &self.records
    }

    /// Drops every finished record whose retention has passed by this
    /// store's clock, and the blobs of an owner that has gone with its
    /// record, and says how many records and blobs it dropped. The timer
    /// loop calls it at the store's `next_due`, since nothing else sweeps a
    /// store that is not written to.
    pub fn sweep(&mut self) -> usize {
        self.records.sweep(self.clock.now())
    }

    /// Keeps each of `chunks` as a blob owned by `owner`. A one-node store
    /// has no grace and asks no one: the owner's record always arrives,
    /// unless its submission is refused or dropped, and then
    /// [`Self::forget_blobs_of`] is called.
    pub fn put_blobs(&mut self, owner: &BlobOwner, chunks: Vec<Vec<u8>>) {
        let now = self.clock.now();
        for chunk in chunks {
            let key = BlobKey::of(&chunk);
            let record = BlobRecord {
                value: chunk,
                owner: owner.clone(),
                placement: vec![self.worker.clone()],
                result: false,
            };
            self.records
                .put_blob(&key, record, now)
                .expect("a chunk of a payload is no larger than a blob and is stored under its own key");
        }
    }

    /// The bytes held under `key`.
    pub fn blob(&self, key: &BlobKey) -> Option<Vec<u8>> {
        self.records.blob(key).map(|record| record.value)
    }

    /// The `owner` that will never run: its blobs are dropped.
    pub fn forget_blobs_of(&mut self, owner: &BlobOwner) {
        self.records.forget_blobs_of(owner);
    }

    /// Each write since the last call, and whether the store kept it.
    pub fn take_settled(&mut self) -> Vec<(Write, bool)> {
        std::mem::take(&mut self.settled)
    }
}

impl<C: Clock> Observer for LocalRecords<C> {
    fn revision(&mut self, mut revision: TaskRecord) {
        revision.placement = vec![self.worker.clone().into()];
        let write = Write::of(&revision);
        let kept = self.records.put(revision, self.clock.now()).is_ok();
        self.settled.push((write, kept));
    }
}

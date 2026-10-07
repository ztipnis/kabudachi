//! Task records: the whole state of one Task as its shard's leader last
//! wrote it, the order between two revisions of one, and the store a worker
//! keeps them in.

mod gate;
mod local;
mod store;
mod version;

pub use gate::{EffectGate, Settled, Write};
pub use local::LocalRecords;
pub use store::{Put, PutRefusal, VersionedRecords, identify};
pub use version::{RecordVersion, VersionOrder};

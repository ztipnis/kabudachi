//! Task records: the whole state of one Task as its shard's leader last
//! wrote it, the order between two revisions of one, and the store a worker
//! keeps them in.

mod candidate;
mod gate;
mod leader_records;
mod ledger;
mod local;
mod order;
mod outbox;
mod repair;
mod store;
mod version;

pub use candidate::looks_claimable;
pub use gate::{EffectGate, PlacedWrite, PriorPlacement, Settled, Write, WriteOutcome};
pub use leader_records::{OfficeReconciliation, Placement, Progress, RecordPorts, Stuck};
pub use ledger::{Waits, WriteLedger};
pub use local::LocalRecords;
pub use order::{Settlement, WriteOrder};
pub use outbox::RecordOutbox;
pub use repair::Repair;
pub use store::{Origin, Put, PutRefusal, VersionedRecords, identify};
pub use version::{RecordVersion, VersionOrder};

use crate::scheduler::MAX_CLAIM_FRAME_BYTES;

/// Room a record keeps for its runs, the leader's placement and its own
/// fields beyond what a claim carries: enough for well over a hundred
/// attempts.
pub const RUN_HISTORY_ALLOWANCE_BYTES: u64 = 64 * 1024;

/// The largest encoded Task record a leader writes. The network's record
/// store refuses any value larger, and its record packet limit is sized from
/// this constant, so every worker accepts a record of this size.
pub const MAX_RECORD_BYTES: u64 = MAX_CLAIM_FRAME_BYTES + RUN_HISTORY_ALLOWANCE_BYTES;

/// The failure kind of a run after which the task ended because its record
/// had no room for another attempt.
pub const HISTORY_TOO_LARGE_FAILURE_KIND: &str = "HistoryTooLarge";

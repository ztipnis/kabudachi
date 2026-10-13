//! What a worker's heartbeats say of the worker itself, beside its standing
//! in the shard.

use crate::queues::Subscription;

/// What this worker's heartbeats say of it.
#[derive(Debug, Default)]
pub(super) struct SelfReport {
    /// What its heartbeats say of the runs it holds (see
    /// `WorkerNode::set_active_runs_digest`); empty until set.
    pub(super) active_runs_digest: Vec<u8>,
    /// Whether this worker runs compaction, which its heartbeats say.
    pub(super) runs_compaction: bool,
    /// The places this worker has free, which every heartbeat it sends
    /// carries.
    pub(super) available_capacity: u32,
    /// The queues this worker takes work from, which its heartbeats say; every
    /// queue until set.
    pub(super) queues: Subscription,
}

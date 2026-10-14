//! What a worker's heartbeats say of the worker itself, beside its standing
//! in the shard.

/// What this worker's heartbeats say of it.
#[derive(Debug, Default)]
pub(super) struct SelfReport {
    /// What its heartbeats say of the runs it holds (see
    /// `WorkerNode::set_active_runs_digest`); empty until set.
    pub(super) active_runs_digest: Vec<u8>,
    /// Whether this worker runs compaction, which its heartbeats say.
    pub(super) runs_compaction: bool,
}

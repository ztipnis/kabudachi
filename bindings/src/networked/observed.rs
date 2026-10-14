//! What the worker's node last showed, published for waits to read, and the
//! guard that marks the worker gone when it ends.

use std::sync::Arc;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::RealClock;
use tokio::sync::watch;

/// What the worker's node last showed.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct Observed {
    pub(super) leader: Option<(WorkerId, u64)>,
    /// How many times `leader` has changed, to or from none included, so a
    /// reader that missed a spell with no leader named still sees it passed.
    pub(super) leader_changes: u64,
    pub(super) voters: usize,
    pub(super) shard_id: String,
    /// The worker is out of its shard: `Worker::run` ended, however it did.
    pub(super) gone: bool,
    /// The node's election state; `None` before the worker's first step.
    pub(super) state: Option<WorkerState>,
    /// A leader holding a grant has vouched for hearing this worker: the
    /// gate its driver claims behind.
    pub(super) contact_floor: bool,
}

/// Marks the worker out of its shard when dropped: when `Worker::run`
/// returns, and also when it panics or is aborted, so no wait outlives it.
pub(super) struct LeftOnDrop(pub(super) Arc<watch::Sender<Observed>>);

impl Drop for LeftOnDrop {
    fn drop(&mut self) {
        self.0.send_modify(|seen| seen.gone = true);
    }
}

pub(super) fn publish(sender: &watch::Sender<Observed>, node: &WorkerNode<RealClock>) {
    let leader = node.known_leader();
    sender.send_if_modified(|seen| {
        let now = Observed {
            leader_changes: seen.leader_changes + u64::from(seen.leader != leader),
            leader,
            voters: node.voters().len(),
            shard_id: node.shard_id().as_str().to_owned(),
            gone: false,
            state: Some(node.state()),
            contact_floor: node.has_contact_floor(),
        };
        let changed = *seen != now;
        if changed {
            *seen = now;
        }
        changed
    });
}

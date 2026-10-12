//! What the worker's node last showed, published for waits to read, and the
//! guard that marks the worker gone when it ends.

use std::sync::Arc;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::time::RealClock;
use tokio::sync::watch;

/// What the worker's node last showed.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct Observed {
    pub(super) leader: Option<(WorkerId, u64)>,
    pub(super) voters: usize,
    pub(super) shard_id: String,
    /// The worker is out of its shard: `Worker::run` ended, however it did.
    pub(super) gone: bool,
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
    let now = Observed {
        leader: node.known_leader(),
        voters: node.voters().len(),
        shard_id: node.shard_id().as_str().to_owned(),
        gone: false,
    };
    sender.send_if_modified(|seen| {
        let changed = *seen != now;
        if changed {
            *seen = now.clone();
        }
        changed
    });
}
